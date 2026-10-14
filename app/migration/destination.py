"""On-premises Disk adapter with explicit, non-replayed mutations."""

import os
import random
import re
import threading
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit, urlunsplit

import requests

from app.throttle import (
    DEFAULT_TIMEOUT, SplitRateLimiter,
    _env_float, backoff_delay, retry_after_seconds,
)
from .errors import MigrationError, disk_path, https_url
from .diagnostics import request_not_sent, response_details, transport_details
from .operations import OPERATION_ID, operation_endpoint


_LIMITERS = {}
_LIMITER_LOCK = threading.Lock()
_VD_HASH = re.compile(r"[A-Za-z0-9_-]{1,256}")


def normalize_destination_url(value):
    https_url(value, "invalid_destination_url")
    parsed = urlsplit(value)
    if parsed.query or parsed.path.rstrip("/") not in ("", "/v1/disk"):
        raise MigrationError("invalid_destination_url")
    host = parsed.hostname.lower()
    if ":" in host:
        host = "[" + host + "]"
    if parsed.port and parsed.port != 443:
        host += ":" + str(parsed.port)
    return urlunsplit(("https", host, "/v1/disk", "", ""))


def destination_path(config, logical_path):
    """Qualify a logical path without credentials or an HTTP client."""
    destination_type = config.get("destination_type", "personal")
    if destination_type not in ("personal", "shared"):
        raise MigrationError("invalid_destination_type")
    if destination_type == "shared":
        vd_hash = config.get("destination_vd_hash", "")
        if not isinstance(vd_hash, str) or not _VD_HASH.fullmatch(vd_hash):
            raise MigrationError("invalid_destination_shared_disk")
        if isinstance(logical_path, str) and logical_path.startswith("vd:"):
            raise MigrationError("invalid_disk_path")
        return f"vd:{vd_hash}:{disk_path(logical_path)}"
    return disk_path(logical_path)


def get_onprem_limiter(api_url=None):
    # Different installations must not consume or cool down each other's budget.
    key = api_url or "default"
    with _LIMITER_LOCK:
        if key not in _LIMITERS:
            _LIMITERS[key] = SplitRateLimiter(
                _env_float("ONPREM_API_READ_RPS", 10.0),
                _env_float("ONPREM_API_WRITE_RPS", 2.0),
            )
        return _LIMITERS[key]


class _UploadProgress:
    def __init__(self, handle, progress):
        self.handle, self.progress = handle, progress

    def read(self, size=-1):
        chunk = self.handle.read(size)
        if chunk:
            self.progress(len(chunk))
        return chunk

    def __getattr__(self, name):
        return getattr(self.handle, name)


class OnPremDiskDestination:
    def __init__(self, config, credentials):
        self.api_url = normalize_destination_url(config["destination_url"])
        self.destination_type = config.get("destination_type", "personal")
        self.vd_hash = config.get("destination_vd_hash", "")
        destination_path(config, "disk:/")
        self.resources_endpoint = (
            "/virtual-disks/resources" if self.destination_type == "shared"
            else "/resources"
        )
        self.token = credentials.get("destination_token")
        if not isinstance(self.token, str) or not self.token:
            raise MigrationError("destination_auth_failed", auth=True)
        self.session = requests.Session()
        self.session.trust_env = False
        self.limiter = get_onprem_limiter(self.api_url)
        self.verify = os.environ.get("ONPREM_CA_BUNDLE") or True
        self.last_operation_diagnostics = {}
        self.checkpoint = None

    def close(self):
        self.session.close()

    def _request(self, method, endpoint, *, capability=False, **kwargs):
        url = endpoint if capability else self.api_url + endpoint
        is_read = method == "GET"
        kwargs.setdefault("timeout", (5.0, 15.0) if is_read else DEFAULT_TIMEOUT)
        kwargs["allow_redirects"] = False
        kwargs["verify"] = self.verify
        if not capability:
            kwargs["headers"] = {"Authorization": "OAuth " + self.token}
        if self.checkpoint is not None:
            self.limiter.acquire(method, checkpoint=self.checkpoint)
        else:
            self.limiter.acquire(method)
        try:
            response = self.session.request(method, url, **kwargs)
        except (requests.RequestException, OSError) as exc:
            if self.checkpoint is not None:
                self.checkpoint()
            if isinstance(exc, requests.Timeout):
                code = "destination_timeout"
            elif isinstance(exc, requests.ConnectionError):
                code = "destination_connection_failed"
            elif isinstance(exc, (
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ContentDecodingError,
            )):
                code = "destination_stream_interrupted"
            else:
                code = "destination_unavailable"
            raise MigrationError(
                code, uncertain=not is_read and not request_not_sent(exc),
                diagnostics=transport_details(exc, "onprem", url=url),
            ) from None
        if response.status_code == 429:
            delay = retry_after_seconds(response.headers.get("Retry-After"))
            self.limiter.penalize(
                delay if delay is not None else random.uniform(0, backoff_delay(1))
            )
        if self.checkpoint is not None:
            try:
                self.checkpoint()
            except Exception:
                response.close()
                raise
        return response

    @staticmethod
    def _status(response, allowed, *, mutation=False):
        status = response.status_code
        if status in allowed:
            return
        details = response_details(response, "onprem")
        if status in (401, 403):
            raise MigrationError("destination_auth_failed", auth=True,
                                 http_status=status, diagnostics=details)
        codes = {
            404: "destination_not_found", 409: "destination_conflict",
            413: "destination_file_too_large", 423: "destination_read_only",
            429: "destination_rate_limited",
            507: "destination_quota_exceeded",
        }
        uncertain = mutation and (status >= 500 or 200 <= status < 400)
        raise MigrationError(
            codes.get(status, "destination_unavailable"), uncertain=uncertain,
            http_status=status, diagnostics=details,
        )

    @staticmethod
    def _json(response, *, mutation=False):
        try:
            payload = response.json()
        except ValueError:
            raise MigrationError(
                "destination_invalid_response", uncertain=mutation,
                http_status=response.status_code,
                diagnostics=response_details(response, "onprem"),
            ) from None
        if not isinstance(payload, dict):
            raise MigrationError("destination_invalid_response", uncertain=mutation,
                                 http_status=response.status_code,
                                 diagnostics=response_details(response, "onprem"))
        return payload

    def _operation_reference(self, payload, *, mutation=False, http_status=None,
                             diagnostics=None):
        identifier = payload.get("operation_id")
        href = payload.get("href")
        query_route = False
        issue = "metadata"
        try:
            if payload.get("method", "GET") != "GET" or (
                payload.get("templated", False) is not False
            ):
                raise ValueError
            if href is not None:
                issue = "url"
                if (
                    not isinstance(href, str) or "\\" in href
                    or any(ord(c) <= 32 for c in href)
                ):
                    raise ValueError
                reference = urlsplit(href)
                base = urlsplit(self.api_url)
                if reference.scheme or reference.netloc:
                    https_url(href, "invalid_operation_reference")
                    issue = "origin"
                    same_host = (
                        reference.hostname == base.hostname
                        and (reference.port or 443) == (base.port or 443)
                    )
                    if not same_host:
                        raise ValueError
                if reference.fragment:
                    issue = "url"
                    raise ValueError
                path = base.path + "/operations"
                issue = "route"
                if reference.path == path:
                    issue = "query"
                    parameters = parse_qsl(
                        reference.query, keep_blank_values=True,
                        strict_parsing=True, max_num_fields=1,
                    )
                    if len(parameters) != 1 or parameters[0][0] != "id":
                        raise ValueError
                    extracted = parameters[0][1]
                    query_route = True
                elif reference.path.startswith(path + "/") and not reference.query:
                    extracted = reference.path[len(path) + 1:]
                else:
                    raise ValueError
                if identifier is not None and identifier != extracted:
                    issue = "id_mismatch"
                    raise ValueError
                identifier = extracted
            issue = "missing_id" if identifier is None else "id_format"
            if (
                not isinstance(identifier, str)
                or not OPERATION_ID.fullmatch(identifier)
            ):
                raise ValueError
        except (ValueError, MigrationError):
            raise MigrationError(
                "invalid_operation_reference", uncertain=mutation,
                http_status=http_status, operation_reference_issue=issue,
                diagnostics=diagnostics,
            ) from None
        # Persist only the safe ID and route marker, never the supplied full URL.
        return "?id=" + identifier if query_route else identifier

    def _wire_path(self, path):
        return destination_path({
            "destination_type": self.destination_type,
            "destination_vd_hash": self.vd_hash,
        }, path)

    def _logical_path(self, path):
        if self.destination_type == "shared" and path.startswith("vd:"):
            prefixes = (f"vd:{self.vd_hash}:disk:", f"vd:/{self.vd_hash}/disk")
            for prefix in prefixes:
                if path == prefix or path.startswith(prefix + "/"):
                    path = path[len(prefix):]
                    break
            else:
                raise MigrationError("destination_invalid_response")
        return disk_path(path)

    def stat(self, path):
        wire_path = self._wire_path(path)
        canonical = disk_path(path)
        response = self._request("GET", self.resources_endpoint, params={
            "path": wire_path,
            "fields": "path,type,name,size,md5,sha256,modified,resource_id",
        })
        try:
            if response.status_code == 404:
                return None
            self._status(response, (200,))
            result = self._json(response)
            if (
                result.get("type") not in ("file", "dir")
                or not isinstance(result.get("path"), str)
                or self._logical_path(result["path"]) != canonical
            ):
                raise MigrationError(
                    "destination_invalid_response", http_status=response.status_code,
                    diagnostics=response_details(response, "onprem"),
                )
            if self.destination_type == "shared":
                if result.get("vd_hash", self.vd_hash) != self.vd_hash:
                    raise MigrationError(
                        "destination_invalid_response", http_status=response.status_code,
                        diagnostics=response_details(response, "onprem"),
                    )
                result["path"] = canonical
            for key in ("md5", "sha256"):
                if isinstance(result.get(key), str):
                    result[key] = result[key].lower()
            return result
        except MigrationError as exc:
            if exc.http_status is None:
                raise MigrationError(
                    exc.code, uncertain=exc.uncertain, auth=exc.auth,
                    http_status=response.status_code,
                    diagnostics=response_details(response, "onprem"),
                ) from None
            raise
        finally:
            response.close()

    def info(self):
        response = self._request("GET", "/")
        try:
            self._status(response, (200,))
            owner = self._json(response)
        finally:
            response.close()
        if self.destination_type == "personal":
            return owner
        response = self._request("GET", "/virtual-disks", params={
            "vd_hash": self.vd_hash,
        })
        try:
            self._status(response, (200,))
            shared = self._json(response)
            if shared.get("vd_hash") != self.vd_hash:
                raise MigrationError(
                    "destination_invalid_response", http_status=response.status_code,
                    diagnostics=response_details(response, "onprem"),
                )
            permissions = shared.get("permissions")
            if (not isinstance(permissions, list)
                    or "read" not in permissions or "write" not in permissions):
                raise MigrationError(
                    "destination_shared_access_required", http_status=response.status_code,
                    diagnostics=response_details(response, "onprem"),
                )
            return dict(shared, user=owner.get("user") or {})
        finally:
            response.close()

    def check_available(self):
        info = self.info()
        root = self.stat("disk:/")
        if root is None:
            raise MigrationError("destination_not_found", http_status=404,
                                 diagnostics={"error_side": "onprem"})
        if root.get("type") != "dir":
            raise MigrationError("destination_invalid_response", http_status=200,
                                 diagnostics={"error_side": "onprem"})
        return info

    def ensure_dir(self, path):
        self._wire_path(path)
        canonical = disk_path(path)
        parts = canonical[6:].split("/") if canonical != "disk:/" else []
        for index in range(1, len(parts) + 1):
            current = "disk:/" + "/".join(parts[:index])
            existing = self.stat(current)
            if existing is not None:
                if existing["type"] != "dir":
                    raise MigrationError("destination_conflict")
                continue
            response = self._request("PUT", self.resources_endpoint, params={
                "path": self._wire_path(current),
            })
            try:
                if response.status_code == 409:
                    existing = self.stat(current)
                    if existing is None or existing["type"] != "dir":
                        raise MigrationError(
                            "destination_conflict", http_status=response.status_code,
                            diagnostics=response_details(response, "onprem"),
                        )
                else:
                    self._status(response, (201,), mutation=True)
            finally:
                response.close()

    def claim_dir(self, path):
        """Claim a new root exclusively; an existing root is never adopted."""
        response = self._request(
            "PUT", self.resources_endpoint, params={"path": self._wire_path(path)},
        )
        try:
            if response.status_code == 409:
                raise MigrationError(
                    "technical_namespace_exists", http_status=409,
                    diagnostics=response_details(response, "onprem"),
                )
            self._status(response, (201,), mutation=True)
        finally:
            response.close()

    def import_url(self, path, url, *, overwrite=False):
        https_url(url)
        response = self._request("POST", self.resources_endpoint + "/upload", params={
            "path": self._wire_path(path), "url": url,
            "overwrite": "true" if overwrite is True else "false",
        })
        try:
            self._status(response, (202,), mutation=True)
            return self._operation_reference(
                self._json(response, mutation=True), mutation=True,
                http_status=response.status_code,
                diagnostics=response_details(response, "onprem"),
            )
        finally:
            response.close()

    def upload(self, path, local_path, *, before_upload=None, on_progress=None,
               overwrite=False):
        try:
            local = Path(local_path)
            handle = local.open("rb")
        except OSError:
            raise MigrationError("staging_file_unavailable") from None
        with handle:
            size = os.fstat(handle.fileno()).st_size
            response = self._request(
                "GET", self.resources_endpoint + "/upload", params={
                    "path": self._wire_path(path),
                    "overwrite": "true" if overwrite is True else "false",
                },
            )
            try:
                self._status(response, (200,))
                payload = self._json(response)
                if payload.get("method", "PUT") != "PUT" or (
                    payload.get("templated", False) is not False
                ):
                    raise MigrationError(
                        "invalid_upload_url", http_status=response.status_code,
                        diagnostics=response_details(response, "onprem"),
                    )
                try:
                    href = https_url(payload.get("href"), "invalid_upload_url")
                except MigrationError as exc:
                    raise MigrationError(
                        exc.code, http_status=response.status_code,
                        diagnostics=response_details(response, "onprem"),
                    ) from None
                operation_id = payload.get("operation_id")
                if operation_id is not None:
                    operation_id = self._operation_reference({
                        "operation_id": operation_id,
                    }, http_status=response.status_code,
                        diagnostics=response_details(response, "onprem"))
            finally:
                response.close()
            if before_upload is not None:
                before_upload(operation_id)
            # An empty file stream makes requests add chunked framing to CL: 0.
            body = handle if size else b""
            if on_progress is not None:
                on_progress(0)
                if size:
                    body = _UploadProgress(handle, on_progress)
            response = self._request(
                "PUT", href, capability=True, data=body,
                headers={"Content-Length": str(size)},
            )
            try:
                self._status(response, (201, 202), mutation=True)
                if response.status_code == 202:
                    if operation_id is None:
                        raise MigrationError(
                            "invalid_operation_reference", uncertain=True,
                            http_status=response.status_code,
                            diagnostics=response_details(response, "onprem"),
                        )
                    return operation_id
                return None
            finally:
                response.close()

    def move(self, src, dst):
        response = self._request("POST", self.resources_endpoint + "/move", params={
            "from": self._wire_path(src), "path": self._wire_path(dst),
            "overwrite": "false",
        })
        try:
            self._status(response, (201, 202), mutation=True)
            if response.status_code == 202:
                return self._operation_reference(
                    self._json(response, mutation=True), mutation=True,
                    http_status=response.status_code,
                    diagnostics=response_details(response, "onprem"),
                )
            return None
        finally:
            response.close()

    def poll(self, opid):
        self.last_operation_diagnostics = {}
        response = self._request("GET", operation_endpoint(opid))
        try:
            self._status(response, (200,))
            status = self._json(response).get("status")
            if status not in ("in-progress", "success", "failed"):
                raise MigrationError(
                    "destination_invalid_response", http_status=response.status_code,
                    diagnostics=response_details(response, "onprem"),
                )
            if status == "failed":
                self.last_operation_diagnostics = dict(
                    response_details(response, "onprem"), http_status=response.status_code,
                )
            return status
        finally:
            response.close()
