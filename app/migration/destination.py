"""On-premises Disk adapter with explicit, non-replayed mutations."""

import os
import random
import re
import threading
import time
from http.cookiejar import DefaultCookiePolicy
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
_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")


def account_uid(value):
    """Return canonical decimal uint64 text for a Disk user.uid."""
    if isinstance(value, str):
        if not value.isascii() or not value.isdecimal():
            return None
        digits = value.lstrip("0") or "0"
        if len(digits) > 20:
            return None
        value = int(digits)
    if type(value) is int and 0 <= value < 2**64:
        return str(value)
    return None


def account_upn(value):
    if not isinstance(value, str) or re.search(r"[\s/:\\\x00-\x1f\x7f]", value):
        return None
    name, _, domain = value.partition("@")
    if name and domain and "@" not in domain:
        return value
    return None


def verified_service_uid(identity, info):
    user = info.get("user") if isinstance(info, dict) else None
    uid = account_uid(user.get("uid")) if isinstance(user, dict) else None
    if uid is None:
        raise MigrationError("destination_identity_unverified", auth=True)
    expected = account_uid(identity)
    actual = uid
    if expected is None:
        expected = account_upn(identity)
        email = user.get("email")
        # OAuth binds the token to the requested email. Disk may return only its owner's UID.
        if expected is not None and email in (None, ""):
            return uid
        actual = account_upn(email)
        if expected is None or actual is None:
            raise MigrationError("destination_identity_unverified", auth=True)
    if actual != expected:
        raise MigrationError("destination_identity_mismatch", auth=True)
    return uid


def normalize_oauth_url(value):
    https_url(value, "invalid_destination_oauth_url")
    parsed = urlsplit(value)
    if parsed.path != "/token" or "?" in value or "#" in value:
        raise MigrationError("invalid_destination_oauth_url")
    host = parsed.hostname.lower()
    if ":" in host:
        host = "[" + host + "]"
    if parsed.port is not None and parsed.port != 443:
        host += ":" + str(parsed.port)
    return urlunsplit(("https", host, "/token", "", ""))


def personal_oauth_url(disk_url):
    error = "invalid_personal_disk_api_url"
    parsed = urlsplit(https_url(disk_url, error))
    service, _, domain = parsed.hostname.partition(".")
    if (service != "cloud-api" or parsed.port not in (None, 443)
            or not parsed.netloc.isascii() or len(parsed.hostname) > 253
            or not all(_DNS_LABEL.fullmatch(part) for part in domain.split("."))
            or parsed.path not in ("", "/", "/v1/disk", "/v1/disk/")
            or "?" in disk_url or "#" in disk_url):
        raise MigrationError(error)
    return f"https://oauth.{domain}/token"


def bound_oauth_url(config):
    endpoint = personal_oauth_url(config.get("destination_url"))
    if ("destination_oauth_url" in config
            and normalize_oauth_url(config["destination_oauth_url"]) != endpoint):
        raise MigrationError("invalid_destination_oauth_url")
    return endpoint


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
    supports_personal_admin_token = True
    supports_personal_service_application = True
    supports_shared_service_application = True

    def __init__(self, config, credentials):
        self.service_application = config.get("destination_auth") == "service_application"
        if self.service_application and not config.get("destination_oauth_url"):
            raise MigrationError("onprem_personal_grant_unavailable")
        self.api_url = normalize_destination_url(config["destination_url"])
        self.destination_type = config.get("destination_type", "personal")
        self.vd_hash = config.get("destination_vd_hash", "")
        destination_path(config, "disk:/")
        self.resources_endpoint = (
            "/virtual-disks/resources" if self.destination_type == "shared"
            else "/resources"
        )
        self.token = credentials.get("destination_token")
        self.expected_uid = None
        if self.service_application:
            self.token = None
            self.token_lock = threading.Lock()
            self.token_deadline = 0
            self.oauth_url = bound_oauth_url(config)
            self.identity = config.get("destination_identity")
            uid = account_uid(self.identity)
            if not account_upn(self.identity) and (uid is None or self.destination_type == "shared"):
                raise MigrationError("destination_identity_unverified", auth=True)
            self.exchange_form = {
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "client_id": config.get("destination_client_id"),
                "client_secret": credentials.get("destination_client_secret"),
                "access_token": credentials.get("destination_owner_token"),
                "scope": "cloud_api:disk.read cloud_api:disk.write cloud_api:disk.info",
                "subject_token": uid if uid is not None else self.identity,
                "subject_token_type": "urn:yandex:params:oauth:token-type:" + (
                    "uid" if uid is not None else "email"
                ),
            }
            if not all(isinstance(v, str) and v.strip() for v in self.exchange_form.values()):
                raise MigrationError("destination_auth_failed", auth=True)
        elif not isinstance(self.token, str) or not self.token:
            raise MigrationError("destination_auth_failed", auth=True)
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.cookies = requests.cookies.RequestsCookieJar(
            policy=DefaultCookiePolicy(allowed_domains=()),
        )
        self.limiter = get_onprem_limiter(self.api_url)
        self.verify = os.environ.get("ONPREM_CA_BUNDLE") or True
        self.last_operation_diagnostics = {}
        self.checkpoint = None

    def close(self):
        self.session.close()

    def _verified_access(self):
        with self.token_lock:
            now = time.monotonic()
            if self.token and now < self.token_deadline:
                return self.token, self.owner_info, self.token_deadline
            try:
                return self._exchange_and_verify(now)
            except MigrationError as exc:
                exc._migration_stage = "check_destination_access"
                raise

    def _exchange_and_verify(self, now):
        try:
            response = self.session.request(
                "POST", self.oauth_url, data=self.exchange_form,
                verify=self.verify, allow_redirects=False, timeout=DEFAULT_TIMEOUT,
            )
        except (requests.RequestException, OSError):
            raise MigrationError("destination_unavailable") from None
        try:
            if response.status_code != 200:
                rejected = 400 <= response.status_code < 500 and response.status_code != 429
                raise MigrationError(
                    "destination_auth_failed" if rejected else "destination_unavailable",
                    auth=rejected, http_status=response.status_code,
                )
            try:
                payload = response.json()
            except ValueError:
                payload = None
            token = payload.get("access_token") if isinstance(payload, dict) else None
            ttl = payload.get("expires_in", 3600) if isinstance(payload, dict) else None
            if (not isinstance(token, str) or not token.isascii()
                    or not token.isprintable() or " " in token
                    or not token or type(ttl) is not int or ttl <= 0):
                raise MigrationError("destination_auth_failed", auth=True)
        finally:
            response.close()
        ttl = min(ttl, 3600)
        deadline = now + ttl - min(300, ttl / 10)
        proof = self._request("GET", "/", proof_access=(token, deadline))
        try:
            self._status(proof, (200,))
            info = self._json(proof)
        finally:
            proof.close()
        uid = verified_service_uid(self.identity, info)
        if self.expected_uid is not None and self.expected_uid != uid:
            raise MigrationError("destination_identity_mismatch", auth=True)
        if time.monotonic() >= deadline:
            raise MigrationError("destination_auth_failed", auth=True)
        self.expected_uid = uid
        self.owner_info = info
        self.token = token
        self.token_deadline = deadline
        return token, info, deadline

    def _acquire_slot(self, method):
        if self.checkpoint is not None:
            self.limiter.acquire(method, checkpoint=self.checkpoint)
        else:
            self.limiter.acquire(method)

    def _request(self, method, endpoint, *, capability=False, proof_access=None, **kwargs):
        url = endpoint if capability else self.api_url + endpoint
        is_read = method == "GET"
        kwargs.setdefault("timeout", (5.0, 15.0) if is_read else DEFAULT_TIMEOUT)
        kwargs["allow_redirects"] = False
        kwargs["verify"] = self.verify
        if self.service_application and not capability and proof_access is None:
            # Refresh and proof consume their own time and rate-limit slot.
            for _ in range(2):
                token, _, deadline = self._verified_access()
                self._acquire_slot(method)
                if time.monotonic() < deadline:
                    break
            else:
                error = MigrationError("destination_auth_failed", auth=True)
                error._migration_stage = "check_destination_access"
                raise error
        else:
            self._acquire_slot(method)
            token = self.token
            if proof_access is not None:
                token, deadline = proof_access
                if time.monotonic() >= deadline:
                    raise MigrationError("destination_auth_failed", auth=True)
        if not capability:
            kwargs["headers"] = {"Authorization": "OAuth " + token}
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
        if self.service_application:
            owner = self._verified_access()[1]
        else:
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
