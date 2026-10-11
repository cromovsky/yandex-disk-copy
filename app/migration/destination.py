"""On-premises Disk adapter with explicit, non-replayed mutations."""

import os
import random
import threading
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit, urlunsplit

import requests

from app.throttle import (
    DEFAULT_TIMEOUT, MAX_ATTEMPTS, RETRY_STATUSES, SplitRateLimiter,
    _env_float, backoff_delay, retry_after_seconds,
)
from .errors import MigrationError, disk_path, https_url
from .operations import OPERATION_ID, operation_endpoint


_LIMITERS = {}
_LIMITER_LOCK = threading.Lock()


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


class OnPremDiskDestination:
    def __init__(self, config, credentials):
        self.api_url = normalize_destination_url(config["destination_url"])
        self.token = credentials.get("destination_token")
        if not isinstance(self.token, str) or not self.token:
            raise MigrationError("destination_auth_failed", auth=True)
        self.session = requests.Session()
        self.session.trust_env = False
        self.limiter = get_onprem_limiter(self.api_url)
        self.verify = os.environ.get("ONPREM_CA_BUNDLE") or True

    def close(self):
        self.session.close()

    def _request(self, method, endpoint, *, capability=False, **kwargs):
        url = endpoint if capability else self.api_url + endpoint
        kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
        kwargs["allow_redirects"] = False
        kwargs["verify"] = self.verify
        if not capability:
            kwargs["headers"] = {"Authorization": "OAuth " + self.token}
        is_read = method == "GET"
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self.limiter.acquire(method)
            try:
                response = self.session.request(method, url, **kwargs)
            except (requests.RequestException, OSError):
                if is_read and attempt < MAX_ATTEMPTS:
                    self.limiter.pause(random.uniform(0, backoff_delay(attempt)))
                    continue
                raise MigrationError(
                    "destination_unavailable", uncertain=not is_read,
                ) from None
            if response.status_code == 429:
                delay = retry_after_seconds(response.headers.get("Retry-After"))
                self.limiter.penalize(
                    delay if delay is not None
                    else random.uniform(0, backoff_delay(attempt))
                )
            if not is_read or response.status_code not in RETRY_STATUSES:
                return response
            if attempt == MAX_ATTEMPTS:
                return response
            if response.status_code != 429:
                delay = retry_after_seconds(response.headers.get("Retry-After"))
                self.limiter.pause(
                    delay if delay is not None
                    else random.uniform(0, backoff_delay(attempt))
                )
            response.close()

    @staticmethod
    def _status(response, allowed, *, mutation=False):
        status = response.status_code
        if status in allowed:
            return
        if status in (401, 403):
            raise MigrationError("destination_auth_failed", auth=True)
        codes = {
            404: "destination_not_found", 409: "destination_conflict",
            413: "destination_file_too_large", 429: "destination_rate_limited",
            507: "destination_quota_exceeded",
        }
        uncertain = mutation and (status >= 500 or 200 <= status < 400)
        raise MigrationError(
            codes.get(status, "destination_unavailable"), uncertain=uncertain,
        )

    @staticmethod
    def _json(response, *, mutation=False):
        try:
            payload = response.json()
        except ValueError:
            raise MigrationError(
                "destination_invalid_response", uncertain=mutation,
            ) from None
        if not isinstance(payload, dict):
            raise MigrationError("destination_invalid_response", uncertain=mutation)
        return payload

    def _operation_reference(self, payload, *, mutation=False):
        identifier = payload.get("operation_id")
        href = payload.get("href")
        query_route = False
        try:
            if payload.get("method", "GET") != "GET" or (
                payload.get("templated", False) is not False
            ):
                raise ValueError
            if href is not None:
                if (
                    not isinstance(href, str) or "\\" in href
                    or any(ord(c) <= 32 for c in href)
                ):
                    raise ValueError
                reference = urlsplit(href)
                base = urlsplit(self.api_url)
                if reference.scheme or reference.netloc:
                    https_url(href, "invalid_operation_reference")
                    same_host = (
                        reference.hostname == base.hostname
                        and (reference.port or 443) == (base.port or 443)
                    )
                    if not same_host:
                        raise ValueError
                if reference.fragment:
                    raise ValueError
                path = base.path + "/operations"
                if reference.path == path:
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
                    raise ValueError
                identifier = extracted
            if (
                not isinstance(identifier, str)
                or not OPERATION_ID.fullmatch(identifier)
            ):
                raise ValueError
        except (ValueError, MigrationError):
            raise MigrationError(
                "invalid_operation_reference", uncertain=mutation,
            ) from None
        # Persist only the safe ID and route marker, never the supplied full URL.
        return "?id=" + identifier if query_route else identifier

    def stat(self, path):
        canonical = disk_path(path)
        response = self._request("GET", "/resources", params={
            "path": canonical,
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
                or disk_path(result["path"]) != canonical
            ):
                raise MigrationError("destination_invalid_response")
            for key in ("md5", "sha256"):
                if isinstance(result.get(key), str):
                    result[key] = result[key].lower()
            return result
        finally:
            response.close()

    def info(self):
        response = self._request("GET", "/")
        try:
            self._status(response, (200,))
            return self._json(response)
        finally:
            response.close()

    def ensure_dir(self, path):
        canonical = disk_path(path)
        parts = canonical[6:].split("/") if canonical != "disk:/" else []
        for index in range(1, len(parts) + 1):
            current = "disk:/" + "/".join(parts[:index])
            existing = self.stat(current)
            if existing is not None:
                if existing["type"] != "dir":
                    raise MigrationError("destination_conflict")
                continue
            response = self._request("PUT", "/resources", params={"path": current})
            try:
                if response.status_code == 409:
                    existing = self.stat(current)
                    if existing is None or existing["type"] != "dir":
                        raise MigrationError("destination_conflict")
                else:
                    self._status(response, (201,), mutation=True)
            finally:
                response.close()

    def claim_dir(self, path):
        """Claim a new root exclusively; an existing root is never adopted."""
        response = self._request(
            "PUT", "/resources", params={"path": disk_path(path)},
        )
        try:
            if response.status_code == 409:
                raise MigrationError("technical_namespace_exists")
            self._status(response, (201,), mutation=True)
        finally:
            response.close()

    def import_url(self, path, url):
        https_url(url)
        response = self._request("POST", "/resources/upload", params={
            "path": disk_path(path), "url": url,
        })
        try:
            self._status(response, (202,), mutation=True)
            return self._operation_reference(
                self._json(response, mutation=True), mutation=True,
            )
        finally:
            response.close()

    def upload(self, path, local_path):
        try:
            local = Path(local_path)
            handle = local.open("rb")
        except OSError:
            raise MigrationError("staging_file_unavailable") from None
        with handle:
            size = os.fstat(handle.fileno()).st_size
            response = self._request("GET", "/resources/upload", params={
                "path": disk_path(path), "overwrite": "false",
            })
            try:
                self._status(response, (200,))
                payload = self._json(response)
                href = https_url(payload.get("href"), "invalid_upload_url")
                operation_id = payload.get("operation_id")
                if operation_id is not None:
                    operation_id = self._operation_reference({
                        "operation_id": operation_id,
                    })
            finally:
                response.close()
            response = self._request(
                "PUT", href, capability=True, data=handle,
                headers={"Content-Length": str(size)},
            )
            try:
                self._status(response, (201, 202), mutation=True)
                if response.status_code == 202:
                    if operation_id is None:
                        raise MigrationError(
                            "invalid_operation_reference", uncertain=True,
                        )
                    return operation_id
                return None
            finally:
                response.close()

    def move(self, src, dst):
        response = self._request("POST", "/resources/move", params={
            "from": disk_path(src), "path": disk_path(dst), "overwrite": "false",
        })
        try:
            self._status(response, (201, 202), mutation=True)
            if response.status_code == 202:
                return self._operation_reference(
                    self._json(response, mutation=True), mutation=True,
                )
            return None
        finally:
            response.close()

    def poll(self, opid):
        response = self._request("GET", operation_endpoint(opid))
        try:
            self._status(response, (200,))
            status = self._json(response).get("status")
            if status not in ("in-progress", "success", "failed"):
                raise MigrationError("destination_invalid_response")
            return status
        finally:
            response.close()
