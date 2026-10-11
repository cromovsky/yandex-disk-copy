"""Read-only SaaS Disk access for migration; resources are never published."""

import random
import time
from collections import deque
from urllib.parse import urljoin

import requests

from app.throttle import (
    DEFAULT_TIMEOUT, MAX_ATTEMPTS, RETRY_STATUSES, backoff_delay,
    get_limiter, retry_after_seconds,
)
from .errors import MigrationError, disk_path, https_url


API = "https://cloud-api.yandex.net/v1/disk"
TOKEN_URL = "https://oauth.yandex.ru/token"
RESOURCE_FIELDS = "path,type,name,size,md5,sha256,modified,resource_id"


class SaasDiskSource:
    def __init__(self, config, credentials):
        self.config = config
        self.credentials = credentials
        self.session = requests.Session()
        self.session.trust_env = False
        self.limiter = get_limiter()
        self.page_limit = 1000
        self._token = ""
        self._token_deadline = 0.0

    def close(self):
        self.session.close()

    def _request(self, method, url, **kwargs):
        kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
        kwargs.setdefault("allow_redirects", False)
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self.limiter.acquire(method)
            try:
                response = self.session.request(method, url, **kwargs)
            except requests.RequestException:
                if method == "GET" and attempt < MAX_ATTEMPTS:
                    self.limiter.pause(random.uniform(0, backoff_delay(attempt)))
                    continue
                raise MigrationError("source_unavailable") from None
            if method != "GET" or response.status_code not in RETRY_STATUSES:
                return response
            delay = retry_after_seconds(response.headers.get("Retry-After"))
            if delay is None:
                delay = random.uniform(0, backoff_delay(attempt))
            if response.status_code == 429:
                self.limiter.penalize(delay)
            if attempt == MAX_ATTEMPTS:
                return response
            response.close()
            if response.status_code != 429:
                self.limiter.pause(delay)

    def _payload(self, response):
        status = response.status_code
        if status in (401, 403):
            raise MigrationError("source_auth_failed", auth=True)
        if status == 404:
            raise MigrationError("source_not_found")
        if status != 200:
            raise MigrationError("source_unavailable")
        try:
            payload = response.json()
        except ValueError:
            raise MigrationError("source_invalid_response") from None
        if not isinstance(payload, dict):
            raise MigrationError("source_invalid_response")
        return payload

    def _owner_token(self):
        if self._token and time.monotonic() < self._token_deadline:
            return self._token
        if not self.credentials.get("source_client_secret"):
            raise MigrationError("source_auth_failed", auth=True)
        response = self._request("POST", TOKEN_URL, data={
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "client_id": self.config["source_client_id"],
            "client_secret": self.credentials["source_client_secret"],
            "subject_token": self.config["source_email"],
            "subject_token_type": "urn:yandex:params:oauth:token-type:email",
        })
        try:
            if 400 <= response.status_code < 500 and response.status_code != 429:
                raise MigrationError("source_auth_failed", auth=True)
            payload = self._payload(response)
            token = payload.get("access_token")
            ttl = payload.get("expires_in")
            if (
                not isinstance(token, str) or not token
                or not isinstance(ttl, int) or isinstance(ttl, bool) or ttl <= 0
            ):
                raise MigrationError("source_auth_failed", auth=True)
            self._token = token
            self._token_deadline = time.monotonic() + max(1, ttl - 100)
            return token
        finally:
            response.close()

    def _api_get(self, endpoint, params):
        for refresh in range(2):
            response = self._request(
                "GET", API + endpoint, params=params,
                headers={"Authorization": "OAuth " + self._owner_token()},
            )
            if response.status_code == 401 and refresh == 0:
                self._token = ""
                response.close()
                continue
            try:
                return self._payload(response)
            finally:
                response.close()

    @staticmethod
    def _metadata(payload):
        if payload.get("type") not in ("file", "dir") or not isinstance(
            payload.get("path"), str
        ):
            raise MigrationError("source_invalid_response")
        result = {key: payload[key] for key in RESOURCE_FIELDS.split(",")
                  if key in payload}
        result["path"] = disk_path(result["path"])
        for key in ("md5", "sha256"):
            if isinstance(result.get(key), str):
                result[key] = result[key].lower()
        if "size" in result and (
            not isinstance(result["size"], int) or isinstance(result["size"], bool)
            or result["size"] < 0
        ):
            raise MigrationError("source_invalid_response")
        return result

    def stat(self, path):
        canonical = disk_path(path)
        result = self._metadata(self._api_get("/resources", {
            "path": canonical, "fields": RESOURCE_FIELDS,
        }))
        if result["path"] != canonical:
            raise MigrationError("source_invalid_response")
        return result

    def walk(self, path):
        root = self.stat(path)
        if root["path"] != "disk:/":
            yield root
        if root["type"] != "dir":
            return
        pending = deque([root["path"]])
        seen = {root["path"]}
        while pending:
            folder = pending.popleft()
            offset = 0
            while True:
                payload = self._api_get("/resources", {
                    "path": folder, "limit": self.page_limit, "offset": offset,
                    "sort": "path", "fields": (
                        "_embedded.total," + ",".join(
                            "_embedded.items." + key
                            for key in RESOURCE_FIELDS.split(",")
                        )
                    ),
                })
                embedded = payload.get("_embedded")
                if not isinstance(embedded, dict):
                    raise MigrationError("source_invalid_response")
                total, items = embedded.get("total"), embedded.get("items")
                if (
                    not isinstance(total, int) or isinstance(total, bool) or total < 0
                    or not isinstance(items, list) or (not items and offset < total)
                ):
                    raise MigrationError("source_invalid_response")
                for item in items:
                    if not isinstance(item, dict):
                        raise MigrationError("source_invalid_response")
                    entry = self._metadata(item)
                    prefix = folder.rstrip("/") + "/"
                    if not entry["path"].startswith(prefix):
                        raise MigrationError("source_invalid_response")
                    relative = entry["path"][len(prefix):]
                    if "/" in relative or not relative or entry["path"] in seen:
                        raise MigrationError("source_changed")
                    seen.add(entry["path"])
                    yield entry
                    if entry["type"] == "dir":
                        pending.append(entry["path"])
                offset += len(items)
                if offset >= total:
                    break

    def download_url(self, path):
        payload = self._api_get("/resources/download", {"path": disk_path(path)})
        return https_url(payload.get("href"))

    def chunks(self, path):
        url = self.download_url(path)
        response = None
        try:
            for _hop in range(6):
                response = self._request("GET", url, stream=True)
                if response.status_code not in (301, 302, 303, 307, 308):
                    break
                next_url = response.headers.get("Location")
                response.close()
                response = None
                if not isinstance(next_url, str) or any(
                    ord(c) <= 32 for c in next_url
                ):
                    raise MigrationError("invalid_download_url")
                url = https_url(urljoin(url, next_url))
            if response is None or response.status_code != 200:
                raise MigrationError("source_download_failed")
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    yield chunk
        except requests.RequestException:
            raise MigrationError("source_download_failed") from None
        finally:
            if response is not None:
                response.close()
