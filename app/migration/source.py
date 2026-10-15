"""SaaS Disk reads and explicit, owner-authorized migration publication."""

import math
import random
import re
import threading
import time
import unicodedata
from collections import deque
from urllib.parse import urljoin, urlsplit

import requests

from app.throttle import (
    DEFAULT_TIMEOUT, MAX_ATTEMPTS, RETRY_STATUSES, backoff_delay,
    get_limiter, retry_after_seconds,
)
from .errors import MigrationError, disk_path, https_url
from app.retry_policy import RetryPolicy
from .diagnostics import response_details, transport_details


API = "https://cloud-api.yandex.net/v1/disk"
TOKEN_URL = "https://oauth.yandex.ru/token"
RESOURCE_FIELDS = "path,type,name,size,md5,sha256,modified,resource_id"


def shared_source_folder(name, vd_hash):
    """A stable single directory component, keeping the full distinguishing ID."""
    if not isinstance(vd_hash, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", vd_hash):
        raise MigrationError("source_invalid_response")
    if not isinstance(name, str) or not name:
        raise MigrationError("source_invalid_response")
    safe = unicodedata.normalize("NFC", name)
    safe = "".join("_" if char in "/\\:" or ord(char) < 32 or ord(char) == 127
                   else char for char in safe).strip(" .") or "Общий диск"
    suffix = " (" + vd_hash + ")"
    budget = 255 - len(suffix.encode("utf-8"))
    if budget < 1:
        raise MigrationError("source_invalid_response")
    safe = safe.encode("utf-8")[:budget].decode("utf-8", errors="ignore").rstrip(" .")
    if not safe:
        safe = "_"
    return safe + suffix


class _RetryCancelled(Exception):
    pass


class SaasDiskSource:
    def __init__(self, config, credentials):
        self.config = config
        self.credentials = credentials
        self.session = requests.Session()
        self.session.trust_env = False
        self.limiter = get_limiter()
        self.page_limit = max(1, min(1000, int(config.get("page_limit", 1000))))
        self._token_lock = threading.RLock()
        self._token = ""
        self._token_deadline = 0.0
        self._publications = {}
        self._unknown_publications = set()
        self.checkpoint = None
        self.retry_policy = (RetryPolicy(config["retry_enabled"])
                             if "retry_enabled" in config else None)

    def close(self):
        self.session.close()

    def _retries_enabled(self, generation=None):
        policy = self.retry_policy
        return policy is None or (policy.enabled and (
            generation is None or generation == policy.cancellation_generation
        ))

    def _retry_wait(self, delay, checkpoint, generation):
        if self.retry_policy is not None:
            return self.retry_policy.wait(delay, checkpoint=checkpoint, generation=generation)
        if self.checkpoint is not None:
            self.limiter.pause(delay, checkpoint=checkpoint)
        else:
            self.limiter.pause(delay)
        return True

    def _request(self, method, url, *, max_attempts=MAX_ATTEMPTS,
                 checkpoint=None, **kwargs):
        checkpoint = checkpoint or self.checkpoint
        generation = (self.retry_policy.cancellation_generation
                      if self.retry_policy is not None else None)
        kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
        kwargs.setdefault("allow_redirects", False)
        previous_response, previous_error = None, None
        for attempt in range(1, max_attempts + 1):
            def request_checkpoint():
                if checkpoint is not None:
                    checkpoint()
                if attempt > 1 and not self._retries_enabled(generation):
                    raise _RetryCancelled()
            try:
                if self.checkpoint is not None or (attempt > 1 and self.retry_policy is not None):
                    self.limiter.acquire(method, checkpoint=request_checkpoint)
                else:
                    self.limiter.acquire(method)
                request_checkpoint()
            except _RetryCancelled:
                if previous_response is not None:
                    return previous_response
                raise previous_error from None
            except BaseException:
                if previous_response is not None:
                    previous_response.close()
                raise
            if previous_response is not None:
                previous_response.close()
                previous_response = None
            try:
                response = self.session.request(method, url, **kwargs)
            except requests.RequestException as exc:
                if checkpoint is not None:
                    checkpoint()
                code = ("source_timeout" if isinstance(exc, requests.Timeout) else
                        "source_connection_failed" if isinstance(
                            exc, requests.ConnectionError,
                        ) else "source_unavailable")
                previous_error = MigrationError(
                    code, diagnostics=transport_details(exc, "saas", url=url),
                )
                if method == "GET" and attempt < max_attempts and self._retries_enabled(generation):
                    delay = random.uniform(0, backoff_delay(attempt))
                    if self._retry_wait(delay, checkpoint, generation):
                        continue
                raise previous_error from None
            cooldown = None
            if response.status_code == 429:
                delay = retry_after_seconds(response.headers.get("Retry-After"))
                if delay is None or not math.isfinite(delay):
                    delay = random.uniform(0, backoff_delay(attempt))
                cooldown = self.limiter.penalize(delay)
            try:
                if checkpoint is not None:
                    checkpoint()
            except BaseException:
                response.close()
                raise
            if (response.status_code not in RETRY_STATUSES or method != "GET"
                    or attempt == max_attempts or not self._retries_enabled(generation)):
                return response
            delay = retry_after_seconds(response.headers.get("Retry-After"))
            if delay is None or not math.isfinite(delay):
                delay = random.uniform(0, backoff_delay(attempt))
            if response.status_code == 429:
                # Keep the shared barrier even when this particular retry is cancelled.
                # Wait on the job switch too, so disabling never submits a queued replay.
                if self.retry_policy is None:
                    previous_response = response
                    continue
                delay = cooldown if cooldown is not None else delay
            try:
                if not self._retry_wait(delay, checkpoint, generation):
                    return response
            except BaseException:
                response.close()
                raise
            previous_response = response

    def _payload(self, response, *, url=None):
        status = response.status_code
        diagnostics = {
            "http_status": status,
            "retry_after": retry_after_seconds(response.headers.get("Retry-After")),
        }
        if status != 200:
            diagnostics["diagnostics"] = response_details(response, "saas", url=url)
        if status in (401, 403):
            raise MigrationError("source_auth_failed", auth=True, **diagnostics)
        if status == 404:
            raise MigrationError("source_not_found", **diagnostics)
        if status != 200:
            raise MigrationError("source_unavailable", **diagnostics)
        try:
            payload = response.json()
        except ValueError:
            raise MigrationError(
                "source_invalid_response", **diagnostics,
                diagnostics=response_details(response, "saas", url=url),
            ) from None
        if not isinstance(payload, dict):
            raise MigrationError(
                "source_invalid_response", **diagnostics,
                diagnostics=response_details(response, "saas", url=url),
            )
        return payload

    @property
    def shared(self):
        return self.config.get("source_type", "personal") == "shared"

    def _resource_endpoint(self):
        return "/virtual-disks/resources" if self.shared else "/resources"

    def _source_path(self, path):
        canonical = disk_path(path)
        if not self.shared:
            return canonical
        vd_hash = self.config.get("source_vd_hash")
        if not isinstance(vd_hash, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", vd_hash):
            raise MigrationError("source_invalid_response")
        return "vd:" + vd_hash + ":" + canonical

    def _canonical_path(self, path):
        if self.shared and isinstance(path, str) and path.startswith("vd:"):
            vd_hash = self.config.get("source_vd_hash")
            for prefix in ("vd:" + vd_hash + ":disk:", "vd:/" + vd_hash + "/disk"):
                if path == prefix or path.startswith(prefix + "/"):
                    return disk_path(path[len(prefix):] or "/")
            raise MigrationError("source_invalid_response")
        return disk_path(path)

    def describe(self):
        if not self.shared:
            return {"type": "personal", "name": self.config["source_email"], "vd_hash": None}
        vd_hash = self.config.get("source_vd_hash")
        self._source_path("/")
        payload = self._api_get("/virtual-disks", {"vd_hash": vd_hash})
        name = payload.get("name")
        if (not isinstance(name, str) or not name or len(name) > 4096
                or payload.get("vd_hash", vd_hash) != vd_hash):
            raise MigrationError("source_invalid_response")
        return {"type": "shared", "name": name, "vd_hash": vd_hash}

    def _invalidate_token(self, rejected):
        with self._token_lock:
            if self._token == rejected:
                self._token = ""
                self._token_deadline = 0.0

    def _owner_token(self):
        with self._token_lock:
            return self._refresh_owner_token()

    def _check_admin_access(self):
        """Check source membership and status without changing the employee."""
        admin_token = self.credentials.get("source_admin_token")
        if not admin_token:
            return
        org_id = self.config.get("source_org_id")
        if not isinstance(org_id, str) or not re.fullmatch(r"[0-9]{1,32}", org_id):
            raise MigrationError("source_admin_check_failed")
        url = "https://cloud-api.yandex.net/v1/directory/organizations/" + org_id + "/users"
        try:
            response = self._request("GET", url, params={
                "email": self.config["source_email"], "limit": 100, "offset": 0,
            }, headers={"Authorization": "OAuth " + admin_token})
        except MigrationError as exc:
            raise MigrationError("source_admin_check_failed",
                                 http_status=exc.http_status,
                                 retry_after=exc.retry_after,
                                 diagnostics=exc.diagnostics) from None
        try:
            details = {"http_status": response.status_code,
                       "diagnostics": response_details(response, "saas", url=url),
                       "retry_after": retry_after_seconds(response.headers.get("Retry-After"))}
            if response.status_code in {401, 403}:
                raise MigrationError("source_admin_auth_failed", auth=True, **details)
            if response.status_code == 404:
                raise MigrationError("source_user_not_in_organization", **details)
            if response.status_code != 200:
                raise MigrationError("source_admin_check_failed", **details)
            try:
                payload = response.json()
            except ValueError:
                raise MigrationError("source_admin_check_failed", **details) from None
            if not isinstance(payload, dict):
                raise MigrationError("source_admin_check_failed", **details)
            users, total = payload.get("items"), payload.get("total")
            if (not isinstance(users, list) or not isinstance(total, int)
                    or isinstance(total, bool) or total < 0):
                raise MigrationError("source_admin_check_failed", **details)
            if total == 0 and not users:
                raise MigrationError("source_user_not_in_organization", **details)
            # An exact email lookup must identify one employee, not silently
            # pick the first page or an arbitrary matching account.
            if total != 1 or len(users) != 1 or not isinstance(users[0], dict):
                raise MigrationError("source_admin_check_failed", **details)
            user = users[0]
            uid = user.get("id")
            if (isinstance(uid, bool) or not isinstance(uid, (str, int))
                    or not re.fullmatch(r"[1-9][0-9]{0,19}", str(uid))
                    or not isinstance(user.get("is_enabled"), bool)):
                raise MigrationError("source_admin_check_failed", **details)
            emails = [user.get("email"), user.get("default_email")]
            aliases = user.get("aliases")
            if isinstance(aliases, list):
                domains = {email.rsplit("@", 1)[1] for email in emails
                           if isinstance(email, str) and email.count("@") == 1}
                for alias in aliases:
                    if not isinstance(alias, str):
                        continue
                    if "@" in alias:
                        emails.append(alias)
                    else:
                        emails.extend(alias + "@" + domain for domain in domains)
            contacts = user.get("contacts")
            if isinstance(contacts, list):
                emails.extend(contact.get("value") for contact in contacts
                              if isinstance(contact, dict) and contact.get("type") == "email"
                              and (contact.get("synthetic") is True
                                   or contact.get("alias") is True))
            if self.config["source_email"].casefold() not in {
                email.casefold() for email in emails if isinstance(email, str)
            }:
                raise MigrationError("source_user_not_in_organization", **details)
            if not user["is_enabled"]:
                raise MigrationError("source_user_blocked", **details)
        finally:
            response.close()

    def _refresh_owner_token(self):
        if self._token and time.monotonic() < self._token_deadline:
            return self._token
        if not self.credentials.get("source_client_secret"):
            raise MigrationError("source_auth_failed", auth=True)
        self._check_admin_access()
        response = self._request("POST", TOKEN_URL, data={
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "client_id": self.config["source_client_id"],
            "client_secret": self.credentials["source_client_secret"],
            "subject_token": self.config["source_email"],
            "subject_token_type": "urn:yandex:params:oauth:token-type:email",
        })
        try:
            if 400 <= response.status_code < 500 and response.status_code != 429:
                raise MigrationError("source_auth_failed", auth=True,
                                     http_status=response.status_code,
                                     diagnostics=response_details(
                                         response, "saas", url=TOKEN_URL,
                                     ))
            payload = self._payload(response, url=TOKEN_URL)
            token = payload.get("access_token")
            ttl = payload.get("expires_in")
            if (
                not isinstance(token, str) or not token
                or not isinstance(ttl, int) or isinstance(ttl, bool) or ttl <= 0
            ):
                raise MigrationError("source_auth_failed", auth=True,
                                     http_status=response.status_code,
                                     diagnostics=response_details(
                                         response, "saas", url=TOKEN_URL,
                                     ))
            self._token = token
            self._token_deadline = time.monotonic() + max(1, ttl - 100)
            return token
        finally:
            response.close()

    def _api_get(self, endpoint, params, *, max_attempts=MAX_ATTEMPTS):
        for refresh in range(2):
            token = self._owner_token()
            response = self._request(
                "GET", API + endpoint, params=params, max_attempts=max_attempts,
                headers={"Authorization": "OAuth " + token},
            )
            if response.status_code == 401 and refresh == 0:
                self._invalidate_token(token)
                response.close()
                continue
            try:
                return self._payload(response, url=API + endpoint)
            finally:
                response.close()

    def _metadata(self, payload):
        if payload.get("type") not in ("file", "dir") or not isinstance(
            payload.get("path"), str
        ):
            raise MigrationError("source_invalid_response",
                                 source_path=payload.get("path"))
        result = {key: payload[key] for key in RESOURCE_FIELDS.split(",")
                  if key in payload}
        result["path"] = self._canonical_path(result["path"])
        for key in ("md5", "sha256"):
            if isinstance(result.get(key), str):
                result[key] = result[key].lower()
        if "size" in result and (
            not isinstance(result["size"], int) or isinstance(result["size"], bool)
            or result["size"] < 0
        ):
            raise MigrationError("source_invalid_response", source_path=result["path"])
        return result

    def stat(self, path):
        canonical = disk_path(path)
        result = self._metadata(self._api_get(self._resource_endpoint(), {
            "path": self._source_path(canonical), "fields": RESOURCE_FIELDS,
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
                try:
                    payload = self._api_get(self._resource_endpoint(), {
                        "path": self._source_path(folder), "limit": self.page_limit, "offset": offset,
                        "sort": "path", "fields": (
                            "_embedded.total," + ",".join(
                                "_embedded.items." + key
                                for key in RESOURCE_FIELDS.split(",")
                            )
                        ),
                    })
                except MigrationError as exc:
                    exc.source_path = folder
                    raise
                embedded = payload.get("_embedded")
                if not isinstance(embedded, dict):
                    raise MigrationError("source_invalid_response", source_path=folder)
                total, items = embedded.get("total"), embedded.get("items")
                if (
                    not isinstance(total, int) or isinstance(total, bool) or total < 0
                    or not isinstance(items, list) or (not items and offset < total)
                ):
                    raise MigrationError("source_invalid_response", source_path=folder)
                for item in items:
                    if not isinstance(item, dict):
                        raise MigrationError("source_invalid_response",
                                             source_path=folder)
                    try:
                        entry = self._metadata(item)
                    except MigrationError as exc:
                        if not exc.source_path:
                            exc.source_path = folder
                        raise
                    prefix = folder.rstrip("/") + "/"
                    if not entry["path"].startswith(prefix):
                        raise MigrationError("source_invalid_response",
                                             source_path=entry["path"])
                    relative = entry["path"][len(prefix):]
                    if "/" in relative or not relative or entry["path"] in seen:
                        raise MigrationError("source_changed",
                                             source_path=entry["path"])
                    seen.add(entry["path"])
                    yield entry
                    if entry["type"] == "dir":
                        pending.append(entry["path"])
                offset += len(items)
                if offset >= total:
                    break

    def download_url(self, path):
        payload = self._api_get(self._resource_endpoint() + "/download", {
            "path": self._source_path(path),
        }, max_attempts=1)
        return https_url(payload.get("href"))

    def _publication_root(self, path):
        canonical = disk_path(path)
        selected = disk_path(self.config["source_path"])
        if selected != "disk:/":
            if canonical != selected and not canonical.startswith(selected + "/"):
                raise MigrationError("source_invalid_response")
            return selected
        if canonical == "disk:/":
            raise MigrationError("source_invalid_response")
        return "disk:/" + canonical[6:].split("/", 1)[0]

    def has_publication(self, path):
        return disk_path(path) in self._publications

    def _publication_metadata(self, path):
        payload = self._api_get("/resources", {
            "path": path, "fields": RESOURCE_FIELDS + ",public_key,public_url",
        })
        metadata = self._metadata(payload)
        if metadata["path"] != path:
            raise MigrationError("source_invalid_response")
        key = payload.get("public_key")
        if key is not None and (
            not isinstance(key, str) or len(key) > 8192
            or any(ord(char) < 32 or ord(char) == 127 for char in key)
        ):
            raise MigrationError("source_public_invalid_response")
        if payload.get("public_url") and not key:
            raise MigrationError("source_public_invalid_response")
        return metadata, key or None

    @staticmethod
    def _check_publication_node(node, metadata):
        if metadata["type"] != node["kind"]:
            raise MigrationError("source_changed")
        expected_id = node.get("resource_id")
        if expected_id is not None and metadata.get("resource_id") != expected_id:
            raise MigrationError("source_changed")
        if node["kind"] == "file":
            for field in ("size", "md5", "modified"):
                expected = node.get(field)
                if field == "md5" and isinstance(expected, str):
                    expected = expected.lower()
                if expected is not None and metadata.get(field) != expected:
                    raise MigrationError("source_changed")

    def ensure_publication(self, node, *, checkpoint=None):
        """Publish only an unchanged plan root; retain every existing share."""
        if self.shared or not self.config.get("source_public", False):
            return "disabled"
        path = disk_path(node["source_path"])
        if self._publication_root(path) != path:
            raise MigrationError("source_invalid_response")
        metadata, key = self._publication_metadata(path)
        self._check_publication_node(node, metadata)
        if key:
            self._publications[path] = key
            self._unknown_publications.discard(path)
            return "existing"
        if path in self._unknown_publications or node.get("state") == "intent":
            raise MigrationError("source_publication_outcome_unknown", uncertain=True)
        if node.get("state") == "published":
            raise MigrationError("source_publication_not_confirmed")
        token = self._owner_token()
        for refresh in range(2):
            try:
                response = self._request(
                    "PUT", API + "/resources/publish", params={"path": path},
                    headers={"Authorization": "OAuth " + token},
                    max_attempts=1, checkpoint=checkpoint,
                )
            except MigrationError as exc:
                self._unknown_publications.add(path)
                raise MigrationError("source_publication_outcome_unknown",
                                     uncertain=True, diagnostics=exc.diagnostics) from None
            if response.status_code != 401 or refresh:
                break
            response.close()
            self._invalidate_token(token)
            # 401 proves the mutation was rejected. A failed token exchange is
            # an authentication error, not an uncertain publication outcome.
            token = self._owner_token()
        try:
            status = response.status_code
            diagnostics = {
                "http_status": status,
                "retry_after": retry_after_seconds(response.headers.get("Retry-After")),
            }
            if status != 200:
                diagnostics["diagnostics"] = response_details(
                    response, "saas", url=API + "/resources/publish",
                )
            if status == 401:
                self._invalidate_token(token)
                raise MigrationError("source_auth_failed", auth=True, **diagnostics)
            if status in (403, 423):
                raise MigrationError("source_publication_denied", **diagnostics)
            if status >= 500:
                self._unknown_publications.add(path)
                raise MigrationError("source_publication_outcome_unknown",
                                     uncertain=True, **diagnostics)
            if status != 200:
                raise MigrationError("source_publication_failed", **diagnostics)
        finally:
            response.close()
        self._unknown_publications.add(path)
        try:
            metadata, key = self._publication_metadata(path)
            self._check_publication_node(node, metadata)
        except MigrationError as exc:
            raise MigrationError(
                "source_publication_outcome_unknown", uncertain=True,
                auth=exc.auth, http_status=exc.http_status, retry_after=exc.retry_after,
                diagnostics=exc.diagnostics,
            ) from None
        if not key:
            raise MigrationError("source_publication_outcome_unknown", uncertain=True)
        self._publications[path] = key
        self._unknown_publications.discard(path)
        return "published"

    @staticmethod
    def _public_payload(response, *, url=None, streaming=False):
        status = response.status_code
        diagnostics = {
            "http_status": status,
            "retry_after": retry_after_seconds(response.headers.get("Retry-After")),
        }
        payload = None
        if status == 200:
            try:
                payload = response.json()
            except ValueError:
                pass
        if status != 200 or not isinstance(payload, dict):
            diagnostics["diagnostics"] = response_details(
                response, "saas", url=url, streaming=streaming,
            )
        if status == 429 or (
            status in (403, 409)
            and diagnostics["diagnostics"].get("api_error") in (
                "DiskDownloadLimitExceededError", "DownloadLimitExceededError",
            )
        ):
            raise MigrationError("source_public_download_limit", **diagnostics)
        if status in (401, 403):
            raise MigrationError("source_public_access_denied", **diagnostics)
        if status == 404:
            raise MigrationError("source_public_resource_unavailable", **diagnostics)
        if status != 200:
            raise MigrationError("source_public_download_failed", **diagnostics)
        if not isinstance(payload, dict):
            raise MigrationError("source_public_invalid_response", **diagnostics)
        return payload

    def public_download_url(self, path):
        canonical = disk_path(path)
        root = self._publication_root(canonical)
        if root not in self._publications:
            _metadata, key = self._publication_metadata(root)
            if not key:
                raise MigrationError("source_publication_not_confirmed")
            self._publications[root] = key
        params = {"public_key": self._publications[root]}
        if canonical != root:
            params["path"] = canonical[len(root):]
        response = self._request(
            "GET", API + "/public/resources/download", params=params,
            max_attempts=1,
        )
        try:
            return https_url(self._public_payload(
                response, url=API + "/public/resources/download",
            ).get("href"))
        finally:
            response.close()

    def chunks(self, path, *, download_url=None):
        public = self.config.get("source_public", False) and not self.shared
        url = https_url(download_url) if download_url is not None else (
            self.public_download_url(path) if public else self.download_url(path)
        )
        headers = {"Authorization": "OAuth " + self._owner_token()} if self.shared else {}
        response = None
        refreshed = False
        try:
            for _hop in range(7):
                response = self._request("GET", url, stream=True, max_attempts=1,
                                         headers=headers)
                if response.status_code == 401 and headers and not refreshed:
                    refreshed = True
                    response.close()
                    response = None
                    self._invalidate_token(headers["Authorization"][6:])
                    url = self.download_url(path)
                    headers = {"Authorization": "OAuth " + self._owner_token()}
                    continue
                if response.status_code not in (301, 302, 303, 307, 308):
                    break
                next_url = response.headers.get("Location")
                response.close()
                response = None
                if not isinstance(next_url, str) or any(
                    ord(c) <= 32 for c in next_url
                ):
                    raise MigrationError("invalid_download_url")
                redirected = https_url(urljoin(url, next_url))
                if urlsplit(redirected).netloc.lower() != urlsplit(url).netloc.lower():
                    headers = {}
                url = redirected
            if response is None or response.status_code != 200:
                if public and response is not None:
                    self._public_payload(response, url=url, streaming=True)
                raise MigrationError("source_download_failed", http_status=(
                    response.status_code if response is not None else None
                ), retry_after=(retry_after_seconds(response.headers.get("Retry-After"))
                                if response is not None else None), diagnostics=(
                    response_details(response, "saas", url=url, streaming=True)
                    if response is not None else {"error_side": "saas"}
                ))
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    yield chunk
        except requests.RequestException as exc:
            code = ("source_download_timeout" if isinstance(exc, requests.Timeout)
                    else "source_stream_interrupted")
            raise MigrationError(code, http_status=(
                response.status_code if response is not None else None
            ), retry_after=(retry_after_seconds(response.headers.get("Retry-After"))
                            if response is not None else None),
                diagnostics=transport_details(exc, "saas", url=url),
            ) from None
        finally:
            if response is not None:
                response.close()
