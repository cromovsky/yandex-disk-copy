"""Копирование файлов с одного Яндекс.Диска на другой.

Рефакторинг исходного скрипта: вместо глобальных переменных и записи логов в
файл — класс DiskCopier, который параметризуется конфигом и шлёт строки лога
через callback (его web-слой стримит в браузер по SSE).

Используются:
- сервисные приложения Яндекс 360 (token-exchange по email сотрудника)
- API Диска (cloud-api.yandex.net) — личные и общие диски (virtual-disks)
- API360 для блокировки/разблокировки пользователя
"""

from __future__ import annotations

import math
import errno
import os
import random
import re
import tempfile
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from types import SimpleNamespace
from pathlib import Path
from datetime import datetime, timedelta
from typing import Callable, Optional
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter, Retry
from urllib3.exceptions import MaxRetryError

from .retry_policy import RetryPolicy
from .file_cooldown import FileCooldown

from .throttle import (
    RETRY_STATUSES, ThrottledSession, backoff_delay, get_limiter,
    retry_after_seconds,
)
from .migration.diagnostics import response_details, transport_details


def _parse_bool(raw: str | None) -> Optional[bool]:
    v = (raw or "").strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return None


# Сверка места (GET /v1/disk) требует scope cloud_api:disk.info. По умолчанию
# пропускаем: копирование идёт через disk.read/disk.write, и 403 на info
# не должен останавливать перенос. Включить сверку: SPACE_CHECK=1.
_space = _parse_bool(os.environ.get("SPACE_CHECK"))
_skip = _parse_bool(os.environ.get("SKIP_SPACE_CHECK"))
if _space is not None:
    SKIP_SPACE_CHECK = not _space
elif _skip is not None:
    SKIP_SPACE_CHECK = _skip
else:
    SKIP_SPACE_CHECK = True


class _ObservedRetry(Retry):
    observer = None
    limiter = None
    retry_policy = None

    def new(self, **kwargs):
        clone = super().new(**kwargs)
        clone.observer = self.observer
        clone.limiter = self.limiter
        clone.retry_policy = self.retry_policy
        return clone

    def _disabled(self, error=None, pool=None, url=None, attempt=None):
        last = self.history[-1] if self.history else None
        actual_error = error or (last.error if last else None)
        details = transport_details(actual_error, "saas") if actual_error else {}
        if callable(self.observer):
            try:
                self.observer({"event_type": "retry", "status": "skipped",
                               "error": "transport_error", "retry_enabled": False,
                               **details,
                               "attempt": attempt or max(1, len(self.history)),
                               "message": "Автоматические повторы отключены; ошибка возвращена после текущей попытки."})
            except Exception:
                pass
        raise MaxRetryError(pool, url or (last.url if last else None), actual_error)

    def increment(self, method=None, url=None, response=None, error=None,
                  _pool=None, _stacktrace=None):
        if self.retry_policy is not None and not self.retry_policy.enabled:
            self._disabled(error, _pool, url, len(self.history) + 1)
        return super().increment(method, url, response, error, _pool, _stacktrace)

    def sleep(self, response=None):
        delay = (self.get_retry_after(response)
                 if self.respect_retry_after_header and response else None)
        delay = delay or self.get_backoff_time()
        if self.retry_policy is not None and not self.retry_policy.enabled:
            self._disabled()
        generation = (self.retry_policy.cancellation_generation
                      if self.retry_policy is not None else None)
        if callable(self.observer):
            try:
                payload = {
                    "event_type": "retry", "status": "waiting",
                    "error": "transport_error", "attempt": len(self.history),
                    "retry_delay_ms": int(delay * 1000),
                    "message": "Внутренний повтор HTTP-запроса.",
                }
                if self.history and self.history[-1].error is not None:
                    category = transport_details(self.history[-1].error, "saas").get(
                        "transport_error",
                    )
                    if category and category != "transport":
                        payload["transport_error"] = category
                self.observer(payload)
            except Exception:
                pass
        if self.retry_policy is None:
            super().sleep(response)
        elif not self.retry_policy.wait(delay, generation=generation):
            self._disabled()
        if self.limiter is not None:
            method = self.history[-1].method if self.history else "GET"
            if self.retry_policy is None:
                self.limiter.acquire(method)
            else:
                def checkpoint():
                    if not self.retry_policy.wait(0, generation=generation):
                        self._disabled()
                self.limiter.acquire(method, checkpoint=checkpoint)
                checkpoint()


def _build_session(retry_policy=None) -> requests.Session:
    """Сессия с общим троттлингом, ретраями по статусам и таймаутами.

    Статусы (429/5xx) обрабатывает наш ThrottledSession, а не urllib3: ретраи
    urllib3 живут внутри adapter.send(), мимо гейта — они не занимают слот в
    бюджете и не сообщают другим потокам, что API прислал 429. За urllib3
    остаются только ошибки соединения; каждый их повтор тоже занимает слот.
    """
    transport_retries = _ObservedRetry(
        total=None,
        connect=3,
        read=2,
        status=0,
        other=0,
        status_forcelist=[],
        # ретрай транспорта нужен и для POST: соединение не установилось —
        # серверной работы почти наверняка не было
        allowed_methods=None,
        backoff_factor=0.5,
    )
    limiter = get_limiter()
    transport_retries.limiter = limiter
    transport_retries.retry_policy = retry_policy
    session = ThrottledSession(limiter, retry_policy=retry_policy)
    session.mount("https://", HTTPAdapter(max_retries=transport_retries))
    return session


# Сколько непереносённых файлов перечислять в логе поимённо: список нужен
# для разбора, но при тысячах ошибок он вытеснит остальной лог из окна.
FAILS_LOG_LIMIT = 50
LOCAL_DOWNLOAD_RETRIES = 5
# Прямое копирование тащит файл через процесс. Выше этого порога отказываемся,
# чтобы один большой файл не съел память контейнера.
DIRECT_COPY_MAX_BYTES = 512 * 1024 * 1024

# Опрос статуса асинхронной операции: интервал растёт 0.5→1→2→4→8→10→10…
POLL_FIRST_DELAY = 0.5
POLL_MAX_DELAY = 10.0


def _strip_disk_schema(path: str) -> str:
    """disk:/foo/bar → /foo/bar; /foo → /foo."""
    if path.startswith("disk:"):
        path = path[5:]
    if not path.startswith("/"):
        path = "/" + path
    return path.rstrip("/") or "/"


@dataclass
class OrgCreds:
    """Реквизиты доступа одной организации.

    client_id/client_secret — сервисное приложение орги (нужно для token-exchange).
    orgid/admin_token — только для API360 (проверка/смена блокировки); у стороны,
    для которой блокировка не управляется (получатель в cross-org), пустые.
    """

    client_id: str
    client_secret: str
    orgid: str = ""
    admin_token: str = ""


@dataclass
class CopyConfig:
    source: OrgCreds
    destination: OrgCreds
    source_disk_id: str
    destination_disk_id: str
    path: str = "/"
    page_limit: int = 1000
    # personal — личный Диск сотрудника (email в destination_disk_id);
    # shared — общий диск организации (нужен destination_vd_hash; destination_disk_id
    #          — email сотрудника с правом записи на этот общий диск).
    destination_type: str = "personal"
    destination_vd_hash: str = ""
    # Локальное сохранение на машину, с которой запущена утилита.
    # local_root задаёт каталог внутри процесса; на хост он попадает томом.
    local_save: bool = False
    local_root: str = ""
    # Для локального сохранения общего диска source_disk_id — email читателя.
    source_type: str = "personal"
    source_vd_hash: str = ""
    retry_enabled: bool = True
    retry_policy: Optional[RetryPolicy] = field(default=None, repr=False, compare=False)


@dataclass
class _DiskAuth:
    token: str = ""
    expiry: datetime = field(default_factory=datetime.now)


class CopyError(Exception):
    """Прерывание процесса с человекочитаемой причиной."""

    def __init__(self, message, *, diagnostics=None, http_status=None,
                 uncertain=False, code="copy_failed"):
        super().__init__(message)
        self.diagnostics = diagnostics or {}
        self.http_status = http_status
        self.uncertain = uncertain
        self.code = code


def _access_failure(exc):
    return (getattr(exc, "code", None) == "token_failed"
            or getattr(exc, "journal_stage", None) == "get_token")


def validate_source_email(value: str, *, shared: bool = False) -> str:
    email = (value or "").strip()
    if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email):
        if shared:
            raise CopyError(
                "В SOURCE укажите email сотрудника с доступом к общему диску. "
                "Метку диска укажите в SOURCE_VD_HASH."
            )
        raise CopyError("В SOURCE укажите email владельца личного Диска.")
    return email


LogCallback = Callable[[str], None]


class DiskCopier:
    def __init__(
        self,
        config: CopyConfig,
        log: Optional[LogCallback] = None,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        retry_policy=None,
    ):
        self.cfg = config
        self._log = log or (lambda _msg: None)
        self._secrets = set()
        self._context = {}
        self._request_context = {}
        self._file_outcome = "succeeded"
        for creds in (config.source, config.destination):
            self._secret(creds.client_secret)
            self._secret(creds.admin_token)
        # часы инъектируются, чтобы тесты не спали по-настоящему
        self._monotonic = monotonic
        self._sleep = sleep
        self._file_cooldown = FileCooldown(monotonic=monotonic)
        self.retry_policy = retry_policy or config.retry_policy or RetryPolicy(
            config.retry_enabled, monotonic=monotonic, sleep=sleep,
        )
        self.session = _build_session()
        self.session.set_retry_policy(self.retry_policy)
        # 429 и ретраи должны быть видны в SSE-логе браузера, а не выглядеть
        # для пользователя как «просто стало медленно»
        self.session.set_log(self.log)
        self.session.set_observer(self._http_event)
        self._download_session: Optional[requests.Session] = None
        self.source = _DiskAuth()
        self.destination = _DiskAuth()
        self._source_token_lock = threading.RLock()
        self._destination_token_lock = threading.RLock()
        self.links: list[dict] = []
        self.fails: list[dict] = []
        self.skipped: list[dict] = []
        self.unconfirmed: list[dict] = []
        self._failed_publications: set[str] = set()
        # уже созданные папки общего диска: сосед в той же папке запросов не тратит
        self._vd_created: set[str] = set()

    # ── жизненный цикл ─────────────────────────────────────────────────
    def close(self) -> None:
        """Отпускает пул соединений (в батче живёт до BATCH_CONCURRENCY копиров)."""
        self.session.close()
        if self._download_session is not None:
            self._download_session.close()

    def __enter__(self) -> "DiskCopier":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ── логирование ────────────────────────────────────────────────────
    def log(self, message: str) -> None:
        self._log(f"{datetime.now():%Y-%m-%d %H:%M:%S} | {self._safe_text(message)}")

    def _detail(self, message):
        if not callable(getattr(self._log, "event", None)):
            self.log(message)

    def _secret(self, value, *, link=False):
        if isinstance(value, str) and value:
            # Full HTTP links are always masked as URLs. Keeping one exact
            # secret per file would make every journal event scan the disk's
            # entire link history; opaque public keys still need exact masking.
            if link and re.fullmatch(r"https?://\S+", value, flags=re.I):
                return
            if value in self._secrets:
                return
            self._secrets.add(value)
            register = getattr(self._log, "add_secret", None)
            if callable(register):
                register(value)

    def _safe_text(self, value):
        text = str(value)
        for secret in sorted(self._secrets, key=len, reverse=True):
            text = text.replace(secret, "[скрыто]")
        text = re.sub(r"https?://[^\s]+", "[ссылка скрыта]", text, flags=re.I)
        return re.sub(r"\b(?:OAuth|Bearer)\s+\S+", "[доступ скрыт]", text, flags=re.I)

    def _event(self, payload):
        data = {**self._context, **payload}
        for key in ("message", "source", "destination"):
            if key in data and data[key] is not None:
                if isinstance(data[key], str):
                    data[key] = self._safe_text(data[key])
                else:
                    data.pop(key)
        if data.get("kind") not in (None, "file", "dir"):
            data.pop("kind")
        size = data.get("size")
        if size is not None and (
            not isinstance(size, int) or isinstance(size, bool) or size < 0
        ):
            data.pop("size")
        for key in ("api_error", "api_reason", "request_id", "request_host", "operation_id"):
            value = data.get(key)
            if isinstance(value, str) and self._safe_text(value) != value:
                data.pop(key)
        emit = getattr(self._log, "event", None)
        if callable(emit):
            emit(data)
        else:
            self.log(" | ".join(str(data[key]) for key in (
                "stage", "status", "source", "destination", "message", "http_status",
            ) if data.get(key) is not None))

    def _response_details(self, response, side, url=None, *, stream=False):
        url = url or getattr(response, "_journal_url", None)
        if stream:
            response = SimpleNamespace(content=b"", headers=response.headers,
                                       url=url or getattr(response, "url", None))
        details = response_details(response, "saas", url=url)
        details["error_side"] = side
        return details

    def _error_details(self, exc, side, url=None):
        details = getattr(exc, "diagnostics", None) or getattr(exc, "journal_diagnostics", {})
        if not details and isinstance(exc, requests.RequestException):
            details = transport_details(exc, "saas", url=url)
        details = dict(details)
        details["error_side"] = details.get("error_side") if details.get("error_side") in {
            "source", "destination", "local",
        } else side
        return details

    def _http_event(self, payload):
        self._event({**payload, **self._request_context})

    def _request(self, method, url, *, stage, side, session=None,
                 source=None, destination=None, kind=None, operation_id=None,
                 attempt=None, max_attempts=None, **kwargs):
        context = self._request_context
        self._request_context = {"stage": stage, "error_side": side}
        self._request_context.update({key: value for key, value in {
            "source": source, "destination": destination, "kind": kind,
            "operation_id": operation_id, "attempt": attempt, "max_attempts": max_attempts,
        }.items() if value is not None})
        client = session or self.session
        observer = getattr(client, "set_observer", None)
        if callable(observer):
            observer(self._http_event)
        if isinstance(client, requests.Session):
            try:
                client.get_adapter(url).max_retries.observer = self._http_event
            except requests.exceptions.InvalidSchema:
                pass
        started = self._monotonic()
        self._event({**self._request_context, "event_type": "stage", "status": "started"})
        try:
            response = getattr(client, method.lower())(url, **kwargs)
            headers = kwargs.get("headers") or {}
            rejected = headers.get("Authorization", "")
            if (response.status_code == 401 and stage != "get_token"
                    and urlsplit(url).hostname == "cloud-api.yandex.net"
                    and rejected.startswith("OAuth ")
                    and side in {"source", "destination"}):
                # A rejected owner token is refreshed once; public links/admin
                # tokens are different credentials and must not use token-exchange.
                lock = (self._source_token_lock if side == "source"
                        else self._destination_token_lock)
                response.close()
                with lock:
                    auth = self.source if side == "source" else self.destination
                    if auth.token == rejected[6:]:
                        auth.token = ""
                    token = (self._source_token() if side == "source"
                             else self._destination_token())
                self._event({**self._request_context, "event_type": "retry",
                             "status": "waiting", "http_status": 401,
                             "error": "token_expired", "attempt": 2,
                             "max_attempts": 2, "retry_delay_ms": 0,
                             "message": "Токен отклонён сервером; доступ обновлён."})
                kwargs = dict(kwargs, headers={**headers, "Authorization": "OAuth " + token})
                response = getattr(client, method.lower())(url, **kwargs)
        except Exception as exc:
            details = self._error_details(exc, side, url)
            uncertain = method not in {"GET", "HEAD"} and stage != "get_token"
            if isinstance(exc, requests.RequestException):
                exc.journal_diagnostics = details
                if not getattr(exc, "journal_stage", None):
                    exc.journal_stage = stage
                exc.uncertain = getattr(exc, "uncertain", uncertain)
            self._event({**self._request_context, **details, "event_type": "stage",
                         "status": "outcome_unknown" if uncertain else "failed",
                         "error": "transport_failed" if isinstance(exc, requests.RequestException)
                         else "request_failed",
                         "duration_ms": int((self._monotonic() - started) * 1000)})
            raise
        else:
            response._journal_url = url
            response._journal_context = dict(self._request_context)
            status = response.status_code
            state = ("waiting" if status == 202 else "skipped" if status == 409
                     else "succeeded" if status < 400 else "outcome_unknown"
                     if status >= 500 and method not in {"GET", "HEAD"} else "failed")
            if kwargs.get("stream") and status == 200:
                state = "waiting"
            details = self._response_details(
                response, side, url, stream=kwargs.get("stream", False),
            )
            payload = {**details, "event_type": "http", "status": state,
                       "http_status": status,
                       "duration_ms": int((self._monotonic() - started) * 1000)}
            if not callable(observer):
                self._http_event(payload)
            if state != "succeeded":
                self._event({**self._request_context, **payload, "event_type": "stage"})
            return response
        finally:
            self._request_context = context

    def _failure(self, response, message, *, side, mutation=False, stream=False,
                 code="copy_failed", uncertain=False):
        error = CopyError(f"{message}: HTTP {response.status_code}",
                          diagnostics=self._response_details(response, side, stream=stream),
                          http_status=response.status_code,
                          uncertain=uncertain or (mutation and response.status_code >= 500),
                          code=code)
        error.journal_stage = getattr(response, "_journal_context", {}).get("stage")
        return error

    def _file_failure(self, record, exc, *, stage="file", side="destination",
                      destination=None):
        """Record one failed resource; disabling retries never hides its cause."""
        error = self._safe_text(exc)
        self.fails.append({**record, "error": error})
        if self.retry_policy.enabled:
            return
        self._file_cooldown.mark(self.cfg.source_disk_id, after_error=True)
        details = self._error_details(exc, side)
        code = getattr(exc, "code", None) or (
            "transport_failed" if isinstance(exc, requests.RequestException) else "transfer_failed"
        )
        self._event({"event_type": "stage", "stage": getattr(exc, "journal_stage", None) or stage,
                     "status": "outcome_unknown" if getattr(exc, "uncertain", False) else "skipped",
                     "source": record.get("path"), "destination": destination,
                     "kind": record.get("type", "file"), **details,
                     "error": code, "http_status": getattr(exc, "http_status", None),
                     "retry_enabled": False, "attempt": 1, "max_attempts": 1,
                     "message": "Автоматические повторы отключены; ресурс пропущен после ошибки: "
                     + error})

    def _wait_file_pause(self):
        delay = self._file_cooldown.remaining(self.cfg.source_disk_id)
        if delay > 0:
            self._event({"event_type": "stage", "stage": "file_pause", "status": "waiting",
                         "retry_delay_ms": int(delay * 1000),
                         "message": "Пауза между передачами: 20 с. Очередь продолжится автоматически."})
            self._sleep(delay)

    def _record_result(self, record):
        if self._file_outcome == "skipped":
            self.skipped.append(record)
        elif self._file_outcome in {"waiting", "outcome_unknown"}:
            self.unconfirmed.append(record)
        else:
            self.links.append(record)
            self._file_cooldown.mark(self.cfg.source_disk_id)

    @contextmanager
    def _stage(self, stage, side, **context):
        previous, outcome = self._context, self._file_outcome
        self._context = {**previous, **context}
        if stage == "file":
            self._file_outcome = "succeeded"
        started = self._monotonic()
        self._event({"event_type": "stage", "stage": stage,
                     "status": "started", "error_side": side})
        try:
            yield
        except Exception as exc:
            if not getattr(exc, "journal_stage", None):
                exc.journal_stage = stage
            local_error = isinstance(exc, OSError) and not isinstance(
                exc, requests.RequestException,
            )
            code = ("transport_failed" if isinstance(exc, requests.RequestException)
                    else "transfer_failed")
            message = f"{type(exc).__name__}: подробности указаны в диагностике этапа."
            if local_error:
                code, message = {
                    errno.ENOSPC: ("local_no_space", "На локальном диске недостаточно места."),
                    errno.EDQUOT: ("local_quota_exceeded", "Превышена квота локального диска."),
                    errno.EACCES: ("local_permission_denied",
                                   "Нет доступа к локальному файлу или папке."),
                    errno.EPERM: ("local_permission_denied",
                                  "Локальная файловая операция запрещена."),
                }.get(exc.errno, ("local_io_failed", "Ошибка локальной файловой операции."))
                exc.journal_diagnostics = {"error_side": "local"}
                exc.code = code
            self._event({"event_type": "stage", "stage": stage,
                         "status": "outcome_unknown" if getattr(exc, "uncertain", False)
                         else "failed",
                         "error": getattr(exc, "code", code),
                         **self._error_details(exc, "local" if local_error else side),
                         "message": self._safe_text(exc) if isinstance(exc, CopyError)
                         else message,
                         "http_status": getattr(exc, "http_status", None),
                         "duration_ms": int((self._monotonic() - started) * 1000)})
            raise
        else:
            self._event({"event_type": "stage", "stage": stage,
                         "status": self._file_outcome if stage == "file" else "succeeded",
                         "error_side": side,
                         "duration_ms": int((self._monotonic() - started) * 1000)})
        finally:
            self._context, self._file_outcome = previous, outcome

    @property
    def _is_shared_dest(self) -> bool:
        return self.cfg.destination_type == "shared"

    @property
    def _is_shared_source(self) -> bool:
        return self.cfg.source_type == "shared"

    def _source_inner_path(self, path: str) -> str:
        """Путь внутри выбранного источника, без метки общего диска."""
        if self._is_shared_source and path.startswith("vd:"):
            # Запросы используют vd:<hash>:disk:/..., ответы API также
            # возвращают vd:/<hash>/disk/... (включая вложенные каталоги).
            prefixes = (
                f"vd:{self.cfg.source_vd_hash}:disk:",
                f"vd:/{self.cfg.source_vd_hash}/disk",
            )
            for prefix in prefixes:
                if path == prefix or path.startswith(prefix + "/"):
                    path = path[len(prefix):]
                    break
            else:
                raise CopyError(f"Путь не принадлежит выбранному общему диску: {path}")
        return _strip_disk_schema(path)

    def _source_path(self, path: str) -> str:
        if self._is_shared_source:
            return f"vd:{self.cfg.source_vd_hash}:disk:{self._source_inner_path(path)}"
        return path

    # ── токены сервисного приложения ───────────────────────────────────
    def _get_token(self, creds: OrgCreds, disk_id: str) -> _DiskAuth:
        url = "https://oauth.yandex.ru/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "client_id": creds.client_id,
            "client_secret": creds.client_secret,
            "subject_token": disk_id,
            "subject_token_type": "urn:yandex:params:oauth:token-type:email",
        }
        side = "source" if disk_id == self.cfg.source_disk_id else "destination"
        response = self._request("POST", url, stage="get_token", side=side,
                                 data=data, headers=headers)
        payload = self._response_payload(response)
        # scope выданного токена может быть уже, чем scopes приложения в OAuth:
        # при регистрации сервисного приложения набор фиксируется отдельно
        scope = payload.get("scope", "")
        scope = " ".join(value for value in str(scope).split()
                         if re.fullmatch(r"cloud_api:disk\.(?:read|write|info)", value))
        self._detail(
            f"get_token | user: {disk_id}, status: {response.status_code}"
            + (f", scope: {scope}" if scope else "")
        )
        if "access_token" not in payload:
            desc = str(payload.get("error_description", "")).lower()
            hint = ""
            if response.status_code == 400 and "unauthorized" in desc:
                hint = (
                    " | Вероятная причина: OAuth-приложение не зарегистрировано как "
                    "сервисное для организации (создать приложение с правами на Диск "
                    "недостаточно). Зарегистрируйте его токеном владельца: "
                    "POST https://api360.yandex.net/security/v1/org/<ORGID>/"
                    "service_applications. Проверьте также CLIENT_ID/CLIENT_SECRET "
                    "и что сотрудник состоит в этой организации."
                )
            error = self._failure(response, f"Не удалось получить токен для {disk_id}",
                                  side=side, code="token_failed")
            error.args = (str(error) + hint,)
            raise error
        self._secret(payload["access_token"])
        ttl = max(1, int(payload["expires_in"]) - 100)
        self._event({"event_type": "stage", "stage": "get_token", "error_side": side,
                     "status": "succeeded", "message": f"Получение токена для {disk_id}."
                     + (f" Права: {scope}." if scope else "")})
        return _DiskAuth(
            token=payload["access_token"],
            expiry=datetime.now() + timedelta(seconds=ttl),
        )

    def _source_token(self) -> str:
        """Токен исходного диска, с авто-обновлением по истечении."""
        with self._source_token_lock:
            if datetime.now() >= self.source.expiry or not self.source.token:
                self.log("source token expired/empty — refreshing")
                self.source = self._get_token(self.cfg.source, self.cfg.source_disk_id)
            self._secret(self.source.token)
            return self.source.token

    def _destination_token(self) -> str:
        with self._destination_token_lock:
            if datetime.now() >= self.destination.expiry or not self.destination.token:
                self.log("destination token expired/empty — refreshing")
                self.destination = self._get_token(
                    self.cfg.destination, self.cfg.destination_disk_id
                )
            self._secret(self.destination.token)
            return self.destination.token

    # ── API360: блокировка/разблокировка ───────────────────────────────
    def _api360_get_ban_status(self, user_id: str) -> Optional[bool]:
        url = (
            f"https://api360.yandex.net/directory/v1/org/"
            f"{self.cfg.source.orgid}/users/{user_id}"
        )
        headers = {"Authorization": f"OAuth {self.cfg.source.admin_token}"}
        response = self._request("GET", url, stage="user_status", side="source",
                                 headers=headers)
        if response.status_code != 200:
            raise self._failure(response, f"Не удалось получить статус сотрудника {user_id} "
                                "в API360; проверьте ADMIN_TOKEN и ORGID", side="source")
        with self._response_parse(response):
            is_active = response.json().get("isEnabled")
        self._detail(f"api360 ban status | {user_id} isEnabled: {is_active}")
        return is_active

    def _api360_patch_user(self, user_id: str, status: bool) -> None:
        url = (
            f"https://api360.yandex.net/directory/v1/org/"
            f"{self.cfg.source.orgid}/users/{user_id}"
        )
        headers = {"Authorization": f"OAuth {self.cfg.source.admin_token}"}
        response = self._request("PATCH", url, stage="set_user_status", side="source",
                                 json={"isEnabled": status}, headers=headers)
        self._detail(
            f"api360 patch user | {user_id} isEnabled -> {status}, "
            f"status: {response.status_code}"
        )
        if response.status_code != 200:
            raise self._failure(response, f"Не удалось сменить статус сотрудника {user_id} "
                                f"(isEnabled -> {status})", side="source", mutation=True)

    # ── пути назначения ────────────────────────────────────────────────
    def _target_folder_personal(self) -> str:
        """Папка на личном Диске получателя (по email источника)."""
        return f"disk:/{self.cfg.source_disk_id}"

    def _vd_path(self, inner: str = "/") -> str:
        """Составной путь ресурса на общем диске: vd:<hash>:disk:/..."""
        inner_n = _strip_disk_schema(inner)
        if inner_n == "/":
            return f"vd:{self.cfg.destination_vd_hash}:disk:/"
        return f"vd:{self.cfg.destination_vd_hash}:disk:{inner_n}"

    def _target_folder_shared(self) -> str:
        """Корневая папка переноса на общем диске (по email источника)."""
        return self._vd_path(f"/{self.cfg.source_disk_id}")

    def _dest_inner(self, item: dict) -> str:
        """Путь файла внутри общего диска (без vd:-префикса).

        Вынесен из save_to_shared, чтобы предвычисление родительских папок и цикл
        по файлам не могли разойтись в логике.
        """
        root_inner = f"/{self.cfg.source_disk_id}"
        src_path = item["path"]
        name = item.get("name") or src_path.rsplit("/", 1)[-1]
        rel = self._relative_from_source(src_path)
        return f"{root_inner}/{rel}" if rel else f"{root_inner}/{name}"

    def _relative_from_source(self, item_path: str) -> str:
        """Относительный путь файла внутри cfg.path (без ведущего /)."""
        base = _strip_disk_schema(self.cfg.path)
        full = _strip_disk_schema(item_path)
        if base != "/" and (full == base or full.startswith(base + "/")):
            rel = full[len(base) :].lstrip("/")
        else:
            rel = full.lstrip("/")
        return rel

    def _access_hint(self, status_code: int, scope: str) -> str:
        """Подсказка для 401/403: у токена сотрудника нет доступа к Диску."""
        if status_code not in (401, 403):
            return ""
        return (
            f" | Токену сотрудника отказано в доступе (нужен scope {scope}). "
            "Если 403 приходит на все операции Диска — сервисное приложение "
            "фактически не получило доступ к Дискам: сверьте scope в строке "
            "get_token, перерегистрируйте приложение "
            "(POST /security/v1/org/<ORGID>/service_applications) и проверьте, "
            "что тариф организации допускает работу с API Диска."
        )

    def _invalid_response(self, response, exc, code):
        context = getattr(response, "_journal_context", {})
        side = context.get("error_side", "source")
        details = self._response_details(response, side)
        exc.journal_diagnostics = details
        exc.journal_stage = context.get("stage")
        exc.http_status = response.status_code
        exc.code = code
        self._event({**context, **details, "event_type": "stage", "status": "failed",
                     "error": code, "http_status": response.status_code,
                     "message": "Сервер вернул некорректный JSON-ответ."})

    @contextmanager
    def _response_parse(self, response, *, code="invalid_response"):
        try:
            yield
        except (ValueError, AttributeError, TypeError, RecursionError) as exc:
            self._invalid_response(response, exc, code)
            if not self.retry_policy.enabled:
                raise self._failure(response, "Сервер вернул некорректный JSON-ответ",
                                    side=exc.journal_diagnostics.get("error_side", "source"),
                                    code=code, uncertain=response.status_code == 202) from None
            raise

    def _response_payload(self, response) -> dict:
        """JSON-тело ответа или {}: на ошибках API не должно быть KeyError."""
        try:
            with self._response_parse(response):
                payload = response.json() if response.content else {}
        except (ValueError, RecursionError):
            payload = {}
        if isinstance(payload, dict):
            for key in ("access_token", "href", "public_key", "public_url"):
                self._secret(payload.get(key), link=key != "access_token")
        else:
            self._invalid_response(response, TypeError(), "invalid_response")
        return payload if isinstance(payload, dict) else {"raw": payload}

    # ── API Диска (личный) ─────────────────────────────────────────────
    def _disk_space_info(self, token: str, *, disk_id: str = "") -> tuple[int, int]:
        """Возвращает (used_space, free_space) в байтах для личного Диска."""
        url = "https://cloud-api.yandex.net/v1/disk/"
        headers = {"Authorization": f"OAuth {token}"}
        side = "source" if token == self.source.token else "destination"
        response = self._request("GET", url, stage="disk_space", side=side,
                                 headers=headers)
        payload = self._response_payload(response)
        who = disk_id or "личный Диск"
        self._detail(f"disk_space_info | {who} | status: {response.status_code}")
        if (
            response.status_code != 200
            or "used_space" not in payload
            or "total_space" not in payload
        ):
            hint = ""
            if response.status_code == 403:
                hint = (
                    " | Токену сотрудника не хватает scope cloud_api:disk.info. "
                    "Сверьте scope в строке get_token выше: набор фиксируется при "
                    "регистрации сервисного приложения, добавления прав в "
                    "oauth.yandex.ru недостаточно — перерегистрируйте приложение "
                    "(POST /security/v1/org/<ORGID>/service_applications). "
                    "По умолчанию сверка места пропускается; включить: SPACE_CHECK=1"
                )
            error = self._failure(response, f"Не удалось получить информацию о Диске {who}",
                                  side=side)
            error.args = (str(error) + hint,)
            raise error
        used = int(payload["used_space"])
        total = int(payload["total_space"])
        return used, total - used

    def _shared_space_info(self, token: str) -> tuple[int, int]:
        """Возвращает (used_space, free_space) для общего диска по vd_hash."""
        url = "https://cloud-api.yandex.net/v1/disk/virtual-disks"
        headers = {"Authorization": f"OAuth {token}"}
        params = {"vd_hash": self.cfg.destination_vd_hash}
        response = self._request("GET", url, stage="disk_space", side="destination",
                                 params=params, headers=headers)
        self._detail(
            f"shared_space_info | status: {response.status_code} | "
            f"vd_hash: {self.cfg.destination_vd_hash}"
        )
        payload = self._response_payload(response)
        if response.status_code != 200 or "used_space" not in payload:
            raise self._failure(response, "Не удалось получить информацию об общем диске "
                                f"{self.cfg.destination_vd_hash}", side="destination")
        used = int(payload["used_space"])
        total = int(payload["total_space"])
        name = payload.get("name", "")
        perms = payload.get("permissions") or []
        self._detail(
            f"shared disk | name: {name}, permissions: {perms}, "
            f"used: {used}, total: {total}"
        )
        if "write" not in perms:
            raise CopyError(
                f"У сотрудника {self.cfg.destination_disk_id} нет права записи "
                f"на общий диск «{name or self.cfg.destination_vd_hash}» "
                f"(permissions={perms}). Выдайте доступ «Редактирование»."
            )
        return used, total - used

    def _disk_get_meta(self, token: str, limit: int, offset: int, path: str) -> dict:
        resource = "virtual-disks/resources" if self._is_shared_source else "resources"
        url = f"https://cloud-api.yandex.net/v1/disk/{resource}"
        headers = {"Authorization": f"OAuth {token}"}
        params = {
            "path": self._source_path(path),
            "fields": (
                "path,type,_embedded.total,_embedded.items.path,"
                "_embedded.items.type,_embedded.items.public_key,"
                "_embedded.items.name,_embedded.items.size"
            ),
            "limit": limit,
            "offset": offset,
            "sort": "path",
        }
        response = self._request("GET", url, stage="list_folder", side="source",
                                 source=path, kind="dir", params=params, headers=headers)
        self._detail(
            f"disk_get_meta | status: {response.status_code} | "
            f"path: {path}, limit: {limit}, offset: {offset}"
        )
        # Для личного Диска 404 обработает _walk; общий источник обязан
        # сообщить об отсутствующей папке, а не завершиться пустым успехом.
        # Остальные не-200 (401/403/5xx) молча превращать в «папка пуста» нельзя:
        # иначе перенос отрапортует COMPLETED, не скопировав ничего.
        allowed = (200,) if self._is_shared_source else (200, 404)
        if response.status_code not in allowed:
            error = self._failure(response, f"Не удалось прочитать {path} на Диске "
                                  f"{self.cfg.source_disk_id}",
                                  side="source")
            error.args = (str(error) + self._access_hint(
                response.status_code, "cloud_api:disk.read",
            ),)
            raise error
        return self._response_payload(response)

    def _disk_resource(self, token: str, path: str) -> dict:
        """Метаинформация одного ресурса (файл или папка)."""
        url = "https://cloud-api.yandex.net/v1/disk/resources"
        headers = {"Authorization": f"OAuth {token}"}
        params = {
            "path": path,
            "fields": "path,type,name,public_key,public_url",
        }
        response = self._request("GET", url, stage="metadata", side="source",
                                 source=path, params=params, headers=headers)
        if not self.retry_policy.enabled and response.status_code >= 400:
            raise self._failure(response, f"Не удалось прочитать свойства {path}", side="source")
        return self._response_payload(response)

    def _disk_publish_resource(self, token: str, path: str) -> None:
        url = "https://cloud-api.yandex.net/v1/disk/resources/publish"
        headers = {"Authorization": f"OAuth {token}"}
        response = self._request("PUT", url, stage="publish", side="source", source=path,
                                 params={"path": path}, headers=headers)
        self._detail(
            f"disk_publish_resource | status: {response.status_code} | path: {path}"
        )
        # With retries enabled retain the legacy access-failure behavior.
        # A disabled policy makes every failed publication final for this resource.
        if response.status_code in (401, 403) or (
            not self.retry_policy.enabled and response.status_code >= 400
        ):
            error = self._failure(response, f"Не удалось опубликовать {path}",
                                  side="source", mutation=True)
            error.args = (str(error) + self._access_hint(
                response.status_code, "cloud_api:disk.write",
            ),)
            raise error

    def _disk_ensure_folder(self, token: str, path: str) -> None:
        """Создать папку на личном Диске (идемпотентно: 409 = уже существует)."""
        url = "https://cloud-api.yandex.net/v1/disk/resources"
        headers = {"Authorization": f"OAuth {token}"}
        response = self._request("PUT", url, stage="create_directory", side="destination",
                                 destination=path, kind="dir", params={"path": path},
                                 headers=headers)
        if response.status_code in (201, 409):
            state = "создана" if response.status_code == 201 else "уже существует"
            self._detail(f"ensure_folder | {path} — {state}")
            return
        # без папки назначения сохранять некуда — молчать нельзя
        error = self._failure(response, f"Не удалось создать папку {path} на Диске "
                              f"{self.cfg.destination_disk_id}",
                              side="destination", mutation=True)
        error.args = (str(error) + self._access_hint(response.status_code, "cloud_api:disk.write"),)
        raise error

    def _disk_save_public_resource(self, public_key: str, name: str) -> None:
        self._secret(public_key, link=True)
        url = "https://cloud-api.yandex.net/v1/disk/public/resources/save-to-disk"
        params = {
            "public_key": public_key,
            "name": name,
            "save_path": self._target_folder_personal(),
        }
        headers = {"Authorization": f"OAuth {self._destination_token()}"}
        response = self._request("POST", url, stage="upload", side="destination",
                                 destination=f"{self._target_folder_personal()}/{name}",
                                 params=params, headers=headers)
        self._detail(
            f"disk_save_public_resource | status: {response.status_code} | "
            f"name: {name}"
        )
        if response.status_code == 202:
            href = self._response_payload(response).get("href")
            if not href:
                raise self._failure(response, f"Нет ссылки на операцию при сохранении {name}",
                                    side="destination", uncertain=True)
            self._wait_operation(href, self._destination_token())
        elif response.status_code == 201:
            return
        elif response.status_code == 409:
            self._file_outcome = "skipped"
            self._detail(f"disk_save_public_resource | уже существует, пропускаю: {name}")
        else:
            raise self._failure(response, f"Не удалось сохранить {name} на Диск получателя",
                                side="destination", mutation=True)

    def _public_download_href(self, public_key: str) -> str:
        """Прямая ссылка на скачивание опубликованного ресурса."""
        self._secret(public_key, link=True)
        url = "https://cloud-api.yandex.net/v1/disk/public/resources/download"
        response = self._request("GET", url, stage="get_download_url", side="source",
                                 params={"public_key": public_key})
        with self._response_parse(response, code="invalid_download_response"):
            payload = response.json()
            href = payload.get("href")
        if not href:
            raise self._failure(response, "Не удалось получить ссылку скачивания для public_key",
                                side="source")
        self._secret(href, link=True)
        return href

    def _wait_operation(self, href: str, token: str, timeout_sec: int = 600) -> None:
        """Ждёт завершения асинхронной операции Диска.

        Интервал опроса растёт от POLL_FIRST_DELAY до POLL_MAX_DELAY: короткие
        операции забираем почти сразу, длинные не опрашиваем 300 раз подряд.
        Рекомендаций по интервалу в документации API Диска нет — величины наши.
        """
        headers = {"Authorization": f"OAuth {token}"}
        self._secret(href, link=True)
        self._secret(token)
        operation = {}
        try:
            parsed = urlsplit(href)
            prefix = "/v1/disk/operations/"
            identifier = parsed.path[len(prefix):] if parsed.path.startswith(prefix) else ""
            if parsed.hostname == "cloud-api.yandex.net" and re.fullmatch(
                r"[A-Za-z0-9_-]{1,256}", identifier,
            ):
                operation["operation_id"] = identifier
        except ValueError:
            pass
        # Иногда href — шаблон или относительный; берём как есть.
        deadline = self._monotonic() + timeout_sec
        delay = POLL_FIRST_DELAY
        while True:
            try:
                response = self._request("GET", href, stage="poll_operation", side="destination",
                                         headers=headers, **operation)
            except requests.RequestException as exc:
                exc.uncertain = True
                self._event({"event_type": "stage", "stage": "poll_operation",
                             "status": "outcome_unknown",
                             **operation,
                             **self._error_details(exc, "destination", href),
                             "error": "operation_status_unavailable"})
                raise
            if not self.retry_policy.enabled and response.status_code >= 400:
                raise self._failure(response, "Не удалось проверить результат принятой операции",
                                    side="destination", uncertain=True,
                                    code="operation_status_unavailable")
            try:
                payload = response.json() if response.content else {}
                status = payload.get("status")
            except (ValueError, AttributeError, RecursionError) as exc:
                exc.uncertain = True
                exc.journal_diagnostics = self._response_details(response, "destination", href)
                self._event({"event_type": "stage", "stage": "poll_operation",
                             "status": "outcome_unknown", **exc.journal_diagnostics, **operation,
                             "error": "invalid_operation_response"})
                if not self.retry_policy.enabled:
                    raise self._failure(response, "Некорректный ответ проверки операции; исход неизвестен",
                                        side="destination", uncertain=True,
                                        code="invalid_operation_response") from None
                raise
            if not self.retry_policy.enabled and status not in ("in-progress", "success", "failed"):
                raise self._failure(response, "Сервер не сообщил корректный статус принятой операции",
                                    side="destination", uncertain=True,
                                    code="invalid_operation_response")
            self._event({"event_type": "stage", "stage": "poll_operation",
                         "error_side": "destination",
                         **operation,
                         "status": "succeeded" if status == "success" else "failed"
                         if status == "failed" else "waiting",
                         "http_status": response.status_code,
                         **self._response_details(response, "destination", href),
                         "message": "Операция подтверждена API." if status == "success"
                         else "Операция завершилась ошибкой." if status == "failed"
                         else "Ожидание результата операции."})
            if status == "success":
                return
            if status == "failed":
                raise self._failure(response, "Операция Диска завершилась ошибкой",
                                    side="destination")
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                self._event({"event_type": "stage", "stage": "poll_operation",
                             "error_side": "destination",
                             **operation,
                             "status": "outcome_unknown", "error": "operation_timeout",
                             "message": "Таймаут ожидания; исход операции не подтверждён."})
                raise CopyError("Таймаут ожидания операции Диска; исход неизвестен", uncertain=True,
                                diagnostics={"error_side": "destination"}, code="operation_timeout")
            self._event({"event_type": "retry", "stage": "poll_operation",
                         "error_side": "destination", "status": "waiting", **operation,
                         "retry_delay_ms": int(min(delay, POLL_MAX_DELAY, remaining) * 1000),
                         "message": "Следующая проверка статуса операции."})
            self._sleep(min(delay, POLL_MAX_DELAY, remaining))
            delay *= 2

    # ── API общих дисков (virtual-disks) ───────────────────────────────
    def _vd_ensure_folder(self, path: str) -> None:
        # свежий токен на каждый запрос — перенос может длиться дольше TTL токена
        headers = {"Authorization": f"OAuth {self._destination_token()}"}
        url = "https://cloud-api.yandex.net/v1/disk/virtual-disks/resources"
        response = self._request("PUT", url, stage="create_directory", side="destination",
                                 destination=path, kind="dir", params={"path": path},
                                 headers=headers)
        if response.status_code in (201, 409):
            state = "создана" if response.status_code == 201 else "уже существует"
            self._detail(f"vd_ensure_folder | {path} — {state}")
        else:
            raise self._failure(response, f"Не удалось создать папку на общем диске {path}",
                                side="destination", mutation=True)

    def _vd_ensure_folder_tree(self, inner: str) -> None:
        """Создаёт цепочку папок внутри общего диска (inner без vd: префикса).

        Созданные уровни помнит в self._vd_created: раньше цепочка пересоздавалась
        на каждый файл и почти все ответы были 409 «уже существует».
        """
        parts = [p for p in _strip_disk_schema(inner).strip("/").split("/") if p]
        acc: list[str] = []
        for part in parts:
            acc.append(part)
            path = self._vd_path("/".join(acc))
            if path in self._vd_created:
                continue
            # в кэш только после успеха: упавшая папка не считается созданной
            self._vd_ensure_folder(path)
            self._vd_created.add(path)

    def _vd_upload_from_url(self, file_url: str, dest_path: str) -> None:
        """Загрузка файла из интернета на общий диск (async → ждём операцию)."""
        url = "https://cloud-api.yandex.net/v1/disk/virtual-disks/resources/upload"
        headers = {"Authorization": f"OAuth {self._destination_token()}"}
        params = {"url": file_url, "path": dest_path}
        self._secret(file_url, link=True)
        response = self._request("POST", url, stage="upload", side="destination",
                                 destination=dest_path, params=params, headers=headers)
        self._detail(
            f"vd_upload_from_url | status: {response.status_code} | path: {dest_path}"
        )
        if response.status_code == 202:
            href = self._response_payload(response).get("href")
            if not href:
                raise self._failure(response, "Нет ссылки на операцию при загрузке на общий диск",
                                    side="destination", uncertain=True)
            self._wait_operation(href, self._destination_token())
        elif response.status_code == 201:
            return
        elif response.status_code == 409:
            self._file_outcome = "skipped"
            self._detail(f"vd_upload_from_url | уже существует, пропускаю: {dest_path}")
        else:
            raise self._failure(response, f"Не удалось загрузить на общий диск {dest_path}",
                                side="destination", mutation=True)

    # ── обход и публикация ─────────────────────────────────────────────
    def _walk(self, path: str, handler: Callable[[dict], None]) -> None:
        """Итеративный обход всех страниц ресурса с применением handler к items."""
        offset = 0
        while True:
            response = self._disk_get_meta(
                self._source_token(), self.cfg.page_limit, offset, path
            )
            emb = response.get("_embedded")
            if not isinstance(emb, dict):
                if self._is_shared_source:
                    raise CopyError(f"Не удалось прочитать папку общего диска: {path}")
                self.log(f"No such resource: {self.cfg.source_disk_id} | {path}")
                return
            total = emb.get("total", 0)
            items = emb.get("items", [])
            # ceil-деление: при total, кратном page_limit, лишнего запроса
            # за пустой страницей не делаем
            limit = self.cfg.page_limit
            pages = max(1, (total + limit - 1) // limit)
            page = offset // limit + 1
            self.log(f"walk | path: {path}, page: {page}/{pages}, items: {len(items)}")
            for item in items:
                handler(item)
            if page >= pages:
                return
            offset = page * self.cfg.page_limit

    def _collect_files_recursive(self, path: str) -> list[dict]:
        """Рекурсивно собирает все файлы под path."""
        files: list[dict] = []

        def collect(item: dict) -> None:
            if item.get("type") == "dir":
                self._walk(item["path"], collect)
            elif item.get("type") == "file":
                files.append(item)

        self._walk(path, collect)
        return files

    def make_links(self) -> None:
        """Публикует все ресурсы в исходной папке (создаёт публичные ссылки)."""
        self.log(f"make_links | start | path: {self.cfg.path}")
        def publish(item):
            self._wait_file_pause()
            token = self._source_token()
            try:
                self._disk_publish_resource(token, item["path"])
            except (CopyError, requests.RequestException) as exc:
                if self.retry_policy.enabled or _access_failure(exc):
                    raise
                self._failed_publications.add(item["path"])
                self._file_failure(item, exc, stage="publish", side="source")

        self._walk(self.cfg.path, publish)
        self.log("make_links | done")

    def get_links(self) -> None:
        """Собирает public_key опубликованных ресурсов; что без ключа — в fails."""
        self.log(f"get_links | start | path: {self.cfg.path}")

        def collect(item: dict) -> None:
            path = item.get("path", "")
            if any(path == failed or path.startswith(failed.rstrip("/") + "/")
                   for failed in self._failed_publications):
                return
            public_key = item.get("public_key")
            record = {
                "path": item.get("path"),
                "name": item.get("name"),
                "type": item.get("type"),
            }
            if isinstance(public_key, str):
                self._secret(public_key, link=True)
                self.links.append({**record, "public_key": public_key})
            else:
                # без public_key сохранять нечего; раньше терялось в счётчике
                reason = "публикация не дала public_key"
                self._event({"event_type": "stage", "stage": "collect_links", "status": "failed",
                             "error_side": "source", "source": record["path"],
                             "kind": record["type"],
                             "error": "publication_not_confirmed", "message": reason})
                self._detail(f"get_links | пропуск {record['path']}: {reason}")
                self._file_failure(record, CopyError(reason, code="publication_not_confirmed",
                                   diagnostics={"error_side": "source"}),
                                   stage="collect_links", side="source")

        self._walk(self.cfg.path, collect)
        self.log(
            f"get_links | done | links: {len(self.links)}, fails: {len(self.fails)}"
        )

    def save_links(self) -> None:
        """Сохраняет ресурсы на личный Диск получателя (оригинальные имена).

        В self.links остаются только реально сохранённые ресурсы; всё, что не
        доехало, уходит в self.fails с причиной — иначе отчёт врёт про «saved».
        """
        folder = self._target_folder_personal()
        pending = self.links
        self.links = []
        self.log(f"save_links | start | count: {len(pending)}, папка: {folder}")
        self._disk_ensure_folder(self._destination_token(), folder)
        for i, link in enumerate(pending, 1):
            try:
                with self._stage("file", "destination", source=link.get("path"),
                                 destination=f"{folder}/{link.get('name')}", kind=link.get("type")):
                    self._wait_file_pause()
                    self._save_one_link(link)
                    self._record_result(link)
            except (CopyError, requests.RequestException) as exc:
                if _access_failure(exc):
                    raise
                self._detail(f"save_links | ошибка {link.get('path')}: {exc}")
                self._file_failure(link, exc, destination=f"{folder}/{link.get('name')}")
            self.log(f"save_links | progress {i}/{len(pending)}")
        self.log(
            f"save_links | done | saved: {len(self.links)}, fails: {len(self.fails)}"
        )

    def _owner_download_href(self, path: str, *, token: str = "") -> str:
        """Временная ссылка на скачивание файла токеном владельца.

        Не путать с публичной: та для некоторых файлов отвечает 404, хотя
        сам файл на Диске есть (типично — метка антивируса).
        """
        resource = "virtual-disks/resources" if self._is_shared_source else "resources"
        url = f"https://cloud-api.yandex.net/v1/disk/{resource}/download"
        headers = {"Authorization": f"OAuth {token or self._source_token()}"}
        response = self._request(
            "GET", url, stage="get_download_url", side="source", source=path,
            params={"path": self._source_path(path)}, headers=headers
        )
        payload = self._response_payload(response)
        href = payload.get("href") if response.status_code == 200 else None
        self._detail(
            f"disk_download_href | status: {response.status_code} | path: {path}"
        )
        if not isinstance(href, str) or not href:
            raise self._failure(response, f"Не удалось получить ссылку скачивания {path}",
                                side="source")
        self._secret(href, link=True)
        return href

    def _owner_upload_href(self, dest_path: str) -> Optional[str]:
        """Куда PUT-ить байты. None — файл у получателя уже есть."""
        url = "https://cloud-api.yandex.net/v1/disk/resources/upload"
        headers = {"Authorization": f"OAuth {self._destination_token()}"}
        response = self._request(
            "GET", url, stage="get_upload_url", side="destination", destination=dest_path,
            params={"path": dest_path, "overwrite": "false"},
            headers=headers,
        )
        self._detail(
            f"disk_upload_href | status: {response.status_code} | path: {dest_path}"
        )
        if response.status_code == 409:
            self._file_outcome = "skipped"
            self._detail(f"disk_upload_href | уже существует, пропускаю: {dest_path}")
            return None
        href = self._response_payload(response).get("href")
        if response.status_code != 200 or not isinstance(href, str) or not href:
            raise self._failure(response, f"Не удалось получить ссылку загрузки {dest_path}",
                                side="destination")
        self._secret(href, link=True)
        return href

    def _read_stream(self, response, href):
        started = self._monotonic()
        size = 0
        context = getattr(response, "_journal_context", {})
        self._event({**context, "event_type": "stage", "stage": "download", "status": "started",
                     "error_side": "source"})
        try:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    size += len(chunk)
                    yield chunk
        except requests.RequestException as exc:
            details = self._error_details(exc, "source", href)
            exc.journal_diagnostics = details
            self._event({**context, "event_type": "stage", "stage": "download", "status": "failed",
                         "error": "stream_interrupted", **details,
                         "size": size, "duration_ms": int((self._monotonic() - started) * 1000)})
            raise
        else:
            self._event({**context, "event_type": "stage", "stage": "download",
                         "status": "succeeded",
                         "error_side": "source", "size": size,
                         "duration_ms": int((self._monotonic() - started) * 1000)})

    def _copy_file_bytes(self, src_path: str, name: str) -> None:
        """Скачивает файл у источника и заливает байты получателю."""
        dest_path = f"{self._target_folder_personal()}/{name}"
        download_href = self._owner_download_href(src_path)
        upload_href = self._owner_upload_href(dest_path)
        if upload_href is None:
            return

        downloaded = self._request("GET", download_href, stage="download", side="source",
                                   source=src_path,
                                   destination=dest_path, kind="file", stream=True)
        if downloaded.status_code != 200:
            raise self._failure(downloaded, f"Не удалось скачать {src_path}",
                                side="source", stream=True)
        body = bytearray()
        for chunk in self._read_stream(downloaded, download_href):
            if not chunk:
                continue
            body.extend(chunk)
            if len(body) > DIRECT_COPY_MAX_BYTES:
                raise CopyError(
                    f"{src_path} больше "
                    f"{DIRECT_COPY_MAX_BYTES // (1024 * 1024)} Мб — "
                    f"прямое копирование через сервис не выполняется"
                )
        uploaded = self._request(
            "PUT", upload_href, stage="upload", side="destination", source=src_path,
            destination=dest_path, kind="file",
            data=bytes(body),
            headers={"Content-Length": str(len(body))},
        )
        self._detail(
            f"direct_copy | {len(body)} байт | upload status: "
            f"{uploaded.status_code} | {dest_path}"
        )
        if uploaded.status_code not in (200, 201, 202):
            raise self._failure(uploaded, f"Не удалось залить {dest_path}",
                                side="destination", mutation=True)
        if uploaded.status_code == 202:
            self._file_outcome = "waiting"

    def _save_one_link(self, link: dict) -> None:
        """Сохраняет ресурс. Если публичная ссылка мертва — копирует файл байтами.

        save-to-disk и публичное скачивание отвечают 404 DiskNotFoundError на
        один и тот же public_key, хотя publish вернул 200. Повторная публикация
        ключ не меняет. Для файла остаётся скачать его токеном владельца и
        залить получателю. Папку так не перенести.
        """
        try:
            self._disk_save_public_resource(link["public_key"], link["name"])
            return
        except CopyError as exc:
            if not self.retry_policy.enabled or "404" not in str(exc):
                raise
            if link.get("type") != "file":
                raise CopyError(
                    f"{exc}. Папку без рабочей публичной ссылки не перенести — "
                    f"скопируйте {link.get('path')} вручную"
                )
            self.log(
                f"save_links | публичная ссылка {link.get('path')} недоступна — "
                f"копирую файл напрямую"
            )
        self._copy_file_bytes(link["path"], link["name"])

    def save_to_shared(self) -> None:
        """Публикует файлы источника и загружает их на общий диск, сохраняя дерево."""
        root_inner = f"/{self.cfg.source_disk_id}"
        root_vd = self._target_folder_shared()
        self.log(
            f"save_to_shared | start | vd_hash: {self.cfg.destination_vd_hash} | "
            f"папка: {root_vd}"
        )
        self._vd_ensure_folder_tree(root_inner)

        files = self._collect_files_recursive(self.cfg.path)
        self.log(f"save_to_shared | файлов к переносу: {len(files)}")
        if not files:
            self.log("save_to_shared | нечего переносить")
            return

        for i, item in enumerate(files, 1):
            src_path = item["path"]
            name = item.get("name") or src_path.rsplit("/", 1)[-1]
            inner = self._dest_inner(item)
            dest_vd = self._vd_path(inner)
            record = {"path": src_path, "name": name}
            token = self._source_token()
            stage = "create_directory"
            try:
                with self._stage("file", "destination", source=src_path, destination=dest_vd,
                                 kind="file", size=item.get("size")):
                    self._wait_file_pause()
                    self._vd_ensure_folder_tree(inner.rsplit("/", 1)[0])
                    stage = "publish"
                    self._disk_publish_resource(token, src_path)
                    stage = "metadata"
                    meta = self._disk_resource(self._source_token(), src_path)
                    public_key = meta.get("public_key")
                    if not isinstance(public_key, str):
                        raise CopyError("Публикация не дала публичного ключа.",
                                        diagnostics={"error_side": "source"},
                                        code="publication_not_confirmed")
                    stage = "get_download_url"
                    href = self._public_download_href(public_key)
                    stage = "upload"
                    self._vd_upload_from_url(href, dest_vd)
                    self._record_result(
                        {**record, "public_key": public_key, "dest": dest_vd})
            except (CopyError, requests.RequestException) as exc:
                if _access_failure(exc) or (
                    self.retry_policy.enabled and stage in {"create_directory", "publish", "metadata"}
                    and getattr(exc, "code", None) != "publication_not_confirmed"
                ):
                    raise
                self._detail(f"save_to_shared | ошибка {src_path}: {exc}")
                self._file_failure(record, exc, stage=stage,
                                   side="source" if stage in {"publish", "metadata", "get_download_url"}
                                   else "destination", destination=dest_vd)
            self.log(f"save_to_shared | progress {i}/{len(files)}")

        self.log(f"save_to_shared | папок создано: {len(self._vd_created)}")

        self.log(
            f"save_to_shared | done | saved: {len(self.links)}, "
            f"fails: {len(self.fails)}"
        )

    def _local_file_path(self, disk_path: str, base: Path) -> Path:
        """Безопасный путь файла внутри локальной папки выбранного источника."""
        rel = self._source_inner_path(disk_path or "").lstrip("/")
        parts: list[str] = []
        for part in rel.split("/"):
            if part in ("", "."):
                continue
            if part == ".." or "\\" in part:
                raise CopyError(f"Небезопасный путь на Диске: {disk_path}")
            parts.append(part)
        dest = base.joinpath(*parts) if parts else base
        resolved = dest.resolve()
        base_resolved = base.resolve()
        if resolved != base_resolved:
            resolved.relative_to(base_resolved)
        return resolved

    def _download_to_path(self, src_path: str, dest: Path) -> None:
        """Скачивает файл токеном владельца и атомарно кладёт его в dest.

        Ссылка на скачивание и само тело файла — GET, то есть читающие запросы:
        они расходуют общий бюджет запросов и полосу чтения.
        """
        with self._stage("create_directory", "local", source=src_path,
                         destination=str(dest.parent), kind="dir"):
            dest.parent.mkdir(parents=True, exist_ok=True)
        if self._download_session is None:
            # Повторы всего файла считаем здесь; signed URL в общий лог не пишем.
            self._download_session = ThrottledSession(
                get_limiter(), max_attempts=1, retry_policy=self.retry_policy,
            )
        for attempt in range(LOCAL_DOWNLOAD_RETRIES + 1):
            response = part = None
            status = retry_after = None
            try:
                token = self._source_token()
                href = self._owner_download_href(src_path, token=token)
                # API общего диска требует тот же OAuth-токен и для полученного URL.
                kwargs = (
                    {"headers": {"Authorization": f"OAuth {self._source_token()}"}}
                    if self._is_shared_source else {}
                )
                response = self._request("GET", href, stage="download", side="source",
                                         source=src_path, destination=str(dest), kind="file",
                                         session=self._download_session, stream=True,
                                         attempt=attempt + 1,
                                         max_attempts=(LOCAL_DOWNLOAD_RETRIES + 1
                                                       if self.retry_policy.enabled else 1), **kwargs)
                status = response.status_code
                if status != 200:
                    error = self._failure(response, f"Не удалось скачать {src_path}",
                                          side="source", stream=True)
                    if status not in RETRY_STATUSES and status != 408:
                        raise error
                    reason = f"HTTP {status}"
                    retry_after = retry_after_seconds(
                        response.headers.get("Retry-After")
                    )
                else:
                    with self._stage("local_write", "local", source=src_path,
                                     destination=str(dest), kind="file"):
                        with tempfile.NamedTemporaryFile(
                            mode="wb", dir=dest.parent,
                            prefix=".disk-copy-", suffix=".part",
                            delete=False,
                        ) as handle:
                            part = Path(handle.name)
                            for chunk in self._read_stream(response, href):
                                handle.write(chunk)
                        os.replace(part, dest)
                    return
            except requests.exceptions.SSLError:
                raise
            except (
                requests.Timeout, requests.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ContentDecodingError,
            ) as exc:
                error = exc
                reason = type(exc).__name__
            finally:
                if response is not None:
                    response.close()
                if part is not None and part.exists():
                    part.unlink()

            if not self.retry_policy.enabled:
                raise error
            if attempt == LOCAL_DOWNLOAD_RETRIES:
                self._event({"event_type": "retry", "stage": "download", "status": "failed",
                             "source": src_path, "destination": str(dest),
                             **self._error_details(error, "source"), "http_status": status,
                             "attempt": attempt + 1, "max_attempts": LOCAL_DOWNLOAD_RETRIES + 1,
                             "message": "Попытки скачивания исчерпаны; файл пропущен."})
                raise error
            base_delay = backoff_delay(attempt + 2)
            delay = base_delay + random.uniform(0, base_delay / 4)
            if retry_after is not None and math.isfinite(retry_after):
                delay = max(delay, retry_after)
            if status == 429:
                get_limiter().penalize(delay)
            generation = self.retry_policy.cancellation_generation
            message = (f"save_local | {src_path} | {reason} | "
                       f"retry {attempt + 1}/{LOCAL_DOWNLOAD_RETRIES} через {delay:.1f} с")
            if callable(getattr(self._log, "event", None)):
                self._event({"event_type": "retry", "stage": "download", "status": "waiting",
                             "source": src_path, "destination": str(dest),
                             **self._error_details(error, "source"), "http_status": status,
                             "attempt": attempt + 1, "max_attempts": LOCAL_DOWNLOAD_RETRIES + 1,
                             "retry_delay_ms": int(delay * 1000), "message": message})
            else:
                self.log(message)
            if not self.retry_policy.wait(delay, generation=generation):
                self._event({"event_type": "retry", "stage": "download", "status": "skipped",
                             "source": src_path, "destination": str(dest),
                             **self._error_details(error, "source"), "http_status": status,
                             "attempt": attempt + 1, "retry_enabled": False,
                             "error": getattr(error, "code", "transport_failed"),
                             "message": "Автоматические повторы отключены; ожидавший повтор отменён."})
                raise error

    def save_local(self) -> None:
        """Скачивает дерево PATH на машину, с которой запущена утилита.

        Повторный запуск не качает файл заново, если локальная копия уже есть
        и её размер совпадает с размером на Диске.
        """
        root = Path(self.cfg.local_root).expanduser()
        email = (self.cfg.source_disk_id or "").strip()
        if not email or "/" in email or email in (".", ".."):
            raise CopyError(f"Некорректный email источника: {email}")
        subdir = (
            Path("shared") / self.cfg.source_vd_hash
            if self._is_shared_source else Path(email)
        )
        try:
            base = self._local_file_path("disk:/", root / subdir)
            base.relative_to(root.resolve())
            with self._stage("create_directory", "local", source=self.cfg.path,
                             destination=str(base), kind="dir"):
                base.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise CopyError(
                f"Не удалось создать каталог сохранения: {exc}"
            ) from exc
        except ValueError as exc:
            raise CopyError(
                f"Каталог сохранения выходит за пределы LOCAL_SAVE_DIR: {exc}"
            ) from exc

        host = (os.environ.get("LOCAL_SAVE_HOST_DIR") or "").strip()
        self.log(f"save_local | start | path: {self.cfg.path} | каталог: {base}")
        if host:
            self.log(
                f"save_local | на машине запуска: "
                f"{host.rstrip('/')}/{subdir.as_posix()}"
            )
        downloads = Path("/downloads")
        if root.resolve() == downloads and not downloads.is_mount():
            self.log(
                "save_local | ВНИМАНИЕ | /downloads не примонтирован с хоста, "
                "файлы останутся внутри контейнера. Запускайте с томом "
                "./downloads:/downloads"
            )

        files: list[dict] = []

        def visit(item: dict) -> None:
            kind = item.get("type")
            path = item.get("path")
            if not isinstance(path, str):
                return
            if kind == "dir":
                dest_dir = None
                try:
                    with self._stage("create_directory", "local", source=path, kind="dir"):
                        if self._file_cooldown.after_error(self.cfg.source_disk_id):
                            self._wait_file_pause()
                        dest_dir = self._local_file_path(path, base)
                        self._context["destination"] = str(dest_dir)
                        dest_dir.mkdir(parents=True, exist_ok=True)
                except (CopyError, ValueError, OSError) as exc:
                    if self.retry_policy.enabled:
                        raise
                    self._file_failure(item, exc, stage="create_directory", side="local",
                                       destination=str(dest_dir) if dest_dir is not None else None)
                    return
                self._walk(path, visit)
            elif kind == "file":
                files.append(item)

        self._walk(self.cfg.path, visit)
        self.log(f"save_local | файлов: {len(files)}")
        for i, item in enumerate(files, 1):
            src_path = item["path"]
            if self._file_cooldown.after_error(self.cfg.source_disk_id):
                self._wait_file_pause()
            dest = None
            try:
                with self._stage("file", "local", source=src_path, kind="file",
                                 size=item.get("size")):
                    dest = self._local_file_path(src_path, base)
                    self._context["destination"] = str(dest)
                    size = item.get("size")
                    if (
                        isinstance(size, int)
                        and dest.is_file()
                        and dest.stat().st_size == size
                    ):
                        self._file_outcome = "skipped"
                        self._event({"event_type": "stage", "stage": "check_local",
                                     "status": "skipped", "error_side": "local",
                                     "message": "Локальный файл существует; совпадает размер. "
                                     "Содержимое повторно не проверялось."})
                        self._detail(f"save_local | уже есть, пропускаю: {src_path}")
                        self.skipped.append(
                            {"path": src_path, "name": item.get("name"), "dest": str(dest)}
                        )
                    else:
                        self._wait_file_pause()
                        self._download_to_path(src_path, dest)
                        self._file_cooldown.mark(self.cfg.source_disk_id)
                        self.links.append(
                            {"path": src_path, "name": item.get("name"), "dest": str(dest)}
                        )
                        self._detail(f"save_local | сохранён {src_path}")
            except (CopyError, OSError, ValueError, requests.RequestException) as exc:
                if _access_failure(exc):
                    raise
                error = (
                    f"Ошибка скачивания: {type(exc).__name__}"
                    if isinstance(exc, requests.RequestException) else self._safe_text(exc)
                )
                self._detail(f"save_local | ошибка {src_path}: {error}")
                self._file_failure({"path": src_path, "name": item.get("name")}, exc,
                                   side="local", destination=str(dest) if dest is not None else None)
            self.log(f"save_local | progress {i}/{len(files)}")
        self.log(
            f"save_local | done | saved: {len(self.links)}, fails: {len(self.fails)}"
        )

    # ── оркестрация ────────────────────────────────────────────────────
    def run(self) -> dict:
        cfg = self.cfg
        if cfg.source_type not in ("personal", "shared"):
            raise CopyError("source_type должен быть 'personal' или 'shared'")
        if self._is_shared_source:
            if not cfg.local_save:
                raise CopyError(
                    "Общий диск как источник поддержан для локального сохранения"
                )
            if not re.fullmatch(r"[A-Za-z0-9_-]+", cfg.source_vd_hash):
                raise CopyError(
                    "Для общего источника укажите корректный source_vd_hash"
                )
        if cfg.local_save:
            cfg.source_disk_id = validate_source_email(
                cfg.source_disk_id, shared=self._is_shared_source
            )
            if not (cfg.local_root or "").strip():
                raise CopyError(
                    "Каталог локального сохранения не задан (LOCAL_SAVE_DIR)"
                )
            source_label = (
                f"shared:{cfg.source_vd_hash} (via {cfg.source_disk_id})"
                if self._is_shared_source else cfg.source_disk_id
            )
            self.log(f"START | save local from {source_label} | path: {cfg.path}")
        elif self._is_shared_dest and not cfg.destination_vd_hash:
            raise CopyError(
                "Для переноса на общий диск укажите destination_vd_hash "
                "(метка из адресной строки Диска после vd/)."
            )
        else:
            dest_label = (
                f"shared:{cfg.destination_vd_hash} (via {cfg.destination_disk_id})"
                if self._is_shared_dest
                else cfg.destination_disk_id
            )
            self.log(
                f"START | copy from {cfg.source_disk_id} to {dest_label} | "
                f"path: {cfg.path} | dest_type: {cfg.destination_type}"
            )

        # 1. Токен исходного диска + uid сотрудника из него.
        source_token = self._source_token()
        source_uid = source_token.split(".")[1]

        # 2. Если задан admin-токен источника и сотрудник заблокирован —
        #    временно разблокируем (в cross-org управляем блокировкой только
        #    источника; у получателя admin-токена нет).
        ban_need = False
        if cfg.source.admin_token:
            is_active = self._api360_get_ban_status(source_uid)
            if is_active is False:
                self._api360_patch_user(source_uid, True)
                ban_need = True
            else:
                self.log(f"User {cfg.source_disk_id} active — no status change needed")
        else:
            self.log("source admin_token не задан — пропускаю проверку блокировки")

        transfer_ok = False
        try:
            if cfg.local_save:
                self.save_local()
            else:
                # 3. Проверка места на диске назначения.
                if SKIP_SPACE_CHECK:
                    self.log(
                        "space check | пропущена (по умолчанию) — если места не "
                        "хватит, файлы начнут падать в fails уже в процессе "
                        "переноса. Включить сверку: SPACE_CHECK=1"
                    )
                    needed_space = free_space = 0
                else:
                    needed_space, _ = self._disk_space_info(
                        self._source_token(), disk_id=cfg.source_disk_id
                    )
                    if self._is_shared_dest:
                        _, free_space = self._shared_space_info(
                            self._destination_token()
                        )
                    else:
                        _, free_space = self._disk_space_info(
                            self._destination_token(),
                            disk_id=cfg.destination_disk_id,
                        )
                    self.log(
                        f"space check | need: {needed_space} bytes, "
                        f"free: {free_space} bytes"
                    )

                if not SKIP_SPACE_CHECK and free_space <= needed_space:
                    raise CopyError(
                        f"Недостаточно места на диске назначения ({dest_label}): "
                        f"нужно {needed_space}, свободно {free_space}"
                    )

                # 4. Публикация → сохранение.
                if self._is_shared_dest:
                    self.save_to_shared()
                else:
                    self.make_links()
                    self.get_links()
                    self.save_links()
            transfer_ok = True
        finally:
            # 5. Возвращаем исходную блокировку, что бы ни случилось.
            if ban_need:
                try:
                    self._api360_patch_user(source_uid, False)
                except CopyError as exc:
                    # Сотрудник остался разблокированным — это надо увидеть.
                    self.log(
                        f"ВНИМАНИЕ | не удалось вернуть блокировку "
                        f"{cfg.source_disk_id}: {exc} — заблокируйте сотрудника "
                        f"вручную в админке организации"
                    )
                    # Если перенос упал, его ошибка важнее — не маскируем её.
                    if transfer_ok:
                        raise

        self._log_fails()
        self.log(
            f"COMPLETED | saved: {len(self.links)}, skipped: {len(self.skipped)}, "
            f"unconfirmed: {len(self.unconfirmed)}, fails: {len(self.fails)}"
        )
        result = {"saved": self.links, "fails": self.fails}
        if self.skipped:
            result["skipped"] = self.skipped
        if self.unconfirmed:
            result["unconfirmed"] = self.unconfirmed
        return result

    def _log_fails(self) -> None:
        """Перечисляет непереносённые файлы: по счётчику их не найти."""
        if not self.fails:
            return
        self.log(f"NOT COPIED | файлов не перенесено: {len(self.fails)}")
        for i, item in enumerate(self.fails[:FAILS_LOG_LIMIT], 1):
            path = item.get("path") or item.get("name") or "?"
            reason = item.get("error") or "причина не указана"
            self.log(f"NOT COPIED | {i}. {path} — {reason}")
        hidden = len(self.fails) - FAILS_LOG_LIMIT
        if hidden > 0:
            self.log(
                f"NOT COPIED | ещё {hidden} — полный список: "
                f"GET /api/run/<job_id>, поле fails"
            )
