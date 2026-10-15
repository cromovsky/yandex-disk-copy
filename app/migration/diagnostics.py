"""Bounded error details: API identifiers and network categories, never raw text."""

import ipaddress
import json
import re
import socket
from http.client import RemoteDisconnected
from urllib.parse import urlsplit

import requests
from urllib3.exceptions import (
    ConnectTimeoutError, MaxRetryError, NameResolutionError, NewConnectionError,
)


DIAGNOSTIC_KEYS = frozenset({
    "error_side", "api_error", "transport_error", "request_host", "request_id",
    "api_reason",
})
TRANSPORT_MESSAGES = {
    "connect_timeout": "Истекло время установления соединения.",
    "connect_failed": (
        "Не удалось установить соединение с сервером; HTTP-запрос не отправлен."
    ),
    "read_timeout": "Истекло время ожидания ответа сервера.",
    "timeout": "Истекло время ожидания; тип таймаута не указан.",
    "tls": "Не удалось установить защищённое TLS-соединение.",
    "dns": "Не удалось определить сетевой адрес сервера.",
    "connection": "Не удалось установить соединение с сервером.",
    "remote_closed": (
        "Соединение закрыто до получения HTTP-ответа. "
        "Сервер или промежуточный сетевой узел не передал ответ."
    ),
    "stream": "Соединение прервалось при передаче данных.",
    "decode": "Не удалось декодировать ответ сервера.",
    "transport": "Сетевая ошибка; более точная причина не предоставлена.",
}
HTTP_MESSAGES = {
    200: "Запрос обработан сервером.",
    202: "Запрос принят; результат операции ещё не подтверждён.",
    301: "Сервер вернул перенаправление.",
    302: "Сервер вернул перенаправление.",
    303: "Сервер вернул перенаправление.",
    307: "Сервер вернул перенаправление.",
    308: "Сервер вернул перенаправление.",
    400: "Некорректный запрос или параметры.",
    401: "Авторизация отсутствует или недействительна.",
    403: "Сервер отказал в доступе.",
    404: "Запрошенный ресурс не найден.",
    405: "Метод запроса не поддерживается.",
    406: "Сервер не поддерживает запрошенный формат ответа.",
    409: "Запрос конфликтует с текущим состоянием ресурса.",
    410: "Ресурс больше недоступен.",
    412: "Предварительное условие запроса не выполнено.",
    413: "Размер файла или запроса превышает допустимый.",
    415: "Формат данных не поддерживается.",
    416: "Запрошенный диапазон байтов недоступен.",
    423: "Ресурс заблокирован.",
    429: "Превышен лимит запросов.",
    500: "Внутренняя ошибка сервера.",
    502: "Промежуточный сервер получил некорректный ответ.",
    503: "Сервис временно недоступен.",
    504: "Промежуточный сервер не дождался ответа.",
    507: "Недостаточно свободного места на сервере.",
}
API_MESSAGES = {
    "UnauthorizedError": "Авторизация отсутствует или недействительна.",
    "ForbiddenError": "Сервер отказал в доступе.",
    "InternalServerError": "Внутренняя ошибка сервера.",
    "DiskNotFoundError": "Файл или папка не найдены.",
    "DiskResourceNotFoundError": "Ресурс не найден.",
    "DiskResourceAlreadyExistsError": "Ресурс по указанному пути уже существует.",
    "DiskInsufficientStorageError": "Недостаточно свободного места.",
    "DiskNotEnoughSpaceError": "Недостаточно свободного места.",
    "DiskOperationFailedError": "Сервер сообщил, что операция завершилась ошибкой.",
    "DiskDownloadLimitExceededError": "Превышен лимит скачивания.",
    "DownloadLimitExceededError": "Превышен лимит скачивания.",
    "UPLOAD_FILE_SIZE_LIMIT_EXCEEDED": "Превышен лимит размера загружаемого файла.",
    "UPLOAD_TRAFFIC_LIMIT_EXCEEDED": "Превышен лимит трафика загрузки.",
    "invalid_client": "Сервисное приложение не прошло авторизацию.",
    "invalid_grant": "Сервер отклонил данные для получения токена.",
    "invalid_request": "Запрос получения токена составлен некорректно.",
    "invalid_scope": "Запрошенные права приложения недопустимы.",
    "unauthorized_client": "Приложению не разрешён этот способ получения токена.",
    "unsupported_grant_type": "Способ получения токена не поддерживается.",
    "authorization_pending": "Авторизация пользователя ещё не подтверждена.",
    "bad_verification_code": "Код подтверждения недействителен.",
    "Basic auth required": "Сервер требует авторизацию приложения Basic.",
    "Malformed Authorization header": "Заголовок авторизации имеет неверный формат.",
    "unrecognized_api_error": "Сервер вернул нестандартный код ошибки.",
}
_API_CODE = re.compile(r"(?:[A-Z][A-Za-z0-9]{1,79}Error|[A-Z][A-Z0-9_]{1,79})\Z")
_REQUEST_ID = re.compile(
    r"(?:[a-fA-F0-9]{16,128}|[a-fA-F0-9]{8}(?:-[a-fA-F0-9]{4}){3}"
    r"-[a-fA-F0-9]{12})\Z"
)
_HOST = re.compile(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?\Z")
_BODY_LIMIT = 16384
_SECRET_CODE = re.compile(r"secret|password|bearer|oauth|signature|token|credential", re.I)
_LIMIT_REASONS = frozenset({
    "UPLOAD_FILE_SIZE_LIMIT_EXCEEDED", "UPLOAD_TRAFFIC_LIMIT_EXCEEDED",
})


def diagnostic_fields(value):
    if not isinstance(value, dict):
        return {}
    result = {}
    for key in DIAGNOSTIC_KEYS:
        item = value.get(key)
        if not isinstance(item, str):
            continue
        valid = False
        if key == "error_side":
            valid = item in {"saas", "onprem", "local"}
        elif key == "api_error":
            valid = item in API_MESSAGES or (
                bool(_API_CODE.fullmatch(item)) and not _SECRET_CODE.search(item)
            ) or bool(re.fullmatch(r"[0-9]{1,10}", item))
        elif key == "api_reason":
            valid = item in _LIMIT_REASONS
        elif key == "transport_error":
            valid = item in TRANSPORT_MESSAGES
        elif key == "request_id":
            valid = bool(_REQUEST_ID.fullmatch(item))
        elif key == "request_host":
            valid = bool(_HOST.fullmatch(item))
            if not valid:
                try:
                    address = ipaddress.ip_address(item)
                    valid = (isinstance(address, ipaddress.IPv6Address)
                             and address.scope_id is None)
                except ValueError:
                    pass
        if valid:
            result[key] = item
    return result


def _host(url):
    if not isinstance(url, str):
        return {}
    try:
        hostname = urlsplit(url).hostname
    except ValueError:
        return {}
    return diagnostic_fields({"request_host": hostname.lower() if hostname else ""})


def _error_payload(response, streaming):
    try:
        headers = getattr(response, "headers", {})
        if streaming:
            content_type = headers.get("Content-Type", "").split(";")[0].strip().lower()
            if content_type != "application/json" and not content_type.endswith("+json"):
                return None
            length = headers.get("Content-Length")
            if length is not None and int(length) > _BODY_LIMIT:
                return None
            body = bytearray()
            for chunk in response.iter_content(chunk_size=8192):
                if len(body) + len(chunk) > _BODY_LIMIT:
                    return None
                body.extend(chunk)
            payload = json.loads(body)
        else:
            # requests has already buffered ordinary API responses.
            content = getattr(response, "content", None)
            if isinstance(content, (bytes, str)):
                if len(content) > _BODY_LIMIT:
                    return None
                payload = json.loads(content)
            else:
                payload = response.json()
        return payload if isinstance(payload, dict) else None
    except (ValueError, TypeError, AttributeError, OSError, RecursionError,
            requests.RequestException):
        return None


def response_details(response, side, url=None, *, streaming=False):
    details = {"error_side": side, **_host(url or getattr(response, "url", None))}
    headers = getattr(response, "headers", {})
    for key in ("X-Request-Id", "X-Yandex-Request-Id", "X-Correlation-Id"):
        identifier = headers.get(key)
        if diagnostic_fields({"request_id": identifier}):
            details["request_id"] = identifier
            break
    payload = _error_payload(response, streaming)
    if payload is not None:
        code = payload.get("error")
        if isinstance(code, dict):
            code = code.get("error") or code.get("code")
        code = code or payload.get("reason") or payload.get("code")
        if isinstance(code, int) and not isinstance(code, bool):
            code = str(code)
        if code:
            details["api_error"] = (code if diagnostic_fields({"api_error": code})
                                    else "unrecognized_api_error")
        reason = payload.get("reason")
        if isinstance(reason, str) and reason in _LIMIT_REASONS:
            details["api_reason"] = reason
        extra = payload.get("details")
        if (isinstance(extra, dict) and isinstance(extra.get("reason"), str)
                and extra["reason"] in _LIMIT_REASONS):
            details["api_reason"] = extra["reason"]
    return diagnostic_fields(details)


def request_not_sent(exc):
    """Only a current connection failure proves that no HTTP write was sent."""
    if isinstance(exc, requests.ConnectTimeout):
        return True
    return (
        isinstance(exc, requests.ConnectionError)
        and not isinstance(exc, (requests.exceptions.SSLError, requests.exceptions.ProxyError))
        and len(exc.args) == 1 and isinstance(exc.args[0], MaxRetryError)
        and isinstance(exc.args[0].reason, ConnectTimeoutError)
    )


def transport_details(exc, side, url=None):
    if isinstance(exc, requests.ConnectTimeout):
        cause = "connect_timeout"
    elif isinstance(exc, requests.ReadTimeout):
        cause = "read_timeout"
    elif isinstance(exc, requests.Timeout):
        cause = "timeout"
    elif isinstance(exc, requests.exceptions.SSLError):
        cause = "tls"
    elif isinstance(exc, requests.exceptions.ChunkedEncodingError):
        cause = "stream"
    elif isinstance(exc, requests.exceptions.ContentDecodingError):
        cause = "decode"
    elif isinstance(exc, requests.ConnectionError):
        cause = "connection"
    else:
        cause = "transport"
    pending, seen = [exc], set()
    while pending and len(seen) < 16 and cause in {"connection", "transport", "timeout"}:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, socket.gaierror) or (
            type(current).__module__ == "urllib3.exceptions"
            and type(current).__name__ == "NameResolutionError"
        ):
            cause = "dns"
            break
        if (type(current).__module__ == "urllib3.exceptions"
                and type(current).__name__ == "ReadTimeoutError"):
            cause = "read_timeout"
        elif isinstance(current, RemoteDisconnected) and cause == "connection":
            cause = "remote_closed"
        elif (type(current).__module__ == "urllib3.exceptions"
              and type(current).__name__ == "ConnectTimeoutError"):
            cause = "connect_timeout"
        pending.extend(item for item in (
            getattr(current, "reason", None), *getattr(current, "args", ()),
        ) if isinstance(item, BaseException))
    if request_not_sent(exc):
        if isinstance(exc, requests.ConnectTimeout):
            cause = "connect_timeout"
        else:
            reason = exc.args[0].reason
            if isinstance(reason, NameResolutionError):
                cause = "dns"
            elif isinstance(reason, NewConnectionError):
                cause = "connect_failed" if side == "onprem" else "connection"
            else:
                cause = "connect_timeout"
    return diagnostic_fields({
        "error_side": side, "transport_error": cause, **_host(url),
    })


def describe_diagnostics(payload):
    details = diagnostic_fields(payload)
    side = details.get("error_side")
    error = payload.get("error", "")
    if side is None and isinstance(error, str):
        if error.startswith("source_"):
            side = "saas"
        elif error.startswith("destination_") or error == "remote_operation_failed":
            side = "onprem"
    text = []
    http_status = payload.get("http_status")
    if isinstance(http_status, int) and not isinstance(http_status, bool):
        label = HTTP_MESSAGES.get(http_status, "Сервер вернул этот HTTP-статус.")
        if http_status == 200 and (
            payload.get("remote_status") == "failed"
            or error in {"remote_operation_failed", "destination_url_import_failed"}
        ):
            label = "Статус получен; удалённая операция завершилась ошибкой."
        text.append(f"HTTP {http_status}: {label}")
    api_error = details.get("api_error")
    if api_error:
        label = API_MESSAGES.get(
            api_error, "Пояснение этого кода отсутствует в справочнике утилиты.",
        )
        text.append(f"Код API: {api_error}. {label}")
    reason = details.get("api_reason")
    if reason and reason != api_error:
        text.append(f"Причина API: {reason}. {API_MESSAGES[reason]}")
    transport = details.get("transport_error")
    if transport:
        text.append(TRANSPORT_MESSAGES[transport])
    if details.get("request_host"):
        text.append(f"Сервер: {details['request_host']}.")
    if details.get("request_id"):
        text.append(f"ID запроса: {details['request_id']}.")
    if text and side:
        text.insert(0, {"saas": "SaaS.", "onprem": "On-premises.", "local": "Локальный кеш."}[side])
    return " ".join(text)
