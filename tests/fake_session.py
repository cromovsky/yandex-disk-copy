"""Подмена requests.Session для тестов DiskCopier — без сетевых вызовов.

Роутинг по (метод, подстрока URL): для каждого совпадения отдаём заранее
заготовленный ответ. Все вызовы пишутся в self.calls, чтобы тест мог проверить
не только результат, но и что именно ушло в API (заголовки, params, порядок).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Optional


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        payload: Optional[dict] = None,
        text: Optional[str] = None,
    ):
        self.status_code = status_code
        self._payload = payload
        if text is not None:
            self.text = text
        elif payload is not None:
            self.text = json.dumps(payload, ensure_ascii=False)
        else:
            self.text = ""
        self.content = self.text.encode("utf-8")

    def json(self) -> Any:
        if self._payload is None:
            raise ValueError("ответ без JSON-тела")
        return self._payload


@dataclass
class Call:
    method: str
    url: str
    params: Optional[dict] = None
    headers: Optional[dict] = None
    json_body: Optional[dict] = None
    data: Optional[dict] = None


@dataclass
class FakeSession:
    """routes: {(метод, подстрока url): FakeResponse | список ответов подряд}."""

    routes: dict = field(default_factory=dict)
    calls: list = field(default_factory=list)
    closed: bool = False

    def _respond(self, method: str, url: str, **kw) -> FakeResponse:
        self.calls.append(
            Call(
                method=method,
                url=url,
                params=kw.get("params"),
                headers=kw.get("headers"),
                json_body=kw.get("json"),
                data=kw.get("data"),
            )
        )
        for (route_method, fragment), response in self.routes.items():
            if route_method != method or fragment not in url:
                continue
            if isinstance(response, list):
                # последний ответ залипает — удобно для polling-эндпоинтов
                return response.pop(0) if len(response) > 1 else response[0]
            return response
        raise AssertionError(f"нет заготовленного ответа для {method} {url}")

    def get(self, url, **kw):
        return self._respond("GET", url, **kw)

    def post(self, url, **kw):
        return self._respond("POST", url, **kw)

    def put(self, url, **kw):
        return self._respond("PUT", url, **kw)

    def patch(self, url, **kw):
        return self._respond("PATCH", url, **kw)

    def close(self):
        self.closed = True

    # ── помощники для проверок ──────────────────────────────────────────
    def calls_to(self, fragment: str, method: Optional[str] = None) -> list:
        return [
            c
            for c in self.calls
            if fragment in c.url and (method is None or c.method == method)
        ]

    def auth_tokens_for(self, fragment: str) -> list:
        return [
            (c.headers or {}).get("Authorization") for c in self.calls_to(fragment)
        ]
