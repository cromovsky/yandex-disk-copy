"""Копирование файлов с одного Яндекс.Диска на другой.

Рефакторинг исходного скрипта: вместо глобальных переменных и записи логов в
файл — класс DiskCopier, который параметризуется конфигом и шлёт строки лога
через callback (его web-слой стримит в браузер по SSE).

Используются:
- сервисные приложения Яндекс 360 (token-exchange по email сотрудника)
- API Диска (cloud-api.yandex.net)
- API360 для блокировки/разблокировки пользователя
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Optional

import requests
from requests.adapters import HTTPAdapter, Retry


def _build_session() -> requests.Session:
    """Сессия с ретраями на сетевые/серверные ошибки."""
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


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


@dataclass
class _DiskAuth:
    token: str = ""
    expiry: datetime = field(default_factory=datetime.now)


class CopyError(Exception):
    """Прерывание процесса с человекочитаемой причиной."""


LogCallback = Callable[[str], None]


class DiskCopier:
    def __init__(self, config: CopyConfig, log: Optional[LogCallback] = None):
        self.cfg = config
        self._log = log or (lambda _msg: None)
        self.session = _build_session()
        self.source = _DiskAuth()
        self.destination = _DiskAuth()
        self.links: list[dict] = []
        self.fails: list[dict] = []

    # ── логирование ────────────────────────────────────────────────────
    def log(self, message: str) -> None:
        self._log(f"{datetime.now():%Y-%m-%d %H:%M:%S} | {message}")

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
        response = self.session.post(url, data=data, headers=headers)
        self.log(f"get_token | user: {disk_id}, status: {response.status_code}")
        payload = response.json()
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
            raise CopyError(
                f"Не удалось получить токен для {disk_id}: "
                f"{response.status_code} {payload}{hint}"
            )
        ttl = int(payload["expires_in"]) - 100
        return _DiskAuth(
            token=payload["access_token"],
            expiry=datetime.now() + timedelta(seconds=ttl),
        )

    def _source_token(self) -> str:
        """Токен исходного диска, с авто-обновлением по истечении."""
        if datetime.now() >= self.source.expiry or not self.source.token:
            self.log("source token expired/empty — refreshing")
            self.source = self._get_token(self.cfg.source, self.cfg.source_disk_id)
        return self.source.token

    def _destination_token(self) -> str:
        if datetime.now() >= self.destination.expiry or not self.destination.token:
            self.log("destination token expired/empty — refreshing")
            self.destination = self._get_token(
                self.cfg.destination, self.cfg.destination_disk_id
            )
        return self.destination.token

    # ── API360: блокировка/разблокировка ───────────────────────────────
    def _api360_get_ban_status(self, user_id: str) -> Optional[bool]:
        url = (
            f"https://api360.yandex.net/directory/v1/org/"
            f"{self.cfg.source.orgid}/users/{user_id}"
        )
        headers = {"Authorization": f"OAuth {self.cfg.source.admin_token}"}
        response = self.session.get(url, headers=headers)
        is_active = response.json().get("isEnabled")
        self.log(f"api360 ban status | {user_id} isEnabled: {is_active}")
        return is_active

    def _api360_patch_user(self, user_id: str, status: bool) -> None:
        url = (
            f"https://api360.yandex.net/directory/v1/org/"
            f"{self.cfg.source.orgid}/users/{user_id}"
        )
        headers = {"Authorization": f"OAuth {self.cfg.source.admin_token}"}
        response = self.session.patch(url, json={"isEnabled": status}, headers=headers)
        self.log(
            f"api360 patch user | {user_id} isEnabled -> {status}, "
            f"status: {response.status_code}"
        )

    # ── API Диска ──────────────────────────────────────────────────────
    def _disk_space_info(self, token: str) -> tuple[int, int]:
        """Возвращает (used_space, free_space) в байтах."""
        url = "https://cloud-api.yandex.net/v1/disk/"
        headers = {"Authorization": f"OAuth {token}"}
        response = self.session.get(url, headers=headers)
        payload = response.json()
        used = int(payload["used_space"])
        total = int(payload["total_space"])
        return used, total - used

    def _disk_get_meta(self, token: str, limit: int, offset: int, path: str) -> dict:
        url = "https://cloud-api.yandex.net/v1/disk/resources"
        headers = {"Authorization": f"OAuth {token}"}
        params = {
            "path": path,
            "fields": (
                "path,type,_embedded.total,_embedded.items.path,"
                "_embedded.items.type,_embedded.items.public_key,"
                "_embedded.items.name"
            ),
            "limit": limit,
            "offset": offset,
            "sort": "path",
        }
        response = self.session.get(url, params=params, headers=headers)
        self.log(
            f"disk_get_meta | status: {response.status_code} | "
            f"path: {path}, limit: {limit}, offset: {offset}"
        )
        return response.json()

    def _disk_publish_resource(self, token: str, path: str) -> None:
        url = "https://cloud-api.yandex.net/v1/disk/resources/publish"
        headers = {"Authorization": f"OAuth {token}"}
        response = self.session.put(url, params={"path": path}, headers=headers)
        self.log(
            f"disk_publish_resource | status: {response.status_code} | path: {path}"
        )

    def _target_folder(self) -> str:
        """Папка на Диске получателя, куда складываем перенос (по email источника)."""
        return f"disk:/{self.cfg.source_disk_id}"

    def _disk_ensure_folder(self, token: str, path: str) -> None:
        """Создать папку на Диске (идемпотентно: 409 = уже существует)."""
        url = "https://cloud-api.yandex.net/v1/disk/resources"
        headers = {"Authorization": f"OAuth {token}"}
        response = self.session.put(url, params={"path": path}, headers=headers)
        if response.status_code in (201, 409):
            state = "создана" if response.status_code == 201 else "уже существует"
            self.log(f"ensure_folder | {path} — {state}")
        else:
            self.log(
                f"ensure_folder | {path} — статус {response.status_code}: "
                f"{response.text[:200]}"
            )

    def _disk_save_public_resource(self, public_key: str, name: str) -> None:
        url = "https://cloud-api.yandex.net/v1/disk/public/resources/save-to-disk"
        params = {
            "public_key": public_key,
            "name": name,                        # оригинальное имя, без префикса
            "save_path": self._target_folder(),  # в отдельную папку получателя
        }
        headers = {"Authorization": f"OAuth {self._destination_token()}"}
        response = self.session.post(url, params=params, headers=headers)
        self.log(
            f"disk_save_public_resource | status: {response.status_code} | "
            f"name: {name}"
        )

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
                self.log(f"No such resource: {self.cfg.source_disk_id} | {path}")
                return
            total = emb.get("total", 0)
            items = emb.get("items", [])
            pages = total // self.cfg.page_limit + 1
            page = offset // self.cfg.page_limit + 1
            self.log(f"walk | path: {path}, page: {page}/{pages}, items: {len(items)}")
            for item in items:
                handler(item)
            if page >= pages:
                return
            offset = page * self.cfg.page_limit

    def make_links(self) -> None:
        """Публикует все ресурсы в исходной папке (создаёт публичные ссылки)."""
        self.log(f"make_links | start | path: {self.cfg.path}")
        self._walk(
            self.cfg.path,
            lambda item: self._disk_publish_resource(
                self._source_token(), item["path"]
            ),
        )
        self.log("make_links | done")

    def get_links(self) -> None:
        """Собирает public_key опубликованных ресурсов; что без ключа — в fails."""
        self.log(f"get_links | start | path: {self.cfg.path}")

        def collect(item: dict) -> None:
            public_key = item.get("public_key")
            record = {"path": item.get("path"), "name": item.get("name")}
            if isinstance(public_key, str):
                self.links.append({**record, "public_key": public_key})
            else:
                self.fails.append(record)

        self._walk(self.cfg.path, collect)
        self.log(
            f"get_links | done | links: {len(self.links)}, fails: {len(self.fails)}"
        )

    def save_links(self) -> None:
        """Сохраняет ресурсы на Диск получателя в отдельную папку (оригинальные имена)."""
        folder = self._target_folder()
        self.log(f"save_links | start | count: {len(self.links)}, папка: {folder}")
        self._disk_ensure_folder(self._destination_token(), folder)
        for i, link in enumerate(self.links, 1):
            self._disk_save_public_resource(link["public_key"], link["name"])
            self.log(f"save_links | progress {i}/{len(self.links)}")
        self.log("save_links | done")

    # ── оркестрация ────────────────────────────────────────────────────
    def run(self) -> dict:
        cfg = self.cfg
        self.log(
            f"START | copy from {cfg.source_disk_id} to {cfg.destination_disk_id} | "
            f"path: {cfg.path}"
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

        try:
            # 3. Проверка места на диске назначения.
            needed_space, _ = self._disk_space_info(self._source_token())
            _, free_space = self._disk_space_info(self._destination_token())
            self.log(
                f"space check | need: {needed_space} bytes, free: {free_space} bytes"
            )

            if free_space <= needed_space:
                raise CopyError(
                    f"Недостаточно места на диске {cfg.destination_disk_id}: "
                    f"нужно {needed_space}, свободно {free_space}"
                )

            # 4. Публикация → сбор ссылок → сохранение на диск назначения.
            self.make_links()
            self.get_links()
            self.save_links()
        finally:
            # 5. Возвращаем исходную блокировку, что бы ни случилось.
            if ban_need:
                self._api360_patch_user(source_uid, False)

        self.log(
            f"COMPLETED | saved: {len(self.links)}, fails: {len(self.fails)}"
        )
        return {"saved": self.links, "fails": self.fails}
