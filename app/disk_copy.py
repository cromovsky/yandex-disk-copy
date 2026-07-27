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

import time
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

    @property
    def _is_shared_dest(self) -> bool:
        return self.cfg.destination_type == "shared"

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

    def _relative_from_source(self, item_path: str) -> str:
        """Относительный путь файла внутри cfg.path (без ведущего /)."""
        base = _strip_disk_schema(self.cfg.path)
        full = _strip_disk_schema(item_path)
        if base != "/" and (full == base or full.startswith(base + "/")):
            rel = full[len(base) :].lstrip("/")
        else:
            rel = full.lstrip("/")
        return rel

    # ── API Диска (личный) ─────────────────────────────────────────────
    def _disk_space_info(self, token: str) -> tuple[int, int]:
        """Возвращает (used_space, free_space) в байтах для личного Диска."""
        url = "https://cloud-api.yandex.net/v1/disk/"
        headers = {"Authorization": f"OAuth {token}"}
        response = self.session.get(url, headers=headers)
        payload = response.json()
        used = int(payload["used_space"])
        total = int(payload["total_space"])
        return used, total - used

    def _shared_space_info(self, token: str) -> tuple[int, int]:
        """Возвращает (used_space, free_space) для общего диска по vd_hash."""
        url = "https://cloud-api.yandex.net/v1/disk/virtual-disks"
        headers = {"Authorization": f"OAuth {token}"}
        params = {"vd_hash": self.cfg.destination_vd_hash}
        response = self.session.get(url, params=params, headers=headers)
        self.log(
            f"shared_space_info | status: {response.status_code} | "
            f"vd_hash: {self.cfg.destination_vd_hash}"
        )
        payload = response.json()
        if response.status_code != 200 or "used_space" not in payload:
            raise CopyError(
                f"Не удалось получить информацию об общем диске "
                f"{self.cfg.destination_vd_hash}: {response.status_code} {payload}"
            )
        used = int(payload["used_space"])
        total = int(payload["total_space"])
        name = payload.get("name", "")
        perms = payload.get("permissions") or []
        self.log(
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

    def _disk_resource(self, token: str, path: str) -> dict:
        """Метаинформация одного ресурса (файл или папка)."""
        url = "https://cloud-api.yandex.net/v1/disk/resources"
        headers = {"Authorization": f"OAuth {token}"}
        params = {
            "path": path,
            "fields": "path,type,name,public_key,public_url",
        }
        response = self.session.get(url, params=params, headers=headers)
        return response.json()

    def _disk_publish_resource(self, token: str, path: str) -> None:
        url = "https://cloud-api.yandex.net/v1/disk/resources/publish"
        headers = {"Authorization": f"OAuth {token}"}
        response = self.session.put(url, params={"path": path}, headers=headers)
        self.log(
            f"disk_publish_resource | status: {response.status_code} | path: {path}"
        )

    def _disk_ensure_folder(self, token: str, path: str) -> None:
        """Создать папку на личном Диске (идемпотентно: 409 = уже существует)."""
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
            "name": name,
            "save_path": self._target_folder_personal(),
        }
        headers = {"Authorization": f"OAuth {self._destination_token()}"}
        response = self.session.post(url, params=params, headers=headers)
        self.log(
            f"disk_save_public_resource | status: {response.status_code} | "
            f"name: {name}"
        )

    def _public_download_href(self, public_key: str) -> str:
        """Прямая ссылка на скачивание опубликованного ресурса."""
        url = "https://cloud-api.yandex.net/v1/disk/public/resources/download"
        response = self.session.get(url, params={"public_key": public_key})
        payload = response.json()
        href = payload.get("href")
        if not href:
            raise CopyError(
                f"Не удалось получить ссылку скачивания для public_key: "
                f"{response.status_code} {payload}"
            )
        return href

    def _wait_operation(self, href: str, token: str, timeout_sec: int = 600) -> None:
        """Ждёт завершения асинхронной операции Диска."""
        headers = {"Authorization": f"OAuth {token}"}
        # Иногда href — шаблон или относительный; берём как есть.
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            response = self.session.get(href, headers=headers)
            payload = response.json() if response.content else {}
            status = payload.get("status")
            self.log(f"operation | status: {status} | {href[:80]}")
            if status == "success":
                return
            if status == "failed":
                raise CopyError(f"Операция Диска завершилась ошибкой: {payload}")
            time.sleep(2)
        raise CopyError(f"Таймаут ожидания операции Диска: {href}")

    # ── API общих дисков (virtual-disks) ───────────────────────────────
    def _vd_ensure_folder(self, token: str, path: str) -> None:
        url = "https://cloud-api.yandex.net/v1/disk/virtual-disks/resources"
        headers = {"Authorization": f"OAuth {token}"}
        response = self.session.put(url, params={"path": path}, headers=headers)
        if response.status_code in (201, 409):
            state = "создана" if response.status_code == 201 else "уже существует"
            self.log(f"vd_ensure_folder | {path} — {state}")
        else:
            raise CopyError(
                f"Не удалось создать папку на общем диске {path}: "
                f"{response.status_code} {response.text[:300]}"
            )

    def _vd_ensure_folder_tree(self, token: str, inner: str) -> None:
        """Создаёт цепочку папок внутри общего диска (inner без vd: префикса)."""
        parts = [p for p in _strip_disk_schema(inner).strip("/").split("/") if p]
        acc: list[str] = []
        for part in parts:
            acc.append(part)
            self._vd_ensure_folder(token, self._vd_path("/".join(acc)))

    def _vd_upload_from_url(self, file_url: str, dest_path: str) -> None:
        """Загрузка файла из интернета на общий диск (async → ждём операцию)."""
        url = "https://cloud-api.yandex.net/v1/disk/virtual-disks/resources/upload"
        headers = {"Authorization": f"OAuth {self._destination_token()}"}
        params = {"url": file_url, "path": dest_path}
        response = self.session.post(url, params=params, headers=headers)
        self.log(
            f"vd_upload_from_url | status: {response.status_code} | path: {dest_path}"
        )
        if response.status_code == 202:
            href = (response.json() or {}).get("href")
            if not href:
                raise CopyError(
                    f"Нет ссылки на операцию при загрузке на общий диск: "
                    f"{response.text[:300]}"
                )
            self._wait_operation(href, self._destination_token())
        elif response.status_code == 201:
            return
        elif response.status_code == 409:
            self.log(f"vd_upload_from_url | уже существует, пропускаю: {dest_path}")
        else:
            raise CopyError(
                f"Не удалось загрузить на общий диск {dest_path}: "
                f"{response.status_code} {response.text[:300]}"
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
        """Сохраняет ресурсы на личный Диск получателя (оригинальные имена)."""
        folder = self._target_folder_personal()
        self.log(f"save_links | start | count: {len(self.links)}, папка: {folder}")
        self._disk_ensure_folder(self._destination_token(), folder)
        for i, link in enumerate(self.links, 1):
            self._disk_save_public_resource(link["public_key"], link["name"])
            self.log(f"save_links | progress {i}/{len(self.links)}")
        self.log("save_links | done")

    def save_to_shared(self) -> None:
        """Публикует файлы источника и заливает их на общий диск (с сохранением дерева)."""
        root_inner = f"/{self.cfg.source_disk_id}"
        root_vd = self._target_folder_shared()
        self.log(
            f"save_to_shared | start | vd_hash: {self.cfg.destination_vd_hash} | "
            f"папка: {root_vd}"
        )
        token_dst = self._destination_token()
        self._vd_ensure_folder_tree(token_dst, root_inner)

        files = self._collect_files_recursive(self.cfg.path)
        self.log(f"save_to_shared | файлов к переносу: {len(files)}")
        if not files:
            self.log("save_to_shared | нечего переносить")
            return

        for i, item in enumerate(files, 1):
            src_path = item["path"]
            name = item.get("name") or src_path.rsplit("/", 1)[-1]
            rel = self._relative_from_source(src_path)
            dest_inner = f"{root_inner}/{rel}" if rel else f"{root_inner}/{name}"
            dest_vd = self._vd_path(dest_inner)

            # родители файла на общем диске
            parent_inner = dest_inner.rsplit("/", 1)[0]
            if parent_inner and parent_inner != root_inner:
                self._vd_ensure_folder_tree(token_dst, parent_inner)

            self._disk_publish_resource(self._source_token(), src_path)
            meta = self._disk_resource(self._source_token(), src_path)
            public_key = meta.get("public_key")
            record = {"path": src_path, "name": name}
            if not isinstance(public_key, str):
                self.log(f"save_to_shared | нет public_key: {src_path}")
                self.fails.append(record)
                continue

            try:
                href = self._public_download_href(public_key)
                self._vd_upload_from_url(href, dest_vd)
                self.links.append({**record, "public_key": public_key, "dest": dest_vd})
            except CopyError as exc:
                self.log(f"save_to_shared | ошибка {src_path}: {exc}")
                self.fails.append({**record, "error": str(exc)})

            self.log(f"save_to_shared | progress {i}/{len(files)}")

        self.log(
            f"save_to_shared | done | saved: {len(self.links)}, fails: {len(self.fails)}"
        )

    # ── оркестрация ────────────────────────────────────────────────────
    def run(self) -> dict:
        cfg = self.cfg
        if self._is_shared_dest and not cfg.destination_vd_hash:
            raise CopyError(
                "Для переноса на общий диск укажите destination_vd_hash "
                "(метка из адресной строки Диска после vd/)."
            )

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

        try:
            # 3. Проверка места на диске назначения.
            needed_space, _ = self._disk_space_info(self._source_token())
            if self._is_shared_dest:
                _, free_space = self._shared_space_info(self._destination_token())
            else:
                _, free_space = self._disk_space_info(self._destination_token())
            self.log(
                f"space check | need: {needed_space} bytes, free: {free_space} bytes"
            )

            if free_space <= needed_space:
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
        finally:
            # 5. Возвращаем исходную блокировку, что бы ни случилось.
            if ban_need:
                self._api360_patch_user(source_uid, False)

        self.log(
            f"COMPLETED | saved: {len(self.links)}, fails: {len(self.fails)}"
        )
        return {"saved": self.links, "fails": self.fails}
