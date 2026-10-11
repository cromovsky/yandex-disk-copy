"""Тесты DiskCopier: обработка ошибок API, пагинация, ротация токенов.

Сеть не используется — session подменяется FakeSession. Внимание на порядок
ключей в routes: матчинг идёт по подстроке URL в порядке вставки, поэтому
специфичные фрагменты (".../resources/publish") добавляем ДО общих (".../resources").
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
import requests

from app.disk_copy import (
    POLL_FIRST_DELAY,
    POLL_MAX_DELAY,
    CopyConfig,
    CopyError,
    DiskCopier,
    OrgCreds,
    _build_session,
    _DiskAuth,
)
from app.throttle import get_limiter

from .fake_clock import FakeClock
from .fake_session import FakeResponse, FakeSession

TOKEN_URL = "oauth.yandex.ru/token"
SPACE_URL = "yandex.net/v1/disk/"
RESOURCES_URL = "/v1/disk/resources"
PUBLISH_URL = "/v1/disk/resources/publish"
SAVE_TO_DISK_URL = "/v1/disk/public/resources/save-to-disk"
PUBLIC_DOWNLOAD_URL = "/v1/disk/public/resources/download"
DOWNLOAD_HREF_URL = "/v1/disk/resources/download"
UPLOAD_HREF_URL = "/v1/disk/resources/upload"
VD_RESOURCES_URL = "/v1/disk/virtual-disks/resources"
API360_USER_URL = "api360.yandex.net/directory/v1/org"


def make_copier(
    session: FakeSession,
    *,
    dest_type: str = "personal",
    vd_hash: str = "",
    path: str = "/",
    page_limit: int = 1000,
    admin_token: str = "admin-tok",
    source_token: str = "t.uid-42.sig",
    dest_expiry: datetime | None = None,
    clock: FakeClock | None = None,
) -> DiskCopier:
    cfg = CopyConfig(
        source=OrgCreds(
            client_id="ci",
            client_secret="cs",
            orgid="org-1",
            admin_token=admin_token,
        ),
        destination=OrgCreds(client_id="ci-dst", client_secret="cs-dst"),
        source_disk_id="src@company.ru",
        destination_disk_id="dst@company.ru",
        path=path,
        page_limit=page_limit,
        destination_type=dest_type,
        destination_vd_hash=vd_hash,
    )
    clock = clock or FakeClock()
    copier = DiskCopier(
        cfg, log=lambda _m: None, monotonic=clock.monotonic, sleep=clock.sleep
    )
    copier.session = session
    far = datetime.now() + timedelta(hours=1)
    copier.source = _DiskAuth(token=source_token, expiry=far)
    copier.destination = _DiskAuth(token="dst-token", expiry=dest_expiry or far)
    return copier


def embedded(items: list[dict], total: int | None = None) -> dict:
    return {
        "_embedded": {
            "total": total if total is not None else len(items),
            "items": items,
        }
    }


def file_item(name: str) -> dict:
    return {"path": f"disk:/{name}", "type": "file", "name": name}


# ── сохранение на личный Диск: ошибки не должны быть молчаливыми ─────────
def test_save_public_resource_raises_on_server_error():
    session = FakeSession(
        {("POST", SAVE_TO_DISK_URL): FakeResponse(503, text="upstream")}
    )
    copier = make_copier(session)

    with pytest.raises(CopyError, match="Не удалось сохранить"):
        copier._disk_save_public_resource("pk-1", "file.txt")


def test_save_public_resource_ok_on_201():
    session = FakeSession({("POST", SAVE_TO_DISK_URL): FakeResponse(201)})
    copier = make_copier(session)

    copier._disk_save_public_resource("pk-1", "file.txt")  # не бросает


def test_save_public_resource_waits_for_async_operation():
    operation_url = "https://cloud-api.yandex.net/v1/disk/operations/op1"
    session = FakeSession(
        {
            ("POST", SAVE_TO_DISK_URL): FakeResponse(
                202, payload={"href": operation_url}
            ),
            ("GET", "/v1/disk/operations/"): FakeResponse(
                200, payload={"status": "success"}
            ),
        }
    )
    copier = make_copier(session)

    copier._disk_save_public_resource("pk-1", "file.txt")

    polled = session.calls_to("/v1/disk/operations/", "GET")
    assert polled, "асинхронная операция должна опрашиваться до успеха"


def test_save_public_resource_raises_when_202_without_href():
    session = FakeSession({("POST", SAVE_TO_DISK_URL): FakeResponse(202, payload={})})
    copier = make_copier(session)

    with pytest.raises(CopyError, match="Нет ссылки на операцию"):
        copier._disk_save_public_resource("pk-1", "file.txt")


def test_save_links_moves_failed_file_to_fails():
    """Главная регрессия: упавший файл не должен попасть в saved."""
    session = FakeSession(
        {
            ("PUT", RESOURCES_URL): FakeResponse(201),
            ("POST", SAVE_TO_DISK_URL): [
                FakeResponse(201),
                FakeResponse(500, text="boom"),
            ],
        }
    )
    copier = make_copier(session)
    copier.links = [
        {"path": "disk:/a.txt", "name": "a.txt", "public_key": "pk-a"},
        {"path": "disk:/b.txt", "name": "b.txt", "public_key": "pk-b"},
    ]

    copier.save_links()

    assert [link["name"] for link in copier.links] == ["a.txt"]
    assert len(copier.fails) == 1
    assert copier.fails[0]["name"] == "b.txt"
    assert "500" in copier.fails[0]["error"]


def test_save_links_keeps_all_on_success():
    session = FakeSession(
        {
            ("PUT", RESOURCES_URL): FakeResponse(201),
            ("POST", SAVE_TO_DISK_URL): FakeResponse(201),
        }
    )
    copier = make_copier(session)
    copier.links = [{"path": "disk:/a.txt", "name": "a.txt", "public_key": "pk-a"}]

    copier.save_links()

    assert len(copier.links) == 1
    assert copier.fails == []


# ── API360: статус ответа нельзя игнорировать ───────────────────────────
def test_api360_get_ban_status_raises_on_forbidden():
    session = FakeSession(
        {("GET", API360_USER_URL): FakeResponse(403, text="no rights")}
    )
    copier = make_copier(session)

    with pytest.raises(CopyError, match="ADMIN_TOKEN"):
        copier._api360_get_ban_status("uid-42")


def test_api360_patch_user_raises_on_forbidden():
    session = FakeSession(
        {("PATCH", API360_USER_URL): FakeResponse(403, text="no rights")}
    )
    copier = make_copier(session)

    with pytest.raises(CopyError, match="Не удалось сменить статус"):
        copier._api360_patch_user("uid-42", False)


def _routes_for_empty_personal_run(patch_responses: list[FakeResponse]) -> dict:
    """Пустая исходная папка: перенос доходит до конца, файлов нет."""
    return {
        ("GET", API360_USER_URL): FakeResponse(200, payload={"isEnabled": False}),
        ("PATCH", API360_USER_URL): patch_responses,
        ("GET", RESOURCES_URL): FakeResponse(200, payload={}),  # нет _embedded
        ("GET", SPACE_URL): FakeResponse(
            200, payload={"used_space": 10, "total_space": 1000}
        ),
        ("PUT", RESOURCES_URL): FakeResponse(201),
    }


def test_run_raises_when_ban_restore_fails_after_successful_transfer():
    session = FakeSession(
        _routes_for_empty_personal_run(
            [FakeResponse(200), FakeResponse(403, text="denied")]
        )
    )
    copier = make_copier(session)

    with pytest.raises(CopyError, match="Не удалось сменить статус"):
        copier.run()

    patches = session.calls_to(API360_USER_URL, "PATCH")
    assert [c.json_body for c in patches] == [{"isEnabled": True}, {"isEnabled": False}]


def test_run_keeps_transfer_error_when_restore_also_fails(monkeypatch):
    """Ошибка переноса важнее ошибки откатa — она и должна всплыть."""
    monkeypatch.setattr("app.disk_copy.SKIP_SPACE_CHECK", False)
    routes = _routes_for_empty_personal_run(
        [FakeResponse(200), FakeResponse(403, text="denied")]
    )
    # места на диске назначения нет → перенос падает до сохранения
    routes[("GET", SPACE_URL)] = FakeResponse(
        200, payload={"used_space": 900, "total_space": 1000}
    )
    session = FakeSession(routes)
    copier = make_copier(session)

    with pytest.raises(CopyError, match="Недостаточно места"):
        copier.run()


def test_run_restores_ban_after_successful_transfer():
    session = FakeSession(
        _routes_for_empty_personal_run([FakeResponse(200), FakeResponse(200)])
    )
    copier = make_copier(session)

    result = copier.run()

    assert result == {"saved": [], "fails": []}
    assert len(session.calls_to(API360_USER_URL, "PATCH")) == 2


def test_disk_space_info_raises_when_api_omits_used_space():
    """Раньше это был сырой KeyError: 'used_space' — лог без тела ответа API."""
    session = FakeSession(
        {
            ("GET", SPACE_URL): FakeResponse(
                403, payload={"error": "ForbiddenError", "message": "Forbidden"}
            )
        }
    )
    copier = make_copier(session)

    with pytest.raises(CopyError, match="Не удалось получить информацию о Диске"):
        copier._disk_space_info("tok", disk_id="src@company.ru")


def test_disk_space_info_403_hints_at_missing_scope():
    session = FakeSession(
        {("GET", SPACE_URL): FakeResponse(403, payload={"error": "ForbiddenError"})}
    )
    copier = make_copier(session)

    with pytest.raises(CopyError, match="cloud_api:disk.info"):
        copier._disk_space_info("tok", disk_id="src@company.ru")


# ── 404 по публичной ссылке лечится переизданием ────────────────────────
def test_save_links_copies_bytes_when_public_link_is_dead():
    """Боевой случай: public_key отвечает 404 и на save-to-disk, и на скачивание."""
    session = FakeSession(
        {
            ("PUT", RESOURCES_URL): FakeResponse(201),  # ensure_folder
            ("POST", SAVE_TO_DISK_URL): FakeResponse(
                404, text='{"error":"DiskNotFoundError"}'
            ),
            ("GET", DOWNLOAD_HREF_URL): FakeResponse(
                200, payload={"href": "https://downloader/HR.xlsx"}
            ),
            ("GET", "https://downloader/HR.xlsx"): FakeResponse(200, text="xlsx"),
            ("GET", UPLOAD_HREF_URL): FakeResponse(
                200, payload={"href": "https://uploader/put", "method": "PUT"}
            ),
            ("PUT", "https://uploader/put"): FakeResponse(201),
        }
    )
    copier = make_copier(session)
    copier.links = [
        {
            "path": "disk:/HR.xlsx",
            "name": "HR.xlsx",
            "type": "file",
            "public_key": "pk",
        }
    ]

    copier.save_links()

    assert copier.fails == []
    assert len(copier.links) == 1
    download = session.calls_to(DOWNLOAD_HREF_URL, "GET")[0]
    assert download.params["path"] == "disk:/HR.xlsx"
    upload_href = session.calls_to(UPLOAD_HREF_URL, "GET")[0]
    assert upload_href.params["path"] == "disk:/src@company.ru/HR.xlsx"
    put = session.calls_to("https://uploader/put", "PUT")[0]
    assert put.data == b"xlsx"


def test_save_links_does_not_use_direct_upload_for_folders():
    session = FakeSession(
        {
            ("PUT", RESOURCES_URL): FakeResponse(201),
            ("POST", SAVE_TO_DISK_URL): FakeResponse(
                404, text='{"error":"DiskNotFoundError"}'
            ),
        }
    )
    copier = make_copier(session)
    copier.links = [
        {"path": "disk:/Док", "name": "Док", "type": "dir", "public_key": "pk"}
    ]

    copier.save_links()

    assert copier.links == []
    assert "вручную" in copier.fails[0]["error"]
    assert session.calls_to(DOWNLOAD_HREF_URL, "GET") == []


def test_save_links_does_not_fall_back_on_non_404():
    session = FakeSession(
        {
            ("PUT", RESOURCES_URL): FakeResponse(201),
            ("POST", SAVE_TO_DISK_URL): FakeResponse(503, text="upstream"),
        }
    )
    copier = make_copier(session)
    copier.links = [
        {"path": "disk:/a.txt", "name": "a.txt", "type": "file", "public_key": "pk"}
    ]

    copier.save_links()

    assert len(copier.fails) == 1
    assert session.calls_to(DOWNLOAD_HREF_URL, "GET") == []


# ── непереносённые файлы должны быть видны поимённо ─────────────────────
def test_get_links_records_reason_for_file_without_public_key():
    items = [{**file_item("ok.txt"), "public_key": "pk-1"}, file_item("bad.txt")]
    session = FakeSession(
        {("GET", RESOURCES_URL): FakeResponse(200, payload=embedded(items))}
    )
    lines: list[str] = []
    copier = make_copier(session)
    copier._log = lines.append

    copier.get_links()

    assert [link["name"] for link in copier.links] == ["ok.txt"]
    assert len(copier.fails) == 1
    assert copier.fails[0]["error"]
    assert any("пропуск" in line and "bad.txt" in line for line in lines)


def test_log_fails_lists_each_failed_file():
    copier = make_copier(FakeSession({}))
    lines: list[str] = []
    copier._log = lines.append
    copier.fails = [
        {"path": "disk:/a.txt", "error": "boom"},
        {"path": "disk:/b.txt", "error": "bang"},
    ]

    copier._log_fails()

    joined = "\n".join(lines)
    assert "NOT COPIED" in joined
    assert "disk:/a.txt — boom" in joined
    assert "disk:/b.txt — bang" in joined


def test_log_fails_caps_long_list():
    copier = make_copier(FakeSession({}))
    lines: list[str] = []
    copier._log = lines.append
    copier.fails = [{"path": f"disk:/{i}.txt", "error": "e"} for i in range(70)]

    copier._log_fails()

    listed = [ln for ln in lines if ".txt — e" in ln]
    assert len(listed) == 50
    assert any("ещё 20" in ln for ln in lines)


# ── 403 не должен выглядеть как успешный перенос ────────────────────────
def test_walk_raises_on_forbidden_instead_of_reporting_empty_folder():
    """403 на чтении раньше логировался как «No such resource» → COMPLETED 0/0."""
    session = FakeSession(
        {("GET", RESOURCES_URL): FakeResponse(403, payload={"error": "ForbiddenError"})}
    )
    copier = make_copier(session)

    with pytest.raises(CopyError, match="Не удалось прочитать"):
        copier._walk("/", lambda _item: None)


def test_walk_treats_404_as_missing_folder():
    """404 — это «нет такой папки», перенос не падает."""
    session = FakeSession(
        {("GET", RESOURCES_URL): FakeResponse(404, payload={"error": "NotFound"})}
    )
    copier = make_copier(session)
    seen: list[dict] = []

    copier._walk("/nope", seen.append)

    assert seen == []


def test_ensure_folder_raises_on_forbidden():
    session = FakeSession(
        {("PUT", RESOURCES_URL): FakeResponse(403, text='{"error":"ForbiddenError"}')}
    )
    copier = make_copier(session)

    with pytest.raises(CopyError, match="Не удалось создать папку"):
        copier._disk_ensure_folder("tok", "disk:/src@company.ru")


def test_run_fails_loudly_when_disk_access_is_denied():
    """Сквозной сценарий из боевого лога: 403 на всём — статус обязан быть ошибкой."""
    routes = _routes_for_empty_personal_run([FakeResponse(200), FakeResponse(200)])
    routes[("GET", RESOURCES_URL)] = FakeResponse(
        403, payload={"error": "ForbiddenError"}
    )
    session = FakeSession(routes)
    copier = make_copier(session)

    with pytest.raises(CopyError, match="Не удалось прочитать"):
        copier.run()


def test_run_skips_space_check_by_default():
    """По умолчанию 403 на /v1/disk не останавливает перенос."""
    routes = _routes_for_empty_personal_run([FakeResponse(200), FakeResponse(200)])
    routes[("GET", SPACE_URL)] = FakeResponse(403, payload={"error": "ForbiddenError"})
    session = FakeSession(routes)
    copier = make_copier(session)

    assert copier.run() == {"saved": [], "fails": []}
    # SPACE_URL — подстрока и для /v1/disk/resources, поэтому сверяем точный эндпоинт
    assert [c for c in session.calls if c.url.endswith("/v1/disk/")] == []


def test_run_skips_ban_handling_without_admin_token():
    routes = _routes_for_empty_personal_run([FakeResponse(200)])
    session = FakeSession(routes)
    copier = make_copier(session, admin_token="")

    copier.run()

    assert session.calls_to(API360_USER_URL) == []


# ── пагинация ───────────────────────────────────────────────────────────
def test_walk_makes_no_extra_request_when_total_is_multiple_of_limit():
    items = [file_item("1.txt"), file_item("2.txt")]
    session = FakeSession(
        {("GET", RESOURCES_URL): FakeResponse(200, payload=embedded(items, 2))}
    )
    copier = make_copier(session, page_limit=2)
    seen: list[dict] = []

    copier._walk("disk:/", seen.append)

    assert len(seen) == 2
    assert len(session.calls_to(RESOURCES_URL, "GET")) == 1


def test_walk_paginates_when_total_exceeds_limit():
    page1 = [file_item("1.txt"), file_item("2.txt")]
    page2 = [file_item("3.txt")]
    session = FakeSession(
        {
            ("GET", RESOURCES_URL): [
                FakeResponse(200, payload=embedded(page1, 3)),
                FakeResponse(200, payload=embedded(page2, 3)),
            ]
        }
    )
    copier = make_copier(session, page_limit=2)
    seen: list[dict] = []

    copier._walk("disk:/", seen.append)

    assert len(seen) == 3
    calls = session.calls_to(RESOURCES_URL, "GET")
    assert len(calls) == 2
    assert [c.params["offset"] for c in calls] == [0, 2]


def test_walk_stops_on_missing_resource():
    session = FakeSession({("GET", RESOURCES_URL): FakeResponse(200, payload={})})
    copier = make_copier(session)
    seen: list[dict] = []

    copier._walk("disk:/nope", seen.append)

    assert seen == []


# ── ротация токена назначения ───────────────────────────────────────────
def test_vd_ensure_folder_refreshes_expired_destination_token():
    session = FakeSession(
        {
            ("POST", TOKEN_URL): FakeResponse(
                200, payload={"access_token": "fresh-token", "expires_in": 3600}
            ),
            ("PUT", VD_RESOURCES_URL): FakeResponse(201),
        }
    )
    copier = make_copier(
        session,
        dest_type="shared",
        vd_hash="hash1",
        dest_expiry=datetime.now() - timedelta(minutes=5),
    )

    copier._vd_ensure_folder("vd:hash1:disk:/folder")

    assert session.auth_tokens_for(VD_RESOURCES_URL) == ["OAuth fresh-token"]


def test_vd_ensure_folder_raises_on_error_status():
    session = FakeSession({("PUT", VD_RESOURCES_URL): FakeResponse(507, text="quota")})
    copier = make_copier(session, dest_type="shared", vd_hash="hash1")

    with pytest.raises(CopyError, match="Не удалось создать папку"):
        copier._vd_ensure_folder("vd:hash1:disk:/folder")


def test_vd_ensure_folder_tree_creates_each_level():
    session = FakeSession({("PUT", VD_RESOURCES_URL): FakeResponse(201)})
    copier = make_copier(session, dest_type="shared", vd_hash="hash1")

    copier._vd_ensure_folder_tree("/src@company.ru/docs/2026")

    created = [c.params["path"] for c in session.calls_to(VD_RESOURCES_URL, "PUT")]
    assert created == [
        "vd:hash1:disk:/src@company.ru",
        "vd:hash1:disk:/src@company.ru/docs",
        "vd:hash1:disk:/src@company.ru/docs/2026",
    ]


# ── сокращение числа запросов ───────────────────────────────────────────
def _shared_routes(files: list[dict]) -> dict:
    """Роуты для успешного shared-переноса перечисленных файлов."""
    return {
        ("PUT", VD_RESOURCES_URL): FakeResponse(201),
        ("GET", RESOURCES_URL): FakeResponse(200, payload=embedded(files)),
        ("PUT", PUBLISH_URL): FakeResponse(200),
        ("GET", PUBLIC_DOWNLOAD_URL): FakeResponse(
            200, payload={"href": "https://downloader.disk.yandex.net/file"}
        ),
        ("POST", "/v1/disk/virtual-disks/resources/upload"): FakeResponse(201),
    }


def test_save_to_shared_creates_each_folder_once():
    """Регресс: раньше цепочка родителей пересоздавалась на каждый файл."""
    files = [
        {"path": "disk:/docs/2026/a.txt", "type": "file", "name": "a.txt"},
        {"path": "disk:/docs/2026/b.txt", "type": "file", "name": "b.txt"},
        {"path": "disk:/docs/2026/c.txt", "type": "file", "name": "c.txt"},
    ]
    routes = _shared_routes(files)
    # meta для publish/public_key
    routes[("GET", RESOURCES_URL)] = [
        FakeResponse(200, payload=embedded(files)),
        FakeResponse(200, payload={"public_key": "pk", "type": "file"}),
    ]
    session = FakeSession(routes)
    copier = make_copier(session, dest_type="shared", vd_hash="h1", path="disk:/")

    copier.save_to_shared()

    created = [c.params["path"] for c in session.calls_to(VD_RESOURCES_URL, "PUT")]
    # /src@company.ru + /docs + /2026 — по одному разу, а не по разу на файл
    assert len(created) == len(set(created))
    assert len(created) == 3


def test_save_to_shared_creates_parents_top_down():
    files = [{"path": "disk:/x/y/z/f.txt", "type": "file", "name": "f.txt"}]
    routes = _shared_routes(files)
    routes[("GET", RESOURCES_URL)] = [
        FakeResponse(200, payload=embedded(files)),
        FakeResponse(200, payload={"public_key": "pk", "type": "file"}),
    ]
    session = FakeSession(routes)
    copier = make_copier(session, dest_type="shared", vd_hash="h1", path="disk:/")

    copier.save_to_shared()

    created = [c.params["path"] for c in session.calls_to(VD_RESOURCES_URL, "PUT")]
    depths = [p.count("/") for p in created]
    assert depths == sorted(depths), "родитель должен создаваться раньше ребёнка"


def test_vd_ensure_folder_tree_memoizes_across_calls():
    session = FakeSession({("PUT", VD_RESOURCES_URL): FakeResponse(201)})
    copier = make_copier(session, dest_type="shared", vd_hash="h1")

    copier._vd_ensure_folder_tree("/a/b")
    copier._vd_ensure_folder_tree("/a/b")
    copier._vd_ensure_folder_tree("/a/b/c")

    created = [c.params["path"] for c in session.calls_to(VD_RESOURCES_URL, "PUT")]
    assert created == [
        "vd:h1:disk:/a",
        "vd:h1:disk:/a/b",
        "vd:h1:disk:/a/b/c",
    ]


def test_vd_ensure_folder_tree_does_not_memoize_failures():
    """Упавшую папку нельзя считать созданной — иначе файлы поедут в пустоту."""
    session = FakeSession(
        {("PUT", VD_RESOURCES_URL): [FakeResponse(500, text="boom"), FakeResponse(201)]}
    )
    copier = make_copier(session, dest_type="shared", vd_hash="h1")

    with pytest.raises(CopyError):
        copier._vd_ensure_folder_tree("/a")
    copier._vd_ensure_folder_tree("/a")  # вторая попытка должна снова стучать

    assert len(session.calls_to(VD_RESOURCES_URL, "PUT")) == 2


# ── опрос асинхронных операций ──────────────────────────────────────────
OPERATION_URL = "https://cloud-api.yandex.net/v1/disk/operations/op-1"


def test_wait_operation_backoff_grows():
    clock = FakeClock()
    session = FakeSession(
        {
            ("GET", "/v1/disk/operations/"): [
                FakeResponse(200, payload={"status": "in-progress"}),
                FakeResponse(200, payload={"status": "in-progress"}),
                FakeResponse(200, payload={"status": "in-progress"}),
                FakeResponse(200, payload={"status": "success"}),
            ]
        }
    )
    copier = make_copier(session, clock=clock)

    copier._wait_operation(OPERATION_URL, "tok")

    assert clock.sleeps == [
        pytest.approx(POLL_FIRST_DELAY),
        pytest.approx(POLL_FIRST_DELAY * 2),
        pytest.approx(POLL_FIRST_DELAY * 4),
    ]


def test_wait_operation_caps_delay_at_max():
    clock = FakeClock()
    session = FakeSession(
        {
            ("GET", "/v1/disk/operations/"): FakeResponse(
                200, payload={"status": "in-progress"}
            )
        }
    )
    copier = make_copier(session, clock=clock)

    with pytest.raises(CopyError, match="Таймаут"):
        copier._wait_operation(OPERATION_URL, "tok", timeout_sec=120)

    assert max(clock.sleeps) == pytest.approx(POLL_MAX_DELAY)
    assert sum(clock.sleeps) == pytest.approx(120, abs=POLL_MAX_DELAY)


def test_wait_operation_no_sleep_when_first_poll_succeeds():
    clock = FakeClock()
    session = FakeSession(
        {
            ("GET", "/v1/disk/operations/"): FakeResponse(
                200, payload={"status": "success"}
            )
        }
    )
    copier = make_copier(session, clock=clock)

    copier._wait_operation(OPERATION_URL, "tok")

    assert clock.sleeps == []


def test_wait_operation_does_not_sleep_past_deadline():
    clock = FakeClock()
    session = FakeSession(
        {
            ("GET", "/v1/disk/operations/"): FakeResponse(
                200, payload={"status": "in-progress"}
            )
        }
    )
    copier = make_copier(session, clock=clock)

    with pytest.raises(CopyError, match="Таймаут"):
        copier._wait_operation(OPERATION_URL, "tok", timeout_sec=1)

    assert sum(clock.sleeps) <= 1.0


def test_wait_operation_raises_on_failed_status():
    session = FakeSession(
        {
            ("GET", "/v1/disk/operations/"): FakeResponse(
                200, payload={"status": "failed"}
            )
        }
    )
    copier = make_copier(session)

    with pytest.raises(CopyError, match="завершилась ошибкой"):
        copier._wait_operation(OPERATION_URL, "tok")


# ── сессия и ретраи ─────────────────────────────────────────────────────
def test_build_session_leaves_statuses_to_our_layer():
    """Статусы ретраит ThrottledSession: ретраи urllib3 идут мимо гейта."""
    session = _build_session()
    retries = session.get_adapter("https://cloud-api.yandex.net/").max_retries

    assert not retries.status_forcelist  # urllib3 нормализует список в set
    assert retries.is_retry("POST", 429) is False
    assert retries.is_retry("GET", 503) is False


def test_build_session_still_retries_connect_for_post():
    """Транспортные ретраи остаются, и для POST тоже — соединения важнее."""
    session = _build_session()
    retries = session.get_adapter("https://cloud-api.yandex.net/").max_retries

    assert retries.connect == 3
    assert retries._is_method_retryable("POST") is True


def test_build_session_uses_shared_limiter():
    """Все копиры делят один бюджет — иначе троттлинг не защищает."""
    first = _build_session()
    second = _build_session()

    assert first._limiter is second._limiter is get_limiter()


def test_context_manager_closes_session():
    session = FakeSession()
    with make_copier(session) as copier:
        assert copier.session is session

    assert session.closed


# ── локальное сохранение на машину запуска ──────────────────────────────
def _local_session(listing: dict, body: str = "hello") -> FakeSession:
    return FakeSession(
        {
            ("GET", DOWNLOAD_HREF_URL): FakeResponse(
                200, payload={"href": "https://downloader/file"}
            ),
            ("GET", "https://downloader/file"): FakeResponse(200, text=body),
            ("GET", RESOURCES_URL): FakeResponse(200, payload=listing),
        }
    )


def test_save_local_writes_file_under_email_directory(tmp_path):
    name = "HR аналитика.xlsx"
    session = _local_session(
        embedded(
            [{"path": f"disk:/{name}", "type": "file", "name": name, "size": 5}]
        )
    )
    copier = make_copier(session)
    copier.cfg.local_root = str(tmp_path)

    copier.save_local()

    dest = tmp_path / "src@company.ru" / name
    assert dest.read_bytes() == b"hello"
    assert copier.fails == []


def test_save_local_skips_file_of_the_same_size(tmp_path):
    name = "a.txt"
    session = _local_session(
        embedded([{"path": f"disk:/{name}", "type": "file", "name": name, "size": 4}]),
        body="same",
    )
    dest = tmp_path / "src@company.ru" / name
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"same")
    copier = make_copier(session)
    copier.cfg.local_root = str(tmp_path)

    copier.save_local()

    assert session.calls_to(DOWNLOAD_HREF_URL, "GET") == []
    assert dest.read_bytes() == b"same"


def test_save_local_replaces_file_when_size_differs(tmp_path):
    name = "a.txt"
    session = _local_session(
        embedded([{"path": f"disk:/{name}", "type": "file", "name": name, "size": 5}]),
        body="hello",
    )
    dest = tmp_path / "src@company.ru" / name
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"old")
    copier = make_copier(session)
    copier.cfg.local_root = str(tmp_path)

    copier.save_local()

    assert dest.read_bytes() == b"hello"


def test_save_local_rejects_parent_path(tmp_path):
    session = _local_session(
        embedded(
            [{"path": "disk:/../secret", "type": "file", "name": "secret", "size": 1}]
        )
    )
    copier = make_copier(session)
    copier.cfg.local_root = str(tmp_path)

    copier.save_local()

    assert copier.links == []
    assert "Небезопасный путь" in copier.fails[0]["error"]
    assert not (tmp_path / "secret").exists()


def test_run_local_does_not_call_save_to_disk(tmp_path):
    session = FakeSession(
        {("GET", RESOURCES_URL): FakeResponse(200, payload=embedded([]))}
    )
    copier = make_copier(session, admin_token="")
    copier.cfg.local_save = True
    copier.cfg.local_root = str(tmp_path)

    result = copier.run()

    assert result == {"saved": [], "fails": []}
    assert session.calls_to(SAVE_TO_DISK_URL, "POST") == []


VD_DOWNLOAD_URL = "/v1/disk/virtual-disks/resources/download"


def shared_local_copier(session, tmp_path, *, path="/", page_limit=1000):
    copier = make_copier(session, admin_token="", path=path, page_limit=page_limit)
    copier.cfg.local_save = True
    copier.cfg.local_root = str(tmp_path)
    copier.cfg.source_type = "shared"
    copier.cfg.source_vd_hash = "9Uyws5pZmXgDNA"
    return copier


@pytest.mark.parametrize("response_prefix", [
    "vd:9Uyws5pZmXgDNA:disk:", "vd:/9Uyws5pZmXgDNA/disk"
])
def test_shared_local_downloads_paginated_tree_without_publishing(
    tmp_path, response_prefix
):
    prefix = "vd:9Uyws5pZmXgDNA:disk:"
    session = FakeSession({
        ("GET", VD_DOWNLOAD_URL): FakeResponse(
            200, payload={"href": "https://downloader/file"}
        ),
        ("GET", "https://downloader/file"): FakeResponse(200, text="hello"),
        ("GET", VD_RESOURCES_URL): [
            FakeResponse(200, payload=embedded([
                {"path": response_prefix + "/Документы с пробелами", "type": "dir"}
            ], total=2)),
            FakeResponse(200, payload=embedded([
                {"path": response_prefix + "/Документы с пробелами/отчёт.txt",
                 "type": "file",
                 "name": "отчёт.txt", "size": 5}
            ])),
            FakeResponse(200, payload=embedded([
                {"path": response_prefix + "/root.txt", "type": "file",
                 "name": "root.txt", "size": 5}
            ], total=2)),
        ],
    })
    copier = shared_local_copier(session, tmp_path, page_limit=1)

    result = copier.run()

    base = tmp_path / "shared" / "9Uyws5pZmXgDNA"
    assert (base / "Документы с пробелами" / "отчёт.txt").read_bytes() == b"hello"
    assert (base / "root.txt").read_bytes() == b"hello"
    assert len(result["saved"]) == 2
    assert result["fails"] == []
    listings = [c for c in session.calls if c.url.endswith(VD_RESOURCES_URL)]
    assert [(c.params["path"], c.params["offset"]) for c in listings] == [
        (prefix + "/", 0), (prefix + "/Документы с пробелами", 0),
        (prefix + "/", 1)
    ]
    assert [c.params["path"] for c in session.calls_to(VD_DOWNLOAD_URL)] == [
        prefix + "/Документы с пробелами/отчёт.txt", prefix + "/root.txt"
    ]
    assert session.auth_tokens_for("https://downloader/file") == [
        "OAuth t.uid-42.sig", "OAuth t.uid-42.sig"
    ]
    assert all(c.method == "GET" for c in session.calls)
    assert session.calls_to(RESOURCES_URL) == []


@pytest.mark.parametrize(
    "path", [
        "/Docs", "disk:/Docs", "vd:9Uyws5pZmXgDNA:disk:/Docs",
        "vd:/9Uyws5pZmXgDNA/disk/Docs",
    ]
)
def test_shared_local_preserves_subfolder_and_skips_same_size(tmp_path, path):
    prefix = "vd:9Uyws5pZmXgDNA:disk:"
    session = FakeSession({
        ("GET", VD_RESOURCES_URL): FakeResponse(200, payload=embedded([
            {"path": prefix + "/Docs/a.txt", "type": "file", "size": 4}
        ])),
    })
    dest = tmp_path / "shared" / "9Uyws5pZmXgDNA" / "Docs" / "a.txt"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"same")
    copier = shared_local_copier(session, tmp_path, path=path)

    result = copier.run()

    assert result["fails"] == []
    assert len(result["saved"]) == 1
    assert dest.read_bytes() == b"same"
    assert session.calls[0].params["path"] == prefix + "/Docs"
    assert session.calls_to(VD_DOWNLOAD_URL) == []


@pytest.mark.parametrize("status", [403, 404])
def test_shared_local_unavailable_folder_is_error(tmp_path, status):
    session = FakeSession({
        ("GET", VD_RESOURCES_URL): FakeResponse(status, payload={"error": "missing"})
    })
    copier = shared_local_copier(session, tmp_path)

    with pytest.raises(CopyError, match=str(status)):
        copier.run()


@pytest.mark.parametrize("path", [
    "vd:another:disk:/secret.txt", "vd:9Uyws5pZmXgDNA:disk:/../secret.txt",
    "vd:/another/disk/secret.txt", "vd:/9Uyws5pZmXgDNA/disk/../secret.txt",
    "vd:/9Uyws5pZmXgDNA-extra/disk/secret.txt",
    "vd:/9Uyws5pZmXgDNA/disk-extra/secret.txt",
])
def test_shared_local_rejects_resource_outside_selected_disk(tmp_path, path):
    session = FakeSession({
        ("GET", VD_RESOURCES_URL): FakeResponse(200, payload=embedded([
            {"path": path, "type": "file", "size": 5}
        ]))
    })
    copier = shared_local_copier(session, tmp_path)

    result = copier.run()

    assert result["saved"] == []
    assert len(result["fails"]) == 1
    assert session.calls_to(VD_DOWNLOAD_URL) == []


def test_shared_local_does_not_follow_base_symlink(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    root = tmp_path / "downloads"
    root.mkdir()
    (root / "shared").symlink_to(outside, target_is_directory=True)
    copier = shared_local_copier(FakeSession(), root)

    with pytest.raises(CopyError, match="LOCAL_SAVE_DIR"):
        copier.run()

    assert list(outside.iterdir()) == []


@pytest.mark.parametrize("status", [200, 403])
def test_local_download_preserves_user_part_file(tmp_path, status):
    session = _local_session(embedded([
        {"path": "disk:/a.txt", "type": "file", "size": 5}
    ]))
    session.routes[("GET", "https://downloader/file")] = FakeResponse(
        status, text="hello"
    )
    base = tmp_path / "src@company.ru"
    base.mkdir()
    (base / "a.txt.part").write_bytes(b"user data")
    copier = make_copier(session)
    copier.cfg.local_root = str(tmp_path)

    copier.save_local()

    assert (base / "a.txt.part").read_bytes() == b"user data"
    assert sorted(p.name for p in base.iterdir()) == (
        ["a.txt", "a.txt.part"] if status == 200 else ["a.txt.part"]
    )


def test_shared_local_download_failure_keeps_files_and_hides_signed_url(tmp_path):
    class InterruptedResponse(FakeResponse):
        def iter_content(self, chunk_size=1024 * 1024):
            yield b"partial"
            raise requests.ConnectionError("https://downloader/file?secret=signature")

    session = FakeSession({
        ("GET", VD_DOWNLOAD_URL): FakeResponse(
            200, payload={"href": "https://downloader/file"}
        ),
        ("GET", "https://downloader/file"): InterruptedResponse(200),
        ("GET", VD_RESOURCES_URL): FakeResponse(200, payload=embedded([
            {"path": "vd:9Uyws5pZmXgDNA:disk:/a.txt", "type": "file", "size": 5}
        ])),
    })
    base = tmp_path / "shared" / "9Uyws5pZmXgDNA"
    base.mkdir(parents=True)
    (base / "a.txt").write_bytes(b"old")
    (base / "a.txt.part").write_bytes(b"user data")
    copier = shared_local_copier(session, tmp_path)

    result = copier.run()

    assert result["saved"] == []
    assert len(result["fails"]) == 1
    assert "signature" not in result["fails"][0]["error"]
    assert (base / "a.txt").read_bytes() == b"old"
    assert (base / "a.txt.part").read_bytes() == b"user data"
    assert sorted(p.name for p in base.iterdir()) == ["a.txt", "a.txt.part"]


def test_shared_local_rejects_hash_as_email_without_api_requests(tmp_path):
    session = FakeSession()
    copier = shared_local_copier(session, tmp_path)
    copier.cfg.source_disk_id = "3f90fe10-ee1a-48b1-b445-2b2f09f45af7"

    with pytest.raises(CopyError, match="email сотрудника с доступом"):
        copier.run()

    assert session.calls == []
