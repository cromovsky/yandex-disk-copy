"""Тесты DiskCopier: обработка ошибок API, пагинация, ротация токенов.

Сеть не используется — session подменяется FakeSession. Внимание на порядок
ключей в routes: матчинг идёт по подстроке URL в порядке вставки, поэтому
специфичные фрагменты (".../resources/publish") добавляем ДО общих (".../resources").
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

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


def test_run_keeps_transfer_error_when_restore_also_fails():
    """Ошибка переноса важнее ошибки откатa — она и должна всплыть."""
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
