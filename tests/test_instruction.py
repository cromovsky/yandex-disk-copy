"""The in-app manual includes the standalone migration runbook."""

from pathlib import Path
from xml.etree import ElementTree

from app import main as web
from app.source_batches.store import parse_sources


def test_instruction_includes_onprem_single_source(tmp_path, monkeypatch):
    main = tmp_path / "INSTRUCTION.md"
    onprem = tmp_path / "ONPREM.md"
    access = tmp_path / "ONPREM_ACCESS.md"
    main.write_text(
        "# Guide\n\n[Migration][onprem-guide]\n\n"
        "<!-- onprem-instruction -->\n\n<!-- onprem-access -->\n\n[onprem-guide]: ONPREM.md\n",
        encoding="utf-8",
    )
    onprem.write_text(
        '<details markdown="1" id="onprem-guide">\n<summary>Migration</summary>\n\n'
        '<details markdown="1">\n<summary>Sources</summary>\n\n'
        "Single **source** runbook.\n\n[API](https://example.org/api)\n\n"
        "[Access](ONPREM_ACCESS.md#onprem-access)\n\n</details>\n\n</details>\n",
        encoding="utf-8",
    )
    access.write_text(
        '<details markdown="1" id="onprem-access">\n<summary>Access</summary>\n\n'
        '[Migration](ONPREM.md#onprem-guide)\n\n</details>\n', encoding="utf-8",
    )
    monkeypatch.setattr(web, "INSTRUCTION_MD", main)
    monkeypatch.setattr(web, "ONPREM_INSTRUCTION_MD", onprem, raising=False)
    monkeypatch.setattr(web, "ONPREM_ACCESS_MD", access)
    monkeypatch.setattr(web, "_instruction_html", None)

    rendered = web._render_instruction()

    assert "Single <strong>source</strong> runbook." in rendered
    assert 'id="onprem-guide"' in rendered
    assert 'href="#onprem-guide"' in rendered
    assert rendered.count('id="onprem-access"') == 1
    assert 'href="#onprem-access"' in rendered
    assert 'href="https://example.org/api"' in rendered
    assert "<!-- onprem-" not in rendered


def test_instruction_reports_missing_onprem_runbook(tmp_path, monkeypatch):
    main = tmp_path / "INSTRUCTION.md"
    main.write_text("# Guide\n\n<!-- onprem-instruction -->\n\n<!-- onprem-access -->\n", encoding="utf-8")
    monkeypatch.setattr(web, "INSTRUCTION_MD", main)
    monkeypatch.setattr(
        web, "ONPREM_INSTRUCTION_MD", tmp_path / "missing.md", raising=False,
    )
    monkeypatch.setattr(web, "ONPREM_ACCESS_MD", tmp_path / "missing-access.md")
    monkeypatch.setattr(web, "_instruction_html", None)

    rendered = web._render_instruction()

    assert "Инструкция по переносу в on-premises недоступна." in rendered
    assert 'id="onprem-access"' in rendered
    assert "Инструкция по доступам On-premises недоступна." in rendered
    assert "<!-- onprem-" not in rendered
    assert str(tmp_path) not in rendered


def test_real_instruction_contains_migration_fields_and_official_links(monkeypatch):
    monkeypatch.setattr(web, "_instruction_html", None)
    rendered = web._render_instruction()
    assert 'id="onprem-guide"' in rendered
    for field in (
        "ID организации", "Client ID сервисного приложения", "Client secret",
        "Email владельца личного Диска SaaS", "Исходный путь", "Адрес API",
        "Владелец токена назначения", "Токен сотрудника с доступом к общему диску",
        "Метка существующего общего диска", "временный кеш на своей машине",
    ):
        assert field in rendered
    for journal_detail in (
        "Скачать отчёт CSV", "Скачать журнал CSV", "последние 500 загруженных событий",
        "timestamp_utc", "duration_ms",
        "5 попыток всего", "2, 4, 8 и 16 секунд", "Retry-After",
        "Автоматические повторы и проверка результата загрузки в On-premises",
        "повтор сразу завершится конкретной ошибкой",
    ):
        assert journal_detail in rendered
    for url in (
        "https://yandex.ru/dev/id/doc/ru/register-api",
        "https://yandex.ru/support/yandex-360/business/on-premises/deployment/ru/"
        "after-install/create-organization#get-data-token",
        "https://yandex.ru/dev/on-premises/disk-api/doc/ru/",
        "https://yandex.ru/dev/on-premises/disk-api/doc/ru/concepts/quickstart#oauth",
        "https://yandex.ru/dev/on-premises/disk-api/doc/ru/reference/shared-disks/shd-info",
        "https://yandex.ru/dev/on-premises/disk-api/doc/ru/reference/upload_shd",
        "https://yandex.ru/dev/on-premises/disk-api/doc/ru/reference/operations",
    ):
        assert f'href="{url}"' in rendered
    assert "<!-- onprem-instruction -->" not in rendered


def test_migration_instruction_describes_automatic_access_and_job_controls(monkeypatch):
    monkeypatch.setattr(web, "_instruction_html", None)
    rendered = web._render_instruction()

    for text in (
        "Dry Run", "Начать перенос", "Пауза", "Продолжить",
        "Остановить полностью", "Начать заново", "Проверить и повторить",
        "Доступ проверяется автоматически", "повторного чтения SaaS не требуется",
    ):
        assert text in rendered
    for removed in (
        "Предоставить доступ", "Повторить ошибки", "Повторить перенос заново",
        "обычное продолжение не повторяет неудачные",
        "Сохранить проверенный исход", "Подтверждение по серверным журналам",
    ):
        assert removed not in rendered
    for anchor in (
        "onprem-source-access", "onprem-destination-access", "onprem-buttons",
        "onprem-errors", "onprem-journal", "onprem-unknown", "onprem-retry", "onprem-admin",
    ):
        assert f'id="{anchor}"' in rendered


def test_public_instruction_explains_staged_transfer_and_manual_start(monkeypatch):
    monkeypatch.setattr(web, "_instruction_html", None)
    rendered = web._render_instruction()

    for text in (
        "Публикация личного источника обязательна",
        "С временным сохранением в кэш", "начинается после",
        "верхнего уровня", "публичными после завершения",
        "Существующие пароли и ограничения ссылок сохраняются",
        "скачивает файл на свою машину", "проверяет размер и MD5",
        "поддерживают перенос на личные и общие Диски", "3.4.0",
        "ONPREM_ALLOW_INSECURE_HTTP", "ONPREM_ADMIN_TOKEN",
        "21474836480", "1073741824", "107374182400", "2147483648",
        "четыре крупнейших одновременно скачиваемых файла",
    ):
        assert text in rendered
    assert "Утилита не публикует\nих" not in rendered
    assert 'href="https://yandex.ru/dev/disk-api/doc/ru/reference/publish"' in rendered
    assert "показывается только для способа" not in rendered
    assert "Напрямую по ссылке" not in rendered
    assert "upload-ext_shd.html" not in rendered
    assert "ONPREM_ENABLED=0" not in rendered


def test_instruction_explains_source_lists_and_read_only_complete_plans(monkeypatch):
    monkeypatch.setattr(web, "_instruction_html", None)
    rendered = web._render_instruction()
    for text in (
        "Список дисков из CSV", "type,email,vd_hash,path",
        "Повторить Dry Run", "полными планами всех выбранных источников",
        "новые файлы требуют нового Dry Run", "размеру и MD5",
        "четыре источника", "только с временным кешем", "source-batches.db",
    ):
        assert text in rendered
    manual = ElementTree.fromstring("<article>" + rendered + "</article>")
    rights = manual.find(".//details[@id='saas-access-modes']")
    rows = [" ".join(row.itertext()) for row in rights.findall(".//tr")]
    csv = next(row for row in rows if "по CSV" in row)
    assert "Необязателен" in csv and "directory:read_users" in csv
    assert "directory:write_users" not in csv
    local = manual.find("./details[@id='help-local']")
    assert "ADMIN_TOKEN можно оставить пустым" in "".join(local.itertext())
    assert "ID организации (ORGID)" in rendered
    assert "Dry Run его статус не меняет" in rendered
    assert "source_user_not_in_organization" in rendered
    assert "source_user_blocked" in rendered
    assert "до <strong>40 запросов/с на чтение</strong>" not in rendered
    assert "3 изменений/с" in rendered


def test_manual_opens_with_separate_collapsed_chapters_for_each_mode(monkeypatch):
    monkeypatch.setattr(web, "_instruction_html", None)
    manual = ElementTree.fromstring("<article>" + web._render_instruction() + "</article>")
    modes = manual.findall("./details")[:4]
    assert [mode.findtext("summary") for mode in modes] == [
        "Одна организация — перенос между Дисками в SaaS",
        "Между организациями — перенос из одной организации SaaS в другую",
        "Сохранить локально — скачать из SaaS на машину с утилитой",
        "SaaS → On-premises — перенос на личные и общие Диски",
    ]
    for mode in modes[:3]:
        assert len(mode.findall("./details/summary")) == 4
    onprem = modes[3]
    assert [chapter.get("id") for chapter in onprem.findall("./details")] == [
        "onprem-requirements", "onprem-personal", "onprem-shared", "onprem-personal-list",
        "onprem-shared-list", "onprem-recovery", "onprem-admin",
    ]
    assert manual.find("./details[@id='saas-access']") is not None
    assert manual.find("./details[@id='onprem-access']/details[@id='onprem-personal-access']") is not None
    assert not manual.findall(".//details[@open]")
    ids = [element.attrib["id"] for element in manual.iter() if "id" in element.attrib]
    assert len(ids) == len(set(ids))
    for link in manual.iter("a"):
        href = link.get("href", "")
        if href.startswith("#"):
            assert href[1:] in ids, href


def test_onprem_personal_access_guide_covers_registration_exchange_and_upload(monkeypatch):
    monkeypatch.setattr(web, "_instruction_html", None)
    manual = ElementTree.fromstring("<article>" + web._render_instruction() + "</article>")
    chapter = manual.find(".//details[@id='onprem-personal-access']")
    assert chapter is not None
    steps = [summary.text.split(".", 1)[0] for summary in chapter.findall("./details/summary")]
    assert [int(step) for step in steps if step.isdecimal()] == list(range(1, 9))
    text = " ".join(chapter.itertext())
    for item in (
        "APIGW360_CLIENT_ID", "DISK_MIGRATION_CLIENT_ID", "DISK_MIGRATION_CLIENT_SECRET",
        "/oauth/authorize", "/service_applications", "/users", "cloud_api:disk.app_folder",
        "urn:ietf:params:oauth:grant-type:token-exchange", "urn:yandex:params:oauth:token-type:email",
        "urn:yandex:params:oauth:token-type:uid", "refresh_token", "/resources/upload",
        "201", "202", "Wrong client secret", "Scope from POST does not match the client's one",
        "No scopes allowed for token exchange", "err_scope not empty", "Unauthorized",
        "uid type must be a number",
    ):
        assert item in text, item
    assert chapter.findall(".//pre/code"), "Commands must be copyable from the instruction"


def test_csv_examples_copied_from_the_manual_can_be_imported(monkeypatch):
    monkeypatch.setattr(web, "_instruction_html", None)
    rendered = web._render_instruction()
    manual = ElementTree.fromstring("<article>" + rendered + "</article>")
    for section_id, expected_headers in (
        ("help-local", {"type,email,vd_hash,path"}),
        ("onprem-guide", {"source_email,path,destination_email,destination_folder", "type,email,vd_hash,path"}),
    ):
        section = manual.find(f"./details[@id='{section_id}']")
        assert section is not None
        examples = [code.text for code in section.findall(".//pre/code[@class='language-csv']")]
        assert {example.splitlines()[0] for example in examples} == expected_headers
        for example in examples:
            assert parse_sources(example, personal=example.startswith("source_email,"))


def test_public_documents_only_describe_supported_staged_release():
    root = Path(__file__).resolve().parents[1]
    for name in (
        "README.md", "docs/INSTRUCTION.md", "docs/ONPREM.md", "docs/UPDATE.md",
    ):
        content = (root / name).read_text(encoding="utf-8")
        assert "3.4.0" in content, name
        assert "SaaS → On-premises" in content, name
        for removed in (
            "Напрямую по ссылке", "upload-ext_shd", "ONPREM_ENABLED=0",
            "При прямом способе", "Для прямого способа",
        ):
            assert removed not in content, (name, removed)


def test_update_guide_distinguishes_images_and_source_builds():
    guide = (Path(__file__).resolve().parents[1] / "docs/UPDATE.md").read_text(
        encoding="utf-8",
    )
    for required in (
        "cromovsky/yandex-disk-copy-v2:3.4.0",
        "cromovsky/yandex-disk-copy:3.4.0",
        "docker compose pull", "docker compose up -d --no-build",
        "git pull --ff-only", "docker compose up --build -d",
        "docker compose stop disk-copy", "docker cp",
        "migration-data", "downloads", "ONPREM_ALLOW_INSECURE_HTTP",
        "ONPREM_ADMIN_TOKEN", "onprem_staged_required", "введите Client secret",
    ):
        assert required in guide
    assert "удаляет историю, планы и кеш" in guide
    assert "Секреты хранятся только в памяти" in guide
