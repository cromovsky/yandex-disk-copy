"""The in-app manual includes the standalone migration runbook."""

from pathlib import Path

from app import main as web


def test_instruction_includes_onprem_single_source(tmp_path, monkeypatch):
    main = tmp_path / "INSTRUCTION.md"
    onprem = tmp_path / "ONPREM.md"
    main.write_text(
        "# Guide\n\n[Migration][onprem-guide]\n\n"
        "<!-- onprem-instruction -->\n\n[onprem-guide]: ONPREM.md\n",
        encoding="utf-8",
    )
    onprem.write_text(
        '<h2 id="onprem-guide">Migration</h2>\n\n'
        "Single source runbook.\n\n[API](https://example.org/api)\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(web, "INSTRUCTION_MD", main)
    monkeypatch.setattr(web, "ONPREM_INSTRUCTION_MD", onprem, raising=False)
    monkeypatch.setattr(web, "_instruction_html", None)

    rendered = web._render_instruction()

    assert "Single source runbook." in rendered
    assert 'id="onprem-guide"' in rendered
    assert 'href="#onprem-guide"' in rendered
    assert 'href="https://example.org/api"' in rendered
    assert "<!-- onprem-instruction -->" not in rendered


def test_instruction_reports_missing_onprem_runbook(tmp_path, monkeypatch):
    main = tmp_path / "INSTRUCTION.md"
    main.write_text("# Guide\n\n<!-- onprem-instruction -->\n", encoding="utf-8")
    monkeypatch.setattr(web, "INSTRUCTION_MD", main)
    monkeypatch.setattr(
        web, "ONPREM_INSTRUCTION_MD", tmp_path / "missing.md", raising=False,
    )
    monkeypatch.setattr(web, "_instruction_html", None)

    rendered = web._render_instruction()

    assert "Инструкция по переносу в on-premises недоступна." in rendered
    assert "<!-- onprem-instruction -->" not in rendered
    assert str(tmp_path) not in rendered


def test_real_instruction_contains_migration_fields_and_official_links(monkeypatch):
    monkeypatch.setattr(web, "_instruction_html", None)
    rendered = web._render_instruction()
    assert 'id="onprem-guide"' in rendered
    for field in (
        "ID организации", "Client ID сервисного приложения", "Client secret",
        "Email владельца личного Диска SaaS", "Исходный путь", "Адрес API",
        "Владелец токена назначения", "Токен сотрудника с доступом к общему диску",
        "Метка существующего общего диска", "Временный кеш",
    ):
        assert field in rendered
    for journal_detail in (
        "Скачать отчёт CSV", "Скачать журнал CSV", "последние 500 загруженных событий",
        "timestamp_utc", "duration_ms",
        "5 попыток всего", "2, 4, 8 и 16 секунд", "Retry-After",
        "Повторы и проверка результата", "повтор сразу завершится конкретной ошибкой",
    ):
        assert journal_detail in rendered
    for url in (
        "https://yandex.ru/dev/id/doc/ru/register-api",
        "https://help.solution.korobka360.ru/disk/api/concepts/quickstart.html#oauth",
        "https://help.solution.korobka360.ru/devops/deployment/after-install/"
        "create-organization.html#get-data-token",
        "https://help.y360-dr.korobka360.ru/disk/api/reference/upload_shd.html",
        "https://help.y360-dr.korobka360.ru/disk/api/reference/operations.html",
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
        "доступен в обоих публичных образах", "3.1.0",
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
    assert "<strong>ADMIN_TOKEN</strong> виден и" in rendered
    assert "необязателен" in rendered
    assert "directory:read_users" in rendered
    assert "ID организации (ORGID)" in rendered
    assert "Dry Run его статус не меняет" in rendered
    assert "source_user_not_in_organization" in rendered
    assert "source_user_blocked" in rendered
    assert "до <strong>40 запросов/с на чтение</strong>" not in rendered
    assert "3 изменений/с" in rendered


def test_public_documents_only_describe_supported_staged_release():
    root = Path(__file__).resolve().parents[1]
    for name in (
        "README.md", "docs/INSTRUCTION.md", "docs/ONPREM.md", "docs/UPDATE.md",
    ):
        content = (root / name).read_text(encoding="utf-8")
        assert "3.1.0" in content, name
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
        "cromovsky/yandex-disk-copy-v2:3.1.0",
        "cromovsky/yandex-disk-copy:3.1.0",
        "docker compose pull", "docker compose up -d --no-build",
        "git pull --ff-only", "docker compose up --build -d",
        "docker compose stop disk-copy", "docker cp",
        "migration-data", "downloads", "ONPREM_ALLOW_INSECURE_HTTP",
        "ONPREM_ADMIN_TOKEN", "onprem_staged_required", "введите Client secret",
    ):
        assert required in guide
    assert "удаляет историю, планы и кеш" in guide
    assert "Секреты хранятся только в памяти" in guide
