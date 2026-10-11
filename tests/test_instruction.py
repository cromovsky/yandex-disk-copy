"""The in-app manual includes the standalone migration runbook."""

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
        "Email сотрудника", "Исходный путь", "Адрес API",
        "Владелец токена назначения", "OAuth-токен получателя",
        "Папка назначения", "Способ переноса",
    ):
        assert field in rendered
    for url in (
        "https://yandex.ru/dev/id/doc/ru/register-api",
        "https://help.solution.korobka360.ru/disk/api/concepts/quickstart.html#oauth",
        "https://help.solution.korobka360.ru/devops/deployment/after-install/"
        "create-organization.html#get-data-token",
        "https://help.solution.korobka360.ru/disk/api/reference/upload-ext.html",
        "https://help.solution.korobka360.ru/disk/api/reference/operations.html",
    ):
        assert f'href="{url}"' in rendered
    assert "<!-- onprem-instruction -->" not in rendered
