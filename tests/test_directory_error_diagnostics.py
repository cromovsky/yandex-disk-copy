"""Directory API codes are visible without copying administrative response text."""

import json

import pytest

from app.migration.diagnostics import response_details
from tests.test_api_error_diagnostics import response


@pytest.mark.parametrize("code,wanted", [(403, "403"), ("403", "403"), ("FORBIDDEN", "FORBIDDEN")])
def test_directory_top_level_code_is_preserved_with_safe_context(code, wanted):
    details = response_details(response({"code": code, "message": "private-admin-token"}),
                               "saas", url="https://cloud-api.yandex.net/v1/directory/organizations/123/users")
    assert details == {"error_side": "saas", "api_error": wanted,
                       "request_host": "cloud-api.yandex.net"}
    assert "private-admin-token" not in json.dumps(details)


@pytest.mark.parametrize("code", [True, 1130000000000001, "SECRET_TOKEN", "private-admin-token"])
def test_directory_unbounded_or_sensitive_codes_are_not_persisted(code):
    details = response_details(response({"code": code}), "saas")
    assert details["api_error"] == "unrecognized_api_error"
    assert str(code) not in json.dumps(details)
