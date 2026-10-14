from app.migration.journal import enrich_event
from tests.test_migration_runner import CONFIG


def test_full_stop_request_has_explanatory_csv_labels():
    event = enrich_event({"type": "full_stop_requested", "payload": {
        "full_stop_requested": True, "stop_requested": True,
    }}, CONFIG, "disk:/.disk-copy-transfer-test")
    assert event["stage_label"] == "Полная остановка задания"
    assert event["status_label"] == "Запрошено"


def test_reserved_destination_error_explains_file_protection():
    event = enrich_event({"type": "stage", "payload": {
        "stage": "check_destination", "status": "failed",
        "error": "destination_path_reserved",
    }}, CONFIG, "disk:/.disk-copy-transfer-test")
    assert "незавершённой" in event["error_message"]
    assert "операци" in event["error_message"]
