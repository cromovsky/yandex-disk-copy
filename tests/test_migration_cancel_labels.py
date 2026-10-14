from app.migration.journal import enrich_event
from tests.test_migration_runner import CONFIG


def test_cancelled_entry_has_local_cancellation_labels():
    event = enrich_event({"type": "entry_state", "payload": {
        "state": "cancelled", "error": "local_operation_cancelled",
    }}, CONFIG, "")
    assert event["status_label"] == "Отменено"
    assert event["error_message"] == "Действие отменено при остановке задания."


def test_local_cancellation_event_has_explanatory_csv_labels():
    event = enrich_event({"type": "local_operations_cancelled", "payload": {
        "count": 3,
    }}, CONFIG, "")
    assert event["stage_label"] == "Отмена локальных действий"
    assert event["status_label"] == "Отменено"


def test_released_path_error_explains_available_retry():
    event = enrich_event({"type": "entry_state", "payload": {
        "state": "failed", "error": "stopped_job_path_released",
    }}, CONFIG, "")
    assert event["error_message"] == (
        "Блокировка остановленного задания снята. Файл можно повторить."
    )


def test_historical_reserved_path_event_keeps_original_explanation():
    event = enrich_event({"type": "transfer_stage", "payload": {
        "stage": "check_destination", "status": "failed",
        "error": "destination_path_reserved",
    }}, CONFIG, "")
    assert "Целевой путь защищён" in event["error_message"]
