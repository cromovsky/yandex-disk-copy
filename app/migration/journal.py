"""Credential-free labels and immutable resource context for migration events."""

from .destination import destination_path
from .diagnostics import describe_diagnostics, diagnostic_fields


STAGE_LABELS = {
    "retry_policy": "Автоматические повторы",
    "prepare_clients": "Подключение к дискам",
    "planning": "Чтение структуры исходного диска",
    "claim_namespace": "Создание служебной папки",
    "create_directory": "Создание папки",
    "check_source": "Проверка исходного файла",
    "check_destination": "Проверка целевого файла",
    "check_destination_access": "Проверка доступности выбранного диска On-premises",
    "abandon_attempt": "Локальная попытка прервана; API не поддерживает отмену удалённой операции",
    "prepare_upload_directory": "Подготовка папки для загрузки",
    "downloading": "Скачивание в локальный кеш",
    "download_retry": "Повтор скачивания",
    "point_retry": "Проверка и повтор файла",
    "get_download_url": "Получение ссылки для скачивания",
    "publish_source": "Публикация исходной папки или файла",
    "public_download_link": "Получение публичной ссылки скачивания",
    "submit_url_upload": "Запуск загрузки по ссылке",
    "upload_file": "Загрузка файла в On-premises",
    "poll_transfer": "Ожидание загрузки",
    "verify_temporary": "Проверка загруженной временной копии",
    "prepare_destination_directory": "Подготовка целевой папки",
    "move_file": "Перемещение в целевую папку",
    "poll_move": "Ожидание перемещения",
    "verify_destination": "Проверка итогового файла",
    "cleanup": "Очистка временных файлов",
}
STAGE_STATUSES = frozenset({
    "started", "succeeded", "failed", "outcome_unknown", "interrupted", "waiting",
    "skipped", "cancelled",
})
STATUS_LABELS = {
    "started": "Начато", "succeeded": "Успешно", "failed": "Ошибка",
    "outcome_unknown": "Результат неизвестен", "interrupted": "Прервано",
    "skipped": "Пропущено",
    "waiting": "Ожидание", "in-progress": "В процессе", "success": "Успешно",
    "queued": "В очереди", "ready": "План готов", "running": "Выполняется",
    "planning": "Подготовка плана", "planned": "Запланировано",
    "paused": "Приостановлено", "stopped": "Остановлено", "done": "Готово",
    "cancelled": "Отменено",
    "already_verified": "Уже перенесено и проверено", "conflict": "Конфликт",
    "skipped_by_user": "Пропущен пользователем",
    "completed": "Завершено", "completed_with_issues": "Завершено с ошибками",
    "needs_attention": "Нужно вмешательство",
    "awaiting_credentials": "Нужно повторно указать доступы",
    "prepared": "Попытка подготовлена", "downloading": "Скачивание в локальный кеш",
    "staged": "Файл сохранён в локальном кеше", "transferring": "Загрузка",
    "waiting_operation": "Ожидание удалённой операции", "verifying": "Проверка копии",
    "committing": "Перемещение в целевую папку",
    "transfer_intent": "Подготовка запроса загрузки",
    "move_intent": "Подготовка запроса перемещения",
    "waiting_transfer": "Ожидание загрузки", "waiting_move": "Ожидание перемещения",
    "unknown_transfer": "Результат загрузки неизвестен",
    "unknown_move": "Результат перемещения неизвестен",
    "abandoned_transfer": "Ожидание предыдущей загрузки прекращено",
    "abandoned_move": "Ожидание предыдущего перемещения прекращено",
    "verifying_final": "Проверка итогового файла",
    "intent": "Публикация запрошена", "published": "Публичный доступ подтверждён",
}
ERROR_MESSAGES = {
    "local_operation_cancelled": "Действие отменено при остановке задания.",
    "stopped_job_path_released": (
        "Блокировка остановленного задания снята. Файл можно повторить."
    ),
    "destination_path_reserved": (
        "Целевой путь защищён незавершённой операцией предыдущего задания. "
        "Подтвердите завершение операции перед повтором этого файла."
    ),
    "source_auth_failed": "Не удалось авторизоваться на исходном диске SaaS.",
    "source_admin_auth_failed": "SaaS: проверьте токен администратора и право directory:read_users.",
    "source_user_not_in_organization": "SaaS: сотрудник не найден в указанной организации. Проверьте email и ID организации (ORGID).",
    "source_user_blocked": "SaaS: сотрудник заблокирован. Разблокируйте его в организации и повторите Dry Run.",
    "source_admin_check_failed": "SaaS: не удалось проверить сотрудника через API организации. Проверьте HTTP-код и сетевую ошибку в журнале.",
    "source_admin_invalid_response": "SaaS: API организации вернул некорректный ответ при проверке сотрудника.",
    "source_not_found": "Файл или папка на исходном диске SaaS не найдены.",
    "source_unavailable": "Исходный диск SaaS недоступен; запрос не завершён.",
    "source_invalid_response": "Исходный диск SaaS вернул некорректный ответ.",
    "source_download_failed": "Не удалось скачать файл с исходного диска SaaS.",
    "source_connection_failed": "Не удалось установить соединение с диском SaaS.",
    "source_timeout": "Истекло время ожидания ответа диска SaaS.",
    "source_stream_interrupted": "Соединение с SaaS прервалось при скачивании файла.",
    "source_download_timeout": "Истекло время ожидания скачивания файла с SaaS.",
    "source_publication_denied": "SaaS запретил публикацию исходного ресурса.",
    "source_publication_failed": "Не удалось опубликовать папку или файл в SaaS.",
    "source_publication_outcome_unknown": (
        "SaaS не подтвердил результат публикации; требуется проверка."
    ),
    "source_publication_not_confirmed": (
        "Публичный доступ к исходному ресурсу SaaS не подтверждён."
    ),
    "source_public_resource_unavailable": "Ресурс SaaS по публичной ссылке недоступен.",
    "source_public_access_denied": "SaaS отказал в доступе по публичной ссылке.",
    "source_public_download_limit": "SaaS ограничил скачивание по публичной ссылке.",
    "source_public_download_failed": (
        "Не удалось скачать файл по публичной ссылке SaaS."
    ),
    "source_public_invalid_response": (
        "SaaS вернул некорректный ответ на публичный запрос."
    ),
    "source_changed": "Исходный файл SaaS изменился после подготовки плана.",
    "source_content_changed": (
        "Размер или MD5 файла, скачанного с SaaS, расходятся с планом."
    ),
    "source_hash_unavailable": "Исходный диск SaaS не сообщил MD5 файла.",
    "source_size_unavailable": "Исходный диск SaaS не сообщил размер файла.",
    "invalid_source_tree": "Структура диска SaaS не совпала с выбранным путём.",
    "unsupported_resource": "Тип ресурса не поддерживается для переноса.",
    "invalid_download_url": "Диск SaaS вернул некорректную ссылку скачивания.",
    "destination_auth_failed": "Не удалось авторизоваться на диске On-premises.",
    "destination_not_found": "Файл или папка на диске On-premises не найдены.",
    "destination_unavailable": "Диск On-premises недоступен; запрос не завершён.",
    "destination_timeout": "Истекло время ожидания ответа диска On-premises.",
    "destination_connection_failed": (
        "Не удалось установить соединение с диском On-premises."
    ),
    "destination_stream_interrupted": (
        "Соединение с On-premises прервалось при передаче данных."
    ),
    "destination_invalid_response": "Диск On-premises вернул некорректный ответ.",
    "destination_conflict": (
        "По целевому пути On-premises находится другой файл или папка."
    ),
    "destination_file_too_large": "Размер файла превышает лимит On-premises.",
    "destination_read_only": (
        "On-premises отклонил запрос: ресурс заблокирован или доступ ограничен."
    ),
    "destination_rate_limited": "Превышен лимит запросов к диску On-premises.",
    "destination_quota_exceeded": "На диске On-premises недостаточно свободного места.",
    "destination_space_insufficient": (
        "На диске On-premises недостаточно места для переноса по плану."
    ),
    "destination_hash_unavailable": "Диск On-premises не сообщил MD5 для проверки.",
    "destination_shared_access_required": (
        "Нет доступа к выбранному общему диску On-premises."
    ),
    "destination_identity_mismatch": "Учётная запись On-premises не совпала с выбором.",
    "invalid_upload_url": "Диск On-premises вернул некорректную ссылку загрузки.",
    "directory_confirmation_pending": (
        "On-premises ещё не подтвердил создание папки метаданными."
    ),
    "verification_mismatch": (
        "Размер или MD5 файла On-premises не совпадают с исходным."
    ),
    "remote_operation_failed": "On-premises сообщил об ошибке удалённой операции.",
    "remote_operation_still_pending": (
        "On-premises сообщил, что прежняя операция ещё выполняется. "
        "Для новой проверки диска и загрузки нажмите «Проверить и повторить» у файла."
    ),
    "destination_url_import_failed": (
        "On-premises сообщил об ошибке скачивания файла по ссылке; "
        "API не сообщил подробную причину."
    ),
    "invalid_operation_status": "On-premises вернул неизвестный статус операции.",
    "polling_unavailable": "Не удалось проверить статус операции в On-premises.",
    "transfer_outcome_unknown": (
        "On-premises не подтвердил результат отправленного запроса; "
        "подробная причина указана на стадии сбоя."
    ),
    "final_upload_confirmation_required": (
        "On-premises не подтвердил завершение загрузки в итоговый путь. "
        "Нажмите «Проверить и повторить» у файла."
    ),
    "final_move_confirmation_required": (
        "On-premises не подтвердил итоговое перемещение. "
        "Нажмите «Проверить и повторить» у файла."
    ),
    "point_retry_not_available": "Для этого файла точечный повтор сейчас недоступен.",
    "point_retry_in_progress": "Точечный повтор файла уже выполняется.",
    "technical_namespace_exists": "Служебная папка существует; проверьте её владельца.",
    "technical_namespace_outcome_unknown": (
        "On-premises не подтвердил создание служебной папки."
    ),
    "technical_path_collision": "Путь исходного ресурса совпадает со служебной папкой.",
    "path_collision": "Несколько исходных ресурсов получили один целевой путь.",
    "staging_space_insufficient": "Не хватает места или лимита кеша для файла.",
    "staging_file_unavailable": (
        "Файл отсутствует в локальном кеше или повреждён. "
        "Повторное скачивание возможно после очистки кеша; потребуется доступ к SaaS."
    ),
    "staging_io_failed": "Ошибка записи файла в локальный кеш.",
    "invalid_staging_object": (
        "В локальном кеше повреждена структура или найден неподдерживаемый объект. "
        "Проверьте каталог кеша на машине утилиты."
    ),
    "cache_in_use": (
        "Перенос выполняется. Приостановите или остановите задания перед очисткой кеша."
    ),
    "cache_cleanup_failed": (
        "Не удалось полностью очистить локальный кеш. Проверьте доступ к диску и повторите очистку."
    ),
    "credentials_required": "Для продолжения нужно повторно указать доступы.",
    "identity_confirmation_required": "Нужно подтвердить целевую учётную запись.",
    "quota_unavailable": "Не удалось получить сведения о свободном месте On-premises.",
    "internal_error": "Внутренняя ошибка утилиты; стадия указана в журнале.",
    "migration_failed": "Перенос не завершён из-за ошибки.",
    "invalid_destination_url": "Некорректный адрес API On-premises.",
    "invalid_destination_shared_disk": "Некорректный общий диск или целевая папка.",
    "invalid_destination_type": "Некорректный тип целевого диска.",
    "onprem_staged_required": (
        "Перенос в On-premises поддерживает только локальный буфер. "
        "Создайте новое задание и выполните новый Dry Run; история старого сохранена."
    ),
    "invalid_source_public": (
        "Публичное скачивание доступно только при переносе на общий диск."
    ),
    "invalid_disk_path": "Некорректный путь файла или папки.",
    "invalid_operation_reference": (
        "Утилита не смогла распознать ссылку на статус операции On-premises."
    ),
}


OPERATION_ISSUE_MESSAGES = {
    "metadata": "Поля method или templated не соответствуют ожидаемому формату.",
    "url": "Ссылка операции не прошла проверку формата и безопасности URL.",
    "origin": "Адрес сервера в ссылке операции не совпадает с настроенным адресом API.",
    "route": "Путь ссылки не соответствует поддерживаемому адресу статуса операции.",
    "query": "Параметры ссылки не соответствуют ожидаемому формату id=ID.",
    "id_mismatch": "Идентификаторы в href и operation_id не совпадают.",
    "missing_id": "В ответе отсутствует распознаваемая ссылка или идентификатор операции.",
    "id_format": "Формат или длина идентификатора не поддерживается утилитой.",
}


def enrich_event(event, config, remote_root, entry=None, attempt=None):
    """Use only this event's attempt; mutable checkpoints never rewrite history."""
    payload = event["payload"]
    source = payload.get("source_path") or (
        entry["source_path"] if entry else config["source_path"]
    )
    destination = entry["destination_path"] if entry else config["destination_root"]
    if payload.get("stage") == "claim_namespace":
        destination = remote_root
    stage = payload.get("stage") or payload.get("phase")
    status = payload.get("status") or payload.get("state")
    error = payload.get("error")
    error_message = (
        ERROR_MESSAGES.get(error, f"Ошибка переноса: {error}.") if error else ""
    )
    if error == "invalid_operation_reference":
        issue = OPERATION_ISSUE_MESSAGES.get(payload.get("operation_reference_issue"))
        if issue:
            error_message += " " + issue
        if payload.get("http_status") == 202:
            error_message = "Запрос принят сервером On-premises (HTTP 202). " + error_message
    if status == "outcome_unknown" and error != "remote_operation_still_pending":
        operation = {
            "publish_source": "публикации в SaaS",
            "submit_url_upload": "загрузки в On-premises",
            "upload_file": "загрузки в On-premises",
            "poll_transfer": "загрузки в On-premises",
            "unknown_transfer": "загрузки в On-premises",
            "move_file": "перемещения в On-premises",
            "poll_move": "перемещения в On-premises",
            "unknown_move": "перемещения в On-premises",
            "claim_namespace": "создания служебной папки в On-premises",
            "create_directory": "создания папки в On-premises",
        }.get(stage, "операции в On-premises")
        error_message = (
            f"Результат {operation} не подтвержден. {error_message} "
            "Запрос мог выполниться на сервере."
        )
    if (error == "destination_url_import_failed"
            and diagnostic_fields(payload).get("api_error") not in {
                None, "unrecognized_api_error",
            }):
        error_message = "On-premises сообщил об ошибке скачивания файла по ссылке."
    diagnostic_message = describe_diagnostics(payload)
    if error_message and diagnostic_message:
        error_message += " " + diagnostic_message
    event_labels = {
        "entry_selection_changed": ("Выбор объекта",
                                    "Выбран" if payload.get("selected") else "Исключён"),
        "selection_frozen": ("Выбор объектов", "Подтверждён для переноса"),
        "full_stop_requested": ("Полная остановка задания", "Запрошено"),
        "local_operations_cancelled": ("Отмена локальных действий", "Отменено"),
        "local_cleanup": ("Очистка локального кеша",
                          STATUS_LABELS.get(status, status or "")),
    }.get(event.get("type"))
    event.update(
        context={
            "source": source, "destination": destination_path(config, destination),
            "kind": entry["kind"] if entry else None,
            "size": entry["size"] if entry else None,
            "strategy": (attempt["strategy"] if attempt else
                         payload.get("strategy", config["strategy"])),
            "remote_path": (destination_path(config, attempt["remote_path"])
                            if attempt else None),
        },
        stage_label=(event_labels[0] if event_labels
                     else STAGE_LABELS.get(stage, STATUS_LABELS.get(stage, stage or ""))),
        status_label=(event_labels[1] if event_labels
                      else STATUS_LABELS.get(status, status or "")),
        error_message=error_message,
        diagnostic_message=diagnostic_message,
    )
    return event
