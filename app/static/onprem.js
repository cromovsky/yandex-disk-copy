(function () {
  "use strict";

  const byId = (id) => document.getElementById(id);
  const panel = byId("mode-onprem");
  const tab = document.querySelector('.mode-tab[data-mode="onprem"]');
  if (!panel || !tab) return;

  const fields = Array.from(panel.querySelectorAll("[data-onprem-config]"));
  const operationReferencePattern = /^(?:\?id=)?[A-Za-z0-9_-]{1,256}$/;
  const jobIdPattern = /^[a-f0-9]{32}$/;
  const labels = {
    queued: "В очереди", planning: "Подготовка плана", ready: "План готов",
    running: "Перенос выполняется", paused: "Приостановлено",
    awaiting_credentials: "Нужен повторный доступ", needs_attention: "Нужна проверка",
    completed: "Завершено", completed_with_issues: "Завершено с замечаниями",
    stopped: "Остановлено", cancelled: "Отменено",
    planned: "В плане", transferring: "Передача", downloading: "Скачивание",
    staged: "Сохранён временно", waiting_operation: "Ожидание операции",
    verifying: "Проверка", committing: "Финализация", done: "Перенесено",
    already_verified: "Уже совпадает", conflict: "Конфликт",
    source_changed: "Источник изменился", failed: "Ошибка",
    outcome_unknown: "Исход неизвестен", verification_unavailable: "Проверка недоступна",
    created: "Каталог создан", success: "Успешно", "in-progress": "Выполняется",
    prepared: "Попытка подготовлена", transfer_intent: "Передача подготовлена",
    waiting_transfer: "Ожидание загрузки", move_intent: "Перемещение подготовлено",
    waiting_move: "Ожидание перемещения", verifying_final: "Проверка итогового файла",
    unknown_transfer: "Исход загрузки неизвестен", unknown_move: "Исход перемещения неизвестен",
    abandoned_transfer: "Ожидание предыдущей загрузки прекращено",
    abandoned_move: "Ожидание предыдущего перемещения прекращено",
    download_retry_wait: "Ожидание повтора скачивания",
    intent: "Публикация запрошена", published: "Публичный доступ подтверждён",
  };
  const errors = {
    onprem_access_required: "Нужен действующий ключ доступа к режиму on-premises.",
    credentials_required: "Проверьте Client secret и токен в форме, затем нажмите «Продолжить».",
    source_credentials_required: "Введите Client secret источника.",
    destination_credentials_required: "Введите токен сотрудника с доступом к общему диску.",
    identity_confirmation_required: "Подтвердите владельца токена назначения.",
    destination_identity_mismatch: "Учётная запись On-premises не совпала с выбором.",
    invalid_destination_shared_disk: "Укажите корректную метку существующего общего диска из его адреса.",
    destination_shared_access_required: "Нет доступа к выбранному общему диску On-premises.",
    destination_shared_disk_mismatch: "Ответ API относится к другому общему диску. Проверьте метку назначения.",
    destination_read_only: "On-premises отклонил запрос: ресурс заблокирован или доступ ограничен.",
    onprem_staged_required: "Это историческое задание с прежним способом переноса. Доступны просмотр, остановка и экспорт. Для переноса выполните новый Dry Run.",
    legacy_personal_job: "Это историческое задание на личный Диск. Доступны просмотр, остановка и CSV. Для общего диска подготовьте новое задание.",
    credential_binding_mismatch: "Доступы не соответствуют настройкам задания.",
    credentials_binding_mismatch: "Доступы не соответствуют настройкам задания.",
    source_changed: "Исходный файл SaaS изменился после подготовки плана.",
    source_not_found: "Файл или папка на исходном диске SaaS не найдены.",
    conflict: "Итоговый путь занят другим объектом; перезапись запрещена.",
    outcome_unknown: "Результат передачи не подтверждён. Повторите файл кнопкой «Проверить и повторить» в его строке, если она доступна.",
    verification_unavailable: "Контрольная сумма недоступна; перенос не подтверждён.",
    verification_failed: "Содержимое не прошло проверку размера и контрольной суммы.",
    integrity_mismatch: "Содержимое не прошло проверку размера и контрольной суммы.",
    quota_exceeded: "В назначении недостаточно свободного места.",
    insufficient_space: "Недостаточно свободного места.",
    staging_budget_exceeded: "Файл не помещается в бюджет временного сохранения.",
    local_space_exhausted: "На сервере переноса закончилось свободное место.",
    invalid_path: "Путь содержит неподдерживаемые элементы.",
    invalid_destination_url: "Адрес API назначения не прошёл проверку.",
    invalid_state: "Действие недоступно в текущем состоянии. Обновите задание.",
    plan_not_ready: "План ещё не готов. Обновите задание.",
    job_not_running: "Пауза недоступна в текущем состоянии задания. Обновите задание.",
    job_not_resumable: "Задание сейчас нельзя продолжить. Обновите его состояние.",
    job_not_awaiting_credentials: "Задание сейчас не принимает доступы. Обновите его состояние.",
    revision_conflict: "Задание изменилось. Обновите его перед повтором.",
    stale_revision: "Задание изменилось. Обновите его перед повтором.",
    validation_error: "Проверьте обязательные поля и допустимые пути.",
    network_error: "Соединение прервалось. Обновите задание перед повторным действием.",
    unavailable: "Сервис временно недоступен. Состояние задания сохраняется.",
    invalid_response: "Ответ сервера не удалось прочитать. Обновите задание перед повторным действием.",
    destination_path_reserved: "Целевой путь занят операцией другого задания. Откройте и остановите это задание, затем повторите файл.",
    local_operation_cancelled: "Действие отменено при остановке задания.",
    stopped_job_path_released: "Блокировка остановленного задания снята. Файл можно повторить.",
    destination_conflict: "По целевому пути On-premises находится другой файл или папка.",
    transfer_outcome_unknown: "On-premises не подтвердил результат отправленного запроса; подробная причина указана на стадии сбоя.",
    verification_mismatch: "Содержимое не прошло проверку размера и контрольной суммы.",
    source_hash_unavailable: "Исходный диск SaaS не сообщил MD5 файла.",
    destination_hash_unavailable: "Диск On-premises не сообщил MD5 для проверки.",
    staging_space_insufficient: "На сервере переноса недостаточно места для временного файла.",
    destination_space_insufficient: "На диске On-premises недостаточно места для переноса по плану.",
    technical_namespace_exists: "Технический каталог уже существует. Создайте новое задание.",
    technical_namespace_outcome_unknown: "В историческом задании не удалось подтвердить создание служебной папки. Остановите это задание и создайте новый перенос.",
    destination_auth_failed: "Не удалось авторизоваться на диске On-premises.",
    source_auth_failed: "Не удалось авторизоваться на исходном диске SaaS.",
    source_admin_auth_failed: "SaaS: проверьте админский токен и право directory:read_users.",
    source_user_not_in_organization: "Email не найден в указанной SaaS-организации. Проверьте email и ORGID.",
    source_user_blocked: "Сотрудник SaaS заблокирован. Разблокируйте его в админке и повторите Dry Run; утилита не меняет статус.",
    source_admin_check_failed: "Не удалось проверить сотрудника в SaaS-организации. Проверьте доступность API 360 и журнал.",
    source_admin_invalid_response: "API 360 вернул некорректный ответ при проверке сотрудника. Подробности — в журнале.",
    remote_operation_still_pending: "On-premises ещё выполняет передачу. Повторите проверку файла позже.",
    remote_operation_failed: "On-premises сообщил об ошибке удалённой операции.",
    destination_url_import_failed: "On-premises сообщил об ошибке скачивания файла по ссылке.",
    polling_unavailable: "Не удалось проверить статус операции в On-premises.",
    onprem_https_required: "Для удалённого доступа к утилите требуется HTTPS.",
    onprem_origin_rejected: "Источник запроса не совпадает с адресом утилиты.",
    job_busy: "Задание сейчас выполняет другое действие. Обновите состояние.",
    cache_in_use: "Кеш используется переносом. Дождитесь завершения, нажмите «Пауза» и дождитесь состояния «Приостановлено» или полностью остановите задание.",
    cache_cleanup_failed: "Не удалось полностью очистить локальный кеш. Проверьте права доступа к каталогу кеша и повторите попытку.",
    active_or_unknown_attempt: "Файл сейчас обрабатывается. Дождитесь завершения текущего действия и повторите попытку.",
    invalid_paths_or_destination: "Проверьте исходный путь, папку назначения и адрес API.",
    managed_batch_paused: "Список приостановлен. Выберите «Список дисков из CSV» и продолжите его.",
    managed_batch_not_active: "Источник входит в список. Откройте список в истории и запустите или продолжите его.",
    managed_batch_start_required: "Источник входит в список. Начните перенос всего выбранного списка после Dry Run.",
    invalid_request: "Проверьте обязательные поля запроса.",
    invalid_source_email: "Укажите корректный email источника.",
    invalid_config: "Проверьте настройки источника и получателя.",
    invalid_source_public: "Публичное скачивание доступно только при переносе на общий диск.",
    invalid_credentials: "Проверьте введённые доступы.",
    invalid_operation_reference: "Утилита не смогла распознать ссылку на статус операции On-premises.",
    attempt_not_found: "Попытка не найдена. Обновите задание.",
    entry_not_found: "Файл не найден в этом задании. Обновите его состояние.",
    attempt_not_retryable: "Повтор этого файла сейчас недоступен. Обновите задание.",
    job_not_found: "Задание не найдено. Обновите историю.",
    job_not_stoppable: "Задание сейчас нельзя остановить. Обновите его состояние.",
    job_not_restartable: "Новый перенос доступен после окончательной остановки или завершения задания.",
    restart_config_mismatch: "Для нового переноса сохраните первоначальные параметры выбранного задания.",
    job_stopped: "Задание окончательно остановлено. Подготовьте новый перенос.",
    onprem_unavailable: "Режим on-premises временно недоступен.",
    directory_confirmation_pending: "Создание каталога ещё не подтверждено. Повторите ошибочные объекты для безопасной проверки.",
    source_unavailable: "Исходный диск SaaS недоступен; запрос не завершён.",
    source_invalid_response: "Исходный диск SaaS вернул некорректный ответ.",
    source_download_failed: "Не удалось скачать файл с исходного диска SaaS.",
    source_publication_denied: "SaaS запретил публикацию исходного ресурса.",
    source_publication_failed: "Не удалось опубликовать папку или файл в SaaS.",
    source_publication_outcome_unknown: "SaaS не подтвердил результат публикации; требуется проверка.",
    source_publication_not_confirmed: "Публичный доступ к исходному ресурсу SaaS не подтверждён.",
    source_public_resource_unavailable: "Ресурс SaaS по публичной ссылке недоступен.",
    source_public_access_denied: "SaaS отказал в доступе по публичной ссылке.",
    source_public_download_limit: "SaaS ограничил скачивание по публичной ссылке.",
    source_public_download_failed: "Не удалось скачать файл по публичной ссылке SaaS.",
    source_public_invalid_response: "SaaS вернул некорректный ответ на публичный запрос.",
    source_content_changed: "Размер или MD5 файла, скачанного с SaaS, расходятся с планом.",
    source_size_unavailable: "Исходный диск SaaS не сообщил размер файла.",
    invalid_source_tree: "Структура диска SaaS не совпала с выбранным путём.",
    unsupported_resource: "Тип ресурса не поддерживается для переноса.",
    invalid_download_url: "Диск SaaS вернул некорректную ссылку скачивания.",
    destination_not_found: "Файл или папка на диске On-premises не найдены.",
    destination_unavailable: "Диск On-premises недоступен; запрос не завершён.",
    destination_timeout: "Истекло время ожидания ответа диска On-premises.",
    destination_connection_failed: "Не удалось установить соединение с диском On-premises.",
    destination_stream_interrupted: "Соединение с On-premises прервалось при передаче данных.",
    final_upload_confirmation_required: "On-premises не подтвердил загрузку файла. Проверьте доступность диска и повторите файл.",
    final_move_confirmation_required: "On-premises не подтвердил перемещение файла. Проверьте доступность диска и повторите файл.",
    point_retry_not_available: "Для этого файла точечный повтор сейчас недоступен.",
    point_retry_required: "Для прерванной передачи используйте кнопку «Проверить и повторить» у файла.",
    point_retry_in_progress: "Точечный повтор файла уже выполняется.",
    destination_invalid_response: "Диск On-premises вернул некорректный ответ.",
    destination_file_too_large: "Размер файла превышает лимит On-premises.",
    destination_rate_limited: "Превышен лимит запросов к диску On-premises.",
    destination_quota_exceeded: "На диске On-premises недостаточно свободного места.",
    invalid_upload_url: "Диск On-premises вернул некорректную ссылку загрузки.",
    invalid_operation_status: "On-premises вернул неизвестный статус операции.",
    technical_path_collision: "Путь исходного ресурса совпадает со служебной папкой.",
    path_collision: "Несколько исходных ресурсов получили один целевой путь.",
    staging_file_unavailable: "Файл отсутствует в локальном кеше или повреждён. Повторное скачивание возможно после очистки кеша; потребуется доступ к SaaS.",
    staging_io_failed: "Ошибка записи файла в локальный кеш.",
    invalid_staging_object: "В локальном кеше повреждена структура или найден неподдерживаемый объект. Проверьте каталог кеша на машине утилиты.",
    quota_unavailable: "Не удалось получить сведения о свободном месте On-premises.",
    source_connection_failed: "Не удалось установить соединение с диском SaaS.",
    source_timeout: "Истекло время ожидания ответа диска SaaS.",
    source_stream_interrupted: "Соединение с SaaS прервалось при скачивании файла.",
    source_download_timeout: "Истекло время ожидания скачивания файла с SaaS.",
    internal_error: "Внутренняя ошибка утилиты; стадия указана в журнале.",
    migration_failed: "Перенос не завершён из-за ошибки.",
  };
  const stageLabels = {
    prepare_clients: "Подключение к дискам", planning: "Чтение структуры исходного диска",
    claim_namespace: "Создание служебной папки", create_directory: "Создание папки",
    check_source: "Проверка исходного файла", check_destination: "Проверка целевого файла",
    check_destination_access: "Проверка доступности выбранного диска On-premises",
    abandon_attempt: "Локальная попытка прервана; API не поддерживает отмену удалённой операции",
    prepare_upload_directory: "Подготовка папки для загрузки", downloading: "Скачивание в локальный кеш",
    get_download_url: "Получение ссылки для скачивания", submit_url_upload: "Запуск загрузки по ссылке",
    upload_file: "Загрузка файла в On-premises", poll_transfer: "Ожидание загрузки",
    verify_temporary: "Проверка загруженной временной копии", prepare_destination_directory: "Подготовка целевой папки",
    move_file: "Перемещение в целевую папку", poll_move: "Ожидание перемещения",
    verify_destination: "Проверка итогового файла", cleanup: "Очистка временных файлов",
    download_retry: "Повтор скачивания",
    point_retry: "Проверка и повтор файла",
    publish_source: "Публикация исходного ресурса",
    public_download_link: "Получение публичной ссылки скачивания",
  };
  const stageStatuses = {
    started: "Начато", succeeded: "Успешно", failed: "Ошибка",
    outcome_unknown: "Исход неизвестен", interrupted: "Прервано", waiting: "Ожидание",
    skipped: "Пропущен", cancelled: "Отменено",
  };
  const eventLabels = {
    publication_checkpoint: "Публичный доступ к источнику",
    job_created: "Задание создано", job_state: "Состояние задания", job_updated: "Задание обновлено",
    state_changed: "Состояние изменено", job_started: "Перенос запущен",
    planning_started: "Планирование начато", plan_ready: "План готов",
    credentials_attached: "Доступ предоставлен", pause_requested: "Запрошена пауза",
    job_paused: "Задание приостановлено", job_resumed: "Задание продолжено",
    stop_requested: "Запрошена окончательная остановка", full_stop_requested: "Запрошена полная остановка",
    local_operations_cancelled: "Локальные действия отменены",
    job_stopped: "Задание остановлено",
    job_restarted: "Создан новый перенос",
    attempt_created: "Создана попытка", attempt_started: "Передача начата",
    attempt_updated: "Попытка изменена",
    operation_started: "Операция запущена", operation_finished: "Операция завершена",
    entry_state: "Состояние объекта", entry_verified: "Объект проверен",
    entry_done: "Объект перенесён",
    outcome_unknown: "Исход операции неизвестен", cleanup: "Локальная очистка",
    local_cleanup: "Локальная очистка", error: "Ошибка",
    transfer_stage: "Этап переноса",
    // Подписи старых событий сохранены для чтения истории. Ручное разрешение
    // исхода удалено; новых действий через эти подписи интерфейс не запускает.
    attempt_resolved: "Исход попытки установлен",
    administrative_resolution: "Администратор установил исход операции",
  };

  let enabled = false;
  let authRequired = false;
  let selected = null;
  let entries = [];
  let pointRetryButtons = [];
  let entryOffset = 0;
  const entryPageSize = 100;
  let history = [];
  let historyOffset = 0;
  let historyHasOlder = false;
  const historyPageSize = 50;
  let busy = false;
  let generation = 0;
  let eventSequence = 0;
  let logLines = [];
  let pollTimer = null;
  let pendingCreate = null;
  const pendingRestarts = new Map();
  let verifiedDestination = null;
  let verifiedBinding = null;
  let destinationCheckRevision = 0;

  function errorCode(value) {
    if (typeof value === "string") return value;
    if (value && typeof value.code === "string") return value.code;
    return "";
  }

  function errorText(value, historical = false) {
    if (!historical && errorCode(value) === "remote_operation_still_pending" && selected?.full_stop_requested === true && selected.state === "stopped") {
      return "On-premises: прежняя операция ещё выполняется. Локальные действия отменены; общая очередь свободна, пути не блокируются.";
    }
    if (errorCode(value) === "credentials_required" && oldWorkStopped(selected)) {
      return "Задание остановлено. Для переноса подготовьте новое задание.";
    }
    if (errorCode(value) === "request_rejected") {
      const status = Number.isInteger(value?.status) && value.status >= 100 && value.status <= 599
        ? ` (HTTP ${value.status})` : "";
      return `Запрос отклонён${status}. Обновите задание и проверьте доступность действия.`;
    }
    const text = errors[errorCode(value)] || "Операция не выполнена. Проверьте доступы и состояние задания.";
    const diagnostic = Object.hasOwn(errors, errorCode(value)) ? journalDiagnostic(value?.diagnostic_message) : "";
    return diagnostic ? `${text} ${diagnostic}` : text;
  }

  function failure(code, status, diagnostic) {
    const error = new Error("onprem_request_failed");
    error.code = code;
    if (Number.isInteger(status) && status >= 100 && status <= 599) error.status = status;
    const message = journalDiagnostic(diagnostic);
    if (message) error.diagnostic_message = message;
    return error;
  }

  function notice(text, isError = false) {
    byId("onprem-notice").textContent = text;
    byId("onprem-notice").className = isError ? "error" : "";
  }

  function pollNotice(text) {
    const node = byId("onprem-poll-notice");
    if (!node) return;
    node.textContent = text;
    node.hidden = !text;
  }

  function authorized() {
    return !authRequired || Boolean(byId("onprem-access-key").value.trim());
  }

  function active() {
    return enabled && tab.classList.contains("active") && !document.hidden && byId("onprem-source-input")?.value !== "list";
  }

  async function request(path, options = {}) {
    if (!authorized()) throw failure("onprem_access_required");
    const headers = {Accept: options.blob ? "text/csv" : "application/json"};
    const key = byId("onprem-access-key").value.trim();
    if (key) headers["X-Onprem-Key"] = key;
    if (options.body !== undefined) headers["Content-Type"] = "application/json";
    let response;
    try {
      response = await fetch("/api/onprem" + path, {
        method: options.method || "GET", headers,
        body: options.body === undefined ? undefined : JSON.stringify(options.body),
        credentials: "same-origin", cache: "no-store", redirect: "error",
      });
    } catch (_) {
      throw failure("network_error");
    }
    if (!response.ok) {
      const status = Number.isInteger(response.status) && response.status >= 100 && response.status <= 599
        ? response.status : undefined;
      let code = status >= 500 ? "unavailable" : "request_rejected";
      let diagnostic;
      try {
        const body = await response.json();
        if (typeof body.detail === "string" && Object.hasOwn(errors, body.detail)) {
          code = body.detail;
          diagnostic = body.diagnostic_message;
        }
      } catch (_) {}
      throw failure(code, status, diagnostic);
    }
    if (options.blob) return response.blob();
    if (response.status === 204) return null;
    try { return await response.json(); } catch (_) { throw failure("invalid_response"); }
  }

  function readConfig() {
    const config = {};
    fields.forEach((field) => { config[field.dataset.onpremConfig] = field.value.trim(); });
    config.strategy = "staged";
    config.destination_type = "shared";
    config.destination_base = "/";
    config.identity_confirmed = byId("onprem-identity-confirmed").checked;
    config.source_public = true;
    if (selected?.config?.source_type === "shared" && config.source_email === selected.config.source_email) {
      config.source_type = "shared";
      config.source_vd_hash = selected.config.source_vd_hash;
      config.source_folder = selected.config.source_folder;
      config.source_public = false;
      config.strategy = "staged";
    }
    return config;
  }

  function sharedConfig(config) { return config?.destination_type === "shared"; }

  function friendlyPath(value) {
    const path = String(value || "/");
    return path.startsWith("disk:") ? path.slice(5) || "/" : path;
  }

  function comparable(key, value) {
    let text = String(value ?? "").trim();
    if (["source_path", "destination_base"].includes(key)) {
      text = friendlyPath(text);
      if (!text.startsWith("/")) text = "/" + text;
      return text.replace(/\/+$/, "") || "/";
    }
    if (key === "destination_url") {
      try {
        const url = new URL(text);
        const path = url.pathname.replace(/\/+$/, "");
        url.pathname = path === "" || path === "/v1/disk" ? "/v1/disk" : path;
        return url.href;
      } catch (_) { return text; }
    }
    return text;
  }

  function bindingKey(config) {
    return JSON.stringify(fields.map((field) => {
      const key = field.dataset.onpremConfig;
      return [key, comparable(key, config[key])];
    }));
  }

  function checkedDestination(config) {
    return sharedConfig(config) && verifiedBinding === bindingKey(config) ? verifiedDestination : null;
  }

  function destinationLabel(config) {
    if (!sharedConfig(config)) return "Личный Диск On-premises";
    const name = checkedDestination(config)?.name;
    return `Общий диск On-premises ${name ? "«" + name + "» " : ""}[${config.destination_vd_hash || "метка не указана"}]`;
  }

  function destinationPath(value, config) {
    let path = String(value || "/");
    const prefix = `vd:${config?.destination_vd_hash}:disk:`;
    if (sharedConfig(config) && path.startsWith(prefix)) path = path.slice(prefix.length);
    path = friendlyPath(path);
    return `${destinationLabel(config)} · ${path}`;
  }

  function destinationFolder(config) {
    const base = friendlyPath(config.destination_base).replace(/^\/+|\/+$/g, "");
    return "/" + (base ? base + "/" : "") + (config.source_folder || config.source_email || "<email сотрудника>");
  }

  function updateDestinationPreview() {
    const node = byId("onprem-destination-preview");
    if (!node) return;
    const config = readConfig();
    node.textContent = "Для нового переноса: " + destinationPath(destinationFolder(config), config);
    if (selected && sharedConfig(selected.config) && originalConfigMatches() &&
        comparable("destination_base", selected.config.destination_base) !== "/") {
      node.textContent += ". Текущее задание: " + destinationPath(selected.config.destination_root, selected.config);
    }
  }

  function clearDestinationCheck() {
    destinationCheckRevision += 1;
    verifiedDestination = null;
    verifiedBinding = null;
    const node = byId("onprem-destination-check");
    if (node) { node.hidden = true; node.textContent = ""; }
  }

  async function checkDestination(config) {
    clearDestinationCheck();
    const snapshot = destinationCheckRevision;
    const result = await request("/preflight", {method: "POST", body: {config, credentials: credentials()}});
    if (snapshot !== destinationCheckRevision || bindingKey(readConfig()) !== bindingKey(config)) {
      throw failure("credential_binding_mismatch");
    }
    const target = result?.destination;
    if (target?.type !== "shared" || target.vd_hash !== config.destination_vd_hash) {
      throw failure("destination_shared_disk_mismatch");
    }
    const permissions = target.permissions;
    const read = Array.isArray(permissions) ? permissions.includes("read")
      : typeof permissions?.read === "boolean" ? permissions.read : null;
    const write = Array.isArray(permissions) ? permissions.includes("write")
      : typeof permissions?.write === "boolean" ? permissions.write : null;
    if (read === false) throw failure("destination_shared_access_required");
    if (write === false) throw failure("destination_read_only");
    const rights = read === true && write === true ? "Чтение и запись разрешены" : "API не указал права";
    verifiedDestination = target;
    verifiedBinding = bindingKey(config);
    const space = typeof target.free_bytes === "number" && Number.isFinite(target.free_bytes) && target.free_bytes >= 0
      ? "Свободно: " + bytes(target.free_bytes) : "Свободное место неизвестно";
    const node = byId("onprem-destination-check");
    if (node) {
      node.textContent = `${destinationLabel(config)}. ${rights}. ${space}. Папка: ${friendlyPath("disk:" + destinationFolder(config))}`;
      node.hidden = false;
    }
    updateDestinationPreview();
  }

  function credentials() {
    const value = {
      source_client_secret: byId("onprem-source-secret").value,
      destination_token: byId("onprem-destination-token").value,
    };
    const admin = byId("onprem-source-admin-token").value;
    if (admin.trim()) value.source_admin_token = admin;
    return value;
  }

  function clearCredentials() {
    byId("onprem-source-secret").value = "";
    byId("onprem-source-admin-token").value = "";
    byId("onprem-destination-token").value = "";
  }

  function fillConfig(config) {
    fields.forEach((field) => {
      const key = field.dataset.onpremConfig;
      const value = key === "strategy" ? "staged" : config[key] || (["source_path", "destination_base"].includes(key) ? "/" : "");
      field.value = ["source_path", "destination_base"].includes(key) ? friendlyPath(value) : value;
    });
    byId("onprem-identity-confirmed").checked = false;
    const sharedSource = config.source_type === "shared";
    const label = byId("onprem-source-email-label");
    if (label) label.textContent = sharedSource ? "Email сотрудника с доступом к общему диску SaaS" : "Email владельца личного Диска SaaS";
    const note = byId("onprem-source-folder-note");
    if (note) note.textContent = sharedSource ? `Источник: общий диск [${config.source_vd_hash || ""}]. Папка назначения: ${config.source_folder || ""}.` : "Для каждого email создаётся собственная папка на общем диске; структура исходных папок сохраняется.";
    clearDestinationCheck();
    updateDestinationPreview();
    clearCredentials();
  }

  function bytes(value) {
    const number = Number(value);
    if (!Number.isFinite(number) || number < 0) return "—";
    if (number < 1024) return `${number.toLocaleString("ru-RU")} Б`;
    const units = ["Кб", "Мб", "Гб", "Тб"];
    let amount = number / 1024;
    let index = 0;
    while (amount >= 1024 && index < units.length - 1) { amount /= 1024; index += 1; }
    return `${amount.toLocaleString("ru-RU", {maximumFractionDigits: 1})} ${units[index]}`;
  }

  function transferRate(value, active) {
    return active === true && typeof value === "number" && Number.isFinite(value) && value >= 0
      ? `${bytes(value)}/с` : "—";
  }

  function stateLabel(state) { return labels[state] || "Состояние уточняется"; }

  function validDownloadAttempts(value) {
    return Number.isInteger(value) && value >= 1 && value <= 5;
  }

  function retryDate(value) {
    return Number.isSafeInteger(value) && value > 0 && value <= 8640000000000000
      ? new Date(value) : null;
  }

  function isStopping(job) { return Boolean((job?.stop_requested || job?.full_stop_requested === true) && job.state !== "stopped"); }

  function oldWorkStopped(job) { return Boolean(job?.stop_requested || job?.full_stop_requested === true || job?.state === "stopped"); }

  function requiresFullStop(job) {
    return job?.state === "stopped" && (job.remote_pending === true || job.stop_blocked === true);
  }

  function originalConfigMatches() {
    return Boolean(selected?.config && bindingKey(readConfig()) === bindingKey(selected.config));
  }

  function hasBlocker(job) {
    const waiting = job?.state === "running" ||
      (job?.state === "awaiting_credentials" && job.resume_state === "running");
    return waiting && job.blocked_by !== null && job.blocked_by !== undefined;
  }

  function isWaiting(job) {
    return job?.state === "running" && hasBlocker(job);
  }

  function blockingJob(job) {
    const blocker = job?.blocked_by;
    if (!hasBlocker(job) || !blocker || typeof blocker !== "object" || Array.isArray(blocker) ||
        typeof blocker.job_id !== "string" || !jobIdPattern.test(blocker.job_id) || blocker.job_id === job.id ||
        !["operation_outcome_unknown", "operation_in_progress"].includes(blocker.reason) ||
        !["queued", "planning", "ready", "running", "paused", "awaiting_credentials", "needs_attention",
          "completed", "completed_with_issues", "stopped"].includes(blocker.state)) return null;
    return blocker;
  }

  function jobStateLabel(job) {
    return isStopping(job) ? "Остановка запрошена"
      : job.state === "stopped" && job.full_stop_requested === true ? "Остановлено полностью"
      : isWaiting(job) ? "Ожидание другого задания" : stateLabel(job.state);
  }

  function jobDateLabel(job) {
    if (typeof job?.created_at !== "string" || !job.created_at.trim()) return "без даты";
    const date = new Date(job.created_at);
    if (!Number.isFinite(date.getTime())) return "без даты";
    return "от " + date.toLocaleString("ru-RU", {
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", minute: "2-digit", second: "2-digit",
    });
  }

  function renderBlocked() {
    const blocked = hasBlocker(selected);
    const blocker = blockingJob(selected);
    byId("onprem-job-blocked").hidden = !blocked;
    byId("onprem-job-blocked-reason").textContent = blocked
      ? blocker
        ? blocker.reason === "operation_outcome_unknown"
          ? blocker.state === "stopped"
            ? "Предыдущее задание удерживает очередь. Откройте его и нажмите «Остановить»."
            : "Другое задание требует проверки. Откройте его, повторите файл или остановите задание."
          : "Другое задание ещё не завершило операцию или проверку результата. Перенос ожидает освобождения очереди."
        : "Задание ожидает освобождения очереди. Обновите состояние, чтобы узнать причину."
      : "";
    const blockingHistoryJob = blocker && history.find((job) => job.id === blocker.job_id);
    byId("onprem-job-blocked-holder").textContent = blocker
      ? `${blockingHistoryJob ? "Задание " + jobDateLabel(blockingHistoryJob) : "Другое задание"} · ${stateLabel(blocker.state)}` : "";
  }

  function resultClass(state) {
    if (["done", "already_verified", "created", "success"].includes(state)) return "good";
    if (["failed", "conflict", "source_changed"].includes(state)) return "bad";
    if (["outcome_unknown", "verification_unavailable"].includes(state)) return "attention";
    return "";
  }

  function renderHistory() {
    const select = byId("onprem-history");
    select.replaceChildren();
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = history.length ? "Выберите задание" : "Заданий пока нет";
    select.appendChild(placeholder);
    const jobs = selected ? [selected, ...history.filter((job) => job.id !== selected.id)] : history;
    jobs.forEach((job) => {
      const option = document.createElement("option");
      option.value = job.id;
      option.textContent = `Задание ${jobDateLabel(job)} · ${job.config?.source_email || "Источник не указан"} · ${jobStateLabel(job)}`;
      select.appendChild(option);
    });
    select.value = selected ? selected.id : "";
    const page = Math.floor(historyOffset / historyPageSize) + 1;
    byId("onprem-history-page").textContent = history.length
      ? `Страница ${page} · задания ${historyOffset + 1}–${historyOffset + history.length}`
      : `Страница ${page} · записей нет`;
  }

  function renderJob() {
    renderBlocked();
    const legacy = selected && (!sharedConfig(selected.config) || selected.config.strategy !== "staged");
    byId("onprem-legacy-note").hidden = !legacy;
    byId("onprem-legacy-note").textContent = legacy
      ? !sharedConfig(selected.config) ? errors.legacy_personal_job : errors.onprem_staged_required : "";
    ["onprem-job", "onprem-log-panel"].forEach((id) => {
      byId(id).hidden = !selected;
    });
    if (!selected) { updateActions(); return; }
    byId("onprem-job-id").textContent = jobDateLabel(selected);
    byId("onprem-job-state").textContent = jobStateLabel(selected);
    byId("onprem-job-state").className = "onprem-result " + (isWaiting(selected) ? "attention" : resultClass(selected.state));
    const stop = byId("onprem-job-stop");
    stop.hidden = !oldWorkStopped(selected);
    stop.textContent = selected.full_stop_requested === true
      ? isStopping(selected)
        ? "Полная остановка запрошена. Дождитесь подтверждения локальной остановки; очередь ещё не освобождена."
        : selected.remote_pending === true
          ? "Все незавершённые локальные действия отменены, фоновые проверки остановлены. Общая очередь освобождена; пути не блокируются. Принятая сервером операция может продолжаться: API не предоставляет её отмену."
          : "Все незавершённые локальные действия отменены, фоновые проверки остановлены. Общая очередь освобождена; пути не блокируются. История, локальный кеш и файлы сохранены."
      : isStopping(selected)
        ? "Остановка запрошена. Ожидается подтверждение локальной остановки; принятая сервером операция может продолжаться."
        : requiresFullStop(selected)
          ? "Локальная работа остановлена. Чтобы освободить очередь для нового переноса, нажмите «Остановить»."
          : "Локальная работа остановлена. Исходники, файлы под исходным именем в целевой папке, история и кеш сохранены.";
    const counts = Object.entries(selected.counts || {}).filter(([, value]) => Number.isFinite(Number(value)));
    byId("onprem-job-counts").textContent = counts.length
      ? counts.map(([state, count]) => `${state === "total" ? "Всего" : stateLabel(state)}: ${count}`).join(" · ")
      : "План готовится";
    if (Number(selected.total_directories) > 0) {
      byId("onprem-job-counts").textContent += ` · Каталогов: ${selected.total_directories}`;
    }
    byId("onprem-job-bytes").textContent = `${bytes(selected.verified_bytes)} / ${bytes(selected.total_bytes)}`;
    const staged = selected.config.strategy === "staged";
    const rates = selected.state === "running" && !oldWorkStopped(selected) ? selected.transfer_rates : null;
    byId("onprem-download-rate-row").hidden = !staged;
    byId("onprem-download-rate").textContent = staged
      ? transferRate(rates?.download_bytes_per_second, rates?.download_active) : "—";
    byId("onprem-upload-rate").textContent = staged
      ? transferRate(rates?.upload_bytes_per_second, rates?.upload_active) : "Недоступна через API";
    byId("onprem-upload-rate").title = staged
      ? "Скорость передачи файла в On-premises; подтверждённый объём показан отдельно в «Проверено байтов»."
      : "Файл скачивает сервер On-premises; API сообщает только статус операции";
    byId("onprem-job-destination").textContent = `${selected.config.source_email || "Источник"} (SaaS) → ${destinationPath(selected.config.destination_root, selected.config)}`;
    byId("onprem-job-error").hidden = !selected.error;
    byId("onprem-job-error").textContent = selected.error ? errorText(selected.error) : "";
    const pause = byId("onprem-job-pause");
    pause.hidden = !selected.pause_requested || selected.state === "paused" || oldWorkStopped(selected);
    if (selected.state === "awaiting_credentials" && selected.resume_state === "paused") {
      pause.textContent = "Перенос приостановлен.";
    } else if (hasBlocker(selected) ||
        (selected.state === "awaiting_credentials" && selected.resume_state === "running")) {
      pause.textContent = "Пауза запрошена. Ожидается остановка задания.";
    } else {
      pause.textContent = "Пауза запрошена. Текущая операция ещё выполняется.";
    }
    renderHistory();
    renderEntries();
    updateActions();
  }

  function renderEntries() {
    const body = byId("onprem-entry-body");
    body.replaceChildren();
    pointRetryButtons = [];
    if (!entries.length) {
      const row = document.createElement("tr");
      const cell = document.createElement("td");
      cell.colSpan = 5;
      cell.textContent = ["queued", "planning", "awaiting_credentials"].includes(selected?.state)
        ? "План ещё не готов" : "В плане нет объектов";
      row.appendChild(cell);
      body.appendChild(row);
    }
    entries.forEach((entry) => {
      const row = document.createElement("tr");
      [entry.source_path, destinationPath(entry.destination_path, selected.config),
        entry.kind === "dir" || entry.kind === "directory" ? "Каталог" : bytes(entry.size),
        entry.md5 || "—"].forEach((value, index) => {
        const cell = document.createElement("td");
        cell.textContent = value || "—";
        if (index < 2 || index === 3) cell.className = "onprem-path";
        row.appendChild(cell);
      });
      const result = document.createElement("td");
      const state = document.createElement("span");
      const retryAt = entry.state === "downloading" && selected.state === "running" && !oldWorkStopped(selected)
        ? retryDate(entry.retry_at_ms) : null;
      state.className = "onprem-result " + (retryAt ? "attention" : resultClass(entry.state));
      state.textContent = retryAt ? "Ожидание повтора" : stateLabel(entry.state);
      result.appendChild(state);
      if (validDownloadAttempts(entry.download_attempts)) {
        const counter = document.createElement("div");
        counter.textContent = `Скачивание: ${entry.download_attempts}/5`;
        result.appendChild(counter);
      }
      if (retryAt) {
        const wait = document.createElement("div");
        wait.textContent = `Следующая попытка через ${Math.max(0, Math.ceil((retryAt.getTime() - Date.now()) / 1000))} с (${retryAt.toLocaleString("ru-RU")})`;
        result.appendChild(wait);
      }
      if (validPointRetryAttempts(entry.point_retry_attempts) && (entry.point_retry_requested || entry.point_retry_attempts > 0)) {
        const counter = document.createElement("div");
        counter.textContent = `Точечный повтор: ${entry.point_retry_attempts}/3`;
        result.appendChild(counter);
      }
      if (entry.point_retry_requested && !oldWorkStopped(selected) && retryDate(entry.retry_at_ms)) {
        const wait = document.createElement("div");
        wait.textContent = `Следующая попытка: ${retryDate(entry.retry_at_ms).toLocaleString("ru-RU")}`;
        result.appendChild(wait);
      }
      if (entry.error) {
        const error = document.createElement("span");
        error.textContent = errorText(entry.error);
        result.appendChild(error);
      }
      if (entry.attempt_id) {
        const attempt = document.createElement("div");
        attempt.className = "onprem-path";
        attempt.textContent = `Попытка: ${entry.attempt_id}`;
        result.appendChild(attempt);
      }
      if (entry.kind === "file" && entry.can_retry === true && !oldWorkStopped(selected) && sharedConfig(selected.config)) {
        const retry = document.createElement("button");
        retry.type = "button";
        retry.className = "secondary-btn";
        retry.textContent = "Проверить и повторить";
        retry.title = `Проверить и повторить файл: ${entry.source_path}`;
        retry.addEventListener("click", () => retryEntry(entry));
        pointRetryButtons.push({button: retry, entry});
        result.appendChild(retry);
      }
      row.appendChild(result);
      body.appendChild(row);
    });
    const total = entryTotal();
    byId("onprem-entry-count").textContent = entries.length
      ? `Объекты ${entryOffset + 1}–${entryOffset + entries.length} из ${total}`
      : `Объектов в плане: ${total}`;
    byId("onprem-entry-page").textContent = `Страница ${Math.floor(entryOffset / entryPageSize) + 1} из ${Math.max(1, Math.ceil(total / entryPageSize))}`;
  }

  function entryTotal() {
    if (Number.isFinite(Number(selected?.total_files)) && Number.isFinite(Number(selected?.total_directories))) {
      return Number(selected.total_files) + Number(selected.total_directories);
    }
    const count = [...Object.values(selected?.counts || {}), ...Object.values(selected?.directory_counts || {})]
      .reduce((sum, value) => sum + (Number(value) || 0), 0);
    return Math.max(count, entryOffset + entries.length);
  }

  function hasState(state) {
    return Number(selected?.counts?.[state]) > 0 || Number(selected?.directory_counts?.[state]) > 0 ||
      entries.some((entry) => entry.state === state);
  }

  function effectiveState() {
    return selected?.state === "awaiting_credentials" ? selected.resume_state : selected?.state;
  }

  function hasRunnableEntries() {
    return ["planned", "transferring", "waiting_operation", "verifying", "committing", "downloading", "staged"].some(hasState);
  }

  function retryFailedEntries() {
    return !pointRetryActive() && ["paused", "needs_attention", "completed_with_issues"].includes(effectiveState()) &&
      hasState("failed") && !hasRunnableEntries() && !hasState("outcome_unknown");
  }

  function accessReady() {
    return originalConfigMatches() && byId("onprem-identity-confirmed").checked &&
      Boolean(credentials().source_client_secret.trim() && credentials().destination_token.trim()) &&
      sharedConfig(selected?.config) && byId("onprem-form").checkValidity();
  }

  function validPointRetryAttempts(value) {
    return Number.isInteger(value) && value >= 0 && value <= 3;
  }

  function pointRetryActive() {
    return selected?.point_retry_active === true || entries.some((entry) => entry.point_retry_requested === true);
  }

  function pointRetryAvailable(entry) {
    return !busy && enabled && authorized() && selected && sharedConfig(selected.config) && selected.config.strategy === "staged" &&
      !oldWorkStopped(selected) && accessReady() && !pointRetryActive() &&
      entries.includes(entry) && entry?.kind === "file" && entry.can_retry === true;
  }

  function retryEntry(entry) {
    if (!pointRetryAvailable(entry)) return;
    const id = selected.id;
    const entryId = entry.id;
    const attemptId = entry.attempt_id ?? null;
    action(async () => {
      await attachAccess(entryId);
      const snapshot = generation;
      const accessRevision = destinationCheckRevision;
      await refreshSelected();
      const current = await request(`/jobs/${encodeURIComponent(id)}/entries/${encodeURIComponent(entryId)}`);
      if (snapshot !== generation || accessRevision !== destinationCheckRevision) throw failure("credential_binding_mismatch");
      if (!selected || selected.id !== id || current?.id !== entryId || (current.attempt_id ?? null) !== attemptId ||
          current.can_retry !== true || current.kind !== "file" || pointRetryActive() || !accessReady() || oldWorkStopped(selected)) {
        throw failure("point_retry_not_available");
      }
      await mutate(`entries/${encodeURIComponent(entryId)}/retry`, {attempt_id: attemptId, revision: selected.revision});
      notice("Проверка и повтор выбранного файла запущены: до трёх попыток. Ход и причины ошибок записываются в журнал.");
    });
  }

  function updateActions() {
    const locked = busy || !enabled || !authorized();
    const legacy = selected && (!sharedConfig(selected.config) || selected.config.strategy !== "staged");
    const confirmed = byId("onprem-identity-confirmed").checked;
    const state = selected?.state;
    const stopped = oldWorkStopped(selected);
    const fullStopNeeded = requiresFullStop(selected);
    const unknown = hasState("outcome_unknown");
    const runnable = hasRunnableEntries();
    const namespaceBlocked = ["technical_namespace_exists", "technical_namespace_outcome_unknown"].includes(errorCode(selected?.error)) ||
      ["intent", "unknown"].includes(selected?.namespace_state);
    const access = accessReady();
    const restoringAccess = ["paused", "awaiting_credentials"].includes(state) && (runnable || pointRetryActive());
    byId("onprem-plan").disabled = locked || !confirmed;
    byId("onprem-refresh").disabled = locked;
    byId("onprem-history").disabled = locked;
    byId("onprem-history-newer").disabled = locked || historyOffset === 0;
    byId("onprem-history-older").disabled = locked || !historyHasOlder;
    byId("onprem-open-blocker").disabled = locked || !blockingJob(selected);
    byId("onprem-start").disabled = locked || legacy || !access || stopped || unknown || effectiveState() !== "ready";
    byId("onprem-pause").disabled = locked || legacy || stopped || state !== "running" || selected?.pause_requested;
    byId("onprem-stop").disabled = locked || selected?.full_stop_requested === true ||
      !(["queued", "planning", "ready", "running", "paused", "awaiting_credentials", "needs_attention", "completed_with_issues"].includes(state) || fullStopNeeded);
    byId("onprem-restart").disabled = locked || legacy || isStopping(selected) || !access ||
      !["stopped", "completed", "completed_with_issues"].includes(state);
    byId("onprem-resume").disabled = locked || legacy || !originalConfigMatches() || (!access && !restoringAccess) || stopped ||
      (unknown && !pointRetryActive() && !runnable) || namespaceBlocked ||
      !(state === "paused" || state === "needs_attention" || state === "awaiting_credentials" || retryFailedEntries());
    byId("onprem-report").disabled = locked || !selected;
    byId("onprem-journal").disabled = locked || !selected;
    byId("onprem-cleanup").disabled = locked;
    byId("onprem-entry-previous").disabled = locked || !selected || entryOffset === 0;
    byId("onprem-entry-next").disabled = locked || !selected || entryOffset + entries.length >= entryTotal();
    pointRetryButtons.forEach(({button, entry}) => {button.disabled = !pointRetryAvailable(entry);});

  }

  function journalPath(value) {
    return typeof value === "string" && value.length <= 4096 &&
      /^(?:\/|disk:\/|vd:[A-Za-z0-9_-]+:disk:\/)/.test(value) &&
      !/[\x00-\x1f\x7f]|https?:\/\/|bearer\s|(?:token|secret|password|signature)=/i.test(value) ? value : "";
  }

  function journalDiagnostic(value) {
    if (!(typeof value === "string" && value.length <= 1200 &&
      !/[\x00-\x1f\x7f<>]|[a-z][a-z0-9+.-]*:\/\/|bearer\s|oauth\s|(?:token|secret|password|signature|authorization|assertion|credential)s?\s*[:=]/i.test(value)
    )) return "";
    const secrets = [...Object.values(credentials()), byId("onprem-access-key")?.value];
    for (const secret of secrets) {
      if (secret && secret.length >= 4) value = value.split(secret).join("[скрыто]");
    }
    return value;
  }

  function appendEvents(events) {
    (events || []).slice().sort((a, b) => a.sequence - b.sequence).forEach((event) => {
      const sequence = Number(event.sequence);
      if (!Number.isSafeInteger(sequence) || sequence <= eventSequence) return;
      const payload = event.payload || {};
      const context = event.context || {};
      const detail = [];
      if (Object.hasOwn(stageLabels, payload.stage)) detail.push(stageLabels[payload.stage]);
      if (Object.hasOwn(stageStatuses, payload.status)) detail.push(stageStatuses[payload.status]);
      if (payload.state) detail.push(stateLabel(payload.state));
      if (event.type === "local_operations_cancelled" && Number.isSafeInteger(payload.count) && payload.count >= 0) {
        detail.push(`Действий: ${payload.count}`);
      }
      if (payload.phase) {
        const phase = Object.hasOwn(labels, payload.phase) ? stateLabel(payload.phase)
          : Object.hasOwn(stageStatuses, payload.phase) ? stageStatuses[payload.phase]
          : Object.hasOwn(stageLabels, payload.phase) ? stageLabels[payload.phase] : "Фаза уточняется";
        detail.push(`Фаза: ${phase}`);
      }
      const code = errorCode(payload.error || payload.code);
      if (/^[a-z][a-z0-9_]{0,127}$/.test(code)) {
        let message = errorText(code, true);
        if (payload.status === "outcome_unknown" && Object.hasOwn(errors, code) && code !== "remote_operation_still_pending") {
          const operation = payload.stage === "publish_source" ? "публикации в SaaS"
            : ["move_file", "poll_move"].includes(payload.stage) ? "перемещения в On-premises" : "загрузки в On-premises";
          message = `Результат ${operation} не подтверждён. ${message} Запрос мог выполниться на сервере.`;
        }
        detail.push(Object.hasOwn(errors, code) ? `${message} (${code})` : `Код ошибки: ${code}`);
      }
      if (context.kind === "file") detail.push("Файл");
      if (["dir", "directory"].includes(context.kind)) detail.push("Каталог");
      const source = journalPath(context.source) || journalPath(payload.source_path);
      if (source) detail.push(`Источник: ${source}`);
      const destination = journalPath(context.destination);
      if (destination) detail.push(`Назначение: ${destination}`);
      const remote = journalPath(context.remote_path);
      if (remote) detail.push(`Временный путь: ${remote}`);
      if (typeof context.size === "number" && Number.isFinite(context.size) && context.size >= 0) {
        detail.push(`Размер: ${bytes(context.size)}`);
      }
      if (context.strategy === "url") detail.push("Прежний способ переноса");
      if (context.strategy === "staged") detail.push("С временным сохранением в кэш");
      const diagnostic = journalDiagnostic(event.diagnostic_message);
      if (diagnostic) detail.push(diagnostic);
      else if (Number.isInteger(payload.http_status) && payload.http_status >= 100 && payload.http_status <= 599) {
        detail.push(`HTTP ${payload.http_status}`);
      }
      if (Number.isSafeInteger(payload.duration_ms) && payload.duration_ms >= 0) {
        detail.push(`${payload.duration_ms} мс`);
      }
      if (validPointRetryAttempts(payload.point_retry_attempts) && payload.max_point_retry_attempts === 3) {
        detail.push(`Точечный повтор: ${payload.point_retry_attempts}/3`);
      }
      if (validDownloadAttempts(payload.download_attempts) && payload.max_download_attempts === 5) {
        detail.push(`Скачивание: ${payload.download_attempts}/5`);
      }
      if (Number.isSafeInteger(payload.retry_delay_ms) && payload.retry_delay_ms >= 0) {
        detail.push(`Пауза перед повтором: ${payload.retry_delay_ms / 1000} с`);
      }
      const retryAt = retryDate(payload.retry_at_ms);
      if (retryAt) detail.push(`Следующая попытка: ${retryAt.toLocaleString("ru-RU")}`);
      if (["in-progress", "success", "failed"].includes(payload.remote_status)) {
        detail.push(`Удалённый статус: ${stateLabel(payload.remote_status)}`);
      }
      ["entry_id", "attempt_id"].forEach((key) => {
        if (typeof payload[key] === "string" && /^[A-Za-z0-9._:-]{1,256}$/.test(payload[key])) {
          detail.push(`${key === "entry_id" ? "Объект" : "Попытка"}: ${payload[key]}`);
        }
      });
      if (typeof payload.operation_id === "string" && operationReferencePattern.test(payload.operation_id)) {
        detail.push(`Операция: ${payload.operation_id}`);
      }
      const date = new Date(event.created_at);
      const time = Number.isNaN(date.getTime()) ? "" : date.toLocaleString("ru-RU");
      logLines.push(`${sequence} ${time} ${eventLabels[event.type] || "Событие"}${detail.length ? ": " + detail.join(" · ") : ""}`);
      eventSequence = sequence;
    });
    logLines = logLines.slice(-500);
    const log = byId("onprem-log");
    log.textContent = logLines.join("\n");
    log.scrollTop = log.scrollHeight;
  }

  function selectJob(job, restore = false) {
    generation += 1;
    selected = job;
    entries = [];
    entryOffset = 0;
    eventSequence = 0;
    logLines = [];
    byId("onprem-log").textContent = "";
    if (restore) fillConfig(job.config || {});
    updateDestinationPreview();
    renderJob();
  }

  async function openJob(id) {
    const job = await request(`/jobs/${encodeURIComponent(id)}`);
    if (job?.id !== id) throw failure("job_not_found");
    selectJob(job, true);
    await refreshSelected();
  }

  async function refreshSelected() {
    if (!selected) return;
    const id = selected.id;
    const snapshot = generation;
    const path = `/jobs/${encodeURIComponent(id)}`;
    const [job, result, events] = await Promise.all([
      request(path), request(path + `/entries?limit=${entryPageSize}&offset=${entryOffset}`),
      request(path + `/events?format=json&after=${eventSequence}${eventSequence === 0 ? "&tail=500" : ""}`),
    ]);
    if (!selected || selected.id !== id || snapshot !== generation) return;
    selected = job;
    entries = Array.isArray(result) ? result : result.entries || [];
    appendEvents(events.events || []);
    renderJob();
  }

  async function refreshAll(offset = historyOffset) {
    const snapshot = generation;
    const result = await request(`/jobs?limit=${historyPageSize + 1}&offset=${offset}`);
    if (snapshot !== generation) return;
    const jobs = Array.isArray(result) ? result : result.jobs || [];
    historyOffset = offset;
    historyHasOlder = jobs.length > historyPageSize;
    history = jobs.slice(0, historyPageSize);
    renderHistory();
    await refreshSelected();
    if (snapshot !== generation) return;
    pollNotice("");
    updateActions();
  }

  function schedulePoll() {
    clearTimeout(pollTimer);
    if (!active() || !authorized()) return;
    pollTimer = setTimeout(async () => {
      if (busy) { schedulePoll(); return; }
      const snapshot = generation;
      try { await refreshAll(); }
      catch (error) { if (snapshot === generation && !busy) pollNotice(errorText(error)); }
      schedulePoll();
    }, 2500);
  }

  async function action(work) {
    if (busy) return;
    busy = true;
    generation += 1;
    clearTimeout(pollTimer);
    updateActions();
    notice("");
    try { await work(); }
    catch (error) { notice(errorText(error), true); }
    finally { busy = false; updateActions(); schedulePoll(); }
  }

  async function mutate(suffix, body = {}) {
    if (!selected) return;
    if (!sharedConfig(selected.config)) throw failure("legacy_personal_job");
    if (selected.config.strategy !== "staged") throw failure("onprem_staged_required");
    if (suffix !== "pause" && !originalConfigMatches()) throw failure("credential_binding_mismatch");
    if (oldWorkStopped(selected)) return;
    const result = await request(`/jobs/${encodeURIComponent(selected.id)}/${suffix}`, {method: "POST", body});
    if (result?.id === selected.id) selected = result;
    await refreshSelected();
    renderJob();
  }

  async function attachAccess(pointRetryEntryId = null) {
    if (selected.config.strategy !== "staged") throw failure("onprem_staged_required");
    if (!originalConfigMatches()) throw failure("credential_binding_mismatch");
    if (!byId("onprem-identity-confirmed").checked) throw failure("identity_confirmation_required");
    const secret = credentials();
    if (!secret.source_client_secret.trim()) throw failure("source_credentials_required");
    if (!secret.destination_token.trim()) throw failure("destination_credentials_required");
    if (sharedConfig(selected.config) && !byId("onprem-form").reportValidity()) throw failure("validation_error");
    const id = selected.id;
    const snapshot = generation;
    const revision = destinationCheckRevision;
    notice("Проверяю доступ к дискам…");
    const result = await request(`/jobs/${encodeURIComponent(id)}/credentials`, {
      method: "PUT", body: {
        config: {...selected.config, strategy: "staged", identity_confirmed: true}, credentials: secret,
        ...(pointRetryEntryId ? {point_retry_entry_id: pointRetryEntryId} : {}),
      },
    });
    if (!selected || selected.id !== id || snapshot !== generation || revision !== destinationCheckRevision || !accessReady() ||
        credentials().source_client_secret !== secret.source_client_secret || credentials().source_admin_token !== secret.source_admin_token || credentials().destination_token !== secret.destination_token) {
      throw failure("credential_binding_mismatch");
    }
    if (result?.id !== id) throw failure("invalid_response");
    selected = {...selected, ...result};
  }

  function uuid() {
    if (typeof crypto.randomUUID === "function") return crypto.randomUUID();
    const values = crypto.getRandomValues(new Uint8Array(16));
    values[6] = (values[6] & 15) | 64;
    values[8] = (values[8] & 63) | 128;
    const hex = Array.from(values, (value) => value.toString(16).padStart(2, "0"));
    return `${hex.slice(0, 4).join("")}-${hex.slice(4, 6).join("")}-${hex.slice(6, 8).join("")}-${hex.slice(8, 10).join("")}-${hex.slice(10).join("")}`;
  }

  byId("onprem-form").addEventListener("submit", (event) => {
    event.preventDefault();
    if (byId("onprem-source-input")?.value === "list") return;
    if (byId("onprem-plan").disabled || !byId("onprem-form").reportValidity()) return;
    action(async () => {
      const config = readConfig();
      await checkDestination(config);
      const fingerprint = JSON.stringify(config);
      if (!pendingCreate || pendingCreate.fingerprint !== fingerprint) pendingCreate = {fingerprint, key: uuid()};
      const job = await request("/jobs", {method: "POST", body: {
        config, credentials: credentials(), idempotency_key: pendingCreate.key,
      }});
      pendingCreate = null;
      selectJob(job);
      notice("Dry Run запущен. Дождитесь готовности плана и проверьте итоговые пути.");
      await refreshAll();
    });
  });

  byId("onprem-refresh").addEventListener("click", () => action(refreshAll));
  byId("onprem-history-newer").addEventListener("click", () => action(() =>
    refreshAll(Math.max(0, historyOffset - historyPageSize))));
  byId("onprem-history-older").addEventListener("click", () => action(() =>
    refreshAll(historyOffset + historyPageSize)));
  byId("onprem-history").addEventListener("change", () => {
    const id = byId("onprem-history").value;
    if (!id) {
      selected = null;
      entries = [];
        entryOffset = 0;
      generation += 1;
      clearCredentials();
      byId("onprem-identity-confirmed").checked = false;
      updateDestinationPreview();
      renderJob();
      return;
    }
    action(() => openJob(id));
  });
  byId("onprem-open-blocker").addEventListener("click", () => {
    const blocker = blockingJob(selected);
    if (blocker) action(() => openJob(blocker.job_id));
  });
  byId("onprem-start").addEventListener("click", () => {
    if (byId("onprem-start").disabled) return;
    action(async () => {
      const revision = destinationCheckRevision;
      await attachAccess();
      if (selected.state === "awaiting_credentials") await mutate("resume");
      if (revision !== destinationCheckRevision || !accessReady()) throw failure("credential_binding_mismatch");
      await mutate("start");
      notice("Перенос начат.");
    });
  });
  byId("onprem-entry-previous").addEventListener("click", () => action(async () => {
    entryOffset = Math.max(0, entryOffset - entryPageSize);
    await refreshSelected();
  }));
  byId("onprem-entry-next").addEventListener("click", () => action(async () => {
    entryOffset += entryPageSize;
    await refreshSelected();
  }));
  byId("onprem-pause").addEventListener("click", () => {
    if (!byId("onprem-pause").disabled) action(() => mutate("pause"));
  });
  byId("onprem-stop").addEventListener("click", () => {
    if (byId("onprem-stop").disabled || !window.confirm(
      "Остановить задание полностью и навсегда? Все незавершённые локальные действия будут отменены, фоновые проверки прекратятся. После подтверждения остановки общая очередь освобождается, пути не блокируются. История, кеш и файлы сохраняются. Принятая сервером операция может продолжаться: API не предоставляет её отмену."
    )) return;
    action(async () => {
      const id = selected.id;
      const snapshot = generation;
      const job = await request(`/jobs/${encodeURIComponent(id)}/stop`, {method: "POST"});
      if (!selected || selected.id !== id || snapshot !== generation) return;
      if (job?.id === id) selected = job;
      renderJob();
      await refreshAll();
      if (selected?.id === id && snapshot === generation) {
        notice(selected.state === "stopped" && selected.full_stop_requested === true
          ? "Задание остановлено полностью. Незавершённые локальные действия отменены; общая очередь освобождена, пути не блокируются."
          : "Полная остановка запрошена. Дождитесь подтверждения локальной остановки; очередь ещё не освобождена.");
      }
    });
  });
  byId("onprem-restart").addEventListener("click", () => {
    if (byId("onprem-restart").disabled || !byId("onprem-form").reportValidity()) return;
    action(async () => {
      const id = selected.id;
      const snapshot = generation;
      if (selected.config.strategy !== "staged") throw failure("onprem_staged_required");
      const config = {...selected.config, strategy: "staged", identity_confirmed: true};
      const fingerprint = JSON.stringify(config);
      let pending = pendingRestarts.get(id);
      if (!pending || pending.fingerprint !== fingerprint) {
        pending = {fingerprint, key: uuid()};
        pendingRestarts.set(id, pending);
      }
      const job = await request(`/jobs/${encodeURIComponent(id)}/restart`, {method: "POST", body: {
        config, credentials: credentials(), idempotency_key: pending.key,
      }});
      if (!selected || selected.id !== id || snapshot !== generation) return;
      if (!job || job.id === id || job.restart_of !== id) throw failure("invalid_response");
      pendingRestarts.delete(id);
      selectJob(job);
      await refreshAll(0);
      notice("Создан новый перенос в папку email в корне общего диска, с публикацией источника. Дождитесь плана, проверьте его и нажмите «Начать перенос».");
    });
  });
  byId("onprem-resume").addEventListener("click", () => {
    if (byId("onprem-resume").disabled) return;
    action(async () => {
      await attachAccess();
      const retry = retryFailedEntries();
      await mutate(retry ? "retry" : "resume", retry ? {strategy: "staged"} : {});
      notice(retry ? "Повтор ошибочных объектов запущен." : selected.state === "ready"
        ? "План готов. Проверьте его и нажмите «Начать перенос»." : "Перенос продолжен.");
    });
  });
  byId("onprem-cleanup").addEventListener("click", () => {
    if (busy || !enabled || !authorized()) return;
    return action(async () => {
      const output = byId("onprem-cleanup-result");
      output.className = "field-note";
      output.textContent = "Очищаю локальный кеш…";
      try {
        const result = await request("/cache/cleanup", {method: "POST", body: {}});
        if (!Number.isSafeInteger(result?.removed) || result.removed < 0 ||
            !Number.isSafeInteger(result?.removed_bytes) || result.removed_bytes < 0) throw failure("invalid_response");
        output.textContent = `Удалено файлов: ${result.removed}. Объём: ${bytes(result.removed_bytes)}. История заданий и файлы на дисках сохранены.`;
      } catch (error) {
        output.className = "field-note onprem-result bad";
        const message = {
          network_error: "Соединение с утилитой прервалось. Проверьте связь и повторите очистку локального кеша.",
          invalid_response: "Не удалось прочитать результат очистки локального кеша. Повторите попытку.",
          unavailable: "Сервис временно недоступен. Повторите очистку локального кеша позже.",
        }[errorCode(error)];
        output.textContent = message || errors[errorCode(error)] || errors.cache_cleanup_failed;
      }
    });
  });
  ["report", "journal"].forEach((kind) => byId(`onprem-${kind}`).addEventListener("click", () => action(async () => {
    if (!enabled || !authorized() || !selected) return;
    const id = selected.id;
    const snapshot = generation;
    const blob = await request(`/jobs/${encodeURIComponent(id)}/${kind}`, {blob: true});
    if (!selected || selected.id !== id || snapshot !== generation) return;
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `onprem-${kind === "journal" ? "journal-" : ""}${id.replace(/[^A-Za-z0-9._-]/g, "_")}.csv`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  })));


  panel.querySelectorAll("input, select, textarea").forEach((field) => {
    const changed = () => {
      if (fields.includes(field) || ["onprem-source-secret", "onprem-source-admin-token", "onprem-destination-token", "onprem-identity-confirmed"].includes(field.id)) {
        clearDestinationCheck();
        updateDestinationPreview();
      }
      updateActions();
    };
    field.addEventListener("input", changed);
    field.addEventListener("change", changed);
  });
  updateDestinationPreview();

  function modeChanged() {
    const onprem = tab.classList.contains("active") && enabled;
    document.querySelectorAll("[data-legacy-output]").forEach((element) => { element.hidden = onprem; });
    clearTimeout(pollTimer);
    if (!onprem || byId("onprem-source-input")?.value === "list") return;
    if (!authorized()) { notice(errors.onprem_access_required); updateActions(); return; }
    action(refreshAll);
  }

  document.querySelectorAll(".mode-tab").forEach((mode) => mode.addEventListener("click", modeChanged));
  document.addEventListener("visibilitychange", schedulePoll);
  window.addEventListener("open-onprem-job", (event) => {
    const id = event.detail?.jobId;
    if (!enabled || !authorized() || typeof id !== "string" || !jobIdPattern.test(id)) return;
    const choice = byId("onprem-source-input");
    if (choice) { choice.value = "single"; choice.dispatchEvent(new Event("change")); }
    action(async () => { await openJob(id); byId("onprem-job")?.scrollIntoView?.({behavior: "smooth", block: "start"}); });
  });
  window.addEventListener("pagehide", () => {
    clearTimeout(pollTimer);
    clearCredentials();
    byId("onprem-access-key").value = "";
  });

  Promise.resolve(window.diskCopyConfigPromise).then((config) => {
    if (!config?.onprem_enabled) return;
    enabled = true;
    authRequired = Boolean(config.onprem_auth_required);
    panel.hidden = false;
    tab.disabled = false;
    tab.title = "SaaS → On-premises";
    byId("onprem-access").hidden = !authRequired;
    updateActions();
  }).catch(() => {});
})();
