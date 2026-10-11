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
    stopped: "Остановлено",
    planned: "В плане", transferring: "Передача", downloading: "Скачивание",
    staged: "Сохранён временно", waiting_operation: "Ожидание операции",
    verifying: "Проверка", committing: "Финализация", done: "Перенесено",
    already_verified: "Уже совпадает", conflict: "Конфликт",
    source_changed: "Источник изменился", failed: "Ошибка",
    outcome_unknown: "Исход неизвестен", verification_unavailable: "Проверка недоступна",
    created: "Каталог создан", success: "Успешно", "in-progress": "Выполняется",
  };
  const errors = {
    onprem_access_required: "Нужен действующий ключ доступа к режиму on-premises.",
    credentials_required: "Предоставьте доступ заново, затем продолжите задание.",
    source_credentials_required: "Введите Client secret источника.",
    destination_credentials_required: "Введите OAuth-токен получателя.",
    identity_confirmation_required: "Подтвердите владельца токена назначения.",
    destination_identity_mismatch: "Токен относится к другому получателю.",
    credential_binding_mismatch: "Доступы не соответствуют настройкам задания.",
    credentials_binding_mismatch: "Доступы не соответствуют настройкам задания.",
    source_changed: "Источник изменился. Нужен новый план.",
    source_not_found: "Исходный объект больше не найден.",
    conflict: "Итоговый путь занят другим объектом; перезапись запрещена.",
    outcome_unknown: "Исход операции неизвестен. Требуется подтверждённое решение.",
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
    revision_conflict: "Задание изменилось. Обновите его перед подтверждением.",
    stale_revision: "Задание изменилось. Обновите его перед подтверждением.",
    invalid_operation_id: "Укажите ID операции или ?id=ID. В ID допустимо от 1 до 256 символов: A-Z, a-z, 0-9, - и _. Полные URL и параметры доступа запрещены.",
    evidence_required: "Для терминального решения требуется подтверждение администратора.",
    validation_error: "Проверьте обязательные поля и допустимые пути.",
    network_error: "Соединение прервалось. Обновите задание перед повторным действием.",
    unavailable: "Сервис временно недоступен. Состояние задания сохраняется.",
    invalid_response: "Ответ сервера не удалось прочитать. Обновите задание перед повторным действием.",
    destination_conflict: "Итоговый путь занят другим объектом; перезапись запрещена.",
    transfer_outcome_unknown: "Исход передачи неизвестен. Требуется подтверждённое решение.",
    verification_mismatch: "Содержимое не прошло проверку размера и контрольной суммы.",
    source_hash_unavailable: "Контрольная сумма источника недоступна; перенос не подтверждён.",
    destination_hash_unavailable: "Контрольная сумма назначения недоступна; перенос не подтверждён.",
    staging_space_insufficient: "На сервере переноса недостаточно места для временного файла.",
    destination_space_insufficient: "В назначении недостаточно свободного места.",
    technical_namespace_exists: "Технический каталог уже существует. Создайте новое задание.",
    technical_namespace_outcome_unknown: "Исход создания технического каталога неизвестен. Администратор должен проверить его; для переноса создайте новое задание.",
    destination_auth_failed: "Авторизация назначения не прошла. Проверьте токен и владельца.",
    source_auth_failed: "Авторизация источника не прошла. Проверьте сервисное приложение.",
    remote_operation_failed: "Удалённая операция завершилась ошибкой.",
    polling_unavailable: "Статус операции временно недоступен. Повторная передача не запускается.",
    onprem_https_required: "Для удалённого доступа к утилите требуется HTTPS.",
    onprem_origin_rejected: "Источник запроса не совпадает с адресом утилиты.",
    job_busy: "Задание сейчас выполняет другое действие. Обновите состояние.",
    active_or_unknown_attempt: "Попытка активна или имеет неизвестный исход; повтор заблокирован.",
    terminal_server_evidence_required: "Укажите подтверждение терминального результата по серверным журналам, минимум 20 символов. Таймаут и 404 не являются доказательством.",
    invalid_paths_or_destination: "Проверьте исходный путь, папку назначения и адрес API.",
    invalid_request: "Проверьте обязательные поля запроса.",
    invalid_source_email: "Укажите корректный email источника.",
    invalid_config: "Проверьте настройки источника и получателя.",
    invalid_credentials: "Проверьте введённые доступы.",
    invalid_operation_reference: "Не удалось обработать ссылку на операцию от сервера назначения. Исход операции не подтверждён; администратор должен проверить серверные журналы.",
    operation_reference_mismatch: "У этой операции уже сохранён другой ID. Используйте ID из журнала задания.",
    attempt_not_unknown: "Попытка уже не требует ручного разрешения. Обновите задание.",
    attempt_not_found: "Попытка не найдена. Обновите задание.",
    job_not_found: "Задание не найдено. Обновите историю.",
    job_not_stoppable: "Задание сейчас нельзя остановить. Обновите его состояние.",
    job_not_restartable: "Новый перенос доступен после окончательной остановки или завершения задания.",
    restart_config_mismatch: "Для нового переноса сохраните первоначальные параметры выбранного задания.",
    job_stopped: "Задание окончательно остановлено. Подготовьте новый перенос.",
    onprem_unavailable: "Режим on-premises временно недоступен.",
    directory_confirmation_pending: "Создание каталога ещё не подтверждено. Повторите ошибочные объекты для безопасной проверки.",
  };
  const eventLabels = {
    job_created: "Задание создано", job_state: "Состояние задания",
    state_changed: "Состояние изменено", job_started: "Перенос запущен",
    planning_started: "Планирование начато", plan_ready: "План готов",
    credentials_attached: "Доступ предоставлен", pause_requested: "Запрошена пауза",
    job_paused: "Задание приостановлено", job_resumed: "Задание продолжено",
    stop_requested: "Запрошена окончательная остановка", job_stopped: "Задание остановлено",
    job_restarted: "Создан новый перенос",
    attempt_created: "Создана попытка", attempt_started: "Передача начата",
    operation_started: "Операция запущена", operation_finished: "Операция завершена",
    entry_state: "Состояние объекта", entry_verified: "Объект проверен",
    entry_done: "Объект перенесён", attempt_resolved: "Исход попытки установлен",
    outcome_unknown: "Исход операции неизвестен", cleanup: "Локальная очистка",
    local_cleanup: "Локальная очистка", error: "Ошибка",
    administrative_resolution: "Администратор установил исход операции",
  };

  let enabled = false;
  let authRequired = false;
  let selected = null;
  let entries = [];
  let unknownEntries = [];
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
  let attachedJobId = null;
  let pendingCreate = null;
  const pendingRestarts = new Map();
  let autoRoot = "";

  function errorCode(value) {
    if (typeof value === "string") return value;
    if (value && typeof value.code === "string") return value.code;
    return "";
  }

  function errorText(value) {
    if (errorCode(value) === "credentials_required" && oldWorkStopped(selected)) {
      return "Предоставьте доступ для проверки итогового перемещения. Старый перенос остаётся остановленным.";
    }
    if (errorCode(value) === "request_rejected") {
      const status = Number.isInteger(value?.status) && value.status >= 100 && value.status <= 599
        ? ` (HTTP ${value.status})` : "";
      return `Запрос отклонён${status}. Обновите задание и проверьте доступность действия.`;
    }
    return errors[errorCode(value)] || "Операция не выполнена. Проверьте доступы и состояние задания.";
  }

  function failure(code, status) {
    const error = new Error("onprem_request_failed");
    error.code = code;
    if (Number.isInteger(status) && status >= 100 && status <= 599) error.status = status;
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
    return enabled && tab.classList.contains("active") && !document.hidden;
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
      try {
        const body = await response.json();
        if (typeof body.detail === "string" && Object.hasOwn(errors, body.detail)) code = body.detail;
      } catch (_) {}
      throw failure(code, status);
    }
    if (options.blob) return response.blob();
    if (response.status === 204) return null;
    try { return await response.json(); } catch (_) { throw failure("invalid_response"); }
  }

  function readConfig() {
    const config = {};
    fields.forEach((field) => { config[field.dataset.onpremConfig] = field.value.trim(); });
    config.identity_confirmed = byId("onprem-identity-confirmed").checked;
    return config;
  }

  function credentials() {
    return {
      source_client_secret: byId("onprem-source-secret").value,
      destination_token: byId("onprem-destination-token").value,
    };
  }

  function clearCredentials() {
    byId("onprem-source-secret").value = "";
    byId("onprem-destination-token").value = "";
  }

  function fillConfig(config) {
    fields.forEach((field) => { field.value = config[field.dataset.onpremConfig] || ""; });
    byId("onprem-identity-confirmed").checked = false;
    byId("onprem-retry-strategy").value = config.strategy || "url";
    autoRoot = "";
    clearCredentials();
  }

  function bytes(value) {
    const number = Number(value);
    if (!Number.isFinite(number) || number < 0) return "—";
    if (number < 1024) return `${number.toLocaleString("ru-RU")} Б`;
    const units = ["КиБ", "МиБ", "ГиБ", "ТиБ"];
    let amount = number / 1024;
    let index = 0;
    while (amount >= 1024 && index < units.length - 1) { amount /= 1024; index += 1; }
    return `${amount.toLocaleString("ru-RU", {maximumFractionDigits: 1})} ${units[index]}`;
  }

  function stateLabel(state) { return labels[state] || "Состояние уточняется"; }

  function isStopping(job) { return Boolean(job?.stop_requested && job.state !== "stopped"); }

  function oldWorkStopped(job) { return Boolean(job?.stop_requested || job?.state === "stopped"); }

  function canRecoverFinalMove(job) { return job?.state === "stopped" && job.stop_blocked === true; }

  function recoverableFinalAttempts(job) {
    return Array.isArray(job?.stop_resolvable_attempt_ids)
      ? job.stop_resolvable_attempt_ids.filter((id) => typeof id === "string" && jobIdPattern.test(id)) : [];
  }

  function originalConfigMatches() {
    return Boolean(selected?.config && fields.every((field) =>
      field.value.trim() === String(selected.config[field.dataset.onpremConfig] ?? "")));
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
      : isWaiting(job) ? "Ожидание другого задания" : stateLabel(job.state);
  }

  function renderBlocked() {
    const blocked = hasBlocker(selected);
    const blocker = blockingJob(selected);
    byId("onprem-job-blocked").hidden = !blocked;
    byId("onprem-job-blocked-reason").textContent = blocked
      ? blocker
        ? blocker.reason === "operation_outcome_unknown"
          ? blocker.state === "stopped"
            ? "В остановленном задании не установлен исход итогового перемещения. Очередь ждёт подтверждённого результата."
            : "В другом задании исход операции неизвестен. Сначала установите исход в этом задании."
          : "Другое задание ещё не завершило операцию или проверку результата. Перенос ожидает освобождения очереди."
        : "Задание ожидает освобождения очереди. Обновите состояние, чтобы узнать причину."
      : "";
    byId("onprem-job-blocked-holder").textContent = blocker
      ? `Задание ${blocker.job_id} · ${stateLabel(blocker.state)}` : "";
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
      option.textContent = `${job.config?.source_email || job.id} · ${jobStateLabel(job)} · ${job.id}`;
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
    ["onprem-job", "onprem-cleanup-panel", "onprem-log-panel"].forEach((id) => {
      byId(id).hidden = !selected;
    });
    if (!selected) { byId("onprem-resolve-panel").hidden = true; updateActions(); return; }
    byId("onprem-job-id").textContent = selected.id;
    byId("onprem-job-state").textContent = jobStateLabel(selected);
    byId("onprem-job-state").className = "onprem-result " + (isWaiting(selected) ? "attention" : resultClass(selected.state));
    const stop = byId("onprem-job-stop");
    stop.hidden = !oldWorkStopped(selected);
    stop.textContent = isStopping(selected)
      ? "Остановка запрошена. Ожидается подтверждение локальной остановки; принятая сервером загрузка может продолжаться в старом техническом каталоге."
      : canRecoverFinalMove(selected)
        ? "Локальная работа остановлена. Итоговое перемещение удерживает очередь до подтверждённого завершения. Предоставьте доступ: утилита проверяет статус известных операций в фоне. При недоступном статусе сохраните подтверждённый исход по журналам администратора. Старый перенос не возобновится."
        : "Локальная работа окончательно остановлена. Исходники, итоговые файлы, история и попытки сохранены. Серверная загрузка может завершиться в старом техническом каталоге; автоматической финализации больше не будет.";
    const counts = Object.entries(selected.counts || {}).filter(([, value]) => Number.isFinite(Number(value)));
    byId("onprem-job-counts").textContent = counts.length
      ? counts.map(([state, count]) => `${state === "total" ? "Всего" : stateLabel(state)}: ${count}`).join(" · ")
      : "План готовится";
    if (Number(selected.total_directories) > 0) {
      byId("onprem-job-counts").textContent += ` · Каталогов: ${selected.total_directories}`;
    }
    byId("onprem-job-bytes").textContent = `${bytes(selected.verified_bytes)} / ${bytes(selected.total_bytes)}`;
    byId("onprem-remote-root").textContent = selected.remote_root || "Не создан";
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
      [entry.source_path, entry.destination_path,
        entry.kind === "dir" || entry.kind === "directory" ? "Каталог" : bytes(entry.size),
        entry.md5 || "—"].forEach((value, index) => {
        const cell = document.createElement("td");
        cell.textContent = value || "—";
        if (index < 2 || index === 3) cell.className = "onprem-path";
        row.appendChild(cell);
      });
      const result = document.createElement("td");
      const state = document.createElement("span");
      state.className = "onprem-result " + resultClass(entry.state);
      state.textContent = stateLabel(entry.state);
      result.appendChild(state);
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
      row.appendChild(result);
      body.appendChild(row);
    });
    const total = entryTotal();
    byId("onprem-entry-count").textContent = entries.length
      ? `Объекты ${entryOffset + 1}–${entryOffset + entries.length} из ${total}`
      : `Объектов в плане: ${total}`;
    byId("onprem-entry-page").textContent = `Страница ${Math.floor(entryOffset / entryPageSize) + 1} из ${Math.max(1, Math.ceil(total / entryPageSize))}`;
    renderResolve();
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
      entries.some((entry) => entry.state === state) || unknownEntries.some((entry) => entry.state === state);
  }

  function clearResolution() {
    byId("onprem-resolve-operation").value = "";
    byId("onprem-resolve-status").value = "";
    byId("onprem-resolve-evidence").value = "";
    byId("onprem-resolve-confirmed").checked = false;
  }

  function renderResolve() {
    const stopped = selected?.state === "stopped";
    const finalAttempts = recoverableFinalAttempts(selected);
    const unknown = unknownEntries.filter((entry) => entry.attempt_id && (stopped
      ? canRecoverFinalMove(selected) && finalAttempts.includes(entry.attempt_id)
      : entry.state === "outcome_unknown"));
    byId("onprem-resolve-title").textContent = stopped
      ? "Проверка итогового перемещения" : "Неопределённый исход операции";
    byId("onprem-resolve-warning").textContent = stopped
      ? "Перенос окончательно остановлен. Укажите ID итогового перемещения или подтверждённый терминальный результат по серверным журналам администратора. Проверка не запускает старые попытки. Таймаут и отсутствие файла не подтверждают завершение."
      : "Повтор этой попытки заблокирован. Укажите восстановленный ID операции или терминальный результат по серверным журналам администратора. Таймаут и отсутствие файла не подтверждают завершение.";
    const select = byId("onprem-resolve-attempt");
    const previous = select.value;
    select.replaceChildren();
    unknown.forEach((entry) => {
      const option = document.createElement("option");
      option.value = entry.attempt_id;
      option.textContent = `${entry.source_path} · ${entry.attempt_id}`;
      select.appendChild(option);
    });
    if (unknown.some((entry) => entry.attempt_id === previous)) select.value = previous;
    else { select.value = unknown[0]?.attempt_id || ""; clearResolution(); }
    byId("onprem-resolve-panel").hidden = !selected || !unknown.length;
  }

  function resolutionValid() {
    const operation = byId("onprem-resolve-operation").value.trim();
    const terminal = byId("onprem-resolve-status").value;
    const evidence = byId("onprem-resolve-evidence").value.trim();
    return operation ? operationReferencePattern.test(operation)
      : Boolean(terminal && evidence.length >= 20 &&
        !/https?:\/\/|oauth|bearer|token=|404|timeout|таймер|таймаут/i.test(evidence));
  }

  function updateActions() {
    const locked = busy || !enabled || !authorized();
    const confirmed = byId("onprem-identity-confirmed").checked;
    const state = selected?.state;
    const stopped = oldWorkStopped(selected);
    const finalRecovery = canRecoverFinalMove(selected);
    const unknown = hasState("outcome_unknown");
    const namespaceBlocked = ["technical_namespace_exists", "technical_namespace_outcome_unknown"].includes(errorCode(selected?.error));
    const terminalOrPaused = ["paused", "needs_attention", "completed", "completed_with_issues"].includes(state);
    byId("onprem-plan").disabled = locked || !confirmed;
    byId("onprem-refresh").disabled = locked;
    byId("onprem-history").disabled = locked;
    byId("onprem-history-newer").disabled = locked || historyOffset === 0;
    byId("onprem-history-older").disabled = locked || !historyHasOlder;
    byId("onprem-open-blocker").disabled = locked || !blockingJob(selected);
    byId("onprem-start").disabled = locked || stopped || !confirmed || state !== "ready";
    byId("onprem-pause").disabled = locked || stopped || state !== "running" || selected?.pause_requested;
    byId("onprem-stop").disabled = locked || stopped ||
      !["queued", "planning", "ready", "running", "paused", "awaiting_credentials", "needs_attention", "completed_with_issues"].includes(state);
    byId("onprem-restart").disabled = locked || isStopping(selected) || !confirmed ||
      !["stopped", "completed", "completed_with_issues"].includes(state) ||
      !originalConfigMatches() || !byId("onprem-form").checkValidity();
    byId("onprem-resume").disabled = locked || stopped || unknown || namespaceBlocked ||
      !(state === "paused" || state === "needs_attention" ||
        (state === "awaiting_credentials" && attachedJobId === selected?.id));
    byId("onprem-credentials").disabled = locked || !selected || (stopped && !finalRecovery);
    byId("onprem-report").disabled = locked || !selected;
    byId("onprem-retry").disabled = locked || stopped || !terminalOrPaused || unknown ||
      !hasState("failed");
    byId("onprem-cleanup").disabled = locked || !terminalOrPaused;
    byId("onprem-entry-previous").disabled = locked || !selected || entryOffset === 0;
    byId("onprem-entry-next").disabled = locked || !selected || entryOffset + entries.length >= entryTotal();
    byId("onprem-resolve").disabled = locked || !selected || (stopped && !finalRecovery) || !byId("onprem-resolve-attempt").value ||
      !byId("onprem-resolve-confirmed").checked || !resolutionValid();
    const hasOperation = Boolean(byId("onprem-resolve-operation").value.trim());
    byId("onprem-resolve-status").disabled = hasOperation;
    byId("onprem-resolve-evidence").disabled = hasOperation;
  }

  function appendEvents(events) {
    (events || []).slice().sort((a, b) => a.sequence - b.sequence).forEach((event) => {
      const sequence = Number(event.sequence);
      if (!Number.isSafeInteger(sequence) || sequence <= eventSequence) return;
      const payload = event.payload || {};
      const detail = [];
      if (payload.state) detail.push(stateLabel(payload.state));
      if (payload.code) detail.push(errorText(payload.code));
      ["entry_id", "attempt_id"].forEach((key) => {
        if (typeof payload[key] === "string" && /^[A-Za-z0-9._:-]{1,256}$/.test(payload[key])) detail.push(payload[key]);
      });
      if (typeof payload.operation_id === "string" && operationReferencePattern.test(payload.operation_id)) {
        detail.push(`Операция: ${payload.operation_id}`);
      }
      const date = new Date(event.created_at);
      const time = Number.isNaN(date.getTime()) ? "" : date.toLocaleTimeString("ru-RU");
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
    unknownEntries = [];
    entryOffset = 0;
    eventSequence = 0;
    logLines = [];
    byId("onprem-log").textContent = "";
    byId("onprem-cleanup-result").textContent = "";
    byId("onprem-remote-attempts").textContent = "";
    clearResolution();
    if (restore) { attachedJobId = null; fillConfig(job.config || {}); }
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
    const [job, result, events, unknown] = await Promise.all([
      request(path), request(path + `/entries?limit=${entryPageSize}&offset=${entryOffset}`),
      request(path + `/events?format=json&after=${eventSequence}`),
      request(path + "/entries?limit=1000&offset=0&state=outcome_unknown"),
    ]);
    if (!selected || selected.id !== id || snapshot !== generation) return;
    let recovery = [];
    if (canRecoverFinalMove(job) && recoverableFinalAttempts(job).length) {
      const results = await Promise.all(["committing", "waiting_operation"].map((state) =>
        request(path + `/entries?limit=1000&offset=0&state=${state}`)));
      if (!selected || selected.id !== id || snapshot !== generation) return;
      recovery = results.flatMap((value) => Array.isArray(value) ? value : value.entries || []);
    }
    selected = job;
    entries = Array.isArray(result) ? result : result.entries || [];
    const candidates = [...(Array.isArray(unknown) ? unknown : unknown.entries || []), ...recovery];
    unknownEntries = Array.from(new Map(candidates.map((entry) => [entry.attempt_id || entry.id, entry])).values());
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
    if (oldWorkStopped(selected) && !(suffix === "resolve" && canRecoverFinalMove(selected))) return;
    const result = await request(`/jobs/${encodeURIComponent(selected.id)}/${suffix}`, {method: "POST", body});
    if (result?.id === selected.id) selected = result;
    await refreshSelected();
    renderJob();
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
    if (!byId("onprem-form").reportValidity()) return;
    action(async () => {
      const config = readConfig();
      const fingerprint = JSON.stringify(config);
      if (!pendingCreate || pendingCreate.fingerprint !== fingerprint) pendingCreate = {fingerprint, key: uuid()};
      const job = await request("/jobs", {method: "POST", body: {
        config, credentials: credentials(), idempotency_key: pendingCreate.key,
      }});
      pendingCreate = null;
      attachedJobId = job.id;
      selectJob(job);
      notice("Задание создано. Дождитесь готовности плана и проверьте итоговые пути.");
      await refreshAll();
    });
  });

  byId("onprem-source-email").addEventListener("input", () => {
    const root = byId("onprem-destination-root");
    if (!root.value || root.value === autoRoot) {
      const email = byId("onprem-source-email").value.trim();
      autoRoot = email ? "disk:/" + email : "";
      root.value = autoRoot;
    }
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
      unknownEntries = [];
      entryOffset = 0;
      generation += 1;
      clearCredentials();
      byId("onprem-identity-confirmed").checked = false;
      renderJob();
      return;
    }
    action(() => openJob(id));
  });
  byId("onprem-open-blocker").addEventListener("click", () => {
    const blocker = blockingJob(selected);
    if (blocker) action(() => openJob(blocker.job_id));
  });
  byId("onprem-start").addEventListener("click", () => action(() => mutate("start")));
  byId("onprem-entry-previous").addEventListener("click", () => action(async () => {
    entryOffset = Math.max(0, entryOffset - entryPageSize);
    await refreshSelected();
  }));
  byId("onprem-entry-next").addEventListener("click", () => action(async () => {
    entryOffset += entryPageSize;
    await refreshSelected();
  }));
  byId("onprem-pause").addEventListener("click", () => action(() => mutate("pause")));
  byId("onprem-stop").addEventListener("click", () => {
    if (byId("onprem-stop").disabled || !window.confirm(
      "Окончательно остановить это задание навсегда? Исходники, итоговые файлы, история и попытки не удаляются. Это локальная остановка, а не отмена операции на удалённом сервере: принятая загрузка может продолжаться в старом техническом каталоге и расходовать квоту и сеть. Технические остатки потребуют проверки перед ручной очисткой. Неизвестный исход итогового перемещения продолжит удерживать очередь."
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
        notice(selected.state === "stopped" ? "Задание окончательно остановлено. История и попытки сохранены."
          : "Остановка запрошена. Дождитесь подтверждения локальной остановки.");
      }
    });
  });
  byId("onprem-restart").addEventListener("click", () => {
    if (byId("onprem-restart").disabled || !byId("onprem-form").reportValidity()) return;
    action(async () => {
      const id = selected.id;
      const snapshot = generation;
      const config = readConfig();
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
      attachedJobId = job.id;
      selectJob(job);
      await refreshAll(0);
      notice("Создан новый перенос. Дождитесь нового плана, проверьте его и нажмите «Начать перенос» вручную.");
    });
  });
  byId("onprem-resume").addEventListener("click", () => action(() => mutate("resume")));
  byId("onprem-retry").addEventListener("click", () => action(() => mutate("retry", {
    strategy: byId("onprem-retry-strategy").value,
  })));
  byId("onprem-credentials").addEventListener("click", () => {
    if (!selected || (oldWorkStopped(selected) && !canRecoverFinalMove(selected))) return;
    if (!byId("onprem-form").reportValidity()) return;
    action(async () => {
      await request(`/jobs/${encodeURIComponent(selected.id)}/credentials`, {
        method: "PUT", body: {config: readConfig(), credentials: credentials()},
      });
      attachedJobId = selected.id;
      await refreshSelected();
      notice(selected?.state === "stopped"
        ? "Доступ предоставлен для проверки итогового перемещения. Старый перенос остаётся остановленным."
        : "Доступ предоставлен. Продолжите задание.");
    });
  });
  byId("onprem-resolve-attempt").addEventListener("change", () => { clearResolution(); updateActions(); });
  byId("onprem-resolve").addEventListener("click", () => action(async () => {
    if (oldWorkStopped(selected) && !canRecoverFinalMove(selected)) return;
    if (!resolutionValid() || !byId("onprem-resolve-confirmed").checked) return;
    const body = {
      attempt_id: byId("onprem-resolve-attempt").value,
      revision: selected.revision, confirmed: true,
    };
    const operation = byId("onprem-resolve-operation").value.trim();
    if (operation) body.operation_id = operation;
    else {
      body.terminal_status = byId("onprem-resolve-status").value;
      body.evidence = byId("onprem-resolve-evidence").value.trim();
    }
    await mutate("resolve", body);
    clearResolution();
    notice(selected?.state === "stopped"
      ? "Решение записано. Старый перенос остаётся остановленным; итоговое перемещение проверяется без новых записей."
      : "Решение записано. Следующий шаг определяется проверкой результата.");
  }));
  byId("onprem-cleanup").addEventListener("click", () => action(async () => {
    const result = await request(`/jobs/${encodeURIComponent(selected.id)}/cleanup`, {method: "POST", body: {}});
    byId("onprem-cleanup-result").textContent = `Удалено локальных объектов: ${Number(result.removed) || 0}. Удалённые файлы не удалялись.`;
    const attempts = result.remote_attempts || [];
    byId("onprem-remote-attempts").textContent = attempts.length
      ? attempts.map((attempt) => `${attempt.remote_path} · ${stateLabel(attempt.phase)} · ${attempt.id}`).join("\n")
      : "Удалённых технических попыток нет.";
    if (result.remote_root) byId("onprem-remote-root").textContent = result.remote_root;
    await refreshSelected();
  }));
  byId("onprem-report").addEventListener("click", () => action(async () => {
    const blob = await request(`/jobs/${encodeURIComponent(selected.id)}/report`, {blob: true});
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `onprem-${selected.id.replace(/[^A-Za-z0-9._-]/g, "_")}.csv`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }));

  panel.querySelectorAll("input, select, textarea").forEach((field) => {
    field.addEventListener("input", updateActions);
    field.addEventListener("change", updateActions);
  });

  function modeChanged() {
    const onprem = tab.classList.contains("active") && enabled;
    document.querySelectorAll("[data-legacy-output]").forEach((element) => { element.hidden = onprem; });
    clearTimeout(pollTimer);
    if (!onprem) return;
    if (!authorized()) { notice(errors.onprem_access_required); updateActions(); return; }
    action(refreshAll);
  }

  document.querySelectorAll(".mode-tab").forEach((mode) => mode.addEventListener("click", modeChanged));
  document.addEventListener("visibilitychange", schedulePoll);
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
    tab.title = "Пилотный режим";
    byId("onprem-access").hidden = !authRequired;
    updateActions();
  }).catch(() => {});
})();
