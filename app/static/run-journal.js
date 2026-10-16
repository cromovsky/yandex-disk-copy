(function () {
  "use strict";
  const get = (id) => document.getElementById(id);
  if (!get("run-history")) return;
  const modes = ["single", "cross", "local"];
  const names = {single: "Одна организация", cross: "Между организациями", local: "Сохранить локально"};
  const states = {running: "Выполняется", done: "Завершено", error: "Ошибка", interrupted: "Прервано при перезапуске утилиты"};
  const statuses = {started: "Начато", running: "Выполняется", done: "Завершено", succeeded: "Успешно", success: "Успешно", failed: "Ошибка", error: "Ошибка",
    skipped: "Пропущено", waiting: "Ожидание", retry: "Повтор", outcome_unknown: "Исход неизвестен", interrupted: "Прервано", enabled: "Включены", disabled: "Выключены"};
  const sides = {source: "SaaS: источник", destination: "SaaS: получатель", local: "Локальный диск / кеш"};
  const stages = {get_token: "Получение токена", source_auth: "Авторизация источника", destination_auth: "Авторизация получателя",
    check_user: "Проверка сотрудника", check_space: "Проверка свободного места", list_resources: "Чтение папки",
    publish: "Публикация", publish_source: "Публикация источника", download: "Скачивание", downloading: "Скачивание",
    upload: "Загрузка", upload_file: "Загрузка файла", create_directory: "Создание папки", verify: "Проверка файла",
    user_status: "Проверка сотрудника", set_user_status: "Изменение статуса сотрудника", disk_space: "Проверка свободного места",
    list_folder: "Чтение папки", metadata: "Сведения о файле", get_download_url: "Получение ссылки скачивания",
    get_upload_url: "Получение ссылки загрузки", file: "Обработка файла", local_write: "Запись на локальный диск", collect_links: "Сбор ссылок",
    check_local: "Проверка локального файла", transfer: "Перенос", retry_policy: "Настройка повторов", retry_settings: "Автоматические повторы",
    file_pause: "Пауза между передачами",
    poll_operation: "Ожидание операции", save_local: "Локальное сохранение", save_public: "Сохранение по ссылке"};
  const idPattern = /^[A-Za-z0-9._:-]{1,128}$/;
  const storageKey = (mode) => "diskCopyJournalSelection:v1:" + mode;
  function remembered(mode) {
    try { const id = window.localStorage.getItem(storageKey(mode)); return idPattern.test(id || "") ? id : ""; }
    catch (_) { return ""; }
  }
  function remember(mode, id) {
    try { if (id) window.localStorage.setItem(storageKey(mode), id); else window.localStorage.removeItem(storageKey(mode)); }
    catch (_) {}
  }
  const data = Object.fromEntries(modes.map((mode) => [mode, {jobs: [], knownIds: new Set(), selected: remembered(mode), job: null, offset: 0,
    hasMore: false, restored: false, lines: [], cursor: 0, loaded: false, warning: "", retryWarning: "", cleanup: "", cleanupError: false}]));
  const activity = Object.fromEntries(modes.map((mode) => [mode, null]));
  let mode = document.querySelector(".mode-tab.active")?.dataset.mode || "single";
  let version = 0;
  let renderedSelection = "";
  let mutationVersion = 0;
  let timer = null;
  let refreshing = null;
  let refreshAgain = false;
  let starting = false;
  let uncertainStart = null;
  let cleanupBusy = false;
  let exportBusy = false;
  let connectionWarning = "";
  const runButtons = ["run", "run-cross", "run-local"];
  const knownErrors = {
    job_not_running: "Задание уже завершено. Настройте повторы перед новым запуском.",
    job_not_found: "Задание не найдено. Обновите историю и выберите задание заново.",
    journal_in_use: "В этом режиме ещё выполняется перенос. Дождитесь его завершения, затем очистите журнальный кеш.",
    journal_write_failed: "Не удалось сохранить журнал. Проверьте свободное место и права доступа к хранилищу утилиты.",
    journal_storage_unavailable: "Хранилище журналов недоступно. Проверьте свободное место и права доступа.",
    journal_unavailable: "Хранилище журналов недоступно. Проверьте свободное место и права доступа.",
    journal_cleanup_failed: "Не удалось полностью очистить журнальный кеш. Проверьте хранилище и повторите попытку.",
    journal_origin_rejected: "Запрос отклонён: откройте утилиту по её обычному адресу и повторите действие.",
  };
  const clean = (value, limit = 1500) => typeof value === "string" && value.length <= limit &&
    !/[\x00-\x08\x0b-\x1f\x7f]|https?:\/\/|(?:bearer|oauth)\s|(?:token|secret|password|signature|authorization)\s*[:=]/i.test(value) ? value : "";
  function volume(value) {
    if (!Number.isFinite(value) || value < 0) return "—";
    let amount = value; let unit = 0;
    const units = ["Б", "Кб", "Мб", "Гб", "Тб"];
    while (amount >= 1024 && unit < units.length - 1) { amount /= 1024; unit += 1; }
    return amount.toLocaleString("ru-RU", {maximumFractionDigits: 1}) + " " + units[unit];
  }
  function date(value) {
    const stamp = new Date(value);
    return value && Number.isFinite(stamp.getTime()) ? stamp.toLocaleString("ru-RU") : "Дата неизвестна";
  }
  function state(job) {
    if (job?.status === "done" && (job.error || Number(job.summary?.failed) > 0 || Number(job.summary?.unconfirmed) > 0)) return "Завершено с ошибками";
    return states[job?.status] || "Состояние неизвестно";
  }
  function apiError(code, status) { const error = new Error("journal_request_failed"); error.code = code; error.status = status; return error; }
  function errorText(error) {
    if (knownErrors[error?.code]) return knownErrors[error.code];
    if (error?.code === "network") return "Связь с утилитой прервалась. Состояние переноса не подтверждено; обновите журнал.";
    if (error?.code === "invalid_response") return "Не удалось прочитать ответ утилиты. Обновите журнал.";
    return "Не удалось выполнить действие" + (Number.isInteger(error?.status) ? ` (HTTP ${error.status})` : "") + ". Повторите после проверки связи.";
  }
  async function request(path, options = {}) {
    let response;
    try { response = await fetch(path, {method: options.method || "GET", credentials: "same-origin", cache: "no-store", redirect: "error",
      headers: {Accept: options.blob ? "text/csv" : "application/json", ...(options.body === undefined ? {} : {"Content-Type": "application/json"})},
      body: options.body === undefined ? undefined : JSON.stringify(options.body)}); }
    catch (_) { throw apiError("network"); }
    if (!response.ok) {
      let detail;
      try { detail = (await response.json()).detail; } catch (_) {}
      throw apiError(typeof detail === "string" && Object.hasOwn(knownErrors, detail) ? detail : "rejected", response.status);
    }
    try { return options.blob ? await response.blob() : await response.json(); }
    catch (_) { throw apiError("invalid_response"); }
  }
  function isLocked() { return starting || Boolean(uncertainStart) || modes.some((key) => activity[key] === null || activity[key] > 0); }
  function controls() {
    for (const key of modes) {
      if (key === "local" && get("local-source-input")?.value === "list") continue;
      const current = data[key], job = current.job, id = current.selected;
      window.diskCopyRetries?.bind(key, {
        id: "run:" + (id || "new"), enabled: job ? job.retry_enabled !== false : window.diskCopyRetries.value(key),
        loading: Boolean(id && !job), editable: job?.status === "running",
        save: async (enabled) => {
          const result = await request(`/api/run/${encodeURIComponent(id)}/retries`, {method: "POST", body: {enabled}});
          if (current.selected === id && current.job) {current.job.retry_enabled = result.retry_enabled; current.retryWarning = "";}
          return result;
        }, errorText,
      });
    }
    runButtons.forEach((id) => { if (get(id)) get(id).disabled = isLocked(); });
    const current = data[mode];
    get("run-journal-csv").disabled = !current?.selected || exportBusy || cleanupBusy;
    get("run-history-previous").disabled = !current || current.offset === 0;
    get("run-history-next").disabled = !current?.hasMore;
    get("run-journal-cleanup").disabled = !current || cleanupBusy || exportBusy || activity[mode] > 0;
    get("run-start-recovery").hidden = !uncertainStart;
    get("run-start-unlock").disabled = !uncertainStart?.checked || starting || modes.some((key) => activity[key] !== 0);
  }
  function resetSelection(key, id) {
    const current = data[key]; current.selected = id; current.job = null; current.lines = []; current.cursor = 0; current.loaded = false;
    current.selectionRevision = (current.selectionRevision || 0) + 1;
    current.warning = ""; current.retryWarning = ""; current.restored = true; remember(key, id);
  }
  function render() {
    const current = data[mode];
    document.querySelectorAll("[data-legacy-output]").forEach((node) => { node.hidden = !current; });
    controls();
    if (!current) return;
    get("run-journal-mode").textContent = "Журнал: «" + names[mode] + "»";
    const history = get("run-history"); history.replaceChildren();
    const placeholder = document.createElement("option"); placeholder.value = "";
    placeholder.textContent = current.jobs.length ? "Выберите задание" : "Заданий пока нет";
    history.appendChild(placeholder);
    const jobs = current.job && !current.jobs.some((job) => job.id === current.job.id) ? [current.job, ...current.jobs] : current.jobs;
    jobs.forEach((job) => {
      const option = document.createElement("option"); option.value = job.id;
      option.textContent = `${date(job.created_at)} · ${state(job)}${clean(job.source) ? " · " + clean(job.source) : ""}`;
      history.appendChild(option);
    });
    history.value = current.selected;
    get("run-history-page").textContent = "Страница " + (Math.floor(current.offset / 50) + 1);
    const job = current.job;
    get("status").textContent = job ? `${date(job.created_at)} · ${state(job)}` : current.selected ? "Загружаю задание…" : "";
    get("status").className = "status " + (job?.status === "running" ? "running" : job?.status === "done" && !job.error && !(Number(job.summary?.failed) > 0) && !(Number(job.summary?.unconfirmed) > 0) ? "done" : "error");
    get("run-journal-error").textContent = clean(job?.error);
    const summary = job?.summary || {};
    get("run-journal-summary").textContent = [["saved", "Сохранено"], ["skipped", "Пропущено"], ["failed", "Ошибок"], ["unconfirmed", "Не подтверждено"]]
      .filter(([key]) => Number.isSafeInteger(summary[key]) && summary[key] >= 0)
      .map(([key, label]) => label + ": " + summary[key]).join(" · ");
    get("run-journal-direction").textContent = job ? [clean(job.source), clean(job.destination)].filter(Boolean).join(" → ") : "";
    const warnings = [connectionWarning, current.warning, current.retryWarning];
    if (uncertainStart) warnings.push("Ответ на запуск не получен. Обновите историю и убедитесь, что задание в ней не появилось. Повторный запуск может создать второе задание. Запрос автоматически не повторяется.");
    if (job?.journal_error) warnings.push("Журнал этого задания может быть неполным: произошла ошибка записи. Результат переноса показан отдельно. Проверьте хранилище утилиты.");
    if (job?.status === "interrupted") warnings.push("Утилита была перезапущена. Старый перенос автоматически не продолжается; проверьте уже сохранённые файлы перед новым запуском.");
    get("run-journal-notice").textContent = warnings.filter(Boolean).join(" ");
    get("run-active-note").textContent = modes.filter((key) => activity[key] > 0).map((key) => `«${names[key]}»: выполняется заданий — ${activity[key]}`).join(". ");
    const log = get("log"), selection = `${mode}:${current.selected}:${current.selectionRevision || 0}`;
    const previousTop = Number(log.scrollTop) || 0;
    const atBottom = (Number(log.scrollHeight) || 0) - previousTop - (Number(log.clientHeight) || 0) <= 24;
    const text = current.lines.map((line) => line.text).join("\n");
    if (log.textContent !== text) log.textContent = text;
    log.scrollTop = renderedSelection !== selection || atBottom ? log.scrollHeight : previousTop;
    renderedSelection = selection;
    get("run-journal-cleanup-note").textContent = `Удаляет журналы только режима «${names[mode]}». Скачанные файлы и журналы SaaS → On-premises сохраняются. Во время переноса в этом режиме очистка недоступна.`;
    get("run-journal-cleanup-result").textContent = current.cleanup;
    get("run-journal-cleanup-result").className = "field-note" + (current.cleanupError ? " onprem-result bad" : "");
  }
  function eventText(event) {
    const parts = [date(event.created_at)];
    if (Number.isSafeInteger(event.pair_index)) parts.push("Пара " + event.pair_index);
    if (sides[event.error_side]) parts.push(sides[event.error_side]);
    if (clean(event.stage, 128)) parts.push(stages[event.stage] || event.stage);
    if (clean(event.status, 128)) parts.push(statuses[event.status] || event.status);
    if (clean(event.message)) parts.push(event.message);
    if (typeof event.retry_enabled === "boolean") parts.push("Автоматические повторы: " + (event.retry_enabled ? "включены" : "выключены"));
    if (clean(event.source)) parts.push("Источник: " + event.source);
    if (clean(event.destination)) parts.push("Назначение: " + event.destination);
    if (Number.isFinite(event.size) && event.size >= 0) parts.push("Размер: " + volume(event.size));
    if (Number.isInteger(event.http_status) && event.http_status >= 100 && event.http_status <= 599) parts.push("HTTP " + event.http_status);
    for (const [key, label] of [["error", "Ошибка"], ["api_error", "API"], ["api_reason", "Причина API"],
      ["transport_error", "Сеть"], ["request_host", "Сервер"], ["request_id", "ID запроса"], ["operation_id", "Операция"]]) {
      if (clean(event[key], 500)) parts.push(label + ": " + event[key]);
    }
    if (Number.isSafeInteger(event.attempt) && event.attempt > 0) parts.push("Попытка: " + event.attempt + (Number.isSafeInteger(event.max_attempts) ? "/" + event.max_attempts : ""));
    if (Number.isSafeInteger(event.retry_delay_ms) && event.retry_delay_ms >= 0) parts.push("До повтора: " + event.retry_delay_ms / 1000 + " с");
    if (Number.isSafeInteger(event.duration_ms) && event.duration_ms >= 0) parts.push(event.duration_ms + " мс");
    return parts.join(" · ");
  }
  async function readSelected(key, stamp) {
    const current = data[key], id = current.selected;
    if (!id) return;
    const initial = !current.loaded;
    try {
      const [job, result] = await Promise.all([request(`/api/run/${encodeURIComponent(id)}`),
        request(`/api/run/${encodeURIComponent(id)}/events?after=${current.cursor}&limit=500${initial ? "&tail=500" : ""}`)]);
      if (version !== stamp || mode !== key || current.selected !== id) return;
      if (job?.id !== id || job.mode !== key || !Array.isArray(result?.events)) throw apiError("invalid_response");
      current.job = {...current.jobs.find((item) => item.id === id), ...job}; current.warning = "";
      for (const event of result.events.slice().sort((a, b) => a.sequence - b.sequence)) {
        if (!Number.isSafeInteger(event.sequence) || event.sequence <= current.cursor) continue;
        current.lines.push({sequence: event.sequence, text: eventText(event)}); current.cursor = event.sequence;
      }
      current.lines = current.lines.slice(-500); current.loaded = true;
    } catch (error) {
      if (version !== stamp || mode !== key || current.selected !== id) return;
      if (error.status === 404) { current.jobs = current.jobs.filter((item) => item.id !== id); resetSelection(key, current.jobs[0]?.id || ""); refreshAgain = Boolean(current.selected); }
      else current.warning = errorText(error);
    }
  }
  function schedule() {
    clearTimeout(timer);
    timer = setTimeout(() => { if (!document.hidden) refresh(); else schedule(); }, modes.some((key) => activity[key] > 0) ? 2500 : 5000);
  }
  async function synchronizeStartRetries(id, initial, intent) {
    let saved = initial !== false;
    while (intent && saved !== intent.enabled) {
      let result;
      try { result = await request(`/api/run/${encodeURIComponent(id)}/retries`, {method: "POST", body: {enabled: intent.enabled}}); }
      catch (error) {
        if (["job_not_running", "job_not_found"].includes(error.code)) {refreshAgain = true; return;}
        throw error;
      }
      if (typeof result?.retry_enabled !== "boolean") throw apiError("invalid_response");
      saved = result.retry_enabled;
    }
  }
  async function refresh() {
    if (refreshing) { refreshAgain = true; return refreshing; }
    clearTimeout(timer);
    if (uncertainStart) { uncertainStart.checked = false; controls(); }
    refreshing = (async () => {
      const key = mode, stamp = version, mutation = mutationVersion;
      const snapshots = modes.map((item) => ({key: item, offset: data[item].offset}));
      const responses = await Promise.allSettled(snapshots.map((item) => request(`/api/history?mode=${item.key}&limit=50&offset=${item.offset}`)));
      if (mutation !== mutationVersion) { refreshAgain = true; return; }
      let failed = false;
      responses.forEach((response, index) => {
        const snapshot = snapshots[index], current = data[snapshot.key];
        if (response.status !== "fulfilled" || !Array.isArray(response.value?.jobs) ||
            !Number.isSafeInteger(response.value.running_count) || response.value.running_count < 0) { activity[snapshot.key] = null; failed = true; return; }
        const result = response.value;
        activity[snapshot.key] = result.running_count;
        if (snapshot.offset !== current.offset) return;
        current.jobs = result.jobs.filter((job) => idPattern.test(job.id || "") && job.mode === snapshot.key);
        current.hasMore = result.has_more === true;
        current.jobs.forEach((job) => current.knownIds.add(job.id));
        if (!current.restored) {
          if (!current.selected) resetSelection(snapshot.key, current.jobs[0]?.id || "");
          current.restored = true;
        }
        if (uncertainStart?.mode === snapshot.key) {
          const candidates = current.jobs.filter((job) => !uncertainStart.before.has(job.id));
          const candidate = candidates[0];
          if (candidate) {
            const pending = uncertainStart;
            const showStarted = mode === snapshot.key && version === pending.viewVersion;
            uncertainStart = null;
            pending.retryIntent?.close();
            if (showStarted) {
              resetSelection(snapshot.key, candidate.id);
              current.retryWarning = "Найдено задание в истории. Проверьте источник и настройку автоматических повторов: результат исходного запроса запуска не подтверждён. Изменение галочки во время запуска могло не примениться.";
            }
          }
        }
      });
      if (uncertainStart) uncertainStart.checked = !failed;
      connectionWarning = failed ? "Не удалось обновить историю всех режимов. Состояние активных переносов не подтверждено; проверьте связь и нажмите «Обновить»." : "";
      if (version === stamp && mode === key && data[key]) await readSelected(key, stamp);
      render();
    })();
    try { await refreshing; }
    finally { refreshing = null; if (refreshAgain) { refreshAgain = false; refresh(); } else schedule(); }
  }
  function selectMode(next) { mode = next; version += 1; render(); return refresh(); }
  async function start({endpoint, body, mode: key}) {
    if (!data[key] || isLocked()) return;
    starting = true;
    const stamp = version;
    let retryIntent = window.diskCopyRetries.track(key);
    const before = new Set(data[key].knownIds);
    data[key].offset = 0;
    if (data[key].selected) before.add(data[key].selected);
    data[key].warning = "Запускаю перенос…"; render();
    try {
      const result = await request(endpoint, {method: "POST", body});
      if (!idPattern.test(result?.job_id || "")) throw apiError("invalid_response");
      activity[key] = Math.max(1, activity[key] || 0);
      let retryWarning = "";
      try { await synchronizeStartRetries(result.job_id, body.retry_enabled, retryIntent); }
      catch (error) {retryWarning = errorText(error);}
      if (version === stamp && mode === key) {resetSelection(key, result.job_id); data[key].warning = retryWarning; version += 1;}
    } catch (error) {
      data[key].warning = errorText(error);
      if (error.code === "network" || error.code === "invalid_response" || (error.status >= 500 && !knownErrors[error.code])) {
        uncertainStart = {mode: key, before, checked: false, viewVersion: stamp, retryIntent}; retryIntent = null;
      }
    } finally {retryIntent?.close(); mutationVersion += 1; starting = false; render(); await refresh();}
  }
  get("run-history").addEventListener("change", () => {
    if (!data[mode]) return;
    const id = get("run-history").value;
    if (id && !idPattern.test(id)) return;
    resetSelection(mode, id); version += 1; render(); return refresh();
  });
  get("run-history-refresh").addEventListener("click", refresh);
  get("run-start-unlock").addEventListener("click", () => {
    if (!uncertainStart?.checked || starting || modes.some((key) => activity[key] !== 0)) return;
    data[uncertainStart.mode].warning = ""; uncertainStart.retryIntent?.close(); uncertainStart = null; render();
  });
  for (const [id, direction] of [["run-history-previous", -1], ["run-history-next", 1]]) get(id).addEventListener("click", () => {
    if (!data[mode] || get(id).disabled) return;
    data[mode].offset = Math.max(0, data[mode].offset + direction * 50); version += 1; render(); return refresh();
  });
  get("run-journal-csv").addEventListener("click", async () => {
    const key = mode, current = data[key], id = current?.selected, stamp = version;
    if (!id || exportBusy || cleanupBusy) return;
    exportBusy = true; controls();
    try {
      const blob = await request(`/api/run/${encodeURIComponent(id)}/journal`, {blob: true});
      if (version !== stamp || mode !== key || current.selected !== id) return;
      const url = URL.createObjectURL(blob), link = document.createElement("a");
      link.href = url; link.download = `${key}-journal-${id}.csv`; document.body.appendChild(link); link.click(); link.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch (error) { current.warning = errorText(error); }
    finally { exportBusy = false; render(); }
  });
  get("run-journal-cleanup").addEventListener("click", async () => {
    const key = mode, current = data[key];
    if (!current || cleanupBusy || exportBusy || activity[key] > 0) return;
    cleanupBusy = true; current.cleanupError = false; current.cleanup = "Очищаю журнальный кеш…"; render();
    try {
      const result = await request("/api/history/cleanup", {method: "POST", body: {mode: key}});
      if (![result?.removed_jobs, result?.removed_events, result?.removed_bytes].every((value) => Number.isSafeInteger(value) && value >= 0)) throw apiError("invalid_response");
      current.cleanup = `«${names[key]}»: удалено журналов — ${result.removed_jobs}, событий — ${result.removed_events}. Объём: ${volume(result.removed_bytes)}.`;
      current.jobs = []; current.offset = 0; current.hasMore = false; resetSelection(key, ""); version += 1; mutationVersion += 1;
    } catch (error) { current.cleanupError = true; current.cleanup = errorText(error); }
    finally { cleanupBusy = false; render(); await refresh(); }
  });
  window.addEventListener("open-local-journal", (event) => {
    const id = event.detail?.jobId;
    if (typeof id !== "string" || !idPattern.test(id)) return;
    mode = "local"; resetSelection("local", id); version += 1; render(); refresh();
    get("run-journal-mode")?.scrollIntoView?.({behavior: "smooth", block: "start"});
  });
  get("local-source-input")?.addEventListener("change", controls);
  window.runJournal = {selectMode, start, refresh};
  document.addEventListener("visibilitychange", () => { if (!document.hidden) refresh(); });
  window.addEventListener("pagehide", () => clearTimeout(timer));
  render();
  refresh();
})();
