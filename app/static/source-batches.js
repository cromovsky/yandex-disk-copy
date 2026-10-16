(function () {
  "use strict";

  const get = (id) => document.getElementById(id);
  const stateNames = {
    draft: "Нужен Dry Run", needs_attention: "Нужна проверка", pending: "Ожидает проверки", access: "Проверка доступа",
    inventory: "Составление полного плана", planning: "Dry Run выполняется", ready: "План готов",
    transfer: "Перенос", running: "Перенос выполняется", paused: "Приостановлено",
    awaiting_credentials: "Введите доступы заново", completed: "Завершено",
    completed_with_issues: "Завершено с ошибками", stopped: "Остановлено", error: "Ошибка",
    skipped_by_user: "Пропущен пользователем",
    planned: "В плане", verified: "Проверено", done: "Перенесено", saved: "Сохранено",
    skipped: "Пропущено", already_present_verified: "Уже есть, MD5 совпадает", already_verified: "Уже есть, MD5 совпадает",
    conflict: "Конфликт содержимого", destination_conflict: "Конфликт содержимого",
    verification_unavailable: "Проверка недоступна", failed: "Ошибка", source_changed: "Источник изменился",
    downloading: "Скачивание", staged: "В кеше", transferring: "Передача",
    waiting_operation: "Ожидание операции", verifying: "Проверка результата", committing: "Завершение",
  };
  const errorNames = {
    plan_conflicts: "В плане есть конфликты или файлы без проверяемого MD5. Откройте файлы источника и устраните причину.",
    plan_incomplete: "Не все выбранные источники прошли полный Dry Run. Повторите проверку или исключите проблемные источники.",
    target_folder_collision: "Источники получили одинаковую папку назначения. Уточните список и повторите Dry Run.",
    local_cache_space_insufficient: "Недостаточно места в кеше для четырёх одновременных файлов и резерва.",
    batch_not_active: "Список сейчас не выполняется. Обновите его состояние.",
    batch_not_paused: "Список сейчас не приостановлен. Обновите его состояние.",
    batch_stopped: "Список окончательно остановлен. Нажмите «Начать заново».",
    batch_cannot_replan: "После начала переноса состав списка зафиксирован. Для нового плана используйте общий Dry Run.",
    selection_not_allowed: "Выбор файлов доступен до начала переноса, после полного Dry Run.",
    empty_selection: "Выберите хотя бы один файл или папку для переноса.",
    stale_revision: "План изменился. Выбор обновлён из сохранённого плана; повторите действие.",
    selection_locked: "Состав выполнявшегося списка зафиксирован. Для других источников создайте новый Dry Run.",
    invalid_source_selection: "Выберите хотя бы один источник. После ошибки выбор восстановлен из сохранённого плана.",
    source_busy: "Источник ещё завершает действие. Обновите состояние и повторите позже.",
    dry_run_required: "Сначала завершите Dry Run всех выбранных источников.",
    invalid_sources_csv: "CSV должен быть текстовым файлом размером до 2 Мб.",
    invalid_sources_columns: "Первая строка CSV должна быть type,email,vd_hash,path, с запятыми в качестве разделителей.",
    invalid_sources_row: "Проверьте строки CSV: type — personal/shared, email — почта, vd_hash — метка для общего диска. Допустимо до 1000 строк.",
    empty_source_list: "Добавьте хотя бы один источник в CSV.",
    invalid_batch_config: "Заполните ID организации, Client ID и размер страницы.",
    invalid_batch_destination: "Проверьте адрес API, владельца токена, метку общего диска и подтверждение владельца.",
    invalid_credentials: "Введите корректный Client secret и, для On-premises, токен назначения.",
    invalid_page_limit: "PAGE_LIMIT должен быть целым числом от 1 до 1000.",
    destination_check_failed: "Не удалось проверить диск назначения. Подробности — в журнале.",
    invalid_csv: "Проверьте CSV: нужны столбцы type,email,vd_hash,path.",
    invalid_source_email: "В email укажите почту владельца личного диска или сотрудника с доступом к общему диску.",
    invalid_source_type: "В type допустимы personal и shared.",
    invalid_source_vd_hash: "Для общего диска укажите метку vd_hash из адреса /vd/.",
    duplicate_source: "В списке повторяется один источник.",
    path_collision: "Несколько источников или объектов имеют одинаковый итоговый путь.",
    source_target_collision: "Папки назначения источников совпадают.",
    source_auth_failed: "SaaS: проверьте сервисное приложение, секрет, организацию и email.",
    source_admin_auth_failed: "SaaS: проверьте админский токен и право directory:read_users.",
    source_user_not_in_organization: "Email не найден в указанной SaaS-организации. Проверьте email и ORGID.",
    source_user_blocked: "Сотрудник SaaS заблокирован. Разблокируйте его в админке и повторите Dry Run; утилита не меняет статус.",
    source_admin_check_failed: "Не удалось проверить сотрудника в SaaS-организации. Проверьте доступность API 360 и журнал.",
    source_admin_invalid_response: "API 360 вернул некорректный ответ при проверке сотрудника. Подробности — в журнале.",
    source_unavailable: "SaaS недоступен. Восстановите связь и повторите Dry Run источника.",
    destination_unavailable: "On-premises недоступен. Кеш сохранён; восстановите доступ и продолжите.",
    destination_connection_failed: "Не удалось соединиться с On-premises. Проверьте сеть из контейнера.",
    destination_auth_failed: "On-premises: проверьте токен, владельца и доступ к общему диску.",
    destination_read_only: "На общем диске нужны права записи.",
    destination_space_insufficient: "На диске назначения недостаточно места для выбранных источников.",
    local_space_insufficient: "На машине утилиты недостаточно места для локальных копий.",
    staging_space_insufficient: "Недостаточно места для временного кеша.",
    source_hash_unavailable: "SaaS не сообщил MD5; содержимое нельзя подтвердить.",
    destination_conflict: "В назначении есть другое содержимое; автоматической перезаписи нет.",
    plan_not_ready: "Дождитесь полного плана всех выбранных источников или исключите источники с ошибками.",
    incomplete_plan: "Обход диска прерван. Повторите Dry Run этого источника.",
    no_sources_selected: "Выберите хотя бы один источник.",
    credentials_required: "Введите Client secret и, для On-premises, токен назначения заново.",
    credential_binding_mismatch: "Настройки изменились. Восстановите настройки списка или создайте новый Dry Run.",
    onprem_staged_required: "Это исторический список с прежним способом переноса. Для переноса выполните новый Dry Run; история, остановка и экспорт доступны.",
    onprem_access_required: "Введите ключ доступа к режиму On-premises.",
    onprem_disabled: "SaaS → On-premises в этой сборке выключен.",
    batch_busy: "Действие ещё выполняется. Обновите состояние списка.",
    invalid_state: "Действие недоступно в текущем состоянии. Обновите список.",
    network_error: "Соединение с утилитой прервалось. Обновите список перед повтором.",
    invalid_response: "Ответ утилиты не удалось прочитать. Обновите список.",
    validation_error: "Заполните общие настройки доступа и проверьте CSV.",
  };
  const activeStates = new Set(["planning", "running"]);
  const terminalStates = new Set(["completed", "completed_with_issues", "stopped"]);
  const controllers = {};
  let configuration = null;

  function humanState(value) { return stateNames[value] || String(value || "—"); }
  function bytes(value) {
    const number = Number(value);
    if (!Number.isFinite(number) || number < 0) return "—";
    const units = ["Б", "Кб", "Мб", "Гб", "Тб"];
    let scaled = number, index = 0;
    while (scaled >= 1024 && index < units.length - 1) { scaled /= 1024; index += 1; }
    return `${scaled.toLocaleString("ru-RU", {maximumFractionDigits: index ? 1 : 0})} ${units[index]}`;
  }
  function date(value) {
    const parsed = new Date(value);
    return Number.isNaN(parsed.valueOf()) ? "—" : parsed.toLocaleString("ru-RU");
  }
  function failure(code, status, diagnostic) {
    const error = new Error(code);
    error.code = code; error.status = status; error.diagnostic = diagnostic;
    return error;
  }
  function uuid() {
    if (typeof crypto.randomUUID === "function") return crypto.randomUUID();
    return Array.from(crypto.getRandomValues(new Uint8Array(16)), (value) => value.toString(16).padStart(2, "0")).join("");
  }
  function csvValue(value) { return '"' + String(value ?? "").replace(/"/g, '""') + '"'; }
  function sourceCsv(sources) {
    return "type,email,vd_hash,path\n" + sources.map((source) => [source.type, source.email, source.vd_hash || "", String(source.path || "/").replace(/^disk:/, "")].map(csvValue).join(",")).join("\n");
  }

  class SourceList {
    constructor(mode) {
      this.mode = mode; this.batch = null; this.sources = []; this.history = [];
      this.busy = false; this.offset = 0; this.hasMore = false; this.timer = null;
      this.revision = 0; this.pendingCreate = null; this.plannedCsv = ""; this.entrySource = null; this.entryOffset = 0; this.entryChecks = []; this.entryEntries = [];
      this.choice = get(mode + "-source-input");
      if (!this.choice) return;
      this.choice.addEventListener("change", () => this.toggle());
      this.node("file").addEventListener("change", () => this.loadFile());
      this.node("csv").addEventListener("input", () => this.controls());
      this.node("plan").addEventListener("click", () => this.action(() => this.createPlan()));
      this.node("restart").addEventListener("click", () => this.action(() => this.createPlan()));
      this.node("start").addEventListener("click", () => this.action(() => this.mutate("start", true)));
      this.node("pause").addEventListener("click", () => this.action(() => this.mutate("pause")));
      this.node("resume").addEventListener("click", () => this.action(() => this.mutate("resume", true)));
      this.node("stop").addEventListener("click", () => this.action(() => this.mutate("stop")));
      this.node("refresh").addEventListener("click", () => this.action(() => this.refresh()));
      this.node("history-previous").addEventListener("click", () => this.action(() => this.refresh(Math.max(0, this.offset - 50))));
      this.node("history-next").addEventListener("click", () => this.action(() => this.refresh(this.offset + 50)));
      this.node("history").addEventListener("change", () => {
        const id = this.node("history").value;
        if (id) return this.action(() => this.open(id));
      });
      this.node("entries-previous").addEventListener("click", () => this.action(() => this.loadEntries(this.entrySource, Math.max(0, this.entryOffset - 100))));
      this.node("entries-next").addEventListener("click", () => this.action(() => this.loadEntries(this.entrySource, this.entryOffset + 100)));
      ["report", "journal"].forEach((kind) => this.node(kind).addEventListener("click", () => this.action(() => this.export(kind))));
      this.commonIds().forEach((id) => {
        const field = get(id);
        field?.addEventListener("input", () => this.controls());
        field?.addEventListener("change", () => this.controls());
      });
      this.toggle(false);
    }
    node(name) { return get(`${this.mode}-batch-${name}`); }
    listMode() { return this.choice.value === "list"; }
    enabled() { return Boolean(configuration) && (this.mode === "local" || configuration.onprem_enabled); }
    authorized() { return this.mode !== "onprem" || !configuration?.onprem_auth_required || Boolean(get("onprem-access-key").value.trim()); }
    visible() { return this.listMode() && document.querySelector(`.mode-tab[data-mode="${this.mode}"]`)?.classList.contains("active") && !document.hidden; }
    commonIds() {
      return this.mode === "local" ? ["local_orgid", "local_client_id", "local_client_secret", "local_admin_token", "local_page_limit"]
        : ["onprem-source-org", "onprem-source-client", "onprem-source-secret", "onprem-source-admin-token", "onprem-destination-url", "onprem-destination-identity", "onprem-destination-token", "onprem-destination-vd-hash", "onprem-strategy", "onprem-identity-confirmed", "onprem-access-key"];
    }
    config() {
      const value = (id) => get(id).value.trim();
      if (this.mode === "local") return {source_org_id: value("local_orgid"), source_client_id: value("local_client_id"), page_limit: Number(value("local_page_limit")), retry_enabled: window.diskCopyRetries?.value(this.mode) !== false};
      return {source_org_id: value("onprem-source-org"), source_client_id: value("onprem-source-client"), page_limit: 1000,
        destination_url: value("onprem-destination-url"), destination_identity: value("onprem-destination-identity"), destination_vd_hash: value("onprem-destination-vd-hash"),
        retry_enabled: window.diskCopyRetries?.value(this.mode) !== false, strategy: "staged", identity_confirmed: get("onprem-identity-confirmed").checked};
    }
    secrets() {
      const value = this.mode === "local" ? {source_client_secret: get("local_client_secret").value}
        : {source_client_secret: get("onprem-source-secret").value, destination_token: get("onprem-destination-token").value};
      const admin = get(this.mode === "local" ? "local_admin_token" : "onprem-source-admin-token").value;
      if (admin.trim()) value.source_admin_token = admin;
      return value;
    }
    valid() {
      const config = this.config(), secret = this.secrets();
      if (!config.source_org_id || !config.source_client_id || !secret.source_client_secret.trim() || !Number.isInteger(config.page_limit) || config.page_limit < 1 || config.page_limit > 1000) return false;
      if (this.mode === "onprem") {
        if (!config.destination_url || !config.destination_identity || !config.destination_vd_hash || !config.identity_confirmed || !secret.destination_token.trim()) return false;
        try { const url = new URL(config.destination_url); if (!["https:", "http:"].includes(url.protocol)) return false; } catch (_) { return false; }
      }
      return this.enabled() && this.authorized();
    }
    matches() {
      if (!this.batch?.config) return false;
      const current = this.config(), original = this.batch.config;
      return Object.keys(current).every((key) => {
        if (["identity_confirmed", "retry_enabled"].includes(key)) return true;
        if (key === "destination_url") {
          const normalize = (value) => {
            try { const url = new URL(value); url.pathname = url.pathname.replace(/\/+$/, "") || "/v1/disk"; return url.href; } catch (_) { return String(value || ""); }
          };
          return normalize(current[key]) === normalize(original[key]);
        }
        return String(current[key]) === String(original[key]);
      });
    }
    legacy() {
      return this.mode === "onprem" && Boolean(this.batch) && (this.batch.config?.strategy !== "staged" || this.sources.some((source) => source.child_strategy === "url"));
    }
    requireStaged() { if (this.legacy()) throw failure("onprem_staged_required"); }
    toggle(refresh = true) {
      const list = this.listMode();
      document.querySelectorAll(`[data-source-single="${this.mode}"]`).forEach((node) => { node.hidden = list; });
      get(this.mode + "-source-batches").hidden = !list;
      if (this.mode === "local") {
        get("local-admin-note").textContent = list
          ? "Для списка необязателен: проверяет email в указанной организации и блокировку сотрудника, без изменения статуса. Нужно право directory:read_users."
          : "Для одного диска обязателен: проверяет сотрудника и при необходимости временно снимает блокировку. Нужны directory:read_users и directory:write_users.";
      }
      if (this.mode === "onprem") {
        ["onprem-source-email", "onprem-source-path"].forEach((id) => { get(id).required = !list; get(id).disabled = list; });
        const preview = get("onprem-destination-preview");
        if (preview) preview.hidden = list;
      }
      clearTimeout(this.timer);
      this.controls();
      if (refresh && this.visible() && this.enabled() && this.authorized()) return this.action(() => this.refresh());
    }
    message(text, error = false) { this.node("notice").textContent = text; this.node("notice").className = "batch-notice" + (error ? " error" : ""); }
    diagnostic(value) {
      let result = String(value || "").replace(/https?:\/\/[^\s<>"']+/gi, (url) => url.split(/[?#]/)[0]).replace(/\b(?:OAuth|Bearer)\s+\S+/gi, "[доступ скрыт]");
      const values = [...Object.values(this.secrets()), get("onprem-access-key")?.value];
      values.filter((secret) => secret && secret.length >= 4).forEach((secret) => { result = result.split(secret).join("[скрыто]"); });
      return result.slice(0, 600);
    }
    errorText(error) {
      if (!error) return "";
      const code = typeof error === "string" ? error : error.code || error.detail || error.error || "";
      const status = error.http_status || error.status;
      const prefix = errorNames[code] || (/^[a-z][a-z0-9_]{0,100}$/.test(code) ? `Ошибка: ${code}.` : "Действие не выполнено. Проверьте журнал.");
      const details = this.diagnostic(error.diagnostic_message || error.diagnostic || error.message);
      return `${prefix}${Number.isInteger(status) ? ` HTTP ${status}.` : ""}${details && details !== code ? ` ${details}` : ""}`;
    }
    async request(path = "", options = {}) {
      if (!this.enabled() || !this.authorized()) throw failure("onprem_access_required");
      const headers = {Accept: options.blob ? "text/csv" : "application/json"};
      const key = this.mode === "onprem" ? get("onprem-access-key").value.trim() : "";
      if (key) headers["X-Onprem-Key"] = key;
      if (options.body !== undefined) headers["Content-Type"] = "application/json";
      let response;
      try { response = await fetch("/api/source-batches" + path, {method: options.method || "GET", headers,
        body: options.body === undefined ? undefined : JSON.stringify(options.body), credentials: "same-origin", cache: "no-store", redirect: "error"}); }
      catch (_) { throw failure("network_error"); }
      if (!response.ok) {
        let body = {};
        try { body = await response.json(); } catch (_) {}
        const detail = body.detail;
        throw failure(typeof detail === "string" ? detail : detail?.code || "invalid_request", response.status, body.diagnostic_message || detail?.diagnostic_message);
      }
      try { return options.blob ? await response.blob() : await response.json(); }
      catch (_) { throw failure("invalid_response"); }
    }
    path(suffix = "") { return "/" + encodeURIComponent(this.batch.id) + (suffix ? "/" + suffix : ""); }
    accept(detail) {
      if (!detail?.batch?.id || !Array.isArray(detail.sources) || detail.batch.mode !== this.mode) throw failure("invalid_response");
      this.batch = detail.batch; this.sources = detail.sources; this.render();
    }
    async action(work) {
      if (this.busy || !this.enabled() || !this.authorized()) return;
      this.busy = true; this.revision += 1; clearTimeout(this.timer); this.controls(); this.message("");
      try { await work(); } catch (error) { this.message(this.errorText(error), true); }
      finally { this.busy = false; this.controls(); this.schedule(); }
    }
    controls() {
      if (this.listMode()) {
        const batch = this.batch, id = batch?.id;
        window.diskCopyRetries?.bind(this.mode, {
          id: "list:" + (id || "new"), enabled: batch ? batch.config?.retry_enabled !== false : window.diskCopyRetries.value(this.mode),
          loading: false, editable: Boolean(batch && !this.legacy() && !terminalStates.has(batch.state)),
          save: async (enabled) => {
            const result = await this.request("/" + encodeURIComponent(id) + "/retries", {method: "POST", body: {enabled}});
            if (this.batch?.id === id) this.batch.config.retry_enabled = result.retry_enabled;
            return result;
          }, errorText: (error) => this.errorText(error),
        });
      }
      const locked = this.busy || !this.enabled() || !this.authorized();
      const selected = this.sources.filter((source) => source.selected);
      const batch = this.batch, legacy = this.legacy();
      const csvMatches = this.node("csv").value === this.plannedCsv;
      const ready = selected.length > 0 && selected.every((source) => source.plan_complete && ["ready", "completed"].includes(source.state));
      this.node("plan").disabled = locked || (!legacy && activeStates.has(batch?.state)) || !this.valid() || !this.node("csv").value.trim();
      this.node("start").disabled = locked || legacy || !batch || batch.state !== "ready" || !ready || !csvMatches || !this.matches() || !this.valid() || Number(batch?.selection?.selected) === 0;
      this.node("pause").disabled = locked || legacy || !batch || !activeStates.has(batch.state);
      this.node("resume").disabled = locked || legacy || !batch || !["paused", "awaiting_credentials"].includes(batch.state) || !csvMatches || !this.matches() || !this.valid();
      this.node("stop").disabled = locked || !batch || ["stopped", "completed"].includes(batch.state);
      this.node("restart").disabled = locked || !batch || !terminalStates.has(batch.state) || !this.valid() || !this.node("csv").value.trim();
      ["report", "journal"].forEach((name) => { this.node(name).disabled = locked || !batch; });
      this.node("refresh").disabled = locked;
      this.node("history").disabled = locked;
      this.node("history-previous").disabled = locked || this.offset === 0;
      this.node("history-next").disabled = locked || !this.hasMore;
      this.entryChecks.forEach(({check, entry, sourceId}) => { check.disabled = !this.entrySelectionAvailable(sourceId, entry); });
      this.sources.forEach((source) => {
        if (source._checkbox) source._checkbox.disabled = locked || legacy || !batch || batch.started || !csvMatches || activeStates.has(batch.state) || terminalStates.has(batch.state);
        if (source._plan) source._plan.disabled = locked || legacy || !this.matches() || !this.valid() || !batch || batch.started || !csvMatches || activeStates.has(batch.state) || terminalStates.has(batch.state);
        if (source._details) source._details.disabled = locked;
      });
    }
    async loadFile() {
      const file = this.node("file").files?.[0];
      if (!file) return;
      if (file.size > 2 * 1024 * 1024) { this.message("CSV слишком большой. Разделите список на несколько групп до 2 Мб.", true); return; }
      try { this.node("csv").value = await file.text(); this.controls(); this.message("Список загружен. Заполните общие доступы и нажмите Dry Run."); }
      catch (_) { this.message("Не удалось прочитать CSV. Выберите файл повторно.", true); }
    }
    fill(config) {
      const map = this.mode === "local" ? {source_org_id: "local_orgid", source_client_id: "local_client_id", page_limit: "local_page_limit"}
        : {source_org_id: "onprem-source-org", source_client_id: "onprem-source-client", destination_url: "onprem-destination-url", destination_identity: "onprem-destination-identity", destination_vd_hash: "onprem-destination-vd-hash", strategy: "onprem-strategy"};
      Object.entries(map).forEach(([key, id]) => {
        if (key === "strategy") get(id).value = "staged";
        else if (config[key] !== undefined) get(id).value = String(config[key]);
      });
      if (this.mode === "onprem") get("onprem-identity-confirmed").checked = false;
    }
    async createPlan() {
      if (!this.valid()) throw failure("validation_error");
      const retryIntent = window.diskCopyRetries.track(this.mode);
      try {
        const config = this.config(), sources_csv = this.node("csv").value;
        const fingerprint = JSON.stringify({mode: this.mode, config: {...config, retry_enabled: undefined}, sources_csv});
        if (!this.pendingCreate || this.pendingCreate.fingerprint !== fingerprint) this.pendingCreate = {fingerprint, key: uuid()};
        const detail = await this.request("", {method: "POST", body: {mode: this.mode, config, sources_csv, credentials: this.secrets(), idempotency_key: this.pendingCreate.key}});
        if (!detail?.batch?.id) throw failure("invalid_response");
        while ((detail.batch.config?.retry_enabled !== false) !== retryIntent.enabled) {
          const result = await this.request("/" + encodeURIComponent(detail.batch.id) + "/retries", {method: "POST", body: {enabled: retryIntent.enabled}});
          if (typeof result?.retry_enabled !== "boolean") throw failure("invalid_response");
          detail.batch.config.retry_enabled = result.retry_enabled;
        }
        this.accept(detail);
        this.pendingCreate = null;
        this.plannedCsv = sources_csv;
        this.node("details").hidden = true; this.entrySource = null;
        this.accept(await this.request(this.path("plan"), {method: "POST", body: {}}));
        this.message("Dry Run запущен: проверка доступа, затем полный план каждого источника. Диски остаются без изменений.");
        await this.refreshHistory();
      } finally {retryIntent.close();}
    }

    async attach() {
      this.requireStaged();
      if (!this.valid() || !this.matches()) throw failure("credential_binding_mismatch");
      this.accept(await this.request(this.path("credentials"), {method: "POST", body: {credentials: this.secrets()}}));
    }
    async mutate(action, access = false) {
      if (!this.batch) return;
      if (action !== "stop") this.requireStaged();
      if (access) await this.attach();
      if (action === "resume" && this.batch.state === "ready") {
        this.message("Доступ восстановлен. План готов; нажмите кнопку начала переноса."); return;
      }
      const endpoint = action === "resume" && this.batch.state === "draft" ? "plan" : action;
      this.accept(await this.request(this.path(endpoint), {method: "POST", body: {}}));
      this.message({start: "Перенос выбранных источников запущен.", pause: "Пауза запрошена. Дождитесь состояния «Приостановлено».", resume: "Работа списка продолжена.", stop: "Остановка запрошена. Локальные операции и очередь списка будут освобождены."}[action]);
    }
    async select(source, checked) {
      const ids = this.sources.filter((row) => row.id === source.id ? checked : row.selected).map((row) => row.id);
      try { this.requireStaged(); this.accept(await this.request(this.path("selection"), {method: "PUT", body: {source_ids: ids}})); }
      catch (error) { if (source._checkbox) source._checkbox.checked = Boolean(source.selected); throw error; }
    }
    entrySelectionAvailable(sourceId, entry) {
      const source = this.sources.find((item) => item.id === sourceId);
      return !this.busy && this.enabled() && this.authorized() && this.listMode() && !this.legacy() &&
        this.entrySource === sourceId && this.entryEntries.includes(entry) &&
        this.node("csv").value === this.plannedCsv && this.matches() &&
        source?.summary?.selection?.editable === true && entry.can_select === true;
    }
    async selectEntry(sourceId, entry, checked, check) {
      const batchId = this.batch.id, revision = this.revision;
      const current = () => this.batch?.id === batchId && this.entrySource === sourceId && this.revision === revision;
      try {
        const detail = await this.request(this.path(`sources/${encodeURIComponent(sourceId)}/entries/${encodeURIComponent(entry.id)}/selection`), {
          method: "POST", body: {selected: checked},
        });
        if (!current()) return;
        if (detail?.batch?.id !== batchId) throw failure("invalid_response");
        this.accept(detail);
        await this.loadEntries(sourceId, this.entryOffset);
      } catch (error) {
        if (!current()) return;
        check.checked = entry.selected !== false; check.indeterminate = entry.selection_partial === true;
        try { await this.refresh(); } catch (_) {}
        if (current()) throw error;
      }
    }
    async replan(source) {
      await this.attach();
      this.accept(await this.request(this.path(`sources/${encodeURIComponent(source.id)}/plan`), {method: "POST", body: {}}));
      this.message("Повторный Dry Run выбранного источника запущен. Планы остальных источников сохраняются.");
    }
    async open(id) {
      this.accept(await this.request("/" + encodeURIComponent(id)));
      this.fill(this.batch.config || {}); this.node("csv").value = sourceCsv(this.sources); this.plannedCsv = this.node("csv").value;
      this.node("details").hidden = true; this.entrySource = null;
      this.renderHistory(); this.controls();
      if (this.legacy()) this.message(errorNames.onprem_staged_required);
      else if (!this.batch.credentials_available) this.message("История сохранена. Для продолжения введите доступы в полях выше; они не сохраняются на диск.");
    }
    async refreshHistory(offset = this.offset, revision = this.revision) {
      const result = await this.request(`?mode=${this.mode}&limit=50&offset=${offset}`);
      if (revision !== this.revision) return;
      if (!Array.isArray(result?.batches)) throw failure("invalid_response");
      this.history = result.batches; this.offset = offset; this.hasMore = Boolean(result.has_more); this.renderHistory();
    }
    async refresh(offset = this.offset) {
      const revision = this.revision;
      await this.refreshHistory(offset, revision);
      if (revision !== this.revision) return;
      if (this.batch) {
        const detail = await this.request(this.path());
        if (revision !== this.revision) return;
        this.accept(detail);
      }
      if (this.entrySource) await this.loadEntries(this.entrySource, this.entryOffset);
    }
    renderHistory() {
      const select = this.node("history"); select.replaceChildren();
      const empty = document.createElement("option"); empty.value = ""; empty.textContent = this.history.length ? "Выберите список" : "Списков пока нет"; select.appendChild(empty);
      const lists = this.batch && !this.history.some((batch) => batch.id === this.batch.id) ? [this.batch, ...this.history] : this.history;
      lists.forEach((batch) => { const option = document.createElement("option"); option.value = batch.id; option.textContent = `${date(batch.created_at)} · ${humanState(batch.state)} · источников: ${batch.summary?.selected_sources ?? "—"}`; select.appendChild(option); });
      select.value = this.batch?.id || ""; this.controls();
    }
    render() {
      const batch = this.batch, summary = batch.summary || {};
      const selection = this.batch.selection;
      this.node("entries-selection").textContent = selection
        ? `Выбрано: ${selection.selected} · исключено: ${selection.excluded} · объём: ${bytes(selection.selected_bytes)} (в списке)` : "";
      this.node("result").hidden = false;
      this.node("date").textContent = date(batch.created_at);
      this.node("state").textContent = batch.state === "stopped" && batch.stop_pending ? "Остановка выполняется · новый Dry Run доступен"
        : humanState(batch.state) + (activeStates.has(batch.state) && batch.phase ? ` · ${humanState(batch.phase)}` : "");
      this.node("count").textContent = `${summary.selected_sources ?? this.sources.filter((source) => source.selected).length} выбрано · ${summary.ready_sources ?? 0} готово · ${summary.failed_sources ?? 0} с ошибками`;
      this.node("bytes").textContent = bytes(summary.total_bytes);
      this.node("required").textContent = bytes(summary.required_bytes);
      this.node("verified").textContent = bytes(summary.verified_bytes);
      const download = this.sources.reduce((total, source) => total + (Number(source.transfer_rates?.download_bytes_per_second) || 0), 0);
      const upload = this.sources.reduce((total, source) => total + (Number(source.transfer_rates?.upload_bytes_per_second) || 0), 0);
      this.node("download-rate").textContent = bytes(download) + "/с";
      this.node("upload-rate").textContent = this.mode === "onprem" ? bytes(upload) + "/с" : "—";
      this.node("error").textContent = this.errorText(batch.error); this.node("error").hidden = !batch.error;
      const body = this.node("sources"); body.replaceChildren();
      this.sources.forEach((source) => {
        const row = document.createElement("tr");
        const labels = ["Вкл.", "Источник → папка", "Состояние", "Файлы / папки", "Всего / к переносу", "Крупнейший файл", "Скачивание / загрузка", "Действия и ошибка"];
        const cell = (text) => { const node = document.createElement("td"); node.setAttribute("data-label", labels[row.children.length]); if (text !== undefined) node.textContent = text; row.appendChild(node); return node; };
        const check = document.createElement("input"); check.type = "checkbox"; check.checked = Boolean(source.selected); check.setAttribute("aria-label", `Включить ${source.email}${source.vd_hash ? " · " + source.vd_hash : ""}`);
        check.addEventListener("change", () => this.action(() => this.select(source, check.checked))); cell().appendChild(check); source._checkbox = check;
        cell(`${source.type === "shared" ? "Общий диск" : "Личный Диск"} · ${source.email}${source.vd_hash ? " [" + source.vd_hash + "]" : ""} · ${String(source.path || "/").replace(/^disk:/, "")}\n→ ${source.folder || "папка определится при Dry Run"}`);
        cell(humanState(source.state) + (!source.plan_complete && source.state === "ready" ? " · план неполный" : ""));
        const totals = source.summary || {};
        cell(`${totals.total_files ?? 0} / ${totals.total_directories ?? 0}`);
        cell(`${bytes(totals.total_bytes)} / ${bytes(totals.required_bytes)}`);
        cell(bytes(totals.largest_file));
        const rates = source.transfer_rates || {};
        cell(`${bytes(rates.download_bytes_per_second)}/с / ${this.mode === "onprem" ? bytes(rates.upload_bytes_per_second) + "/с" : "—"}`);
        const actions = cell();
        const button = (text, handler) => { const node = document.createElement("button"); node.type = "button"; node.className = "secondary-btn"; node.textContent = text; node.addEventListener("click", handler); actions.appendChild(node); return node; };
        source._plan = button("Повторить Dry Run", () => this.action(() => this.replan(source)));
        source._details = button("Файлы", () => this.action(() => this.loadEntries(source.id, 0)));
        if (source.child_job_id) button("Подробный журнал", () => window.dispatchEvent(new CustomEvent("open-onprem-job", {detail: {jobId: source.child_job_id}})));
        else if (source.journal_id) button("Подробный журнал", () => window.dispatchEvent(new CustomEvent("open-local-journal", {detail: {jobId: source.journal_id}})));
        if (source.error) { const error = document.createElement("p"); error.className = "field-note onprem-result bad"; error.textContent = this.errorText(source.error); actions.appendChild(error); }
        body.appendChild(row);
      });
      this.controls();
    }
    async loadEntries(sourceId, offset) {
      if (!sourceId || !this.batch) return;
      const revision = this.revision, batchId = this.batch.id;
      const result = await this.request(this.path(`sources/${encodeURIComponent(sourceId)}/entries?limit=100&offset=${offset}`));
      if (revision !== this.revision || this.batch?.id !== batchId) return;
      if (!Array.isArray(result?.entries)) throw failure("invalid_response");
      this.entrySource = sourceId; this.entryOffset = offset; this.entryEntries = result.entries;
      this.node("details").hidden = false;
      const source = this.sources.find((item) => item.id === sourceId);
      this.node("details-name").textContent = `${source?.email || ""}${source?.vd_hash ? " [" + source.vd_hash + "]" : ""}`;
      const body = this.node("entries"); body.replaceChildren(); this.entryChecks = [];
      result.entries.forEach((entry) => {
        const row = document.createElement("tr");
        const labels = ["Источник", "Назначение", "Размер", "Результат"];
        const excluded = entry.selected === false || entry.state === "skipped_by_user";
        const state = entry.state === "skipped_by_user" ? humanState(entry.state)
          : excluded ? "Исключено из переноса" : humanState(entry.state) + (entry.error ? ": " + this.errorText(entry.error) : "");
        [entry.source_path, entry.destination_path || entry.relative_path, entry.kind === "dir" || entry.kind === "directory" ? "Каталог" : bytes(entry.size), state].forEach((value, index) => {
          const cell = document.createElement("td"); cell.setAttribute("data-label", labels[index]); cell.textContent = String(value ?? "—");
          if (index === 0) {
            const label = document.createElement("label"); label.className = "entry-choice";
            const check = document.createElement("input"); check.type = "checkbox";
            check.checked = entry.selected !== false; check.indeterminate = entry.selection_partial === true;
            check.setAttribute("aria-label", `Включить в перенос: ${entry.source_path}`);
            check.disabled = !this.entrySelectionAvailable(sourceId, entry);
            check.addEventListener("change", () => {
              if (!this.entrySelectionAvailable(sourceId, entry)) {
                check.checked = entry.selected !== false; check.indeterminate = entry.selection_partial === true; return;
              }
              return this.action(() => this.selectEntry(sourceId, entry, check.checked, check));
            });
            const path = document.createElement("span"); path.textContent = String(value ?? "—");
            label.appendChild(check); label.appendChild(path); cell.replaceChildren(label);
            this.entryChecks.push({check, entry, sourceId});
          }
          row.appendChild(cell);
        }); body.appendChild(row);
      });
      this.node("entries-page").textContent = `Страница ${Math.floor(offset / 100) + 1} · объектов: ${result.total ?? result.entries.length}`;
      this.node("entries-previous").disabled = offset === 0;
      this.node("entries-next").disabled = offset + result.entries.length >= Number(result.total || 0);
    }
    async export(kind) {
      if (!this.batch) return;
      const id = this.batch.id;
      const blob = await this.request(this.path(kind + ".csv"), {blob: true});
      const url = URL.createObjectURL(blob), link = document.createElement("a");
      link.href = url; link.download = `${this.mode}-list-${kind}-${id.replace(/[^a-z0-9_-]/gi, "_")}.csv`;
      document.body.appendChild(link); link.click(); link.remove(); setTimeout(() => URL.revokeObjectURL(url), 1000);
    }
    schedule() {
      clearTimeout(this.timer);
      if (!this.visible() || !this.enabled() || !this.authorized()) return;
      this.timer = setTimeout(async () => {
        if (this.busy) { this.schedule(); return; }
        const revision = this.revision;
        try { await this.refresh(); } catch (error) { if (revision === this.revision) this.message(this.errorText(error), true); }
        this.schedule();
      }, 2500);
    }
  }

  ["local", "onprem"].forEach((mode) => { controllers[mode] = new SourceList(mode); });
  document.querySelectorAll(".mode-tab").forEach((tab) => tab.addEventListener("click", () => {
    Object.values(controllers).forEach((list) => {
      clearTimeout(list.timer);
      if (list.visible() && list.enabled() && list.authorized()) list.action(() => list.refresh());
    });
  }));
  document.addEventListener("visibilitychange", () => Object.values(controllers).forEach((list) => list.schedule()));
  window.addEventListener("pagehide", () => Object.values(controllers).forEach((list) => { clearTimeout(list.timer); }));
  window.sourceBatches = {open: (mode, id) => controllers[mode]?.action(() => controllers[mode].open(id)), refresh: (mode) => controllers[mode]?.action(() => controllers[mode].refresh())};
  Promise.resolve(window.diskCopyConfigPromise).then((config) => {
    configuration = config;
    Object.values(controllers).forEach((list) => { list.controls(); list.schedule(); });
  }).catch(() => {});
})();
