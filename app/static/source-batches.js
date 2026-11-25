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
    source_publication_denied: "SaaS отклонил публикацию исходного файла или папки. Код ответа можно посмотреть в подробном журнале.",
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
    invalid_disk_path: "Укажите абсолютный путь от корня Диска, например /Документы.",
    personal_mapping_conflict: "Одному источнику должен соответствовать один личный получатель, а каждому получателю — один источник. Для сбора на один Диск выберите «Общий диск».",
    overlapping_source_paths: "У одной пары повторён путь или выбрано его поддерево. Исключите одну из строк или измените CSV.",
    destination_path_collision: "Выбранные пути пересекаются на Диске получателя. Проверьте папки назначения и выбор файлов.",
    destination_identity_unverified: "Disk API не подтвердил UID сотрудника On-premises.",
    destination_identity_mismatch: "Учётная запись On-premises отличается от сотрудника, подтверждённого в плане.",
    invalid_shared_service_application: "Укажите полный email сотрудника с доступом к общему Диску и параметры сервисного приложения On-premises.",
    onprem_shared_grant_unavailable: "Эта версия утилиты не поддерживает сервисное приложение для общих Дисков.",
    invalid_personal_disk_api_url: "Введите адрес Disk API в формате https://cloud-api.<домен>/v1/disk.",
    invalid_destination_oauth_url: "Адрес OAuth не соответствует адресу Disk API этой установки. Проверьте адрес Disk API и составьте новый план.",
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
  function rowHasError(source) {
    return Boolean(source.error || source.mapping_error || Number(source.summary?.issues) > 0 ||
      source.state === "error" || source.state === "completed_with_issues");
  }
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
  function diskPath(value) { return String(value || "/").replace(/^disk:/, ""); }
  const personalCsvHeader = "source_email,path,destination_email,destination_folder";
  function sourceCsv(sources, personal) {
    const header = personal ? personalCsvHeader : "type,email,vd_hash,path";
    return [header, ...sources.map(source => {
      const values = personal ? [source.email, diskPath(source.path), source.destination_email, diskPath(source.destination_folder)]
        : [source.type, source.email, source.vd_hash || "", diskPath(source.path)];
      return values.map(csvValue).join(",");
    })].join("\n");
  }
  function normalizedApiUrl(value) {
    try {
      const url = new URL(value);
      url.pathname = url.pathname.replace(/\/+$/, "") || "/v1/disk";
      return url.href;
    } catch (_) { return String(value || ""); }
  }

  class SourceList {
    constructor(mode) {
      this.mode = mode; this.batch = null; this.sources = []; this.history = [];
      this.busy = false; this.offset = 0; this.hasMore = false; this.timer = null;
      this.revision = 0; this.pendingCreate = null; this.plannedCsv = ""; this.entrySource = null; this.entryOffset = 0; this.entryChecks = []; this.entryEntries = [];
      this.choice = get(mode + "-source-input");
      if (!this.choice) return;
      this.formRevision = 0;
      this.csvDrafts = {};
      this.errorsOnly = false;
      if (mode === "onprem") {
        const filter = this.node("filter-errors"), all = this.node("filter-all");
        filter.addEventListener("click", () => {
          if (filter.disabled || this.node("plan-title").hidden) return;
          this.errorsOnly = !this.errorsOnly;
          this.render();
          if (filter.disabled) this.node("plan-heading").focus?.({preventScroll: true});
        });
        all.addEventListener("click", () => {
          if (all.disabled || all.hidden || this.node("plan-title").hidden) return;
          this.errorsOnly = false;
          this.render();
          (filter.disabled ? this.node("plan-heading") : filter).focus?.({preventScroll: true});
        });
      }
      const edited = ({target}) => {
        this.formRevision++;
        const bindingField = target.dataset.onpremConfig || ["onprem-destination-personal", "onprem-destination-shared"].includes(target.id);
        if (this.listMode() && this.usesEmployeeToken() &&
            ((bindingField && !this.matches()) || (target === this.node("csv") && target.value !== this.plannedCsv))) {
          this.leaveEmployeePlan();
        }
        this.controls();
      };
      this.choice.addEventListener("change", () => this.toggle());
      this.node("file").addEventListener("change", () => this.loadFile());
      this.node("csv").addEventListener("input", edited);
      const command = (name, work) => {
        const button = this.node(name);
        button.addEventListener("click", () => {
          if (!button.disabled && !button.hidden) return this.action(work);
        });
      };
      command("plan", () => this.createPlan());
      command("restart", () => this.createPlan());
      command("start", () => this.mutate("start", true));
      command("pause", () => this.mutate("pause"));
      command("resume", () => this.mutate("resume", true));
      this.node("stop").addEventListener("click", () => {
        if (!this.batch || this.node("stop").disabled || this.node("stop").hidden) return;
        const name = date(this.batch.created_at);
        if (window.confirm(`Остановить весь список от ${name}? Обработка очереди прекратится. Для продолжения понадобится новый план. История и уже сохранённые файлы останутся.`)) {
          return this.action(() => this.mutate("stop"));
        }
      });
      this.node("refresh").addEventListener("click", () => this.action(() => this.refresh()));
      this.node("history-previous").addEventListener("click", () => this.action(() => this.refresh(Math.max(0, this.offset - 50))));
      this.node("history-next").addEventListener("click", () => this.action(() => this.refresh(this.offset + 50)));
      this.node("history").addEventListener("change", () => {
        const id = this.node("history").value;
        if (id) return this.action(() => this.open(id));
        if (this.usesEmployeeToken()) this.leaveEmployeePlan();
      });
      this.node("entries-previous").addEventListener("click", () => this.action(() => this.loadEntries(this.entrySource, Math.max(0, this.entryOffset - 100))));
      this.node("entries-next").addEventListener("click", () => this.action(() => this.loadEntries(this.entrySource, this.entryOffset + 100)));
      ["report", "journal"].forEach(kind => command(kind, () => this.export(kind)));
      this.commonIds().forEach((id) => {
        const field = get(id);
        field?.addEventListener("input", edited);
        field?.addEventListener("change", edited);
      });
      this.toggle(false);
    }
    node(name) { return get(`${this.mode}-batch-${name}`); }
    listMode() { return this.choice.value === "list"; }
    personal() { return this.mode === "onprem" && get("onprem-destination-personal").checked; }
    usesEmployeeToken() {
      return this.mode === "onprem" && Boolean(this.batch) &&
        this.batch.config?.destination_type !== "personal" && !this.batch.config?.destination_auth;
    }
    serviceAccess() {
      return this.mode === "onprem" && (this.personal() || !this.usesEmployeeToken());
    }
    leaveEmployeePlan() {
      this.revision++;
      this.formRevision++;
      this.batch = this.pendingCreate = this.entrySource = null;
      this.sources = []; this.entryChecks = []; this.plannedCsv = "";
      this.node("details").hidden = this.node("result").hidden = true;
      for (const field of ["token", "admin-token", "client-secret", "owner-token"]) get("onprem-destination-" + field).value = "";
      get("onprem-identity-confirmed").checked = false;
      window.dispatchEvent(new CustomEvent("onprem-batch-config-loaded", {detail: {}}));
      this.renderHistory();
      this.message("Введите доступы сервисного приложения и выполните Dry Run нового списка.");
    }
    enabled() { return Boolean(configuration) && (this.mode === "local" || configuration.onprem_enabled); }
    authorized() { return this.mode !== "onprem" || !configuration?.onprem_auth_required || Boolean(get("onprem-access-key").value.trim()); }
    visible() { return this.listMode() && document.querySelector(`.mode-tab[data-mode="${this.mode}"]`)?.classList.contains("active") && !document.hidden; }
    commonIds() {
      return this.mode === "local" ? ["local_orgid", "local_client_id", "local_client_secret", "local_admin_token", "local_page_limit"]
        : ["onprem-source-org", "onprem-source-client", "onprem-source-secret", "onprem-source-admin-token", "onprem-destination-url", "onprem-destination-identity", "onprem-destination-token", "onprem-destination-vd-hash", "onprem-strategy", "onprem-identity-confirmed", "onprem-access-key",
          "onprem-destination-personal", "onprem-destination-shared", "onprem-destination-client-id", "onprem-destination-client-secret", "onprem-destination-owner-token"];
    }
    config() {
      const value = (id) => get(id).value.trim();
      if (this.mode === "local") return {source_org_id: value("local_orgid"), source_client_id: value("local_client_id"), page_limit: Number(value("local_page_limit")), retry_enabled: window.diskCopyRetries?.value(this.mode) !== false};
      const config = {source_org_id: value("onprem-source-org"), source_client_id: value("onprem-source-client"), page_limit: 1000,
        destination_url: value("onprem-destination-url"), retry_enabled: window.diskCopyRetries?.value(this.mode) !== false, strategy: "staged"};
      if (this.serviceAccess()) {
        config.destination_auth = "service_application";
        config.destination_client_id = value("onprem-destination-client-id");
      }
      if (this.personal()) return {...config, destination_type: "personal"};
      return {...config, destination_type: "shared", destination_identity: value("onprem-destination-identity"),
        destination_vd_hash: value("onprem-destination-vd-hash"), identity_confirmed: !this.serviceAccess() && get("onprem-identity-confirmed").checked};
    }
    secrets() {
      const value = {source_client_secret: get(this.mode === "local" ? "local_client_secret" : "onprem-source-secret").value};
      if (this.serviceAccess()) {
        value.destination_client_secret = get("onprem-destination-client-secret").value;
        value.destination_owner_token = get("onprem-destination-owner-token").value;
      } else if (this.mode === "onprem") value.destination_token = get("onprem-destination-token").value;
      const admin = get(this.mode === "local" ? "local_admin_token" : "onprem-source-admin-token").value;
      if (admin.trim()) value.source_admin_token = admin;
      return value;
    }
    valid() {
      const config = this.config(), secret = this.secrets();
      if (!config.source_org_id || !config.source_client_id || !secret.source_client_secret.trim() || !Number.isInteger(config.page_limit) || config.page_limit < 1 || config.page_limit > 1000) return false;
      if (this.mode === "onprem") {
        try { if (new URL(config.destination_url).protocol !== "https:") return false; } catch (_) { return false; }
        if (!this.personal() && (!config.destination_identity || !/^[A-Za-z0-9_-]{1,256}$/.test(config.destination_vd_hash))) return false;
        if (this.serviceAccess()) {
          const available = this.personal() ? configuration?.onprem_personal_service_application_available :
            configuration?.onprem_shared_service_application_available && /^[^\s@]+@[^\s@]+$/.test(config.destination_identity);
          if (!available || !config.destination_client_id || !secret.destination_client_secret.trim() || !secret.destination_owner_token.trim()) return false;
        } else if (!config.identity_confirmed || !secret.destination_token.trim()) return false;
      }
      return this.enabled() && this.authorized();
    }
    matches() {
      if (!this.batch?.config) return false;
      const current = this.config(), original = this.batch.config;
      if (this.mode === "onprem" && (current.destination_auth || "") !== (original.destination_auth || "")) return false;
      return Object.keys(current).every((key) => {
        if (["identity_confirmed", "retry_enabled"].includes(key)) return true;
        if (key === "destination_type") return current[key] === (original[key] || "shared");
        if (key === "destination_url") {
          return normalizedApiUrl(current[key]) === normalizedApiUrl(original[key]);
        }
        return String(current[key]) === String(original[key]);
      });
    }
    legacy() {
      return this.mode === "onprem" && Boolean(this.batch) && (this.batch.config?.strategy !== "staged" || this.sources.some((source) => source.child_strategy === "url"));
    }
    requireStaged() { if (this.legacy()) throw failure("onprem_staged_required"); }
    updateTemplate() {
      const kind = this.personal() ? "personal" : "shared";
      if (this.csvKind === kind) return;
      const csv = this.node("csv");
      if (this.csvKind) {
        this.csvDrafts[this.csvKind] = csv.value;
        csv.value = this.csvDrafts[kind] || "";
        this.node("file").value = "";
      }
      this.csvKind = kind;
      const personal = this.personal();
      this.node("template").setAttribute("href", personal ? "/static/personal-disk-pairs.csv?v=c6ebe6b1cf63" : "/static/source-list-template.csv");
      csv.placeholder = personal ? personalCsvHeader + "\naivanov@cloud.example,/Документы,apetrov@office.example,/Архив/2026\nolga@cloud.example,,osokolova@office.example,"
        : "type,email,vd_hash,path\npersonal,user@company.ru,,/\nshared,reader@company.ru,disk-hash,/";
      this.node("personal-note").hidden = !personal;
      this.node("shared-note").hidden = personal;
    }
    toggle(refresh = true) {
      this.formRevision++;
      this.revision++;
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
      const personalErrors = {
        invalid_sources_columns: "В заголовке CSV нужны source_email и destination_email. Допустимые дополнительные колонки — path и destination_folder.",
        invalid_sources_row: "Проверьте email, число значений в строке и лимит в 1000 пар.",
        invalid_credentials: "Введите Client secret обоих приложений и токен владельца организации On-premises.",
        invalid_batch_destination: "Проверьте Client ID сервисного приложения On-premises и адрес Disk API.",
        destination_auth_failed: "Проверьте сервисное приложение On-premises и токен владельца организации.",
        destination_read_only: "Проверьте право записи сервисного приложения на личный Диск получателя.",
      };
      const prefix = (this.personal() && personalErrors[code]) || errorNames[code] || (/^[a-z][a-z0-9_]{0,100}$/.test(code) ? `Ошибка: ${code}.` : "Действие не выполнено. Проверьте журнал.");
      const details = this.diagnostic(error.diagnostic_message || error.diagnostic || error.message);
      const row = Number.isSafeInteger(error.csv_row) ? `Строка ${error.csv_row}. ` : "";
      return `${row}${prefix}${Number.isInteger(status) ? ` HTTP ${status}.` : ""}${details && details !== code ? ` ${details}` : ""}`;
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
        const error = failure(typeof detail === "string" ? detail : detail?.code || "invalid_request", response.status, body.diagnostic_message || detail?.diagnostic_message);
        error.csv_row = detail?.csv_row;
        throw error;
      }
      try { return options.blob ? await response.blob() : await response.json(); }
      catch (_) { throw failure("invalid_response"); }
    }
    path(suffix = "") { return "/" + encodeURIComponent(this.batch.id) + (suffix ? "/" + suffix : ""); }
    accept(detail) {
      if (!detail?.batch?.id || !Array.isArray(detail.sources) || detail.batch.mode !== this.mode) throw failure("invalid_response");
      if (detail.batch.id !== this.batch?.id) this.errorsOnly = false;
      this.batch = detail.batch; this.sources = detail.sources; this.render();
    }
    async action(work) {
      if (this.busy || !this.enabled() || !this.authorized()) return;
      const focused = document.activeElement?.id;
      this.busy = true; this.revision += 1; clearTimeout(this.timer); this.controls(); this.message("");
      try { await work(); } catch (error) { this.message(this.errorText(error), true); }
      finally {
        this.busy = false; this.controls(); this.schedule();
        if (focused?.startsWith("onprem-pair-") && document.activeElement === document.body) get(focused)?.focus({preventScroll: true});
      }
    }
    controls() {
      if (this.listMode() && this.mode === "onprem") this.updateTemplate();
      const formMatches = this.node("csv").value === this.plannedCsv && this.matches();
      const savedPairs = this.batch?.config?.destination_type === "personal";
      const delegated = this.batch?.config?.destination_auth === "service_application";
      const stale = Boolean(this.batch && (this.serviceAccess() || savedPairs || delegated) && !formMatches);
      const batch = stale ? null : this.batch, legacy = this.legacy();
      if (this.listMode()) {
        const id = batch?.id;
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
      const valid = this.valid();
      const ready = selected.length > 0 && selected.every((source) => source.plan_complete &&
        (!savedPairs || (source.planned_destination_path && source.destination_verified_uid)) &&
        (!delegated || savedPairs || (/^[0-9]{1,20}$/.test(batch?.shared_operator_uid || "") && source.destination_verified_uid === batch.shared_operator_uid)) &&
        (!source.mapping_error || source.credential_check === true) && ["ready", "completed"].includes(source.state));
      const canPlan = this.usesEmployeeToken() ? configuration?.onprem_shared_service_application_available : valid;
      this.node("plan").disabled = locked || (!legacy && activeStates.has(batch?.state)) || !canPlan || !this.node("csv").value.trim();
      this.node("start").disabled = locked || legacy || !batch || batch.state !== "ready" || !ready || !formMatches || !valid || Number(batch?.selection?.selected) === 0;
      this.node("pause").disabled = locked || legacy || !batch || !activeStates.has(batch.state);
      this.node("resume").disabled = locked || legacy || !batch || !["paused", "awaiting_credentials"].includes(batch.state) || !formMatches || !valid;
      this.node("stop").disabled = locked || !batch || ["stopped", "completed"].includes(batch.state);
      this.node("restart").disabled = locked || !batch || !terminalStates.has(batch.state) || !canPlan || !this.node("csv").value.trim();
      if (this.mode === "onprem") this.planPresentation(selected, stale, ready, valid);
      ["report", "journal"].forEach((name) => { this.node(name).disabled = locked || !batch; });
      this.node("refresh").disabled = locked;
      this.node("history").disabled = locked;
      this.node("history").value = batch?.id || "";
      this.node("history-previous").disabled = locked || this.offset === 0;
      this.node("history-next").disabled = locked || !this.hasMore;
      this.entryChecks.forEach(({check, entry, sourceId}) => { check.disabled = !this.entrySelectionAvailable(sourceId, entry); });
      this.sources.forEach((source) => {
        if (source._checkbox) source._checkbox.disabled = locked || legacy || !batch || batch.started || !formMatches || activeStates.has(batch.state) || terminalStates.has(batch.state);
        if (source._plan) source._plan.disabled = locked || legacy || !formMatches || !valid || !batch || batch.started || activeStates.has(batch.state) || terminalStates.has(batch.state);
        if (source._details) source._details.disabled = locked || stale;
      });
    }
    planPresentation(selected, stale, ready, valid) {
      const batch = stale ? null : this.batch;
      const personal = this.personal();
      const savedPairs = this.batch?.config?.destination_type === "personal";
      const showPairs = personal && savedPairs && !stale && this.listMode();
      const state = batch?.state;
      const review = showPairs && state === "ready" && ready;
      const planState = this.node("plan-state");
      const verified = review && selected.every(source => !source.mapping_error);
      if (planState.classList.contains("is-ready") !== Boolean(verified)) {
        this.node("archive").open = !verified;
      }
      planState.classList.toggle("is-ready", Boolean(verified));
      get("onprem-source-batches").classList.toggle("personal-plan-active", showPairs);
      this.node("result").hidden = !batch;
      this.node("plan-title").hidden = !showPairs;
      this.node("pairs-head").hidden = !savedPairs;
      this.node("sources-head").hidden = savedPairs;
      this.node("publish-warning").hidden = !showPairs || !selected.length || !["ready", "needs_attention"].includes(state);
      if (showPairs) {
        this.node("plan-count").textContent = `Личные Диски · выбрано строк: ${selected.length} из ${this.sources.length}`;
        const errorCount = this.sources.filter(rowHasError).length;
        const filter = this.node("filter-errors");
        filter.textContent = `С ошибками: ${errorCount}`;
        filter.setAttribute("aria-pressed", String(this.errorsOnly));
        filter.disabled = this.busy || (!this.errorsOnly && errorCount === 0);
        this.node("filter-all").hidden = !this.errorsOnly;
        this.node("filter-all").disabled = this.busy;
        const needsCheck = state === "ready" && (!ready || selected.some(source => source.mapping_error));
        planState.textContent = needsCheck ? "Требуется проверка" : this.node("state").textContent;
      }
      const plan = this.node("plan"), start = this.node("start");
      plan.textContent = personal ? (review ? "Проверить ещё раз" : "Проверить план") : "Dry Run";
      plan.className = review ? "secondary-btn" : "run-btn";
      start.textContent = review ? `Начать перенос (${selected.length})` : "Начать перенос";
      start.hidden = stale || (personal && !review);
      plan.hidden = showPairs && (activeStates.has(state) || terminalStates.has(state) || state === "paused" || state === "awaiting_credentials");
      this.node("pause").hidden = stale || (personal && !activeStates.has(state));
      this.node("resume").hidden = stale || (personal && !["paused", "awaiting_credentials"].includes(state));
      this.node("restart").hidden = stale || (personal && !terminalStates.has(state));
      this.node("stop").hidden = stale || (personal && !["planning", "running", "paused", "awaiting_credentials", "needs_attention", "completed_with_issues"].includes(state));
      const hint = this.node("start-hint");
      let reason = "";
      if (stale) reason = "CSV или параметры изменились. Проверьте новый план.";
      else if (personal) {
        if (!this.node("csv").value.trim()) reason = "Загрузите CSV или вставьте список пар.";
        else if (review && Number(batch.selection?.selected) === 0) reason = "Для переноса выберите хотя бы один файл или папку.";
        else if (!valid) reason = "Для продолжения заполните поля доступа" + (showPairs ? " в разделе «Параметры и доступы»." : ".");
      }
      hint.textContent = reason;
      hint.hidden = !reason;
    }
    async loadFile() {
      const file = this.node("file").files?.[0];
      if (!file) return;
      const revision = ++this.formRevision;
      if (file.size > 2 * 1024 * 1024) { this.message("CSV слишком большой. Разделите список на несколько групп до 2 Мб.", true); return; }
      try {
        const csv = await file.text();
        if (revision !== this.formRevision) return;
        this.node("csv").value = csv;
        if (this.usesEmployeeToken() && csv !== this.plannedCsv) this.leaveEmployeePlan();
        this.controls(); this.message(`Список загружен. Заполните общие доступы и нажмите «${this.personal() ? "Проверить план" : "Dry Run"}».`);
      }
      catch (_) { this.message("Не удалось прочитать CSV. Выберите файл повторно.", true); }
    }
    fill(config, previous) {
      const map = this.mode === "local" ? {source_org_id: "local_orgid", source_client_id: "local_client_id", page_limit: "local_page_limit"}
        : {source_org_id: "onprem-source-org", source_client_id: "onprem-source-client", destination_url: "onprem-destination-url", destination_identity: "onprem-destination-identity", destination_vd_hash: "onprem-destination-vd-hash", destination_client_id: "onprem-destination-client-id", strategy: "onprem-strategy"};
      Object.entries(map).forEach(([key, id]) => {
        if (key === "strategy") get(id).value = "staged";
        else if (config[key] !== undefined) get(id).value = String(config[key]);
      });
      if (this.mode === "onprem") {
        get("onprem-destination-personal").checked = config.destination_type === "personal";
        get("onprem-destination-shared").checked = !this.personal();
        this.updateTemplate();
        const current = this.config();
        if (current.source_org_id !== previous.source_org_id || current.source_client_id !== previous.source_client_id) {
          get("onprem-source-secret").value = "";
          get("onprem-source-admin-token").value = "";
        }
        if (current.destination_auth !== previous.destination_auth || current.destination_type !== previous.destination_type || current.destination_client_id !== previous.destination_client_id ||
            normalizedApiUrl(current.destination_url) !== normalizedApiUrl(previous.destination_url) || current.destination_identity !== previous.destination_identity) {
          for (const field of ["token", "admin-token", "client-secret", "owner-token"]) get("onprem-destination-" + field).value = "";
        }
        get("onprem-identity-confirmed").checked = false;
        window.dispatchEvent(new CustomEvent("onprem-batch-config-loaded", {detail: {}}));
      }
    }
    async createPlan() {
      if (this.usesEmployeeToken()) {
        this.leaveEmployeePlan();
        return;
      }
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
      if (!this.listMode() || !this.valid() || !this.matches() || this.node("csv").value !== this.plannedCsv) throw failure("credential_binding_mismatch");
      const revision = this.formRevision;
      const detail = await this.request(this.path("credentials"), {method: "POST", body: {credentials: this.secrets()}});
      if (revision !== this.formRevision || !this.listMode()) throw failure("credential_binding_mismatch");
      this.accept(detail);
    }
    async updateCurrent(suffix, options) {
      const id = this.batch.id;
      const detail = await this.request(this.path(suffix), options);
      if (id !== this.batch?.id) return false;
      if (detail?.batch?.id !== id) throw failure("invalid_response");
      this.accept(detail);
      return true;
    }
    async mutate(action, access = false) {
      if (!this.batch) return;
      if (action !== "stop") this.requireStaged();
      if (access) await this.attach();
      if (action === "resume" && this.batch.state === "ready") {
        this.message("Доступ восстановлен. План готов; нажмите кнопку начала переноса."); return;
      }
      const endpoint = action === "resume" && this.batch.state === "draft" ? "plan" : action;
      if (!await this.updateCurrent(endpoint, {method: "POST", body: {}})) return;
      this.message({start: "Перенос выбранных источников запущен.", pause: "Пауза запрошена. Дождитесь состояния «Приостановлено».", resume: "Работа списка продолжена.", stop: "Остановка запрошена. Локальные операции и очередь списка будут освобождены."}[action]);
    }
    async select(source, checked) {
      const ids = this.sources.filter((row) => row.id === source.id ? checked : row.selected).map((row) => row.id);
      try { this.requireStaged(); await this.updateCurrent("selection", {method: "PUT", body: {source_ids: ids}}); }
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
      if (!await this.updateCurrent(`sources/${encodeURIComponent(source.id)}/plan`, {method: "POST", body: {}})) return;
      this.message("Повторный Dry Run выбранного источника запущен. Планы остальных источников сохраняются.");
    }
    async open(id) {
      const revision = ++this.formRevision;
      const detail = await this.request("/" + encodeURIComponent(id));
      if (revision !== this.formRevision || !this.listMode()) { this.renderHistory(); return; }
      const previous = this.config();
      this.accept(detail);
      this.fill(this.batch.config || {}, previous); this.node("csv").value = sourceCsv(this.sources, this.personal()); this.plannedCsv = this.node("csv").value;
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
      lists.forEach((batch) => {
        const option = document.createElement("option"); option.value = batch.id;
        const target = this.mode === "onprem" ? (batch.config?.destination_type === "personal" ? "Личный Диск · " : "Общий диск · ") : "";
        option.textContent = `${date(batch.created_at)} · ${target}${humanState(batch.state)} · источников: ${batch.summary?.selected_sources ?? "—"}`;
        select.appendChild(option);
      });
      select.value = this.batch?.id || ""; this.controls();
    }
    render() {
      const batch = this.batch, summary = batch.summary || {};
      const selection = this.batch.selection;
      this.node("entries-selection").textContent = selection
        ? `Выбрано: ${selection.selected} · исключено: ${selection.excluded} · объём: ${bytes(selection.selected_bytes)} (в списке)` : "";
      this.node("result").hidden = false;
      this.node("date").textContent = date(batch.created_at);
      if (this.mode === "onprem") {
        const operator = this.node("operator");
        operator.hidden = batch.config?.destination_type !== "shared" || batch.config?.destination_auth !== "service_application";
        operator.textContent = operator.hidden ? "" : `Общий Диск [${batch.config.destination_vd_hash}] · Сотрудник: ${batch.config.destination_identity} · UID: ${batch.shared_operator_uid || "ещё не подтверждён"}`;
      }
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
      const body = this.node("sources");
      const focused = document.activeElement?.closest("tbody") === body ? document.activeElement.id : null;
      const openDetails = new Set(Array.from(body.querySelectorAll("details")).filter(item => item.open).map(item => item.dataset.sourceId));
      body.replaceChildren();
      const personal = this.mode === "onprem" && batch.config?.destination_type === "personal";
      const filtered = personal && this.errorsOnly;
      const rows = filtered ? this.sources.filter(rowHasError) : this.sources;
      const details = this.node("details");
      const focusedInDetails = document.activeElement?.closest("[data-batch-details]") === details;
      if (filtered && this.entrySource && !rows.some(row => row.id === this.entrySource)) {
        details.hidden = true;
        this.entrySource = null; this.entryEntries = []; this.entryChecks = [];
      }
      const labels = personal ? ["№", "Источник SaaS", "Получатель On-premises", "Подробности"] :
        ["Вкл.", this.mode === "onprem" ? "Источник → назначение" : "Источник → папка", "Состояние", "Файлы / папки", "Всего / к переносу", "Крупнейший файл", "Скачивание / загрузка", "Действия и ошибка"];
      rows.forEach((source) => {
        const row = document.createElement("tr");
        const cell = (text) => { const node = document.createElement("td"); node.setAttribute("data-label", labels[row.children.length]); if (text !== undefined) node.textContent = text; row.appendChild(node); return node; };
        const check = document.createElement("input"); check.type = "checkbox"; check.checked = Boolean(source.selected);
        check.setAttribute("aria-label", personal ? `Включить строку ${source.csv_row}: ${source.email} → ${source.destination_email}`
          : `Включить ${source.email}${source.vd_hash ? " · " + source.vd_hash : ""}`);
        check.addEventListener("change", () => this.action(() => this.select(source, check.checked)));
        const first = cell(); first.appendChild(check); source._checkbox = check;
        const line = (parent, className, text) => {
          const span = document.createElement("span"); span.className = className; span.textContent = text; parent.appendChild(span);
        };
        const state = source.mapping_error ? "Требует проверки" : humanState(source.state) + (!source.plan_complete && source.state === "ready" ? " · план неполный" : "");
        const totals = source.summary || {}, rates = source.transfer_rates || {};
        const counts = `${totals.total_files ?? 0} / ${totals.total_directories ?? 0}`;
        const volume = `${bytes(totals.total_bytes)} / ${bytes(totals.required_bytes)}`;
        const speed = `${bytes(rates.download_bytes_per_second)}/с / ${this.mode === "onprem" ? bytes(rates.upload_bytes_per_second) + "/с" : "—"}`;
        if (personal) {
          check.id = `onprem-pair-${source.id}-select`;
          line(first, "pair-number", source.csv_row);
          const from = cell(), to = cell();
          line(from, "pair-email", source.email);
          line(from, "pair-path", diskPath(source.path));
          line(to, "pair-email", source.destination_email);
          line(to, "pair-path", source.planned_destination_path ? diskPath(source.planned_destination_path) : "Итоговый путь — после проверки плана");
          line(to, "pair-uid", `UID: ${source.destination_verified_uid || "не подтверждён"}`);
        } else {
          cell(`${source.type === "shared" ? "Общий диск" : "Личный Диск"} · ${source.email}${source.vd_hash ? " [" + source.vd_hash + "]" : ""} · ${diskPath(source.path)}\n→ ${source.folder || "папка определится при Dry Run"}`);
          cell(state); cell(counts); cell(volume); cell(bytes(totals.largest_file)); cell(speed);
        }
        const actions = cell();
        let tools = actions;
        if (personal) {
          actions.className = "pair-actions-cell";
          tools = document.createElement("details"); tools.dataset.sourceId = source.id;
          tools.open = openDetails.has(source.id);
          const summary = document.createElement("summary"); summary.textContent = "Файлы и статус"; summary.id = `onprem-pair-${source.id}-summary`; tools.appendChild(summary);
          const stats = document.createElement("p"); stats.className = "field-note";
          stats.textContent = `${state} · Файлы / папки: ${counts} · Всего / к переносу: ${volume} · Крупнейший файл: ${bytes(totals.largest_file)} · Скачивание / загрузка: ${speed}`;
          tools.appendChild(stats); actions.appendChild(tools);
        }
        const button = (key, text, handler, parent = tools) => {
          const node = document.createElement("button"); node.type = "button"; node.className = "secondary-btn";
          if (personal) node.id = `onprem-pair-${source.id}-${key}`;
          node.textContent = text; node.addEventListener("click", handler); parent.appendChild(node); return node;
        };
        source._plan = button("plan", personal ? "Проверить строку ещё раз" : "Повторить Dry Run", () => this.action(() => this.replan(source)), filtered ? actions : tools);
        source._details = button("files", "Файлы", () => this.action(() => this.loadEntries(source.id, 0)));
        if (source.child_job_id) button("journal", "Подробный журнал", () => window.dispatchEvent(new CustomEvent("open-onprem-job", {detail: {jobId: source.child_job_id}})));
        else if (source.journal_id) button("journal", "Подробный журнал", () => window.dispatchEvent(new CustomEvent("open-local-journal", {detail: {jobId: source.journal_id}})));
        if (source.error) { const error = document.createElement("p"); error.className = "field-note onprem-result bad"; error.textContent = this.errorText(source.error); actions.appendChild(error); }
        if (source.mapping_error) {
          const error = document.createElement("p"); error.className = "field-note onprem-result bad";
          const rows = source.conflict_rows?.length ? ` Строки CSV: ${source.conflict_rows.join(", ")}.` : "";
          error.textContent = this.errorText(source.mapping_error) + rows + (source.conflict_path ? ` Путь: ${diskPath(source.conflict_path)}` : "");
          actions.appendChild(error);
        }
        if (filtered && !source.error && !source.mapping_error) {
          const note = document.createElement("p"); note.className = "field-note onprem-result bad";
          note.textContent = "Откройте «Файлы», чтобы посмотреть результат по каждому объекту."; actions.appendChild(note);
        }
        body.appendChild(row);
      });
      if (filtered && !rows.length) {
        const row = document.createElement("tr"), cell = document.createElement("td");
        cell.colSpan = labels.length; cell.textContent = "Строк с ошибками нет.";
        row.appendChild(cell); body.appendChild(row);
      }
      this.controls();
      if (!this.node("result").hidden) {
        const target = focused ? get(focused) : null;
        if (target) target.focus({preventScroll: true});
        else if (filtered && (focused || (focusedInDetails && details.hidden))) this.node("filter-errors").focus?.({preventScroll: true});
      }
    }
    async loadEntries(sourceId, offset) {
      if (!sourceId || !this.batch) return;
      const revision = this.revision, batchId = this.batch.id;
      const result = await this.request(this.path(`sources/${encodeURIComponent(sourceId)}/entries?limit=100&offset=${offset}`));
      if (revision !== this.revision || this.batch?.id !== batchId) return;
      const source = this.sources.find((item) => item.id === sourceId);
      if (!source || (this.errorsOnly && !rowHasError(source))) return;
      if (!Array.isArray(result?.entries)) throw failure("invalid_response");
      this.entrySource = sourceId; this.entryOffset = offset; this.entryEntries = result.entries;
      this.node("details").hidden = false;
      this.node("details-name").textContent = source?.destination_email ? `Строка ${source.csv_row} · ${source.email} → ${source.destination_email}`
        : `${source?.email || ""}${source?.vd_hash ? " [" + source.vd_hash + "]" : ""}`;
      const body = this.node("entries");
      const focused = document.activeElement?.closest("tbody") === body ? document.activeElement.id : null;
      body.replaceChildren(); this.entryChecks = [];
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
            if (source?.destination_email && entry.id != null) check.id = `onprem-pair-${sourceId}-entry-${entry.id}`;
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
      if (focused && !this.node("result").hidden) get(focused)?.focus({preventScroll: true});
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
  window.addEventListener("pageshow", (event) => {
    if (event.persisted) Object.values(controllers).forEach((list) => list.schedule());
  });
  window.sourceBatches = {open: (mode, id) => controllers[mode]?.action(() => controllers[mode].open(id)), refresh: (mode) => controllers[mode]?.action(() => controllers[mode].refresh()), usesEmployeeToken: () => controllers.onprem.usesEmployeeToken()};
  Promise.resolve(window.diskCopyConfigPromise).then((config) => {
    configuration = config;
    Object.values(controllers).forEach((list) => { list.controls(); list.schedule(); });
  }).catch(() => {});
})();
