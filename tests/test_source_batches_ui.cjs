const assert = require("node:assert/strict");
const {test} = require("node:test");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const crypto = require("node:crypto").webcrypto;
const {loadUi} = require("./helpers/legacy_ui.cjs");
const code = fs.readFileSync(path.resolve(__dirname, "../app/static/source-batches.js"), "utf8");
const flush = async () => { for (let count = 0; count < 5; count += 1) await new Promise((resolve) => setImmediate(resolve)); };
const clone = (value) => JSON.parse(JSON.stringify(value));
async function setup({mode = "local", enabled = true, sharedAvailable = true, state = "ready", complete = true, credentials = true, sourceError = null, started = false, selectionError = null, failFirstPlan = false, strategy = "staged", childStrategy = null, employeeToken = strategy === "url" || childStrategy === "url", createdBatchId = "list-id", failCreates = false, createError = null, retryEnabled = true, retryError = null, createdRetryEnabled = null, createGate = null, waitForOpen, waitForEntries} = {}) {
  const ui = await loadUi({cross_org: true, onprem_enabled: enabled});
  const values = {local_client_id: "client", local_orgid: "org", local_client_secret: "secret-value", local_admin_token: "admin-token-value", local_page_limit: "1000",
    "onprem-source-org": "org", "onprem-source-client": "client", "onprem-source-secret": "secret-value", "onprem-source-admin-token": "admin-token-value",
    "onprem-destination-url": "https://cloud-api.example.test", "onprem-destination-client-id": "office-app", "onprem-destination-identity": "operator@example.test",
    "onprem-destination-token": "destination-token-value", "onprem-destination-vd-hash": "target", "onprem-strategy": "staged"};
  for (const [id, value] of Object.entries(values)) ui.byId.get(id).value = value;
  ui.byId.get("onprem-identity-confirmed").checked = true;
  const config = {source_org_id: "org", source_client_id: "client", page_limit: 1000, retry_enabled: retryEnabled,
    ...(mode === "onprem" ? {destination_url: "https://cloud-api.example.test/v1/disk", destination_identity: "operator@example.test", destination_vd_hash: "target", strategy,
      ...(employeeToken ? {identity_confirmed: true} : {identity_confirmed: false, destination_type: "shared", destination_auth: "service_application", destination_client_id: "office-app"})} : {})};
  const detail = {batch: {id: "list-id", mode, state, phase: "inventory", created_at: "2026-10-14T10:00:00Z", config,
    credentials_available: credentials, started, summary: {selected_sources: 2, ready_sources: 2, failed_sources: 0, total_files: 2, total_bytes: 2048, required_bytes: 1024, verified_bytes: 1024}},
    sources: [{id: "source-a", type: "personal", email: "user@example.test", path: "disk:/", selected: true, plan_complete: complete, state: sourceError ? "error" : "ready", error: sourceError,
      folder: "user@example.test", summary: {total_files: 1, total_directories: 0, total_bytes: 1024, required_bytes: 1024, largest_file: 1024}},
      {id: "source-b", type: "shared", email: "reader@example.test", vd_hash: "source-vd", path: "disk:/", selected: true, plan_complete: true, state: "ready", folder: "<script>disk</script> (source-vd)", summary: {total_files: 1, total_bytes: 1024, required_bytes: 0, largest_file: 1024}}]};
  if (mode === "onprem" && !employeeToken) {
    detail.batch.shared_operator_uid = "1000123";
    detail.sources.forEach(row => {row.destination_verified_uid = detail.batch.shared_operator_uid;});
  }
  if (childStrategy) Object.assign(detail.sources[0], {child_job_id: "a".repeat(32), child_strategy: childStrategy});
  const requests = [], events = [], timers = [];
  const listeners = {};
  const window = {diskCopyRetries: ui.window.diskCopyRetries, confirm: () => true, diskCopyConfigPromise: Promise.resolve({onprem_enabled: enabled, onprem_auth_required: false, onprem_personal_service_application_available: true, onprem_shared_service_application_available: sharedAvailable}),
    addEventListener(name, fn) {(listeners[name] ||= []).push(fn);},
    dispatchEvent(event) {events.push(event); (listeners[event.type] || []).forEach((fn) => fn(event));}};
  const fetch = async (address, options = {}) => {
    const url = new URL(address, "http://localhost");
    const method = options.method || "GET", body = options.body ? JSON.parse(options.body) : null;
    requests.push({url, method, body, options});
    if (waitForOpen && method === "GET" && url.pathname === "/api/source-batches/list-id") await waitForOpen();
    if (waitForEntries && url.pathname.endsWith("/entries")) await waitForEntries();
    if (createGate && method === "POST" && url.pathname === "/api/source-batches") await createGate;
    if (failCreates && method === "POST" && url.pathname === "/api/source-batches") throw new Error("simulated create response interruption");
    if (method === "POST" && url.pathname === "/api/source-batches") {
      if (createError) return {ok: false, status: 422, json: async () => ({detail: createError})};
      detail.batch.id = createdBatchId; detail.batch.state = "draft"; detail.batch.credentials_available = true;
      detail.batch.config = clone(body.config); detail.batch.started = false;
      if (createdRetryEnabled !== null) detail.batch.config.retry_enabled = createdRetryEnabled;
      detail.sources.forEach((source) => {delete source.child_job_id; delete source.child_strategy;});
    }
    if (url.pathname.endsWith("/plan")) {
      if (failFirstPlan) {
        failFirstPlan = false;
        return {ok: false, status: 409, json: async () => ({detail: "batch_busy"})};
      }
      detail.batch.state = "planning";
    }
    if (url.pathname.endsWith("/start")) detail.batch.state = "running";
    if (url.pathname.endsWith("/resume")) detail.batch.state = detail.batch.resume_state || "running";
    if (url.pathname.endsWith("/pause")) detail.batch.state = "paused";
    if (url.pathname.endsWith("/stop")) detail.batch.state = "stopped";
    if (url.pathname.endsWith("/credentials")) detail.batch.credentials_available = true;
    if (url.pathname.endsWith("/selection")) {
      if (selectionError) return {ok: false, status: 409, json: async () => ({detail: selectionError})};
      detail.sources.forEach((source) => {source.selected = body.source_ids.includes(source.id);});
      detail.batch.state = detail.sources.filter((source) => source.selected).every((source) => source.state === "ready") ? "ready" : "draft";
    }
    if (url.pathname.endsWith("/retries")) {
      if (retryError) return {ok: false, status: 403, json: async () => ({detail: retryError, private_detail: "secret-value"})};
      detail.batch.config.retry_enabled = body.enabled;
      return {ok: true, status: 200, json: async () => ({retry_enabled: body.enabled})};
    }
    let response = detail;
    if (url.pathname.endsWith("/entries")) response = {total: 1, entries: [{id: "file-1", source_path: "disk:/<unsafe>.txt", destination_path: "/<unsafe>.txt", size: 1024, state: "planned"}]};
    else if (url.pathname === "/api/source-batches" && method === "GET") response = {batches: [detail.batch], has_more: false};
    return {ok: true, status: 200, json: async () => clone(response), blob: async () => new Blob(["report"])};
  };
  // Any unsafe HTML renderer makes the test fail, including source/destination/API values.
  for (const node of ui.byId.values()) Object.defineProperty(node, "innerHTML", {set() {throw new Error("unsafe HTML");}});
  vm.runInNewContext(code, {window, document: ui.document, fetch, crypto, Uint8Array, URL, Blob, console,
    CustomEvent: class {constructor(type, options = {}) {this.type = type; this.detail = options.detail;}},
    setTimeout(fn) {timers.push(fn); return timers.length;}, clearTimeout() {}});
  await flush();
  const tab = ui.document.querySelector(`.mode-tab[data-mode="${mode}"]`); tab.classList.add("active");
  const node = (name) => ui.byId.get(`${mode}-batch-${name}`);
  ui.byId.get(`${mode}-source-input`).value = "list";
  await ui.byId.get(`${mode}-source-input`).fire("change");
  await window.sourceBatches.open(mode, "list-id");
  if (mode === "onprem" && employeeToken) {
    ui.byId.get("onprem-destination-token").value = "destination-token-value";
    ui.byId.get("onprem-identity-confirmed").checked = true;
    await ui.byId.get("onprem-identity-confirmed").fire("change");
  } else if (mode === "onprem") {
    ui.byId.get("onprem-destination-client-secret").value = "app-secret";
    ui.byId.get("onprem-destination-owner-token").value = "owner";
    await ui.byId.get("onprem-destination-owner-token").fire("input");
  }
  return {ui, node, window, requests, events, timers, detail};
}

test("local source-list preserves organization access and hides only standalone source fields", async () => {
  const {ui, node, requests} = await setup();
  assert.equal(ui.byId.get("local-single-source").hidden, true);
  assert.equal(ui.byId.get("local-single-admin").hidden, false, "organization admin token remains available for list mode");
  assert.equal(ui.byId.get("local_orgid").closest("[data-source-single]")?.hidden, undefined, "organization ID remains available for all local sources");
  assert.equal(ui.byId.get("local-source-batches").hidden, false);
  assert.equal(node("start").disabled, false);
  assert.equal(node("plan").disabled, false);
  assert.equal(requests.filter((request) => request.method !== "GET").length, 0, "opening list never starts a transfer");
  node("csv").value += "\npersonal,new@example.test,,/";
  await node("csv").fire("input");
  assert.equal(node("start").disabled, true, "editing source membership requires a new Dry Run");
  assert.equal(node("plan").disabled, false);
});

test("local organization ID is labeled plainly beside its API parameter name", () => {
  const html = fs.readFileSync(path.resolve(__dirname, "../app/static/index.html"), "utf8");
  const label = html.match(/<label for="local_orgid">([^<]*)<\/label>/)?.[1];
  assert.match(label, /ID организации[^<]*ORGID/);
});

test("incomplete inventory blocks Start even if server incorrectly says ready", async () => {
  const {node} = await setup({complete: false});
  assert.equal(node("start").disabled, true);
});

test("source error can be excluded; ready selected plans then unlock Start", async () => {
  const {node, requests} = await setup({state: "draft", sourceError: {code: "source_auth_failed"}});
  assert.equal(node("start").disabled, true);
  const checkbox = node("sources").children[0].children[0].children[0];
  checkbox.checked = false; await checkbox.fire("change");
  assert.deepEqual(requests.at(-1).body, {source_ids: ["source-b"]});
  assert.equal(node("start").disabled, false);
  await node("start").fire("click");
  assert.equal(requests.at(-1).url.pathname, "/api/source-batches/list-id/start");
  assert.equal(requests.at(-2).url.pathname, "/api/source-batches/list-id/credentials", "credentials attach automatically");
});

test("history and files render untrusted source names only as text", async () => {
  const {node} = await setup();
  assert.match(node("sources").children[1].children[1].textContent, /<script>disk<\/script>/);
  await node("sources").children[1].children[7].children[1].fire("click");
  assert.equal(node("details").hidden, false);
  assert.equal(node("entries").children[0].children[0].textContent, "disk:/<unsafe>.txt");
});

test("Onprem list removes hidden per-source required fields and restores them in single mode", async () => {
  const {ui, node} = await setup({mode: "onprem"});
  for (const id of ["onprem-source-email", "onprem-source-path"]) {
    assert.equal(ui.byId.get(id).required, false);
    assert.equal(ui.byId.get(id).disabled, true);
  }
  assert.equal(node("start").disabled, false, "normalized Onprem API root retains binding");
  ui.byId.get("onprem-source-input").value = "single"; await ui.byId.get("onprem-source-input").fire("change");
  assert.equal(ui.byId.get("onprem-source-email").required, true);
  assert.equal(ui.byId.get("onprem-source-email").disabled, false);
  assert.equal(ui.byId.get("onprem-single-run").hidden, false);
});

for (const edit of ["single", "url", "csv"]) test(`late list history respects new ${edit} input`, async () => {
  let gate, release;
  const {ui, node, window} = await setup({mode: "onprem", waitForOpen: () => gate});
  gate = new Promise(resolve => {release = resolve;});
  const pending = window.sourceBatches.open("onprem", "list-id");
  const url = ui.byId.get("onprem-destination-url");
  url.value = "https://cloud-api.draft.example";
  node("csv").value = "personal,new@example.test,,/";
  if (edit === "single") {
    const choice = ui.byId.get("onprem-source-input");
    choice.value = "single"; await choice.fire("change");
  } else await (edit === "url" ? url : node("csv")).fire("input");
  release(); await pending;
  assert.equal(url.value, "https://cloud-api.draft.example");
  assert.equal(node("csv").value, "personal,new@example.test,,/");
});

test("opening another list clears access only for the changed organization or destination", async () => {
  const {ui, detail, window} = await setup({mode: "onprem", employeeToken: true});
  const get = name => ui.byId.get("onprem-" + name);
  assert.ok(get("destination-token").value, "the same API with /v1/disk keeps its token");
  detail.batch.config.source_org_id = "another-org";
  await window.sourceBatches.open("onprem", "list-id");
  assert.equal(get("source-secret").value, "");
  assert.equal(get("source-admin-token").value, "");
  assert.ok(get("destination-token").value);
  get("source-secret").value = "new-test-only";
  detail.batch.config.destination_url = "https://cloud-api.other.example/v1/disk";
  await window.sourceBatches.open("onprem", "list-id");
  assert.equal(get("destination-token").value, "");
  assert.equal(get("source-secret").value, "new-test-only");
});

test("personal history restores both addresses, quoted paths and the verified recipient, including mapping errors", async () => {
  const {ui, node, detail, window, requests} = await setup({mode: "onprem"});
  const get = id => ui.byId.get("onprem-" + id);
  Object.assign(detail.batch.config, {destination_type: "personal", destination_auth: "service_application",
    destination_url: "https://cloud-api.office.example/v1/disk", destination_client_id: "office-client"});
  delete detail.batch.config.destination_vd_hash;
  delete detail.batch.config.destination_identity;
  Object.assign(detail.sources[0], {csv_row: 1, path: 'disk:/А, "Б"', destination_email: "first@office.example",
    destination_folder: "disk:/Архив", planned_destination_path: 'disk:/Архив/А, "Б"', destination_verified_uid: "9007199254740993210", mapping_error: "personal_mapping_conflict", conflict_rows: [1, 2]});
  Object.assign(detail.sources[1], {type: "personal", csv_row: 2, destination_email: "alias@office.example",
    destination_folder: "disk:/", destination_verified_uid: "9007199254740993210"});
  await window.sourceBatches.open("onprem", "list-id");
  assert.equal(get("destination-personal").checked, true);
  assert.match(node("template").getAttribute("href"), /^\/static\/personal-disk-pairs\.csv(?:\?v=\w+)?$/);
  assert.match(node("csv").value, /^source_email,path,destination_email,destination_folder\n/);
  assert.match(node("csv").value, /"user@example.test","\/А, ""Б""","first@office.example","\/Архив"/);
  assert.deepEqual(node("csv").value.split("\n").slice(2), ['"reader@example.test","/","alias@office.example","/"']);
  const row = node("sources").children[0];
  assert.deepEqual(row.children.map(td => td.getAttribute("data-label")), ["№", "Источник SaaS", "Получатель On-premises", "Подробности"]);
  assert.equal(row.children[0].textContent, "1");
  assert.match(row.children[0].children[0].getAttribute("aria-label"), /строку 1/);
  assert.equal(row.children[1].querySelector(".pair-path").textContent, '/А, "Б"');
  assert.equal(row.children[2].querySelector(".pair-path").textContent, '/Архив/А, "Б"');
  assert.match(node("sources").children[1].children[2].querySelector(".pair-path").textContent, /после проверки/);
  for (const value of ["first@office.example", "9007199254740993210", "1, 2", "Общий диск"]) assert.ok(row.textContent.includes(value), value);
  assert.equal(node("sources-head").hidden, true);
  assert.equal(node("plan-state").textContent, "Требуется проверка");
  assert.equal(node("start").disabled, true);
  assert.equal(node("plan").disabled, true);
  get("destination-client-secret").value = "office-secret";
  get("destination-owner-token").value = "owner-access";
  await get("destination-owner-token").fire("input");
  assert.equal(node("start").disabled, true, "correct credentials cannot bypass a conflicting pair");
  Object.assign(detail.sources[0], {mapping_error: "destination_identity_mismatch", credential_check: true, conflict_rows: [1]});
  detail.sources[1].planned_destination_path = "disk:/";
  await window.sourceBatches.refresh("onprem");
  assert.equal(node("start").disabled, false, "Start rechecks corrected credentials against the saved UID");
  assert.equal(node("start").textContent, "Начать перенос (2)");
  assert.equal(node("plan").textContent, "Проверить ещё раз");
  assert.equal(node("plan").disabled, false);
  await node("plan").fire("click");
  const body = requests.find(r => r.url.pathname === "/api/source-batches" && r.method === "POST").body;
  assert.deepEqual(body.credentials, {source_client_secret: "secret-value", source_admin_token: "admin-token-value",
    destination_client_secret: "office-secret", destination_owner_token: "owner-access"});
  assert.equal(body.config.destination_type, "personal");
  assert.equal(body.config.destination_auth, "service_application");
  assert.equal(body.config.destination_client_id, "office-client");
  for (const key of ["destination_identity", "destination_vd_hash", "identity_confirmed", "destination_oauth_url"]) assert.ok(!(key in body.config));
  assert.equal(node("history").children.at(-1).textContent.includes("Личный Диск"), true);
});

async function personalList(extra = {}) {
  const result = await setup({mode: "onprem", ...extra});
  const {detail, window} = result;
  Object.assign(detail.batch.config, {destination_type: "personal", destination_auth: "service_application", destination_client_id: "recipient-app"});
  detail.sources.forEach((s, i) => Object.assign(s, {type: "personal", csv_row: i + 1, destination_email: `user${i}@office.example`,
    destination_folder: "disk:/", planned_destination_path: "disk:/", destination_verified_uid: String(i+20)}));
  await window.sourceBatches.open("onprem", "list-id");
  return result;
}

test("personal CSV error filter keeps CSV numbers, diagnoses and selection of the full plan", async () => {
  const {ui, node, window, requests, detail} = await personalList();
  Object.assign(detail.sources[1], {mapping_error: "destination_path_collision", conflict_rows: [2, 5], conflict_path: "disk:/Архив",
    error: "source_publication_denied", selected: false});
  await window.sourceBatches.refresh("onprem");
  const filter = node("filter-errors"), all = node("filter-all"), rows = node("sources");
  assert.equal(filter.textContent, "С ошибками: 1");
  assert.equal(filter.getAttribute("aria-pressed"), "false");
  const before = requests.length;
  await filter.fire("click");
  assert.equal(filter.getAttribute("aria-pressed"), "true");
  assert.equal(all.hidden, false);
  assert.equal(rows.children.length, 1);
  const row = rows.children[0];
  assert.equal(row.querySelector(".pair-number").textContent, "2");
  assert.match(row.textContent, /Строки CSV: 2, 5.*Архив/);
  assert.match(row.textContent, /отклонил публикацию/);
  assert.equal(row.querySelector("#onprem-pair-source-b-plan").closest("details"), null);
  assert.equal(node("start").textContent, "Начать перенос (1)");
  assert.equal(node("plan-count").textContent, "Личные Диски · выбрано строк: 1 из 2");
  await all.fire("click");
  assert.deepEqual(rows.children.map(r => r.querySelector(".pair-number").textContent), ["1", "2"]);
  assert.deepEqual(rows.children.map(r => r.querySelector("input").checked), [true, false]);
  assert.equal(requests.length, before);

  for (const id of ["onprem-destination-client-secret", "onprem-destination-owner-token"]) {
    const input = ui.byId.get(id); input.value = "test-value"; await input.fire("input");
  }
  await filter.fire("click");
  await rows.children[0].querySelector("#onprem-pair-source-b-plan").fire("click");
  assert.ok(requests.some(r => r.method === "POST" && r.url.pathname === "/api/source-batches/list-id/sources/source-b/plan"));
  assert.ok(!requests.some(r => r.url.pathname.endsWith("/selection") || r.url.pathname.endsWith("/start")));
});

test("personal CSV polling updates filtered rows, clears hidden file panels and resets the filter on a new plan", async () => {
  const {node, window, timers, detail} = await personalList();
  detail.sources[1].summary.issues = 2;
  await timers.at(-1)();
  await node("sources").children[0].querySelector("#onprem-pair-source-a-files").fire("click");
  assert.equal(node("details").hidden, false);
  await node("filter-errors").fire("click");
  assert.equal(node("details").hidden, true);
  assert.equal(node("filter-errors").textContent, "С ошибками: 1");
  assert.match(node("sources").textContent, /Откройте.*«Файлы»/);
  await node("sources").children[0].querySelector("#onprem-pair-source-b-files").fire("click");
  await timers.at(-1)();
  assert.equal(node("details").hidden, false);
  assert.equal(node("sources").children.length, 1);
  detail.sources[1].summary.issues = 0;
  detail.sources[1].state = "completed_with_issues";
  await timers.at(-1)();
  assert.equal(node("filter-errors").textContent, "С ошибками: 1");
  detail.sources[1].state = "ready";
  await timers.at(-1)();
  assert.equal(node("details").hidden, true);
  assert.equal(node("filter-errors").textContent, "С ошибками: 0");
  assert.equal(node("filter-errors").getAttribute("aria-pressed"), "true");
  assert.match(node("sources").textContent, /Строк с ошибками нет/);
  await node("filter-all").fire("click");
  assert.equal(node("sources").children.length, 2);
  detail.sources[0].state = "error";
  await timers.at(-1)();
  await node("filter-errors").fire("click");
  detail.batch.id = "another-list";
  await window.sourceBatches.open("onprem", detail.batch.id);
  assert.equal(node("sources").children.length, 2);
  assert.equal(node("filter-errors").getAttribute("aria-pressed"), "false");
});

test("a late file list update cannot reopen a row hidden by the personal CSV error filter", async () => {
  let pendingRead, releaseRead, startedRead;
  const {node, detail, timers, requests} = await personalList({waitForEntries: () => {
    if (startedRead) startedRead();
    return pendingRead;
  }});
  detail.sources[1].error = "source_auth_failed";
  await timers.at(-1)();
  await node("sources").children[0].querySelector("#onprem-pair-source-a-files").fire("click");
  pendingRead = new Promise(resolve => { releaseRead = resolve; });
  const reading = new Promise(resolve => { startedRead = resolve; });
  const refresh = timers.at(-1)();
  await reading;
  await node("filter-errors").fire("click");
  releaseRead();
  await refresh;
  assert.equal(node("details").hidden, true);
  const checkbox = node("sources").querySelector("input");
  checkbox.checked = false;
  await checkbox.fire("change");
  assert.deepEqual(requests.at(-1).body, {source_ids: ["source-a"]});
  assert.equal(node("plan-count").textContent, "Личные Диски · выбрано строк: 1 из 2");
});

test("editing a reviewed personal list hides the stale routes and ready action until restored", async () => {
  const {ui, node} = await personalList();
  const root = ui.byId.get("onprem-source-batches");
  assert.ok(root.classList.contains("personal-plan-active"));
  assert.equal(node("start").disabled, true, "history alone cannot start transfer");
  assert.match(node("start-hint").textContent, /Параметры и доступы/);
  assert.equal(node("publish-warning").hidden, false);
  const selected = node("sources").children[0].children[0].children[0];
  selected.checked = false; await selected.fire("change");
  assert.equal(node("start").textContent, "Начать перенос (1)");
  assert.equal(node("plan-count").textContent, "Личные Диски · выбрано строк: 1 из 2");
  const reviewed = node("csv").value;
  node("csv").value += "\nthird@example.test,other@example.test"; await node("csv").fire("input");
  assert.equal(node("result").hidden, true);
  assert.equal(node("start").hidden, true);
  assert.equal(node("publish-warning").hidden, true);
  assert.equal(node("plan").textContent, "Проверить план");
  assert.equal(root.classList.contains("personal-plan-active"), false);
  node("csv").value = reviewed; await node("csv").fire("input");
  assert.equal(node("result").hidden, false);
  assert.equal(node("plan-title").hidden, false);
  assert.ok(node("plan-state").classList.contains("is-ready"));
});

for (const change of ["csv", "shared"]) test(`a different ${change} draft cannot control or export a hidden personal batch`, async () => {
  const {ui, node, requests, detail} = await personalList({state: "running", started: true});
  assert.equal(node("pause").hidden, false);
  assert.equal(node("stop").disabled, false);
  assert.equal(node("plan-state").classList.contains("is-ready"), false);
  if (change === "csv") {
    node("csv").value += "\nother@cloud.example,other@office.example"; await node("csv").fire("input");
  } else {
    ui.byId.get("onprem-destination-personal").checked = false;
    const shared = ui.byId.get("onprem-destination-shared"); shared.checked = true; await shared.fire("change");
  }
  assert.equal(node("result").hidden, true);
  assert.equal(node("history").value, "");
  const baseline = requests.length;
  for (const k of ["pause", "resume", "start", "stop", "restart", "report", "journal"]) {
    assert.ok(node(k).disabled, k);
    await node(k).dispatch("click");
  }
  const retry = ui.byId.get("retry-onprem"); retry.checked = false; await retry.fire("change");
  assert.equal(requests.length, baseline);
  assert.equal(detail.batch.config.retry_enabled, true);
});

test("personal batch polling keeps an open disclosure and keyboard focus on the same control or file", async () => {
  const {ui, node, timers} = await personalList();
  const freshElement = ui.document.createElement;
  ui.document.createElement = tag => {
    const el = freshElement(tag);
    el.focus = opts => { assert.equal(opts.preventScroll, true); ui.document.activeElement = el; };
    return el;
  };
  const panel = node("sources").children[0].querySelector("details"); panel.open = true;
  const summary = panel.querySelector("summary"); ui.document.activeElement = summary;
  await timers.at(-1)();
  const refreshed = node("sources").children[0].querySelector("details");
  assert.notEqual(refreshed, panel);
  assert.equal(refreshed.open, true);
  assert.equal(ui.document.activeElement, refreshed.querySelector("summary"));
  await refreshed.querySelector("#onprem-pair-source-a-files").fire("click");
  const file = node("entries").querySelector("input");
  ui.document.activeElement = file;
  await timers.at(-1)();
  const currentFile = node("entries").querySelector("input");
  assert.notEqual(file, currentFile);
  assert.equal(ui.document.activeElement, currentFile);
});

for (const mode of ["local", "onprem"]) test(`${mode} list stop requires confirmation, pause does not`, async () => {
  const {node, window, requests} = await setup({mode, state:"running", started:true});
  let decisions = 0;
  window.confirm = prompt => { decisions++; assert.match(prompt, /Остановить весь список от/); return false; };
  await node("stop").fire("click");
  assert.equal(decisions, 1);
  assert.equal(requests.some(r => r.url.pathname.endsWith("/stop")), false);
  await node("pause").fire("click");
  assert.equal(decisions, 1);
  assert.ok(requests.some(r => r.url.pathname.endsWith("/pause")));
});

test("switching the list destination keeps separate CSV drafts and the old shared template", async () => {
  const {ui, node} = await setup({mode: "onprem"});
  const get = id => ui.byId.get("onprem-destination-" + id);
  const sharedCsv = node("csv").value;
  get("personal").checked = true; get("shared").checked = false; await get("personal").fire("change");
  assert.equal(node("csv").value, "");
  const pairs = "source_email,destination_email\nuser@cloud.example,user@office.example";
  node("csv").value = pairs; await node("csv").fire("input");
  get("personal").checked = false; get("shared").checked = true; await get("shared").fire("change");
  assert.equal(node("csv").value, sharedCsv);
  assert.equal(node("template").getAttribute("href"), "/static/source-list-template.csv");
  get("personal").checked = true; get("shared").checked = false; await get("personal").fire("change");
  assert.equal(node("csv").value, pairs);
  const templatePath = new URL(node("template").getAttribute("href"), "http://localhost").pathname;
  const template = fs.readFileSync(path.join(__dirname, "../app", templatePath), "utf8").trimEnd();
  assert.equal(node("csv").placeholder, template);
  const [header, withFolder, wholeDisk] = template.split("\n");
  assert.equal(header, "source_email,path,destination_email,destination_folder");
  assert.deepEqual(withFolder.split(","), ["aivanov@cloud.example", "/Документы", "apetrov@office.example", "/Архив/2026"]);
  assert.deepEqual(wholeDisk.split(","), ["olga@cloud.example", "", "osokolova@office.example", ""]);
});

for (const [code, csvRow, message] of [
  ["invalid_sources_columns", null, /^В заголовке CSV.*source_email.*destination_email/],
  ["invalid_sources_row", 1, /^Строка 1\. .*email/],
]) test(`a malformed personal CSV identifies the header or the account (${code})`, async () => {
  const {ui, node} = await setup({mode: "onprem", createError: {code, csv_row: csvRow}});
  const get = id => ui.byId.get("onprem-destination-" + id);
  get("personal").checked = true; get("shared").checked = false; await get("personal").fire("change");
  get("client-id").value = "office-client"; get("client-secret").value = "office-secret"; get("owner-token").value = "owner-token";
  node("csv").value = csvRow ? "source_email,destination_email\n\na@cloud.example,?" : "source,destination\na@cloud.example,b@office.example";
  await node("csv").fire("input");
  await node("plan").fire("click");
  assert.match(node("notice").textContent, message);
  assert.doesNotMatch(node("notice").textContent, /vd_hash/);
});

test("returning to a cached page resumes polling the active list", async () => {
  const {window, timers, requests} = await setup({mode: "onprem"});
  window.dispatchEvent({type: "pagehide", persisted: true});
  const before = timers.length;
  window.dispatchEvent({type: "pageshow", persisted: true});
  assert.equal(timers.length, before + 1);
  const reads = requests.length;
  await timers.at(-1)();
  assert.ok(requests.length > reads);
});

test("restart credential recovery automatically attaches entered secrets then resumes", async () => {
  const {node, requests, ui, detail} = await setup({mode: "onprem", employeeToken: true, sharedAvailable: false, state: "awaiting_credentials", credentials: false});
  detail.batch.resume_state = "running";
  assert.equal(node("resume").disabled, false);
  await node("resume").fire("click");
  const mutations = requests.filter((request) => request.method !== "GET");
  assert.deepEqual(mutations.map((request) => request.url.pathname), ["/api/source-batches/list-id/credentials", "/api/source-batches/list-id/resume"]);
  assert.equal(mutations[0].body.credentials.source_client_secret, "secret-value");
  assert.equal(mutations[0].body.credentials.source_admin_token, "admin-token-value");
  assert.equal(mutations[0].body.credentials.destination_token, "destination-token-value");
  assert.equal(node("pause").disabled, false);
  assert.equal(node("resume").disabled, true);
  assert.equal(ui.storageWrites.some((entry) => /admin-token-value|secret-value|destination-token-value/.test(entry.value)), false);
});

test("Dry Run creates persistent list but never sends Start or per-source secrets in CSV", async () => {
  const {node, requests} = await setup({state: "stopped"});
  node("csv").value = "type,email,vd_hash,path\npersonal,new@example.test,,/";
  await node("csv").fire("input");
  await node("restart").fire("click");
  const mutations = requests.filter((request) => request.method !== "GET");
  assert.deepEqual(mutations.map((request) => request.url.pathname), ["/api/source-batches", "/api/source-batches/list-id/plan"]);
  assert.equal(mutations[0].body.mode, "local");
  assert.equal(mutations[0].body.config.source_client_secret, undefined);
  assert.equal(mutations[0].body.config.source_admin_token, undefined);
  assert.equal(mutations[0].body.config.admin_token, undefined);
  assert.equal(mutations[0].body.config.source_org_id, "org");
  assert.equal(mutations[0].body.credentials.source_client_secret, "secret-value");
  assert.equal(mutations[0].body.credentials.source_admin_token, "admin-token-value");
  assert.equal(mutations[0].body.sources_csv.includes("admin-token-value"), false);
  assert.equal(node("start").disabled, true);
  assert.equal(node("pause").disabled, false, "Dry Run itself can be paused");
});

test("local source-list reattaches administrator access from the form without saving it in browser storage", async () => {
  const {node, requests, ui, detail} = await setup({state: "awaiting_credentials", credentials: false});
  detail.batch.resume_state = "running";
  await node("resume").fire("click");
  const mutations = requests.filter((request) => request.method !== "GET");
  assert.equal(mutations[0].url.pathname, "/api/source-batches/list-id/credentials");
  assert.equal(mutations[0].body.credentials.source_admin_token, "admin-token-value");
  assert.equal(mutations[0].body.credentials.source_client_secret, "secret-value");
  assert.equal(ui.storageWrites.some((entry) => /admin-token-value|secret-value/.test(entry.value)), false);
});

test("disabled public Onprem list cannot create requests even with filled fields", async () => {
  const {node, requests, ui, window} = await setup({mode: "onprem", enabled: false});
  assert.equal(node("plan").disabled, true);
  assert.equal(node("start").disabled, true);
  await node("plan").fire("click");
  await window.sourceBatches.open("onprem", "list-id");
  assert.equal(requests.length, 0);
  assert.equal(ui.byId.get("mode-onprem").hidden, true);
});

test("single-source Dry Run preserves the other manifests and attaches current credentials", async () => {
  const {node, requests} = await setup({state: "needs_attention", sourceError: {code: "source_auth_failed"}});
  const button = node("sources").children[0].children[7].children[0];
  assert.equal(button.disabled, false);
  await button.fire("click");
  const mutations = requests.filter((request) => request.method !== "GET");
  assert.deepEqual(mutations.map((request) => request.url.pathname), ["/api/source-batches/list-id/credentials", "/api/source-batches/list-id/sources/source-a/plan"]);
  assert.equal(node("start").disabled, true);
});

test("configuration edits invalidate saved plan; changing secrets keeps plan membership", async () => {
  const {ui, node} = await setup();
  ui.byId.get("local_client_secret").value = "replacement-secret";
  await ui.byId.get("local_client_secret").fire("input");
  assert.equal(node("start").disabled, false);
  ui.byId.get("local_admin_token").value = "replacement-admin-token";
  await ui.byId.get("local_admin_token").fire("input");
  assert.equal(node("start").disabled, false, "administrator access is not saved plan configuration");
  ui.byId.get("local_orgid").value = "other-organization";
  await ui.byId.get("local_orgid").fire("input");
  assert.equal(node("start").disabled, true);
});

test("source journal link opens exact child rather than an arbitrary default job", async () => {
  const {node, detail, events, window} = await setup({mode: "onprem"});
  const jobId = "a".repeat(32);
  detail.sources[1].child_job_id = jobId;
  await window.sourceBatches.refresh("onprem");
  await node("sources").children[1].children[7].children[2].fire("click");
  assert.equal(events.at(-1).type, "open-onprem-job");
  assert.equal(events.at(-1).detail.jobId, jobId);
});

test("batch and per-source rates show readable volumes and aggregate active sources", async () => {
  const {node, detail, window} = await setup({mode: "onprem"});
  detail.sources[0].transfer_rates = {download_bytes_per_second: 1024 * 1024, upload_bytes_per_second: 1024};
  detail.sources[1].transfer_rates = {download_bytes_per_second: 1024 * 1024, upload_bytes_per_second: 1024};
  await window.sourceBatches.refresh("onprem");
  assert.match(node("download-rate").textContent, /2 Мб\/с/);
  assert.match(node("upload-rate").textContent, /2 Кб\/с/);
  assert.match(node("sources").children[0].children[6].textContent, /1 Мб\/с/);
});

test("error details preserve HTTP information and redact typed secrets and signed URL query", async () => {
  const {node} = await setup({sourceError: {code: "source_unavailable", http_status: 503,
    diagnostic_message: "retry secret-value admin-token-value https://download.example.test/file?token=hidden-secret"}});
  const text = node("sources").children[0].children[7].children.at(-1).textContent;
  assert.match(text, /HTTP 503/);
  assert.match(text, /\[скрыто\]/);
  assert.doesNotMatch(text, /admin-token-value|secret-value|hidden-secret|token=/);
});

test("list administrator checks are optional and the UI omits empty credentials", async () => {
  for (const mode of ["local", "onprem"]) {
    const {node, requests, ui} = await setup({mode});
    const field = ui.byId.get(mode === "local" ? "local_admin_token" : "onprem-source-admin-token");
    field.value = "";
    await field.fire("input");
    assert.equal(node("start").disabled, false);
    await node("start").fire("click");
    const access = requests.find((request) => request.url.pathname.endsWith("/credentials"));
    assert.equal(access.body.credentials.source_admin_token, undefined);
    assert.equal(node("start").disabled, true, "starting still follows the normal task lifecycle");
  }
});

test("administrator check errors show the employee and organization action instead of an opaque code", async () => {
  const cases = {
    source_admin_auth_failed: /админский токен.*directory:read_users/,
    source_user_not_in_organization: /Email.*ORGID/,
    source_user_blocked: /заблокирован.*Разблокируйте/,
    source_admin_check_failed: /API 360/,
    source_admin_invalid_response: /API 360.*некорректный ответ/,
  };
  for (const [code, expected] of Object.entries(cases)) {
    const {node} = await setup({sourceError: {code}});
    assert.match(node("sources").children[0].children[7].children.at(-1).textContent, expected);
  }
});

test("shared Onprem child opens correct journal and retains source identity on credential recovery", async () => {
  const ui = await loadUi({cross_org: true, onprem_enabled: true, onprem_auth_required: false});
  const jobId = "a".repeat(32);
  const sharedConfig = {source_org_id: "org", source_client_id: "client", source_email: "reader@example.test", source_path: "disk:/",
    source_type: "shared", source_vd_hash: "source-vd", source_folder: "Общий архив (source-vd)", source_public: false,
    destination_url: "https://disk.example.test/v1/disk", destination_identity: "operator@example.test", destination_vd_hash: "target", destination_type: "shared", destination_base: "disk:/", destination_root: "disk:/Общий архив (source-vd)", strategy: "staged", identity_confirmed: true};
  const job = {id: jobId, config: sharedConfig, state: "paused", resume_state: "running", created_at: "2026-10-14T10:00:00Z", counts: {planned: 1}, total_files: 1, total_directories: 0, total_bytes: 1024};
  const requests = [];
  const fetch = async (address, options = {}) => {
    const body = options.body ? JSON.parse(options.body) : null;
    requests.push({address, options, body});
    let value = job;
    if (address.endsWith("/resume")) job.state = "running";
    if (address.includes("/entries")) value = {entries: [{id: "entry-a", kind: "file", source_path: "disk:/file.txt", state: "planned", size: 1024}]};
    else if (address.includes("/events")) value = {events: []};
    else if (address.includes("jobs?")) value = {jobs: [job]};
    return {ok: true, status: 200, json: async () => clone(value)};
  };
  const choice = ui.byId.get("onprem-source-input");
  choice.value = "list"; choice.dispatchEvent = (event) => choice.fire(event.type);
  const form = ui.byId.get("onprem-form"); form.checkValidity = () => true; form.reportValidity = () => true;
  ui.document.querySelector('.mode-tab[data-mode="onprem"]').classList.add("active");
  vm.runInNewContext(fs.readFileSync(path.resolve(__dirname, "../app/static/onprem.js"), "utf8"), {
    document: ui.document, window: ui.window, fetch, URL, Blob, crypto, Uint8Array, console,
    Event: class {constructor(type) {this.type = type;}}, setTimeout() {return 1;}, clearTimeout() {},
  });
  await flush();
  ui.window.dispatchEvent({type: "open-onprem-job", detail: {jobId}});
  await flush();
  assert.equal(choice.value, "single");
  assert.match(ui.byId.get("onprem-source-email-label").textContent, /с доступом к общему диску/);
  assert.match(ui.byId.get("onprem-source-folder-note").textContent, /Общий архив/);
  ui.byId.get("onprem-source-secret").value = "secret-value";
  ui.byId.get("onprem-destination-token").value = "token-value";
  ui.byId.get("onprem-identity-confirmed").checked = true;
  await ui.byId.get("onprem-identity-confirmed").fire("change");
  assert.equal(ui.byId.get("onprem-resume").disabled, false);
  await ui.byId.get("onprem-resume").fire("click");
  await flush();
  const access = requests.find((request) => request.address.endsWith("/credentials"));
  assert.ok(access);
  assert.equal(access.body.config.source_type, "shared");
  assert.equal(access.body.config.source_vd_hash, "source-vd");
  assert.equal(access.body.config.source_folder, "Общий архив (source-vd)");
  assert.equal(access.body.config.source_public, false);
  assert.equal(access.body.config.strategy, "staged");
  assert.equal(job.state, "running");
});


test("failed source selection restores visible checkbox to saved plan", async () => {
  const {node} = await setup({selectionError: "invalid_source_selection"});
  const checkbox = node("sources").children[0].children[0].children[0];
  checkbox.checked = false;
  await checkbox.fire("change");
  assert.equal(checkbox.checked, true, "rejected selection must not falsely display excluded source");
  assert.equal(node("start").disabled, false);
  assert.match(node("notice").textContent, /выберите|Выберите/);
});

test("paused transfer freezes source selection and per-source replanning", async () => {
  const {node} = await setup({state: "paused", started: true});
  const row = node("sources").children[0];
  assert.equal(row.children[0].children[0].disabled, true);
  assert.equal(row.children[7].children[0].disabled, true);
  assert.equal(row.children[7].children[1].disabled, false, "file inspection remains available");
  assert.equal(node("resume").disabled, false);
  assert.equal(node("stop").disabled, false);
});

test("edited CSV cannot operate on stored rows or resume old membership silently", async () => {
  const {node} = await setup({state: "paused"});
  node("csv").value += "\npersonal,new@example.test,,/";
  await node("csv").fire("input");
  const row = node("sources").children[0];
  assert.equal(row.children[0].children[0].disabled, true);
  assert.equal(row.children[7].children[0].disabled, true);
  assert.equal(node("resume").disabled, true);
  assert.equal(node("plan").disabled, false, "new CSV can create a new independent plan");
  assert.equal(node("stop").disabled, false, "stopping never depends on changed form fields");
});

test("completed list with file errors can still be stopped to release child operations", async () => {
  const {node, requests} = await setup({state: "completed_with_issues", started: true});
  assert.equal(node("stop").disabled, false);
  await node("stop").fire("click");
  assert.equal(requests.at(-1).url.pathname, "/api/source-batches/list-id/stop");
  assert.equal(node("stop").disabled, true);
  assert.equal(node("restart").disabled, false);
});

test("stopped Dry Run can create a fresh plan after the previous plan request failed", async () => {
  for (const mode of ["local", "onprem"]) {
    const {node, requests} = await setup({mode, state: "stopped", failFirstPlan: true});
    await node("plan").fire("click");
    assert.match(node("notice").textContent, /Действие ещё выполняется/);
    await node("stop").fire("click");
    assert.equal(node("plan").disabled, false);
    assert.equal(node("restart").disabled, false);
    await node("restart").fire("click");
    const creates = requests.filter((request) => request.method === "POST" && request.url.pathname === "/api/source-batches");
    assert.equal(creates.length, 2);
    assert.notEqual(creates[1].body.idempotency_key, creates[0].body.idempotency_key,
      "a confirmed creation must not bind a new Dry Run to the stopped batch");
    assert.equal(node("plan").disabled, true, "active Dry Run cannot start a concurrent plan");
    assert.equal(node("restart").disabled, true);
  }
});

test("accepted stop shows remaining cancellation and permits a fresh Dry Run for either mode", async () => {
  for (const mode of ["local", "onprem"]) {
    const {node, detail, window, requests} = await setup({mode, state: "stopped"});
    detail.batch.stop_pending = true;
    detail.sources.forEach((source) => {source.state = "stopped";});
    await window.sourceBatches.refresh(mode);
    assert.match(node("state").textContent, /Остановка выполняется/);
    assert.match(node("sources").children[0].children[2].textContent, /Остановлено/);
    assert.equal(node("plan").disabled, false);
    assert.equal(node("restart").disabled, false);
    assert.equal(node("resume").disabled, true);
    await node("plan").fire("click");
    const mutations = requests.filter((request) => request.method !== "GET");
    assert.deepEqual(mutations.map((request) => request.url.pathname), ["/api/source-batches", "/api/source-batches/list-id/plan"]);
  }
});


test("Onprem CSV Dry Run uses staged despite DOM strategy tampering", async () => {
  const {ui, node, requests} = await setup({mode: "onprem"});
  ui.byId.get("onprem-strategy").value = "url";
  await ui.byId.get("onprem-strategy").fire("change");
  await node("plan").fire("click");
  await flush();
  const create = requests.find((request) => request.method === "POST" && request.url.pathname === "/api/source-batches");
  assert.ok(create, "Dry Run creates a new source list");
  assert.equal(create.body.config.strategy, "staged", "the source-list payload cannot select URL import");
});

async function enterServiceAccessAfterLegacyPlan(ui, node) {
  assert.equal(ui.byId.get("onprem-destination-token").value, "");
  assert.equal(ui.byId.get("onprem-identity-confirmed").checked, false);
  ui.byId.get("onprem-destination-client-id").value = "office-app";
  ui.byId.get("onprem-destination-client-secret").value = "office-secret";
  ui.byId.get("onprem-destination-owner-token").value = "owner";
  await ui.byId.get("onprem-destination-owner-token").fire("input");
  assert.equal(node("plan").disabled, false);
  await node("plan").fire("click");
}

test("restoring an old URL source list prepares a new staged Dry Run", async () => {
  const {ui, node, requests} = await setup({mode: "onprem", strategy: "url"});
  assert.equal(ui.byId.get("onprem-strategy").value, "staged");
  assert.equal(node("start").disabled, true, "an old URL list cannot start with new staged settings");
  assert.equal(node("report").disabled, false, "history still supports export");
  await node("plan").fire("click");
  assert.ok(requests.every(request => request.method === "GET"));
  await enterServiceAccessAfterLegacyPlan(ui, node);
  const create = requests.find((request) => request.method === "POST" && request.url.pathname === "/api/source-batches");
  assert.ok(create);
  assert.equal(create.body.config.strategy, "staged");
});


test("CSV strategy tampering preserves an uncertain staged creation key", async () => {
  const {ui, node, requests} = await setup({mode: "onprem", failCreates: true});
  await node("plan").fire("click");
  await flush();
  ui.byId.get("onprem-strategy").value = "url";
  await ui.byId.get("onprem-strategy").fire("change");
  await node("plan").fire("click");
  await flush();
  const creates = requests.filter((request) => request.method === "POST" && request.url.pathname === "/api/source-batches");
  assert.equal(creates.length, 2);
  assert.equal(creates[0].body.config.strategy, "staged");
  assert.equal(creates[1].body.config.strategy, "staged");
  assert.equal(creates[1].body.idempotency_key, creates[0].body.idempotency_key);
});

test("historical source errors explain the required new staged Dry Run", async () => {
  const {node} = await setup({mode: "onprem", sourceError: "onprem_staged_required"});
  assert.match(node("sources").children[0].children[7].children.at(-1).textContent, /нов.*Dry Run/i);
});


test("historical URL lists freeze membership and source replanning including staged siblings", async () => {
  for (const options of [{strategy: "url"}, {childStrategy: "url"}]) {
    const {node} = await setup({mode: "onprem", ...options});
    for (const row of node("sources").children) {
      assert.equal(row.children[0].children[0].disabled, true, "the historical list membership is immutable");
      assert.equal(row.children[7].children[0].disabled, true, "old source manifests cannot be replanned");
      assert.equal(row.children[7].children[1].disabled, false, "saved files remain inspectable");
    }
    assert.equal(node("start").disabled, true);
  }
});

test("forced events cannot mutate a historical URL list or attach credentials to URL children", async () => {
  for (const options of [{strategy: "url"}, {childStrategy: "url"}]) {
    const {node, requests} = await setup({mode: "onprem", state: "awaiting_credentials", credentials: false, ...options});
    const row = node("sources").children[0], checkbox = row.children[0].children[0];
    checkbox.checked = false;
    await checkbox.fire("change");
    assert.equal(checkbox.checked, true, "blocked selection restores saved membership");
    await row.children[7].children[0].fire("click");
    await node("resume").fire("click");
    await node("start").fire("click");
    await node("pause").fire("click");
    assert.deepEqual(requests.filter((request) => request.method !== "GET"), [], "no historical mutation reaches the API");
    assert.match(node("notice").textContent, /нов.*Dry Run/i);
    assert.equal(node("resume").disabled, true);
  }
});

test("historical URL lists retain history files exports and stop without attaching credentials", async () => {
  for (const options of [{strategy: "url"}, {childStrategy: "url"}]) {
    const {node, requests, window, events} = await setup({mode: "onprem", state: "running", ...options});
    assert.equal(node("pause").disabled, true);
    assert.equal(node("history").disabled, false);
    assert.equal(node("refresh").disabled, false);
    assert.equal(node("report").disabled, false);
    assert.equal(node("journal").disabled, false);
    assert.equal(node("stop").disabled, false);
    await window.sourceBatches.refresh("onprem");
    const actions = node("sources").children[0].children[7];
    await actions.children[1].fire("click");
    assert.equal(node("details").hidden, false);
    if (options.childStrategy) {
      await actions.children[2].fire("click");
      assert.equal(events.at(-1).detail.jobId, "a".repeat(32));
    }
    await node("report").fire("click");
    await node("journal").fire("click");
    await node("stop").fire("click");
    assert.deepEqual(requests.filter((request) => request.method !== "GET").map((request) => request.url.pathname), ["/api/source-batches/list-id/stop"]);
    assert.ok(requests.some((request) => request.url.pathname.endsWith("/report.csv")));
    assert.ok(requests.some((request) => request.url.pathname.endsWith("/journal.csv")));
  }
});

test("historical URL lists can create an independent staged Dry Run from active and terminal states", async () => {
  for (const options of [{strategy: "url"}, {childStrategy: "url"}]) {
    for (const [state, action] of [["running", "plan"], ["stopped", "restart"]]) {
      const {ui, node, requests, detail} = await setup({mode: "onprem", state, createdBatchId: "fresh-staged-list", ...options});
      assert.equal(node(action).disabled, false, "new staged plan is the recovery path");
      await node(action).fire("click");
      assert.ok(requests.every(request => request.method === "GET"));
      await enterServiceAccessAfterLegacyPlan(ui, node);
      const mutations = requests.filter((request) => request.method !== "GET");
      assert.deepEqual(mutations.map((request) => request.url.pathname), ["/api/source-batches", "/api/source-batches/fresh-staged-list/plan"]);
      assert.equal(mutations[0].body.config.strategy, "staged");
      assert.equal(detail.batch.config.strategy, "staged");
      assert.equal(node("pause").disabled, false, "the new staged Dry Run uses the regular lifecycle");
    }
  }
});

for (const mode of ["local", "onprem"]) {
  for (const state of ["ready", "running", "paused"]) test(`${mode} list ${state}: retry policy changes without credentials or invalidating Dry Run`, async () => {
    const {ui, node, requests, detail} = await setup({mode, state, retryEnabled: false});
    const checkbox = ui.byId.get("retry-" + mode);
    assert.equal(checkbox.checked, false);
    assert.equal(checkbox.disabled, false);
    const before = requests.length;
    checkbox.checked = true; await checkbox.fire("change");
    const writes = requests.slice(before).filter((request) => request.method !== "GET");
    assert.deepEqual(writes.map((request) => request.url.pathname), ["/api/source-batches/list-id/retries"]);
    assert.deepEqual(writes[0].body, {enabled: true});
    assert.equal(detail.batch.config.retry_enabled, true);
    if (state === "ready") assert.equal(node("start").disabled, false, "a policy change keeps the saved plan ready");
  });
  test(`${mode} list: changing retry policy after uncertain creation keeps identity and applies latest setting`, async () => {
    const {ui, node, requests} = await setup({mode, state: "stopped", createdRetryEnabled: true});
    const checkbox = ui.byId.get("retry-" + mode);
    checkbox.checked = false; await checkbox.fire("change");
    await node("plan").fire("click");
    const writes = requests.filter((request) => request.method !== "GET");
    assert.equal(writes[0].body.config.retry_enabled, false);
    assert.deepEqual(writes.map((request) => request.url.pathname), ["/api/source-batches", "/api/source-batches/list-id/retries", "/api/source-batches/list-id/plan"]);
    assert.equal(checkbox.checked, false);
  });
  test(`${mode} list: retry policy cannot modify historical URL jobs or completed lists`, async () => {
    const {ui, requests} = await setup({mode, state: "completed", strategy: mode === "onprem" ? "url" : "staged"});
    const checkbox = ui.byId.get("retry-" + mode);
    checkbox.checked = false; await checkbox.fire("change");
    assert.equal(requests.filter((request) => request.method !== "GET").length, 0);
  });
  test(`${mode} list: rejected retry policy restores the old value and leaves plan actions available`, async () => {
    const {ui, node} = await setup({mode, retryError: "onprem_access_required"});
    const checkbox = ui.byId.get("retry-" + mode);
    checkbox.checked = false; await checkbox.fire("change");
    assert.equal(checkbox.checked, true);
    assert.equal(checkbox.disabled, false);
    assert.equal(node("start").disabled, false);
    assert.doesNotMatch(ui.byId.get("retry-" + mode + "-notice").textContent, /secret-value/);
  });
}

for (const mode of ["local", "onprem"]) test(`${mode} list: retry selection preserves an uncertain create idempotency key`, async () => {
  const {ui, node, requests} = await setup({mode, state: "stopped", failCreates: true});
  await node("plan").fire("click");
  const check = ui.byId.get("retry-" + mode); check.checked = false; await check.fire("change");
  await node("plan").fire("click");
  const creates = requests.filter((request) => request.method === "POST" && request.url.pathname === "/api/source-batches");
  assert.equal(creates.length, 2);
  assert.equal(creates[0].body.idempotency_key, creates[1].body.idempotency_key);
  assert.equal(creates[1].body.config.retry_enabled, false);
});

for (const mode of ["local", "onprem"]) for (const switchContext of [false, true]) test(`${mode} pending CSV creation: ${switchContext ? "standalone preference stays separate" : "latest list preference reaches accepted list"}`, async () => {
  let release;
  const createGate = new Promise((resolve) => {release = resolve;});
  const {ui, node, requests, detail} = await setup({mode, state: "stopped", retryEnabled: true, createdBatchId: "new-list", createGate});
  const pending = node("plan").fire("click"); await flush();
  if (switchContext) {
    ui.byId.get(mode + "-source-input").value = "single"; await ui.byId.get(mode + "-source-input").fire("change");
    // This harness loads source-batches; the standalone Onprem module owns its own context in the page.
    if (mode === "onprem") ui.window.diskCopyRetries.bind(mode, {id: "onprem:standalone", enabled: true, editable: false});
  }
  const check = ui.byId.get("retry-" + mode); check.checked = false; await check.fire("change");
  release(); await pending;
  const changes = requests.filter((request) => request.url.pathname === "/api/source-batches/new-list/retries");
  assert.equal(changes.length, switchContext ? 0 : 1);
  assert.equal(detail.batch.config.retry_enabled, switchContext);
  assert.equal(check.checked, false);
});

test("shared CSV uses one delegated operator for all sources and restores the access method from history", async () => {
  const {ui, node, requests, detail, window} = await setup({mode: "onprem", employeeToken: true});
  const get = suffix => ui.byId.get("onprem-" + suffix);
  assert.equal(get("shared-auth"), undefined);
  assert.equal(window.sourceBatches.usesEmployeeToken(), true);
  get("destination-token").value = "";
  get("identity-confirmed").checked = false;
  await get("identity-confirmed").fire("change");
  await node("plan").fire("click");
  assert.equal(window.sourceBatches.usesEmployeeToken(), false);
  assert.ok(requests.every(r => r.method === "GET"), "switching an old plan never creates a new transfer");
  assert.equal(node("start").disabled, true, "old manual-token list is detached");
  get("destination-url").value = "https://cloud-api.office.test";
  get("destination-identity").value = "writer@office.test";
  get("destination-client-id").value = "app";
  get("destination-client-secret").value = "app-secret";
  get("destination-owner-token").value = "owner";
  get("identity-confirmed").checked = false;
  await get("destination-owner-token").fire("input");
  assert.equal(node("plan").disabled, false);
  await node("plan").fire("click");
  const body = requests.find(r => r.url.pathname === "/api/source-batches" && r.method === "POST").body;
  assert.equal(body.config.destination_auth, "service_application");
  assert.equal(body.config.destination_type, "shared");
  assert.equal(body.config.destination_identity, "writer@office.test");
  assert.equal(body.config.destination_vd_hash, "target");
  assert.equal(body.config.identity_confirmed, false);
  assert.deepEqual(body.credentials, {source_client_secret:"secret-value", source_admin_token:"admin-token-value",
    destination_client_secret:"app-secret", destination_owner_token:"owner"});
  assert.match(body.sources_csv, /^type,email,vd_hash,path/);
  detail.batch.state = "ready"; detail.batch.shared_operator_uid = "1000123";
  detail.sources.forEach(row => { row.destination_verified_uid = "1000123"; });
  get("destination-owner-token").value = "";
  get("destination-client-secret").value = "";
  await window.sourceBatches.open("onprem", detail.batch.id);
  assert.equal(window.sourceBatches.usesEmployeeToken(), false);
  assert.match(node("operator").textContent, /writer@office.test.*1000123/);
  assert.equal(get("destination-owner-token").value, "");
  assert.equal(node("start").disabled, true);
  get("destination-client-secret").value = "restored-secret";
  get("destination-owner-token").value = "restored-owner";
  await get("destination-owner-token").fire("input");
  assert.equal(node("start").disabled, false);
  await node("start").fire("click");
  assert.deepEqual(requests.at(-2).body.credentials, {...body.credentials, destination_client_secret:"restored-secret", destination_owner_token:"restored-owner"});
  assert.match(requests.at(-1).url.pathname, /\/start$/);
});


test("new shared list stays unavailable without delegated access, even with saved employee credentials", async () => {
  const {ui, node, requests, window} = await setup({mode: "onprem", employeeToken: true, sharedAvailable: false});
  assert.equal(window.sourceBatches.usesEmployeeToken(), true);
  assert.equal(node("start").disabled, false);
  node("history").value = ""; await node("history").fire("change");
  assert.equal(window.sourceBatches.usesEmployeeToken(), false);
  assert.equal(ui.byId.get("onprem-destination-token").value, "");
  ui.byId.get("onprem-destination-token").value = "injected-token";
  ui.byId.get("onprem-identity-confirmed").checked = true;
  await ui.byId.get("onprem-identity-confirmed").fire("change");
  assert.equal(node("plan").disabled, true);
  await node("plan").fire("click");
  assert.ok(requests.every(request => request.method === "GET"));
});
