const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const crypto = require("node:crypto").webcrypto;
let formValid = true;
let formValidationCalls = 0;

class Element {
  constructor(tag, attributes = {}) {
    this.tagName = tag.toUpperCase();
    this.attributes = attributes;
    this.id = attributes.id || "";
    this.value = attributes.value || "";
    this.hidden = "hidden" in attributes;
    this.disabled = "disabled" in attributes;
    this.checked = "checked" in attributes;
    this.children = [];
    this.listeners = {};
    this.dataset = {};
    this._text = "";
    this.className = attributes.class || "";
    for (const [key, value] of Object.entries(attributes)) {
      if (key.startsWith("data-")) this.dataset[key.slice(5).replace(/-([a-z])/g, (_, char) => char.toUpperCase())] = value;
    }
    this.classList = {
      contains: (value) => this.className.split(/\s+/).includes(value),
      add: (value) => {if (!this.classList.contains(value)) this.className += " " + value;},
      remove: (value) => {this.className = this.className.split(/\s+/).filter((part) => part !== value).join(" ");},
    };
  }
  addEventListener(name, handler) {(this.listeners[name] ||= []).push(handler);}
  get disabled() {return this._disabled;}
  set disabled(value) {this._disabled = Boolean(value);}
  get title() {return this.attributes.title || "";}
  set title(value) {this.attributes.title = String(value);}
  appendChild(node) {this.children.push(node); node.parent = this; return node;}
  replaceChildren(...nodes) {this.children = nodes; this._text = "";}
  get textContent() {return this._text + this.children.map((child) => child.textContent).join("");}
  set textContent(value) {this._text = String(value); this.children = [];}
  set innerHTML(_) {throw new Error("Unsafe innerHTML renderer");}
  checkValidity() {return formValid;}
  reportValidity() {formValidationCalls += 1; return formValid;}
  remove() {this.removed = true;}
  click() {if (!this.disabled) return fire(this, "click");}
}

const staticRoot = path.resolve(__dirname, "../app/static");
const html = fs.readFileSync(path.join(staticRoot, "index.html"), "utf8");
const source = fs.readFileSync(path.join(staticRoot, "onprem.js"), "utf8");
const matches = (node, selector) => {
  if (selector === ".mode-tab") return node.classList.contains("mode-tab");
  if (selector === '[data-legacy-output]') return "data-legacy-output" in node.attributes;
  if (selector === '[data-onprem-config]') return "data-onprem-config" in node.attributes;
  if (selector === "input, select, textarea") return ["INPUT", "SELECT", "TEXTAREA"].includes(node.tagName);
  const mode = selector.match(/^\.mode-tab\[data-mode="([^"]+)"\]$/);
  return Boolean(mode && node.classList.contains("mode-tab") && node.dataset.mode === mode[1]);
};
function createDocument() {
  const nodes = [];
  const byId = new Map();
  for (const match of html.matchAll(/<([a-z][\w-]*)\b([^>]*)>/gi)) {
    const attributes = {};
    for (const attribute of match[2].matchAll(/([\w:-]+)(?:="([^"]*)")?/g)) attributes[attribute[1]] = attribute[2] || "";
    const node = new Element(match[1], attributes);
    nodes.push(node);
    if (node.id) byId.set(node.id, node);
  }
  for (const match of html.matchAll(/<select\b[^>]*id="([^"]+)"[^>]*>([\s\S]*?)<\/select>/gi)) {
    const options = [...match[2].matchAll(/<option\b([^>]*)>/gi)];
    const chosen = options.find((option) => /\bselected\b/.test(option[1])) || options[0];
    if (chosen) byId.get(match[1]).value = chosen[1].match(/\bvalue="([^"]*)"/)?.[1] || "";
  }
  const panel = byId.get("mode-onprem");
  panel.querySelectorAll = (selector) => nodes.filter((node) => node.id.startsWith("onprem-") && matches(node, selector));
  const document = {
    hidden: false, body: new Element("body"),
    getElementById: (id) => byId.get(id),
    querySelector: (selector) => nodes.find((node) => matches(node, selector)),
    querySelectorAll: (selector) => nodes.filter((node) => matches(node, selector)),
    createElement: (tag) => new Element(tag), addEventListener() {},
  };
  return {nodes, byId, panel, document};
}
const {nodes, byId, panel, document} = createDocument();
const config = {
  source_org_id: "123", source_client_id: "client-id", source_email: "source@example.test",
  source_path: "/", destination_url: "https://disk.example.test", destination_identity: "recipient@example.test",
  destination_type: "shared", destination_vd_hash: "shared-target-hash", destination_base: "/",
  destination_root: "disk:/source@example.test", strategy: "staged", identity_confirmed: true,
};
const job = {id: "job-1", created_at: "2026-10-11T12:34:56.123456+00:00", state: "queued", revision: 1, config, counts: {planned: 1}, directory_counts: {},
  total_files: 1, total_directories: 0, total_bytes: 18, verified_bytes: 0, remote_root: "disk:/.transfer/job-1"};
const first = {id: "entry-1", kind: "file", source_path: "/<unsafe>.txt", destination_path: "disk:/copy/<unsafe>.txt",
  size: 18, md5: "0123456789abcdef0123456789abcdef", state: "planned", attempt_id: "attempt-1"};
let allEntries = [first];
let allJobs = [job];
let operationEvents = [];
const entrySets = new Map();
let apiError = null;
let apiOverride = null;
let preflightDestination = {type: "shared", vd_hash: "shared-target-hash", name: "Архив сотрудников",
  permissions: ["read", "write"], free_bytes: 10240, identity: config.destination_identity,
  path: "vd:shared-target-hash:disk:/source@example.test"};
const restartedJobs = new Map();
const requests = [];
const clone = (value) => JSON.parse(JSON.stringify(value));
const fetch = async (path, options = {}) => {
  const url = new URL(path, "http://localhost");
  const method = options.method || "GET";
  const body = options.body ? JSON.parse(options.body) : null;
  requests.push({path: url.pathname, params: url.searchParams, method, body, headers: options.headers});
  if (apiOverride) {
    const response = await apiOverride({url, method, body});
    if (response) return response;
  }
  const activeJob = allJobs.find((value) => value.id === url.pathname.split("/")[4]) || job;
  let value;
  if (apiError) return {ok: false, status: 403, json: async () => ({detail: apiError})};
  if (options.headers["X-Onprem-Key"] !== "test-key") return {ok: false, status: 403, json: async () => ({detail: "onprem_access_required"})};
  if (url.pathname === "/api/onprem/preflight") value = {source: {path: "/", type: "dir"}, destination: preflightDestination};
  else if (url.pathname === "/api/onprem/jobs") {
    const offset = Number(url.searchParams.get("offset") || 0);
    const limit = Number(url.searchParams.get("limit") || 50);
    value = method === "POST" ? job : {jobs: allJobs.slice(offset, offset + limit)};
  }
  else if (url.pathname.endsWith("/entries")) {
    const offset = Number(url.searchParams.get("offset") || 0);
    const limit = Number(url.searchParams.get("limit") || 200);
    const state = url.searchParams.get("state");
    const available = entrySets.get(activeJob.id) || allEntries;
    value = {entries: available.filter((entry) => !state || entry.state === state).slice(offset, offset + limit)};
  } else if (url.pathname.endsWith("/events")) value = {events: [{sequence: 1, created_at: "2026-10-11T12:00:00Z", type: "job_state", payload: {state: activeJob.state, token: "fake-private-token"}}, ...operationEvents]};
  else if (url.pathname.endsWith("/start")) {job.state = "running"; job.revision += 1; value = job;}
  else if (url.pathname.endsWith("/pause")) {job.state = "paused"; job.revision += 1; value = job;}
  else if (url.pathname.endsWith("/resume")) {activeJob.state = activeJob.resume_state === "ready" ? "ready" : "running"; activeJob.resume_state = null; activeJob.revision += 1; value = activeJob;}
  else if (url.pathname.endsWith("/retry")) {job.state = "running"; first.state = "transferring"; value = job;}
  else if (url.pathname.endsWith("/stop")) {
    activeJob.stop_requested = true; activeJob.full_stop_requested = true;
    activeJob.revision += 1; value = activeJob;
  }
  else if (url.pathname.endsWith("/restart")) {
    const key = activeJob.id + ":" + body.idempotency_key;
    if (!restartedJobs.has(key)) {
      const child = {...clone(activeJob), id: `a${String(restartedJobs.size + 1).padStart(31, "0")}`,
        state: "queued", stop_requested: false, full_stop_requested: false, stop_blocked: false, restart_of: activeJob.id,
        revision: 1, counts: {}, total_files: 0, total_bytes: 0, verified_bytes: 0,
        remote_root: null, error: null, config: {...clone(body.config), destination_base: "/", source_public: true, destination_root: "vd:shared-target-hash:disk:/source@example.test"}};
      restartedJobs.set(key, child); allJobs.unshift(child); entrySets.set(child.id, []);
    }
    value = restartedJobs.get(key);
  }
  else if (url.pathname === "/api/onprem/cache/cleanup") value = {removed: 1, removed_bytes: 2048};
  else value = activeJob;
  return {ok: true, status: method === "POST" && url.pathname === "/api/onprem/jobs" ? 201 : 200,
    json: async () => clone(value), blob: async () => new Blob(["source_path,destination_path,result\n"])};
};
const timers = [];
const storageWrites = [];
const storage = {setItem(key, value) {storageWrites.push({key, value});}, getItem() {return null;}, removeItem() {}, clear() {}};
const confirmations = [];
let confirmStop = true;
const window = {localStorage: storage, sessionStorage: storage, diskCopyConfigPromise: Promise.resolve({onprem_enabled: true, onprem_auth_required: true}), addEventListener() {},
  confirm(message) {confirmations.push(message); return confirmStop;}};
vm.runInNewContext(source, {document, window, fetch, URL, Blob, crypto, Uint8Array, console,
  localStorage: storage, sessionStorage: storage,
  setTimeout: (callback) => {timers.push(callback); return timers.length;}, clearTimeout() {}});

async function flush() {for (let count = 0; count < 5; count += 1) await new Promise((resolve) => setImmediate(resolve));}
async function fire(nodeOrId, event) {
  const node = typeof nodeOrId === "string" ? byId.get(nodeOrId) : nodeOrId;
  for (const handler of node.listeners[event] || []) await handler({preventDefault() {}});
  await flush();
}
const field = (key) => nodes.find((node) => node.dataset.onpremConfig === key);

async function verifyReleaseGate() {
  function loadGate(configPromise, {missingConfig = false} = {}) {
    const ui = createDocument();
    const requests = [];
    const tab = ui.document.querySelector('.mode-tab[data-mode="onprem"]');
    const gateWindow = {addEventListener() {}};
    if (!missingConfig) gateWindow.diskCopyConfigPromise = configPromise;
    vm.runInNewContext(source, {
      document: ui.document, window: gateWindow, URL, Blob, crypto, Uint8Array, console,
      fetch: async (url) => { requests.push(url); throw new Error("No requests allowed before opt-in"); },
      setTimeout() {return 1;}, clearTimeout() {},
    });
    return {...ui, tab, requests};
  }
  function assertDisabled(ui) {
    assert.equal(ui.tab.hidden, false, "release tab is visible before configuration arrives");
    assert.equal(ui.tab.disabled, true);
    assert.equal(ui.tab.title, "SaaS → On-premises");
    assert.equal(ui.panel.hidden, true);
    assert.equal(ui.byId.get("onprem-cleanup").disabled, true);
    assert.equal(ui.requests.length, 0);
  }
  for (const config of [{}, {onprem_enabled: false}, null]) {
    const ui = loadGate(Promise.resolve(config));
    await flush();
    assertDisabled(ui);
    await ui.tab.click();
    await fire(ui.byId.get("onprem-cleanup"), "click");
    assert.equal(ui.requests.length, 0, "native disabled tabs cannot start polling");
  }
  const missing = loadGate(undefined, {missingConfig: true});
  await flush();
  assertDisabled(missing);
  let rejectConfig;
  const rejected = loadGate(new Promise((_, reject) => {rejectConfig = reject;}));
  rejectConfig(new Error("Configuration unavailable"));
  await flush();
  assertDisabled(rejected);
  let resolveConfig;
  const pilot = loadGate(new Promise((resolve) => {resolveConfig = resolve;}));
  assertDisabled(pilot);
  resolveConfig({onprem_enabled: true, onprem_auth_required: true});
  await flush();
  assert.equal(pilot.tab.hidden, false);
  assert.equal(pilot.tab.disabled, false, "enabled configuration enables the tab");
  assert.equal(pilot.tab.title, "SaaS → On-premises");
  assert.equal(pilot.panel.hidden, false);
  assert.equal(pilot.byId.get("onprem-access").hidden, false);
  assert.equal(pilot.requests.length, 0, "enabling a tab does not launch background work");
}

async function verifyGlobalCacheCleanup() {
  const ui = createDocument();
  const calls = [];
  const get = (id) => ui.byId.get(id);
  const globalJobs = [
    {...clone(job), id: "legacy", state: "completed", config: {...config, destination_type: "personal"}},
    {...clone(job), id: "stopped", state: "stopped", full_stop_requested: true},
    {...clone(job), id: "running", state: "running"},
  ];
  let cleanupResponse = {removed: 4, removed_bytes: 4096};
  let cleanupError = null;
  let networkFailure = false;
  let pendingCleanup = null;
  const respond = (value) => ({ok: true, status: 200, json: async () => clone(value)});
  vm.runInNewContext(source, {
    document: ui.document,
    window: {addEventListener() {}, confirm() {throw new Error("Global cache cleanup needs no confirmation");},
      diskCopyConfigPromise: Promise.resolve({onprem_enabled: true, onprem_auth_required: true})},
    fetch: async (address, options = {}) => {
      const url = new URL(address, "http://localhost");
      calls.push({path: url.pathname, method: options.method || "GET", body: options.body && JSON.parse(options.body), headers: clone(options.headers)});
      if (url.pathname === "/api/onprem/cache/cleanup") {
        if (networkFailure) throw new Error("private-network-details");
        if (pendingCleanup) await pendingCleanup;
        if (cleanupError) return {ok: false, status: cleanupError.status, json: async () => clone(cleanupError.body)};
        return respond(cleanupResponse);
      }
      if (url.pathname === "/api/onprem/jobs") return respond({jobs: []});
      if (url.pathname.endsWith("/entries")) return respond({entries: []});
      if (url.pathname.endsWith("/events")) return respond({events: []});
      return respond(globalJobs.find((value) => value.id === url.pathname.split("/")[4]));
    },
    URL, Blob, crypto, Uint8Array, console, setTimeout() {return 1;}, clearTimeout() {},
  });
  await flush();
  assert.equal(get("onprem-cleanup-panel").hidden, false, "the global panel is visible without a selected job");
  assert.equal(get("onprem-cleanup").disabled, true, "global cleanup still needs the application access key");
  await fire(get("onprem-cleanup"), "click");
  assert.equal(calls.length, 0, "manually dispatched unauthenticated cleanup does not send a request");
  get("onprem-access-key").value = "global-test-key";
  await fire(get("onprem-access-key"), "input");
  assert.equal(get("onprem-cleanup").disabled, false, "credentials, identity and a prepared job are not needed");
  const validationBefore = formValidationCalls;
  formValid = false;
  await fire(get("onprem-cleanup"), "click");
  formValid = true;
  assert.equal(formValidationCalls, validationBefore, "cleanup never validates the transfer form");
  assert.equal(calls.length, 1, "cleanup without jobs makes only its own request");
  assert.deepEqual(calls[0], {path: "/api/onprem/cache/cleanup", method: "POST", body: {}, headers: {
    Accept: "application/json", "X-Onprem-Key": "global-test-key", "Content-Type": "application/json",
  }});
  const firstResult = get("onprem-cleanup-result").textContent;
  assert.match(firstResult, /Удалено файлов: 4.*Объём: 4 Кб/);
  assert.equal(get("onprem-cleanup-result").attributes["aria-live"], "polite");
  assert.equal(get("onprem-remote-attempts"), undefined, "cleanup no longer displays job-specific remote attempts");
  assert.match(html, /<button id="onprem-cleanup"[^>]*>Очистить локальный кеш<\/button>/);
  assert.match(html, /#onprem-cleanup-panel\s*\{[^}]*text-align:\s*center/);
  assert.match(html, /#onprem-cleanup-panel \.onprem-actions\s*\{[^}]*justify-content:\s*center/);
  assert.match(html, /<section id="onprem-cleanup-panel"[\s\S]*?<\/section>\s*<\/div>\s*<section data-legacy-output>/,
    "the independent cache section is the last section in SaaS-Onprem");
  assert.ok(html.indexOf('<section id="onprem-cleanup-panel"') > html.indexOf('<section id="onprem-log-panel"'),
    "the cache button is below the job journal");
  assert.match(html, /Удаляет весь временный кеш всех заданий.*нужные файлы скачаются заново/);

  for (const selectedJob of globalJobs) {
    get("onprem-history").value = selectedJob.id;
    await fire(get("onprem-history"), "change");
    assert.equal(get("onprem-cleanup-result").textContent, firstResult, "changing jobs preserves the global cleanup result");
    for (const node of ui.nodes.filter((node) => node.dataset.onpremConfig)) node.value = "";
    get("onprem-source-secret").value = "";
    get("onprem-destination-token").value = "";
    get("onprem-identity-confirmed").checked = false;
    await fire(get("onprem-identity-confirmed"), "change");
    assert.equal(get("onprem-cleanup").disabled, false, `${selectedJob.id} state never gates global cleanup`);
    const previousCount = calls.length;
    await fire(get("onprem-cleanup"), "click");
    assert.deepEqual(calls.slice(previousCount).map(({path, method, body}) => ({path, method, body})),
      [{path: "/api/onprem/cache/cleanup", method: "POST", body: {}}],
      "cleanup never attaches credentials, mutates the selection or reads remote attempts");
  }
  get("onprem-history").value = "";
  await fire(get("onprem-history"), "change");
  assert.equal(get("onprem-cleanup-result").textContent, firstResult, "clearing the selection preserves the result");
  assert.equal(get("onprem-cleanup-panel").hidden, false);
  cleanupResponse = {removed: 0, removed_bytes: 0};
  await fire(get("onprem-cleanup"), "click");
  assert.match(get("onprem-cleanup-result").textContent, /Удалено файлов: 0.*Объём: 0 Б/);

  for (const [code, status, expected] of [
    ["cache_in_use", 409, /Кеш используется.*Пауза.*Приостановлено.*полностью остановите/],
    ["cache_cleanup_failed", 500, /Не удалось полностью очистить локальный кеш.*права доступа/],
    ["onprem_access_required", 403, /Нужен действующий ключ доступа/],
  ]) {
    cleanupError = {status, body: {detail: code, diagnostic_message: "private-server-detail"}};
    await fire(get("onprem-cleanup"), "click");
    assert.match(get("onprem-cleanup-result").textContent, expected);
    assert.doesNotMatch(get("onprem-cleanup-result").textContent, /private-server-detail/,
      "cache errors never include raw diagnostics");
    assert.match(get("onprem-cleanup-result").className, /bad/);
    assert.equal(get("onprem-cleanup").disabled, false, "a rejected cleanup can be retried");
  }
  cleanupError = null;
  networkFailure = true;
  await fire(get("onprem-cleanup"), "click");
  assert.match(get("onprem-cleanup-result").textContent, /Соединение с утилитой прервалось/);
  assert.doesNotMatch(get("onprem-cleanup-result").textContent, /private-network-details/);
  networkFailure = false;
  cleanupResponse = {removed: 1, removed_bytes: "private-invalid-result"};
  await fire(get("onprem-cleanup"), "click");
  assert.match(get("onprem-cleanup-result").textContent, /Не удалось прочитать результат очистки локального кеша/);
  assert.doesNotMatch(get("onprem-cleanup-result").textContent, /private-invalid-result/);
  cleanupResponse = {removed: 0, removed_bytes: 0};
  let releaseCleanup;
  pendingCleanup = new Promise((resolve) => {releaseCleanup = resolve;});
  const previousCount = calls.length;
  const cleanup = fire(get("onprem-cleanup"), "click");
  await flush();
  assert.equal(get("onprem-cleanup").disabled, true, "a cleanup in progress locks the button");
  await fire(get("onprem-cleanup"), "click");
  assert.equal(calls.length, previousCount + 1, "a second click cannot start concurrent cleanup");
  releaseCleanup();
  await cleanup;
  assert.equal(get("onprem-cleanup").disabled, false);
  assert.equal(get("onprem-cleanup-result").className, "field-note", "successful retry clears the error style");
}

async function verifyStagedOnly() {
  const ui = createDocument();
  const calls = [];
  let current = null;
  let errorCode = null;
  const response = (value, status = 200) => ({ok: status < 400, status, json: async () => clone(value), blob: async () => new Blob(["report"])});
  const fakeFetch = async (address, options = {}) => {
    const url = new URL(address, "http://localhost");
    const method = options.method || "GET";
    const body = options.body ? JSON.parse(options.body) : null;
    calls.push({method, path: url.pathname, body});
    if (url.pathname === "/api/onprem/preflight") return errorCode
      ? response({detail: errorCode}, 409) : response({destination: preflightDestination});
    if (url.pathname === "/api/onprem/jobs") {
      if (method === "POST") current = {...clone(job), state: "ready", config: {...body.config, destination_root: config.destination_root}};
      return response(method === "POST" ? current : {jobs: current ? [current] : []});
    }
    if (url.pathname.endsWith("/entries")) return response({entries: [{...clone(first), state: "planned"}]});
    if (url.pathname.endsWith("/events")) return response({events: [{sequence: 1, type: "transfer_stage", payload: {stage: "poll_transfer", status: "waiting"}, context: {kind: "file", strategy: "url"}}]});
    return response(current);
  };
  vm.runInNewContext(source, {document: ui.document, window: {addEventListener() {}, confirm() {return true;},
    diskCopyConfigPromise: Promise.resolve({onprem_enabled: true, onprem_auth_required: true})},
    fetch: fakeFetch, URL, Blob, crypto, Uint8Array, console, setTimeout() {return 1;}, clearTimeout() {}});
  await flush();
  const get = (id) => ui.byId.get(id);
  const strategy = get("onprem-strategy");
  assert.equal(strategy.tagName, "INPUT", "the transfer route is a fixed field, not a strategy selector");
  assert.ok(Object.hasOwn(strategy.attributes, "readonly"));
  assert.equal(strategy.value, "staged");
  assert.doesNotMatch(html, /<option[^>]*value="url"/);
  get("onprem-access-key").value = "test-key";
  const tab = ui.document.querySelector('.mode-tab[data-mode="onprem"]');
  tab.classList.add("active");
  await fire(tab, "click");
  for (const node of ui.nodes.filter((node) => node.dataset.onpremConfig)) node.value = config[node.dataset.onpremConfig] || "";
  get("onprem-source-secret").value = "test-source-secret";
  get("onprem-destination-token").value = "test-destination-token";
  get("onprem-identity-confirmed").checked = true;
  await fire(get("onprem-identity-confirmed"), "change");
  strategy.value = "url";
  await fire(strategy, "change");
  await fire(get("onprem-form"), "submit");
  const writes = calls.filter((call) => call.method !== "GET");
  assert.equal(writes.find((call) => call.path.endsWith("/preflight")).body.config.strategy, "staged", "DOM tampering cannot change preflight strategy");
  assert.equal(writes.find((call) => call.path === "/api/onprem/jobs").body.config.strategy, "staged", "DOM tampering cannot change a new single-source plan");
  current.config.strategy = "url";
  get("onprem-history").value = current.id;
  await fire(get("onprem-history"), "change");
  assert.equal(strategy.value, "staged", "restoring old URL history keeps the new form on staged");
  assert.match(get("onprem-legacy-note").textContent, /нов.*Dry Run/i);
  assert.match(get("onprem-log").textContent, /Прежний способ переноса/);
  assert.doesNotMatch(get("onprem-log").textContent, /Напрямую по ссылке/);
  get("onprem-source-secret").value = "test-source-secret";
  get("onprem-destination-token").value = "test-destination-token";
  get("onprem-identity-confirmed").checked = true;
  await fire(get("onprem-identity-confirmed"), "change");
  assert.equal(get("onprem-start").disabled, true, "old URL plans require a fresh Dry Run");
  assert.equal(get("onprem-stop").disabled, false, "historical URL plans remain stoppable");
  assert.equal(get("onprem-report").disabled, false, "historical URL plans remain exportable");
  current.state = "paused";
  await fire(get("onprem-refresh"), "click");
  assert.equal(get("onprem-resume").disabled, true, "old URL plans cannot be resumed");
  await fire(get("onprem-form"), "submit");
  assert.equal(calls.filter((call) => call.path === "/api/onprem/jobs" && call.method === "POST").at(-1).body.config.strategy, "staged", "a new Dry Run from URL history uses staged");
  errorCode = "onprem_staged_required";
  await fire(get("onprem-form"), "submit");
  assert.match(get("onprem-notice").textContent, /нов.*Dry Run/i, "the backend staged-only error gives an actionable next step");
}

async function verifyPublicSourceConfiguration() {
  const ui = createDocument();
  const calls = [];
  let current = null;
  let rejectCreates = false;
  let rejectPreflight = false;
  let preflightFailure = {detail: "destination_auth_failed"};
  const response = (value) => ({ok: true, status: 200, json: async () => clone(value)});
  const fakeFetch = async (address, options = {}) => {
    const url = new URL(address, "http://localhost");
    const method = options.method || "GET";
    const body = options.body ? JSON.parse(options.body) : null;
    calls.push({method, path: url.pathname, body});
    if (url.pathname === "/api/onprem/preflight") return rejectPreflight
      ? {ok: false, status: 409, json: async () => clone(preflightFailure)}
      : response({destination: preflightDestination});
    if (url.pathname === "/api/onprem/jobs") {
      if (method === "POST" && rejectCreates) throw new Error("simulated create response interruption");
      if (method === "POST") current = {...clone(job), state: "ready", config: {...body.config, destination_root: config.destination_root}};
      return response(method === "POST" ? current : {jobs: current ? [current] : []});
    }
    if (url.pathname.endsWith("/entries")) return response({entries: [{...clone(first), state: "planned"}]});
    if (url.pathname.endsWith("/events")) return response({events: []});
    return response(current);
  };
  vm.runInNewContext(source, {document: ui.document, window: {addEventListener() {}, confirm() {return true;},
    diskCopyConfigPromise: Promise.resolve({onprem_enabled: true, onprem_auth_required: true})},
    fetch: fakeFetch, URL, Blob, crypto, Uint8Array, console, setTimeout() {return 1;}, clearTimeout() {}});
  await flush();
  const get = (id) => ui.byId.get(id);
  assert.match(html, /<button id="onprem-plan"[^>]*>Dry Run<\/button>/);
  assert.equal(get("onprem-start").disabled, true, "Start needs a successfully prepared plan");
  assert.equal(get("onprem-source-public"), undefined, "publication is no longer an editable or hidden input");
  assert.equal(get("onprem-source-admin-token").tagName, "INPUT");
  assert.equal(get("onprem-source-admin-token").attributes.required, undefined, "administrator Directory checks are optional");
  assert.equal(get("onprem-source-admin-token").dataset.onpremConfig, undefined, "administrator access never becomes persisted plan config");
  assert.equal(get("onprem-job-source-mode"), undefined, "the job summary omits the source-access row");
  assert.equal(get("onprem-destination-base"), undefined, "new transfers have no base-folder input");
  assert.doesNotMatch(html, /data-onprem-config="destination_base"|Папка внутри общего диска/);
  assert.match(html, /Ссылки остаются публичными после завершения и остановки/);
  const strategy = get("onprem-strategy");
  assert.equal(strategy.value, "staged", "a fresh form defaults to the local cache route");
  assert.match(html, /С временным сохранением в кэш/);
  get("onprem-access-key").value = "test-key";
  const tab = ui.document.querySelector('.mode-tab[data-mode="onprem"]');
  tab.classList.add("active");
  await fire(tab, "click");
  for (const node of ui.nodes.filter((node) => node.dataset.onpremConfig)) node.value = config[node.dataset.onpremConfig] || "";
  get("onprem-source-secret").value = "fake-secret";
  get("onprem-destination-token").value = "fake-token";
  get("onprem-identity-confirmed").checked = true;
  await fire(get("onprem-identity-confirmed"), "change");
  strategy.value = "staged";
  await fire(strategy, "change");
  assert.equal(calls.filter((call) => call.method !== "GET").length, 0, "selecting a strategy never publishes or starts a job");
  rejectPreflight = true;
  await fire(get("onprem-form"), "submit");
  assert.equal(calls.filter((call) => call.path === "/api/onprem/jobs" && call.method === "POST").length, 0,
    "Dry Run stops before preparing a plan when the automatic disk check fails");
  assert.equal(get("onprem-start").disabled, true, "a rejected Dry Run never enables Start");
  assert.match(get("onprem-notice").textContent, /Не удалось авторизоваться/,
    "legacy errors without diagnostics keep their existing message");
  preflightFailure = {detail: "destination_connection_failed",
    diagnostic_message: "On-premises. Соединение закрыто до получения HTTP-ответа. Сервер: disk.example.",
    message: "raw-secret", description: "raw-secret"};
  await fire(get("onprem-form"), "submit");
  assert.match(get("onprem-notice").textContent, /HTTP-ответа.*Сервер: disk\.example/,
    "preflight errors retain a safe concrete explanation");
  assert.doesNotMatch(get("onprem-notice").textContent, /HTTP 409|raw-secret/,
    "the local API status is not presented as a remote HTTP error");
  for (const unsafe of ["https://disk.example/?token=private-secret", "OAuth private-secret", "<b>private-secret</b>",
    "credential=private-secret", "private-secret\n", "x".repeat(1201)]) {
    preflightFailure.diagnostic_message = unsafe;
    await fire(get("onprem-form"), "submit");
    assert.doesNotMatch(get("onprem-notice").textContent, /private-secret|x{1201}/);
  }
  preflightFailure = {detail: "unknown-server-code", diagnostic_message: "unknown-private-secret"};
  await fire(get("onprem-form"), "submit");
  assert.doesNotMatch(get("onprem-notice").textContent, /unknown-private-secret/,
    "an unknown response code cannot introduce arbitrary diagnostic text");
  get("onprem-source-admin-token").value = "typed-admin-access";
  for (const [detail, expected] of [
    ["source_admin_auth_failed", /админский токен.*directory:read_users/],
    ["source_user_not_in_organization", /Email.*ORGID/],
    ["source_user_blocked", /заблокирован.*Разблокируйте/],
    ["source_admin_check_failed", /API 360/],
    ["source_admin_invalid_response", /API 360.*некорректный ответ/],
  ]) {
    preflightFailure = {detail, diagnostic_message: "API 360 typed-admin-access"};
    await fire(get("onprem-form"), "submit");
    assert.match(get("onprem-notice").textContent, expected);
    assert.doesNotMatch(get("onprem-notice").textContent, /typed-admin-access/);
  }
  get("onprem-source-admin-token").value = "";
  assert.equal(calls.filter((call) => call.path === "/api/onprem/jobs" && call.method === "POST").length, 0);
  assert.equal(get("onprem-start").disabled, true);
  const failedDryRunCalls = calls.length;
  await fire(get("onprem-start"), "click");
  assert.equal(calls.length, failedDryRunCalls, "dispatching locked Start does not attach access or run transfer");
  rejectPreflight = false;
  const beforeDryRun = calls.length;
  await fire(get("onprem-form"), "submit");
  assert.deepEqual(calls.slice(beforeDryRun).filter((call) => call.method !== "GET").map((call) => call.path),
    ["/api/onprem/preflight", "/api/onprem/jobs"], "Dry Run automatically checks the disk before preparing a plan");
  const publicCreate = calls.filter((call) => call.path === "/api/onprem/jobs" && call.method === "POST").at(-1);
  assert.equal(publicCreate.body.config.source_public, true, "new public staged plans retain the explicit flag");
  assert.equal(publicCreate.body.credentials.source_admin_token, undefined, "empty optional admin field is omitted");
  assert.equal(get("onprem-start").disabled, false);
  assert.equal(publicCreate.body.config.destination_base, "/");
  const expectedDate = new Date(current.created_at).toLocaleString("ru-RU", {
    year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
  assert.equal(get("onprem-job-id").textContent, `от ${expectedDate}`,
    "the heading identifies the job by creation time in the browser time zone");
  const historyLabel = get("onprem-history").children.find((option) => option.value === current.id).textContent;
  assert.ok(historyLabel.startsWith(`Задание от ${expectedDate} · ${current.config.source_email} · `));
  assert.equal(historyLabel.includes(current.id), false, "the history keeps UUID only in option values");
  current.updated_at = "2027-02-15T18:22:05Z";
  await fire(get("onprem-refresh"), "click");
  assert.equal(get("onprem-job-id").textContent, `от ${expectedDate}`,
    "later progress updates do not rename the job");
  const createdAt = current.created_at;
  for (const missingDate of [undefined, null, "", "not-a-date"]) {
    current.created_at = missingDate;
    await fire(get("onprem-refresh"), "click");
    assert.equal(get("onprem-job-id").textContent, "без даты");
    assert.ok(get("onprem-history").children.find((option) => option.value === current.id)
      .textContent.startsWith("Задание без даты · "), "legacy missing dates have a readable fallback");
    assert.equal(get("onprem-history").value, current.id, "a missing date never changes selection identity");
  }
  current.created_at = createdAt;
  await fire(get("onprem-refresh"), "click");
  const originalEmail = get("onprem-source-email").value;
  get("onprem-source-email").value = "changed@example.test";
  await fire(get("onprem-source-email"), "input");
  assert.equal(get("onprem-start").disabled, true, "editable source identity remains part of the immutable job binding");
  const beforeStart = calls.length;
  await fire(get("onprem-start"), "click");
  assert.equal(calls.length, beforeStart, "changed source cannot attach access or start the selected plan");
  get("onprem-source-email").value = originalEmail;
  await fire(get("onprem-source-email"), "input");
  assert.equal(get("onprem-start").disabled, false);

  for (const originalStrategy of ["url", "staged"]) {
    current.config = {...clone(config), strategy: originalStrategy, destination_base: "/Old archive",
      destination_root: "vd:shared-target-hash:disk:/Old archive/source@example.test"};
    if (originalStrategy === "staged") current.config.source_public = false;
    get("onprem-history").value = current.id;
    await fire(get("onprem-history"), "change");
    assert.equal(strategy.value, "staged", "history cannot restore the retired transfer route into the new form");
    assert.match(get("onprem-job-destination").textContent, /Old archive/,
      "the selected job shows its actual historical destination separately from a new plan preview");
    get("onprem-source-secret").value = "fake-restored-secret";
    get("onprem-source-admin-token").value = "fake-restored-admin";
    get("onprem-destination-token").value = "fake-restored-token";
    get("onprem-identity-confirmed").checked = true;
    await fire(get("onprem-identity-confirmed"), "change");
    const legacyUrl = originalStrategy === "url";
    assert.equal(get("onprem-start").disabled, legacyUrl, "URL history requires a new Dry Run");
    const retainedConfig = clone(current.config);
    const beforeAccess = calls.filter((call) => call.method !== "GET").length;
    await fire(get("onprem-start"), "click");
    if (legacyUrl) {
      assert.equal(calls.filter((call) => call.method !== "GET").length, beforeAccess, "old URL history cannot attach access or start");
    } else {
      const attached = calls.filter((call) => call.path.endsWith("/credentials")).at(-1);
      assert.deepEqual(attached.body.config, retainedConfig,
        "access restoration preserves historical private/non-root staged config exactly");
      assert.equal(attached.body.credentials.source_admin_token, "fake-restored-admin", "administrator access is attached only as a secret");
      assert.equal(attached.body.config.source_admin_token, undefined);
    }
    current.state = "paused";
    await fire(get("onprem-refresh"), "click");
    assert.equal(get("onprem-resume").disabled, legacyUrl);
    await fire(get("onprem-resume"), "click");
    if (!legacyUrl) assert.deepEqual(calls.filter((call) => call.path.endsWith("/credentials")).at(-1).body.config, retainedConfig);
    await fire(get("onprem-form"), "submit");
    const fresh = calls.filter((call) => call.path === "/api/onprem/jobs" && call.method === "POST").at(-1);
    assert.equal(fresh.body.config.source_public, true, "a new Dry Run never inherits private downloading");
    assert.equal(fresh.body.config.destination_base, "/", "a new Dry Run never inherits the archived base folder");
    assert.equal(fresh.body.config.strategy, "staged", "a new Dry Run always uses local staging");
  }
  rejectCreates = true;
  strategy.value = "staged";
  await fire(strategy, "change");
  await fire(get("onprem-form"), "submit");
  const publicKey = calls.filter((call) => call.path === "/api/onprem/jobs" && call.method === "POST").at(-1).body.idempotency_key;
  await fire(get("onprem-form"), "submit");
  assert.equal(calls.filter((call) => call.path === "/api/onprem/jobs" && call.method === "POST").at(-1).body.idempotency_key, publicKey,
    "uncertain create retains its idempotency key for unchanged public-source binding");
  strategy.value = "url";
  await fire(strategy, "change");
  await fire(get("onprem-form"), "submit");
  assert.equal(calls.filter((call) => call.path === "/api/onprem/jobs" && call.method === "POST").at(-1).body.idempotency_key, publicKey,
    "DOM strategy tampering cannot change a staged creation fingerprint");
}

async function verifyPointRetry() {
  assert.doesNotMatch(html, /onprem-resolve|id="onprem-point-retry"|onprem-remote-root/,
    "manual resolution panel, standalone retry and technical-directory controls are removed");
  assert.doesNotMatch(source, /onprem-resolve|selectedUnknownEntry|clearResolution|renderResolve|resolutionValid|state=outcome_unknown/,
    "manual handlers and off-page resolution queries are removed, not merely hidden");
  const rowButtons = (ui) => ui.get("onprem-entry-body").children.flatMap((row) => row.children.at(-1).children)
    .filter((node) => node.tagName === "BUTTON");
  async function load({entryState = "failed", state = "needs_attention", canRetry = true,
    attemptId = "chosen-attempt", staleAttempt = false, staleEligibility = false, rejectAccess = false,
    delayEntry = false, pointActive = false, offPage = false, legacy = false, fullStop = false,
    stopBlocked = false, historical = false, rejectRetry = false} = {}) {
    const ui = createDocument();
    const calls = [];
    const current = {...clone(job), state, config: {...clone(config), strategy: "staged"},
      revision: 11, point_retry_active: pointActive, counts: {[entryState]: 2}, total_files: offPage ? 101 : 2,
      remote_root: null, stop_blocked: stopBlocked, full_stop_requested: fullStop,
      remote_pending: stopBlocked};
    if (legacy) {delete current.config.destination_type; delete current.config.destination_vd_hash;}
    if (historical) current.config = {...current.config, destination_base: "/Historical archive", source_public: false,
      destination_root: "vd:shared-target-hash:disk:/Historical archive/source@example.test"};
    const target = {...clone(first), id: "chosen-file", attempt_id: attemptId, state: entryState,
      error: "destination_timeout", point_retry_requested: pointActive, point_retry_attempts: pointActive ? 1 : 0,
      can_retry: canRetry};
    const other = {...clone(target), id: "other-file", attempt_id: "other-attempt", source_path: "/other-file",
      point_retry_requested: false};
    let releaseEntry;
    let credentialsAttached = false;
    const response = (value, status = 200) => ({ok: status < 400, status, json: async () => clone(value)});
    const fakeFetch = async (address, options = {}) => {
      const url = new URL(address, "http://localhost");
      const method = options.method || "GET";
      const body = options.body ? JSON.parse(options.body) : null;
      calls.push({method, path: url.pathname, params: url.searchParams, body});
      if (url.pathname === "/api/onprem/jobs") return response({jobs: [current]});
      if (url.pathname.endsWith("/entries")) return response({entries: offPage
        ? Number(url.searchParams.get("offset")) ? [target] : Array.from({length: 100}, (_, i) => ({...other, id: `other-${i}`}))
        : [other, target]});
      if (url.pathname.endsWith("/entries/chosen-file")) {
        if (delayEntry && credentialsAttached) return new Promise((resolve) => {releaseEntry = () => resolve(response(target));});
        return response(target);
      }
      if (url.pathname.endsWith("/events")) return response({events: [{sequence: 1, type: "transfer_stage",
        created_at: "2026-10-12T12:00:00Z", payload: {stage: "point_retry", status: "waiting",
          error: "destination_timeout", point_retry_attempts: 2, max_point_retry_attempts: 3, retry_delay_ms: 30000}},
        {sequence: 2, type: "transfer_stage", created_at: "2026-10-12T12:00:01Z",
          payload: {stage: "upload_file", status: "outcome_unknown", error: "destination_connection_failed"},
          error_message: "Bearer fake-sensitive-error"},
        {sequence: 3, type: "transfer_stage", created_at: "2026-10-12T12:00:02Z",
          payload: {stage: "publish_source", status: "outcome_unknown", error: "source_timeout"}},
        {sequence: 4, type: "transfer_stage", created_at: "2026-10-12T12:00:03Z",
          payload: {stage: "check_destination_access", status: "failed", error: "destination_connection_failed"}},
        {sequence: 5, type: "attempt_resolved", created_at: "2026-10-12T12:00:04Z", payload: {}},
        {sequence: 6, type: "administrative_resolution", created_at: "2026-10-12T12:00:05Z", payload: {}},
        {sequence: 7, type: "transfer_stage", created_at: "2026-10-12T12:00:06Z",
          payload: {stage: "abandon_attempt", status: "succeeded", phase: "abandoned_transfer"}},
        {sequence: 8, type: "attempt_updated", created_at: "2026-10-12T12:00:07Z",
          payload: {phase: "abandoned_move"}}]});
      if (url.pathname.endsWith("/credentials")) {
        if (rejectAccess) return response({detail: "destination_auth_failed"}, 403);
        current.revision += 1;
        credentialsAttached = true;
        if (staleAttempt) target.attempt_id = "replaced-attempt";
        if (staleEligibility) target.can_retry = false;
      }
      if (url.pathname.endsWith("/stop")) {
        current.full_stop_requested = current.stop_requested = true;
        if (current.state === "stopped") current.stop_blocked = false;
      }
      if (url.pathname.endsWith("/resume")) current.state = "running";
      if (url.pathname.endsWith("/entries/chosen-file/retry")) {
        if (rejectRetry) return response({detail: "stale_revision"}, 409);
        target.point_retry_requested = true;
        target.point_retry_attempts = 1;
        target.can_retry = false;
        current.point_retry_active = true;
        current.state = "running";
      }
      return response(current);
    };
    vm.runInNewContext(source, {document: ui.document, window: {addEventListener() {}, confirm() {return true;},
      diskCopyConfigPromise: Promise.resolve({onprem_enabled: true, onprem_auth_required: true})},
      fetch: fakeFetch, URL, Blob, crypto, Uint8Array, console, setTimeout() {return 1;}, clearTimeout() {}});
    await flush();
    const get = (id) => ui.byId.get(id);
    get("onprem-access-key").value = "test-key";
    const tab = ui.document.querySelector('.mode-tab[data-mode="onprem"]');
    tab.classList.add("active");
    await fire(tab, "click");
    get("onprem-history").value = current.id;
    await fire(get("onprem-history"), "change");
    get("onprem-source-secret").value = "fake-secret";
    get("onprem-destination-token").value = "fake-token";
    get("onprem-identity-confirmed").checked = true;
    await fire(get("onprem-identity-confirmed"), "change");
    return {...ui, get, calls, current, target, other, releaseEntry: () => releaseEntry()};
  }
  for (const options of [{}, {entryState: "outcome_unknown", historical: true}, {attemptId: null}]) {
    const ui = await load(options);
    const buttons = rowButtons(ui);
    assert.equal(buttons.length, 2, "eligible failed and historical unknown files expose only per-row retry");
    assert.ok(buttons.every((button) => button.textContent === "Проверить и повторить" && !button.disabled));
    await fire(buttons[1], "click");
    const writes = ui.calls.filter((call) => call.method !== "GET");
    assert.deepEqual(writes.map((call) => [call.method, call.path.split("/").at(-1)]), [["PUT", "credentials"], ["POST", "retry"]]);
    assert.deepEqual(writes[0].body.config, ui.current.config, "access attaches to the original saved configuration");
    assert.equal(writes[0].body.point_retry_entry_id, "chosen-file",
      "cached retry requests destination-only access validation for the chosen file");
    assert.equal(writes[1].path, "/api/onprem/jobs/job-1/entries/chosen-file/retry");
    assert.deepEqual(writes[1].body, {attempt_id: options.attemptId === null ? null : "chosen-attempt", revision: 12},
      "retry carries current revision and nullable attempt CAS values");
    assert.ok(ui.calls.some((call) => call.path === "/api/onprem/jobs/job-1/entries/chosen-file" && call.method === "GET"),
      "the chosen entry is refreshed through its exact endpoint before retry");
    assert.equal(ui.calls.some((call) => call.params.has("state")), false, "no hidden off-page unknown/recovery query remains");
    assert.equal(ui.other.point_retry_requested, false);
    assert.ok(rowButtons(ui).every((button) => button.disabled), "one retry locks other file controls");
    assert.match(ui.get("onprem-entry-body").textContent, /Точечный повтор: 1\/3/);
    assert.match(ui.get("onprem-notice").textContent, /до трёх попыток/);
    assert.doesNotMatch(ui.get("onprem-notice").textContent, /подтверждённой ошибки|журналам администратора|терминальн/);
    assert.match(ui.get("onprem-log").textContent, /Проверка и повтор файла.*время ожидания.*On-premises.*2\/3.*30 с/);
    assert.match(ui.get("onprem-log").textContent, /Результат публикации в SaaS не подтверждён.*время ожидания.*SaaS/);
    assert.doesNotMatch(ui.get("onprem-log").textContent, /fake-sensitive-error/);
    assert.match(ui.get("onprem-log").textContent, /Проверка доступности выбранного диска On-premises.*Не удалось установить соединение/);
    assert.match(ui.get("onprem-log").textContent, /Исход попытки установлен/,
      "historical resolution events retain their audit labels without action controls");
    assert.match(ui.get("onprem-log").textContent, /Администратор установил исход операции/);
    assert.match(ui.get("onprem-log").textContent, /Локальная попытка прервана; API не поддерживает отмену удалённой операции/);
    assert.match(ui.get("onprem-log").textContent, /Ожидание предыдущей загрузки прекращено/);
    assert.match(ui.get("onprem-log").textContent, /Ожидание предыдущего перемещения прекращено/);
    const count = writes.length;
    await fire(buttons[1], "click");
    assert.equal(ui.calls.filter((call) => call.method !== "GET").length, count, "a detached button cannot queue a duplicate retry");
  }
  for (const canRetry of [false, undefined, null, 1, "true"]) {
    const ui = await load({canRetry});
    if (canRetry === undefined) {delete ui.target.can_retry; delete ui.other.can_retry; await fire(ui.get("onprem-refresh"), "click");}
    assert.equal(rowButtons(ui).length, 0, "the server must explicitly permit retry");
  }
  const paged = await load({offPage: true});
  assert.equal(rowButtons(paged).length, 100);
  assert.ok(paged.get("onprem-entry-body").children.every((row) => row.children[0].textContent === "/other-file"));
  await fire(paged.get("onprem-entry-next"), "click");
  assert.equal(rowButtons(paged).length, 1);
  await fire(rowButtons(paged)[0], "click");
  assert.equal(paged.calls.filter((call) => call.method === "POST").at(-1).path,
    "/api/onprem/jobs/job-1/entries/chosen-file/retry", "a file on a later page is independently retryable");
  for (const options of [{staleAttempt: true}, {staleEligibility: true}, {rejectAccess: true}]) {
    const ui = await load(options);
    await fire(rowButtons(ui)[1], "click");
    assert.equal(ui.calls.some((call) => call.method === "POST"), false, "changed entry or rejected credentials prevent retry");
  }
  for (const change of ["credentials", "selection"]) {
    const ui = await load({delayEntry: true});
    await fire(rowButtons(ui)[1], "click");
    if (change === "credentials") {
      ui.get("onprem-destination-token").value = "changed-fake-token";
      await fire(ui.get("onprem-destination-token"), "input");
    } else {
      ui.get("onprem-history").value = "";
      await fire(ui.get("onprem-history"), "change");
    }
    ui.releaseEntry();
    await flush();
    assert.equal(ui.calls.some((call) => call.method === "POST"), false, "stale credentials or selection cannot retry after exact-entry refresh");
  }
  const conflict = await load({rejectRetry: true});
  await fire(rowButtons(conflict)[1], "click");
  assert.match(conflict.get("onprem-notice").textContent, /Задание изменилось/);
  assert.equal(conflict.target.point_retry_requested, false, "rejected CAS never claims success");
  for (const key of ["onprem-source-secret", "onprem-destination-token", "onprem-access-key"]) {
    const ui = await load();
    ui.get(key).value = "";
    await fire(ui.get(key), "input");
    const buttons = rowButtons(ui);
    assert.ok(buttons.every((button) => button.disabled));
    await fire(buttons[1], "click");
    assert.equal(ui.calls.some((call) => call.method !== "GET"), false);
  }
  const binding = await load();
  const disk = binding.nodes.find((node) => node.dataset.onpremConfig === "destination_vd_hash");
  disk.value = "another-disk";
  await fire(disk, "input");
  assert.ok(rowButtons(binding).every((button) => button.disabled));
  await fire(rowButtons(binding)[1], "click");
  assert.equal(binding.calls.some((call) => call.method !== "GET"), false);
  for (const options of [{state: "stopped"}, {state: "stopped", fullStop: true}, {legacy: true}]) {
    const ui = await load(options);
    assert.equal(rowButtons(ui).length, 0, "stopped and historical personal jobs cannot retry");
    assert.equal(ui.get("onprem-report").disabled, false);
    assert.equal(ui.get("onprem-journal").disabled, false);
    assert.equal(ui.calls.some((call) => call.method !== "GET"), false);
  }
  const stopped = await load({state: "stopped", stopBlocked: true});
  stopped.get("onprem-source-secret").value = "";
  stopped.get("onprem-destination-token").value = "";
  stopped.get("onprem-identity-confirmed").checked = false;
  await fire(stopped.get("onprem-identity-confirmed"), "change");
  assert.equal(stopped.get("onprem-stop").disabled, false, "historical queue holders can still be fully stopped without access");
  assert.match(stopped.get("onprem-job-stop").textContent, /нажмите «Остановить»/);
  await fire(stopped.get("onprem-stop"), "click");
  assert.deepEqual(stopped.calls.filter((call) => call.method !== "GET").map((call) => call.path.split("/").at(-1)), ["stop"]);
  assert.match(stopped.get("onprem-job-stop").textContent, /очередь освобождена.*пути не блокируются/);
  assert.doesNotMatch(stopped.get("onprem-job-stop").textContent, /форму ниже|журналам администратора|техническ/);
  for (const state of ["paused", "awaiting_credentials"]) {
    const ui = await load({state, pointActive: true});
    assert.ok(rowButtons(ui).every((button) => button.disabled));
    assert.equal(ui.get("onprem-resume").disabled, false);
    await fire(ui.get("onprem-resume"), "click");
    assert.deepEqual(ui.calls.filter((call) => call.method !== "GET").map((call) => call.path.split("/").at(-1)),
      ["credentials", "resume"], "Continue resumes the existing target without globally retrying other files");
    assert.equal(Object.hasOwn(ui.calls.find((call) => call.path.endsWith("/credentials")).body, "point_retry_entry_id"), false,
      "ordinary Continue retains full access validation");
  }
  const errors = await load();
  for (const [code, expected] of [
    ["active_or_unknown_attempt", /Файл сейчас обрабатывается/],
    ["entry_not_found", /Файл не найден в этом задании/],
    ["attempt_not_retryable", /Повтор этого файла сейчас недоступен/],
    ["point_retry_required", /Для прерванной передачи.*«Проверить и повторить» у файла/],
    ["staging_file_unavailable", /отсутствует в локальном кеше или повреждён.*после очистки кеша.*доступ к SaaS/],
    ["invalid_staging_object", /повреждена структура.*Проверьте каталог кеша/],
    ["technical_namespace_outcome_unknown", /историческом задании.*Остановите.*новый перенос/],
    ["final_upload_confirmation_required", /On-premises не подтвердил загрузку.*повторите файл/],
    ["final_move_confirmation_required", /On-premises не подтвердил перемещение.*повторите файл/],
  ]) {
    errors.target.error = code;
    await fire(errors.get("onprem-refresh"), "click");
    assert.match(errors.get("onprem-entry-body").textContent, expected);
    assert.doesNotMatch(errors.get("onprem-entry-body").textContent,
      /неизвестный исход.*заблокирован|подтвердите.*исход|Администратор должен|журналам администратора/);
  }
  errors.target.error = "source_download_timeout";
  await fire(errors.get("onprem-refresh"), "click");
  assert.match(errors.get("onprem-entry-body").textContent, /скачивания.*SaaS/);
  errors.target.error = "Bearer fake-private-destination";
  await fire(errors.get("onprem-refresh"), "click");
  assert.doesNotMatch(errors.get("onprem-entry-body").textContent, /fake-private/);
  for (const value of [-1, 4, 1.5, "2", "<unsafe-retry>", null]) {
    errors.target.point_retry_attempts = value;
    await fire(errors.get("onprem-refresh"), "click");
    assert.doesNotMatch(errors.get("onprem-entry-body").textContent, /Точечный повтор:|unsafe-retry/);
  }
}

async function verifyAutomaticJobActions() {
  const startControls = html.match(/<div id="onprem-run-actions"[^>]*>([\s\S]*?)<\/div>/)?.[1];
  const controls = html.match(/<div id="onprem-transfer-actions"[^>]*>([\s\S]*?)<\/div>/)?.[1];
  assert.ok(startControls, "Dry Run and Start have a shared upper row");
  assert.ok(controls, "the remaining transfer controls have their own lower row");
  const labels = (markup) => Array.from(markup.matchAll(/<button id="([^"]+)"[^>]*>([^<]+)<\/button>/g),
    (match) => [match[1], match[2]]);
  assert.deepEqual(labels(startControls), [["onprem-plan", "Dry Run"], ["onprem-start", "Начать перенос"]]);
  assert.deepEqual(labels(controls), [
    ["onprem-pause", "Пауза"],
    ["onprem-resume", "Продолжить"], ["onprem-stop", "Остановить полностью"],
    ["onprem-restart", "Начать заново"],
  ], "lower controls expose the remaining four requested actions");
  assert.match(html, /id="onprem-run-actions"[\s\S]*?<\/div>\s*<div id="onprem-transfer-actions"/,
    "the second row immediately follows Dry Run and Start");
  assert.ok(html.indexOf('id="onprem-transfer-actions"') < html.indexOf('id="onprem-history"'), "controls stay above history");
  for (const id of ["onprem-check-destination", "onprem-credentials", "onprem-retry", "onprem-retry-strategy"]) {
    assert.equal(createDocument().byId.has(id), false, `obsolete ${id} is removed from the DOM`);
  }

  async function load({state, resumeState, entryState = "failed", extraEntryStates = [], rejectAccess = false, delayResume = false}) {
    const ui = createDocument();
    const calls = [];
    const scheduled = [];
    const states = [entryState, ...extraEntryStates];
    const counts = states.reduce((result, value) => {
      result[value] = (result[value] || 0) + 1;
      return result;
    }, {});
    const selected = {...clone(job), state, resume_state: resumeState, counts,
      config: {...clone(config), strategy: "staged"}};
    const entries = states.map((state, index) => ({...clone(first), id: `entry-${index + 1}`, state}));
    let releaseResume;
    const response = (value, status = 200) => ({ok: status < 400, status, json: async () => clone(value)});
    const fakeFetch = async (address, options = {}) => {
      const url = new URL(address, "http://localhost");
      const method = options.method || "GET";
      const body = options.body ? JSON.parse(options.body) : null;
      calls.push({method, path: url.pathname, body});
      if (url.pathname === "/api/onprem/jobs") return response({jobs: [selected]});
      if (url.pathname.endsWith("/entries")) return response({entries: entries.filter((entry) =>
        !url.searchParams.has("state") || entry.state === url.searchParams.get("state"))});
      if (url.pathname.endsWith("/events")) return response({events: []});
      if (url.pathname.endsWith("/credentials") && rejectAccess) return response({detail: "destination_auth_failed"}, 403);
      if (url.pathname.endsWith("/resume")) {
        const resume = () => {
          selected.state = selected.resume_state === "ready" ? "ready" : "running";
          selected.resume_state = null;
          return response(selected);
        };
        if (delayResume) return new Promise((resolve) => {releaseResume = () => resolve(resume());});
        return resume();
      }
      if (url.pathname.endsWith("/retry") || url.pathname.endsWith("/start")) selected.state = "running";
      return response(selected);
    };
    vm.runInNewContext(source, {
      document: ui.document, window: {addEventListener() {}, confirm() {return true;},
        diskCopyConfigPromise: Promise.resolve({onprem_enabled: true, onprem_auth_required: true})},
      fetch: fakeFetch, URL, Blob, crypto, Uint8Array, console,
      setTimeout(callback) {scheduled.push(callback); return scheduled.length;}, clearTimeout() {},
    });
    await flush();
    ui.byId.get("onprem-access-key").value = "test-key";
    const tab = ui.document.querySelector('.mode-tab[data-mode="onprem"]');
    tab.classList.add("active");
    await fire(tab, "click");
    ui.byId.get("onprem-history").value = selected.id;
    await fire(ui.byId.get("onprem-history"), "change");
    ui.byId.get("onprem-source-secret").value = "fake-restored-source-secret";
    ui.byId.get("onprem-destination-token").value = "fake-restored-destination-token";
    ui.byId.get("onprem-identity-confirmed").checked = true;
    await fire(ui.byId.get("onprem-identity-confirmed"), "change");
    await scheduled.at(-1)();
    await flush();
    const writes = () => calls.filter((call) => call.method !== "GET");
    assert.deepEqual(writes(), [], "filling access and passive polling never writes or starts a job");
    return {...ui, calls, writes, selected, releaseResume: () => releaseResume()};
  }

  for (const state of ["completed_with_issues", "awaiting_credentials"]) {
    const ui = await load({state, resumeState: state === "awaiting_credentials" ? "paused" : undefined});
    assert.equal(ui.byId.get("onprem-resume").disabled, false);
    await fire(ui.byId.get("onprem-resume"), "click");
    assert.deepEqual(ui.writes().map((call) => [call.method, call.path.split("/").at(-1)]),
      [["PUT", "credentials"], ["POST", "retry"]], "Continue attaches access then retries failures once");
    assert.equal(ui.writes()[1].body.strategy, "staged", "error retry keeps the original transfer strategy");
  }
  const rejected = await load({state: "completed_with_issues", rejectAccess: true});
  await fire(rejected.byId.get("onprem-resume"), "click");
  assert.deepEqual(rejected.writes().map((call) => call.path.split("/").at(-1)), ["credentials"],
    "rejected access prevents both retry and resume");

  for (const state of ["paused", "awaiting_credentials"]) {
    for (const extraEntryStates of [[], ["outcome_unknown"], ["failed"]]) {
      const ui = await load({state, resumeState: state === "awaiting_credentials" ? "paused" : undefined,
        entryState: "planned", extraEntryStates});
      assert.equal(ui.byId.get("onprem-resume").disabled, false,
        "a paused job can continue planned files without replaying a separate historical unknown operation");
      await fire(ui.byId.get("onprem-resume"), "click");
      assert.deepEqual(ui.writes().map((call) => call.path.split("/").at(-1)), ["credentials", "resume"],
        "Continue restores credentials and resumes remaining work instead of globally retrying unknown files");
      assert.equal(ui.selected.state, "running");
    }
  }

  for (const state of ["paused", "awaiting_credentials"]) {
    const ui = await load({state, resumeState: state === "awaiting_credentials" ? "paused" : undefined,
      entryState: "planned"});
    const resume = ui.byId.get("onprem-resume");
    ui.byId.get("onprem-source-secret").value = "";
    ui.byId.get("onprem-destination-token").value = "";
    await fire(ui.byId.get("onprem-source-secret"), "input");
    assert.equal(resume.disabled, false, "paused work offers Continue even when access needs to be restored");
    await fire(resume, "click");
    assert.match(ui.byId.get("onprem-notice").textContent, /Введите Client secret источника/);
    assert.deepEqual(ui.writes(), [], "missing secrets never attach access or start work");
    ui.byId.get("onprem-source-secret").value = "restored-fake-secret";
    await fire(ui.byId.get("onprem-source-secret"), "input");
    await fire(resume, "click");
    assert.match(ui.byId.get("onprem-notice").textContent, /Введите токен сотрудника/);
    assert.deepEqual(ui.writes(), []);
    ui.byId.get("onprem-destination-token").value = "restored-fake-token";
    ui.byId.get("onprem-identity-confirmed").checked = false;
    await fire(ui.byId.get("onprem-identity-confirmed"), "change");
    await fire(resume, "click");
    assert.match(ui.byId.get("onprem-notice").textContent, /Подтвердите владельца токена/);
    assert.deepEqual(ui.writes(), []);
    ui.byId.get("onprem-identity-confirmed").checked = true;
    await fire(ui.byId.get("onprem-identity-confirmed"), "change");
    await fire(resume, "click");
    assert.deepEqual(ui.writes().map((call) => call.path.split("/").at(-1)), ["credentials", "resume"]);
  }

  const active = await load({state: "awaiting_credentials", resumeState: "running", entryState: "waiting_operation"});
  await fire(active.byId.get("onprem-resume"), "click");
  assert.deepEqual(active.writes().map((call) => call.path.split("/").at(-1)), ["credentials", "resume"],
    "a recovered active operation resumes without replaying the file");

  const ready = await load({state: "awaiting_credentials", resumeState: "ready", entryState: "planned"});
  assert.equal(ready.byId.get("onprem-start").disabled, false);
  await fire(ready.byId.get("onprem-start"), "click");
  assert.deepEqual(ready.writes().map((call) => call.path.split("/").at(-1)), ["credentials", "resume", "start"],
    "Start restores a ready plan before starting transfer");

  const transfer = await load({state: "running", entryState: "downloading"});
  const downloadRate = transfer.byId.get("onprem-download-rate");
  const uploadRate = transfer.byId.get("onprem-upload-rate");
  assert.ok(downloadRate && uploadRate, "the summary has distinct cache and shared-disk rates");
  assert.equal(transfer.byId.get("onprem-download-rate-row").hidden, false);
  assert.equal(downloadRate.textContent, "—", "idle cache transfers have no current speed");
  assert.equal(uploadRate.textContent, "—", "idle uploads do not reuse a completed transfer's speed");
  transfer.selected.transfer_rates = {download_bytes_per_second: 2048, download_active: true,
    upload_bytes_per_second: 1024, upload_active: true};
  await fire(transfer.byId.get("onprem-refresh"), "click");
  assert.equal(downloadRate.textContent, "2 Кб/с");
  assert.equal(uploadRate.textContent, "1 Кб/с");
  transfer.selected.transfer_rates.download_bytes_per_second = 0;
  transfer.selected.transfer_rates.upload_active = false;
  await fire(transfer.byId.get("onprem-refresh"), "click");
  assert.equal(downloadRate.textContent, "0 Б/с", "a stalled active stream is distinguished from an idle stage");
  assert.equal(uploadRate.textContent, "—");
  for (const invalid of [-1, NaN, Infinity, "2048", null]) {
    transfer.selected.transfer_rates = {download_bytes_per_second: invalid, download_active: true,
      upload_bytes_per_second: invalid, upload_active: true};
    await fire(transfer.byId.get("onprem-refresh"), "click");
    assert.equal(downloadRate.textContent, "—", "unavailable or malformed speeds are not shown as measurements");
    assert.equal(uploadRate.textContent, "—");
  }
  transfer.selected.transfer_rates = {download_bytes_per_second: 2048, download_active: "true",
    upload_bytes_per_second: 1024, upload_active: "true"};
  await fire(transfer.byId.get("onprem-refresh"), "click");
  assert.equal(downloadRate.textContent, "—", "activity markers must be booleans");
  assert.equal(uploadRate.textContent, "—");
  transfer.selected.transfer_rates.download_active = true;
  transfer.selected.transfer_rates.upload_active = true;
  for (const state of ["paused", "awaiting_credentials", "stopped", "completed_with_issues"]) {
    transfer.selected.state = state;
    await fire(transfer.byId.get("onprem-refresh"), "click");
    assert.equal(downloadRate.textContent, "—", "non-running jobs never display a stale active rate");
    assert.equal(uploadRate.textContent, "—");
  }
  transfer.selected.state = "running";
  transfer.selected.config.strategy = "url";
  await fire(transfer.byId.get("onprem-refresh"), "click");
  assert.equal(transfer.byId.get("onprem-download-rate-row").hidden, true, "direct imports do not download into a local cache");
  assert.equal(uploadRate.textContent, "Недоступна через API");
  assert.match(uploadRate.title, /Файл скачивает сервер On-premises; API сообщает только статус операции/);
  transfer.selected.config.strategy = "staged";
  transfer.selected.transfer_rates = {};
  await fire(transfer.byId.get("onprem-refresh"), "click");
  assert.equal(transfer.byId.get("onprem-download-rate-row").hidden, false, "opening a staged job restores the cache indicator");
  assert.equal(uploadRate.textContent, "—");

  for (const id of ["onprem-source-secret", "onprem-identity-confirmed"]) {
    const delayed = await load({state: "awaiting_credentials", resumeState: "ready", entryState: "planned", delayResume: true});
    await fire(delayed.byId.get("onprem-start"), "click");
    assert.deepEqual(delayed.writes().map((call) => call.path.split("/").at(-1)), ["credentials", "resume"]);
    if (id === "onprem-source-secret") delayed.byId.get(id).value = "changed-fake-secret";
    else delayed.byId.get(id).checked = false;
    await fire(delayed.byId.get(id), id === "onprem-source-secret" ? "input" : "change");
    delayed.releaseResume();
    await flush();
    assert.deepEqual(delayed.writes().map((call) => call.path.split("/").at(-1)), ["credentials", "resume"],
      `changing ${id} while recovery is pending prevents stale Start`);
  }
}

(async () => {
  await verifyStagedOnly();
  await verifyReleaseGate();
  await verifyGlobalCacheCleanup();
  await verifyPublicSourceConfiguration();
  await verifyAutomaticJobActions();
  await verifyPointRetry();
  await flush();
  assert.equal(document.querySelector('.mode-tab[data-mode="onprem"]').hidden, false);
  assert.equal(document.querySelector('.mode-tab[data-mode="onprem"]').disabled, false);
  assert.equal(document.querySelector('.mode-tab[data-mode="onprem"]').title, "SaaS → On-premises");
  assert.equal(panel.hidden, false);
  assert.ok(field("destination_vd_hash"), "new transfers require an existing shared disk mark");
  assert.equal(field("destination_base"), undefined, "new transfers always use the shared-disk root");
  assert.equal(field("destination_root"), undefined, "the per-email destination is derived, not entered manually");
  assert.equal(field("destination_type"), undefined, "the new form has no personal/shared destination picker");
  assert.equal(byId.get("onprem-access").hidden, false);
  assert.equal(byId.get("onprem-plan").disabled, true, "missing access key and identity confirmation block creation");
  assert.equal(byId.get("onprem-journal").disabled, true, "journal export needs an authorized selected job");
  const tab = document.querySelector('.mode-tab[data-mode="onprem"]');
  await fire("onprem-journal", "click");
  assert.equal(requests.length, 0, "manually dispatched locked export does not request a journal");
  tab.classList.add("active");
  await fire(tab, "click");
  assert.equal(byId.get("log").hidden, true);
  assert.equal(requests.length, 0, "no unauthenticated polling requests");
  byId.get("onprem-access-key").value = "test-key";
  for (const [key, value] of Object.entries(config)) if (field(key)) field(key).value = value;
  byId.get("onprem-source-secret").value = "fake-source-secret";
  byId.get("onprem-destination-token").value = "fake-destination-token";
  byId.get("onprem-identity-confirmed").checked = true;
  await fire("onprem-identity-confirmed", "change");
  await fire(field("source_email"), "input");
  assert.match(byId.get("onprem-destination-preview").textContent, /shared-target-hash.*\/source@example\.test/);
  assert.doesNotMatch(byId.get("onprem-destination-preview").textContent, /\/\//);
  field("source_email").value = "another@example.test";
  await fire(field("source_email"), "input");
  assert.match(byId.get("onprem-destination-preview").textContent, /\/another@example\.test/);
  field("source_email").value = config.source_email;
  await fire(field("source_email"), "input");
  assert.equal(byId.get("onprem-plan").disabled, false);
  await fire("onprem-form", "submit");
  assert.equal(requests.filter((request) => request.path === "/api/onprem/jobs" && request.method === "POST").at(-1).body.credentials.source_admin_token, undefined);
  assert.match(byId.get("onprem-destination-check").textContent, /Архив сотрудников/);
  assert.match(byId.get("onprem-destination-check").textContent, /shared-target-hash/);
  assert.match(byId.get("onprem-destination-check").textContent, /10 Кб/);
  assert.match(byId.get("onprem-destination-check").textContent, /Чтение и запись/);
  byId.get("onprem-destination-token").value = "refreshed-fake-token";
  await fire("onprem-destination-token", "input");
  assert.equal(byId.get("onprem-destination-check").hidden, true, "changing credentials invalidates the verified disk card");
  byId.get("onprem-destination-token").value = "fake-destination-token";
  await fire("onprem-form", "submit");
  const create = requests.find((request) => request.method === "POST" && request.path === "/api/onprem/jobs");
  assert.equal(create.body.config.identity_confirmed, true);
  assert.equal(create.body.config.destination_type, "shared");
  assert.equal(create.body.config.destination_vd_hash, "shared-target-hash");
  assert.equal(create.body.config.destination_base, "/");
  assert.equal("destination_root" in create.body.config, false, "backend derives the per-email root");
  assert.equal(create.body.config.strategy, "staged", "the form always submits the staging route");
  assert.equal(create.body.credentials.source_client_secret, "fake-source-secret");
  assert.match(create.body.idempotency_key, /^[0-9a-f-]{36}$/i);
  assert.equal(byId.get("onprem-start").disabled, true, "queued plan cannot start");
  const initialEvents = requests.filter((request) => request.path.endsWith("/events")).at(-1);
  assert.equal(initialEvents.params.get("after"), "0");
  assert.equal(initialEvents.params.get("tail"), "500", "opening history loads the latest bounded journal immediately");
  assert.match(byId.get("onprem-entry-body").textContent, /<unsafe>\.txt/);
  assert.match(byId.get("onprem-entry-body").textContent, /shared-target-hash/);
  assert.equal(byId.has("onprem-remote-root"), false);
  assert.match(byId.get("onprem-job-destination").textContent,
    /^source@example\.test \(SaaS\) → Общий диск On-premises.*\[shared-target-hash\] · \/source@example\.test$/,
    "the route runs from the SaaS source to the On-premises shared disk and its saved destination folder");
  assert.equal((byId.get("onprem-job-destination").textContent.match(/→/g) || []).length, 1,
    "a folder within the destination is not rendered as a second transfer direction");
  job.state = "ready";
  job.total_files = 205;
  job.counts = {planned: 205};
  allEntries = Array.from({length: 205}, (_, index) => ({...first, id: `entry-${index + 1}`, source_path: `/file-${index + 1}`, attempt_id: null}));
  allEntries[204] = {...first, id: "entry-205", source_path: "/last-file", state: "outcome_unknown"};
  await fire("onprem-refresh", "click");
  assert.equal(byId.get("onprem-entry-body").children.length, 100, "plan page is bounded");
  assert.equal(byId.get("onprem-entry-next").disabled, false, "additional plan pages are reachable");
  assert.match(byId.get("onprem-entry-count").textContent, /205/);
  assert.equal(byId.has("onprem-resolve-panel"), false, "retry is available only inside visible file rows");
  await fire("onprem-entry-next", "click");
  await fire("onprem-entry-next", "click");
  assert.match(byId.get("onprem-entry-body").textContent, /last-file/);
  assert.equal(byId.get("onprem-entry-next").disabled, true);
  job.total_files = 1; job.counts = {planned: 1}; allEntries = [first];
  await fire("onprem-entry-previous", "click"); await fire("onprem-entry-previous", "click");
  for (const address of ["https://DISK.example.test", "https://disk.example.test:443", "https://DISK.example.test:443/v1/disk/"]) {
    field("destination_url").value = address;
    await fire(field("destination_url"), "input");
    assert.equal(byId.get("onprem-start").disabled, false,
      "canonical host case, default HTTPS port and API suffix preserve the original destination binding");
  }
  field("destination_url").value = "https://disk.example.test:8443";
  await fire(field("destination_url"), "input");
  assert.equal(byId.get("onprem-start").disabled, true, "a different non-default port changes the bound destination");
  field("destination_url").value = config.destination_url;
  await fire(field("destination_url"), "input");
  assert.equal(byId.get("onprem-start").disabled, false);
  byId.get("onprem-source-admin-token").value = "fake-source-admin";
  await fire("onprem-source-admin-token", "input");
  assert.equal(byId.get("onprem-start").disabled, false, "editing administrator access retains the existing plan");
  const beforeAutoStart = requests.length;
  await fire("onprem-start", "click");
  assert.deepEqual(requests.slice(beforeAutoStart).filter((request) => request.method !== "GET")
    .map((request) => [request.method, request.path]), [
    ["PUT", "/api/onprem/jobs/job-1/credentials"],
    ["POST", "/api/onprem/jobs/job-1/start"],
  ], "Start validates filled access before the transfer without a separate button");
  assert.equal(requests.slice(beforeAutoStart).find((request) => request.path.endsWith("/credentials")).body.credentials.source_admin_token, "fake-source-admin");
  assert.equal(byId.get("onprem-pause").disabled, false);
  await fire("onprem-pause", "click");
  assert.equal(byId.get("onprem-resume").disabled, false);
  await fire("onprem-resume", "click");
  job.state = "running";
  first.state = "downloading";
  first.download_attempts = 2;
  first.retry_at_ms = Date.now() + 12000;
  await fire("onprem-refresh", "click");
  let retryRow = byId.get("onprem-entry-body").textContent;
  assert.match(retryRow, /Скачивание: 2\/5/);
  assert.match(retryRow, /Ожидание повтора/);
  assert.match(retryRow, /Следующая попытка через \d+ с/);
  assert.ok(retryRow.includes(new Date(first.retry_at_ms).toLocaleString("ru-RU")), "retry deadline uses browser time zone");
  job.state = "stopped";
  await fire("onprem-refresh", "click");
  assert.doesNotMatch(byId.get("onprem-entry-body").textContent, /Ожидание повтора|Следующая попытка/, "stopped jobs never promise a retry");
  job.state = "running";
  job.stop_requested = true;
  await fire("onprem-refresh", "click");
  assert.doesNotMatch(byId.get("onprem-entry-body").textContent, /Ожидание повтора|Следующая попытка/, "requesting a stop cancels the retry promise immediately");
  delete job.stop_requested;
  first.state = "failed";
  first.download_attempts = 5;
  first.retry_at_ms = null;
  first.error = "source_download_failed";
  await fire("onprem-refresh", "click");
  retryRow = byId.get("onprem-entry-body").textContent;
  assert.match(retryRow, /Ошибка/);
  assert.match(retryRow, /Скачивание: 5\/5/);
  assert.doesNotMatch(retryRow, /Следующая попытка/);
  for (const download_attempts of [-1, 6, 1.5, "2", "<unsafe-attempt>", null]) {
    first.download_attempts = download_attempts;
    first.retry_at_ms = "<unsafe-retry-deadline>";
    await fire("onprem-refresh", "click");
    assert.doesNotMatch(byId.get("onprem-entry-body").textContent, /Скачивание:|unsafe-attempt|unsafe-retry-deadline|Следующая попытка/, "invalid retry metadata is never rendered");
  }
  delete first.download_attempts; delete first.retry_at_ms; delete first.error;
  await fire("onprem-refresh", "click");
  assert.doesNotMatch(byId.get("onprem-entry-body").textContent, /Скачивание:|Следующая попытка/, "legacy entries do not gain an invented counter");
  job.state = "needs_attention"; first.state = "outcome_unknown"; job.counts = {outcome_unknown: 1}; job.revision += 1;
  operationEvents = [
    {sequence: 2, created_at: "2026-10-11T12:00:01Z", type: "operation_started",
      payload: {entry_id: "entry:legacy", attempt_id: "attempt.legacy", operation_id: "path-operation_1"}},
    {sequence: 3, created_at: "2026-10-11T12:00:02Z", type: "operation_started",
      payload: {entry_id: "?id=entry-private", attempt_id: "?id=attempt-private", operation_id: "?id=query-operation_2"}},
    ...["operation.invalid", "https://disk.example.test/operations?id=private-url", "?id=private-param&token=fake-access",
      "?id=private-fragment#extra", "?id=" + "x".repeat(257)].map((operation_id, index) => ({
      sequence: 4 + index, created_at: "2026-10-11T12:00:03Z", type: "operation_started", payload: {operation_id},
    })),
    {sequence: 9, created_at: "2026-10-11T12:00:04Z", type: "transfer_stage",
      payload: {entry_id: "entry-1", attempt_id: "attempt-1", stage: "downloading", status: "failed",
        error: "source_auth_failed", http_status: 503, duration_ms: 1523, operation_id: "download-operation",
        token: "event-secret-token", download_url: "https://private.test/download?token=url-private"},
      context: {source: "disk:/<script>alert(1)</script>.txt", destination: "vd:shared-target-hash:disk:/copy/<unsafe>.txt",
        remote_path: "vd:shared-target-hash:disk:/.transfer/payload", kind: "file", size: 2048, strategy: "staged"},
      stage_label: "fake-source-secret", status_label: "fake-private-token", error_message: "Bearer fake-destination-token"},
    {sequence: 10, created_at: "2026-10-11T12:00:05Z", type: "entry_state",
      payload: {state: "failed", error: "remote_operation_failed"}},
    {sequence: 11, created_at: "2026-10-11T12:00:06Z", type: "transfer_stage",
      payload: {stage: "poll_move", status: "succeeded", remote_status: "in-progress"},
      context: {kind: "dir", source: "disk:/Папка", destination: "disk:/copy/Папка"}},
    {sequence: 12, created_at: "2026-10-11T12:00:07Z", type: "transfer_stage",
      payload: {stage: "planning", status: "failed", source_path: "disk:/missing",
        error: "unexpected_backend_code", http_status: "503", duration_ms: "123", remote_status: "https://private.test/token=bad"},
      context: {source: "https://private.test/token=path-secret", destination: "disk:/copy/token=path-secret",
        strategy: "url?token=bad", size: "123", remote_path: "file:///private/token"}},
    {sequence: 13, created_at: "2026-10-11T12:00:08Z", type: "transfer_stage",
      payload: {stage: "oauth private-stage", status: "Bearer private-status", error: "Bearer event-private-token"},
      context: {source: "disk:/newline\nprivate-secret", destination: "disk:/copy\tprivate-secret"}},
    {sequence: 14, created_at: "2026-10-11T12:00:09Z", type: "attempt_updated",
      payload: {phase: "downloading", error: "source_download_failed", attempt_id: "historical-attempt"},
      context: {source: "disk:/historical-failure.txt", destination: "disk:/copy/historical-failure.txt",
        kind: "file", size: 18, strategy: "staged"}},
    {sequence: 15, created_at: "2026-10-11T12:00:10Z", type: "transfer_stage",
      payload: {stage: "download_retry", status: "waiting", download_attempts: 2, max_download_attempts: 5,
        retry_delay_ms: 4000, retry_at_ms: Date.parse("2026-10-11T12:00:14Z"), error: "source_unavailable", http_status: 503}},
    {sequence: 16, created_at: "2026-10-11T12:00:15Z", type: "transfer_stage",
      payload: {stage: "download_retry", status: "skipped", download_attempts: 5, max_download_attempts: 5,
        error: "source_download_failed"}},
    {sequence: 17, created_at: "2026-10-11T12:00:16Z", type: "transfer_stage",
      payload: {stage: "download_retry", status: "waiting", download_attempts: 6, max_download_attempts: "<unsafe-max>",
        retry_delay_ms: "<unsafe-delay>", retry_at_ms: "<unsafe-deadline>"}},
    {sequence: 18, created_at: "2026-10-11T12:00:17Z", type: "transfer_stage",
      payload: {stage: "downloading", status: "failed", error: "source_public_download_failed", http_status: 500,
        error_side: "saas", api_error: "InternalServerError", request_host: "storage.yandex.net", request_id: "req-500"},
      diagnostic_message: "SaaS: HTTP 500 — Внутренняя ошибка сервера. Код API: InternalServerError. Сервер: storage.yandex.net. ID запроса: req-500.",
      error_message: "Bearer fake-diagnostic-token", description: "https://private.test/raw?token=raw-secret"},
    {sequence: 19, created_at: "2026-10-11T12:00:18Z", type: "transfer_stage",
      payload: {stage: "upload_file", status: "failed", error: "destination_unavailable", http_status: 507,
        error_side: "onprem", api_error: "DiskInsufficientStorageError", request_host: "disk.example.test", request_id: "req-507"},
      diagnostic_message: "On-premises: HTTP 507 — Недостаточно свободного места. Код API: DiskInsufficientStorageError. Сервер: disk.example.test. ID запроса: req-507."},
    {sequence: 20, created_at: "2026-10-11T12:00:19Z", type: "transfer_stage",
      payload: {stage: "downloading", status: "failed", error: "source_download_timeout", error_side: "saas",
        transport_error: "read_timeout", request_host: "storage.yandex.net"},
      diagnostic_message: "SaaS: Истекло время ожидания чтения данных (read_timeout). Сервер: storage.yandex.net."},
    ...["https://private.test/download?token=diagnostic-url-secret", "OAuth diagnostic-oauth-secret",
      "Bearer diagnostic-bearer-secret", "secret=diagnostic-field-secret", "password = diagnostic-password-secret",
      "<script>diagnostic-html-secret</script>", "diagnostic-long-secret".repeat(100)].map((diagnostic_message, index) => ({
      sequence: 21 + index, created_at: "2026-10-11T12:00:20Z", type: "transfer_stage", diagnostic_message,
      payload: {stage: "downloading", status: "failed", error: "source_public_download_failed", http_status: 500,
        api_error: "Bearer raw-api-secret", transport_error: "https://private.test/network-secret", error_side: "secret=side-secret",
        request_host: "https://private.test/token=host-secret", request_id: "Bearer raw-request-secret",
        message: "raw-message-secret", description: "raw-description-secret"},
    })),
    {sequence: 28, created_at: "2026-10-11T12:00:21Z", type: "transfer_stage",
      payload: {stage: "upload_file", status: "failed", error: "destination_read_only", http_status: 423},
      diagnostic_message: "On-premises. HTTP 423: Ресурс заблокирован."},
    {sequence: 29, created_at: "2026-10-11T12:00:22Z", type: "transfer_stage",
      payload: {stage: "poll_transfer", status: "failed", error: "destination_url_import_failed",
        http_status: 200, remote_status: "failed"},
      diagnostic_message: "On-premises. HTTP 200: Статус получен; удалённая операция завершилась ошибкой. Код API: DiskDownloadLimitExceededError. Превышен лимит скачивания."},
  ];
  await fire("onprem-refresh", "click");
  const operationLog = byId.get("onprem-log").textContent;
  assert.match(operationLog, /path-operation_1/);
  assert.match(operationLog, /\?id=query-operation_2/, "query-form operation references remain inspectable");
  assert.match(operationLog, /entry:legacy/);
  assert.match(operationLog, /attempt\.legacy/);
  assert.doesNotMatch(operationLog, /entry-private|attempt-private|operation\.invalid|private-url|private-param|fake-access|private-fragment|x{257}/,
    "operation references have a stricter filter than entry and attempt IDs");
  assert.match(operationLog, /Скачивание.*Ошибка.*source_auth_failed/);
  assert.match(operationLog, /HTTP 503/);
  const serverErrorLine = operationLog.split("\n").find((line) => line.startsWith("18 "));
  assert.match(serverErrorLine, /SaaS: HTTP 500 — Внутренняя ошибка сервера/);
  assert.match(serverErrorLine, /Код API: InternalServerError.*Сервер: storage\.yandex\.net.*ID запроса: req-500/);
  assert.equal((serverErrorLine.match(/HTTP 500/g) || []).length, 1, "the detailed description does not repeat its HTTP status");
  const quotaErrorLine = operationLog.split("\n").find((line) => line.startsWith("19 "));
  assert.match(quotaErrorLine, /On-premises: HTTP 507 — Недостаточно свободного места/);
  assert.match(operationLog, /Истекло время ожидания чтения данных \(read_timeout\)/);
  const lockedResourceLine = operationLog.split("\n").find((line) => line.startsWith("28 "));
  assert.match(lockedResourceLine, /ресурс заблокирован или доступ ограничен.*HTTP 423: Ресурс заблокирован/);
  assert.doesNotMatch(lockedResourceLine, /только для чтения/, "HTTP 423 does not establish that the disk is read-only");
  const failedOperationLine = operationLog.split("\n").find((line) => line.startsWith("29 "));
  assert.match(failedOperationLine, /ошибке скачивания файла по ссылке.*Код API: DiskDownloadLimitExceededError/);
  assert.doesNotMatch(failedOperationLine, /не сообщил подробную причину/, "an explicit API cause does not coexist with a claim that the cause is absent");
  assert.doesNotMatch(operationLog, /fake-diagnostic-token|raw-secret|diagnostic-.*secret|raw-.*secret|network-secret|side-secret|host-secret/,
    "diagnostic text and fields never reveal raw messages, secrets, full URLs or HTML");
  assert.match(operationLog, /1523 мс/);
  assert.match(operationLog, /11\.10\.2026/);
  assert.match(operationLog, /disk:\/<script>alert\(1\)<\/script>\.txt/, "hostile file names remain text only");
  assert.match(operationLog, /shared-target-hash.*copy.*<unsafe>/);
  assert.match(operationLog, /2 Кб/);
  assert.match(operationLog, /С временным сохранением/);
  assert.match(operationLog, /Удалённый статус: Выполняется/);
  assert.match(operationLog, /Каталог/);
  assert.match(operationLog, /remote_operation_failed/, "legacy payload.error is visible");
  assert.match(operationLog, /unexpected_backend_code/, "unknown safe error codes remain diagnosable");
  assert.match(operationLog, /disk:\/missing/, "planning failures have a source-path fallback without an entry");
  assert.match(operationLog, /Попытка изменена.*Фаза: Скачивание.*source_download_failed.*historical-failure\.txt/,
    "historical attempt phases and errors are displayed without inventing transfer-stage events");
  assert.match(operationLog, /Повтор скачивания.*Ожидание.*source_unavailable.*HTTP 503.*Скачивание: 2\/5.*Пауза перед повтором: 4 с/);
  assert.ok(operationLog.includes(new Date(Date.parse("2026-10-11T12:00:14Z")).toLocaleString("ru-RU")), "journal retry deadline uses browser time zone");
  assert.match(operationLog, /Повтор скачивания.*Пропущен.*source_download_failed.*Скачивание: 5\/5/);
  const hostileRetryEvent = operationLog.split("\n").find((line) => line.startsWith("17 "));
  assert.doesNotMatch(hostileRetryEvent, /Скачивание:|Пауза перед повтором:|Следующая попытка:|unsafe/, "out-of-bounds and hostile retry fields are suppressed");
  await fire("onprem-refresh", "click");
  const incrementalEvents = requests.filter((request) => request.path.endsWith("/events")).at(-1);
  assert.equal(incrementalEvents.params.get("after"), "29", "polling follows the latest received cursor");
  assert.equal(incrementalEvents.params.has("tail"), false, "later polling only requests new events");
  assert.doesNotMatch(operationLog, /fake-source-secret|fake-private-token|fake-destination-token|event-secret-token|event-private-token|url-private|path-secret|private-secret|private-stage|private-status|https?:\/\/|file:\/\//);
  assert.equal(byId.get("onprem-resume").disabled, true);
  assert.equal(byId.get("onprem-resume").disabled, true);
  job.state = "paused"; first.state = "failed"; job.counts = {failed: 1};
  await fire("onprem-refresh", "click");
  assert.equal(byId.get("onprem-resume").disabled, false);
  await fire("onprem-resume", "click");
  assert.equal(requests.find((request) => request.path.endsWith("/retry")).body.strategy, "staged");
  job.state = "awaiting_credentials";
  job.resume_state = "running";
  await fire("onprem-refresh", "click");
  byId.get("onprem-history").value = "job-1";
  await fire("onprem-history", "change");
  assert.equal(byId.get("onprem-source-secret").value, "");
  assert.equal(byId.get("onprem-destination-token").value, "");
  assert.equal(byId.get("onprem-identity-confirmed").checked, false);
  assert.equal(byId.get("onprem-resume").disabled, false, "Continue explains how to restore access after refresh");
  const beforeMissingAccessResume = requests.length;
  await fire("onprem-resume", "click");
  assert.equal(requests.length, beforeMissingAccessResume, "no request is sent until owner confirmation and secrets are supplied");
  assert.match(byId.get("onprem-notice").textContent, /Подтвердите владельца токена/);
  byId.get("onprem-source-secret").value = "new-fake-source-secret";
  byId.get("onprem-destination-token").value = "new-fake-destination-token";
  byId.get("onprem-identity-confirmed").checked = true;
  await fire("onprem-identity-confirmed", "change");
  assert.equal(byId.get("onprem-resume").disabled, false);
  const beforeAutoResume = requests.length;
  await fire("onprem-resume", "click");
  assert.deepEqual(requests.slice(beforeAutoResume).filter((request) => request.method !== "GET").map((request) => request.path.split("/").at(-1)), ["credentials", "resume"]);
  job.state = "paused";
  await fire("onprem-refresh", "click");
  await fire("onprem-cleanup", "click");
  assert.match(byId.get("onprem-cleanup-result").textContent, /Удалено файлов: 1.*Объём: 2 Кб/);
  assert.equal(requests.filter((request) => request.path.endsWith("/cleanup")).at(-1).path, "/api/onprem/cache/cleanup");
  await fire("onprem-report", "click");
  await fire("onprem-journal", "click");
  assert.equal(requests.filter((request) => request.path.endsWith("/journal")).at(-1).path, `/api/onprem/jobs/${job.id}/journal`);
  assert.equal(requests.every((request) => request.headers["X-Onprem-Key"] === "test-key"), true);
  assert.doesNotMatch(byId.get("onprem-log").textContent, /fake-source-secret|fake-destination-token|fake-private-token/);
  assert.deepEqual(storageWrites, [], "no on-prem secrets or access key are stored in browser storage");
  const anchorsBeforeStaleExport = document.body.children.length;
  let finishStaleExport;
  apiOverride = ({url}) => url.pathname.endsWith("/journal")
    ? new Promise((resolve) => {finishStaleExport = resolve;}) : null;
  const staleExport = fire("onprem-journal", "click");
  await flush();
  assert.equal(byId.get("onprem-journal").disabled, true, "an active export locks its button");
  byId.get("onprem-history").value = "";
  await fire("onprem-history", "change");
  finishStaleExport({ok: true, status: 200, blob: async () => new Blob(["journal\n"])});
  await staleExport;
  apiOverride = null;
  assert.equal(document.body.children.length, anchorsBeforeStaleExport, "an export for a cleared selection never downloads");
  assert.equal(byId.get("onprem-journal").disabled, true, "export needs a selected job");
  byId.get("onprem-history").value = job.id;
  await fire("onprem-history", "change");
  assert.equal(byId.get("onprem-journal").disabled, false);

  assert.ok(byId.get("onprem-history-older"), "history has an older-page control");
  const oldJob = {...clone(job), id: "old-unresolved-job", state: "needs_attention", error: "transfer_outcome_unknown",
    counts: {outcome_unknown: 1}, total_files: 1, config: {...config, source_email: "old@example.test"}};
  allJobs = [job, ...Array.from({length: 60}, (_, index) => ({...clone(job), id: `archived-${index + 1}`, state: "completed"})), oldJob];
  entrySets.set(oldJob.id, [{...first, state: "outcome_unknown", attempt_id: "old-attempt"}]);
  allEntries = Array.from({length: 205}, (_, index) => ({...first, id: `entry-${index + 1}`, state: "planned", attempt_id: null}));
  job.total_files = 205; job.counts = {planned: 205};
  await fire("onprem-refresh", "click");
  assert.equal(byId.get("onprem-history").children.length, 51, "history shows 50 jobs per page");
  assert.equal(byId.get("onprem-history-older").disabled, false);
  await fire("onprem-entry-next", "click");
  const entryPage = byId.get("onprem-entry-page").textContent;
  await fire("onprem-history-older", "click");
  assert.equal(byId.get("onprem-entry-page").textContent, entryPage, "history paging preserves the entry page");
  assert.equal(byId.get("onprem-history-newer").disabled, false);
  assert.equal(byId.get("onprem-history-older").disabled, true);
  assert.ok(byId.get("onprem-history").children.some((option) => option.value === oldJob.id), "an unresolved job older than the first 50 is selectable");
  byId.get("onprem-history").value = oldJob.id;
  await fire("onprem-history", "change");
  assert.equal(byId.get("onprem-history").value, oldJob.id);
  assert.match(byId.get("onprem-entry-body").textContent, /Исход неизвестен/, "historical rows remain inspectable");
  await fire("onprem-refresh", "click");
  const lists = requests.filter((request) => request.path === "/api/onprem/jobs" && request.method === "GET");
  assert.equal(lists.at(-1).params.get("offset"), "50", "refresh preserves the history offset");
  await fire("onprem-history-newer", "click");
  assert.equal(byId.get("onprem-history").value, oldJob.id, "the selected job remains selected outside its history page");
  assert.ok(byId.get("onprem-history").children.some((option) => option.value === oldJob.id));
  assert.equal(byId.get("onprem-history-newer").disabled, true);
  oldJob.error = "directory_confirmation_pending";
  await fire("onprem-refresh", "click");
  assert.match(byId.get("onprem-job-error").textContent, /каталог/);
  const holder = {...clone(job), id: "d5da4e2ab55a44a2a7024ae51e3f0476", state: "awaiting_credentials",
    error: "unknown_transfer", counts: {outcome_unknown: 1}, total_files: 1,
    config: {...config, source_email: "holder@example.test"}};
  const waiting = {...clone(job), id: "db9b7472cff1491fa960727a123db7cb", state: "running", error: null,
    counts: {}, directory_counts: {planned: 1}, total_files: 0, total_directories: 1,
    blocked_by: {job_id: holder.id, state: holder.state, reason: "operation_outcome_unknown"}};
  allJobs = [waiting, ...Array.from({length: 60}, (_, index) => ({...clone(job), id: `history-${index}`, state: "completed"})), holder];
  entrySets.set(waiting.id, [{...first, kind: "dir", state: "planned", attempt_id: null}]);
  entrySets.set(holder.id, [{...first, state: "outcome_unknown", attempt_id: "holder-attempt"}]);
  await fire("onprem-refresh", "click");
  byId.get("onprem-history").value = waiting.id;
  await fire("onprem-history", "change");
  assert.equal(byId.get("onprem-job-state").textContent, "Ожидание другого задания",
    "a running job held by an unknown operation must not claim transfer progress");
  assert.equal(byId.get("onprem-job-blocked").hidden, false);
  assert.match(byId.get("onprem-job-blocked-reason").textContent, /Другое задание требует проверки/i);
  assert.match(byId.get("onprem-job-blocked-holder").textContent, /^Другое задание · /,
    "a blocker outside the loaded history page has a neutral label without exposing its UUID");
  assert.match(byId.get("onprem-job-blocked-holder").textContent, /Нужен повторный доступ/);
  assert.equal(byId.get("onprem-open-blocker").disabled, false);
  assert.equal(byId.get("onprem-pause").disabled, false, "waiting jobs can still request a local pause");
  assert.equal(byId.get("onprem-resume").disabled, true);
  assert.equal(byId.get("onprem-resume").disabled, true);
  assert.match(byId.get("onprem-history").children.find((option) => option.value === waiting.id).textContent,
    /Ожидание другого задания/);
  const beforeBlockerNavigation = requests.length;
  await fire("onprem-open-blocker", "click");
  assert.equal(byId.get("onprem-history").value, holder.id,
    "the blocker can be opened directly even when it is outside the current history page");
  assert.match(byId.get("onprem-entry-body").textContent, /Исход неизвестен/);
  assert.equal(byId.get("onprem-job-blocked").hidden, true);
  assert.equal(byId.get("onprem-job-blocked-reason").textContent, "");
  assert.equal(byId.get("onprem-job-blocked-holder").textContent, "");
  assert.equal(holder.counts.outcome_unknown, 1);
  assert.equal(waiting.state, "running", "opening a blocker does not alter the waiting job");
  assert.ok(requests.slice(beforeBlockerNavigation).every((request) => request.method === "GET"),
    "navigation never retries or resolves an unknown operation");
  assert.ok(requests.slice(beforeBlockerNavigation).some((request) => request.path === `/api/onprem/jobs/${holder.id}`));
  byId.get("onprem-history").value = waiting.id;
  await fire("onprem-history", "change");
  waiting.state = "awaiting_credentials";
  waiting.resume_state = "running";
  waiting.pause_requested = true;
  await fire("onprem-refresh", "click");
  assert.equal(byId.get("onprem-job-state").textContent, "Нужен повторный доступ",
    "a restarted waiting job must retain its credentials requirement as the primary status");
  assert.equal(byId.get("onprem-job-blocked").hidden, false,
    "a restarted job also explains which earlier job still holds the transfer lane");
  assert.match(byId.get("onprem-job-blocked-reason").textContent, /Другое задание требует проверки/i);
  assert.equal(byId.get("onprem-open-blocker").disabled, false);
  assert.equal(byId.get("onprem-resume").disabled, false, "Continue is offered so missing access can be explained");
  const beforeBlockedMissingAccessResume = requests.length;
  await fire("onprem-resume", "click");
  assert.equal(requests.length, beforeBlockedMissingAccessResume, "inspecting a blocker does not supply credentials or start work");
  assert.match(byId.get("onprem-notice").textContent, /Подтвердите владельца токена/);
  assert.equal(byId.get("onprem-pause").disabled, true);
  assert.equal(byId.get("onprem-job-pause").hidden, false);
  assert.equal(byId.get("onprem-job-pause").textContent, "Пауза запрошена. Ожидается остановка задания.",
    "a restarted waiting job does not claim an operation is currently executing while pause is pending");
  assert.match(byId.get("onprem-history").children.find((option) => option.value === waiting.id).textContent,
    /Нужен повторный доступ/);
  const restartingBlocker = clone(waiting.blocked_by);
  waiting.blocked_by = null;
  await fire("onprem-refresh", "click");
  assert.equal(byId.get("onprem-job-pause").textContent, "Пауза запрошена. Ожидается остановка задания.",
    "a restarted running job retains a neutral pending-pause message without a blocker");
  waiting.blocked_by = restartingBlocker;
  waiting.resume_state = "paused";
  const beforeRestartPauseRefresh = requests.length;
  await fire("onprem-refresh", "click");
  assert.equal(byId.get("onprem-job-state").textContent, "Нужен повторный доступ");
  assert.equal(byId.get("onprem-job-pause").hidden, false);
  assert.equal(byId.get("onprem-job-pause").textContent, "Перенос приостановлен.",
    "local pause completion remains visible independently from the credentials requirement");
  assert.equal(byId.get("onprem-job-blocked").hidden, true);
  assert.equal(byId.get("onprem-job-blocked-reason").textContent, "");
  assert.equal(byId.get("onprem-job-blocked-holder").textContent, "");
  assert.equal(byId.get("onprem-open-blocker").disabled, true);
  assert.equal(byId.get("onprem-resume").disabled, false, "a completed local pause offers credential recovery through Continue");
  assert.ok(requests.slice(beforeRestartPauseRefresh).every((request) => request.method === "GET"),
    "displaying the completed local pause never mutates a job or supplies credentials");
  assert.equal(holder.counts.outcome_unknown, 1);
  assert.equal(waiting.state, "awaiting_credentials");
  assert.equal(waiting.resume_state, "paused");
  const beforePausedBlockerNavigation = requests.length;
  await fire("onprem-open-blocker", "click");
  assert.equal(requests.length, beforePausedBlockerNavigation,
    "stale blocker data cannot open another job after the local pause completes");
  waiting.resume_state = "running";
  waiting.pause_requested = false;
  await fire("onprem-refresh", "click");
  const beforeRestartNavigation = requests.length;
  await fire("onprem-open-blocker", "click");
  assert.equal(byId.get("onprem-history").value, holder.id);
  assert.match(byId.get("onprem-entry-body").textContent, /Исход неизвестен/);
  assert.equal(holder.counts.outcome_unknown, 1);
  assert.equal(waiting.state, "awaiting_credentials");
  assert.ok(requests.slice(beforeRestartNavigation).every((request) => request.method === "GET"));
  byId.get("onprem-history").value = waiting.id;
  await fire("onprem-history", "change");
  waiting.blocked_by = {...waiting.blocked_by, job_id: "../../private"};
  await fire("onprem-refresh", "click");
  assert.equal(byId.get("onprem-job-blocked").hidden, false);
  assert.equal(byId.get("onprem-open-blocker").disabled, true);
  const beforeInvalidRestartNavigation = requests.length;
  await fire("onprem-open-blocker", "click");
  assert.equal(requests.length, beforeInvalidRestartNavigation);
  waiting.blocked_by = null;
  await fire("onprem-refresh", "click");
  assert.equal(byId.get("onprem-job-state").textContent, "Нужен повторный доступ");
  assert.equal(byId.get("onprem-job-blocked").hidden, true);
  assert.equal(byId.get("onprem-job-blocked-reason").textContent, "");
  assert.equal(byId.get("onprem-job-blocked-holder").textContent, "");
  waiting.state = "running";
  waiting.resume_state = null;
  waiting.blocked_by = {job_id: holder.id, state: "running", reason: "operation_in_progress"};
  await fire("onprem-refresh", "click");
  assert.match(byId.get("onprem-job-blocked-reason").textContent, /операцию или проверку результата/i);
  assert.match(byId.get("onprem-job-blocked-reason").textContent, /ожидает освобождения очереди/i);
  assert.doesNotMatch(byId.get("onprem-job-blocked-reason").textContent, /выполняется операция/i);
  assert.doesNotMatch(byId.get("onprem-job-blocked-reason").textContent, /неизвест/i);
  waiting.blocked_by = null;
  await fire("onprem-refresh", "click");
  assert.equal(byId.get("onprem-job-state").textContent, "Перенос выполняется");
  assert.equal(byId.get("onprem-job-blocked").hidden, true, "refresh removes a stale blocker status");
  assert.equal(byId.get("onprem-job-blocked-reason").textContent, "");
  assert.equal(byId.get("onprem-job-blocked-holder").textContent, "");
  assert.equal(byId.get("onprem-open-blocker").disabled, true);
  assert.doesNotMatch(byId.get("onprem-history").children.find((option) => option.value === waiting.id).textContent,
    /Ожидание другого задания/);
  for (const malformed of [
    {job_id: "<img src=x onerror=alert(1)>", state: "awaiting_credentials", reason: "operation_outcome_unknown"},
    {job_id: "../../private", state: "awaiting_credentials", reason: "operation_outcome_unknown"},
    {job_id: `https://example.test/${holder.id}`, state: "awaiting_credentials", reason: "operation_outcome_unknown"},
    {job_id: holder.id, state: "<unsafe-state>", reason: "operation_outcome_unknown"},
    {job_id: holder.id, state: "awaiting_credentials", reason: "<unsafe-reason>"},
    {job_id: waiting.id, state: "running", reason: "operation_in_progress"},
    {job_id: null, state: "awaiting_credentials", reason: "operation_outcome_unknown"},
    [], "<unsafe-blocker>", false,
  ]) {
    waiting.blocked_by = malformed;
    await fire("onprem-refresh", "click");
    assert.equal(byId.get("onprem-job-state").textContent, "Ожидание другого задания");
    assert.equal(byId.get("onprem-job-blocked").hidden, false);
    assert.equal(byId.get("onprem-open-blocker").disabled, true, "invalid blocker data cannot be navigated");
    assert.doesNotMatch(byId.get("onprem-job-blocked-reason").textContent + byId.get("onprem-job-blocked-holder").textContent,
      /<|onerror|https:|private/);
    const beforeInvalidNavigation = requests.length;
    await fire("onprem-open-blocker", "click");
    assert.equal(requests.length, beforeInvalidNavigation, "invalid blocker data never causes an API request");
  }
  waiting.blocked_by = {job_id: holder.id, state: holder.state, reason: "operation_outcome_unknown"};
  await fire("onprem-refresh", "click");
  byId.get("onprem-access-key").value = "";
  await fire("onprem-access-key", "input");
  assert.equal(byId.get("onprem-open-blocker").disabled, true, "blocker navigation requires access");
  const beforeUnauthenticatedNavigation = requests.length;
  await fire("onprem-open-blocker", "click");
  assert.equal(requests.length, beforeUnauthenticatedNavigation);
  byId.get("onprem-access-key").value = "test-key";
  waiting.state = "paused";
  await fire("onprem-refresh", "click");
  assert.equal(byId.get("onprem-job-state").textContent, "Приостановлено");
  assert.equal(byId.get("onprem-job-blocked").hidden, true, "paused jobs do not show transfer-lane blocking metadata");
  assert.equal(byId.get("onprem-open-blocker").disabled, true);
  byId.get("onprem-history").value = "";
  await fire("onprem-history", "change");
  assert.equal(byId.get("onprem-job-blocked").hidden, true);
  assert.equal(byId.get("onprem-job-blocked-holder").textContent, "");
  apiError = "fake-source-secret https://example.test/?token=private";
  await fire("onprem-refresh", "click");
  assert.doesNotMatch(byId.get("onprem-notice").textContent, /fake-source-secret|https:/);
  apiError = null;
  const failedReply = (status, detail, invalidJson = false) => ({
    ok: false, status, json: async () => {
      if (invalidJson) throw new Error("Mock invalid JSON");
      return {detail};
    },
  });
  const runPoll = async () => {await timers.at(-1)(); await flush();};
  apiOverride = () => failedReply(409, "unrecognized_rejection");
  await fire("onprem-refresh", "click");
  assert.match(byId.get("onprem-notice").textContent, /Запрос отклонён.*HTTP 409/,
    "unknown 4xx refusals must not claim the service is unavailable");
  assert.doesNotMatch(byId.get("onprem-notice").textContent, /недоступен|unrecognized_rejection/);
  const maliciousDetail = "fake-source-secret https://example.test/?token=private <img onerror=alert(1)>";
  apiOverride = () => failedReply(403, maliciousDetail);
  await fire("onprem-refresh", "click");
  assert.match(byId.get("onprem-notice").textContent, /HTTP 403/);
  assert.doesNotMatch(byId.get("onprem-notice").textContent, /fake-source-secret|https:|private|<|onerror/,
    "unknown error details never expose response text, URLs or secrets");
  for (const status of [400, 422, 429, 499]) {
    apiOverride = () => failedReply(status, null, true);
    await fire("onprem-refresh", "click");
    assert.match(byId.get("onprem-notice").textContent, new RegExp(`HTTP ${status}`));
    assert.doesNotMatch(byId.get("onprem-notice").textContent, /недоступен/);
  }
  for (const status of [0, 99, 600, 409.5, "409", "<unsafe-status>", null]) {
    apiOverride = () => failedReply(status, maliciousDetail);
    await fire("onprem-refresh", "click");
    assert.doesNotMatch(byId.get("onprem-notice").textContent, /HTTP|unsafe|fake-source-secret|https:|<|onerror/,
      "only integer HTTP statuses in the valid range may appear in error messages");
  }
  for (const [detail, message] of [
    ["job_not_running", "Пауза недоступна в текущем состоянии задания. Обновите задание."],
    ["job_not_resumable", "Задание сейчас нельзя продолжить. Обновите его состояние."],
    ["job_not_awaiting_credentials", "Задание сейчас не принимает доступы. Обновите его состояние."],
    ["plan_not_ready", "План ещё не готов. Обновите задание."],
    ["restart_config_mismatch", "Для нового переноса сохраните первоначальные параметры выбранного задания."],
  ]) {
    apiOverride = () => failedReply(409, detail);
    await fire("onprem-refresh", "click");
    assert.equal(byId.get("onprem-notice").textContent, message,
      "known state refusals retain an explicit explanation");
  }
  apiOverride = () => ({ok: true, status: 200, json: async () => {throw new Error("Mock invalid JSON");}});
  await fire("onprem-refresh", "click");
  assert.match(byId.get("onprem-notice").textContent, /Ответ сервера не удалось прочитать/);
  assert.doesNotMatch(byId.get("onprem-notice").textContent, /недоступен/,
    "malformed successful JSON responses are distinct from service downtime");
  apiOverride = null;
  await fire("onprem-refresh", "click");
  assert.equal(byId.get("onprem-notice").textContent, "");
  assert.equal(byId.get("onprem-poll-notice").hidden, true);
  apiOverride = () => failedReply(500, null, true);
  const beforeBackgroundReads = requests.filter((request) => request.method !== "GET").length;
  await runPoll();
  assert.equal(byId.get("onprem-poll-notice").hidden, false);
  assert.match(byId.get("onprem-poll-notice").textContent, /Сервис временно недоступен/);
  assert.equal(byId.get("onprem-notice").textContent, "",
    "background failures have a separate notice from command results");
  apiOverride = null;
  await runPoll();
  assert.equal(byId.get("onprem-poll-notice").hidden, true);
  assert.equal(byId.get("onprem-poll-notice").textContent, "",
    "a fully successful current refresh clears a transient background failure");
  assert.equal(requests.filter((request) => request.method !== "GET").length, beforeBackgroundReads);
  apiOverride = () => failedReply(409, "unrecognized_rejection");
  await fire("onprem-refresh", "click");
  const commandFailure = byId.get("onprem-notice").textContent;
  apiOverride = null;
  await runPoll();
  assert.equal(byId.get("onprem-notice").textContent, commandFailure,
    "healthy background reads never erase a command failure");
  apiOverride = () => failedReply(500, null, true);
  await runPoll();
  assert.equal(byId.get("onprem-notice").textContent, commandFailure);
  assert.equal(byId.get("onprem-poll-notice").hidden, false,
    "a future background failure remains visible alongside a command failure");
  apiOverride = null;
  await runPoll();
  assert.equal(byId.get("onprem-notice").textContent, commandFailure);
  assert.equal(byId.get("onprem-poll-notice").hidden, true);
  waiting.state = "awaiting_credentials";
  waiting.resume_state = "paused";
  waiting.blocked_by = null;
  byId.get("onprem-history").value = waiting.id;
  await fire("onprem-history", "change");
  const provideMockCredentials = async () => {
    waiting.state = "awaiting_credentials";
    waiting.resume_state = "paused";
    await fire("onprem-refresh", "click");
    byId.get("onprem-source-secret").value = "fake-source-secret";
    byId.get("onprem-destination-token").value = "fake-destination-token";
    byId.get("onprem-identity-confirmed").checked = true;
    await fire("onprem-identity-confirmed", "change");
    await fire("onprem-resume", "click");
  };
  await provideMockCredentials();
  const commandSuccess = byId.get("onprem-notice").textContent;
  assert.equal(commandSuccess, "Перенос продолжен.");
  await runPoll();
  assert.equal(byId.get("onprem-notice").textContent, commandSuccess);
  apiOverride = ({url}) => url.pathname.endsWith("/entries") ? failedReply(500, null, true) : null;
  await runPoll();
  assert.equal(byId.get("onprem-notice").textContent, commandSuccess);
  assert.equal(byId.get("onprem-poll-notice").hidden, false,
    "a successful job list does not clear a failed selected-job read");
  apiOverride = null;
  await runPoll();
  assert.equal(byId.get("onprem-notice").textContent, commandSuccess);
  assert.equal(byId.get("onprem-poll-notice").hidden, true);
  byId.get("onprem-history").value = holder.id;
  await fire("onprem-history", "change");
  let finishOldEvents;
  apiOverride = ({url}) => url.pathname === `/api/onprem/jobs/${holder.id}/events`
    ? new Promise((resolve) => {finishOldEvents = resolve;}) : null;
  const oldSelectionPoll = timers.at(-1)();
  await flush();
  assert.equal(typeof finishOldEvents, "function", "the old selection has an in-flight background read");
  byId.get("onprem-history").value = waiting.id;
  await fire("onprem-history", "change");
  apiOverride = null;
  await provideMockCredentials();
  finishOldEvents(failedReply(500, null, true));
  await oldSelectionPoll;
  await flush();
  assert.equal(byId.get("onprem-history").value, waiting.id);
  assert.equal(byId.get("onprem-notice").textContent, commandSuccess);
  assert.equal(byId.get("onprem-poll-notice").hidden, true,
    "a stale failure from an old selection never shows a new background error");
  const pollNode = byId.get("onprem-poll-notice");
  byId.delete("onprem-poll-notice");
  const beforeLegacyPolling = requests.filter((request) => request.method !== "GET").length;
  apiOverride = () => failedReply(500, null, true);
  await runPoll();
  assert.equal(byId.get("onprem-notice").textContent, commandSuccess);
  apiOverride = null;
  await runPoll();
  assert.equal(requests.filter((request) => request.method !== "GET").length, beforeLegacyPolling,
    "legacy markup without a background notice still polls without automatic mutations");
  await provideMockCredentials();
  assert.equal(byId.get("onprem-notice").textContent, commandSuccess,
    "new JS with legacy HTML retains normal command notices without crashing");
  byId.set("onprem-poll-notice", pollNode);
  assert.equal(holder.counts.outcome_unknown, 1);
  assert.ok(byId.get("onprem-stop"), "jobs expose a permanent local stop action");
  assert.ok(byId.get("onprem-restart"), "a stopped transfer can create a separate reviewed plan");
  const historicalParentConfig = {...clone(config), destination_base: "/Old archive",
    destination_root: "vd:shared-target-hash:disk:/Old archive/source@example.test", source_public: false};
  const stoppedParent = {...clone(holder), state: "needs_attention", stop_requested: false, stop_blocked: false,
    pause_requested: false, counts: {outcome_unknown: 1}, config: historicalParentConfig};
  allJobs = [stoppedParent, waiting];
  const parentEntries = [{...first, state: "outcome_unknown", attempt_id: "preserved-attempt"}];
  entrySets.set(stoppedParent.id, parentEntries);
  byId.get("onprem-history").value = stoppedParent.id;
  await fire("onprem-history", "change");
  formValid = false;
  await fire("onprem-source-secret", "input");
  assert.equal(byId.get("onprem-stop").disabled, false, "local stop needs no source or destination credentials");
  assert.equal(byId.get("onprem-restart").disabled, true);
  confirmStop = false;
  const beforeCancelledStop = requests.length;
  const beforeStopValidation = formValidationCalls;
  await fire("onprem-stop", "click");
  assert.equal(requests.length, beforeCancelledStop, "declining permanent stop performs no request");
  assert.equal(formValidationCalls, beforeStopValidation, "stop never validates the secret-bearing form");
  assert.match(confirmations.at(-1), /навсегда|окончательно/i);
  assert.match(confirmations.at(-1), /история, кеш и файлы сохраняются/i);
  assert.match(confirmations.at(-1), /сервер.*операци.*может продолжаться/i);
  assert.match(confirmations.at(-1), /локальные действия.*отменены/i);
  confirmStop = true;
  let finishPreStopEvents;
  let holdPreStopEvents = true;
  apiOverride = ({url}) => {
    if (url.pathname === `/api/onprem/jobs/${stoppedParent.id}/events` && holdPreStopEvents) {
      holdPreStopEvents = false;
      return new Promise((resolve) => {finishPreStopEvents = resolve;});
    }
    return null;
  };
  const preStopPoll = timers.at(-1)();
  await flush();
  await fire("onprem-stop", "click");
  const stop = requests.filter((request) => request.path.endsWith("/stop")).at(-1);
  assert.equal(stop.method, "POST");
  assert.equal(stop.body, null, "stop sends no form config, credentials or confirmation payload");
  assert.equal(formValidationCalls, beforeStopValidation);
  assert.equal(byId.get("onprem-job-state").textContent, "Остановка запрошена");
  assert.equal(byId.get("onprem-stop").disabled, true);
  assert.equal(byId.get("onprem-report").disabled, false, "reports remain accessible while stopping");
  assert.equal(byId.get("onprem-journal").disabled, false, "journal remains accessible while stopping");
  for (const id of ["onprem-start", "onprem-resume"]) {
    assert.equal(byId.get(id).disabled, true, "stopping blocks old-work mutations");
  }
  finishPreStopEvents({ok: true, status: 200, json: async () => ({events: [
    {sequence: 1000, created_at: "2026-10-11T12:00:03Z", type: "job_state", payload: {state: "running"}},
  ]})});
  await preStopPoll; await flush(); apiOverride = null;
  assert.equal(byId.get("onprem-job-state").textContent, "Остановка запрошена",
    "a pre-stop background snapshot cannot overwrite the accepted stop request");
  assert.doesNotMatch(byId.get("onprem-log").textContent, /1000 /);
  stoppedParent.state = "stopped";
  await fire("onprem-refresh", "click");
  assert.equal(byId.get("onprem-job-state").textContent, "Остановлено полностью");
  assert.equal(byId.get("onprem-stop").disabled, true);
  assert.equal(byId.get("onprem-restart").disabled, true, "restart needs newly validated credentials and owner confirmation");
  const beforeStoppedPoll = requests.filter((request) => request.method !== "GET").length;
  await runPoll();
  assert.equal(requests.filter((request) => request.method !== "GET").length, beforeStoppedPoll,
    "polling a stopped job never stops, restarts or resolves it automatically");
  assert.deepEqual(parentEntries, [{...first, state: "outcome_unknown", attempt_id: "preserved-attempt"}],
    "stop preserves original object and attempt history");
  byId.get("onprem-source-secret").value = "fresh-source-secret";
  byId.get("onprem-destination-token").value = "fresh-destination-token";
  byId.get("onprem-identity-confirmed").checked = true;
  formValid = true;
  await fire("onprem-identity-confirmed", "change");
  assert.equal(byId.get("onprem-restart").disabled, false);
  field("source_email").value = "changed-original@example.test";
  await fire(field("source_email"), "input");
  assert.equal(byId.get("onprem-restart").disabled, true, "restart cannot change the immutable source identity");
  const beforeInvalidRestart = requests.length;
  await fire("onprem-restart", "click");
  assert.equal(requests.length, beforeInvalidRestart, "changed original parameters cannot reach restart API");
  field("source_email").value = stoppedParent.config.source_email;
  await fire(field("source_email"), "input");
  let loseRestartResponse = true;
  const rememberedParent = clone(stoppedParent);
  apiOverride = ({url, method, body}) => {
    if (method !== "POST" || !url.pathname.endsWith("/restart") || !loseRestartResponse) return null;
    loseRestartResponse = false;
    const key = stoppedParent.id + ":" + body.idempotency_key;
    const child = {...clone(stoppedParent), id: "a0000000000000000000000000000001", state: "queued",
      stop_requested: false, full_stop_requested: false, stop_blocked: false, restart_of: stoppedParent.id, counts: {}, total_files: 0,
      total_bytes: 0, verified_bytes: 0, revision: 1, remote_root: null, error: null, config: {...clone(body.config), destination_base: "/", source_public: true, destination_root: "vd:shared-target-hash:disk:/source@example.test"}};
    restartedJobs.set(key, child); allJobs.unshift(child); entrySets.set(child.id, []);
    throw new Error("Lost reply after server accepted restart");
  };
  const beforeRestart = requests.length;
  await fire("onprem-restart", "click");
  assert.equal(byId.get("onprem-history").value, stoppedParent.id);
  assert.match(byId.get("onprem-notice").textContent, /Соединение прервалось/);
  const uncertainRestart = requests.filter((request) => request.path.endsWith("/restart")).at(-1);
  assert.deepEqual(uncertainRestart.body.config, stoppedParent.config,
    "restart sends the exact historical parent config for strict binding verification");
  assert.equal(uncertainRestart.body.credentials.destination_token, "fresh-destination-token");
  assert.match(uncertainRestart.body.idempotency_key, /^[0-9a-f-]{36}$/i);
  const otherParent = {...clone(stoppedParent), id: "c".repeat(32), state: "completed", stop_requested: false, full_stop_requested: false};
  allJobs.push(otherParent);
  byId.get("onprem-history").value = otherParent.id;
  await fire("onprem-history", "change");
  byId.get("onprem-source-secret").value = "other-fresh-source-secret";
  byId.get("onprem-destination-token").value = "other-fresh-destination-token";
  byId.get("onprem-identity-confirmed").checked = true;
  await fire("onprem-identity-confirmed", "change");
  apiOverride = ({url, method, body}) => {
    if (url.pathname !== `/api/onprem/jobs/${otherParent.id}/restart` || method !== "POST") return null;
    const key = otherParent.id + ":" + body.idempotency_key;
    const child = {...clone(otherParent), id: "b".repeat(32), state: "queued", restart_of: otherParent.id,
      stop_requested: false, config: {...clone(body.config), destination_base: "/", source_public: true, destination_root: "vd:shared-target-hash:disk:/source@example.test"}};
    restartedJobs.set(key, child); allJobs.unshift(child); entrySets.set(child.id, []);
    throw new Error("Lost another accepted restart reply");
  };
  await fire("onprem-restart", "click");
  assert.equal(byId.get("onprem-history").value, otherParent.id);
  byId.get("onprem-history").value = stoppedParent.id;
  await fire("onprem-history", "change");
  byId.get("onprem-source-secret").value = "fresh-source-secret";
  byId.get("onprem-destination-token").value = "current-fresh-destination-token";
  byId.get("onprem-identity-confirmed").checked = true;
  await fire("onprem-identity-confirmed", "change");
  let finishRestartHistory;
  apiOverride = ({url, method}) => url.pathname === "/api/onprem/jobs" && method === "GET"
    ? new Promise((resolve) => {finishRestartHistory = resolve;}) : null;
  await fire("onprem-restart", "click");
  const retriedRestart = requests.filter((request) => request.path.endsWith("/restart")).at(-1);
  assert.equal(retriedRestart.body.idempotency_key, uncertainRestart.body.idempotency_key,
    "uncertain restart retries keep their key after another parent is restarted and secrets refreshed");
  assert.equal(retriedRestart.body.credentials.destination_token, "current-fresh-destination-token");
  assert.equal(restartedJobs.size, 2, "each parent's lost response followed by retry keeps exactly one child");
  const newJob = [...restartedJobs.values()][0];
  assert.equal(byId.get("onprem-history").value, newJob.id);
  assert.equal(byId.get("onprem-restart").disabled, true, "restart stays busy until the refreshed list arrives");
  const beforeDuplicateRestart = requests.length;
  await fire("onprem-restart", "click");
  assert.equal(requests.length, beforeDuplicateRestart, "duplicate clicks while refreshing cannot create another job");
  assert.equal(field("source_email").value, config.source_email);
  assert.equal(byId.get("onprem-identity-confirmed").checked, true,
    "selecting the new job retains confirmed form access for manual Start");
  finishRestartHistory({ok: true, status: 200, json: async () => ({jobs: clone(allJobs)})});
  await flush(); apiOverride = null;
  assert.ok(byId.get("onprem-history").children.some((option) => option.value === stoppedParent.id));
  assert.ok(byId.get("onprem-history").children.some((option) => option.value === newJob.id));
  assert.deepEqual(stoppedParent, rememberedParent, "restart never mutates its original job");
  assert.deepEqual(entrySets.get(stoppedParent.id), parentEntries);
  assert.equal(newJob.restart_of, stoppedParent.id);
  assert.equal(newJob.state, "queued");
  assert.equal(newJob.config.destination_base, "/");
  assert.equal(newJob.config.source_public, true);
  assert.doesNotMatch(byId.get("onprem-destination-preview").textContent, /Old archive/,
    "new child selection clears the historical current-job preview");
  assert.equal(newJob.config.strategy, historicalParentConfig.strategy);
  assert.doesNotMatch(byId.get("onprem-job-destination").textContent, /Old archive/);
  assert.ok(requests.slice(beforeRestart).filter((request) => request.method !== "GET")
    .every((request) => request.path.endsWith("/restart")), "restart never automatically starts or resumes either job");
  assert.match(byId.get("onprem-notice").textContent, /план.*проверь|проверь.*план/i);
  assert.match(byId.get("onprem-notice").textContent, /Начать перенос/);
  stoppedParent.full_stop_requested = false;
  stoppedParent.stop_blocked = true;
  const finalMoveAttempt = "f".repeat(32);
  stoppedParent.stop_blocked_attempt_ids = [finalMoveAttempt];
  const recoveryEntries = Array.from({length: 205}, (_, index) => ({...first,
    id: `recovery-${index}`, state: "planned", attempt_id: null, source_path: `/planned-${index}`}));
  recoveryEntries[0] = {...first, state: "outcome_unknown", attempt_id: "e".repeat(32), source_path: "/isolated-upload"};
  recoveryEntries[204] = {...first, state: "committing", attempt_id: finalMoveAttempt, source_path: "/move-intent"};
  entrySets.set(stoppedParent.id, recoveryEntries);
  stoppedParent.total_files = 205;
  stoppedParent.counts = {planned: 203, committing: 1, outcome_unknown: 1};
  waiting.state = "running";
  waiting.blocked_by = {job_id: stoppedParent.id, state: "stopped", reason: "operation_outcome_unknown"};
  byId.get("onprem-history").value = waiting.id;
  await fire("onprem-history", "change");
  assert.equal(byId.get("onprem-open-blocker").disabled, false,
    "a stopped holder with an unknown final move remains navigable");
  assert.match(byId.get("onprem-job-blocked-holder").textContent, /^Задание от .* · Остановлено/,
    "a blocker on the loaded history page is identified by its creation date");
  assert.equal(byId.get("onprem-job-blocked-holder").textContent.includes(stoppedParent.id), false);
  await fire("onprem-open-blocker", "click");
  assert.equal(byId.get("onprem-history").value, stoppedParent.id);
  assert.match(byId.get("onprem-job-stop").textContent, /нажмите «Остановить»/);
  assert.equal(byId.get("onprem-stop").disabled, false);
  assert.equal(byId.get("onprem-resume").disabled, true);
  assert.equal(byId.get("onprem-start").disabled, true);
  const beforeFullStop = requests.length;
  await fire("onprem-stop", "click");
  assert.deepEqual(requests.slice(beforeFullStop).filter((request) => request.method !== "GET").map((request) => request.path.split("/").at(-1)), ["stop"]);
  assert.equal(stoppedParent.full_stop_requested, true, "historical blockers can be fully stopped without manual outcome resolution");
  const legacyConfig = clone(config);
  delete legacyConfig.destination_type;
  delete legacyConfig.destination_vd_hash;
  delete legacyConfig.destination_base;
  const legacy = {...clone(job), id: "e".repeat(32), state: "paused", stop_requested: false,
    counts: {planned: 1}, config: legacyConfig};
  const unchangedLegacy = clone(legacyConfig);
  allJobs = [legacy, job];
  byId.get("onprem-history").value = legacy.id;
  await fire("onprem-history", "change");
  assert.equal(byId.get("onprem-legacy-note").hidden, false);
  assert.match(byId.get("onprem-legacy-note").textContent, /личный Диск/);
  assert.match(byId.get("onprem-job-destination").textContent, /Личный Диск/);
  const beforeLegacyActions = requests.length;
  byId.get("onprem-source-secret").value = "legacy-fake-secret";
  byId.get("onprem-destination-token").value = "legacy-fake-token";
  byId.get("onprem-identity-confirmed").checked = true;
  await fire("onprem-identity-confirmed", "change");
  for (const id of ["onprem-start", "onprem-resume", "onprem-restart"]) {
    assert.equal(byId.get(id).disabled, true, "legacy jobs cannot silently become shared transfers");
    await fire(id, "click");
  }
  assert.equal(requests.length, beforeLegacyActions, "even manually dispatched legacy mutation events make no request");
  assert.equal(byId.get("onprem-stop").disabled, false);
  assert.equal(byId.get("onprem-report").disabled, false);
  assert.equal(byId.get("onprem-journal").disabled, false);
  await fire("onprem-report", "click");
  await fire("onprem-journal", "click");
  await fire("onprem-stop", "click");
  assert.ok(requests.slice(beforeLegacyActions).some((request) => request.path.endsWith("/report")));
  assert.ok(requests.slice(beforeLegacyActions).some((request) => request.path === `/api/onprem/jobs/${legacy.id}/journal`));
  assert.ok(requests.slice(beforeLegacyActions).some((request) => request.path.endsWith("/stop")));
  assert.deepEqual(legacy.config, unchangedLegacy, "legacy personal binding is never rewritten by the shared-only form");
  legacy.state = "stopped";
  legacy.full_stop_requested = false;
  legacy.stop_blocked = true;
  legacy.stop_blocked_attempt_ids = [finalMoveAttempt];
  legacy.counts = {committing: 1};
  entrySets.set(legacy.id, [{...first, state: "committing", attempt_id: finalMoveAttempt}]);
  await fire("onprem-refresh", "click");
  assert.equal(byId.get("onprem-stop").disabled, false);
  assert.equal(byId.get("onprem-resume").disabled, true);
  assert.equal(byId.get("onprem-restart").disabled, true);
  assert.equal(byId.get("onprem-report").disabled, false);
  assert.equal(byId.get("onprem-journal").disabled, false);
  assert.deepEqual(legacy.config, unchangedLegacy);
  job.state = "paused";
  byId.get("onprem-history").value = job.id;
  await fire("onprem-history", "change");
  field("destination_vd_hash").value = "a-different-disk";
  await fire(field("destination_vd_hash"), "input");
  byId.get("onprem-identity-confirmed").checked = true;
  const beforeChangedBinding = requests.length;
  await fire("onprem-resume", "click");
  assert.equal(requests.length, beforeChangedBinding, "automatic access cannot alter the destination disk binding");
  assert.equal(byId.get("onprem-resume").disabled, true);
  field("destination_vd_hash").value = config.destination_vd_hash;
  await fire(field("destination_vd_hash"), "input");
  preflightDestination = {...preflightDestination, free_bytes: null};
  await fire("onprem-form", "submit");
  assert.match(byId.get("onprem-destination-check").textContent, /Свободное место неизвестно/);
  preflightDestination = {...preflightDestination, permissions: {read: true, write: true}};
  await fire("onprem-form", "submit");
  assert.equal(byId.get("onprem-destination-check").hidden, false,
    "documented boolean permissions are accepted after preflight");
  assert.match(byId.get("onprem-destination-check").textContent, /Чтение и запись разрешены/);
  delete preflightDestination.permissions;
  await fire("onprem-form", "submit");
  assert.equal(byId.get("onprem-destination-check").hidden, false,
    "successful backend preflight may omit optional rights metadata");
  assert.match(byId.get("onprem-destination-check").textContent, /API не указал права/);
  assert.doesNotMatch(byId.get("onprem-destination-check").textContent, /Чтение и запись разрешены/,
    "absent metadata never becomes a verified rights claim");
  for (const permissions of [["read"], {read: true, write: false}]) {
    preflightDestination = {...preflightDestination, permissions};
    const createCount = requests.filter((request) => request.path === "/api/onprem/jobs" && request.method === "POST").length;
    await fire("onprem-form", "submit");
    assert.equal(requests.filter((request) => request.path === "/api/onprem/jobs" && request.method === "POST").length,
      createCount, "explicit read-only access prevents creating a transfer");
    assert.match(byId.get("onprem-notice").textContent, /ресурс заблокирован или доступ ограничен/);
    assert.equal(field("strategy").value, "staged", "read-only access keeps the fixed transfer route");
  }
  preflightDestination = {...preflightDestination, permissions: ["read", "write"]};
  let resolvePendingPreflight;
  apiOverride = ({url}) => {
    if (url.pathname !== "/api/onprem/preflight") return null;
    return new Promise((resolve) => {resolvePendingPreflight = resolve;});
  };
  const beforeStalePreflight = requests.filter((request) => request.path === "/api/onprem/jobs" && request.method === "POST").length;
  const pendingPreflight = fire("onprem-form", "submit");
  await flush();
  assert.equal(typeof resolvePendingPreflight, "function");
  byId.get("onprem-destination-token").value = "changed-during-preflight";
  await fire("onprem-destination-token", "input");
  resolvePendingPreflight({ok: true, status: 200, json: async () => ({destination: clone(preflightDestination)})});
  await pendingPreflight;
  await flush();
  apiOverride = null;
  assert.equal(byId.get("onprem-destination-check").hidden, true, "a stale preflight never displays verified access");
  assert.equal(requests.filter((request) => request.path === "/api/onprem/jobs" && request.method === "POST").length,
    beforeStalePreflight, "changing credentials during preflight prevents creating the transfer");
  assert.match(byId.get("onprem-notice").textContent, /настройкам задания/);
  field("strategy").value = "staged";
  await fire(field("strategy"), "change");
  await fire("onprem-form", "submit");
  const stagedCreate = requests.filter((request) => request.path === "/api/onprem/jobs" && request.method === "POST").at(-1);
  assert.equal(stagedCreate.body.config.strategy, "staged", "explicitly chosen temporary-saving strategy is preserved");
  assert.equal(stagedCreate.body.config.destination_type, "shared");
  assert.equal(Object.hasOwn(stagedCreate.body.config, "destination_root"), false);
  for (const code of ["invalid_destination_shared_disk", "destination_shared_access_required", "destination_shared_disk_mismatch", "destination_read_only"]) {
    apiError = code;
    await fire("onprem-form", "submit");
    assert.match(byId.get("onprem-notice").textContent, /общ|диск|метк|ресурс/i);
    assert.doesNotMatch(byId.get("onprem-notice").textContent, /Операция не выполнена/);
  }
  apiError = null;
  assert.deepEqual(storageWrites, []);
  console.log("PASS: staged-only forms and payloads, historical URL read/stop/export, disabled release gate, explicit Onprem opt-in, configuration failure fallback, asynchronous plan/history, pagination, credential recovery, file-row retry, permanent local stop, idempotent new-job restart, historical blocker full stop, manual Start, stale-read protection and secret-safe rendering/storage");
})().catch((error) => {console.error(error); process.exitCode = 1;});
