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
  destination_root: "disk:/source@example.test", strategy: "url", identity_confirmed: true,
};
const job = {id: "job-1", state: "queued", revision: 1, config, counts: {planned: 1}, directory_counts: {},
  total_files: 1, total_directories: 0, total_bytes: 18, verified_bytes: 0, remote_root: "disk:/.transfer/job-1"};
const first = {id: "entry-1", kind: "file", source_path: "/<unsafe>.txt", destination_path: "disk:/copy/<unsafe>.txt",
  size: 18, md5: "0123456789abcdef0123456789abcdef", state: "planned", attempt_id: "attempt-1"};
let allEntries = [first];
let allJobs = [job];
let operationEvents = [];
const entrySets = new Map();
let apiError = null;
let apiOverride = null;
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
  if (url.pathname === "/api/onprem/jobs") {
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
  else if (url.pathname.endsWith("/resume")) {job.state = "ready"; job.revision += 1; value = job;}
  else if (url.pathname.endsWith("/resolve")) {
    if (activeJob.state === "stopped") {
      activeJob.stop_blocked = false; activeJob.stop_blocked_attempt_ids = []; activeJob.revision += 1;
      const resolved = (entrySets.get(activeJob.id) || []).find((entry) => entry.attempt_id === body.attempt_id);
      if (resolved) resolved.state = "failed";
      value = activeJob;
    } else {job.state = "paused"; first.state = "failed"; job.counts = {failed: 1}; job.revision += 1; value = job;}
  }
  else if (url.pathname.endsWith("/retry")) {job.state = "running"; first.state = "transferring"; value = job;}
  else if (url.pathname.endsWith("/stop")) {
    activeJob.stop_requested = true; activeJob.revision += 1; value = activeJob;
  }
  else if (url.pathname.endsWith("/restart")) {
    const key = activeJob.id + ":" + body.idempotency_key;
    if (!restartedJobs.has(key)) {
      const child = {...clone(activeJob), id: `a${String(restartedJobs.size + 1).padStart(31, "0")}`,
        state: "queued", stop_requested: false, stop_blocked: false, restart_of: activeJob.id,
        revision: 1, counts: {}, total_files: 0, total_bytes: 0, verified_bytes: 0,
        remote_root: null, error: null, config: clone(body.config)};
      restartedJobs.set(key, child); allJobs.unshift(child); entrySets.set(child.id, []);
    }
    value = restartedJobs.get(key);
  }
  else if (url.pathname.endsWith("/cleanup")) value = {removed: 1, remote_root: job.remote_root,
    remote_attempts: [{id: first.attempt_id, remote_path: "disk:/.transfer/job-1/entry-1/attempt-1/payload", phase: "failed"}]};
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
    assert.equal(ui.tab.hidden, false, "release tab is visible before pilot opt-in");
    assert.equal(ui.tab.disabled, true);
    assert.equal(ui.tab.title, "В разработке");
    assert.equal(ui.panel.hidden, true);
    assert.equal(ui.requests.length, 0);
  }
  for (const config of [{}, {onprem_enabled: false}, null]) {
    const ui = loadGate(Promise.resolve(config));
    await flush();
    assertDisabled(ui);
    await ui.tab.click();
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
  assert.equal(pilot.tab.disabled, false, "explicit pilot opt-in enables the tab");
  assert.equal(pilot.tab.title, "Пилотный режим");
  assert.equal(pilot.panel.hidden, false);
  assert.equal(pilot.byId.get("onprem-access").hidden, false);
  assert.equal(pilot.requests.length, 0, "enabling a tab does not launch background work");
}

(async () => {
  await verifyReleaseGate();
  await flush();
  assert.equal(document.querySelector('.mode-tab[data-mode="onprem"]').hidden, false);
  assert.equal(document.querySelector('.mode-tab[data-mode="onprem"]').disabled, false);
  assert.equal(document.querySelector('.mode-tab[data-mode="onprem"]').title, "Пилотный режим");
  assert.equal(panel.hidden, false);
  assert.equal(byId.get("onprem-access").hidden, false);
  assert.equal(byId.get("onprem-plan").disabled, true, "missing access key and identity confirmation block creation");
  const tab = document.querySelector('.mode-tab[data-mode="onprem"]');
  tab.classList.add("active");
  await fire(tab, "click");
  assert.equal(byId.get("log").hidden, true);
  assert.equal(requests.length, 0, "no unauthenticated polling requests");
  byId.get("onprem-access-key").value = "test-key";
  for (const [key, value] of Object.entries(config)) if (key !== "identity_confirmed") field(key).value = value;
  byId.get("onprem-source-secret").value = "fake-source-secret";
  byId.get("onprem-destination-token").value = "fake-destination-token";
  byId.get("onprem-identity-confirmed").checked = true;
  await fire("onprem-identity-confirmed", "change");
  assert.equal(byId.get("onprem-plan").disabled, false);
  await fire("onprem-form", "submit");
  const create = requests.find((request) => request.method === "POST" && request.path === "/api/onprem/jobs");
  assert.equal(create.body.config.identity_confirmed, true);
  assert.equal(create.body.credentials.source_client_secret, "fake-source-secret");
  assert.match(create.body.idempotency_key, /^[0-9a-f-]{36}$/i);
  assert.equal(byId.get("onprem-start").disabled, true, "queued plan cannot start");
  assert.match(byId.get("onprem-entry-body").textContent, /<unsafe>\.txt/);
  job.state = "ready";
  job.total_files = 205;
  job.counts = {planned: 205};
  allEntries = Array.from({length: 205}, (_, index) => ({...first, id: `entry-${index + 1}`, source_path: `/file-${index + 1}`, attempt_id: null}));
  allEntries[204] = {...first, id: "entry-205", source_path: "/last-file", state: "outcome_unknown"};
  await fire("onprem-refresh", "click");
  assert.equal(byId.get("onprem-entry-body").children.length, 100, "plan page is bounded");
  assert.equal(byId.get("onprem-entry-next").disabled, false, "additional plan pages are reachable");
  assert.match(byId.get("onprem-entry-count").textContent, /205/);
  assert.equal(byId.get("onprem-resolve-panel").hidden, false, "unknown attempt on another page is visible");
  await fire("onprem-entry-next", "click");
  await fire("onprem-entry-next", "click");
  assert.match(byId.get("onprem-entry-body").textContent, /last-file/);
  assert.equal(byId.get("onprem-entry-next").disabled, true);
  job.total_files = 1; job.counts = {planned: 1}; allEntries = [first];
  await fire("onprem-entry-previous", "click"); await fire("onprem-entry-previous", "click");
  assert.equal(byId.get("onprem-start").disabled, false);
  await fire("onprem-start", "click");
  assert.equal(byId.get("onprem-pause").disabled, false);
  await fire("onprem-pause", "click");
  assert.equal(byId.get("onprem-resume").disabled, false);
  await fire("onprem-resume", "click");
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
  ];
  await fire("onprem-refresh", "click");
  const operationLog = byId.get("onprem-log").textContent;
  assert.match(operationLog, /path-operation_1/);
  assert.match(operationLog, /\?id=query-operation_2/, "query-form operation references remain inspectable");
  assert.match(operationLog, /entry:legacy/);
  assert.match(operationLog, /attempt\.legacy/);
  assert.doesNotMatch(operationLog, /entry-private|attempt-private|operation\.invalid|private-url|private-param|fake-access|private-fragment|x{257}/,
    "operation references have a stricter filter than entry and attempt IDs");
  assert.equal(byId.get("onprem-retry").disabled, true);
  assert.equal(byId.get("onprem-resume").disabled, true);
  byId.get("onprem-resolve-status").value = "failed";
  byId.get("onprem-resolve-evidence").value = "404 after timeout";
  byId.get("onprem-resolve-confirmed").checked = true;
  await fire("onprem-resolve-confirmed", "change");
  assert.equal(byId.get("onprem-resolve").disabled, true, "timeout/404 is not terminal evidence");
  const beforeResolution = requests.filter((request) => request.path.endsWith("/resolve")).length;
  byId.get("onprem-resolve-evidence").value = "Administrator logs confirm terminal failure.";
  for (const operation of ["https://disk.example.test/operations?id=operation-1", "/operations/operation-1",
    "?id=operation-1&token=fake-access", "?id=operation-1#extra", "?id=", "?id=operation%2D1",
    "?id=operation 1", "operation.invalid", "operation:invalid", "x".repeat(257), "?id=" + "x".repeat(257)]) {
    byId.get("onprem-resolve-operation").value = operation;
    await fire("onprem-resolve-operation", "input");
    assert.equal(byId.get("onprem-resolve").disabled, true, "unsafe operation references block resolution even with terminal evidence");
    await fire("onprem-resolve", "click");
  }
  assert.equal(requests.filter((request) => request.path.endsWith("/resolve")).length, beforeResolution,
    "invalid operation references never reach the resolution API");
  byId.get("onprem-resolve-evidence").value = "404 after timeout";
  for (const operation of ["x".repeat(256), "?id=" + "x".repeat(256), "?id=query-operation_2"]) {
    byId.get("onprem-resolve-operation").value = operation;
    await fire("onprem-resolve-operation", "input");
    assert.equal(byId.get("onprem-resolve").disabled, false, "both safe operation forms accept IDs up to 256 characters");
  }
  assert.equal(requests.filter((request) => request.path.endsWith("/resolve")).length, beforeResolution,
    "entering an operation reference does not resolve an unknown attempt automatically");
  const queryRevision = job.revision;
  apiError = "invalid_operation_reference";
  await fire("onprem-resolve", "click");
  const queryResolve = requests.filter((request) => request.path.endsWith("/resolve")).at(-1);
  assert.equal(queryResolve.body.operation_id, "?id=query-operation_2", "resolution preserves the query-form reference");
  assert.equal(queryResolve.body.revision, queryRevision);
  assert.equal(queryResolve.body.confirmed, true);
  assert.equal(queryResolve.body.terminal_status, undefined);
  assert.equal(queryResolve.body.evidence, undefined);
  assert.equal(first.state, "outcome_unknown", "rejected operation lookup preserves the unknown outcome");
  assert.equal(byId.get("onprem-retry").disabled, true);
  assert.equal(byId.get("onprem-resume").disabled, true);
  apiError = null;
  byId.get("onprem-resolve-operation").value = "operation-1";
  await fire("onprem-resolve-operation", "input");
  const revision = job.revision;
  assert.equal(byId.get("onprem-resolve").disabled, false);
  await fire("onprem-resolve", "click");
  const resolve = requests.filter((request) => request.path.endsWith("/resolve")).at(-1);
  assert.equal(resolve.body.operation_id, "operation-1");
  assert.equal(resolve.body.revision, revision);
  assert.equal(resolve.body.confirmed, true);
  assert.equal(resolve.body.terminal_status, undefined, "operation lookup and terminal evidence are mutually exclusive");
  assert.equal(byId.get("onprem-retry").disabled, false);
  byId.get("onprem-retry-strategy").value = "staged";
  await fire("onprem-retry", "click");
  assert.equal(requests.find((request) => request.path.endsWith("/retry")).body.strategy, "staged");
  job.state = "awaiting_credentials";
  await fire("onprem-refresh", "click");
  byId.get("onprem-history").value = "job-1";
  await fire("onprem-history", "change");
  assert.equal(byId.get("onprem-source-secret").value, "");
  assert.equal(byId.get("onprem-destination-token").value, "");
  assert.equal(byId.get("onprem-identity-confirmed").checked, false);
  assert.equal(byId.get("onprem-resume").disabled, true);
  byId.get("onprem-source-secret").value = "new-fake-source-secret";
  byId.get("onprem-destination-token").value = "new-fake-destination-token";
  byId.get("onprem-identity-confirmed").checked = true;
  await fire("onprem-credentials", "click");
  assert.equal(requests.find((request) => request.path.endsWith("/credentials")).method, "PUT");
  assert.equal(byId.get("onprem-resume").disabled, false);
  job.state = "paused";
  await fire("onprem-refresh", "click");
  await fire("onprem-cleanup", "click");
  assert.match(byId.get("onprem-remote-attempts").textContent, /attempt-1\/payload/);
  await fire("onprem-report", "click");
  assert.equal(requests.every((request) => request.headers["X-Onprem-Key"] === "test-key"), true);
  assert.doesNotMatch(byId.get("onprem-log").textContent, /fake-source-secret|fake-destination-token|fake-private-token/);
  assert.deepEqual(storageWrites, [], "no on-prem secrets or access key are stored in browser storage");
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
  assert.equal(byId.get("onprem-job-id").textContent, oldJob.id);
  assert.equal(byId.get("onprem-resolve-panel").hidden, false, "the older unresolved job can be inspected");
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
  assert.match(byId.get("onprem-job-blocked-reason").textContent, /исход.*неизвест/i);
  assert.match(byId.get("onprem-job-blocked-holder").textContent, new RegExp(holder.id));
  assert.match(byId.get("onprem-job-blocked-holder").textContent, /Нужен повторный доступ/);
  assert.equal(byId.get("onprem-open-blocker").disabled, false);
  assert.equal(byId.get("onprem-pause").disabled, false, "waiting jobs can still request a local pause");
  assert.equal(byId.get("onprem-resume").disabled, true);
  assert.equal(byId.get("onprem-retry").disabled, true);
  assert.match(byId.get("onprem-history").children.find((option) => option.value === waiting.id).textContent,
    /Ожидание другого задания/);
  const beforeBlockerNavigation = requests.length;
  await fire("onprem-open-blocker", "click");
  assert.equal(byId.get("onprem-job-id").textContent, holder.id,
    "the blocker can be opened directly even when it is outside the current history page");
  assert.equal(byId.get("onprem-resolve-panel").hidden, false);
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
  assert.match(byId.get("onprem-job-blocked-reason").textContent, /исход.*неизвест/i);
  assert.equal(byId.get("onprem-open-blocker").disabled, false);
  assert.equal(byId.get("onprem-resume").disabled, true, "inspecting a blocker does not supply credentials");
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
  assert.equal(byId.get("onprem-resume").disabled, true);
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
  assert.equal(byId.get("onprem-job-id").textContent, holder.id);
  assert.equal(byId.get("onprem-resolve-panel").hidden, false);
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
    ["operation_reference_mismatch", "У этой операции уже сохранён другой ID. Используйте ID из журнала задания."],
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
    byId.get("onprem-source-secret").value = "fake-source-secret";
    byId.get("onprem-destination-token").value = "fake-destination-token";
    byId.get("onprem-identity-confirmed").checked = true;
    await fire("onprem-credentials", "click");
  };
  await provideMockCredentials();
  const commandSuccess = byId.get("onprem-notice").textContent;
  assert.equal(commandSuccess, "Доступ предоставлен. Продолжите задание.");
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
  assert.equal(byId.get("onprem-job-id").textContent, waiting.id);
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
  const stoppedParent = {...clone(holder), state: "needs_attention", stop_requested: false, stop_blocked: false,
    pause_requested: false, counts: {outcome_unknown: 1}, config: clone(config)};
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
  assert.match(confirmations.at(-1), /не удал/i);
  assert.match(confirmations.at(-1), /удалённ|сервер.*загруз/i);
  assert.match(confirmations.at(-1), /техническ/i);
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
  for (const id of ["onprem-start", "onprem-resume", "onprem-retry", "onprem-credentials", "onprem-resolve"]) {
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
  assert.equal(byId.get("onprem-job-state").textContent, "Остановлено");
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
  field("strategy").value = "staged";
  await fire(field("strategy"), "change");
  assert.equal(byId.get("onprem-restart").disabled, true, "restart cannot change the immutable original strategy");
  const beforeInvalidRestart = requests.length;
  await fire("onprem-restart", "click");
  assert.equal(requests.length, beforeInvalidRestart, "changed original parameters cannot reach restart API");
  field("strategy").value = "url";
  await fire(field("strategy"), "change");
  let loseRestartResponse = true;
  const rememberedParent = clone(stoppedParent);
  apiOverride = ({url, method, body}) => {
    if (method !== "POST" || !url.pathname.endsWith("/restart") || !loseRestartResponse) return null;
    loseRestartResponse = false;
    const key = stoppedParent.id + ":" + body.idempotency_key;
    const child = {...clone(stoppedParent), id: "a0000000000000000000000000000001", state: "queued",
      stop_requested: false, stop_blocked: false, restart_of: stoppedParent.id, counts: {}, total_files: 0,
      total_bytes: 0, verified_bytes: 0, revision: 1, remote_root: null, error: null, config: clone(body.config)};
    restartedJobs.set(key, child); allJobs.unshift(child); entrySets.set(child.id, []);
    throw new Error("Lost reply after server accepted restart");
  };
  const beforeRestart = requests.length;
  await fire("onprem-restart", "click");
  assert.equal(byId.get("onprem-job-id").textContent, stoppedParent.id);
  assert.match(byId.get("onprem-notice").textContent, /Соединение прервалось/);
  const uncertainRestart = requests.filter((request) => request.path.endsWith("/restart")).at(-1);
  assert.deepEqual(uncertainRestart.body.config, config);
  assert.equal(uncertainRestart.body.credentials.destination_token, "fresh-destination-token");
  assert.match(uncertainRestart.body.idempotency_key, /^[0-9a-f-]{36}$/i);
  const otherParent = {...clone(stoppedParent), id: "c".repeat(32), state: "completed", stop_requested: false};
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
      stop_requested: false, config: clone(body.config)};
    restartedJobs.set(key, child); allJobs.unshift(child); entrySets.set(child.id, []);
    throw new Error("Lost another accepted restart reply");
  };
  await fire("onprem-restart", "click");
  assert.equal(byId.get("onprem-job-id").textContent, otherParent.id);
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
  assert.equal(byId.get("onprem-job-id").textContent, newJob.id);
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
  assert.ok(requests.slice(beforeRestart).filter((request) => request.method !== "GET")
    .every((request) => request.path.endsWith("/restart")), "restart never automatically starts or resumes either job");
  assert.match(byId.get("onprem-notice").textContent, /план.*проверь|проверь.*план/i);
  assert.match(byId.get("onprem-notice").textContent, /Начать перенос/);
  stoppedParent.stop_blocked = true;
  const finalMoveAttempt = "f".repeat(32);
  stoppedParent.stop_blocked_attempt_ids = [finalMoveAttempt];
  stoppedParent.stop_resolvable_attempt_ids = [finalMoveAttempt];
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
  assert.match(byId.get("onprem-job-blocked-holder").textContent, /Остановлено/);
  await fire("onprem-open-blocker", "click");
  assert.equal(byId.get("onprem-job-id").textContent, stoppedParent.id);
  assert.match(byId.get("onprem-job-stop").textContent, /перемещ|итогов/i);
  assert.equal(byId.get("onprem-credentials").disabled, false,
    "stopped final-move holders can attach access only for outcome recovery");
  assert.equal(byId.get("onprem-resolve-attempt").children.length, 1,
    "stopped final-move recovery excludes unknown isolated uploads in the same job");
  assert.equal(byId.get("onprem-resolve-attempt").value, finalMoveAttempt);
  assert.doesNotMatch(byId.get("onprem-entry-body").textContent, /move-intent/,
    "the recoverable committing attempt can be outside the current plan page");
  byId.get("onprem-source-secret").value = "recovery-source-secret";
  byId.get("onprem-destination-token").value = "recovery-destination-token";
  byId.get("onprem-identity-confirmed").checked = true;
  await fire("onprem-credentials", "click");
  assert.match(byId.get("onprem-notice").textContent, /остаётся остановленным/);
  byId.get("onprem-resolve-operation").value = "final-operation";
  byId.get("onprem-resolve-confirmed").checked = true;
  await fire("onprem-resolve-operation", "input");
  assert.equal(byId.get("onprem-resolve").disabled, false,
    "a stopped holder keeps the explicit final-move resolution path");
  assert.equal(byId.get("onprem-resume").disabled, true);
  assert.equal(byId.get("onprem-start").disabled, true);
  assert.equal(byId.get("onprem-retry").disabled, true);
  apiError = "credentials_required";
  await fire("onprem-resolve", "click");
  assert.match(byId.get("onprem-notice").textContent, /проверки итогового перемещения/);
  assert.doesNotMatch(byId.get("onprem-notice").textContent, /продолжите/i,
    "stopped recovery never instructs the operator to resume permanently stopped work");
  apiError = null;
  const beforeOldWork = requests.length;
  for (const id of ["onprem-start", "onprem-resume", "onprem-retry"]) await fire(id, "click");
  assert.equal(requests.length, beforeOldWork, "stopped recovery never starts old writes even through stale action handlers");
  apiOverride = ({url, method}) => {
    if (!url.pathname.endsWith("/resolve") || method !== "POST") return null;
    recoveryEntries[204].state = "waiting_operation";
    return {ok: true, status: 200, json: async () => clone(stoppedParent)};
  };
  await fire("onprem-resolve", "click"); apiOverride = null;
  assert.equal(stoppedParent.state, "stopped", "final-move resolution does not resume a stopped transfer");
  assert.equal(requests.filter((request) => request.path.endsWith("/resolve")).at(-1).body.attempt_id, finalMoveAttempt);
  assert.equal(byId.get("onprem-resume").disabled, true);
  assert.equal(stoppedParent.stop_blocked, true, "an identified operation in progress continues to hold the queue");
  assert.equal(byId.get("onprem-resolve-panel").hidden, false,
    "a stopped known final move retains administrator evidence fallback if polling is unavailable");
  assert.equal(byId.get("onprem-resolve-attempt").value, finalMoveAttempt);
  assert.match(byId.get("onprem-resolve-title").textContent, /итогового перемещения/);
  assert.match(byId.get("onprem-job-stop").textContent, /проверяет статус/);
  assert.doesNotMatch(byId.get("onprem-job-stop").textContent, /установите его исход/,
    "pending final moves explain background status checks and confirmed recovery");
  const beforeKnownMovePoll = requests.filter((request) => request.method !== "GET").length;
  await runPoll();
  assert.equal(requests.filter((request) => request.method !== "GET").length, beforeKnownMovePoll);
  assert.equal(stoppedParent.stop_blocked, true);
  byId.get("onprem-resolve-operation").value = "";
  byId.get("onprem-resolve-status").value = "failed";
  byId.get("onprem-resolve-evidence").value = "Administrator logs confirm the final move failed terminally.";
  byId.get("onprem-resolve-confirmed").checked = true;
  await fire("onprem-resolve-operation", "input");
  await fire("onprem-resolve", "click");
  const terminalFinal = requests.filter((request) => request.path.endsWith("/resolve")).at(-1);
  assert.equal(terminalFinal.body.terminal_status, "failed");
  assert.equal(terminalFinal.body.attempt_id, finalMoveAttempt);
  assert.equal(stoppedParent.state, "stopped");
  assert.equal(stoppedParent.stop_blocked, false, "only confirmed terminal resolution releases the final-move blocker");
  stoppedParent.stop_blocked = false;
  stoppedParent.stop_blocked_attempt_ids = [];
  await fire("onprem-refresh", "click");
  assert.equal(byId.get("onprem-credentials").disabled, true);
  assert.equal(byId.get("onprem-resolve").disabled, true,
    "isolated stopped uploads cannot revive old work through resolution");
  assert.deepEqual(storageWrites, []);
  console.log("PASS: disabled release gate, explicit pilot opt-in, configuration failure fallback, asynchronous plan/history, pagination, credential recovery, safe operation resolution, permanent local stop, idempotent new-job restart, stopped final-move blocker recovery, manual Start, stale-read protection and secret-safe rendering/storage");
})().catch((error) => {console.error(error); process.exitCode = 1;});
