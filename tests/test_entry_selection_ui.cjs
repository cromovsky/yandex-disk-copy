const assert = require("node:assert/strict");
const {test} = require("node:test");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const crypto = require("node:crypto").webcrypto;
const {loadUi} = require("./helpers/legacy_ui.cjs");
const clone = (value) => JSON.parse(JSON.stringify(value));
const flush = async () => {for (let count = 0; count < 6; count += 1) await new Promise((resolve) => setImmediate(resolve));};
const baseEntry = {id: "file", kind: "file", source_path: "disk:/<unsafe>.txt", destination_path: "disk:/copy/<unsafe>.txt", size: 1024, state: "planned"};

async function setup({mode = "single", state = "ready", editable = true, planComplete = true, canStartTransfer = undefined, entries = [baseEntry], rejectSelection = false, gate = null, resumeStarts = false, resumeGate = null} = {}) {
  const ui = await loadUi({cross_org: true, onprem_enabled: true, onprem_auth_required: false});
  const currentEntries = clone(entries), calls = [];
  const config = {source_org_id: "org", source_client_id: "client", source_email: "source@example.test", source_path: "disk:/", source_type: "personal", page_limit: 1000,
    destination_url: "https://disk.example.test/v1/disk", destination_identity: "operator@example.test", destination_type: "shared", destination_vd_hash: "target", destination_base: "disk:/", destination_root: "disk:/source@example.test", strategy: "staged", identity_confirmed: true};
  const job = {id: "job-1", state, revision: 7, config, plan_complete: planComplete, ...(canStartTransfer === undefined ? {} : {can_start_transfer: canStartTransfer}), total_files: currentEntries.filter((entry) => entry.kind === "file").length,
    total_directories: currentEntries.filter((entry) => entry.kind !== "file").length, total_bytes: currentEntries.reduce((total, entry) => total + (entry.size || 0), 0), verified_bytes: 0, counts: {planned: currentEntries.length}};
  const source = {id: "source-a", type: "personal", email: config.source_email, path: "disk:/", folder: config.source_email, selected: true, plan_complete: true, state: "ready", summary: {total_files: job.total_files, total_directories: job.total_directories, total_bytes: job.total_bytes}};
  const detail = {batch: {id: "list-1", mode: mode === "single" ? "onprem" : mode, state, started: false, credentials_available: true, config, summary: {selected_sources: 1, ready_sources: 1}}, sources: [source]};
  function summarize() {
    const selected = currentEntries.filter((entry) => entry.selected !== false);
    const selection = {total: currentEntries.length, selected: selected.length, excluded: currentEntries.length - selected.length,
      selected_files: selected.filter((entry) => entry.kind === "file").length, selected_directories: selected.filter((entry) => entry.kind !== "file").length,
      selected_bytes: selected.reduce((total, entry) => total + (entry.size || 0), 0), editable};
    job.selection = selection; source.summary.selection = selection; detail.batch.selection = selection;
    for (const entry of currentEntries) entry.can_select = editable && entry.can_select !== false;
  }
  summarize();
  const respond = (value, status = 200) => ({ok: status < 400, status, json: async () => clone(value)});
  const fetch = async (address, options = {}) => {
    const url = new URL(address, "http://localhost"), method = options.method || "GET", body = options.body && JSON.parse(options.body);
    calls.push({path: url.pathname, params: url.searchParams, method, body});
    if (url.pathname.endsWith("/selection")) {
      if (gate) await gate;
      if (rejectSelection) return respond({detail: "stale_revision"}, 409);
      const id = decodeURIComponent(url.pathname.split("/").at(-2)), entry = currentEntries.find((value) => value.id === id);
      assert.ok(entry, "selection targets an exact saved entry");
      for (const other of currentEntries) {
        if (other === entry || (entry.kind !== "file" && other.source_path.startsWith(entry.source_path + "/")) ||
            (body.selected && other.kind !== "file" && entry.source_path.startsWith(other.source_path + "/"))) other.selected = body.selected;
      }
      for (const directory of currentEntries.filter((value) => value.kind !== "file")) {
        const children = currentEntries.filter((value) => value.source_path.startsWith(directory.source_path + "/"));
        directory.selection_partial = children.some((value) => value.selected !== false) && children.some((value) => value.selected === false);
      }
      job.revision += 1; summarize();
      return respond(mode === "single" ? job : detail);
    }
    if (url.pathname.endsWith("/entries")) {
      const offset = Number(url.searchParams.get("offset") || 0), limit = Number(url.searchParams.get("limit") || 100);
      return respond({total: currentEntries.length, entries: currentEntries.slice(offset, offset + limit)});
    }
    if (url.pathname.endsWith("/resume")) {
      if (resumeGate) await resumeGate;
      job.state = job.resume_state === "ready" ? "ready"
        : job.plan_complete && job.selection.editable && !resumeStarts ? "needs_attention" : "running";
      job.resume_state = null;
      return respond(job);
    }
    if (url.pathname.endsWith("/start")) {
      if (!["ready", "needs_attention"].includes(job.state)) return respond({detail: "plan_not_ready"}, 409);
      job.state = "running"; return respond(job);
    }
    if (url.pathname.endsWith("/events")) return respond({events: []});
    if (url.pathname === "/api/onprem/jobs") return respond({jobs: [job]});
    if (url.pathname === "/api/source-batches") return respond({batches: [detail.batch], has_more: false});
    return respond(mode === "single" ? job : detail);
  };
  const values = {local_orgid: "org", local_client_id: "client", local_client_secret: "source-secret", local_page_limit: "1000",
    "onprem-source-secret": "source-secret", "onprem-destination-token": "destination-secret"};
  for (const [id, value] of Object.entries(values)) ui.byId.get(id).value = value;
  ui.byId.get("onprem-form").checkValidity = () => true;
  ui.byId.get("onprem-form").reportValidity = () => true;
  const script = mode === "single" ? "onprem.js" : "source-batches.js";
  vm.runInNewContext(fs.readFileSync(path.resolve(__dirname, "../app/static", script), "utf8"), {
    document: ui.document, window: ui.window, fetch, URL, Blob, crypto, Uint8Array, console,
    CustomEvent: class {constructor(type, options) {this.type = type; this.detail = options.detail;}},
    Event: class {constructor(type) {this.type = type;}}, setTimeout() {return 1;}, clearTimeout() {},
  });
  await flush();
  const prefix = mode === "single" ? "onprem" : mode + "-batch";
  const get = (name) => ui.byId.get(prefix + "-" + name);
  if (mode === "single") {
    get("history").value = job.id; await get("history").fire("change"); await flush();
  } else {
    ui.byId.get(mode + "-source-input").value = "list";
    await ui.window.sourceBatches.open(mode, detail.batch.id);
    await get("sources").children[0].children[7].children[1].fire("click"); await flush();
  }
  for (const [id, value] of Object.entries(values)) ui.byId.get(id).value = value;
  ui.byId.get("onprem-identity-confirmed").checked = true;
  await ui.byId.get("onprem-identity-confirmed").fire("change"); await flush();
  const rows = () => get(mode === "single" ? "entry-body" : "entries").children;
  const checkbox = (index = 0) => rows()[index].querySelector("input");
  return {...ui, get, calls, job, detail, currentEntries, rows, checkbox,
    async refresh() {if (mode === "single") await get("refresh").fire("click"); else await ui.window.sourceBatches.refresh(mode); await flush();},
    async toggle(index, selected) {const check = checkbox(index); check.checked = selected; await check.fire("change"); await flush();},
    async next() {await get(mode === "single" ? "entry-next" : "entries-next").fire("click"); await flush();},
  };
}

for (const mode of ["single", "local", "onprem"]) {
  test(`${mode}: persisted entry choice skips conflicts without credentials or a new Dry Run`, async () => {
    const ui = await setup({mode, entries: [{...baseEntry, state: "conflict", error: "destination_conflict"}, {...baseEntry, id: "other", source_path: "disk:/other.txt"}]});
    assert.ok(ui.checkbox(), "every planned entry has a checkbox");
    assert.equal(ui.checkbox().checked, true, "legacy entries default to selected");
    assert.equal(ui.checkbox().disabled, false);
    assert.match(ui.checkbox().getAttribute("aria-label"), /<unsafe>\.txt/);
    const before = ui.calls.length;
    ui.byId.get(mode === "local" ? "local_client_secret" : "onprem-source-secret").value = "";
    await ui.toggle(0, false);
    const writes = ui.calls.slice(before).filter((call) => call.method !== "GET");
    assert.equal(writes.length, 1, "a toggle does not attach credentials or run another plan");
    assert.equal(writes[0].path, mode === "single" ? "/api/onprem/jobs/job-1/entries/file/selection" : "/api/source-batches/list-1/sources/source-a/entries/file/selection");
    assert.deepEqual(writes[0].body, mode === "single" ? {selected: false, revision: 7} : {selected: false});
    assert.equal(ui.checkbox().checked, false);
    assert.match(ui.rows()[0].children.at(-1).textContent, /Исключено из переноса/);
    assert.doesNotMatch(ui.rows()[0].children.at(-1).textContent, /Конфликт|другой файл/);
    assert.match(ui.get(mode === "single" ? "entry-selection" : "entries-selection").textContent, /Выбрано: 1.*исключено: 1/);
    if (mode === "single") assert.equal(ui.get("job-bytes").textContent, "0 Б / 1 Кб", "progress uses selected bytes");
    await ui.refresh(); assert.equal(ui.checkbox().checked, false, "refresh restores the saved choice");
    ui.currentEntries[0].state = "skipped_by_user"; await ui.refresh();
    assert.match(ui.rows()[0].children.at(-1).textContent, /Пропущен пользователем/);
  });
  test(`${mode}: folder selection spans pages and empty selection can be restored`, async () => {
    const entries = [{...baseEntry, id: "folder", kind: "dir", source_path: "disk:/folder", size: 0},
      ...Array.from({length: 101}, (_, index) => ({...baseEntry, id: "file-" + index, source_path: "disk:/folder/" + index}))];
    const ui = await setup({mode, state: "needs_attention", entries});
    assert.equal(ui.checkbox().disabled, false, "backend editability also permits fixing a capacity-blocked plan");
    await ui.toggle(0, false);
    assert.equal(ui.get("start").disabled, true, "a plan without selected objects cannot start");
    assert.equal(ui.checkbox().disabled, false, "empty plans remain editable");
    await ui.next(); assert.equal(ui.rows().length, 2);
    assert.equal(ui.checkbox().checked, false, "excluded descendants on later pages reflect server truth");
    await ui.toggle(1, true);
    assert.equal(ui.currentEntries[0].selected, true, "selecting a child restores its parent");
    await ui.get(mode === "single" ? "entry-previous" : "entries-previous").fire("click"); await flush();
    assert.equal(ui.checkbox().checked, true); assert.equal(ui.checkbox().indeterminate, true, "partially selected folders expose mixed state");
  });
  test(`${mode}: rejected entry selection refreshes saved truth and lifecycle locks forced events`, async () => {
    const ui = await setup({mode, rejectSelection: true});
    await ui.toggle(0, false);
    assert.equal(ui.checkbox().checked, true);
    assert.match(ui.get("notice").textContent, /изменил|изменилось|Обновите/);
    assert.ok(ui.calls.filter((call) => call.path.endsWith("/entries")).length >= 2, "rejection reloads persisted entry truth");
    const locked = await setup({mode, state: "running", editable: false});
    assert.equal(locked.checkbox().disabled, true);
    await locked.toggle(0, false);
    assert.equal(locked.checkbox().checked, true);
    assert.equal(locked.calls.some((call) => call.method === "POST"), false, "forced disabled events cannot mutate a started plan");
  });
  test(`${mode}: pending entry saves lock other changes and detached rows cannot mutate a refreshed page`, async () => {
    let release;
    const gate = new Promise((resolve) => {release = resolve;});
    const ui = await setup({mode, gate, entries: [baseEntry, {...baseEntry, id: "other", source_path: "disk:/other"}]});
    const old = ui.checkbox(), sibling = ui.checkbox(1);
    old.checked = false; const pending = old.fire("change"); await flush();
    assert.equal(sibling.disabled, true); assert.equal(ui.get("start").disabled, true);
    sibling.checked = false; await sibling.fire("change");
    assert.equal(sibling.checked, true, "forced events while saving restore the server's last known choice");
    assert.equal(ui.calls.filter((call) => call.method === "POST").length, 1);
    release(); await pending; await flush();
    old.checked = true; await old.fire("change"); await flush();
    assert.equal(ui.calls.filter((call) => call.method === "POST").length, 1, "a detached checkbox cannot edit the new page snapshot");
  });
  test(`${mode}: per-entry permission and edited configuration lock selection`, async () => {
    const ui = await setup({mode, entries: [baseEntry, {...baseEntry, id: "locked", source_path: "disk:/locked", can_select: false}]});
    assert.equal(ui.checkbox(1).disabled, true);
    await ui.toggle(1, false); assert.equal(ui.calls.some((call) => call.method === "POST"), false);
    const field = ui.byId.get(mode === "local" ? "local_orgid" : "onprem-source-org");
    field.value = "other-org"; await field.fire("input");
    assert.equal(ui.checkbox().disabled, true);
    await ui.toggle(0, false); assert.equal(ui.calls.some((call) => call.method === "POST"), false);
  });
  test(`${mode}: all selected entries excluded disables Start after form credentials are restored`, async () => {
    const ui = await setup({mode});
    assert.equal(ui.get("start").disabled, false);
    await ui.toggle(0, false); assert.equal(ui.get("start").disabled, true);
    await ui.toggle(0, true); assert.equal(ui.get("start").disabled, false);
    const frozen = await setup({mode, editable: false});
    assert.equal(frozen.checkbox().disabled, true);
    assert.equal(frozen.get("start").disabled, false, "a ready plan frozen before a crash can still start");
  });
}

test("standalone: changing selected job during a pending mutation ignores the old response", async () => {
  let release;
  const gate = new Promise((resolve) => {release = resolve;});
  const ui = await setup({gate});
  const check = ui.checkbox(); check.checked = false; const pending = check.fire("change"); await flush();
  assert.equal(ui.get("start").disabled, true, "saving selection locks Start");
  ui.get("history").value = ""; await ui.get("history").fire("change"); await flush();
  release(); await pending; await flush();
  assert.equal(ui.get("job").hidden, true, "stale mutation cannot reopen a deselected job");
});

test("standalone: complete pre-start attention plans can Start to revalidate selected capacity", async () => {
  const ui = await setup({state: "needs_attention", entries: [baseEntry, {...baseEntry, id: "other", source_path: "disk:/other"}]});
  ui.job.error = "destination_space_insufficient";
  await ui.toggle(0, false);
  assert.equal(ui.job.state, "needs_attention", "selection changes do not need a remote quota check");
  assert.equal(ui.get("start").disabled, false, "Start performs quota validation for the reduced selection");
  const incomplete = await setup({state: "needs_attention", planComplete: false});
  assert.equal(incomplete.get("start").disabled, true);
  const transferred = await setup({state: "needs_attention", editable: false});
  assert.equal(transferred.get("start").disabled, true, "a previous transfer uses its recovery actions");
});

test("standalone: Start after pre-start credential recovery does not start an already running job twice", async () => {
  const ui = await setup({state: "awaiting_credentials", resumeStarts: true});
  ui.job.resume_state = "needs_attention"; ui.job.error = "destination_space_insufficient"; await ui.refresh();
  assert.equal(ui.get("start").disabled, false);
  await ui.get("start").fire("click"); await flush();
  assert.equal(ui.job.state, "running");
  assert.deepEqual(ui.calls.filter((call) => call.method !== "GET").map((call) => call.path.split("/").at(-1)), ["credentials", "resume"]);
  assert.equal(ui.get("notice").textContent, "Перенос начат.");
});

test("standalone: recovered pre-start attention plan uses Resume for access, then explicit Start", async () => {
  const ui = await setup({state: "awaiting_credentials"});
  ui.job.resume_state = "needs_attention"; ui.job.error = "destination_space_insufficient"; await ui.refresh();
  await ui.get("start").fire("click"); await flush();
  assert.deepEqual(ui.calls.filter((call) => call.method !== "GET").map((call) => call.path.split("/").at(-1)), ["credentials", "resume", "start"]);
  assert.equal(ui.job.state, "running"); assert.equal(ui.get("notice").textContent, "Перенос начат.");
});

test("standalone: a pending Start recovery stays bound to its original selected job", async () => {
  let release;
  const resumeGate = new Promise((resolve) => {release = resolve;});
  const ui = await setup({state: "awaiting_credentials", resumeGate});
  ui.job.resume_state = "needs_attention"; ui.job.error = "destination_space_insufficient"; await ui.refresh();
  const pending = ui.get("start").fire("click"); await flush();
  ui.get("history").value = ""; await ui.get("history").fire("change"); await flush();
  release(); await pending; await flush();
  assert.equal(ui.calls.some((call) => call.path.endsWith("/start")), false, "changing job during recovery prevents a later launch");
  assert.equal(ui.get("job").hidden, true);
  assert.doesNotMatch(ui.get("notice").textContent, /Перенос начат|План ещё не готов/);
});

test("standalone: a frozen pre-start quota plan can Start using the backend lifecycle flag", async () => {
  const ui = await setup({state: "needs_attention", editable: false, canStartTransfer: true});
  ui.job.selection_frozen = true; ui.job.error = "destination_space_insufficient"; await ui.refresh();
  assert.equal(ui.checkbox().disabled, true, "a frozen selection stays locked");
  assert.equal(ui.get("start").disabled, false, "a complete frozen pre-start plan can revalidate and launch");
  const executed = await setup({state: "needs_attention", editable: true, canStartTransfer: false});
  assert.equal(executed.get("start").disabled, true, "the explicit lifecycle flag overrides stale selection editability");
});
