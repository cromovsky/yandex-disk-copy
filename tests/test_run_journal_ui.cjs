const assert = require("node:assert/strict");
const {test} = require("node:test");
const {loadUi} = require("./helpers/legacy_ui.cjs");
const response = (value, status = 200) => ({ok: status < 400, status, json: async () => value, blob: async () => new Blob(["journal"])});
const job = (id, mode = "single", status = "done", extra = {}) => ({id, mode, status, kind: "single", created_at: "2026-10-14T08:15:00Z",
  source: "reader@example.test", destination: "writer@example.test", summary: {saved: 1, skipped: 2, failed: 0}, ...extra});
const event = (sequence, extra = {}) => ({sequence, created_at: "2026-10-14T08:15:00Z", event_type: "stage", message: "Событие " + sequence, ...extra});
const flush = async () => {for (let i = 0; i < 5; i += 1) await new Promise((resolve) => setImmediate(resolve));};
const get = (ui, id) => ui.byId.get(id);
async function select(ui, id) {get(ui, "run-history").value = id; await get(ui, "run-history").fire("change"); await flush();}
function assertLocked(ui, locked) { for (const id of ["run", "run-cross", "run-local"]) assert.equal(get(ui, id).disabled, locked, id); }

test("mode histories restore remembered or latest jobs and persist only journal IDs", async () => {
  const ui = await loadUi(undefined, {initialJobs: [job("new"), job("old"), job("cross-id", "cross"), job("local-id", "local")],
    initialEvents: {old: [event(1, {message: "Старый журнал"})]}, storageData: {"diskCopyJournalSelection:v1:single": "old"}});
  assert.equal(get(ui, "run-history").value, "old");
  assert.match(get(ui, "log").textContent, /Старый журнал/);
  assert.match(get(ui, "status").textContent, /14\.10\.2026.*Завершено/);
  assert.doesNotMatch(get(ui, "status").textContent, /old/);
  assert.match(get(ui, "run-journal-summary").textContent, /Сохранено: 1 · Пропущено: 2 · Ошибок: 0/);
  await ui.journal.selectMode("cross");
  assert.equal(get(ui, "run-history").value, "cross-id");
  assert.match(get(ui, "run-journal-cleanup-note").textContent, /только режима «Между организациями»/);
  await ui.journal.selectMode("local");
  assert.equal(get(ui, "run-history").value, "local-id");
  await ui.journal.selectMode("single");
  assert.equal(get(ui, "run-history").value, "old");
  assert.ok(ui.storageWrites.every(({key, value}) => key.startsWith("diskCopyJournalSelection:v1:") && ["old", "cross-id", "local-id"].includes(value)));
  assert.equal(ui.requests.length, 0, "restoring journals never launches a transfer");
});

test("events retain last500, deduplicate polling and display HTTP/API/network/retry/unknown details", async () => {
  const ui = await loadUi(undefined, {initialJobs: [job("events")], initialEvents: {events: Array.from({length: 510}, (_, i) => event(i + 1))}});
  assert.equal(get(ui, "log").textContent.split("\n").length, 500);
  assert.doesNotMatch(get(ui, "log").textContent, /Событие 10\n/);
  ui.events.events.push(event(511, {stage: "download", status: "outcome_unknown", source: "/file.txt", destination: "/copy/file.txt", size: 2048,
    http_status: 500, error: "download_failed", api_error: "InternalError", api_reason: "backend_failed", transport_error: "ReadTimeout",
    error_side: "source", request_host: "disk.example.test", request_id: "req-1", operation_id: "operation-1", pair_index: 2,
    attempt: 3, max_attempts: 5, retry_delay_ms: 8000, duration_ms: 5000,
    token: "fake-secret-token", headers: {Authorization: "Bearer fake-token"}}));
  await ui.journal.refresh();
  await ui.journal.refresh();
  const text = get(ui, "log").textContent;
  assert.equal(text.split("\n").length, 500);
  for (const expected of [/SaaS: источник/, /Скачивание/, /Исход неизвестен/, /HTTP 500/, /API: InternalError/, /Причина API: backend_failed/,
    /Сеть: ReadTimeout/, /ID запроса: req-1/, /Операция: operation-1/, /Пара 2/, /Попытка: 3\/5/, /До повтора: 8 с/, /Размер: 2 Кб/]) assert.match(text, expected);
  assert.doesNotMatch(text, /fake-secret|fake-token/);
  assert.equal(text.match(/Событие 511/g).length, 1);
  assert.ok(ui.reads.some(({url}) => /after=510&limit=500/.test(url)));
});

test("running jobs in another mode lock starts during historical inspection and after read failure", async () => {
  let disconnected = false;
  const ui = await loadUi(undefined, {initialJobs: [job("finished"), job("active", "local", "running")],
    journalOverride: ({url}) => {if (disconnected && url.pathname === "/api/history" && url.searchParams.get("mode") === "local") throw Error("private-network-detail");}});
  assertLocked(ui, true);
  assert.match(get(ui, "run-active-note").textContent, /Сохранить локально.*1/);
  await select(ui, "finished");
  assertLocked(ui, true);
  await ui.journal.selectMode("local");
  assert.equal(get(ui, "run-journal-cleanup").disabled, true);
  disconnected = true;
  await ui.journal.refresh();
  assertLocked(ui, true);
  assert.match(get(ui, "run-journal-notice").textContent, /Состояние активных переносов не подтверждено/);
  assert.doesNotMatch(get(ui, "run-journal-notice").textContent, /private-network-detail/);
  disconnected = false; ui.jobs.find((item) => item.id === "active").status = "done";
  await ui.journal.refresh();
  assertLocked(ui, false);
});

test("active count independent of pagination keeps old running jobs locked", async () => {
  const ui = await loadUi(undefined, {initialJobs: [...Array.from({length: 55}, (_, i) => job("complete-" + i)), job("old-active", "single", "running")]});
  assertLocked(ui, true);
  assert.equal(get(ui, "run-history-next").disabled, false);
  await get(ui, "run-history-next").fire("click");
  assert.match(get(ui, "run-history-page").textContent, /Страница 2/);
  assert.ok(get(ui, "run-history").children.some((node) => node.value === "old-active"));
  assertLocked(ui, true);
});

test("journal degradation, interruption and partial completion remain distinct from success", async () => {
  const ui = await loadUi(undefined, {initialJobs: [job("broken", "single", "done", {journal_error: "journal_write_failed", error: "partial_failure", summary: {saved: 2, skipped: 1, failed: 1}}),
    job("interrupted", "single", "interrupted")]});
  assert.match(get(ui, "status").textContent, /Завершено с ошибками/);
  assert.match(get(ui, "status").className, /error/);
  assert.match(get(ui, "run-journal-notice").textContent, /может быть неполным/);
  assert.match(get(ui, "run-journal-summary").textContent, /Сохранено: 2.*Пропущено: 1.*Ошибок: 1/);
  await select(ui, "interrupted");
  assert.match(get(ui, "status").textContent, /Прервано/);
  assert.match(get(ui, "run-journal-notice").textContent, /автоматически не продолжается/);
  assert.equal(ui.requests.length, 0);
});

test("stale selected journal responses never replace another selection", async () => {
  let delay = false, release;
  const ui = await loadUi(undefined, {initialJobs: [job("a"), job("b")], initialEvents: {a: [event(1, {message: "Журнал A"})], b: [event(1, {message: "Журнал B"})]},
    journalOverride: async ({url}) => { if (delay && url.pathname === "/api/run/a/events") {delay = false; await new Promise((resolve) => {release = resolve;}); return response({events: [event(2, {message: "Устаревший ответ A"})], last_sequence: 2});} }});
  delay = true; const refresh = ui.journal.refresh(); await flush();
  get(ui, "run-history").value = "b";
  const choose = get(ui, "run-history").fire("change"); await flush(); release();
  await Promise.all([refresh, choose]); await flush();
  assert.equal(get(ui, "run-history").value, "b");
  assert.match(get(ui, "log").textContent, /Журнал B/);
  assert.doesNotMatch(get(ui, "log").textContent, /Устаревший ответ A/);
});

test("mode changes during reads preserve the newly selected mode", async () => {
  let delay = false, release;
  const ui = await loadUi(undefined, {initialJobs: [job("a"), job("local", "local")], initialEvents: {local: [event(1, {message: "Локальный журнал"})]},
    journalOverride: async ({url}) => { if (delay && url.pathname === "/api/run/a/events") {delay = false; await new Promise((resolve) => {release = resolve;}); return response({events: [event(1, {message: "Старый режим"})], last_sequence: 1});} }});
  delay = true; const refresh = ui.journal.refresh(); await flush();
  const switching = ui.journal.selectMode("local"); release(); await Promise.all([refresh, switching]); await flush();
  assert.match(get(ui, "run-journal-mode").textContent, /Сохранить локально/);
  assert.match(get(ui, "log").textContent, /Локальный журнал/);
  assert.doesNotMatch(get(ui, "log").textContent, /Старый режим/);
});

test("start response cannot overwrite a journal selected while the request was pending", async () => {
  let release;
  const ui = await loadUi(undefined, {initialJobs: [job("old"), job("other")],
    journalOverride: async ({method, url, jobs}) => {if (method === "POST" && url.pathname === "/api/run") {
      await new Promise((resolve) => {release = resolve;}); jobs.unshift(job("started", "single", "running")); return response({job_id: "started"});
    }}});
  const start = ui.journal.start({endpoint: "/api/run", body: {source_disk_id: "reader@example.test"}, mode: "single"}); await flush();
  await select(ui, "other"); release(); await start; await flush();
  assert.equal(get(ui, "run-history").value, "other");
  assertLocked(ui, true);
  assert.equal(ui.requests.length, 1);
});

test("uncertain network start is never retried or unlocked until its job is discovered", async () => {
  const ui = await loadUi(undefined, {journalOverride: ({method, url}) => {if (method === "POST" && url.pathname === "/api/run") throw Error("private-secret-error");}});
  await ui.journal.start({endpoint: "/api/run", body: {client_secret: "fake-secret"}, mode: "single"});
  assertLocked(ui, true);
  assert.match(get(ui, "run-journal-notice").textContent, /Ответ на запуск не получен/);
  assert.doesNotMatch(get(ui, "run-journal-notice").textContent, /private-secret|fake-secret/);
  await ui.journal.start({endpoint: "/api/run", body: {}, mode: "single"});
  assert.equal(ui.requests.length, 1);
  ui.jobs.unshift(job("recovered", "single", "running")); await ui.journal.refresh();
  assert.equal(get(ui, "run-history").value, "recovered"); assertLocked(ui, true);
  await ui.finishRun(); assertLocked(ui, false);
});

test("mode cleanup needs no selected job or form credentials and leaves other modes intact", async () => {
  const ui = await loadUi(undefined, {initialJobs: [job("single"), job("local", "local")],
    journalOverride: ({method, url}) => {if (method === "POST" && url.pathname !== "/api/history/cleanup") throw Error("No transfer allowed");}});
  await ui.journal.selectMode("local"); await select(ui, "");
  assert.equal(get(ui, "run-journal-cleanup").disabled, false);
  await get(ui, "run-journal-cleanup").fire("click");
  assert.deepEqual(ui.requests, [{url: "/api/history/cleanup", body: {mode: "local"}}]);
  assert.deepEqual(ui.jobs.map((item) => item.id), ["single"]);
  assert.match(get(ui, "run-journal-cleanup-result").textContent, /Сохранить локально.*журналов — 1.*событий — 7.*2 Кб/);
  await ui.journal.selectMode("single");
  assert.equal(get(ui, "run-history").value, "single");
  assert.equal(get(ui, "run-journal-cleanup-result").textContent, "");
});

test("cleanup response and retries stay bound to their original mode", async () => {
  let release;
  const ui = await loadUi(undefined, {initialJobs: [job("single"), job("cross", "cross")], journalOverride: async ({url}) => {
    if (url.pathname === "/api/history/cleanup") {await new Promise((resolve) => {release = resolve;}); return response({removed_jobs: 1, removed_events: 2, removed_bytes: 0});}
  }});
  const cleanup = get(ui, "run-journal-cleanup").fire("click"); await flush();
  await ui.journal.selectMode("cross"); release(); await cleanup; await flush();
  assert.deepEqual(ui.requests[0].body, {mode: "single"});
  assert.match(get(ui, "run-journal-mode").textContent, /Между организациями/);
  assert.equal(get(ui, "run-history").value, "cross");
  assert.equal(get(ui, "run-journal-cleanup-result").textContent, "");
});

test("cleanup active-race rejection is local, safe, and is not retried automatically", async () => {
  const ui = await loadUi(undefined, {journalOverride: ({url}) => url.pathname === "/api/history/cleanup"
    ? response({detail: "journal_in_use", diagnostic_message: "private-details"}, 409) : null});
  await get(ui, "run-journal-cleanup").fire("click");
  assert.match(get(ui, "run-journal-cleanup-result").textContent, /ещё выполняется перенос/);
  assert.doesNotMatch(get(ui, "run-journal-cleanup-result").textContent, /private-details/);
  await ui.journal.refresh(); assert.equal(ui.requests.length, 1);
});

test("CSV uses selected id and stale export never downloads into another selection", async () => {
  let delay = false, release;
  const ui = await loadUi(undefined, {initialJobs: [job("a"), job("b")], journalOverride: async ({url}) => {
    if (delay && url.pathname.endsWith("/journal")) {await new Promise((resolve) => {release = resolve;}); return response({});}
  }});
  await get(ui, "run-journal-csv").fire("click");
  assert.equal(ui.downloads.length, 1);
  assert.ok(ui.reads.some(({url}) => url === "/api/run/a/journal"));
  delay = true; const download = get(ui, "run-journal-csv").fire("click"); await flush();
  await select(ui, "b"); release(); await download;
  assert.equal(ui.downloads.length, 1);
});

test("missing remembered journal falls back to latest without replaying deleted requests", async () => {
  const ui = await loadUi(undefined, {initialJobs: [job("latest")], storageData: {"diskCopyJournalSelection:v1:single": "deleted"}});
  await flush();
  assert.equal(get(ui, "run-history").value, "latest");
  assert.equal(ui.requests.length, 0);
});

test("a stale history response cannot unlock a newly accepted transfer", async () => {
  let delayHistory = false, releaseHistory, releaseStart;
  const ui = await loadUi(undefined, {journalOverride: async ({url, method, jobs}) => {
    if (method === "POST" && url.pathname === "/api/run") {
      await new Promise((resolve) => {releaseStart = resolve;}); jobs.unshift(job("new-active", "single", "running")); return response({job_id: "new-active"});
    }
    if (delayHistory && url.pathname === "/api/history" && url.searchParams.get("mode") === "single") {
      delayHistory = false; await new Promise((resolve) => {releaseHistory = resolve;});
      return response({jobs: [], running_count: 0, active_jobs: [], has_more: false});
    }
  }});
  const start = ui.journal.start({endpoint: "/api/run", body: {}, mode: "single"}); await flush();
  delayHistory = true; const history = ui.journal.refresh(); await flush();
  assertLocked(ui, true); releaseStart(); await flush(); assertLocked(ui, true);
  releaseHistory(); await Promise.all([start, history]); await flush();
  assertLocked(ui, true); assert.equal(get(ui, "run-history").value, "new-active");
});

test("explicit journal storage rejection allows a later manual retry without auto-submission", async () => {
  const ui = await loadUi(undefined, {journalOverride: ({url, method}) => method === "POST" && url.pathname === "/api/run"
    ? response({detail: "journal_storage_unavailable", diagnostic_message: "private-path"}, 503) : null});
  await ui.journal.start({endpoint: "/api/run", body: {}, mode: "single"});
  assertLocked(ui, false);
  assert.match(get(ui, "run-journal-notice").textContent, /Хранилище журналов недоступно/);
  assert.doesNotMatch(get(ui, "run-journal-notice").textContent, /private-path/);
  await ui.journal.refresh(); assert.equal(ui.requests.length, 1);
});

test("cleanup and CSV cannot overlap in this interface", async () => {
  let release;
  const ui = await loadUi(undefined, {initialJobs: [job("a")], journalOverride: async ({url}) => {
    if (url.pathname.endsWith("/journal")) {await new Promise((resolve) => {release = resolve;}); return response({});}
  }});
  const download = get(ui, "run-journal-csv").fire("click"); await flush();
  assert.equal(get(ui, "run-journal-cleanup").disabled, true);
  await get(ui, "run-journal-cleanup").dispatch("click"); assert.equal(ui.requests.length, 0);
  release(); await download; assert.equal(get(ui, "run-journal-cleanup").disabled, false);
});

test("untrusted selection and event fields never become markup, credentials or remote requests", async () => {
  const ui = await loadUi(undefined, {initialJobs: [job("safe")], storageData: {"diskCopyJournalSelection:v1:single": "https://example.test?token=private"},
    initialEvents: {safe: [event(1, {message: "<img src=x onerror=evil()>", source: "https://example.test?token=private", token: "private-token"})]}});
  assert.equal(get(ui, "run-history").value, "safe");
  assert.match(get(ui, "log").textContent, /<img src=x onerror=evil\(\)>/);
  assert.equal(get(ui, "log").children.length, 0, "the journal only writes text nodes");
  assert.doesNotMatch(get(ui, "log").textContent, /private-token|example\.test/);
  assert.ok(ui.reads.every(({url}) => url.startsWith("/api/")));
});

test("unknown start requires explicit unlock after all histories confirm no running jobs", async () => {
  let disconnected = false;
  const ui = await loadUi(undefined, {journalOverride: ({url, method}) => {
    if (method === "POST" && url.pathname === "/api/run") throw Error("lost-response");
    if (disconnected && url.pathname === "/api/history") throw Error("offline");
  }});
  assert.equal(get(ui, "run-start-recovery").hidden, true);
  disconnected = true;
  await ui.journal.start({endpoint: "/api/run", body: {}, mode: "single"});
  assertLocked(ui, true);
  assert.equal(get(ui, "run-start-recovery").hidden, false);
  assert.equal(get(ui, "run-start-unlock").disabled, true);
  await get(ui, "run-start-unlock").dispatch("click"); assertLocked(ui, true);
  disconnected = false; await ui.journal.refresh();
  assert.equal(get(ui, "run-start-unlock").disabled, false);
  assert.match(get(ui, "run-journal-notice").textContent, /может создать второе задание/);
  assertLocked(ui, true);
  await get(ui, "run-start-unlock").fire("click"); assertLocked(ui, false);
  assert.equal(get(ui, "run-start-recovery").hidden, true);
  assert.equal(ui.requests.length, 1, "unlock never submits a new transfer");
});

test("origin-rejected cleanup uses an actionable message without raw response details", async () => {
  const ui = await loadUi(undefined, {journalOverride: ({url}) => url.pathname === "/api/history/cleanup"
    ? response({detail: "journal_origin_rejected", diagnostic_message: "private-host"}, 403) : null});
  await get(ui, "run-journal-cleanup").fire("click");
  assert.match(get(ui, "run-journal-cleanup-result").textContent, /откройте утилиту по её обычному адресу/);
  assert.doesNotMatch(get(ui, "run-journal-cleanup-result").textContent, /private-host/);
});

test("unconfirmed accepted operations are not presented as successful completed transfer", async () => {
  const ui = await loadUi(undefined, {initialJobs: [job("unconfirmed", "single", "done", {summary: {saved: 0, failed: 0, skipped: 0, unconfirmed: 1}})]});
  assert.match(get(ui, "status").textContent, /Завершено с ошибками/);
  assert.match(get(ui, "status").className, /error/);
  assert.match(get(ui, "run-journal-summary").textContent, /Сохранено: 0.*Не подтверждено: 1/);
  assert.equal(ui.requests.length, 0);
});

test("job lifecycle event statuses are displayed in Russian", async () => {
  const ui = await loadUi(undefined, {initialJobs: [job("lifecycle")], initialEvents: {lifecycle: [
    event(1, {status: "running"}), event(2, {status: "done"}), event(3, {status: "error"}), event(4, {status: "interrupted"}),
  ]}});
  const text = get(ui, "log").textContent;
  for (const status of ["Выполняется", "Завершено", "Ошибка", "Прервано"]) assert.ok(text.includes(status));
  assert.doesNotMatch(text, /running|done|error|interrupted/);
});

test("journal polling preserves reading position and follows new events only from the bottom", async () => {
  const ui = await loadUi(undefined, {initialJobs: [job("scroll"), job("other")], initialEvents: {scroll: [event(1)], other: [event(1)]}});
  const log = get(ui, "log");
  log.scrollHeight = 4000; log.clientHeight = 380; log.scrollTop = 1000;
  await ui.journal.refresh();
  assert.equal(log.scrollTop, 1000, "polling terminal history never pushes a reader to the bottom");
  ui.events.scroll.push(event(2)); await ui.journal.refresh();
  assert.equal(log.scrollTop, 1000, "new events preserve the position when the user is reading earlier lines");
  log.scrollTop = 3620;
  ui.events.scroll.push(event(3)); await ui.journal.refresh();
  assert.equal(log.scrollTop, log.scrollHeight, "a reader already at the bottom follows new events");
  log.scrollTop = 900; await select(ui, "other");
  assert.equal(log.scrollTop, log.scrollHeight, "selecting another journal starts at its latest events");
});

for (const mode of ["single", "cross", "local"]) {
  test(`${mode}: retry checkbox defaults on and edits the running job without credentials`, async () => {
    const ui = await loadUi(undefined, {initialJobs: [job("live", mode, "running", {retry_enabled: false})], journalOverride: ({url, body}) => {
      if (url.pathname.endsWith("/retries")) return response({retry_enabled: body.enabled});
    }});
    if (mode !== "single") await ui.journal.selectMode(mode);
    const checkbox = get(ui, "retry-" + mode);
    assert.ok(checkbox, "each mode has a retry setting");
    assert.equal(checkbox.checked, false, "stored job setting is restored");
    assert.equal(checkbox.disabled, false, "running form remains editable");
    checkbox.checked = true; await checkbox.fire("change");
    assert.deepEqual(ui.requests.at(-1), {url: "/api/run/live/retries", body: {enabled: true}});
    assert.equal(checkbox.checked, true);
    assert.equal(ui.storageWrites.some(({value}) => /secret|token/.test(value)), false);
  });
}

test("completed history changes only the next run's retry setting", async () => {
  const ui = await loadUi(undefined, {initialJobs: [job("history", "single", "done", {retry_enabled: false})]});
  const checkbox = get(ui, "retry-single"); assert.ok(checkbox);
  checkbox.checked = true; await checkbox.fire("change");
  assert.equal(ui.requests.length, 0);
  assert.equal(checkbox.checked, true);
});

test("a late retry-settings response cannot overwrite a newly selected job", async () => {
  let release;
  const ui = await loadUi(undefined, {initialJobs: [job("a", "single", "running", {retry_enabled: true}), job("b", "single", "done", {retry_enabled: false})],
    journalOverride: async ({url}) => {
      if (url.pathname === "/api/run/a/retries") {await new Promise((resolve) => {release = resolve;}); return response({retry_enabled: false});}
    }});
  const checkbox = get(ui, "retry-single"); assert.ok(checkbox);
  checkbox.checked = false; const pending = checkbox.fire("change"); await flush();
  await select(ui, "b"); checkbox.checked = true; await checkbox.fire("change");
  release(); await pending;
  assert.equal(get(ui, "run-history").value, "b");
  assert.equal(checkbox.checked, true);
  assert.equal(checkbox.disabled, false);
});

test("retry settings reject safely and restore the server's setting", async () => {
  const ui = await loadUi(undefined, {initialJobs: [job("a", "single", "running", {retry_enabled: true})], journalOverride: ({url}) =>
    url.pathname.endsWith("/retries") ? response({detail: "journal_origin_rejected", private_detail: "secret-value"}, 403) : null});
  const checkbox = get(ui, "retry-single"); assert.ok(checkbox);
  checkbox.checked = false; await checkbox.fire("change");
  assert.equal(checkbox.checked, true);
  assert.equal(checkbox.disabled, false);
  assert.match(get(ui, "retry-single-notice").textContent, /не удалось|откройте/i);
  assert.doesNotMatch(get(ui, "retry-single-notice").textContent, /secret-value/);
});

test("retry-policy events show the persisted choice in the journal", async () => {
  const ui = await loadUi(undefined, {initialJobs: [job("policy")], initialEvents: {policy: [event(1, {stage: "retry_policy", retry_enabled: false})]}});
  assert.match(get(ui, "log").textContent, /Автоматические повторы: выключены/);
});

for (const mode of ["single", "cross", "local"]) test(`${mode}: retry choice changed while creation is pending reaches the accepted job`, async () => {
  let release;
  const endpoint = mode === "single" ? "/api/run" : mode === "cross" ? "/api/run-cross" : "/api/save-local";
  const ui = await loadUi(undefined, {journalOverride: async ({url, method, body, jobs}) => {
    if (method === "POST" && url.pathname === endpoint) {
      await new Promise((resolve) => {release = resolve;});
      jobs.unshift(job("accepted", mode, "running", {retry_enabled: body.retry_enabled}));
      return response({job_id: "accepted"});
    }
    if (url.pathname === "/api/run/accepted/retries") {jobs[0].retry_enabled = body.enabled; return response({retry_enabled: body.enabled});}
  }});
  if (mode !== "single") await ui.journal.selectMode(mode);
  const pending = ui.journal.start({endpoint, body: {retry_enabled: true}, mode}); await flush();
  const check = get(ui, "retry-" + mode); check.checked = false; await check.fire("change");
  release(); await pending;
  assert.equal(ui.jobs[0].retry_enabled, false);
  assert.equal(check.checked, false);
  assert.deepEqual(ui.requests.at(-1), {url: "/api/run/accepted/retries", body: {enabled: false}});
});

test("late creation never takes retry choice from another selected history job", async () => {
  let release;
  const ui = await loadUi(undefined, {initialJobs: [job("a"), job("b")], journalOverride: async ({url, method, jobs}) => {
    if (method === "POST" && url.pathname === "/api/run") {
      await new Promise((resolve) => {release = resolve;}); jobs.unshift(job("accepted", "single", "running", {retry_enabled: true})); return response({job_id: "accepted"});
    }
  }});
  const pending = ui.journal.start({endpoint: "/api/run", body: {retry_enabled: true}, mode: "single"}); await flush();
  await select(ui, "b"); get(ui, "retry-single").checked = false; await get(ui, "retry-single").fire("change");
  release(); await pending;
  assert.equal(get(ui, "run-history").value, "b");
  assert.equal(get(ui, "retry-single").checked, false);
  assert.equal(ui.jobs[0].retry_enabled, true);
  assert.equal(ui.requests.length, 1, "the historical form preference does not mutate the accepted run");
});

test("local creation never takes retry preference from a list opened before acceptance", async () => {
  let release;
  const ui = await loadUi(undefined, {journalOverride: async ({url, method, jobs}) => {
    if (method === "POST" && url.pathname === "/api/save-local") {
      await new Promise((resolve) => {release = resolve;}); jobs.unshift(job("accepted", "local", "running", {retry_enabled: true})); return response({job_id: "accepted"});
    }
  }});
  await ui.journal.selectMode("local");
  const pending = ui.journal.start({endpoint: "/api/save-local", body: {retry_enabled: true}, mode: "local"}); await flush();
  get(ui, "local-source-input").value = "list"; await get(ui, "local-source-input").fire("change");
  ui.window.diskCopyRetries.bind("local", {id: "list:new", enabled: true, editable: false});
  get(ui, "retry-local").checked = false; await get(ui, "retry-local").fire("change");
  release(); await pending;
  assert.equal(ui.jobs[0].retry_enabled, true);
  assert.equal(get(ui, "retry-local").checked, false);
  assert.equal(ui.requests.length, 1);
});

for (const foreign of [false, true]) test(`unknown creation discovers ${foreign ? "another browser's" : "the original"} run read-only and asks to check retries`, async () => {
  let release;
  const ui = await loadUi(undefined, {journalOverride: async ({url, method, body, jobs}) => {
    if (method === "POST" && url.pathname === "/api/run") {
      await new Promise((resolve) => {release = resolve;});
      jobs.unshift(job("discovered", "single", "running", {retry_enabled: true, source: foreign ? "unrelated@example.test" : "reader@example.test"}));
      throw Error("start response lost");
    }
    if (url.pathname === "/api/run/discovered/retries") {jobs[0].retry_enabled = body.enabled; return response({retry_enabled: body.enabled});}
  }});
  const pending = ui.journal.start({endpoint: "/api/run", body: {retry_enabled: true}, mode: "single"}); await flush();
  get(ui, "retry-single").checked = false; await get(ui, "retry-single").fire("change"); release(); await pending; await flush();
  assert.equal(get(ui, "run-history").value, "discovered");
  assert.equal(get(ui, "run-start-recovery").hidden, true);
  assert.equal(ui.jobs[0].retry_enabled, true, "history discovery never mutates an uncorrelated run");
  assert.equal(ui.requests.length, 1, "only the original start request was sent");
  assert.match(get(ui, "run-journal-notice").textContent, /проверьте.*автоматических повторов/i);
  await ui.journal.refresh();
  assert.match(get(ui, "run-journal-notice").textContent, /проверьте.*автоматических повторов/i, "the warning survives polling");
  get(ui, "retry-single").checked = false; await get(ui, "retry-single").fire("change");
  assert.equal(ui.jobs[0].retry_enabled, false, "an explicit change applies to the selected run");
});

test("a known accepted run completing before retry synchronization never blocks history or future starts", async () => {
  let release;
  const ui = await loadUi(undefined, {journalOverride: async ({url, method, jobs}) => {
    if (method === "POST" && url.pathname === "/api/run") {
      await new Promise((resolve) => {release = resolve;}); jobs.unshift(job("accepted", "single", "running", {retry_enabled: true})); return response({job_id: "accepted"});
    }
    if (url.pathname === "/api/run/accepted/retries") {jobs[0].status = "done"; return response({detail: "job_not_running"}, 409);}
  }});
  const pending = ui.journal.start({endpoint: "/api/run", body: {retry_enabled: true}, mode: "single"}); await flush();
  get(ui, "retry-single").checked = false; await get(ui, "retry-single").fire("change"); release(); await pending; await flush();
  assert.equal(get(ui, "run-history").value, "accepted");
  assert.equal(get(ui, "run-start-recovery").hidden, true);
  assertLocked(ui, false);
});
