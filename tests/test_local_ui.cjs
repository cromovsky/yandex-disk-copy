const assert = require("node:assert/strict");
const { test } = require("node:test");
const { loadUi } = require("./helpers/legacy_ui.cjs");

function fillTransfer(ui, cross = false) {
  const values = cross
    ? { x_src_client_id: "client", x_src_client_secret: "secret", x_src_orgid: "org", x_src_admin_token: "admin", x_source_disk_id: "reader@example.test", x_path: "/Finance" }
    : { client_id: "client", client_secret: "secret", orgid: "org", admin_token: "admin", source_disk_id: "reader@example.test", path: "/Finance" };
  for (const [id, value] of Object.entries(values)) ui.byId.get(id).value = value;
}

function fillLocal(ui) {
  const values = {
    local_client_id: "local-client", local_client_secret: "local-secret",
    local_orgid: "local-org", local_admin_token: "local-admin",
    local_source_disk_id: " reader@example.test ", local_path: "/Finance",
  };
  for (const [id, value] of Object.entries(values)) {
    assert.ok(ui.byId.get(id), `local form requires ${id}`);
    ui.byId.get(id).value = value;
  }
}

async function chooseRadio(ui, name, type) {
  const options = ui.document.querySelectorAll(`input[name="${name}"]`);
  assert.equal(options.length, 2, "локальная форма должна позволять выбрать личный или общий источник");
  for (const option of options) option.checked = option.value === type;
  await options.find((option) => option.checked).fire("change");
}

async function selectMode(ui, mode) {
  const tab = ui.document.querySelector(`.mode-tab[data-mode="${mode}"]`);
  assert.ok(tab, `main tab ${mode} must exist`);
  await tab.fire("click");
  assert.equal(tab.classList.contains("active"), true);
  assert.deepEqual(ui.document.querySelectorAll(".mode-panel").filter((panel) => panel.classList.contains("active")).map((panel) => panel.id), [`mode-${mode}`]);
}

function assertLocalRequest(ui, type = "personal") {
  assert.equal(ui.requests.length, 1);
  const request = ui.requests[0];
  assert.equal(request.url, "/api/save-local");
  assert.deepEqual(Object.keys(request.body).sort(), [
    "admin_token", "client_id", "client_secret", "orgid", "page_limit", "path",
    "source_disk_id", "source_type", "source_vd_hash",
  ].sort());
  assert.equal(request.body.client_id, "local-client");
  assert.equal(request.body.client_secret, "local-secret");
  assert.equal(request.body.orgid, "local-org");
  assert.equal(request.body.admin_token, "local-admin");
  assert.equal(request.body.source_disk_id, "reader@example.test");
  assert.equal(request.body.path, "/Finance");
  assert.equal(request.body.page_limit, 1000);
  assert.equal(request.body.source_type, type);
  assert.equal(ui.jobs[0].id, "local-test", "a successful start creates a persistent job");
  assert.ok(ui.reads.some((read) => read.url.startsWith("/api/history?mode=local")), "local history is refreshed independently of the selected mode");
}

function assertSharedLogLock(ui, disabled) {
  for (const id of ["run", "run-cross", "run-local"]) {
    const button = ui.document.getElementById(id);
    if (button) assert.equal(button.disabled, disabled, `${id} must share the progress-log lock`);
  }
}

for (const failConfig of [false, true]) {
  test(`onprem release tab remains visible and disabled when configuration ${failConfig ? "fails" : "defaults to disabled"}`, async () => {
    const ui = await loadUi({ cross_org: true, onprem_enabled: false }, { failConfig });
    const tab = ui.document.querySelector('.mode-tab[data-mode="onprem"]');
    assert.ok(tab);
    assert.equal(tab.hidden, false, "mode stays discoverable while configuration is unavailable");
    assert.equal("hidden" in tab.attributes, false);
    assert.equal(tab.disabled, true);
    assert.equal("disabled" in tab.attributes, true, "HTML must disable the tab before configuration arrives");
    assert.equal(tab.title, "SaaS → On-premises");
    assert.equal(ui.byId.get("mode-onprem").hidden, true);
    await selectMode(ui, "local");
    await tab.fire("click");
    await tab.dispatch("click");
    assert.deepEqual(ui.document.querySelectorAll(".mode-panel").filter((panel) => panel.classList.contains("active")).map((panel) => panel.id), ["mode-local"], "disabled clicks and manually dispatched events cannot switch into onprem");
    assert.equal(ui.requests.length, 0);
    fillLocal(ui);
    await ui.byId.get("run-local").fire("click");
    assertLocalRequest(ui);
    await ui.finishRun("done");
    await selectMode(ui, "single");
    fillTransfer(ui);
    ui.byId.get("destination_disk_id").value = "receiver@example.test";
    await ui.byId.get("run").fire("click");
    assert.equal(ui.requests[1].url, "/api/run");
  });
}

test("local saving has its own main tab with source-only fields", async () => {
  const ui = await loadUi();
  const panel = ui.byId.get("mode-local");
  assert.ok(panel, "local saving requires a dedicated panel");
  assert.equal(ui.document.querySelectorAll("#run-local").length, 1);
  assert.equal(ui.byId.get("run-local").closest(".mode-panel"), panel);
  assert.deepEqual(panel.querySelectorAll("input").map((field) => field.id || field.attributes.name).sort(), [
    "local_client_id", "local_client_secret", "local_orgid", "local_admin_token",
    "local_source_disk_id", "local_source_type", "local_source_type",
    "local_source_vd_hash", "local_path", "local_page_limit", "local-batch-file",
  ].sort());
  for (const id of ["run-local-cross", "source_vd_hash", "x_source_vd_hash", "source_shared_fields", "x_source_shared_fields"]) {
    assert.equal(ui.document.getElementById(id), null, `legacy field ${id} must be removed`);
  }
  assert.equal(ui.document.querySelectorAll('input[name="source_type"]').length, 0);
  assert.equal(ui.document.querySelectorAll('input[name="x_source_type"]').length, 0);
  await selectMode(ui, "local");
  await selectMode(ui, "single");
  await selectMode(ui, "cross");
  await selectMode(ui, "local");
});

test("local form defaults to a personal source and changes the reader label", async () => {
  const ui = await loadUi();
  assert.equal(ui.document.querySelector('input[name="local_source_type"]:checked').value, "personal");
  assert.equal(ui.byId.get("local_source_shared_fields").hidden, true);
  await chooseRadio(ui, "local_source_type", "shared");
  assert.equal(ui.byId.get("local_source_shared_fields").hidden, false);
  assert.match(ui.byId.get("local_source_label").textContent, /email сотрудника с доступом к общему диску/);
  await chooseRadio(ui, "local_source_type", "personal");
  assert.equal(ui.byId.get("local_source_shared_fields").hidden, true);
  assert.doesNotMatch(ui.byId.get("local_source_label").textContent, /доступом к общему/);
});

for (const type of ["personal", "shared"]) {
  test(`local: ${type} source saves using only its own credentials`, async () => {
    const ui = await loadUi();
    fillLocal(ui);
    await selectMode(ui, "local");
    await chooseRadio(ui, "local_source_type", type);
    if (type === "shared") ui.byId.get("local_source_vd_hash").value = "https://disk.yandex.ru/client/vd/shared-hash/Finance";
    await ui.byId.get("run-local").fire("click");
    assertLocalRequest(ui, type);
    assert.equal(ui.requests[0].body.source_vd_hash, type === "shared" ? "https://disk.yandex.ru/client/vd/shared-hash/Finance" : "");
    assertSharedLogLock(ui, true);
    await ui.finishRun(type === "personal" ? "done" : "error");
    assertSharedLogLock(ui, false);
  });

  for (const value of ["", "3f90fe10-ee1a-48b1-b445-2b2f09f45af7", "reader@example test"]) {
    test(`local: ${type} rejects invalid email ${JSON.stringify(value)} before requesting`, async () => {
      const ui = await loadUi();
      fillLocal(ui);
      await chooseRadio(ui, "local_source_type", type);
      ui.byId.get("local_source_disk_id").value = value;
      ui.byId.get("local_source_vd_hash").value = "shared-hash";
      await ui.byId.get("run-local").fire("click");
      assert.equal(ui.requests.length, 0);
      assert.match(ui.byId.get("status").textContent, type === "shared" ? /email сотрудника с доступом/ : /email владельца личного/);
      assert.equal(ui.byId.get("run-local").disabled, false);
    });
  }
}

test("local: shared source requires its own hash", async () => {
  const ui = await loadUi();
  fillLocal(ui);
  await chooseRadio(ui, "local_source_type", "shared");
  ui.byId.get("destination_vd_hash").value = "unrelated-destination-hash";
  ui.byId.get("local_source_vd_hash").value = "   ";
  await ui.byId.get("run-local").fire("click");
  assert.equal(ui.requests.length, 0);
  assert.match(ui.byId.get("status").textContent, /SOURCE_VD_HASH/);
});

test("local: returning from shared to personal saves without a required hash", async () => {
  const ui = await loadUi();
  fillLocal(ui);
  await chooseRadio(ui, "local_source_type", "shared");
  await chooseRadio(ui, "local_source_type", "personal");
  await ui.byId.get("run-local").fire("click");
  assertLocalRequest(ui);
  assertSharedLogLock(ui, true);
  await ui.finishRun("done");
  assertSharedLogLock(ui, false);
});

test("local tab remains available and saves when cross-org mode is disabled", async () => {
  const ui = await loadUi({ cross_org: false });
  assert.equal(ui.document.getElementById("mode-cross"), null);
  assert.equal(ui.document.querySelector('.mode-tab[data-mode="cross"]'), null);
  await selectMode(ui, "local");
  fillLocal(ui);
  await ui.byId.get("run-local").fire("click");
  assertLocalRequest(ui);
});

test("late cross-org configuration preserves the selected local tab", async () => {
  const ui = await loadUi({ cross_org: false }, { delayConfig: true });
  await selectMode(ui, "local");
  ui.releaseConfig();
  assert.ok(await ui.configPromise);
  assert.equal(ui.document.getElementById("mode-cross"), null);
  assert.deepEqual(ui.document.querySelectorAll(".mode-panel").filter((panel) => panel.classList.contains("active")).map((panel) => panel.id), ["mode-local"]);
  assert.deepEqual(ui.document.querySelectorAll(".mode-tab").filter((tab) => tab.classList.contains("active")).map((tab) => tab.dataset.mode), ["local"]);
});

for (const cross of [false, true]) {
  const mode = cross ? "cross" : "single";

  test(`${mode}: transfer still uses its own fields and ignores local source selection`, async () => {
    const ui = await loadUi();
    fillLocal(ui);
    await chooseRadio(ui, "local_source_type", "shared");
    ui.byId.get("local_source_vd_hash").value = "unrelated-source-hash";
    fillTransfer(ui, cross);
    await selectMode(ui, mode);
    if (cross) {
      ui.byId.get("x_dst_client_id").value = "receiver-client";
      ui.byId.get("x_dst_client_secret").value = "receiver-secret";
      ui.byId.get("x_destination_disk_id").value = "receiver@example.test";
    } else ui.byId.get("destination_disk_id").value = "receiver@example.test";
    await ui.byId.get(cross ? "run-cross" : "run").fire("click");
    assert.equal(ui.requests.length, 1);
    assert.equal(ui.requests[0].url, cross ? "/api/run-cross" : "/api/run");
    assert.equal("source_type" in ui.requests[0].body, false);
    assert.equal("source_vd_hash" in ui.requests[0].body, false);
    assert.equal(ui.requests[0].body.source_disk_id, "reader@example.test");
    assert.equal(ui.requests[0].body.destination_disk_id, "receiver@example.test");
    assert.equal(ui.requests[0].body.destination_type, "personal");
    assertSharedLogLock(ui, true);
    await ui.finishRun("done");
    assertSharedLogLock(ui, false);
  });

  test(`${mode}: shared destination transfer keeps its own destination hash`, async () => {
    const ui = await loadUi();
    fillTransfer(ui, cross);
    if (cross) {
      ui.byId.get("x_dst_client_id").value = "receiver-client";
      ui.byId.get("x_dst_client_secret").value = "receiver-secret";
    }
    ui.byId.get(cross ? "x_destination_disk_id" : "destination_disk_id").value = "writer@example.test";
    ui.byId.get(cross ? "x_destination_vd_hash" : "destination_vd_hash").value = "destination-hash";
    await chooseRadio(ui, cross ? "x_dest_type" : "dest_type", "shared");
    await ui.byId.get(cross ? "run-cross" : "run").fire("click");
    assert.equal(ui.requests.length, 1);
    assert.equal(ui.requests[0].url, cross ? "/api/run-cross" : "/api/run");
    assert.equal(ui.requests[0].body.destination_type, "shared");
    assert.equal(ui.requests[0].body.destination_vd_hash, "destination-hash");
    assert.equal(ui.requests[0].body.destination_disk_id, "writer@example.test");
    assert.equal("source_type" in ui.requests[0].body, false);
    assert.equal("source_vd_hash" in ui.requests[0].body, false);
    assertSharedLogLock(ui, true);
    await ui.finishRun("error");
    assertSharedLogLock(ui, false);
  });

  test(`${mode}: batch transfer submits pairs, leaves the local tab accessible and locks its run button until done`, async () => {
    const ui = await loadUi();
    fillTransfer(ui, cross);
    if (cross) {
      ui.byId.get("x_dst_client_id").value = "receiver-client";
      ui.byId.get("x_dst_client_secret").value = "receiver-secret";
    }
    await selectMode(ui, mode);
    const subtabs = ui.document.querySelector(`[data-subtabs="${mode}"]`);
    await subtabs.querySelector('[data-subtab-target="many"]').fire("click");
    const textarea = ui.byId.get(cross ? "x_batch_pairs" : "batch_pairs");
    textarea.value = "SOURCE;DESTINATION\nsource@example.test;dest@example.test";
    await textarea.fire("input");
    await ui.byId.get(cross ? "run-cross" : "run").fire("click");
    assert.equal(ui.requests.length, 1);
    assert.equal(ui.requests[0].url, cross ? "/api/run-batch-cross" : "/api/run-batch");
    assert.deepEqual(ui.requests[0].body.pairs, [{ source_disk_id: "source@example.test", destination_disk_id: "dest@example.test" }]);
    assert.equal("source_disk_id" in ui.requests[0].body, false);
    assert.equal("source_type" in ui.requests[0].body, false);
    assert.equal("source_vd_hash" in ui.requests[0].body, false);
    assert.equal(ui.byId.get("run-local").hidden, false);
    assertSharedLogLock(ui, true);
    await ui.finishRun("done");
    assertSharedLogLock(ui, false);
    await selectMode(ui, "local");
    fillLocal(ui);
    await ui.byId.get("run-local").fire("click");
    assert.equal(ui.requests.length, 2);
    assert.equal(ui.requests[1].url, "/api/save-local");
  });

  test(`${mode}: shared destination batch still submits sources and one writer`, async () => {
    const ui = await loadUi();
    fillTransfer(ui, cross);
    if (cross) {
      ui.byId.get("x_dst_client_id").value = "receiver-client";
      ui.byId.get("x_dst_client_secret").value = "receiver-secret";
    }
    await chooseRadio(ui, cross ? "x_dest_type" : "dest_type", "shared");
    ui.byId.get(cross ? "x_destination_vd_hash" : "destination_vd_hash").value = "destination-hash";
    ui.byId.get(cross ? "x_batch_destination_disk_id" : "batch_destination_disk_id").value = "writer@example.test";
    const subtabs = ui.document.querySelector(`[data-subtabs="${mode}"]`);
    await subtabs.querySelector('[data-subtab-target="many"]').fire("click");
    const textarea = ui.byId.get(cross ? "x_batch_pairs" : "batch_pairs");
    textarea.value = "SOURCE\nemployee@example.test";
    await textarea.fire("input");
    await ui.byId.get(cross ? "run-cross" : "run").fire("click");
    assert.equal(ui.requests.length, 1, ui.byId.get("status").textContent);
    assert.equal(ui.requests[0].url, cross ? "/api/run-batch-cross" : "/api/run-batch");
    assert.deepEqual(ui.requests[0].body.pairs, [{ source_disk_id: "employee@example.test", destination_disk_id: "" }]);
    assert.equal(ui.requests[0].body.destination_disk_id, "writer@example.test");
    assert.equal(ui.requests[0].body.destination_type, "shared");
    assert.equal(ui.requests[0].body.destination_vd_hash, "destination-hash");
    assert.equal("source_type" in ui.requests[0].body, false);
    assert.equal("source_vd_hash" in ui.requests[0].body, false);
    assert.equal(ui.byId.get("run-local").hidden, false);
    assertSharedLogLock(ui, true);
    await ui.finishRun("done");
    assertSharedLogLock(ui, false);
  });
}
