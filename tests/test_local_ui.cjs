const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { test } = require("node:test");

class Element {
  constructor(tag, attributes = {}) {
    this.tagName = tag.toUpperCase();
    this.attributes = attributes;
    this.id = attributes.id || "";
    this.value = attributes.value || "";
    this.checked = "checked" in attributes;
    this.hidden = "hidden" in attributes;
    this.disabled = "disabled" in attributes;
    this.className = attributes.class || "";
    this.children = [];
    this.listeners = {};
    this.style = {};
    this.dataset = {};
    this.textContent = "";
    for (const [key, value] of Object.entries(attributes)) {
      if (key.startsWith("data-")) {
        this.dataset[key.slice(5).replace(/-([a-z])/g, (_, letter) => letter.toUpperCase())] = value;
      }
    }
    this.classList = {
      contains: (value) => this.className.split(/\s+/).includes(value),
      add: (value) => { if (!this.classList.contains(value)) this.className += " " + value; },
      remove: (value) => { this.className = this.className.split(/\s+/).filter((part) => part !== value).join(" "); },
      toggle: (value, enabled) => {
        const show = enabled === undefined ? !this.classList.contains(value) : enabled;
        if (show) this.classList.add(value); else this.classList.remove(value);
      },
    };
  }
  addEventListener(name, handler) { (this.listeners[name] ||= []).push(handler); }
  async fire(name) {
    if (name === "click" && this.disabled) return;
    await this.dispatch(name);
  }
  async dispatch(name) {
    for (const handler of this.listeners[name] || []) await handler({ target: this });
  }
  get title() { return this.attributes.title || ""; }
  set title(value) { this.attributes.title = String(value); }
  appendChild(node) { this.children.push(node); node.parent = this; return node; }
  setAttribute(name, value) { this.attributes[name] = value; }
  getAttribute(name) { return this.attributes[name] ?? null; }
  remove() {
    if (this.parent) this.parent.children = this.parent.children.filter((node) => node !== this);
    this.parent = null;
  }
  querySelectorAll(selector) {
    return this.children.flatMap((node) => [node, ...node.querySelectorAll("*")])
      .filter((node) => matches(node, selector));
  }
  querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
  closest(selector) {
    for (let node = this; node; node = node.parent) if (matches(node, selector)) return node;
    return null;
  }
}

function matches(node, selector) {
  if (selector.includes(",")) return selector.split(",").some((part) => matches(node, part.trim()));
  if (selector === "*") return true;
  for (const attr of selector.matchAll(/\[([\w-]+)(?:="([^"]*)")?\]/g)) {
    if (!(attr[1] in node.attributes) || (attr[2] !== undefined && node.attributes[attr[1]] !== attr[2])) return false;
  }
  if (selector.endsWith(":checked") && !node.checked) return false;
  const plain = selector.replace(/\[[^\]]*\]/g, "");
  const id = plain.match(/#([\w-]+)/);
  if (id && node.id !== id[1]) return false;
  for (const className of plain.matchAll(/\.([\w-]+)/g)) {
    if (!node.classList.contains(className[1])) return false;
  }
  const tag = selector.match(/^([a-z][\w-]*)/i);
  return !tag || node.tagName === tag[1].toUpperCase();
}

async function loadUi(config = { cross_org: true }, { delayConfig = false, failConfig = false } = {}) {
  const html = fs.readFileSync(path.resolve(__dirname, "../app/static/index.html"), "utf8");
  const root = new Element("document");
  const byId = new Map();
  const stack = [root];
  const markup = html.replace(/<(script|style)\b[^>]*>[\s\S]*?<\/\1>/gi, "");
  for (const match of markup.matchAll(/<\/?([a-z][\w-]*)\b([^>]*)>/gi)) {
    const tag = match[1].toLowerCase();
    if (match[0].startsWith("</")) {
      while (stack.length > 1 && stack.pop().tagName !== tag.toUpperCase()) {}
      continue;
    }
    const attributes = {};
    for (const attr of match[2].matchAll(/([\w:-]+)(?:="([^"]*)")?/g)) attributes[attr[1]] = attr[2] || "";
    const node = stack.at(-1).appendChild(new Element(tag, attributes));
    if (node.id) byId.set(node.id, node);
    if (!["area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"].includes(tag)) stack.push(node);
  }
  const requests = [];
  const streams = [];
  let releaseConfig;
  const configReady = new Promise((resolve) => { releaseConfig = resolve; });
  const documentListeners = {};
  const document = {
    body: root.querySelector("body"), documentElement: root.querySelector("html"),
    getElementById: (id) => root.querySelector(`#${id}`),
    querySelector: (selector) => root.querySelector(selector),
    querySelectorAll: (selector) => root.querySelectorAll(selector),
    createElement: (tag) => new Element(tag),
    addEventListener: (name, handler) => { (documentListeners[name] ||= []).push(handler); },
    dispatchEvent: (event) => {
      for (const handler of documentListeners[event.type] || []) handler(event);
    },
  };
  const context = {
    document, window: {}, navigator: {}, Event: class { constructor(type) { this.type = type; } },
    localStorage: { getItem: (key) => key === "ydc_disclaimer_accepted_v1" ? "1" : null, setItem() {} },
    setTimeout, clearTimeout,
    fetch: async (url, options) => {
      if (url === "/api/config") {
        if (failConfig) throw new Error("Configuration unavailable");
        return { json: async () => { if (delayConfig) await configReady; return config; } };
      }
      assert.equal(options?.method, "POST", "unexpected request from the UI");
      requests.push({ url, body: JSON.parse(options.body) });
      return { ok: true, json: async () => ({ job_id: "local-test" }) };
    },
    EventSource: class {
      constructor(url) { this.url = url; this.listeners = {}; streams.push(this); }
      addEventListener(name, handler) { this.listeners[name] = handler; }
      close() {}
    },
  };
  vm.createContext(context);
  for (const script of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)) {
    if (script[1].trim()) vm.runInContext(script[1], context);
  }
  if (!delayConfig) {
    const result = await context.window.diskCopyConfigPromise;
    if (failConfig) assert.equal(result, null);
    else assert.ok(result, "configuration must finish successfully");
  }
  return { byId, document, requests, streams, releaseConfig, configPromise: context.window.diskCopyConfigPromise };
}

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
  assert.equal(ui.streams[0].url, "/api/run/local-test/stream");
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
    assert.equal(tab.hidden, false, "unreleased mode stays discoverable");
    assert.equal("hidden" in tab.attributes, false);
    assert.equal(tab.disabled, true);
    assert.equal("disabled" in tab.attributes, true, "HTML must disable the tab before configuration arrives");
    assert.equal(tab.title, "В разработке");
    assert.equal(ui.byId.get("mode-onprem").hidden, true);
    await selectMode(ui, "local");
    await tab.fire("click");
    await tab.dispatch("click");
    assert.deepEqual(ui.document.querySelectorAll(".mode-panel").filter((panel) => panel.classList.contains("active")).map((panel) => panel.id), ["mode-local"], "disabled clicks and manually dispatched events cannot switch into onprem");
    assert.equal(ui.requests.length, 0);
    fillLocal(ui);
    await ui.byId.get("run-local").fire("click");
    assertLocalRequest(ui);
    ui.streams[0].listeners.done({ data: "ok" });
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
    "local_source_vd_hash", "local_path", "local_page_limit",
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
    ui.streams[0].listeners[type === "personal" ? "done" : "error"]({ data: type === "personal" ? "ok" : "test failure" });
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
  ui.streams[0].listeners.done({ data: "ok" });
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
    ui.streams[0].listeners.done({ data: "ok" });
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
    ui.streams[0].listeners.error({ data: "test failure" });
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
    ui.streams[0].listeners.done({ data: "ok" });
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
    ui.streams[0].listeners.done({ data: "ok" });
    assertSharedLogLock(ui, false);
  });
}
