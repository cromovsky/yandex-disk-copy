const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

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
  replaceChildren(...nodes) { this.children = []; this.textContent = ""; nodes.forEach((node) => this.appendChild(node)); }
  click() { return this.fire("click"); }
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

async function loadUi(config = { cross_org: true }, { delayConfig = false, failConfig = false, initialJobs = [], initialEvents = {}, storageData = {}, journalOverride = null } = {}) {
  const html = fs.readFileSync(path.resolve(__dirname, "../../app/static/index.html"), "utf8");
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
  const reads = [], storageWrites = [], downloads = [];
  const jobs = JSON.parse(JSON.stringify(initialJobs));
  const events = JSON.parse(JSON.stringify(initialEvents));
  const stored = {...storageData};
  const storage = {getItem: (key) => key === "ydc_disclaimer_accepted_v1" ? "1" : stored[key] || null,
    setItem(key, value) { stored[key] = value; storageWrites.push({key, value}); }, removeItem(key) { delete stored[key]; }};
  const respond = (value, status = 200) => ({ok: status < 400, status, json: async () => JSON.parse(JSON.stringify(value)), blob: async () => new Blob(["journal"]) });
  const mockFetch = async (address, options = {}) => {
    if (address === "/api/config") {
      if (failConfig) throw new Error("Configuration unavailable");
      return { json: async () => { if (delayConfig) await configReady; return config; } };
    }
    const url = new URL(address, "http://localhost");
    const method = options.method || "GET";
    const body = options.body ? JSON.parse(options.body) : null;
    if (method === "GET") reads.push({url: address, options});
    else requests.push({url: address, body});
    if (journalOverride) { const result = await journalOverride({url, method, body, jobs, events}); if (result) return result; }
    if (url.pathname === "/api/history") {
      const mode = url.searchParams.get("mode"), offset = Number(url.searchParams.get("offset"));
      const matches = jobs.filter((job) => job.mode === mode);
      return respond({jobs: matches.slice(offset, offset + 50), has_more: matches.length > offset + 50,
        running_count: matches.filter((job) => job.status === "running").length,
        active_jobs: matches.filter((job) => job.status === "running")});
    }
    if (url.pathname === "/api/history/cleanup") {
      const selected = jobs.filter((job) => job.mode === body.mode);
      if (selected.some((job) => job.status === "running")) return respond({detail: "journal_in_use"}, 409);
      for (let index = jobs.length - 1; index >= 0; index -= 1) if (jobs[index].mode === body.mode) jobs.splice(index, 1);
      return respond({removed_jobs: selected.length, removed_events: 7, removed_bytes: 2048});
    }
    if (method === "POST") {
      const mode = url.pathname.includes("local") ? "local" : url.pathname.includes("cross") ? "cross" : "single";
      const id = requests.length === 1 ? "local-test" : "run-" + requests.length;
      jobs.unshift({id, mode, status: "running", kind: body.pairs ? "batch" : "single", created_at: "2026-10-14T09:00:00Z",
        retry_enabled: body.retry_enabled !== false, source: body.source_disk_id || "Пакет", destination: body.destination_disk_id || "Локальный диск", error: null,
        summary: {saved: 0, skipped: 0, failed: 0}});
      return respond({job_id: id});
    }
    const id = url.pathname.split("/")[3], job = jobs.find((item) => item.id === id);
    if (!job) return respond({detail: "not_found"}, 404);
    if (url.pathname.split("/").length === 5 && url.pathname.endsWith("/events")) {
      const list = events[id] || [], after = Number(url.searchParams.get("after")), tail = Number(url.searchParams.get("tail"));
      return respond({events: tail ? list.slice(-tail) : list.filter((event) => event.sequence > after).slice(0, 500), last_sequence: list.at(-1)?.sequence || 0});
    }
    return respond(job);
  };
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
  const windowListeners = {};
  const window = {localStorage: storage, addEventListener(name, handler) { (windowListeners[name] ||= []).push(handler); },
    dispatchEvent(event) { for (const handler of windowListeners[event.type] || []) handler(event); }};
  const context = {
    document, window, navigator: {}, Event: class { constructor(type) { this.type = type; } },
    localStorage: storage, setTimeout() {return 1;}, clearTimeout() {}, fetch: mockFetch, Blob,
    URL: class extends URL { static createObjectURL(blob) { downloads.push(blob); return "blob:test"; } static revokeObjectURL() {} },
  };
  vm.createContext(context);
  for (const script of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)) {
    if (script[1].trim()) vm.runInContext(script[1], context);
  }
  vm.runInContext(fs.readFileSync(path.resolve(__dirname, "../../app/static/retry-controls.js"), "utf8"), context);
  vm.runInContext(fs.readFileSync(path.resolve(__dirname, "../../app/static/run-journal.js"), "utf8"), context);
  await context.window.runJournal.refresh();
  await new Promise((resolve) => setImmediate(resolve));
  if (!delayConfig) {
    const result = await context.window.diskCopyConfigPromise;
    if (failConfig) assert.equal(result, null);
    else assert.ok(result, "configuration must finish successfully");
  }
  return { byId, document, window, requests, reads, jobs, events, storageWrites, stored, downloads, releaseConfig,
    journal: context.window.runJournal, configPromise: context.window.diskCopyConfigPromise,
    async finishRun(status = "done") { const active = jobs.find((job) => job.status === "running");
      if (active) {active.status = status; active.error = status === "error" ? "transfer_failed" : null;}
      await context.window.runJournal.refresh();
    },
  };
}

module.exports = {loadUi, Element};
