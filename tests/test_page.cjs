const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

(async () => {
  const nodes = new Map();
  const get = (key) => {
    if (!nodes.has(key)) nodes.set(key, {textContent: '', className: '', addEventListener() {}, appendChild() {}});
    return nodes.get(key);
  };
  let boot, fetches = 0, bridgeCalls = 0;
  const document = {
    readyState: 'loading', querySelector: get,
    documentElement: {classList: {add() {}}},
    addEventListener(name, cb) { if (name === 'DOMContentLoaded') boot = cb; },
    createElement() { return {classList: {add() {}}, appendChild() {}, addEventListener() {}, style: {}}; },
  };
  const window = {};
  const context = vm.createContext({window, document, URLSearchParams, clearTimeout() {}, setTimeout() {},
    fetch() { fetches++; return Promise.reject(new Error('sandbox blocks direct fetch')); }});
  vm.runInContext(fs.readFileSync(path.join(__dirname, '../pages/gate/script.js'), 'utf8'), context);
  // AstrBot injects its SDK after the plugin script, before DOMContentLoaded.
  window.AstrBotPluginPage = {
    ready: async () => {}, getContext: () => ({}),
    apiGet: async (endpoint) => {
      bridgeCalls++;
      return endpoint === 'state' ? {enabled: true, probability: 30, groups: [], modes: [], plugins: [],
        configured_count: 0, joined_count: 3, unconfigured_count: 3, stale_count: 0, inventory_error: null} : {logs: []};
    },
  };
  await boot();
  assert.equal(fetches, 0, 'must use late-injected bridge instead of sandbox fetch');
  assert.equal(bridgeCalls, 2);
  assert.match(get('#status-line').textContent, /实际加入 3 群/);
  console.log('PASS: late-injected SDK loads state and real joined count');
})();
