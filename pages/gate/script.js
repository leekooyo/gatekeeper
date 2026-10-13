(function () {
  "use strict";

  let bridge = null;
  const $ = (sel, root) => (root || document).querySelector(sel);
  let state = null;

  function node(tag, cls, text) {
    const n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text != null) n.textContent = text;
    return n;
  }

  function toast(msg, err) {
    const t = $("#toast");
    t.textContent = msg;
    t.className = "toast" + (err ? " err" : "");
    clearTimeout(t._h);
    t._h = setTimeout(() => { t.className = "toast hidden"; }, 2600);
  }

  async function apiGet(endpoint, params) {
    if (bridge && bridge.apiGet) return bridge.apiGet(endpoint, params);
    const q = params ? "?" + new URLSearchParams(params).toString() : "";
    const r = await fetch("/api/plug/gatekeeper/" + endpoint + q, { credentials: "include" });
    return r.json();
  }

  async function apiPost(endpoint, body) {
    if (bridge && bridge.apiPost) return bridge.apiPost(endpoint, body);
    const r = await fetch("/api/plug/gatekeeper/" + endpoint, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    });
    return r.json();
  }

  async function safePost(endpoint, body) {
    try { return await apiPost(endpoint, body); }
    catch (e) { return { status: "error", message: String(e.message || e) }; }
  }

  function reportWrite(res, success) {
    if (res && res.status === "ok") {
      toast(success);
      refresh();
    } else toast((res && res.message) || "操作失败", true);
  }

  function applyTheme() {
    try {
      const ctx = bridge && bridge.getContext ? bridge.getContext() : null;
      if (ctx && ctx.isDark) document.documentElement.classList.add("dark");
    } catch (e) { /* noop */ }
  }

  async function refresh() {
    try {
      state = await apiGet("state");
    } catch (e) {
      toast("读取状态失败：" + e, true);
      return;
    }
    renderStatus();
    renderGroups();
  }

  async function loadLogs() {
    try {
      const res = await apiGet("logs");
      renderLogs(res && res.logs ? res.logs : []);
    } catch (e) { /* noop */ }
  }

  function renderStatus() {
    const on = !!state.on;
    $("#enable-dot").className = "dot " + (on ? "on" : "off");
    const parts = [
      on ? "总控已启用" : "总控已停用（当死人）",
      "@bot 概率 " + state.probability + "%",
      state.allow_upstream_wake ? "放行上游唤醒" : "严格只认 @bot",
      "已登记 " + state.configured_count + " 群",
      state.joined_count == null ? "实际加入数暂不可读" : "实际加入 " + state.joined_count + " 群",
      "默认 " + (state.default_mode || "plugin"),
    ];
    if (state.stale_count) parts.push("已退群但保留条目 " + state.stale_count + " 群");
    $("#status-line").textContent = parts.join(" · ");
    if (state.inventory_error) toast(state.inventory_error, true);
  }

  function renderGroups() {
    const box = $("#groups");
    box.textContent = "";
    if (!state.groups.length) {
      box.appendChild(node("div", "empty", "尚无群。QQ 里加入任意群即按默认 plugin 生效。"));
      return;
    }
    state.groups.forEach((g) => box.appendChild(renderRow(g)));
  }

  function renderRow(g) {
    const row = node("div", "group-row");
    const top = node("div", "group-top");
    top.appendChild(node("span", "gid", g.gid));
    if (g.name) top.appendChild(node("span", "group-name", g.name));
    if (!g.registered) top.appendChild(node("span", "badge", "未登记 · 默认 " + (state.default_mode || "plugin")));
    else if (g.joined === false) top.appendChild(node("span", "badge", "已退群 · 保留条目"));

    const sel = node("select", "mode");
    state.modes.forEach((m) => {
      const o = document.createElement("option");
      o.value = m;
      o.textContent = m;
      if (g.mode === m) o.selected = true;
      sel.appendChild(o);
    });
    sel.addEventListener("change", async () => {
      const res = await safePost("mode", { gid: g.gid, mode: sel.value });
      reportWrite(res, "已设置 " + g.gid + " → " + sel.value);
    });
    top.appendChild(sel);
    row.appendChild(top);

    if (g.mode === "plugin" || g.mode === "both") {
      const chips = node("div", "chips");
      const off = new Set(g.manual_off || []);
      const allBtn = node("button", "chip all", off.size ? "全不选" : "全选");
      allBtn.title = off.size ? "启用该群全部插件" : "禁用该群全部插件";
      allBtn.addEventListener("click", async () => {
        const enable = off.size > 0;
        const res = await safePost("plugin", { gid: g.gid, name: "all", enable: enable });
        reportWrite(res, enable ? "已启用该群全部插件" : "已禁用该群全部插件");
      });
      chips.appendChild(allBtn);
      state.plugins.forEach((name) => {
        const c = node("button", "chip" + (off.has(name) ? " off" : ""), name);
        c.title = off.has(name) ? "已在该群禁用，点击启用" : "点击在该群禁用";
        c.addEventListener("click", async () => {
          const enable = off.has(name);
          const res = await safePost("plugin", { gid: g.gid, name: name, enable: enable });
          reportWrite(res, name + (enable ? " 已启用" : " 已禁用"));
        });
        chips.appendChild(c);
      });
      row.appendChild(chips);
    } else {
      row.appendChild(node("div", "empty", g.mode === "llm"
        ? "仅 LLM：该群全部插件已在运行时被摘下。"
        : "全禁：该群零回复、零插件。"));
    }
    return row;
  }

  function renderLogs(logs) {
    const box = $("#logs");
    box.textContent = "";
    if (!logs.length) { box.appendChild(node("div", "empty", "暂无记录。")); return; }
    logs.forEach((it) => {
      const line = node("div", "log-line");
      line.appendChild(node("b", null, it.gid));
      line.appendChild(document.createTextNode("  " + it.mode + "  ·  " + it.action + "  ·  " + it.t));
      box.appendChild(line);
    });
  }

  function bind() {
    $("#btn-refresh").addEventListener("click", () => { refresh(); loadLogs(); });

    $("#add-form").addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const gid = $("#add-gid").value.trim();
      const mode = $("#add-mode").value;
      if (!gid) { toast("请输入群号（或 all / * 全选）", true); return; }
      const res = await safePost("mode", { gid: gid, mode: mode });
      if (res && res.status === "ok") {
        $("#add-gid").value = "";
      }
      reportWrite(res, res && res.gid === "all"
        ? "已批量设置当前加入的 " + (res.groups || 0) + " 个群 → " + mode
        : "已设置 " + gid + " → " + mode);
    });

    $("#btn-on").addEventListener("click", async () => {
      const res = await safePost("on", {});
      if (res && res.status === "ok") {
        const n = (res.removed || []).length;
        toast("总控已启用" + (n ? "，对账剔除 " + n + " 个退群条目" : ""));
        refresh();
      } else toast("启用失败", true);
    });

    $("#btn-off").addEventListener("click", async () => {
      if (!confirm("停用总控？插件将当死人：不拦截、不对账、不落盘。")) return;
      const res = await safePost("off", {});
      if (res && res.status === "ok") { toast("总控已停用"); refresh(); }
      else toast("停用失败", true);
    });
  }

  async function boot() {
    // AstrBot may append its SDK after this script. Resolve it after DOM ready.
    bridge = window.AstrBotPluginPage || null;
    if (bridge && bridge.ready) { try { await bridge.ready(); } catch (e) { /* noop */ } }
    applyTheme();
    bind();
    await refresh();
    await loadLogs();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
