const API = "/api";
const state = { logs: { page: 1, pageSize: 25, query: {} }, alarms: { page: 1, pageSize: 25, query: {} }, sources: [], rules: [], alarms: [] };

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>'"]/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" }[character]));
}

function formatDate(value) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "—" : inputDateValue(date);
}

function inputDateValue(date) {
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const pad = (value) => String(value).padStart(2, "0");
  return `${pad(date.getDate())} ${months[date.getMonth()]} ${date.getFullYear()}, ${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function apiDate(value) {
  const match = /^(\d{2}) ([A-Za-z]{3}) (\d{4}), (\d{2}):(\d{2})$/.exec(value.trim());
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const month = match ? months.indexOf(match[2].slice(0, 1).toUpperCase() + match[2].slice(1).toLowerCase()) : -1;
  if (!match || month < 0) throw new Error("Use date format: DD Mon YYYY, HH:mm");
  const date = new Date(Number(match[3]), month, Number(match[1]), Number(match[4]), Number(match[5]));
  if (Number.isNaN(date.getTime()) || date.getFullYear() !== Number(match[3]) || date.getMonth() !== month || date.getDate() !== Number(match[1]) || date.getHours() !== Number(match[4]) || date.getMinutes() !== Number(match[5])) throw new Error("Enter a valid date and time");
  return date.toISOString();
}

function queryString(values) {
  const params = new URLSearchParams();
  Object.entries(values).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") params.set(key, value);
  });
  return params.toString();
}

async function fetchJson(path, options = {}) {
  const response = await fetch(`${API}${path}`, { headers: { "Content-Type": "application/json", ...(options.headers || {}) }, ...options });
  if (!response.ok) {
    let detail = `Request failed (${response.status})`;
    try { const body = await response.json(); detail = body.detail || detail; } catch (_) { }
    throw new Error(Array.isArray(detail) ? detail.map((item) => item.msg).join(", ") : detail);
  }
  if (response.status === 204) return null;
  return response.json();
}

let toastTimer;
function showToast(message, error = false) {
  const toast = $("#toast");
  toast.textContent = message;
  toast.style.background = error ? "#b43a49" : "#172033";
  toast.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => toast.classList.remove("show"), 3200);
}

let confirmationResolver;
function askConfirmation(message) {
  $("#confirm-message").textContent = message;
  $("#confirm-modal").classList.remove("hidden");
  return new Promise((resolve) => { confirmationResolver = resolve; });
}

function finishConfirmation(result) {
  $("#confirm-modal").classList.add("hidden");
  if (confirmationResolver) confirmationResolver(result);
  confirmationResolver = undefined;
}

function showView(view) {
  $$(".view").forEach((item) => item.classList.toggle("active", item.id === `view-${view}`));
  $$(".nav-item").forEach((item) => item.classList.toggle("active", item.dataset.view === view));
  if (view === "dashboard") refreshDashboard();
  if (view === "logs") loadLogs();
  if (view === "sources") loadSources();
  if (view === "rules") loadRules();
  if (view === "alarms") loadAlarms();
}

async function checkHealth() {
  const pill = $("#health-status");
  try { const response = await fetch("/health"); if (!response.ok) throw new Error(); pill.textContent = "API healthy"; pill.className = "health-pill ok"; }
  catch (_) { pill.textContent = "API unavailable"; pill.className = "health-pill fail"; }
}

function setDefaultLogRange() {
  const now = new Date();
  $("#logs-filter [name=to]").value = inputDateValue(now);
  $("#logs-filter [name=from]").value = inputDateValue(new Date(now.getTime() - 86400000));
}

function setDefaultAlarmRange() {
  const now = new Date();
  $("#alarms-filter [name=to]").value = inputDateValue(now);
  $("#alarms-filter [name=from]").value = inputDateValue(new Date(now.getTime() - 86400000));
}

function populateSources() {
  const options = state.sources.map((source) => `<option value="${escapeHtml(source.sourceIp)}">${escapeHtml(source.sourceIp)}</option>`).join("");
  ["#log-source-filter", "#alarm-source-filter", "#rule-source"].forEach((selector) => {
    const select = $(selector);
    if (!select) return;
    const first = selector === "#rule-source" ? '<option value="">Select a source</option>' : '<option value="">All sources</option>';
    const selected = select.value;
    select.innerHTML = first + options;
    if (selected) select.value = selected;
  });
}

async function loadSources() {
  try {
    state.sources = await fetchJson("/sources");
    populateSources();
    const rows = state.sources.map((source) => `<tr><td><strong>${escapeHtml(source.sourceIp)}</strong></td><td>${source.logCount.toLocaleString()}</td><td>${formatDate(source.firstSeen)}</td><td>${formatDate(source.lastSeen)}</td><td><button class="text-button source-logs" data-source="${escapeHtml(source.sourceIp)}">View logs →</button></td></tr>`).join("");
    $("#sources-table").innerHTML = rows || '<tr><td colspan="5" class="empty">No sources found.</td></tr>';
    renderDashboardSources();
  } catch (error) { $("#sources-table").innerHTML = `<tr><td colspan="5" class="empty">${escapeHtml(error.message)}</td></tr>`; }
}

function renderDashboardSources() {
  const rows = state.sources.slice(0, 5).map((source) => `<div class="source-row"><div><div class="source-ip">${escapeHtml(source.sourceIp)}</div><div class="source-count">Last seen ${escapeHtml(formatDate(source.lastSeen))}</div></div><strong>${source.logCount.toLocaleString()}</strong></div>`).join("");
  $("#dashboard-sources").innerHTML = rows || '<div class="empty">No source data yet.</div>';
}

async function loadLogs() {
  if (!state.logs.query.from) {
    const form = $("#logs-filter");
    state.logs.query = { from: apiDate(form.from.value), to: apiDate(form.to.value), sourceIp: form.sourceIp.value, q: form.q.value };
  }
  const query = { ...state.logs.query, page: state.logs.page, pageSize: state.logs.pageSize };
  try {
    const result = await fetchJson(`/logs?${queryString(query)}`);
    const rows = result.items.map((log) => `<tr><td>${escapeHtml(formatDate(log.receivedAt))}</td><td><span class="badge ${log.protocol.toLowerCase()}">${escapeHtml(log.protocol)}</span></td><td>${escapeHtml(log.sourceIp)}<small class="muted"><br>:${log.sourcePort}</small></td><td class="payload" title="${escapeHtml(log.rawLog)}">${escapeHtml(log.rawLog)}</td><td>${log.rawLogSize.toLocaleString()}</td></tr>`).join("");
    $("#logs-table").innerHTML = rows || '<tr><td colspan="5" class="empty">No logs match these filters.</td></tr>';
    $("#logs-total").textContent = `${result.total.toLocaleString()} records`;
    $("#logs-page").textContent = `Page ${result.page}`;
    $("#logs-prev").disabled = result.page <= 1;
    $("#logs-next").disabled = result.page * result.pageSize >= result.total;
  } catch (error) { $("#logs-table").innerHTML = `<tr><td colspan="5" class="empty">${escapeHtml(error.message)}</td></tr>`; }
}

function renderConditions(conditions = [{ field: "rawLog", operator: "contains", value: "" }]) {
  $("#conditions").innerHTML = conditions.map((condition, index) => `<div class="condition-row" data-index="${index}"><select name="field"><option value="rawLog" ${condition.field === "rawLog" ? "selected" : ""}>rawLog</option><option value="sourceIp" ${condition.field === "sourceIp" ? "selected" : ""}>sourceIp</option></select><select name="operator"><option value="contains" ${condition.operator === "contains" ? "selected" : ""}>contains</option><option value="equals" ${condition.operator === "equals" ? "selected" : ""}>equals</option><option value="startsWith" ${condition.operator === "startsWith" ? "selected" : ""}>startsWith</option><option value="regex" ${condition.operator === "regex" ? "selected" : ""}>regex</option></select><input name="value" value="${escapeHtml(condition.value)}" required placeholder="Match value"><button type="button" class="remove-condition" aria-label="Remove condition">×</button></div>`).join("");
}

function openRuleEditor(rule = null) {
  $("#rule-editor").classList.remove("hidden");
  $("#rule-editor-title").textContent = rule ? "Edit correlation rule" : "New correlation rule";
  const form = $("#rule-form");
  const fields = form.elements;
  form.reset();
  fields.id.value = rule?.id || "";
  fields.name.value = rule?.name || "";
  fields.enabled.checked = rule ? rule.enabled : true;
  fields.sourceIp.value = rule?.sourceIp || "";
  fields.actionAlarm.checked = rule ? rule.actions.includes("Alarm") : true;
  fields.actionEmail.checked = rule ? rule.actions.includes("Email") : false;
  fields.emailTo.value = rule?.emailTo?.join(", ") || "";
  renderConditions(rule?.conditions);
  $("#email-to-row").classList.toggle("hidden", !form.actionEmail.checked);
  $("#rule-editor").scrollIntoView({ behavior: "smooth", block: "start" });
}

async function loadRules() {
  try {
    state.rules = await fetchJson("/rules");
    const rows = state.rules.map((rule) => `<tr><td><strong class="rule-name" title="${escapeHtml(rule.name)}">${escapeHtml(rule.name)}</strong><small class="muted rule-id" title="${escapeHtml(rule.id)}">${escapeHtml(rule.id)}</small></td><td>${escapeHtml(rule.sourceIp)}</td><td>${rule.conditions.length} condition${rule.conditions.length === 1 ? "" : "s"}</td><td>${rule.actions.map((action) => `<span class="badge ${action === "Email" ? "warning" : "udp"}">${escapeHtml(action)}</span>`).join(" ")}</td><td><span class="badge ${rule.enabled ? "success" : "disabled"}">${rule.enabled ? "Enabled" : "Disabled"}</span></td><td><div class="rule-actions"><button class="text-button edit-rule" data-id="${escapeHtml(rule.id)}">Edit</button><button class="text-button delete-rule" data-id="${escapeHtml(rule.id)}">Delete</button></div></td></tr>`).join("");
    $("#rules-table").innerHTML = rows || '<tr><td colspan="6" class="empty">No rules configured.</td></tr>';
    $("#metric-rules").textContent = state.rules.filter((rule) => rule.enabled).length.toLocaleString();
  } catch (error) { $("#rules-table").innerHTML = `<tr><td colspan="6" class="empty">${escapeHtml(error.message)}</td></tr>`; }
}

async function saveRule(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const fields = form.elements;
  const actions = [];
  if (fields.actionAlarm.checked) actions.push("Alarm");
  if (fields.actionEmail.checked) actions.push("Email");
  const conditions = $$("#conditions .condition-row").map((row) => ({ field: row.querySelector("[name=field]").value, operator: row.querySelector("[name=operator]").value, value: row.querySelector("[name=value]").value }));
  const body = { name: fields.name.value, enabled: fields.enabled.checked, sourceIp: fields.sourceIp.value, conditions, actions, emailTo: fields.actionEmail.checked ? fields.emailTo.value.split(",").map((item) => item.trim()).filter(Boolean) : [] };
  try {
    const id = fields.id.value;
    await fetchJson(id ? `/rules/${encodeURIComponent(id)}` : "/rules", { method: id ? "PUT" : "POST", body: JSON.stringify(body) });
    showToast(id ? "Rule updated" : "Rule created");
    $("#rule-editor").classList.add("hidden");
    await loadRules();
  } catch (error) { showToast(error.message, true); }
}

async function loadAlarms() {
  const query = { ...state.alarms.query, page: state.alarms.page, pageSize: state.alarms.pageSize };
  try {
    const result = await fetchJson(`/alarms?${queryString(query)}`);
    state.alarms = { ...state.alarms, items: result.items };
    const rows = result.items.map((alarm) => `<tr><td>${escapeHtml(formatDate(alarm.createdAt))}</td><td><strong class="rule-name" title="${escapeHtml(alarm.ruleName)}">${escapeHtml(alarm.ruleName)}</strong><small class="muted rule-id" title="${escapeHtml(alarm.ruleId)}">${escapeHtml(alarm.ruleId)}</small></td><td>${escapeHtml(alarm.sourceIp)}</td><td class="payload" title="${escapeHtml(alarm.rawLog)}">${escapeHtml(alarm.rawLog)}</td><td><span class="badge ${alarm.emailSent ? "success" : "warning"}">${alarm.emailSent ? "Sent" : "Pending"}</span></td><td><button class="text-button alarm-detail-button" data-id="${escapeHtml(alarm.id)}">Inspect →</button></td></tr>`).join("");
    $("#alarms-table").innerHTML = rows || '<tr><td colspan="6" class="empty">No alarms match these filters.</td></tr>';
    $("#alarms-total").textContent = `${result.total.toLocaleString()} alarms`;
    $("#alarms-page").textContent = `Page ${result.page}`;
    $("#alarms-prev").disabled = result.page <= 1;
    $("#alarms-next").disabled = result.page * result.pageSize >= result.total;
    renderDashboardAlarms(result.items.slice(0, 5));
  } catch (error) { $("#alarms-table").innerHTML = `<tr><td colspan="6" class="empty">${escapeHtml(error.message)}</td></tr>`; }
}

function renderDashboardAlarms(items) {
  $("#dashboard-alarms").innerHTML = items.map((alarm) => `<div class="alarm-row"><strong>${escapeHtml(alarm.ruleName)}</strong><small>${escapeHtml(alarm.sourceIp)} · ${escapeHtml(formatDate(alarm.createdAt))}</small></div>`).join("") || '<div class="empty">No alarms in the selected window.</div>';
}

async function openAlarm(id) {
  try {
    const alarm = await fetchJson(`/alarms/${encodeURIComponent(id)}`);
    $("#alarm-modal-title").textContent = alarm.ruleName;
    const log = alarm.log;
    const logDetails = log ? `<div class="detail-grid"><div class="detail-cell"><span>Protocol</span><strong>${escapeHtml(log.protocol)}</strong></div><div class="detail-cell"><span>Received</span><strong>${escapeHtml(formatDate(log.receivedAt))}</strong></div><div class="detail-cell"><span>Source port</span><strong>${log.sourcePort}</strong></div><div class="detail-cell"><span>Payload size</span><strong>${log.rawLogSize.toLocaleString()} bytes</strong></div></div><p class="eyebrow">TRIGGERING RAW LOG</p><div class="detail-log">${escapeHtml(log.rawLog)}</div>` : `<p class="eyebrow">TRIGGERING RAW LOG</p><div class="detail-log">${escapeHtml(alarm.rawLog)}</div>`;
    $("#alarm-detail").innerHTML = `<div class="detail-grid"><div class="detail-cell"><span>Source</span><strong>${escapeHtml(alarm.sourceIp)}</strong></div><div class="detail-cell"><span>Email</span><strong>${alarm.emailSent ? "Delivered" : "Not delivered"}</strong></div><div class="detail-cell"><span>Created</span><strong>${escapeHtml(formatDate(alarm.createdAt))}</strong></div><div class="detail-cell"><span>Log ID</span><strong>${escapeHtml(alarm.logId)}</strong></div></div>${logDetails}`;
    $("#alarm-modal").classList.remove("hidden");
  } catch (error) { showToast(error.message, true); }
}

async function refreshDashboard() {
  const end = new Date();
  const start = new Date(end.getTime() - 86400000);
  const range = { from: start.toISOString(), to: end.toISOString(), pageSize: 1 };
  try {
    const [logs, sources, rules, alarms] = await Promise.all([fetchJson(`/logs?${queryString(range)}`), fetchJson("/sources"), fetchJson("/rules"), fetchJson(`/alarms?${queryString(range)}`)]);
    state.sources = sources;
    state.rules = rules;
    populateSources();
    $("#metric-logs").textContent = logs.total.toLocaleString();
    $("#metric-sources").textContent = sources.length.toLocaleString();
    $("#metric-rules").textContent = rules.filter((rule) => rule.enabled).length.toLocaleString();
    $("#metric-alarms").textContent = alarms.total.toLocaleString();
    renderDashboardSources();
    renderDashboardAlarms(alarms.items);
  } catch (error) { showToast(error.message, true); }
}

function bindEvents() {
  document.addEventListener("click", async (event) => {
    const viewTarget = event.target.closest("[data-view]");
    if (viewTarget) { event.preventDefault(); showView(viewTarget.dataset.view); return; }
    const sourceButton = event.target.closest(".source-logs");
    if (sourceButton) { showView("logs"); $("#log-source-filter").value = sourceButton.dataset.source; $("#logs-filter").dispatchEvent(new Event("submit")); return; }
    const editButton = event.target.closest(".edit-rule");
    if (editButton) { openRuleEditor(state.rules.find((rule) => rule.id === editButton.dataset.id)); return; }
    const deleteButton = event.target.closest(".delete-rule");
    if (deleteButton) { const confirmed = await askConfirmation("This rule will be permanently removed."); if (!confirmed) return; try { await fetchJson(`/rules/${encodeURIComponent(deleteButton.dataset.id)}`, { method: "DELETE" }); showToast("Rule deleted"); await loadRules(); } catch (error) { showToast(error.message, true); } return; }
    const alarmButton = event.target.closest(".alarm-detail-button");
    if (alarmButton) openAlarm(alarmButton.dataset.id);
    if (event.target.closest(".remove-condition")) event.target.closest(".condition-row").remove();
  });
  $("#refresh-dashboard").addEventListener("click", refreshDashboard);
  $("#refresh-sources").addEventListener("click", loadSources);
  $("#new-rule").addEventListener("click", () => openRuleEditor());
  $("#close-rule").addEventListener("click", () => $("#rule-editor").classList.add("hidden"));
  $("#cancel-rule").addEventListener("click", () => $("#rule-editor").classList.add("hidden"));
  $("#add-condition").addEventListener("click", () => { const current = $$("#conditions .condition-row").map((row) => ({ field: row.querySelector("[name=field]").value, operator: row.querySelector("[name=operator]").value, value: row.querySelector("[name=value]").value })); renderConditions([...current, { field: "rawLog", operator: "contains", value: "" }]); });
  $("#rule-form").addEventListener("submit", saveRule);
  $("#rule-form [name=actionEmail]").addEventListener("change", (event) => $("#email-to-row").classList.toggle("hidden", !event.target.checked));
  $("#logs-filter").addEventListener("submit", (event) => { event.preventDefault(); const form = event.currentTarget; try { state.logs.page = 1; state.logs.query = { from: apiDate(form.from.value), to: apiDate(form.to.value), sourceIp: form.sourceIp.value, q: form.q.value }; loadLogs(); } catch (error) { showToast(error.message, true); } });
  $("#alarms-filter").addEventListener("submit", (event) => { event.preventDefault(); const form = event.currentTarget; try { state.alarms.page = 1; state.alarms.query = { from: form.from.value ? apiDate(form.from.value) : "", to: form.to.value ? apiDate(form.to.value) : "", sourceIp: form.sourceIp.value, ruleId: form.ruleId.value }; loadAlarms(); } catch (error) { showToast(error.message, true); } });
  $("#logs-prev").addEventListener("click", () => { if (state.logs.page > 1) { state.logs.page -= 1; loadLogs(); } });
  $("#logs-next").addEventListener("click", () => { state.logs.page += 1; loadLogs(); });
  $("#alarms-prev").addEventListener("click", () => { if (state.alarms.page > 1) { state.alarms.page -= 1; loadAlarms(); } });
  $("#alarms-next").addEventListener("click", () => { state.alarms.page += 1; loadAlarms(); });
  $("#close-alarm").addEventListener("click", () => $("#alarm-modal").classList.add("hidden"));
  $("#alarm-modal").addEventListener("click", (event) => { if (event.target.id === "alarm-modal") $("#alarm-modal").classList.add("hidden"); });
  $("#close-confirm").addEventListener("click", () => finishConfirmation(false));
  $("#cancel-confirm").addEventListener("click", () => finishConfirmation(false));
  $("#accept-confirm").addEventListener("click", () => finishConfirmation(true));
  $("#confirm-modal").addEventListener("click", (event) => { if (event.target.id === "confirm-modal") finishConfirmation(false); });
}

document.addEventListener("DOMContentLoaded", () => {
  setDefaultLogRange();
  setDefaultAlarmRange();
  bindEvents();
  checkHealth();
  loadSources();
  refreshDashboard();
});
