const number = new Intl.NumberFormat('vi-VN');
const decimal = new Intl.NumberFormat('vi-VN', { maximumFractionDigits: 1 });
const money = new Intl.NumberFormat('vi-VN', { style: 'currency', currency: 'VND', maximumFractionDigits: 0 });
const time = new Intl.DateTimeFormat('vi-VN', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
const dateTime = new Intl.DateTimeFormat('vi-VN', { dateStyle: 'short', timeStyle: 'medium' });

const reasonScores = {
  'Số tiền giao dịch vượt ngưỡng': 60,
  'Nhiều giao dịch trong thời gian ngắn': 55,
  'Thay đổi quốc gia bất thường trong thời gian ngắn': 70,
  'Thiết bị vừa thay đổi': 15,
};

let generatorStatus = null;
let generatorBusy = false;
let currentAlertId = null;
let toastTimer = null;
let appliedEventParams = null;
let eventsLoading = false;
let eventsLastLoaded = 0;
const riskWindowOptions = new Set([0, 60, 300, 900, 3600, 21600, 86400]);
let riskWindowSeconds = Number(window.localStorage.getItem('riskWindowSeconds') || 60);
if (!riskWindowOptions.has(riskWindowSeconds)) riskWindowSeconds = 60;

function td(text, className = '') {
  const cell = document.createElement('td');
  cell.textContent = text ?? '—';
  if (className) cell.className = className;
  return cell;
}

function badge(value) {
  const cell = document.createElement('td');
  const label = document.createElement('span');
  label.className = `badge badge-${String(value).toLowerCase()}`;
  label.textContent = value;
  cell.append(label);
  return cell;
}

function caseAction(alertId) {
  const cell = document.createElement('td');
  if (!alertId) {
    cell.textContent = '—';
    cell.className = 'no-alert';
    return cell;
  }
  const button = document.createElement('button');
  button.type = 'button';
  button.className = 'case-link';
  button.textContent = 'Xem hồ sơ';
  button.addEventListener('click', event => {
    event.stopPropagation();
    openAlert(alertId);
  });
  button.addEventListener('keydown', event => event.stopPropagation());
  cell.append(button);
  return cell;
}

function showToast(message, isError = false) {
  const toast = document.querySelector('#toast');
  window.clearTimeout(toastTimer);
  toast.textContent = message;
  toast.classList.toggle('error', isError);
  toast.classList.add('show');
  toastTimer = window.setTimeout(() => toast.classList.remove('show'), 3200);
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, { cache: 'no-store', ...options });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
  return data;
}

function renderAlerts(alerts) {
  const body = document.querySelector('#alerts-body');
  if (!alerts.length) return;
  const limit = window.matchMedia('(max-width: 680px)').matches ? 6 : 10;
  body.replaceChildren(...alerts.slice(0, limit).map(alert => {
    const row = document.createElement('tr');
    row.className = 'alert-row';
    row.tabIndex = 0;
    row.setAttribute('role', 'button');
    row.setAttribute('aria-label', `Mở cảnh báo ${alert.severity} của ${alert.accountId}`);
    row.addEventListener('click', () => openAlert(alert.alertId));
    row.addEventListener('keydown', event => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        openAlert(alert.alertId);
      }
    });
    row.append(
      badge(alert.severity),
      td(alert.accountId),
      td(money.format(alert.amount || 0), 'amount'),
      td((alert.reasons || []).join(' · '), 'reason'),
      td(time.format(new Date(alert.transactionTimestamp)), 'muted-cell'),
      caseAction(alert.alertId),
    );
    return row;
  }));
}

function renderEventRows(events, total) {
  const body = document.querySelector('#transactions-body');
  const summary = document.querySelector('#event-result-count');
  if (!events.length) {
    const row = document.createElement('tr');
    const cell = td('Không có sự kiện phù hợp với bộ lọc', 'empty');
    cell.colSpan = 10;
    row.append(cell);
    body.replaceChildren(row);
    summary.textContent = 'Không tìm thấy kết quả';
    return;
  }

  body.replaceChildren(...events.map(tx => {
    const row = document.createElement('tr');
    if (tx.alertId) {
      row.className = 'event-alert-row';
      row.tabIndex = 0;
      row.setAttribute('role', 'button');
      row.setAttribute('aria-label', `Mở cảnh báo của giao dịch ${tx.transactionId}`);
      row.addEventListener('click', () => openAlert(tx.alertId));
      row.addEventListener('keydown', event => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          openAlert(tx.alertId);
        }
      });
    }
    const idCell = td((tx.transactionId || '').slice(0, 10), 'muted-cell');
    idCell.title = tx.transactionId || '';
    const scenario = td(tx.scenario || 'NORMAL', `scenario ${(tx.scenario || 'NORMAL') === 'NORMAL' ? 'normal' : ''}`);
    const severity = tx.severity ? badge(tx.severity) : td('Không', 'no-alert');
    if (tx.reasons?.length) severity.title = tx.reasons.join(' · ');
    row.append(
      idCell,
      td(tx.accountId),
      td(tx.merchantCategory),
      td(tx.country),
      td(money.format(tx.amount || 0), 'amount'),
      scenario,
      severity,
      td(tx.riskScore || '—', tx.riskScore ? 'risk-score' : 'no-alert'),
      td(time.format(new Date(tx.timestamp)), 'muted-cell'),
      caseAction(tx.alertId),
    );
    return row;
  }));
  summary.textContent = `Hiển thị ${number.format(events.length)} / ${number.format(total)} kết quả · Bấm dòng có cảnh báo để điều tra`;
}

function readEventParams() {
  const [sort, order] = document.querySelector('#event-sort').value.split('-');
  const params = new URLSearchParams({ sort, order, limit: '100' });
  const values = {
    q: document.querySelector('#event-query').value.trim(),
    account: document.querySelector('#event-account').value.trim(),
    severity: document.querySelector('#event-severity').value,
    country: document.querySelector('#event-country').value,
    scenario: document.querySelector('#event-scenario').value,
    minutes: document.querySelector('#event-minutes').value,
  };
  Object.entries(values).forEach(([key, value]) => {
    if (value && !(key === 'minutes' && value === '0')) params.set(key, value);
  });
  return params;
}

async function loadEvents(applyCurrentForm = false) {
  if (eventsLoading) return;
  if (applyCurrentForm || !appliedEventParams) appliedEventParams = readEventParams();
  eventsLoading = true;
  try {
    const data = await fetchJson(`/api/events?${appliedEventParams.toString()}`);
    renderEventRows(data.items, data.total);
  } catch (error) {
    document.querySelector('#event-result-count').textContent = `Không tải được dữ liệu: ${error.message}`;
    showToast(error.message, true);
  } finally {
    eventsLoading = false;
    eventsLastLoaded = Date.now();
  }
}

function riskWindowLabel(seconds) {
  return ({ 0: 'toàn bộ dữ liệu', 60: '60 giây', 300: '5 phút', 900: '15 phút', 3600: '1 giờ', 21600: '6 giờ', 86400: '24 giờ' })[seconds] || 'mốc đã chọn';
}

function renderReasons(reasons, windowSeconds) {
  const list = document.querySelector('#reasons');
  if (!reasons.length) {
    const empty = document.createElement('li');
    empty.textContent = `Không có cảnh báo trong ${riskWindowLabel(windowSeconds)}`;
    list.replaceChildren(empty);
    return;
  }
  list.replaceChildren(...reasons.map(item => {
    const li = document.createElement('li');
    const label = document.createElement('span');
    const count = document.createElement('b');
    label.textContent = item.reason;
    count.textContent = item.count;
    li.append(label, count);
    return li;
  }));
}

function updateBars(severities) {
  const values = {
    critical: severities.CRITICAL || 0,
    high: severities.HIGH || 0,
    medium: severities.MEDIUM || 0,
  };
  const max = Math.max(1, ...Object.values(values));
  Object.entries(values).forEach(([name, value]) => {
    document.querySelector(`#${name}-count`).textContent = number.format(value);
    document.querySelector(`#${name}-bar`).style.width = `${(value / max) * 100}%`;
  });
}

function renderGenerator(status) {
  generatorStatus = status;
  const health = document.querySelector('#generator-health');
  const state = document.querySelector('#generator-state');
  const meta = document.querySelector('#generator-meta');
  const slider = document.querySelector('#tps-range');
  const output = document.querySelector('#tps-value');
  const toggle = document.querySelector('#toggle-generator');
  const controls = document.querySelectorAll('#apply-tps, #toggle-generator, .scenario-button');

  health.classList.toggle('online', status.online && !status.paused);
  health.classList.toggle('paused', status.online && status.paused);
  health.querySelector('b').textContent = !status.online ? 'Generator ngoại tuyến' : status.paused ? 'Đang tạm dừng' : 'Đang phát dữ liệu';
  state.textContent = !status.online ? 'Không có tín hiệu' : status.paused ? 'Đã tạm dừng' : `${decimal.format(status.tps)} giao dịch / giây`;
  const lastScenario = status.lastScenario ? ` · gần nhất ${status.lastScenario}` : '';
  meta.textContent = status.online
    ? `${number.format(status.generatedTotal || 0)} giao dịch trong phiên${lastScenario}`
    : 'Chờ dữ liệu trạng thái từ Kafka';

  if (document.activeElement !== slider && status.online) {
    slider.value = status.tps;
    output.textContent = `${decimal.format(status.tps)} TPS`;
  }
  toggle.textContent = status.paused ? 'Tiếp tục' : 'Tạm dừng';
  controls.forEach(control => { control.disabled = generatorBusy || !status.online; });
}

async function sendGeneratorCommand(payload, successMessage) {
  generatorBusy = true;
  if (generatorStatus) renderGenerator(generatorStatus);
  try {
    await fetchJson('/api/generator/control', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    showToast(successMessage);
  } catch (error) {
    showToast(error.message, true);
  } finally {
    generatorBusy = false;
    if (generatorStatus) renderGenerator(generatorStatus);
  }
}

function setText(selector, value) {
  document.querySelector(selector).textContent = value ?? '—';
}

function renderAlertDetail(data) {
  const alert = data.alert;
  const transaction = data.transaction || {};
  const severity = document.querySelector('#case-severity');
  severity.className = `badge badge-${String(alert.severity).toLowerCase()}`;
  severity.textContent = alert.severity;
  setText('#case-alert-id', alert.alertId);
  setText('#case-score', alert.riskScore);
  setText('#case-account', alert.accountId);
  setText('#case-amount', money.format(alert.amount || 0));
  setText('#case-country', alert.country);
  setText('#case-device', alert.deviceId);
  setText('#case-category', transaction.merchantCategory || alert.merchantId);
  setText('#case-time', dateTime.format(new Date(alert.transactionTimestamp)));

  const reasons = (alert.reasons || []).map(reason => {
    const item = document.createElement('li');
    const label = document.createElement('span');
    const score = document.createElement('b');
    label.textContent = reason;
    score.textContent = reasonScores[reason] ? `+${reasonScores[reason]}` : 'Kích hoạt';
    item.append(label, score);
    return item;
  });
  document.querySelector('#case-reasons').replaceChildren(...reasons);

  const timeline = data.timeline.map(tx => {
    const item = document.createElement('li');
    if (tx.transactionId === alert.transactionId) item.className = 'flagged';
    const title = document.createElement('strong');
    const timestamp = document.createElement('time');
    const detail = document.createElement('small');
    title.textContent = `${money.format(tx.amount || 0)} · ${tx.merchantCategory || tx.merchantId}`;
    timestamp.textContent = time.format(new Date(tx.timestamp));
    detail.textContent = `${tx.country || '—'} · ${tx.deviceId || '—'} · ${tx.scenario || 'NORMAL'}`;
    item.append(title, timestamp, detail);
    return item;
  });
  document.querySelector('#case-timeline').replaceChildren(...timeline);

  document.querySelector('#case-status').value = data.case.status;
  document.querySelector('#case-assignee').value = data.case.assignee;
  document.querySelector('#case-notes').value = data.case.notes;
}

async function openAlert(alertId) {
  if (!alertId) return;
  currentAlertId = alertId;
  const layer = document.querySelector('#case-layer');
  const loading = document.querySelector('#case-loading');
  const content = document.querySelector('#case-content');
  layer.classList.add('open');
  layer.setAttribute('aria-hidden', 'false');
  document.body.classList.add('drawer-open');
  loading.hidden = false;
  loading.textContent = 'Đang tải dữ liệu điều tra…';
  content.hidden = true;
  document.querySelector('#close-case').focus();
  try {
    const data = await fetchJson(`/api/alerts/${encodeURIComponent(alertId)}`);
    if (currentAlertId !== alertId) return;
    renderAlertDetail(data);
    loading.hidden = true;
    content.hidden = false;
  } catch (error) {
    loading.textContent = `Không tải được hồ sơ: ${error.message}`;
    showToast(error.message, true);
  }
}

function closeAlert() {
  currentAlertId = null;
  const layer = document.querySelector('#case-layer');
  layer.classList.remove('open');
  layer.setAttribute('aria-hidden', 'true');
  document.body.classList.remove('drawer-open');
}

async function refresh() {
  const connection = document.querySelector('#connection');
  try {
    const data = await fetchJson(`/api/snapshot?riskWindowSeconds=${riskWindowSeconds}`);
    const stats = data.stats;

    document.querySelector('#total-transactions').textContent = number.format(stats.totalTransactions);
    document.querySelector('#total-alerts').textContent = number.format(stats.totalAlerts);
    document.querySelector('#detection-rate').textContent = `${stats.detectionRate}%`;
    document.querySelector('#injected').textContent = number.format(stats.injectedTransactions);
    document.querySelector('#late-transactions').textContent = number.format(stats.lateTransactions || 0);
    const latestLate = data.lateTransactions?.[0];
    document.querySelector('#latest-late-event').textContent = latestLate
      ? `${latestLate.accountId} · trễ ${decimal.format((latestLate.latenessMillis || 0) / 1000)} giây`
      : 'Chưa ghi nhận sự kiện muộn';
    document.querySelector('#tpm').textContent = number.format(stats.transactionsPerMinute);
    const connectionText = data.connected ? 'Kafka trực tuyến' : 'Đang kết nối Kafka';
    connection.classList.toggle('online', data.connected);
    connection.lastChild.textContent = ` ${connectionText}`;
    connection.setAttribute('aria-label', connectionText);
    connection.title = connectionText;

    renderAlerts(data.alerts);
    riskWindowSeconds = data.riskWindowSeconds;
    document.querySelector('#risk-window').value = String(riskWindowSeconds);
    renderReasons(data.topReasons, riskWindowSeconds);
    updateBars(data.severities);
    renderGenerator(data.generator);
    if (Date.now() - eventsLastLoaded >= 5000) loadEvents();
  } catch (error) {
    const connectionText = 'Mất kết nối dashboard';
    connection.classList.remove('online');
    connection.lastChild.textContent = ` ${connectionText}`;
    connection.setAttribute('aria-label', connectionText);
    connection.title = connectionText;
    console.error(error);
  } finally {
    window.setTimeout(refresh, 1000);
  }
}

document.querySelector('#tps-range').addEventListener('input', event => {
  document.querySelector('#tps-value').textContent = `${decimal.format(Number(event.target.value))} TPS`;
});

document.querySelector('#risk-window').value = String(riskWindowSeconds);
document.querySelector('#risk-window').addEventListener('change', event => {
  riskWindowSeconds = Number(event.target.value);
  window.localStorage.setItem('riskWindowSeconds', String(riskWindowSeconds));
});

document.querySelector('#apply-tps').addEventListener('click', () => {
  const tps = Number(document.querySelector('#tps-range').value);
  sendGeneratorCommand({ command: 'configure', tps }, `Đã gửi yêu cầu đổi tốc độ thành ${decimal.format(tps)} TPS`);
});

document.querySelector('#toggle-generator').addEventListener('click', () => {
  const paused = !generatorStatus?.paused;
  sendGeneratorCommand({ command: 'configure', paused }, paused ? 'Đã gửi yêu cầu tạm dừng' : 'Đã gửi yêu cầu tiếp tục');
});

document.querySelectorAll('.scenario-button').forEach(button => {
  button.addEventListener('click', () => {
    sendGeneratorCommand(
      { command: 'inject', scenario: button.dataset.scenario },
      `Đã yêu cầu phát kịch bản ${button.dataset.scenario}`,
    );
  });
});

document.querySelector('#event-filter-form').addEventListener('submit', event => {
  event.preventDefault();
  loadEvents(true);
});

document.querySelector('#reset-event-filters').addEventListener('click', () => {
  document.querySelector('#event-filter-form').reset();
  loadEvents(true);
});

document.querySelector('#critical-quick-filter').addEventListener('click', () => {
  document.querySelector('#event-query').value = '';
  document.querySelector('#event-account').value = '';
  document.querySelector('#event-country').value = '';
  document.querySelector('#event-scenario').value = '';
  document.querySelector('#event-severity').value = 'CRITICAL';
  document.querySelector('#event-minutes').value = '15';
  document.querySelector('#event-sort').value = 'risk-desc';
  loadEvents(true);
});

document.querySelectorAll('.export-button').forEach(button => {
  button.addEventListener('click', () => {
    const params = new URLSearchParams(appliedEventParams || readEventParams());
    params.delete('limit');
    params.set('format', button.dataset.format);
    const link = document.createElement('a');
    link.href = `/api/events/export?${params.toString()}`;
    link.click();
    showToast(`Đang xuất ${button.dataset.format.toUpperCase()} theo bộ lọc hiện tại`);
  });
});

document.querySelector('#close-case').addEventListener('click', closeAlert);
document.querySelector('#case-scrim').addEventListener('click', closeAlert);
document.addEventListener('keydown', event => {
  if (event.key === 'Escape' && currentAlertId) closeAlert();
});

document.querySelector('#case-form').addEventListener('submit', async event => {
  event.preventDefault();
  if (!currentAlertId) return;
  const saveButton = document.querySelector('#save-case');
  saveButton.disabled = true;
  try {
    await fetchJson(`/api/alerts/${encodeURIComponent(currentAlertId)}/case`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        status: document.querySelector('#case-status').value,
        assignee: document.querySelector('#case-assignee').value,
        notes: document.querySelector('#case-notes').value,
      }),
    });
    showToast('Đã lưu hồ sơ điều tra');
  } catch (error) {
    showToast(error.message, true);
  } finally {
    saveButton.disabled = false;
  }
});

refresh();
