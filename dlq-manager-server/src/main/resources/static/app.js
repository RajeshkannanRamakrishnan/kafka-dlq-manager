const state = {
  topics: [],
  currentTopic: null,
  messages: [],
  selected: new Set(), // "partition:offset"
  activeTab: "messages",
};

const el = (id) => document.getElementById(id);

async function api(path, options) {
  const res = await fetch(path, options);
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = await res.json();
      detail = body.detail || detail;
    } catch (ignored) {
      // non-JSON error body, keep statusText
    }
    throw new Error(detail);
  }
  if (res.status === 204) return null;
  return res.json();
}

function showToast(message, isError) {
  const toast = el("toast");
  toast.textContent = message;
  toast.classList.toggle("error", !!isError);
  toast.hidden = false;
  clearTimeout(showToast._timer);
  showToast._timer = setTimeout(() => { toast.hidden = true; }, 5000);
}

function setStatus(message, isError) {
  const status = el("status");
  status.textContent = message;
  status.classList.toggle("error", !!isError);
}

function formatTime(epochMillis) {
  return new Date(epochMillis).toLocaleString();
}

function coordKey(partition, offset) {
  return partition + ":" + offset;
}

// ---- Topics ----

async function loadTopics() {
  try {
    state.topics = await api("/api/v1/topics");
    setStatus(state.topics.length + " DLQ topic(s) discovered");
    renderTopics();
  } catch (e) {
    setStatus("Failed to load topics: " + e.message, true);
  }
}

function renderTopics() {
  const list = el("topics-list");
  list.innerHTML = "";
  state.topics.forEach((t) => {
    const li = document.createElement("li");
    li.className = t.name === state.currentTopic ? "active" : "";
    li.innerHTML = `<div class="topic-name">${escapeHtml(t.name)}</div>` +
      `<div class="topic-meta">${t.partitions} partitions · depth ${t.depth}</div>`;
    li.addEventListener("click", () => selectTopic(t.name));
    list.appendChild(li);
  });
}

function selectTopic(topic) {
  state.currentTopic = topic;
  state.selected.clear();
  el("empty-state").hidden = true;
  el("topic-view").hidden = false;
  el("current-topic-name").textContent = topic;
  renderTopics();
  loadMessages();
  loadFailures();
}

// ---- Messages ----

async function loadMessages() {
  if (!state.currentTopic) return;
  const limit = el("messages-limit").value || 50;
  try {
    state.messages = await api(
      `/api/v1/topics/${encodeURIComponent(state.currentTopic)}/messages?limit=${limit}`);
    renderMessages();
  } catch (e) {
    showToast("Failed to load messages: " + e.message, true);
  }
}

function renderMessages() {
  const tbody = el("messages-tbody");
  tbody.innerHTML = "";
  el("messages-empty").hidden = state.messages.length !== 0;

  state.messages.forEach((m, idx) => {
    const key = coordKey(m.partition, m.offset);
    const d = m.diagnostics;
    const exceptionOrigin = d
      ? `${d.exceptionClass || "(unknown)"} @ ${d.originalTopic || "(unknown)"}`
      : "(no diagnostics)";

    const row = document.createElement("tr");
    row.innerHTML = `
      <td><input type="checkbox" class="row-select" data-key="${key}"></td>
      <td>${m.partition}</td>
      <td>${m.offset}</td>
      <td>${formatTime(m.timestamp)}</td>
      <td class="wrap-cell mono" title="${escapeHtml(exceptionOrigin)}">${escapeHtml(exceptionOrigin)}</td>
      <td>${d && d.deliveryAttempts != null ? d.deliveryAttempts : "-"}</td>
      <td class="mono truncated" title="${escapeHtml(m.key || "")}">${escapeHtml(m.key || "")}</td>
      <td class="mono truncated" title="${escapeHtml(m.value || "")}">${escapeHtml(m.value || "")}</td>
      <td><button class="link-btn" data-toggle="${idx}">Details</button></td>
    `;
    tbody.appendChild(row);

    const detailRow = document.createElement("tr");
    detailRow.className = "detail-row";
    detailRow.hidden = true;
    const cell = document.createElement("td");
    cell.colSpan = 9;
    cell.textContent = JSON.stringify({ headers: m.headers, diagnostics: d, value: m.value }, null, 2);
    detailRow.appendChild(cell);
    tbody.appendChild(detailRow);

    row.querySelector(`[data-toggle="${idx}"]`).addEventListener("click", () => {
      detailRow.hidden = !detailRow.hidden;
    });
    row.querySelector(".row-select").addEventListener("change", (e) => {
      if (e.target.checked) state.selected.add(key);
      else state.selected.delete(key);
      updateSelectionUI();
    });
  });

  updateSelectionUI();
}

function updateSelectionUI() {
  el("selected-count").textContent = state.selected.size;
  el("replay-selected").disabled = state.selected.size === 0;
  el("select-all").checked = state.selected.size > 0 && state.selected.size === state.messages.length;
}

// ---- Failures ----

async function loadFailures() {
  if (!state.currentTopic) return;
  const sampleSize = el("failures-sample-size").value || 500;
  try {
    const failures = await api(
      `/api/v1/topics/${encodeURIComponent(state.currentTopic)}/failures?sampleSize=${sampleSize}`);
    renderFailures(failures);
  } catch (e) {
    showToast("Failed to load failures: " + e.message, true);
  }
}

function renderFailures(failures) {
  const tbody = el("failures-tbody");
  tbody.innerHTML = "";
  const entries = Object.entries(failures);
  el("failures-empty").hidden = entries.length !== 0;

  entries.forEach(([group, count]) => {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${escapeHtml(group)}</td>
      <td>${count}</td>
      <td><button class="link-btn" data-group="${escapeHtml(group)}">Show in Messages</button></td>
    `;
    row.querySelector("[data-group]").addEventListener("click", () => {
      switchTab("messages");
      filterMessagesByGroup(group);
    });
    tbody.appendChild(row);
  });
}

function filterMessagesByGroup(group) {
  const matchingKeys = new Set(
    state.messages
      .filter((m) => {
        const d = m.diagnostics;
        const label = d ? `${d.exceptionClass || "(unknown)"} @ ${d.originalTopic || "(unknown)"}` : null;
        return label === group;
      })
      .map((m) => coordKey(m.partition, m.offset)));

  document.querySelectorAll("#messages-tbody .row-select").forEach((checkbox) => {
    const row = checkbox.closest("tr");
    row.style.display = matchingKeys.has(checkbox.dataset.key) ? "" : "none";
    const detailRow = row.nextElementSibling;
    if (detailRow && detailRow.classList.contains("detail-row") && !matchingKeys.has(checkbox.dataset.key)) {
      detailRow.hidden = true;
    }
  });
}

function switchTab(tab) {
  state.activeTab = tab;
  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.tab === tab);
  });
  el("messages-tab").hidden = tab !== "messages";
  el("failures-tab").hidden = tab !== "failures";
}

// ---- Replay ----

let lastDryRunPassed = false;

function openReplayModal() {
  lastDryRunPassed = false;
  el("replay-modal-count").textContent = state.selected.size;
  el("replay-target-topic").value = "";
  el("replay-modal-body").innerHTML = "";
  el("replay-dry-run-btn").hidden = false;
  el("replay-confirm-btn").hidden = true;
  el("replay-modal").hidden = false;
}

function closeReplayModal() {
  el("replay-modal").hidden = true;
}

function selectedCoordinates() {
  return [...state.selected].map((key) => {
    const [partition, offset] = key.split(":");
    return { partition: Number(partition), offset: Number(offset) };
  });
}

async function runReplay(dryRun) {
  const targetTopic = el("replay-target-topic").value.trim() || null;
  const body = {
    dlqTopic: state.currentTopic,
    messages: selectedCoordinates(),
    targetTopic,
    dryRun,
  };
  try {
    const result = await api("/api/v1/replay", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    renderReplayResult(result);
    if (dryRun) {
      lastDryRunPassed = result.failed === 0 && result.succeeded > 0;
      el("replay-dry-run-btn").hidden = true;
      el("replay-confirm-btn").hidden = !lastDryRunPassed;
    } else {
      el("replay-confirm-btn").hidden = true;
      showToast(`Replay complete: ${result.succeeded} succeeded, ${result.failed} failed, ${result.skipped} skipped`,
        result.failed > 0);
      state.selected.clear();
      loadMessages();
    }
  } catch (e) {
    showToast("Replay failed: " + e.message, true);
  }
}

function renderReplayResult(result) {
  const items = result.items.map((item) => `
    <li>
      <span class="mono">[${item.partition}]@${item.offset}</span>
      <span class="badge ${item.status.toLowerCase()}">${item.status.replace("_", " ")}</span>
      <span>${escapeHtml(item.targetTopic || item.detail || "")}</span>
    </li>`).join("");

  el("replay-modal-body").innerHTML = `
    <div class="summary">${result.dryRun ? "Dry run" : "Executed"} —
      ${result.succeeded} would-succeed/succeeded, ${result.failed} failed, ${result.skipped} skipped</div>
    <ul>${items}</ul>
  `;
}

function escapeHtml(value) {
  const div = document.createElement("div");
  div.textContent = value == null ? "" : String(value);
  return div.innerHTML;
}

// ---- Wiring ----

document.addEventListener("DOMContentLoaded", () => {
  el("refresh-topics").addEventListener("click", loadTopics);
  el("refresh-messages").addEventListener("click", loadMessages);
  el("refresh-failures").addEventListener("click", loadFailures);

  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.addEventListener("click", () => switchTab(btn.dataset.tab));
  });

  el("select-all").addEventListener("change", (e) => {
    state.selected.clear();
    if (e.target.checked) {
      state.messages.forEach((m) => state.selected.add(coordKey(m.partition, m.offset)));
    }
    document.querySelectorAll(".row-select").forEach((cb) => { cb.checked = e.target.checked; });
    updateSelectionUI();
  });

  el("replay-selected").addEventListener("click", openReplayModal);
  el("replay-cancel-btn").addEventListener("click", closeReplayModal);
  el("replay-dry-run-btn").addEventListener("click", () => runReplay(true));
  el("replay-confirm-btn").addEventListener("click", () => runReplay(false));

  loadTopics();
});
