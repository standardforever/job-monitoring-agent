const API_BASE = String(window.__FASTAPI_API_BASE__ || "/api/").replace(/\/?$/, "/");
const state = {
  adminPassword: sessionStorage.getItem("adminPassword") || "",
  activeClientName: sessionStorage.getItem("activeClientName") || "",
  processesPage: 1,
  pageSize: 10,
  processesTimer: null,
  editingClientName: "",
};

function buildApiUrl(path) {
  const normalizedBase = String(API_BASE || "/api/").replace(/\/+$/, "");
  const normalizedPath = String(path || "").replace(/^\/+/, "");
  return `${normalizedBase}/${normalizedPath}`;
}

function byId(id) {
  return document.getElementById(id);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function showAlert(message, level = "info") {
  const alerts = byId("alerts");
  const alert = document.createElement("div");
  alert.className = `alert ${level}`;
  alert.textContent = message;
  alerts.prepend(alert);
  window.setTimeout(() => alert.remove(), 5000);
}

async function apiFetch(path, options = {}) {
  const response = await fetch(buildApiUrl(path), options);
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : await response.text();
  if (!response.ok) {
    const detail = typeof payload === "object" && payload ? payload.detail : payload;
    throw new Error(detail || "Request failed");
  }
  return payload;
}

function togglePanel(panelId) {
  byId("admin-panel").classList.toggle("hidden", panelId !== "admin-panel");
  byId("client-panel").classList.toggle("hidden", panelId !== "client-panel");
}

function setAdminPassword(value) {
  state.adminPassword = value.trim();
  sessionStorage.setItem("adminPassword", state.adminPassword);
}

function clearAdminPassword() {
  state.adminPassword = "";
  sessionStorage.removeItem("adminPassword");
}

function setAdminAccess(isUnlocked) {
  byId("admin-auth-gate").classList.toggle("hidden", isUnlocked);
  byId("admin-dashboard").classList.toggle("hidden", !isUnlocked);
}

function clearClientForm() {
  state.editingClientName = "";
  byId("client-form-title").textContent = "Create Client";
  byId("client-form-submit").textContent = "Create Client";
  byId("client-form-reset").classList.add("hidden");
  byId("editing-client-name").value = "";
  byId("client-name").value = "";
  byId("client-api-key").value = "";
  byId("client-api-key").required = true;
  byId("client-model").value = "gpt-5-nano";
  byId("client-grid-url").value = "";
}

function setClientEditMode(client) {
  state.editingClientName = client.client_name;
  byId("client-form-title").textContent = `Update ${client.client_name}`;
  byId("client-form-submit").textContent = "Update Client";
  byId("client-form-reset").classList.remove("hidden");
  byId("editing-client-name").value = client.client_name;
  byId("client-name").value = client.client_name || "";
  byId("client-api-key").value = "";
  byId("client-api-key").required = false;
  byId("client-api-key").placeholder = "Leave blank to keep current key";
  byId("client-model").value = client.model || "gpt-5-nano";
  byId("client-grid-url").value = client.grid_url || "";
}

async function loadClients() {
  if (!state.adminPassword) {
    setAdminAccess(false);
    return;
  }
  const payload = await apiFetch("/clients", {
    headers: { "x-registration-password": state.adminPassword },
  });
  setAdminAccess(true);
  const clients = payload.clients || [];
  const list = byId("clients-list");
  const empty = byId("clients-empty");
  empty.classList.toggle("hidden", clients.length > 0);
  list.innerHTML = clients
    .map(
      (client) => `
        <article class="client-row">
          <div class="client-row-header">
            <div>
              <h3>${escapeHtml(client.client_name)}</h3>
              <p class="muted">Model: ${escapeHtml(client.model || "gpt-5-nano")} | Grid: ${escapeHtml(client.grid_url || "default")}</p>
            </div>
            <span class="status-pill ${escapeHtml(client.api_key_status || "queued")}">${escapeHtml(client.api_key_status || "unknown")}</span>
          </div>
          <p class="muted">API Key: ${escapeHtml(client.api_key || "")}</p>
          <p class="muted">Updated: ${escapeHtml(client.updated_at || "")}</p>
          <div class="inline-actions">
            <button class="button button-secondary client-edit-button" type="button" data-client='${escapeHtml(JSON.stringify(client))}'>Update</button>
          </div>
        </article>
      `
    )
    .join("");

  for (const button of list.querySelectorAll(".client-edit-button")) {
    button.addEventListener("click", () => {
      const client = JSON.parse(button.dataset.client || "{}");
      setClientEditMode(client);
      byId("client-name").focus();
    });
  }
}

async function submitClientForm(event) {
  event.preventDefault();
  if (!state.adminPassword) {
    showAlert("Enter the registration password first.", "error");
    return;
  }

  const payload = {
    client_name: byId("client-name").value.trim(),
    model: byId("client-model").value.trim() || "gpt-5-nano",
  };
  const apiKey = byId("client-api-key").value.trim();
  const gridUrl = byId("client-grid-url").value.trim();
  if (apiKey) {
    payload.api_key = apiKey;
  }
  if (gridUrl) {
    payload.grid_url = gridUrl;
  }

  const isEdit = Boolean(state.editingClientName);
  if (!isEdit && !payload.api_key) {
    showAlert("API key is required when creating a client.", "error");
    return;
  }

  const path = isEdit
    ? `/clients/${encodeURIComponent(state.editingClientName)}/config`
    : "/clients";
  const method = isEdit ? "PATCH" : "POST";

  const response = await apiFetch(path, {
    method,
    headers: {
      "Content-Type": "application/json",
      "x-registration-password": state.adminPassword,
    },
    body: JSON.stringify(payload),
  });

  showAlert(response.message || response.status || "Client saved.", "success");
  clearClientForm();
  await loadClients();
}

async function unlockAdmin(event) {
  event.preventDefault();
  const password = byId("admin-password").value;
  setAdminPassword(password);
  try {
    await loadClients();
  } catch (error) {
    clearAdminPassword();
    setAdminAccess(false);
    throw error;
  }
  showAlert("Admin dashboard unlocked.", "success");
}

function lockAdmin() {
  clearAdminPassword();
  clearClientForm();
  byId("admin-password").value = "";
  setAdminAccess(false);
  showAlert("Admin dashboard locked.", "info");
}

function setActiveClient(name) {
  state.activeClientName = name.trim();
  state.processesPage = 1;
  sessionStorage.setItem("activeClientName", state.activeClientName);
  byId("dashboard-client-name").value = state.activeClientName;
}

function renderProcesses(payload) {
  const processes = payload.processes || [];
  const list = byId("processes-list");
  const empty = byId("processes-empty");
  const pageIndicator = byId("page-indicator");
  const prevButton = byId("previous-page");
  const nextButton = byId("next-page");

  empty.classList.toggle("hidden", processes.length > 0);
  pageIndicator.textContent = `Page ${payload.page || 1}`;
  prevButton.disabled = !payload.has_previous;
  nextButton.disabled = !payload.has_next;

  list.innerHTML = processes
    .map((process) => {
      const summary = process.summary || {};
      const metadata = process.metadata || {};
      const processId = encodeURIComponent(process.process_id);
      const showJobDownloads = Boolean(metadata.job_extract || metadata.requested_capability?.includes("job"));
      const rerunButton = !["queued", "running", "stop_requested"].includes(process.status)
        ? `<button class="button button-secondary rerun-process-button" type="button" data-process-id="${escapeHtml(process.process_id)}">Rerun</button>`
        : "";
      const stopButton = ["queued", "running", "stop_requested"].includes(process.status)
        ? `<button class="button button-danger stop-process-button" type="button" data-process-id="${escapeHtml(process.process_id)}">${process.status === "stop_requested" ? "Stopping..." : "Stop Process"}</button>`
        : "";

      return `
        <article class="process-card">
          <div class="process-meta">
            <div>
              <h3>${escapeHtml(process.process_id)}</h3>
              <p class="muted">Created: ${escapeHtml(process.created_at || "")}</p>
              <p class="muted">Updated: ${escapeHtml(process.updated_at || "")}</p>
            </div>
            <span class="status-pill ${escapeHtml(process.status)}">${escapeHtml(process.status)}</span>
          </div>
          <div class="summary-grid">
            <div class="summary-item"><span>Total URLs</span><strong>${escapeHtml(summary.total_urls ?? 0)}</strong></div>
            <div class="summary-item"><span>Processed</span><strong>${escapeHtml(summary.processed_url_count ?? 0)}</strong></div>
            <div class="summary-item"><span>Completed</span><strong>${escapeHtml(summary.completed_domain_count ?? 0)}</strong></div>
            <div class="summary-item"><span>Failed</span><strong>${escapeHtml(summary.failed_domain_count ?? 0)}</strong></div>
            <div class="summary-item"><span>Running</span><strong>${escapeHtml(summary.running_url_count ?? 0)}</strong></div>
            <div class="summary-item"><span>Stopped</span><strong>${escapeHtml(summary.stopped_url_count ?? 0)}</strong></div>
          </div>
          <p class="muted">Capability: ${escapeHtml(metadata.requested_capability || "career_page")} | ATS: ${escapeHtml(metadata.ats_check)} | Jobs: ${escapeHtml(metadata.job_extract)}</p>
          <div class="process-actions">
            <a class="button button-secondary" href="${buildApiUrl(`processes/${processId}/important`)}" download>Career/ATS JSON</a>
            <a class="button button-secondary" href="${buildApiUrl(`processes/${processId}/important.csv`)}" download>Career/ATS CSV</a>
            ${showJobDownloads ? `<a class="button button-secondary" href="${buildApiUrl(`processes/${processId}/jobs.json`)}" download>Jobs JSON</a>` : ""}
            ${showJobDownloads ? `<a class="button button-secondary" href="${buildApiUrl(`processes/${processId}/jobs.csv`)}" download>Jobs CSV</a>` : ""}
            ${rerunButton}
            ${stopButton}
          </div>
        </article>
      `;
    })
    .join("");

  for (const button of list.querySelectorAll(".stop-process-button")) {
    button.addEventListener("click", async () => {
      button.disabled = true;
      try {
        const response = await apiFetch(`/processes/${encodeURIComponent(button.dataset.processId)}/stop`, {
          method: "POST",
        });
        showAlert(response.message || "Stop requested.", "info");
        await loadProcesses();
      } catch (error) {
        button.disabled = false;
        showAlert(error.message, "error");
      }
    });
  }

  for (const button of list.querySelectorAll(".rerun-process-button")) {
    button.addEventListener("click", async () => {
      button.disabled = true;
      try {
        const response = await apiFetch(`/processes/${encodeURIComponent(button.dataset.processId)}/rerun`, {
          method: "POST",
        });
        showAlert(`Rerun started: ${response.process_id}`, "success");
        await loadProcesses();
      } catch (error) {
        button.disabled = false;
        showAlert(error.message, "error");
      }
    });
  }
}

async function loadProcesses() {
  if (!state.activeClientName) {
    return;
  }
  const clientName = encodeURIComponent(state.activeClientName);
  const payload = await apiFetch(`/processes?client_name=${clientName}&page=${state.processesPage}&page_size=${state.pageSize}`);
  renderProcesses(payload);
}

function startProcessesRefresh() {
  if (state.processesTimer) {
    window.clearInterval(state.processesTimer);
  }
  // state.processesTimer = window.setInterval(() => {
  //   if (!byId("client-dashboard").classList.contains("hidden")) {
  //     loadProcesses().catch((error) => showAlert(error.message, "error"));
  //   }
  // }, 15000);
}

async function openClientDashboard(event) {
  event.preventDefault();
  const clientName = byId("dashboard-client-name").value.trim();
  if (!clientName) {
    showAlert("Enter a client name first.", "error");
    return;
  }

  const client = await apiFetch(`/clients/${encodeURIComponent(clientName)}/config`);
  setActiveClient(client.client_name || clientName);
  byId("active-client-name").textContent = `${state.activeClientName} Dashboard`;
  byId("active-client-meta").textContent = `Model: ${client.model || "gpt-5-nano"} | Grid: ${client.grid_url || "default"}`;
  byId("client-entry-card").classList.add("hidden");
  byId("client-dashboard").classList.remove("hidden");
  await loadProcesses();
  startProcessesRefresh();
  showAlert("Client dashboard ready.", "success");
}

function collectManualUrls() {
  return byId("manual-urls")
    .value
    .split(/[\n,]+/)
    .map((value) => value.trim())
    .filter(Boolean);
}

async function submitManualProcess(event) {
  event.preventDefault();
  const urls = collectManualUrls();
  if (!urls.length) {
    showAlert("Add at least one URL before starting a process.", "error");
    return;
  }

  const payload = {
    client_name: state.activeClientName,
    urls,
    agent_count: Number(byId("manual-agent-count").value || 1),
    ats_check: byId("manual-ats-check").checked,
    job_extract: byId("manual-job-extract").checked,
    job_monitoring: byId("manual-job-monitoring").checked,
  };

  const response = await apiFetch("/processes", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  showAlert(`Process ${response.process_id} started.`, "success");
  byId("manual-urls").value = "";
  await loadProcesses();
}

async function submitUploadProcess(event) {
  event.preventDefault();
  const fileInput = byId("upload-file");
  if (!fileInput.files.length) {
    showAlert("Choose a CSV or XLSX file first.", "error");
    return;
  }
  const formData = new FormData();
  formData.append("file", fileInput.files[0]);
  formData.append("client_name", state.activeClientName);
  formData.append("agent_count", String(Number(byId("upload-agent-count").value || 1)));
  formData.append("ats_check", String(byId("upload-ats-check").checked));
  formData.append("job_extract", String(byId("upload-job-extract").checked));
  formData.append("job_monitoring", String(byId("upload-job-monitoring").checked));

  const response = await apiFetch("/processes/upload", {
    method: "POST",
    body: formData,
  });
  showAlert(`Process ${response.process_id} started from upload.`, "success");
  fileInput.value = "";
  await loadProcesses();
}

function bindEvents() {
  byId("show-admin").addEventListener("click", () => togglePanel("admin-panel"));
  byId("show-client").addEventListener("click", () => togglePanel("client-panel"));
  byId("admin-auth-form").addEventListener("submit", (event) => {
    unlockAdmin(event).catch((error) => showAlert(error.message, "error"));
  });
  byId("client-form").addEventListener("submit", (event) => {
    submitClientForm(event).catch((error) => showAlert(error.message, "error"));
  });
  byId("client-form-reset").addEventListener("click", clearClientForm);
  byId("admin-logout").addEventListener("click", lockAdmin);
  byId("refresh-clients").addEventListener("click", () => {
    loadClients().catch((error) => showAlert(error.message, "error"));
  });
  byId("client-access-form").addEventListener("submit", (event) => {
    openClientDashboard(event).catch((error) => showAlert(error.message, "error"));
  });
  byId("change-client").addEventListener("click", () => {
    byId("client-dashboard").classList.add("hidden");
    byId("client-entry-card").classList.remove("hidden");
    if (state.processesTimer) {
      window.clearInterval(state.processesTimer);
      state.processesTimer = null;
    }
  });
  byId("manual-process-form").addEventListener("submit", (event) => {
    submitManualProcess(event).catch((error) => showAlert(error.message, "error"));
  });
  byId("upload-process-form").addEventListener("submit", (event) => {
    submitUploadProcess(event).catch((error) => showAlert(error.message, "error"));
  });
  byId("refresh-processes").addEventListener("click", () => {
    loadProcesses().catch((error) => showAlert(error.message, "error"));
  });
  byId("previous-page").addEventListener("click", () => {
    if (state.processesPage > 1) {
      state.processesPage -= 1;
      loadProcesses().catch((error) => showAlert(error.message, "error"));
    }
  });
  byId("next-page").addEventListener("click", () => {
    state.processesPage += 1;
    loadProcesses().catch((error) => showAlert(error.message, "error"));
  });
}

async function bootstrap() {
  bindEvents();
  togglePanel("client-panel");
  setAdminAccess(false);

  if (state.adminPassword) {
    byId("admin-password").value = state.adminPassword;
    loadClients().catch((error) => {
      clearAdminPassword();
      setAdminAccess(false);
      showAlert(error.message, "error");
    });
  }

  if (state.activeClientName) {
    byId("dashboard-client-name").value = state.activeClientName;
  }
}

bootstrap().catch((error) => showAlert(error.message, "error"));
