const API_BASE = String(window.__FASTAPI_API_BASE__ || "/api/").replace(/\/?$/, "/");
const state = {
  processesPage: 1,
  pageSize: 10,
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

function collectManualUrls() {
  return byId("manual-urls")
    .value
    .split(/[\n,]+/)
    .map((value) => value.trim())
    .filter(Boolean);
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
  const payload = await apiFetch(`/processes?page=${state.processesPage}&page_size=${state.pageSize}`);
  renderProcesses(payload);
}

async function submitManualProcess(event) {
  event.preventDefault();
  const urls = collectManualUrls();
  if (!urls.length) {
    showAlert("Add at least one URL before starting a process.", "error");
    return;
  }

  const payload = {
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
  const file = fileInput.files?.[0];
  if (!file) {
    showAlert("Choose a CSV or XLSX file first.", "error");
    return;
  }

  const formData = new FormData();
  formData.append("file", file);
  formData.append("agent_count", String(Number(byId("upload-agent-count").value || 1)));
  formData.append("ats_check", String(byId("upload-ats-check").checked));
  formData.append("job_extract", String(byId("upload-job-extract").checked));
  formData.append("job_monitoring", String(byId("upload-job-monitoring").checked));

  const response = await apiFetch("/processes/upload", {
    method: "POST",
    body: formData,
  });
  showAlert(`Upload accepted. Process ${response.process_id} started.`, "success");
  fileInput.value = "";
  await loadProcesses();
}

function bindEvents() {
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
    loadProcesses().catch((error) => {
      state.processesPage -= 1;
      showAlert(error.message, "error");
    });
  });
}

async function init() {
  bindEvents();
  await loadProcesses();
}

init().catch((error) => showAlert(error.message, "error"));
