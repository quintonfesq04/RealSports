(() => {
  const status = document.getElementById("status-banner");
  const JOB_KEYS = ["schedule_fetch", "injuries", "cbb_scraper", "picks_refresh"];
  const pipelineStatus = document.getElementById("pipeline-status");
  const pipelineLogNode = document.getElementById("pipeline-log");
  const jobLatestList = document.querySelector("[data-job-latest]");
  let pipelinePoll = null;

  const escape = (str = "") =>
    str.replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch] || ch));

  const showStatus = (msg, type = "info") => {
    if (!status) return;
    if (!msg) {
      status.hidden = true;
      status.textContent = "";
      return;
    }
    status.hidden = false;
    status.textContent = msg;
    status.dataset.type = type;
  };

  const refreshJobSummaries = (jobs) => {
    if (!jobLatestList || !jobs) return;
    Object.entries(jobs).forEach(([name, info]) => {
      if (name === "pipeline" || name === "pipeline_log" || name === "job_runtime") return;
      const ran = document.querySelector(`[data-job-latest-ran="${name}"]`);
      const exit = document.querySelector(`[data-job-latest-exit="${name}"]`);
      if (ran) {
        ran.textContent = (info && info.ran_at) || "never";
      }
      if (exit) {
        exit.textContent = (info && info.exit_code !== undefined && info.exit_code !== null) ? info.exit_code : "–";
      }
    });
  };

  const renderPipelineLog = (log = []) => {
    if (!pipelineLogNode) return;
    if (!log.length) {
      pipelineLogNode.hidden = true;
      pipelineLogNode.innerHTML = "";
      return;
    }
    pipelineLogNode.hidden = false;
    pipelineLogNode.innerHTML = log
      .slice(0, 6)
      .map((entry) => `<li><span>${escape(entry.timestamp || "")}</span> — ${escape(entry.message || "")}</li>`)
      .join("");
  };

  const updateJobRuntime = (runtime = {}) => {
    JOB_KEYS.forEach((name) => {
      const state = runtime[name] || {};
      const statusNode = document.querySelector(`[data-job-status="${name}"]`);
      const logNode = document.querySelector(`[data-job-log="${name}"]`);
      if (statusNode) {
        if (state.running) {
          statusNode.hidden = false;
          statusNode.textContent = state.last_message || "Running…";
          statusNode.dataset.type = "info";
        } else if (state.last_error) {
          statusNode.hidden = false;
          statusNode.textContent = `Error: ${state.last_error}`;
          statusNode.dataset.type = "error";
        } else {
          statusNode.hidden = true;
        }
      }
      if (logNode) {
        const log = state.log || [];
        if (state.running && log.length) {
          logNode.hidden = false;
          logNode.innerHTML = log
            .slice(0, 5)
            .map((entry) => `<li><span>${escape(entry.timestamp || "")}</span> — ${escape(entry.message || "")}</li>`)
            .join("");
        } else {
          logNode.hidden = true;
          logNode.innerHTML = "";
        }
      }
    });
  };

  document.querySelectorAll("[data-run-job]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const job = btn.dataset.runJob;
      showStatus(`Running ${job}…`, "info");
      try {
        const res = await fetch(`/admin/run/${job}`, { method: "POST" });
        if (!res.ok) {
          const detail = await res.json().catch(() => ({}));
          throw new Error(detail.detail || res.statusText);
        }
        showStatus(`${job} completed.`, "success");
        fetchJobState();
      } catch (err) {
        showStatus(`Run failed: ${err.message}`, "error");
      }
    });
  });

  const pipelineBtn = document.querySelector("[data-run-pipeline]");
  const updatePipelineUI = (state = {}, log = []) => {
    if (pipelineStatus) {
      const shouldShow = !!state.running || !!state.last_error;
      pipelineStatus.hidden = !shouldShow;
      if (shouldShow) {
        if (state.running) {
          pipelineStatus.textContent =
            state.last_message ||
            `Running ${state.stage || "pipeline"}${state.current_date ? ` (${state.current_date})` : ""}…`;
        } else if (state.last_error) {
          pipelineStatus.textContent = `Pipeline errored: ${state.last_error}`;
        }
      }
    }
    if (pipelineBtn) {
      pipelineBtn.disabled = !!state.running;
      pipelineBtn.textContent = state.running ? "Pipeline Running…" : "Run Full Pipeline";
    }
    renderPipelineLog(state.running ? log : []);
  };

  const fetchJobState = async () => {
    try {
      const res = await fetch("/api/jobs");
      if (!res.ok) return;
      const data = await res.json();
      updatePipelineUI((data && data.pipeline) || {}, data.pipeline_log || []);
      refreshJobSummaries(data);
      updateJobRuntime(data.job_runtime || {});
    } catch (err) {
      /* ignore */
    }
  };

  pipelineBtn?.addEventListener("click", async () => {
    showStatus("Starting full pipeline…", "info");
    try {
      const res = await fetch("/admin/run-all", { method: "POST" });
      if (!res.ok) {
        const detail = await res.json().catch(() => ({}));
        throw new Error(detail.detail || res.statusText);
      }
      showStatus("Pipeline started in the background. Monitor status below.", "success");
      fetchJobState();
    } catch (err) {
      showStatus(`Pipeline failed: ${err.message}`, "error");
    }
  });

  const refreshAllBtn = document.querySelector("[data-refresh-test2-all]");
  refreshAllBtn?.addEventListener("click", async () => {
    showStatus("Refreshing all picks dates…", "info");
    try {
      const res = await fetch("/admin/refresh/test2", { method: "POST" });
      if (!res.ok) {
        const detail = await res.json().catch(() => ({}));
        throw new Error(detail.detail || res.statusText);
      }
      showStatus("All dates refreshed.", "success");
      fetchJobState();
    } catch (err) {
      showStatus(`Refresh failed: ${err.message}`, "error");
    }
  });

  fetchJobState();
  pipelinePoll = setInterval(fetchJobState, 12000);

  window.addEventListener("beforeunload", () => {
    if (pipelinePoll) {
      clearInterval(pipelinePoll);
    }
  });
})();
