(() => {
  const tableBody = document.querySelector("[data-history-body]");
  const pipelineState = document.querySelector("[data-pipeline-state]");
  const pipelineProcessed = document.querySelector("[data-pipeline-processed]");
  const pipelineError = document.querySelector("[data-pipeline-error]");
  const pipelineLog = document.getElementById("history-pipeline-log");
  const JOB_KEYS = ["schedule_fetch", "injuries", "cbb_scraper", "picks_refresh"];

  const escape = (str = "") =>
    str.replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch] || ch));

  const renderHistory = (rows = []) => {
    if (!tableBody) return;
    tableBody.innerHTML = rows
      .map(
        (row) => `
        <tr>
            <td>${escape(row.ran_at || "")}</td>
            <td>${escape(row.name || "")}</td>
            <td>${row.exit_code}</td>
            <td><pre>${escape(row.stdout || "(empty)")}</pre></td>
            <td><pre>${escape(row.stderr || "")}</pre></td>
        </tr>`
      )
      .join("");
  };

  const renderPipeline = (state = {}, log = []) => {
    if (pipelineState) {
      if (state.running) {
        const dateLabel = state.current_date
          ? ` (processing ${state.current_date.slice(0, 4)}-${state.current_date.slice(4, 6)}-${state.current_date.slice(6)})`
          : "";
        pipelineState.textContent = `Running — stage ${state.stage || "pending"}${dateLabel}`;
      } else {
        pipelineState.textContent = `Idle since ${state.last_finished_at || "n/a"}`;
      }
    }
    if (pipelineProcessed) {
      const dates = state.processed_dates || [];
      pipelineProcessed.textContent = dates.length
        ? `Processed dates: ${dates.join(", ")}`
        : "No dates processed yet this run.";
    }
    if (pipelineError) {
      if (state.last_error) {
        pipelineError.hidden = false;
        pipelineError.textContent = `Last error: ${state.last_error}`;
      } else {
        pipelineError.hidden = true;
      }
    }
    if (pipelineLog) {
      if (!log.length) {
        pipelineLog.hidden = true;
        pipelineLog.innerHTML = "";
      } else {
        pipelineLog.hidden = false;
        pipelineLog.innerHTML = log
          .slice(0, 10)
          .map((entry) => `<li><span>${escape(entry.timestamp || "")}</span> — ${escape(entry.message || "")}</li>`)
          .join("");
      }
    }
  };

  const renderJobRuntime = (runtime = {}) => {
    JOB_KEYS.forEach((name) => {
      const state = runtime[name] || {};
      const statusNode = document.querySelector(`[data-job-status="${name}"]`);
      const logNode = document.querySelector(`[data-job-log="${name}"]`);
      if (statusNode) {
        if (state.running) {
          statusNode.hidden = false;
          statusNode.textContent = state.last_message || "Running…";
        } else if (state.last_error) {
          statusNode.hidden = false;
          statusNode.textContent = `Error: ${state.last_error}`;
        } else if (state.last_message) {
          statusNode.hidden = false;
          statusNode.textContent = state.last_message;
        } else {
          statusNode.hidden = true;
        }
      }
      if (logNode) {
        const log = state.log || [];
        if (log.length) {
          logNode.hidden = false;
          logNode.innerHTML = log
            .slice(0, 8)
            .map((entry) => `<li><span>${escape(entry.timestamp || "")}</span> — ${escape(entry.message || "")}</li>`)
            .join("");
        } else {
          logNode.hidden = true;
          logNode.innerHTML = "";
        }
      }
    });
  };

  const poll = async () => {
    try {
      const res = await fetch("/api/jobs/history");
      if (!res.ok) return;
      const payload = await res.json();
      renderHistory(payload.history || []);
      renderPipeline(payload.pipeline || {}, payload.log || []);
      renderJobRuntime(payload.runtime || {});
    } catch (err) {
      /* ignore */
    }
  };

  poll();
  setInterval(poll, 12000);
})();
