(() => {
  const status = document.getElementById("status-banner");

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
      } catch (err) {
        showStatus(`Run failed: ${err.message}`, "error");
      }
    });
  });

  const pipelineBtn = document.querySelector("[data-run-pipeline]");
  pipelineBtn?.addEventListener("click", async () => {
    showStatus("Running full pipeline…", "info");
    try {
        const res = await fetch("/admin/run-all", { method: "POST" });
        if (!res.ok) {
          const detail = await res.json().catch(() => ({}));
          throw new Error(detail.detail || res.statusText);
        }
        showStatus("Pipeline finished.", "success");
    } catch (err) {
        showStatus(`Pipeline failed: ${err.message}`, "error");
    }
  });

  const test2Form = document.getElementById("refresh-test2-form");
  if (test2Form) {
    test2Form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const date = test2Form.date.value;
      if (!date) return;
      showStatus(`Refreshing picks for ${date}…`, "info");
      try {
        const res = await fetch(`/admin/refresh/test2?date=${date}`, { method: "POST" });
        if (!res.ok) {
          const detail = await res.json().catch(() => ({}));
          throw new Error(detail.detail || res.statusText);
        }
        showStatus("Refresh complete.", "success");
      } catch (err) {
        showStatus(`Refresh failed: ${err.message}`, "error");
      }
    });
  }
})();
