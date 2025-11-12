(() => {
  const sections = {
    cbb: document.getElementById("cbb-cards"),
    test2: document.getElementById("test2-cards"),
  };
  const updatedLabels = {
    cbb: document.querySelector('[data-updated="cbb"]'),
    test2: document.querySelector('[data-updated="test2"]'),
  };
  const statusBanner = document.getElementById("status-banner");
  const refreshAllBtn = document.getElementById("refresh-all-btn");
  const searchInput = document.getElementById("search-input");
  const jobMetadata = window.jobMetadata || [];
  let jobState = window.initialJobs || {};
  const pipelineBtn = document.getElementById("pipeline-btn");

  let searchTerm = "";
  let currentData = window.initialPicks || {};

  const showStatus = (message, type = "info") => {
    if (!statusBanner) return;
    if (!message) {
      statusBanner.hidden = true;
      statusBanner.textContent = "";
      return;
    }
    statusBanner.textContent = message;
    statusBanner.dataset.type = type;
    statusBanner.hidden = false;
  };

  const fetchJobState = async () => {
    try {
      const res = await fetch("/api/jobs");
      if (!res.ok) return;
      jobState = await res.json();
      renderJobs();
    } catch (err) {
      console.error("[jobs] refresh failed", err);
    }
  };

  const formatDate = (iso) => {
    if (!iso) return "never";
    try {
      return new Date(iso).toLocaleString();
    } catch {
      return iso;
    }
  };

  const friendlyTitle = (item, index) => {
    if (item.heading) return item.heading;
    if (item.category === "game" && item.matchup) {
      const { team1, team2 } = item.matchup;
      return `${team1 || "Team 1"} vs ${team2 || "Team 2"} — ${item.sport || ""}`.trim();
    }
    return `Entry ${index + 1}`;
  };

  const detailList = (item) => {
    const list = [];
    if (item.stat) list.push(`<span>Stat:</span> ${item.stat}`);
    if (item.suffix) list.push(`<span>Window:</span> ${item.suffix}`);
    if (item.teams && item.teams.length) list.push(`<span>Teams:</span> ${item.teams.join(", ")}`);
    if (item.category) list.push(`<span>Category:</span> ${item.category}`);
    return list.map((line) => `<li>${line}</li>`).join("");
  };

  const renderCards = (kind) => {
    const container = sections[kind];
    if (!container) return;
    const payload = currentData[kind];
    const items = payload?.data || [];
    container.innerHTML = "";
    if (!items.length) {
      container.innerHTML =
        '<p class="empty">No cached data yet. Run a refresh to populate this feed.</p>';
      return;
    }

    const lowered = searchTerm.toLowerCase();

    items.forEach((item, index) => {
      const haystack = JSON.stringify(item).toLowerCase();
      if (lowered && !haystack.includes(lowered)) {
        return;
      }

      const card = document.createElement("article");
      card.className = "card";
      card.innerHTML = `
        <header>
            <div>
                <p class="eyebrow">${item.category || "entry"}</p>
                <h3>${friendlyTitle(item, index)}</h3>
            </div>
            <button class="btn tiny" type="button">Copy JSON</button>
        </header>
        <ul class="meta-list">${detailList(item)}</ul>
        <details>
            <summary>Raw JSON</summary>
            <pre>${JSON.stringify(item, null, 2)}</pre>
        </details>
      `;

      const copyBtn = card.querySelector("button");
      copyBtn.addEventListener("click", () => {
        navigator.clipboard.writeText(JSON.stringify(item, null, 2));
        showStatus(`${kind.toUpperCase()} entry copied to clipboard.`, "success");
        setTimeout(() => showStatus("", "info"), 2000);
      });

      container.appendChild(card);
    });

    if (!container.children.length) {
      container.innerHTML =
        '<p class="empty">No cards matched your filter. Clear the search box to view everything.</p>';
    }
  };

  const renderAll = () => {
    Object.keys(sections).forEach((kind) => renderCards(kind));
    Object.entries(updatedLabels).forEach(([kind, node]) => {
      if (node) node.textContent = formatDate(currentData[kind]?.updated_at);
    });
  };

  const refreshKind = async (kind) => {
    showStatus(`Refreshing ${kind.toUpperCase()}…`, "info");
    try {
      const res = await fetch(`/admin/refresh/${kind}`, { method: "POST" });
      if (!res.ok) {
        const detail = await res.json().catch(() => ({}));
        throw new Error(detail.detail || res.statusText);
      }
      const payload = await res.json();
      currentData[kind] = payload;
      renderCards(kind);
      if (updatedLabels[kind]) {
        updatedLabels[kind].textContent = formatDate(payload.updated_at);
      }
      showStatus(`${kind.toUpperCase()} refreshed successfully.`, "success");
      fetchJobState();
    } catch (err) {
      console.error(err);
      showStatus(`Refresh failed: ${err.message}`, "error");
    }
  };

  const formatBuckets = (entry) => {
    if (!entry?.buckets) return "";
    const chunks = [];
    const labels = {
      green: "🟢",
      yellow: "🟡",
      red: "🔴",
      purple: "🟣",
    };
    Object.entries(entry.buckets).forEach(([key, players]) => {
      if (players && players.length) {
        chunks.push(`${labels[key] || key}: ${players.join(", ")}`);
      }
    });
    return chunks.join(" | ");
  };

  const serializeEntry = (item) => {
    const heading = friendlyTitle(item, 0);
    if (item.entries && item.entries.length) {
      const lines = item.entries.map((entry) => {
        const parts = [
          entry.stat ? entry.stat.toUpperCase() : null,
          entry.suffix || entry.summary ? `(${entry.suffix || "this season"})` : null,
        ].filter(Boolean);
        const buckets = formatBuckets(entry);
        const summary = entry.summary || buckets;
        return `${parts.join(" ")}\n${summary || buckets}`;
      });
      return `${heading}\n${lines.join("\n\n")}`;
    }
    const summary = item.summary || formatBuckets(item);
    return `${heading}\n${summary || JSON.stringify(item, null, 2)}`;
  };

  const copyFeed = (kind) => {
    const payload = currentData[kind];
    if (!payload || !payload.data || !payload.data.length) {
      showStatus(`No ${kind.toUpperCase()} picks to copy yet.`, "error");
      return;
    }
    const text = payload.data.map((item, idx) => `${idx + 1}. ${serializeEntry(item)}`).join("\n\n");
    navigator.clipboard.writeText(text).then(() => {
      showStatus(`${kind.toUpperCase()} feed copied to clipboard.`, "success");
      setTimeout(() => showStatus("", "info"), 2500);
    });
  };

  const refreshAll = async () => {
    showStatus("Refreshing both feeds…", "info");
    try {
      const res = await fetch("/admin/refresh-all", { method: "POST" });
      if (!res.ok) {
        const detail = await res.json().catch(() => ({}));
        throw new Error(detail.detail || res.statusText);
      }
      const payload = await res.json();
      Object.entries(payload.results || {}).forEach(([kind, data]) => {
        currentData[kind] = data;
      });
      renderAll();
      showStatus("All feeds refreshed.", "success");
      fetchJobState();
    } catch (err) {
      console.error(err);
      showStatus(`Refresh failed: ${err.message}`, "error");
    }
  };

  const downloadKind = (kind) => {
    const payload = currentData[kind];
    const blob = new Blob([JSON.stringify(payload?.data ?? [], null, 2)], {
      type: "application/json",
    });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `${kind}_picks.json`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  };

  document.querySelectorAll("[data-refresh]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const kind = btn.dataset.refresh;
      refreshKind(kind);
    });
  });

  document.querySelectorAll("[data-download]").forEach((btn) => {
    btn.addEventListener("click", () => {
      downloadKind(btn.dataset.download);
    });
  });

  document.querySelectorAll("[data-copy]").forEach((btn) => {
    btn.addEventListener("click", () => copyFeed(btn.dataset.copy));
  });

  const runPipeline = async () => {
    showStatus("Running full pipeline (schedule → injuries → picks)…", "info");
    try {
      const res = await fetch("/admin/run-all", { method: "POST" });
      if (!res.ok) {
        const detail = await res.json().catch(() => ({}));
        throw new Error(detail.detail || res.statusText);
      }
      const payload = await res.json();
      const results = payload.results || {};
      Object.entries(results).forEach(([key, value]) => {
        if (Object.prototype.hasOwnProperty.call(jobState, key)) {
          jobState[key] = value;
        }
      });
      const test2 = results.test2;
      if (test2) {
        currentData.test2 = test2;
        renderCards("test2");
        if (updatedLabels.test2) {
          updatedLabels.test2.textContent = formatDate(test2.updated_at);
        }
      }
      renderJobs();
      showStatus("Full pipeline completed.", "success");
    } catch (err) {
      console.error(err);
      showStatus(`Pipeline failed: ${err.message}`, "error");
    }
  };

  const updateJobCard = (name, payload) => {
    const updated = document.querySelector(`[data-job-updated="${name}"]`);
    const exit = document.querySelector(`[data-job-exit="${name}"]`);
    const stdout = document.querySelector(`[data-job-stdout="${name}"]`);
    const stderr = document.querySelector(`[data-job-stderr="${name}"]`);
    if (updated) updated.textContent = payload?.ran_at || "never";
    if (exit) exit.textContent = payload?.exit_code ?? "–";
    if (stdout) stdout.textContent = payload?.stdout || "No runs yet.";
    if (stderr) stderr.textContent = payload?.stderr || "";
  };

  const renderJobs = () => {
    jobMetadata.forEach((meta) => {
      updateJobCard(meta.key, jobState[meta.key]);
    });
  };

  const runJob = async (name) => {
    showStatus(`Running ${name}...`, "info");
    try {
      const res = await fetch(`/admin/run/${name}`, { method: "POST" });
      if (!res.ok) {
        const detail = await res.json().catch(() => ({}));
        throw new Error(detail.detail || res.statusText);
      }
      const payload = await res.json();
      jobState[name] = payload;
      updateJobCard(name, payload);
      showStatus(`${name} completed (exit ${payload.exit_code}).`, payload.exit_code === 0 ? "success" : "error");
    } catch (err) {
      console.error(err);
      showStatus(`Job failed: ${err.message}`, "error");
    }
  };

  const copyJobLog = (name) => {
    const payload = jobState[name];
    if (!payload) {
      showStatus("No log to copy yet.", "error");
      return;
    }
    const log = [
      `Job: ${name}`,
      `Ran At: ${payload.ran_at}`,
      `Exit Code: ${payload.exit_code}`,
      "",
      "STDOUT:",
      payload.stdout || "(empty)",
      "",
      "STDERR:",
      payload.stderr || "(empty)",
    ].join("\n");
    navigator.clipboard.writeText(log).then(() => {
      showStatus(`${name} log copied.`, "success");
      setTimeout(() => showStatus("", "info"), 2500);
    });
  };

  document.querySelectorAll("[data-run]").forEach((btn) => {
    btn.addEventListener("click", () => runJob(btn.dataset.run));
  });

  document.querySelectorAll("[data-copy-log]").forEach((btn) => {
    btn.addEventListener("click", () => copyJobLog(btn.dataset.copyLog));
  });

  if (pipelineBtn) {
    pipelineBtn.addEventListener("click", runPipeline);
  }

  if (refreshAllBtn) {
    refreshAllBtn.addEventListener("click", refreshAll);
  }

  if (searchInput) {
    searchInput.addEventListener("input", (event) => {
      searchTerm = event.target.value || "";
      renderAll();
    });
  }

  currentData = window.initialPicks || {};
  jobState = window.initialJobs || {};
  renderAll();
  renderJobs();
})();
