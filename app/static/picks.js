(() => {
  const datasetNode = document.getElementById("picks-data");
  const kindNode = document.getElementById("picks-kind");
  const dateNode = document.getElementById("selected-date");
  const container = document.querySelector(".picks-grid");
  const statusBanner = document.getElementById("status-banner");
  const copyAllBtn = document.getElementById("copy-all-btn");
  const dateSelect = document.getElementById("date-select");
  const manualForm = document.getElementById("multi-form");
  const manualResult = document.getElementById("multi-result");
  const manualDateLabel = document.getElementById("multi-date-label");
  const pspForm = document.getElementById("psp-form");
  const pspResult = document.getElementById("psp-result");

  if (!datasetNode || !kindNode || !container) {
    return;
  }

  let data = JSON.parse(datasetNode.textContent || "[]");
  const kind = JSON.parse(kindNode.textContent || "\"test2\"");
  const emptyTemplate = container.dataset.emptyMessage || "No picks available for DATE.";
  const getSelectedDate = () => (dateSelect ? dateSelect.value : dateNode ? JSON.parse(dateNode.textContent || "\"\"") : "");
  const humanizeDate = (value) => {
    if (!value || value.length !== 8) return value || "";
    return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
  };

  const showStatus = (msg, type = "info") => {
    if (!statusBanner) return;
    if (!msg) {
      statusBanner.hidden = true;
      statusBanner.textContent = "";
      return;
    }
    statusBanner.hidden = false;
    statusBanner.textContent = msg;
    statusBanner.dataset.type = type;
  };

  const formatEntry = (item) => {
    if (!item) return "";
    const lines = [];
    if (item.heading) lines.push(item.heading);
    if (item.entries && item.entries.length) {
      item.entries.forEach((entry) => {
        const statLine = entry.stat ? `${entry.stat} — ${entry.suffix || "this season"}` : "";
        if (statLine) lines.push(statLine);
        if (entry.summary) lines.push(entry.summary);
      });
    } else if (item.summary) {
      lines.push(item.summary);
    }
    return lines.join("\n");
  };

  const render = () => {
    if (manualDateLabel) {
      manualDateLabel.textContent = `Queries use the currently selected date (${humanizeDate(getSelectedDate())}). Change the dropdown above to switch days.`;
    }
    if (!data.length) {
      const msg = emptyTemplate.replace("DATE", getSelectedDate());
      container.innerHTML = `<p class="empty">${msg}</p>`;
      return;
    }

    container.innerHTML = data
      .map(
        (block, idx) => `
        <article class="card picks-card" data-entry="${idx}">
            <header>
                <div>
                    <p class="eyebrow">${block.sport || block.category || ""}</p>
                    <h3>${block.heading || "Matchup"}</h3>
                    <p class="meta">
                        ${block.entries ? `${block.entries.length} stats · ` : ""}Generated ${block.generated_at || getSelectedDate()}
                    </p>
                </div>
                <button class="btn tiny" type="button" data-copy-entry="${idx}">Copy group</button>
            </header>
            ${
              block.entries
                ? `<ul class="entry-list">${block.entries
                    .map(
                      (entry) => `
                        <li>
                            <strong>${entry.stat} — ${entry.suffix || "this season"}</strong>
                            <pre>${entry.summary}</pre>
                        </li>`
                    )
                    .join("")}</ul>`
                : `<pre>${block.summary || ""}</pre>`
            }
        </article>`
      )
      .join("");

    container.querySelectorAll("[data-copy-entry]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const item = data[Number(btn.dataset.copyEntry)];
        if (!item) return;
        navigator.clipboard.writeText(formatEntry(item)).then(() => showStatus("Group copied to clipboard.", "success"));
      });
    });
  };

  render();

  copyAllBtn?.addEventListener("click", () => {
    if (!data.length) {
      showStatus("No picks to copy yet.", "error");
      return;
    }
    const blob = data
      .map((item, index) => `${index + 1}. ${item.heading || "Entry"}\n${formatEntry(item)}`)
      .join("\n\n");
    navigator.clipboard.writeText(blob).then(() => showStatus("All picks copied.", "success"));
  });

  const fetchDate = async (date) => {
    if (!date) return;
    showStatus("Loading picks…", "info");
    try {
      const res = await fetch(`/api/picks/test2?date=${date}`);
      if (!res.ok) {
        const detail = await res.json().catch(() => ({}));
        throw new Error(detail.detail || res.statusText);
      }
      const payload = await res.json();
      data = payload.data || [];
      render();
      if (manualForm && manualResult) {
        manualResult.innerHTML = "<p>Date updated. Run another lookup to see the new slate.</p>";
      }
      showStatus("", "info");
    } catch (err) {
      showStatus(`Failed to load picks: ${err.message}`, "error");
    }
  };

  if (dateSelect && kind === "test2") {
    dateSelect.addEventListener("change", (event) => fetchDate(event.target.value));
  }

  const normalize = (value, { strict = false } = {}) => {
    if (!value) return "";
    const upper = value.toString().trim().toUpperCase();
    return strict ? upper : upper.replace(/[^A-Z0-9]/g, "");
  };

  const matchesTeam = (block, team) => {
    if (!team) return true;
    const target = normalize(team);
    if (!target) return true;
    const teams = (block.teams || []).map((t) => normalize(t));
    if (teams.some((entry) => entry.includes(target) || target.includes(entry))) {
      return true;
    }
    const heading = normalize(block.heading || "");
    return heading.includes(target);
  };

  const matchesSport = (block, sport) => {
    if (!sport) return true;
    const blockSport = normalize(block.sport || block.category, { strict: true });
    return blockSport === sport;
  };

  const matchesStat = (entry, stat) => {
    if (!stat) return true;
    const entryStat = normalize(entry.stat, { strict: true });
    return entryStat.includes(stat);
  };

  const searchPicks = (criteria) => {
    const results = [];
    data.forEach((block) => {
      if (!matchesSport(block, criteria.sport)) return;
      if (!matchesTeam(block, criteria.team1)) return;
      if (criteria.team2 && !matchesTeam(block, criteria.team2)) return;
      if (!Array.isArray(block.entries)) return;
      block.entries.forEach((entry) => {
        if (!matchesStat(entry, criteria.stat)) return;
        results.push({
          heading: block.heading || "Matchup",
          sport: block.sport || block.category || "",
          stat: entry.stat,
          summary: entry.summary,
          suffix: entry.suffix || "this season",
        });
      });
    });
    return results;
  };

  const renderPspResults = (sport, statsRaw) => {
    if (!pspResult) return;
    const statList = (statsRaw || "")
      .split(",")
      .map((s) => s.trim().toUpperCase())
      .filter(Boolean);
    if (!statList.length) {
      pspResult.innerHTML = "<p>Please enter at least one stat.</p>";
      return;
    }
    const filtered = data.filter((block) => {
      if ((block.category || "").toUpperCase() !== "PSP") return false;
      if (sport && (block.sport || "").toUpperCase() !== sport) return false;
      return statList.includes((block.stat || "").toUpperCase());
    });
    if (!filtered.length) {
      pspResult.innerHTML = "<p>No PSP entries match those filters in the current cache.</p>";
      return;
    }
    pspResult.innerHTML = filtered
      .map(
        (block) => `
        <article class="card">
            <header>
                <p class="eyebrow">${block.sport || "PSP"}</p>
                <strong>${block.heading || block.stat}</strong>
                <p class="meta">${block.stat || ""}</p>
            </header>
            <pre>${block.summary || ""}</pre>
        </article>
      `
      )
      .join("");
  };

  const renderManualResults = (results, criteria) => {
    if (!manualResult) return;
    if (!results.length) {
      manualResult.innerHTML = `<p>No picks found for ${criteria.stat || "that stat"} on ${humanizeDate(
        getSelectedDate()
      )}. Try adjusting the filters.</p>`;
      return;
    }
    manualResult.innerHTML = results
      .map(
        (item) => `
        <article class="card">
            <header>
                <p class="eyebrow">${item.sport}</p>
                <strong>${item.heading}</strong>
                <p class="meta">${item.stat} — ${item.suffix}</p>
            </header>
            <pre>${item.summary}</pre>
        </article>
      `
      )
      .join("");
  };

  manualForm?.addEventListener("submit", (event) => {
    event.preventDefault();
    if (!data.length) {
      manualResult.innerHTML = "<p>No cached picks for this date yet. Refresh the slate and try again.</p>";
      return;
    }
    const formData = new FormData(manualForm);
    const criteria = {
      team1: formData.get("team1"),
      team2: formData.get("team2"),
      sport: normalize(formData.get("sport"), { strict: true }),
      stat: normalize(formData.get("stat"), { strict: true }),
    };
    if (!criteria.stat) {
      manualResult.innerHTML = "<p>Please provide a stat to filter by.</p>";
      return;
    }
    renderManualResults(searchPicks(criteria), criteria);
  });

  pspForm?.addEventListener("submit", (event) => {
    event.preventDefault();
    const formData = new FormData(pspForm);
    const sport = (formData.get("sport") || "").toString().toUpperCase();
    const stats = (formData.get("stats") || "").toString();
    renderPspResults(sport, stats);
  });
})();
