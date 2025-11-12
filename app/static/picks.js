(() => {
  const datasetNode = document.getElementById("picks-data");
  const kindNode = document.getElementById("picks-kind");
  const dateNode = document.getElementById("selected-date");
  const container = document.querySelector(".picks-grid");
  const statusBanner = document.getElementById("status-banner");
  const copyAllBtn = document.getElementById("copy-all-btn");
  const dateSelect = document.getElementById("date-select");

  if (!datasetNode || !kindNode || !container) {
    return;
  }

  let data = JSON.parse(datasetNode.textContent || "[]");
  const kind = JSON.parse(kindNode.textContent || "\"test2\"");
  const emptyTemplate = container.dataset.emptyMessage || "No picks available for DATE.";
  const getSelectedDate = () => (dateSelect ? dateSelect.value : dateNode ? JSON.parse(dateNode.textContent || "\"\"") : "");

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
      showStatus("", "info");
    } catch (err) {
      showStatus(`Failed to load picks: ${err.message}`, "error");
    }
  };

  if (dateSelect && kind === "test2") {
    dateSelect.addEventListener("change", (event) => fetchDate(event.target.value));
  }
})();
