(() => {
  const form = document.getElementById("cbb-form");
  const resultBox = document.getElementById("cbb-result");
  const pspForm = document.getElementById("cbb-psp-form");
  const pspResultBox = document.getElementById("cbb-psp-result");
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

  if (form && resultBox) {
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const payload = {
        team1: form.team1.value.trim(),
        team2: form.team2.value.trim(),
        stat: form.stat.value,
      };
      if (!payload.team1) {
        showStatus("Team 1 is required.", "error");
        return;
      }
      showStatus("Fetching CBB summary…", "info");
      resultBox.innerHTML = "<p>Loading…</p>";
      try {
        const res = await fetch("/api/cbb/fetch", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        if (!res.ok) {
          const detail = await res.json().catch(() => ({}));
          throw new Error(detail.detail || res.statusText);
        }
        const data = await res.json();
        resultBox.innerHTML = `
          <h3>${data.heading} — ${data.stat}</h3>
          <pre>${data.summary}</pre>
        `;
        showStatus("CBB summary ready.", "success");
      } catch (err) {
        console.error(err);
        resultBox.innerHTML = `<p class="error">${err.message}</p>`;
        showStatus(`CBB fetch failed: ${err.message}`, "error");
      }
    });
  }

  if (pspForm && pspResultBox) {
    pspForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      const payload = {
        teams: pspForm.teams.value.trim(),
        stats: pspForm.stats.value.trim(),
      };
      if (!payload.stats) {
        showStatus("Enter at least one stat.", "error");
        return;
      }
      showStatus("Running PSP query…", "info");
      pspResultBox.innerHTML = "<p>Loading…</p>";
      try {
        const res = await fetch("/api/cbb/psp", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        if (!res.ok) {
          const detail = await res.json().catch(() => ({}));
          throw new Error(detail.detail || res.statusText);
        }
        const data = await res.json();
        const results = data.results || [];
        if (!results.length) {
          pspResultBox.innerHTML = "<p>No results for that combination.</p>";
        } else {
          pspResultBox.innerHTML = results
            .map(
              (item) => `
                <article class="card">
                    <header>
                        <p class="eyebrow">${item.stat}</p>
                        <strong>${item.heading}</strong>
                    </header>
                    <pre>${item.summary}</pre>
                </article>
              `
            )
            .join("");
        }
        showStatus("PSP query complete.", "success");
      } catch (err) {
        console.error(err);
        pspResultBox.innerHTML = `<p class="error">${err.message}</p>`;
        showStatus(`PSP query failed: ${err.message}`, "error");
      }
    });
  }
})();
