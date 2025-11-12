(() => {
  const form = document.getElementById("cbb-form");
  const resultBox = document.getElementById("cbb-result");
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

  if (!form || !resultBox) return;

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
})();
