(() => {
  "use strict";

  const form = document.getElementById("filter-bar");
  const output = document.getElementById("log-out");
  const kindsInput = document.getElementById("kinds-input");
  const pauseButton = document.getElementById("pause-btn");
  let request = null;
  let timer = null;
  let debounceTimer = null;
  let etag = "";
  let failures = 0;

  function isPaused() {
    return pauseButton.getAttribute("data-paused") === "1";
  }

  function clearTimer() {
    if (timer !== null) {
      window.clearTimeout(timer);
      timer = null;
    }
  }

  function refreshInterval() {
    const limit = Number.parseInt(
      form.querySelector('[name="limit"]')?.value || "500",
      10,
    );
    if (limit >= 6000) return 15_000;
    if (limit >= 1500) return 8_000;
    return 5_000;
  }

  function schedule() {
    clearTimer();
    if (document.hidden || isPaused()) return;
    const base = Math.min(
      60_000,
      refreshInterval() * 2 ** Math.min(failures, 4),
    );
    timer = window.setTimeout(() => refreshLogs(), base * (0.85 + Math.random() * 0.3));
  }

  function enhanceLogTable() {
    const table = output.querySelector(".log-table");
    if (!table) return;
    table.setAttribute("aria-label", "Filtered validator event log");
    table.querySelectorAll("thead th").forEach((heading) => {
      heading.setAttribute("scope", "col");
    });
    table.classList.toggle(
      "ours-mode",
      Boolean(form.querySelector('[name="ours"]')?.checked),
    );
  }

  async function refreshLogs(manual = false) {
    if (!manual && (document.hidden || isPaused())) return;
    clearTimer();
    request?.abort();
    const current = new AbortController();
    request = current;
    output.setAttribute("aria-busy", "true");
    const query = new URLSearchParams(new FormData(form));
    const headers = { Accept: "text/html" };
    if (etag) headers["If-None-Match"] = etag;
    try {
      const response = await fetch(`/api/logs?${query.toString()}`, {
        signal: current.signal,
        headers,
      });
      if (response.status === 304) {
        failures = 0;
        output.classList.remove("is-stale");
        return;
      }
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const markup = await response.text();
      output.innerHTML = markup;
      etag = response.headers.get("etag") || "";
      failures = 0;
      output.classList.remove("is-stale");
      enhanceLogTable();
    } catch (error) {
      if (error.name === "AbortError") return;
      failures += 1;
      output.classList.add("is-stale");
    } finally {
      if (request === current) {
        request = null;
        output.setAttribute("aria-busy", "false");
        schedule();
      }
    }
  }

  function queueManualRefresh() {
    window.clearTimeout(debounceTimer);
    debounceTimer = window.setTimeout(() => {
      etag = "";
      refreshLogs(true);
    }, 250);
  }

  function syncKinds() {
    kindsInput.value = Array.from(
      document.querySelectorAll('#kind-pills .kind-pill[data-on="1"]'),
    )
      .map((pill) => pill.getAttribute("data-kind"))
      .join(",");
    queueManualRefresh();
  }

  document.getElementById("kind-pills").addEventListener("click", (event) => {
    const pill = event.target.closest(".kind-pill");
    if (!pill) return;
    const enabled = pill.getAttribute("data-on") !== "1";
    pill.setAttribute("data-on", enabled ? "1" : "0");
    pill.setAttribute("aria-pressed", String(enabled));
    syncKinds();
  });

  pauseButton.addEventListener("click", () => {
    const paused = isPaused();
    pauseButton.setAttribute("data-paused", paused ? "0" : "1");
    pauseButton.setAttribute("aria-pressed", String(!paused));
    pauseButton.textContent = paused ? "● Live" : "Ⅱ Paused";
    if (paused) {
      refreshLogs(true);
    } else {
      clearTimer();
      request?.abort();
      request = null;
    }
  });

  form.addEventListener("submit", (event) => event.preventDefault());
  form.addEventListener("change", (event) => {
    if (event.target.closest("#kind-pills")) return;
    queueManualRefresh();
  });
  form.querySelectorAll('input[type="text"]').forEach((input) => {
    input.addEventListener("input", queueManualRefresh);
  });

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      clearTimer();
      request?.abort();
      request = null;
      output.setAttribute("aria-busy", "false");
    } else if (!isPaused()) {
      refreshLogs(true);
    }
  });

  document.body.addEventListener("click", (event) => {
    const row = event.target.closest(".log-row");
    if (!row) return;
    if (window.getSelection && window.getSelection().toString().length > 0) return;
    const ts = row.querySelector(".log-ts")?.textContent?.trim() ?? "";
    const kind = row.querySelector(".log-kind")?.textContent?.trim() ?? "";
    const reason =
      row.querySelector(".log-reason-cell")?.textContent?.trim() ?? "";
    const hotkey = row.querySelector(".log-hk")?.textContent?.trim() ?? "";
    const message = row.querySelector(".log-msg")?.textContent ?? "";
    const line = `${ts} ${kind} ${reason} ${hotkey} ${message}`.trim();
    navigator.clipboard?.writeText(line).then(() => {
      row.style.background = "rgba(232,122,62,0.18)";
      window.setTimeout(() => {
        row.style.background = "";
      }, 320);
    });
  });

  window.isPaused = isPaused;
  refreshLogs(true);
})();
