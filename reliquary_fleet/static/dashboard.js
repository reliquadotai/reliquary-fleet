(() => {
  "use strict";

  const drawer = document.getElementById("drawer");
  const backdrop = document.getElementById("drawer-backdrop");
  const soundToggle = document.getElementById("sound-toggle");
  let drawerTrigger = null;
  let drawerRequest = null;
  let soundOn = false;
  let lastHealth = null;
  let audioContext = null;
  let snapshotRequest = null;
  let snapshotTimer = null;
  let snapshotFailures = 0;
  let snapshotLoaded = false;
  let attemptFilter = "all";
  const demoMode = document.body.dataset.demo === "true";
  const snapshotIntervalMs =
    Math.max(5, Number.parseFloat(document.body.dataset.refreshSeconds) || 5) *
    1000;

  function setSound(enabled) {
    soundOn = enabled;
    soundToggle.setAttribute("aria-pressed", String(enabled));
    soundToggle.setAttribute(
      "title",
      enabled ? "Disable alert sound" : "Enable alert sound",
    );
    soundToggle.querySelector("[data-sound-icon]").textContent = enabled ? "♫" : "♪";
    soundToggle.querySelector("[data-sound-label]").textContent = enabled
      ? "Sound on"
      : "Sound off";
  }

  soundToggle.addEventListener("click", async () => {
    setSound(!soundOn);
    if (soundOn) {
      audioContext ||= new (window.AudioContext || window.webkitAudioContext)();
      if (audioContext.state === "suspended") await audioContext.resume();
    }
  });

  function ping() {
    if (!soundOn || !audioContext) return;
    const osc = audioContext.createOscillator();
    const gain = audioContext.createGain();
    osc.connect(gain);
    gain.connect(audioContext.destination);
    osc.frequency.value = 880;
    gain.gain.setValueAtTime(0.045, audioContext.currentTime);
    gain.gain.exponentialRampToValueAtTime(
      0.001,
      audioContext.currentTime + 0.16,
    );
    osc.start();
    osc.stop(audioContext.currentTime + 0.16);
  }

  function panelForEvent(event) {
    return event.detail?.target || event.target;
  }

  function updateHealth(panel) {
    if (!panel.classList?.contains("area-summary")) return;
    const text = panel.textContent || "";
    const current = text.includes("ALERT")
      ? "alert"
      : text.includes("DEGRADED")
        ? "degraded"
        : "ok";
    if (lastHealth && lastHealth !== current && current !== "ok") ping();
    lastHealth = current;
  }

  function enhanceTables(root = document) {
    root.querySelectorAll("table.grid").forEach((table) => {
      const area = table.closest("[aria-label]");
      if (!table.hasAttribute("aria-label") && area) {
        table.setAttribute("aria-label", area.getAttribute("aria-label"));
      }
      table.querySelectorAll("thead th").forEach((heading) => {
        heading.setAttribute("scope", "col");
      });
    });
  }

  function applyAttemptFilter(root = document) {
    const panels = [
      ...(root.matches?.(".area-one-attempts") ? [root] : []),
      ...root.querySelectorAll(".area-one-attempts"),
    ];
    panels.forEach((panel) => {
      const buttons = Array.from(
        panel.querySelectorAll("[data-one-attempt-filter]"),
      );
      if (!buttons.length) return;
      if (
        attemptFilter !== "all" &&
        !buttons.some((button) => button.dataset.oneAttemptFilter === attemptFilter)
      ) {
        attemptFilter = "all";
      }
      buttons.forEach((button) => {
        const active = button.dataset.oneAttemptFilter === attemptFilter;
        button.classList.toggle("is-active", active);
        button.setAttribute("aria-pressed", String(active));
      });
      let visible = 0;
      panel.querySelectorAll("[data-attempt-row]").forEach((row) => {
        const show =
          attemptFilter === "all" || row.dataset.attemptState === attemptFilter;
        row.hidden = !show;
        if (show) visible += 1;
      });
      const status = panel.querySelector("[data-one-attempt-visible]");
      if (status) status.textContent = `${visible} shown`;
    });
  }

  function enhanceFleetRows(root = document) {
    root.querySelectorAll(".area-fleet table.grid tbody tr").forEach((row) => {
      const label = row.querySelector(".lbl")?.textContent?.trim();
      if (!label) return;
      row.dataset.boxLabel = label;
    });
  }

  function enhanceScrollableRegions(root = document) {
    const selector = Array.from(scrollableClasses, (name) => `.${name}`).join(",");
    const regions = [
      ...(root.matches?.(selector) ? [root] : []),
      ...root.querySelectorAll(selector),
    ];
    regions.forEach((region) => {
      region.tabIndex = 0;
    });
  }

  function focusableDrawerElements() {
    return Array.from(
      drawer.querySelectorAll(
        'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
      ),
    ).filter((element) => !element.hidden);
  }

  function showDrawer(trigger) {
    drawerTrigger = trigger;
    drawer.removeAttribute("inert");
    drawer.classList.add("open");
    drawer.setAttribute("aria-hidden", "false");
    backdrop.hidden = false;
    document.body.classList.add("drawer-open");
  }

  function closeDrawer() {
    if (!drawer.classList.contains("open")) return;
    drawerRequest?.abort();
    drawerRequest = null;
    drawer.classList.remove("open");
    drawer.setAttribute("aria-hidden", "true");
    drawer.setAttribute("inert", "");
    backdrop.hidden = true;
    document.body.classList.remove("drawer-open");
    const trigger = drawerTrigger;
    drawerTrigger = null;
    if (trigger?.isConnected) trigger.focus();
  }

  async function openDrawer(row, trigger = row) {
    const label = row.dataset.boxLabel || row.querySelector(".lbl")?.textContent?.trim();
    if (!label) return;
    drawerRequest?.abort();
    const request = new AbortController();
    drawerRequest = request;
    drawer.innerHTML = '<div class="drawer-loading" role="status">Loading miner details...</div>';
    showDrawer(trigger);
    drawer.focus();
    try {
      const response = await fetch(`/api/box/${encodeURIComponent(label)}`, {
        signal: request.signal,
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      drawer.innerHTML = await response.text();
      enhanceTables(drawer);
      (focusableDrawerElements()[0] || drawer).focus();
    } catch (error) {
      if (error.name === "AbortError") return;
      drawer.innerHTML =
        '<button class="close" type="button" data-drawer-close aria-label="Close miner details">×</button>' +
        '<h3>Miner details unavailable</h3><p class="dim">The local snapshot could not be loaded.</p>';
      drawer.querySelector("[data-drawer-close]").focus();
    } finally {
      if (drawerRequest === request) drawerRequest = null;
    }
  }

  backdrop.addEventListener("click", closeDrawer);

  document.body.addEventListener("click", (event) => {
    const close = event.target.closest("[data-drawer-close]");
    if (close) {
      event.preventDefault();
      closeDrawer();
      return;
    }

    const row = event.target.closest(".area-fleet table.grid tbody tr");
    const detailButton = event.target.closest(".fleet-detail-button");
    if (!row) return;
    if (event.target.closest("a, button, input, select, textarea") && !detailButton) return;
    openDrawer(row, detailButton || row);
  });

  document.body.addEventListener("change", (event) => {
    const filter = event.target.closest("[data-one-log-filter]");
    if (!filter) return;
    const panel = filter.closest(".one-panel");
    if (!panel) return;
    const selected = {};
    panel.querySelectorAll("[data-one-log-filter]").forEach((control) => {
      selected[control.dataset.oneLogFilter] = control.value;
    });
    panel.querySelectorAll("[data-one-log-row]").forEach((row) => {
      row.hidden = Object.entries(selected).some(
        ([name, value]) => value && row.dataset[name] !== value,
      );
    });
  });

  document.body.addEventListener("click", (event) => {
    const button = event.target.closest("[data-one-attempt-filter]");
    if (!button) return;
    attemptFilter = button.dataset.oneAttemptFilter || "all";
    applyAttemptFilter(button.closest(".area-one-attempts") || document);
  });

  document.addEventListener("keydown", (event) => {
    if (!drawer.classList.contains("open")) return;
    if (event.key === "Escape") {
      event.preventDefault();
      closeDrawer();
      return;
    }
    if (event.key !== "Tab") return;
    const focusable = focusableDrawerElements();
    if (!focusable.length) {
      event.preventDefault();
      drawer.focus();
      return;
    }
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  });

  document.body.addEventListener("click", (event) => {
    const pill = event.target.closest(
      "#rundown-tf-row .rd-pill, #rundown-tf-row .rd-pill-active",
    );
    if (!pill) return;
    event.preventDefault();
    const timeframe = pill.getAttribute("data-tf");
    const area = document.getElementById("rundown-area");
    if (!timeframe || !area) return;
    area.dataset.tf = timeframe;
    area.setAttribute(
      "hx-get",
      `/api/validator_rundown?tf=${encodeURIComponent(timeframe)}`,
    );
    refreshSnapshot(true);
  });

  document.body.addEventListener("click", (event) => {
    const pill = event.target.closest(
      "#ema-mode-row .rd-pill, #ema-mode-row .rd-pill-active",
    );
    if (!pill) return;
    event.preventDefault();
    const mode = pill.getAttribute("data-mode");
    const area = document.getElementById("ema-area");
    if (!mode || !area) return;
    area.dataset.mode = mode;
    area.setAttribute("hx-get", `/api/ema?mode=${encodeURIComponent(mode)}`);
    refreshSnapshot(true);
  });

  document.body.addEventListener("click", (event) => {
    const button = event.target.closest(".star-btn");
    if (!button) return;
    event.preventDefault();
    event.stopPropagation();
    const hotkey = button.getAttribute("data-hk");
    if (!hotkey) return;
    button.disabled = true;
    fetch(`/api/star/${encodeURIComponent(hotkey)}`, {
      method: "POST",
      headers: { "X-Reliquary-Fleet": "1" },
    })
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      })
      .then(() => {
        refreshSnapshot(true);
      })
      .catch((error) => console.warn("Star toggle failed", error))
      .finally(() => {
        button.disabled = false;
      });
  });

  const scrollableClasses = new Set([
    "area-one-attempts",
    "area-one-auction",
    "area-one-log",
    "area-ema",
    "area-windows",
    "area-forensics",
    "area-pipeline",
    "area-fleet",
    "area-labs",
    "area-frontier",
    "area-chain",
    "area-baseline",
    "area-rtt",
    "area-slotrank",
    "area-competitors",
    "area-quality",
    "area-valevents",
    "area-events",
    "area-rundown",
  ]);

  function preservePanelScroll(target) {
    if (
      Array.from(target.classList).some((name) => scrollableClasses.has(name))
    ) {
      target.dataset.savedScroll = String(target.scrollTop);
    }
  }

  function restorePanelScroll(target) {
    if (!Object.hasOwn(target.dataset, "savedScroll")) return;
    requestAnimationFrame(() => {
      const top = Number.parseInt(target.dataset.savedScroll, 10);
      if (!Number.isNaN(top)) target.scrollTop = top;
      delete target.dataset.savedScroll;
    });
  }

  function applyPanelHTML(target, markup) {
    preservePanelScroll(target);
    target.innerHTML = markup;
    target.setAttribute("aria-busy", "false");
    target.classList.remove("is-stale");
    enhanceTables(target);
    enhanceFleetRows(target);
    enhanceScrollableRegions(target);
    applyAttemptFilter(target);
    updateHealth(target);
    restorePanelScroll(target);
  }

  function snapshotURL() {
    const query = new URLSearchParams({
      ema_mode: document.getElementById("ema-area")?.dataset.mode || "top",
      rundown_tf: document.getElementById("rundown-area")?.dataset.tf || "30m",
    });
    return `/api/dashboard-snapshot?${query.toString()}`;
  }

  function clearSnapshotTimer() {
    if (snapshotTimer !== null) {
      window.clearTimeout(snapshotTimer);
      snapshotTimer = null;
    }
  }

  function scheduleSnapshot() {
    clearSnapshotTimer();
    if (demoMode || document.hidden) return;
    const backoff = Math.min(
      60_000,
      snapshotIntervalMs * 2 ** Math.min(snapshotFailures, 4),
    );
    const jitter = 0.85 + Math.random() * 0.3;
    snapshotTimer = window.setTimeout(() => {
      refreshSnapshot();
    }, backoff * jitter);
  }

  async function refreshSnapshot(force = false) {
    if (document.hidden && !force) return;
    if (demoMode && snapshotLoaded && !force) return;
    clearSnapshotTimer();
    if (snapshotRequest) {
      if (!force) return;
      snapshotRequest.abort();
    }
    const request = new AbortController();
    snapshotRequest = request;
    document.querySelectorAll("[data-panel]").forEach((panel) => {
      panel.setAttribute("aria-busy", "true");
    });
    try {
      const response = await fetch(snapshotURL(), {
        signal: request.signal,
        headers: { Accept: "application/json" },
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      const panels = payload?.panels || {};
      Object.entries(panels).forEach(([name, markup]) => {
        const target = document.querySelector(`[data-panel="${name}"]`);
        if (target && typeof markup === "string") applyPanelHTML(target, markup);
      });
      const errors = payload?.errors || {};
      Object.keys(errors).forEach((name) => {
        const target = document.querySelector(`[data-panel="${name}"]`);
        target?.setAttribute("aria-busy", "false");
        target?.classList.add("is-stale");
      });
      snapshotFailures = 0;
      snapshotLoaded = true;
    } catch (error) {
      if (error.name === "AbortError") return;
      snapshotFailures += 1;
      document.querySelectorAll("[data-panel]").forEach((panel) => {
        panel.setAttribute("aria-busy", "false");
        panel.classList.add("is-stale");
      });
    } finally {
      if (snapshotRequest === request) {
        snapshotRequest = null;
        scheduleSnapshot();
      }
    }
  }

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      clearSnapshotTimer();
      snapshotRequest?.abort();
      snapshotRequest = null;
      return;
    }
    refreshSnapshot(true);
  });

  window.reliquaryFleetRefresh = () => refreshSnapshot(true);

  document.body.addEventListener("htmx:beforeRequest", (event) => {
    const target = panelForEvent(event);
    target?.setAttribute?.("aria-busy", "true");
  });

  document.body.addEventListener("htmx:beforeSwap", (event) => {
    const target = panelForEvent(event);
    if (!target?.classList) return;
    preservePanelScroll(target);
  });

  document.body.addEventListener("htmx:afterSwap", (event) => {
    const target = panelForEvent(event);
    if (!target) return;
    target.setAttribute?.("aria-busy", "false");
    target.classList?.remove("is-stale");
    enhanceTables(target);
    enhanceFleetRows(target);
    enhanceScrollableRegions(target);
    applyAttemptFilter(target);
    updateHealth(target);
    restorePanelScroll(target);
  });

  ["htmx:responseError", "htmx:sendError", "htmx:timeout"].forEach((name) => {
    document.body.addEventListener(name, (event) => {
      const target = panelForEvent(event);
      target?.setAttribute?.("aria-busy", "false");
      target?.classList?.add("is-stale");
    });
  });

  enhanceTables();
  enhanceFleetRows();
  enhanceScrollableRegions();
  applyAttemptFilter();
  setSound(false);
  refreshSnapshot(true);
})();
