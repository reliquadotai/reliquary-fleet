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
    window.htmx.process(area);
    window.htmx.trigger(area, "load");
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
    window.htmx.process(area);
    window.htmx.trigger(area, "load");
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
        [".area-ema", ".area-summary", ".area-score"].forEach((selector) => {
          const area = document.querySelector(selector);
          if (area) window.htmx.trigger(area, "load");
        });
      })
      .catch((error) => console.warn("Star toggle failed", error))
      .finally(() => {
        button.disabled = false;
      });
  });

  const scrollableClasses = new Set([
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

  document.body.addEventListener("htmx:beforeRequest", (event) => {
    const target = panelForEvent(event);
    target?.setAttribute?.("aria-busy", "true");
  });

  document.body.addEventListener("htmx:beforeSwap", (event) => {
    const target = panelForEvent(event);
    if (!target?.classList) return;
    if (Array.from(target.classList).some((name) => scrollableClasses.has(name))) {
      target.dataset.savedScroll = String(target.scrollTop);
    }
  });

  document.body.addEventListener("htmx:afterSwap", (event) => {
    const target = panelForEvent(event);
    if (!target) return;
    target.setAttribute?.("aria-busy", "false");
    target.classList?.remove("is-stale");
    enhanceTables(target);
    enhanceFleetRows(target);
    enhanceScrollableRegions(target);
    updateHealth(target);
    if (target.dataset && Object.hasOwn(target.dataset, "savedScroll")) {
      requestAnimationFrame(() => {
        const top = Number.parseInt(target.dataset.savedScroll, 10);
        if (!Number.isNaN(top)) target.scrollTop = top;
        delete target.dataset.savedScroll;
      });
    }
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
  setSound(false);
})();
