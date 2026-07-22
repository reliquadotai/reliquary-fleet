import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

const mobileOrder = [
  "Window score",
  "Operations overview",
  "Window history",
  "Window forensics",
  "EMA leaderboard",
  "GPU and reference pipeline",
  "Frontier selection",
  "Fleet health",
];

test.beforeEach(async ({ page }) => {
  await page.goto("/");
  await page.waitForFunction(
    () =>
      document.querySelectorAll("main .panel").length >= 18 &&
      document.querySelectorAll(".skeleton-panel").length === 0,
  );
});

test("matches the sanitized viewport baseline", async ({ page }) => {
  await expect(page.locator('[hx-trigger*="every"]')).toHaveCount(0);
  await expect(page).toHaveScreenshot("dashboard-viewport.png");
});

test("keeps every panel contained and non-overlapping", async ({ page }, testInfo) => {
  const layout = await page.evaluate(() => {
    const root = document.documentElement;
    const nodes = Array.from(
      document.querySelectorAll(
        "main section.dashboard-area, .area-diagnostics .diag-column > div",
      ),
    );
    const boxes = nodes.map((node) => {
      const rect = node.getBoundingClientRect();
      return {
        label: node.getAttribute("aria-label") || node.className,
        left: rect.left,
        right: rect.right,
        top: rect.top + window.scrollY,
        bottom: rect.bottom + window.scrollY,
      };
    });
    const overlaps: string[] = [];
    for (let leftIndex = 0; leftIndex < boxes.length; leftIndex += 1) {
      for (let rightIndex = leftIndex + 1; rightIndex < boxes.length; rightIndex += 1) {
        const left = boxes[leftIndex];
        const right = boxes[rightIndex];
        const width = Math.min(left.right, right.right) - Math.max(left.left, right.left);
        const height = Math.min(left.bottom, right.bottom) - Math.max(left.top, right.top);
        if (width > 1 && height > 1) overlaps.push(`${left.label} / ${right.label}`);
      }
    }
    return {
      overflow: document.body.scrollWidth - root.clientWidth,
      overlaps,
      panelWidths: boxes.map((box) => box.right - box.left),
    };
  });

  expect(layout.overflow).toBeLessThanOrEqual(0);
  expect(layout.overlaps).toEqual([]);
  const minimumWidth = testInfo.project.name === "desktop-1440"
    ? 210
    : testInfo.project.name === "mobile-320"
      ? 295
      : 340;
  expect(Math.min(...layout.panelWidths)).toBeGreaterThanOrEqual(minimumWidth);
});

test("uses the operator-first order on narrow screens", async ({ page }, testInfo) => {
  test.skip(!testInfo.project.name.startsWith("mobile-"));
  const labels = await page.locator(".dashboard-core section").evaluateAll((areas) =>
    areas
      .map((area) => ({
        label: area.getAttribute("aria-label"),
        top: area.getBoundingClientRect().top + window.scrollY,
      }))
      .sort((left, right) => left.top - right.top)
      .map((area) => area.label),
  );
  expect(labels).toEqual(mobileOrder);
});

test("preserves internal panel scroll after an HTMX refresh", async ({ page }) => {
  const area = page.locator(".area-windows");
  const dimensions = await area.evaluate((element) => ({
    clientHeight: element.clientHeight,
    scrollHeight: element.scrollHeight,
  }));
  expect(dimensions.scrollHeight).toBeGreaterThan(dimensions.clientHeight);

  const restored = await area.evaluate(async (element) => {
    element.scrollTop = 72;
    await window.htmx.ajax("GET", element.getAttribute("hx-get") || "/api/windows", {
      source: element,
      target: element,
      swap: "innerHTML",
    });
    await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
    return element.scrollTop;
  });
  expect(restored).toBeGreaterThanOrEqual(65);
});

test("opens and closes miner details from the keyboard", async ({ page }) => {
  const trigger = page.locator(".fleet-detail-button").first();
  await trigger.scrollIntoViewIfNeeded();
  await trigger.focus();
  await trigger.press("Enter");

  const drawer = page.locator("#drawer");
  await expect(drawer).toHaveClass(/open/);
  await expect(drawer).toHaveAttribute("aria-hidden", "false");
  await expect(drawer).not.toHaveAttribute("inert", "");
  await expect(page.locator("[data-drawer-close]")).toBeFocused();

  await page.keyboard.press("Escape");
  await expect(drawer).not.toHaveClass(/open/);
  await expect(drawer).toHaveAttribute("aria-hidden", "true");
  await expect(drawer).toHaveAttribute("inert", "");
  await expect(trigger).toBeFocused();
});

test("keeps alert audio opt-in", async ({ page }) => {
  const sound = page.locator("#sound-toggle");
  await expect(sound).toHaveAttribute("aria-pressed", "false");
  await expect(sound).toContainText("Sound off");
  await sound.click();
  await expect(sound).toHaveAttribute("aria-pressed", "true");
  await expect(sound).toContainText("Sound on");
});

test("loads without browser console errors", async ({ page }) => {
  const errors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(message.text());
  });
  page.on("pageerror", (error) => errors.push(error.message));
  await page.reload();
  await page.waitForFunction(
    () =>
      document.querySelectorAll("main .panel").length >= 18 &&
      document.querySelectorAll(".skeleton-panel").length === 0,
  );
  expect(errors).toEqual([]);
});

test("honors reduced motion", async ({ page }) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  const duration = await page.locator(".pulse").evaluate(
    (element) => getComputedStyle(element).animationDuration,
  );
  expect(Number.parseFloat(duration)).toBeLessThanOrEqual(0.001);
});

test("has no serious automated accessibility violations", async ({ page }) => {
  const results = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"])
    .analyze();
  const serious = results.violations.filter((violation) =>
    ["serious", "critical"].includes(violation.impact || ""),
  );
  expect(serious).toEqual([]);
});

test("keeps the logs surface contained", async ({ page }) => {
  await page.goto("/logs");
  await page.waitForSelector(".log-table");
  const overflow = await page.evaluate(
    () => document.body.scrollWidth - document.documentElement.clientWidth,
  );
  expect(overflow).toBeLessThanOrEqual(0);
  await expect(page.locator(".kind-pill").first()).toHaveAttribute(
    "aria-pressed",
    "true",
  );
});
