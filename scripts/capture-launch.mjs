import { spawn } from "node:child_process";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { chromium } from "@playwright/test";

const root = path.resolve(import.meta.dirname, "..");
const port = 19093;
const url = `http://127.0.0.1:${port}`;
const destination = path.join(
  root,
  "docs",
  "assets",
  "reliquary-fleet-v1.0.1-x.png",
);

const server = spawn(
  "python3",
  ["tests/ui_fixture_server.py", "--port", String(port)],
  { cwd: root, stdio: ["ignore", "ignore", "pipe"] },
);

async function waitUntilReady() {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    try {
      const response = await fetch(`${url}/healthz`);
      if (response.ok) return;
    } catch {
      // The local fixture is still binding its socket.
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error("Fixture server did not become ready");
}

let browser;
try {
  await waitUntilReady();
  browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    viewport: { width: 1600, height: 900 },
    colorScheme: "dark",
    locale: "en-US",
    timezoneId: "UTC",
    deviceScaleFactor: 1,
  });
  const page = await context.newPage();
  await page.goto(url);
  await page.waitForFunction(
    () =>
      document.querySelectorAll("main .panel").length >= 18 &&
      document.querySelectorAll(".skeleton-panel").length === 0,
  );
  await mkdir(path.dirname(destination), { recursive: true });
  await page.screenshot({
    path: destination,
    animations: "disabled",
    caret: "hide",
    fullPage: false,
  });
  console.log(destination);
} finally {
  await browser?.close();
  server.kill("SIGTERM");
}
