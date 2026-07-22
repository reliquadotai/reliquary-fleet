import { spawn } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { launch as launchChrome } from "chrome-launcher";
import lighthouse from "lighthouse";

const root = path.resolve(import.meta.dirname, "..");
const port = 19092;
const url = `http://127.0.0.1:${port}/`;
const outputDirectory = path.join(root, ".lighthouseci");
const budget = JSON.parse(
  await readFile(path.join(root, "lighthouse-budget.json"), "utf8"),
);
const server = spawn(
  "python3",
  ["tests/ui_fixture_server.py", "--port", String(port)],
  { cwd: root, stdio: ["ignore", "ignore", "pipe"] },
);

async function waitUntilReady() {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    try {
      const response = await fetch(`${url}healthz`);
      if (response.ok) return;
    } catch {
      // The fixture is still binding its localhost socket.
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error("Fixture server did not become ready");
}

let chrome;
try {
  await waitUntilReady();
  chrome = await launchChrome({
    ...(process.env.CHROME_PATH ? { chromePath: process.env.CHROME_PATH } : {}),
    chromeFlags: ["--headless", "--no-sandbox", "--disable-dev-shm-usage"],
  });
  const result = await lighthouse(url, {
    port: chrome.port,
    output: "json",
    logLevel: "warn",
    onlyCategories: ["performance", "accessibility", "best-practices"],
    formFactor: "desktop",
    screenEmulation: {
      mobile: false,
      width: 1440,
      height: 900,
      deviceScaleFactor: 1,
      disabled: false,
    },
  });
  if (!result) throw new Error("Lighthouse did not return a report");
  if (result.lhr.runtimeError) {
    throw new Error(
      `Lighthouse runtime error: ${result.lhr.runtimeError.code}: ${result.lhr.runtimeError.message}`,
    );
  }
  await mkdir(outputDirectory, { recursive: true });
  await writeFile(path.join(outputDirectory, "lhr.json"), result.report);

  const failures = [];
  for (const [category, minimum] of Object.entries(budget)) {
    const score = result.lhr.categories[category]?.score;
    if (score == null) {
      failures.push(`${category} did not produce a score`);
      continue;
    }
    console.log(`${category}: ${score.toFixed(2)} (minimum ${minimum.toFixed(2)})`);
    if (score < minimum) failures.push(`${category} ${score.toFixed(2)} < ${minimum.toFixed(2)}`);
  }
  if (failures.length) {
    throw new Error(`Lighthouse budget failed: ${failures.join(", ")}`);
  }
} finally {
  await chrome?.kill();
  server.kill("SIGTERM");
}
