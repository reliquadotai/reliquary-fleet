# Changelog

All notable changes follow [Keep a Changelog](https://keepachangelog.com/) and
this project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- A normalized `reliquary_one` collector for all-in-one Code and Math miners,
  using one bounded read-only SSH probe per cycle and allowlisted structured
  telemetry.
- Operator-first service, checkpoint, GPU, pipeline, attempt, admission, and
  terminal-auction views with explicit pending and unknown states.
### Changed

- Updated Rich, Twine, Playwright, Axe, and the SHA-pinned PyPI publishing
  action to their reviewed current versions.
- Reworked the fleet dashboard around the active miner and compact recent
  history while retaining the existing validator, chain, EMA, and forensic
  panels.
- Deferred collapsed advanced diagnostics until operators open them, reducing
  initial rendering work while preserving the complete snapshot API by default.

### Security

- The Reliquary One adapter excludes wallet files, secret environments, raw
  prompts, request bodies, signatures, proofs, randomness, and exception
  contents from collection and export.
- Public tests and visual baselines use synthetic demo-only windows,
  checkpoints, timings, identities, and paths.

## [1.1.0] - 2026-07-23

### Added

- HTTP-only validator mode; SSH log tail and deployment fingerprint are now
  optional operator enhancements.
- Cacheable credential-free archive transport through `r2.public_base_url`.
- Explicit shared-upstream request budgets in `/healthz` and JSON export.
- Conditional log responses with `ETag`/`304 Not Modified`.

### Changed

- Consolidated 18 browser panel timers into one visibility-aware dashboard
  snapshot request, reducing visible-tab local traffic from 230 to 12 requests
  per minute and stopping it entirely while hidden.
- Split validator state, health, and verdict polling into independent bounded
  cadences. Verdict polling now retains a rolling cache and uses a two-minute
  overlap after the initial one-hour sync.
- Added pooled HTTP keep-alive connections, randomized startup, recurring
  jitter, exponential failure backoff, and validator `Retry-After` handling.
- Reduced archive cold-start work to eight objects per pass with two workers.
  Exhaustive R2 LIST fallback is now opt-in.
- Reduced validator log reconnect history to two minutes and added exponential
  reconnect backoff.

### Security

- Bounded validator response, compressed archive, and decompressed archive
  sizes.
- Validator origins and every config/disk/browser hotkey source are validated
  before they can enter HTTP or remote probe commands.
- Watch-cap omissions are visible in both `doctor` and the operator overview;
  configured fleet keys retain priority.
- General-user onboarding no longer requires or encourages distributing
  validator SSH or private R2 credentials.

## [1.0.2] - 2026-07-22

### Fixed

- Restored live chain/metagraph probes on hardened miner hosts where the
  Bittensor virtual environment is readable only through non-interactive sudo.

## [1.0.1] - 2026-07-22

### Added

- Deterministic, network-free demo data for responsive visual and release-image
  checks without exposing operator infrastructure.
- Keyboard-operable miner details with focus management, loading states, and
  accessible table labels.

### Changed

- Moved dashboard and logs styling and behavior into versioned local assets,
  with self-hosted Geist and JetBrains Mono fonts.
- Reworked the dashboard into independent desktop column stacks and an
  operator-first mobile order, with capped panel scrolling and sticky headings.
- Adopted Reliquary production tokens while keeping windows, percentages, and
  share bands on a distinct violet, cyan, rose, and green data palette.
- Alert sound is muted by default and only starts after explicit activation.
- The HTTP socket now becomes ready before remote collector threads start.

### Fixed

- Removed panel-height coupling that left large empty dashboard regions.
- Added visible stale-panel treatment, reduced-motion behavior, quiet
  scrollbars, and narrow-screen overflow protection.

## [1.0.0] - 2026-07-22

### Added

- Installable `reliquary-fleet` command with `init`, `doctor`, and `serve`.
- Private per-user config and state locations with source-checkout compatibility.
- Live Reliquary Math and Code lane health, checkpoint parity, validator truth,
  sealed-window rewards, chain/metagraph state, and exact reward EMA views.
- Responsive dense dashboard, independent scrolling regions, JSON export, and
  readiness endpoint.
- Reproducible wheel/sdist builds, CI, dependency auditing, and PyPI trusted
  publishing workflow.

### Security

- Localhost-only default with explicit acknowledgement required for remote binds.
- Owner-only config/cache files, atomic cache updates, and duplicate-instance lock.
- Same-origin header required for star mutations and restrictive browser headers.
- HTMX 2.0.10 is bundled and checksum-documented; no runtime CDN is required.

[1.1.0]: https://github.com/reliquadotai/reliquary-fleet/releases/tag/v1.1.0
[1.0.2]: https://github.com/reliquadotai/reliquary-fleet/releases/tag/v1.0.2
[1.0.1]: https://github.com/reliquadotai/reliquary-fleet/releases/tag/v1.0.1
[1.0.0]: https://github.com/reliquadotai/reliquary-fleet/releases/tag/v1.0.0
