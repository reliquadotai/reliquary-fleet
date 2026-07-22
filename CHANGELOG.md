# Changelog

All notable changes follow [Keep a Changelog](https://keepachangelog.com/) and
this project uses [Semantic Versioning](https://semver.org/).

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

[1.0.1]: https://github.com/reliquadotai/reliquary-fleet/releases/tag/v1.0.1
[1.0.0]: https://github.com/reliquadotai/reliquary-fleet/releases/tag/v1.0.0
