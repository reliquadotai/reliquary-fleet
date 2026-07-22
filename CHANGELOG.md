# Changelog

All notable changes follow [Keep a Changelog](https://keepachangelog.com/) and
this project uses [Semantic Versioning](https://semver.org/).

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

[1.0.0]: https://github.com/reliquadotai/reliquary-fleet/releases/tag/v1.0.0
