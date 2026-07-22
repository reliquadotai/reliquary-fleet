# Contributing

## Development setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
```

Run the release checks before opening a pull request:

```bash
ruff check reliquary_fleet settings.py starred.py r2_query.py
pytest
python -m build
twine check dist/*
pip-audit . --strict
```

Keep changes focused, add tests for behavior changes, and never commit a live
`config.yaml`, `.env`, SSH key, wallet material, prompt, or completion. Use only
synthetic hostnames, hotkeys, and telemetry in fixtures and screenshots.

For dashboard changes, verify both desktop and narrow mobile layouts and confirm
that panel refreshes preserve scroll position.
