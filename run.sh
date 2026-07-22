#!/usr/bin/env bash
# One-liner launcher for the fleet dashboard.
#
# Compatibility launcher for source checkouts. Installed users should run
# `reliquary-fleet serve` directly.
#
# On first run, copy the example config and edit it:
#   cp config.example.yaml config.yaml
#   $EDITOR config.yaml
#
set -euo pipefail

cd "$(dirname "$0")"

# Refuse to start when config.yaml is missing — the dashboard needs at
# minimum a validator URL + SSH host before any panel renders, and the
# error from inside python is uglier than this one.
if [[ ! -f config.yaml ]]; then
  echo "error: config.yaml not found." >&2
  echo "       cp config.example.yaml config.yaml && \$EDITOR config.yaml" >&2
  exit 1
fi

# Activate a venv if one exists, otherwise fall through to system python3.
if [[ -d .venv ]]; then
  # shellcheck source=/dev/null
  source .venv/bin/activate
fi

exec python3 -m reliquary_fleet serve \
  --config "$(pwd)/config.yaml" \
  --state-dir "$(pwd)/state" \
  "$@"
