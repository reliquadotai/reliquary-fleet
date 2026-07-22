"""Filesystem locations for installed and source-checkout operation."""

from __future__ import annotations

import os
from pathlib import Path

from platformdirs import user_config_path, user_state_path

APP_NAME = "reliquary-fleet"
CONFIG_ENV = "RELIQUARY_FLEET_CONFIG"
STATE_ENV = "RELIQUARY_FLEET_STATE_DIR"


def _expanded(path: str | os.PathLike[str]) -> Path:
    expanded = os.path.expandvars(os.path.expanduser(os.fspath(path)))
    # abspath normalizes a relative path without dereferencing its final
    # component. Config creation can therefore detect and reject a symlink.
    return Path(os.path.abspath(expanded))


def user_config_file() -> Path:
    return Path(user_config_path(APP_NAME, appauthor=False)) / "config.yaml"


def user_state_dir() -> Path:
    return Path(user_state_path(APP_NAME, appauthor=False))


def resolve_config(explicit: str | os.PathLike[str] | None = None) -> Path:
    """Resolve config while preserving the original checkout workflow."""
    if explicit:
        return _expanded(explicit)
    if configured := os.environ.get(CONFIG_ENV):
        return _expanded(configured)
    legacy = Path.cwd() / "config.yaml"
    if legacy.is_file():
        return legacy.resolve()
    return user_config_file()


def resolve_state_dir(explicit: str | os.PathLike[str] | None = None) -> Path:
    if explicit:
        return _expanded(explicit)
    if configured := os.environ.get(STATE_ENV):
        return _expanded(configured)
    return user_state_dir()
