"""Persistent star state for hotkeys.

Lives outside fleet.py + settings.py because the lifecycle is different:
config.yaml is read-only at runtime, but the star set is mutated by
the dashboard's UI (click a ★ in the EMA leaderboard) and needs to
survive restarts.

File on disk: `state/starred.json` — a sorted list of SS58 hotkeys.
Gitignored. Created on first write; reads return the empty set if
absent so the dashboard never errors out on a fresh clone.
"""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
import threading
from contextlib import suppress
from pathlib import Path

_STATE_DIR = Path(os.environ.get("RELIQUARY_FLEET_STATE_DIR", "state")).expanduser()
_STARRED_FILE = _STATE_DIR / "starred.json"

_LOCK = threading.Lock()
_SS58_HOTKEY_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{40,64}")
# Hotkey prefix length used by the legacy event matcher (`hk[:12]`).
# Storing full SS58 here — the dashboard truncates for the prefix
# comparison at use time.


def _valid_hotkey(value: object) -> str:
    hotkey = str(value or "").strip()
    return hotkey if _SS58_HOTKEY_RE.fullmatch(hotkey) else ""


def configure_state_dir(path: str | Path) -> None:
    """Redirect star persistence before the first read or write."""
    global _STATE_DIR, _STARRED_FILE
    _STATE_DIR = Path(path).expanduser().resolve()
    _STARRED_FILE = _STATE_DIR / "starred.json"


def _read() -> set[str]:
    if not _STARRED_FILE.exists():
        return set()
    try:
        info = _STARRED_FILE.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_size > 1_000_000:
            return set()
        raw = json.loads(_STARRED_FILE.read_text())
        if isinstance(raw, list):
            return {
                hotkey
                for value in raw
                if (hotkey := _valid_hotkey(value))
            }
        return set()
    except (json.JSONDecodeError, OSError):
        return set()


def _write(hotkeys: set[str]) -> None:
    _STATE_DIR.mkdir(parents=True, mode=0o700, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix="starred-",
        suffix=".tmp",
        dir=_STATE_DIR,
    )
    tmp = Path(temporary)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(sorted(hotkeys), handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        tmp.chmod(0o600)
        tmp.replace(_STARRED_FILE)
    finally:
        with suppress(FileNotFoundError):
            tmp.unlink()
    _STARRED_FILE.chmod(0o600)


def load() -> set[str]:
    """Read-only snapshot of the current starred set."""
    with _LOCK:
        return _read()


def add(hotkey: str) -> set[str]:
    """Mark a hotkey as starred. Returns the new full set."""
    hotkey = _valid_hotkey(hotkey)
    if not hotkey:
        return load()
    with _LOCK:
        current = _read()
        current.add(hotkey)
        _write(current)
        return current


def remove(hotkey: str) -> set[str]:
    """Unstar a hotkey. Returns the new full set."""
    hotkey = _valid_hotkey(hotkey)
    if not hotkey:
        return load()
    with _LOCK:
        current = _read()
        current.discard(hotkey)
        _write(current)
        return current


def toggle(hotkey: str) -> tuple[bool, set[str]]:
    """Flip the star for a hotkey. Returns (now_starred, full_set)."""
    hotkey = _valid_hotkey(hotkey)
    if not hotkey:
        return (False, load())
    with _LOCK:
        current = _read()
        if hotkey in current:
            current.remove(hotkey)
            starred = False
        else:
            current.add(hotkey)
            starred = True
        _write(current)
        return (starred, current)
