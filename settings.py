"""Config loader for the fleet dashboard.

Reads `config.yaml` from the cwd (or a path passed to `load()`) and
exposes a mutable singleton that the fleet/web modules read from at
runtime. Env-var overrides are applied at load time for the secrets
that should never live in yaml.

The historical `fleet.py` module shipped these values as top-level
constants. Rather than rewriting every reference, we expose a
`apply_to_fleet_module()` helper that mutates the legacy globals
in-place after the config is loaded. New code should reach for
`SETTINGS` directly.
"""

from __future__ import annotations

import math
import os
import re
import shlex
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

try:
    import yaml
except ImportError as e:  # pragma: no cover — surfaces immediately
    raise SystemExit(
        "missing dep: pip install PyYAML\n(or: pip install -r requirements.txt)"
    ) from e


@dataclass
class FleetUnit:
    """One explicitly allowed systemd unit for a logical fleet box."""

    unit: str
    env_file: str | None = None


@dataclass
class FleetBox:
    """Mirror of one entry under `fleet:` in config.yaml."""

    alias: str
    hotkey: str
    label: str
    color: str
    unit: str | None = None
    # Explicit environment file used by the configured systemd unit.  The
    # reference canary uses ``miner-pro.env`` while templated legacy units use
    # ``miner-pro-<instance>.env``; guessing from the unit name is therefore
    # not reliable.
    env_file: str | None = None
    # Additional service identities that may replace ``unit`` on the same
    # logical box. They are an allowlist, not discovery hints: the poller
    # rejects concurrent entries unless their exact set is also declared in
    # ``coordinated_units``, and rejects every active instance outside it.
    allowed_units: list[FleetUnit] = field(default_factory=list)
    # Exact set of services permitted to be active together. Empty preserves
    # the fail-closed exactly-one-active behavior. A non-empty set must name at
    # least two units already present in ``unit`` + ``allowed_units``.
    coordinated_units: list[str] = field(default_factory=list)
    # Per-box override for the SSH key. Falls back to the validator
    # SSH key (which the legacy fleet.py also used as the global key).
    ssh_key: str | None = None


@dataclass
class LabBox:
    """One explicitly submit-disabled accelerator laboratory.

    Labs are deliberately separate from ``FleetBox``: they have no hotkey and
    therefore cannot enter owned-hotkey, submission, reward, or miner-readiness
    accounting.  The dashboard reads only host telemetry and the dedicated
    offline evidence database and/or one immutable selector artifact manifest.
    """

    alias: str
    label: str
    color: str
    unit: str = ""
    evidence_db: str = ""
    selector_artifact_manifest: str = ""
    source_manifest: str = "/srv/reliquary-miner-pro/state/source-manifest.env"
    ssh_key: str | None = None


@dataclass
class Settings:
    """Mutable singleton of all operator-tunable values.

    Populated by `load()`. Reading any field before `load()` runs
    yields the empty defaults — fine for `import fleet` at module-load
    time, broken for any panel render that actually wants data.
    """

    # Validator under test
    validator_url: str = ""
    validator_ssh_host: str = ""
    validator_ssh_port: int = 22
    validator_ssh_key: str = ""
    validator_container: str = "reliquary-trainer"

    # Fleet of miner boxes
    fleet: list[FleetBox] = field(default_factory=list)
    # Wallet-free, submit-disabled accelerator labs. These are observability
    # rows only and never contribute to the fleet's hotkey set or readiness.
    labs: list[LabBox] = field(default_factory=list)

    # Source-pinned Code grader/readiness surface. These are public host-local
    # service paths, never wallet or R2 credentials. The dashboard uses the
    # same defaults as the miner provisioning scripts while allowing a
    # deliberate operator override for non-standard installs.
    code_grader_unit: str = "reliquary-code-grader.service"
    code_grader_socket: str = "/tmp/reliquary-grader.sock"
    code_grader_bundle_link: str = "/opt/reliquary-code-grader/current"
    code_grader_metrics_port: int = 9876
    code_grader_socket_mode: int = 0o660

    # Hotkeys to treat as "yours" beyond the fleet (loaded from config
    # at startup; the live starred-set persists separately in
    # state/starred.json and is merged in fleet.py at use time).
    starred_hotkeys_seed: list[str] = field(default_factory=list)
    starred_hotkey_labels: dict[str, str] = field(default_factory=dict)

    # Subnet
    netuid: int = 81

    # R2 read access
    r2_endpoint: str = ""
    r2_bucket: str = "reliquary"
    r2_access_key_id: str = ""
    r2_secret_access_key: str = ""
    # Optional cacheable HTTPS origin for immutable archive objects. This is
    # preferred for broad distribution because S3 API requests bypass
    # Cloudflare's edge cache.
    r2_public_base_url: str = ""
    # Exhaustive bucket LIST is intentionally opt-in. A missing validator
    # archive index should serve the last-good cache instead of scanning an
    # entire shared prefix from every installation.
    r2_allow_list_fallback: bool = False
    r2_fetch_batch_size: int = 8
    r2_fetch_workers: int = 2
    r2_refresh_seconds: float = 60.0

    # Dashboard runtime
    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 9091
    refresh_seconds: float = 5.0
    # Maximum age of the last complete atomic probe generation before
    # readiness fails.  This is deliberately separate from refresh_seconds:
    # one generation includes bounded SSH and validator probes that can take
    # substantially longer than the idle delay between generations.
    health_poll_stale_seconds: float = 60.0
    history_windows: int = 216

    # Shared-upstream budgets. Miner SSH still follows refresh_seconds because
    # each operator owns that traffic; validator and chain reads are bounded
    # independently so opening Fleet cannot multiply a shared service at the
    # UI cadence.
    validator_state_seconds: float = 15.0
    validator_health_seconds: float = 30.0
    validator_verdict_seconds: float = 60.0
    max_verdict_hotkeys: int = 8
    chain_refresh_seconds: float = 300.0
    rtt_refresh_seconds: float = 120.0
    startup_jitter_seconds: float = 30.0


# Mutable singleton. Importers can `from settings import SETTINGS`.
SETTINGS = Settings()

_SYSTEMD_UNIT_RE = re.compile(r"^[A-Za-z0-9_.@-]+$")
_MAX_CONFIG_BYTES = 1_000_000


def _mapping(value: Any, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a mapping")
    return value


def _list(value: Any, field_name: str) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return value


def _ssh_host(value: Any, field_name: str) -> str:
    target = str(value or "").strip()
    if target and (target.startswith("-") or any(char.isspace() for char in target)):
        raise ValueError(f"{field_name} must be one SSH host token")
    return target


def _absolute_path(value: Any, field_name: str) -> str:
    path = _expand(str(value or "").strip())
    if not path or not Path(path).is_absolute():
        raise ValueError(f"{field_name} must be an absolute path")
    return path


def _socket_mode(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("code_readiness.grader_socket_mode must be an octal mode")
    if isinstance(value, int):
        mode = value
    else:
        raw = str(value or "").strip()
        if not re.fullmatch(r"[0-7]{3,4}", raw):
            raise ValueError("code_readiness.grader_socket_mode must be an octal mode")
        mode = int(raw, 8)
    if not 0 <= mode <= 0o777:
        raise ValueError("code_readiness.grader_socket_mode must be an octal mode")
    return mode


def positive_finite_seconds(value: Any, field_name: str) -> float:
    """Parse one bounded-cadence setting without accepting NaN or infinity."""
    try:
        seconds = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a finite positive number") from exc
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError(f"{field_name} must be a finite positive number")
    return seconds


def _bounded_seconds(
    value: Any,
    field_name: str,
    *,
    minimum: float,
    maximum: float,
) -> float:
    seconds = positive_finite_seconds(value, field_name)
    if not minimum <= seconds <= maximum:
        raise ValueError(
            f"{field_name} must be between {minimum:g} and {maximum:g} seconds"
        )
    return seconds


def _bounded_int(
    value: Any,
    field_name: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if not minimum <= parsed <= maximum:
        raise ValueError(
            f"{field_name} must be in {minimum}..{maximum}"
        )
    return parsed


def _bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be true or false")
    return value


def _public_base_url(value: Any) -> str:
    url = str(value or "").strip().rstrip("/")
    if not url:
        return ""
    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "r2.public_base_url must be a credential-free http(s) base URL"
        )
    return url


def _validator_base_url(value: Any) -> str:
    url = str(value or "").strip().rstrip("/")
    if not url:
        return ""
    parsed = urlsplit(url)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("validator.url has an invalid port") from exc
    hostname = str(parsed.hostname or "")
    if (
        parsed.scheme not in {"http", "https"}
        or not hostname
        or not re.fullmatch(r"[A-Za-z0-9._:-]+", hostname)
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise ValueError(
            "validator.url must be a credential-free http(s) origin"
        )
    return url


def _ss58_hotkey(value: Any, field_name: str) -> str:
    hotkey = str(value or "").strip()
    if not re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{40,64}", hotkey):
        raise ValueError(f"{field_name} must be a full SS58 value")
    return hotkey


def _expand(path: str) -> str:
    """`~` / `$HOME` expansion + empty-string passthrough."""
    if not path:
        return ""
    return os.path.expanduser(os.path.expandvars(path))


def _coerce_fleet_box(d: dict[str, Any]) -> FleetBox:
    if not isinstance(d, dict):
        raise ValueError("fleet entries must be mappings")
    primary_unit = str(d.get("unit") or "").strip() or None
    raw_allowed = d.get("allowed_units")
    if raw_allowed is None:
        raw_allowed = []
    if not isinstance(raw_allowed, list):
        raise ValueError("fleet.allowed_units must be a list")

    def unit_key(unit: str) -> str:
        if not _SYSTEMD_UNIT_RE.fullmatch(unit):
            raise ValueError(f"invalid fleet systemd unit: {unit!r}")
        return unit if unit.endswith(".service") else f"{unit}.service"

    allowed_units: list[FleetUnit] = []
    seen_units: set[str] = {unit_key(primary_unit)} if primary_unit else set()
    for raw in raw_allowed:
        if isinstance(raw, str):
            unit = raw.strip()
            env_file = None
        elif isinstance(raw, dict):
            unit = str(raw.get("unit") or "").strip()
            env_file = _expand(str(raw.get("env_file") or "")) or None
        else:
            raise ValueError("fleet.allowed_units entries must be strings or mappings")
        if not unit:
            raise ValueError("fleet.allowed_units entries require a non-empty unit")
        canonical = unit_key(unit)
        if canonical in seen_units:
            raise ValueError(f"duplicate fleet allowed unit: {unit}")
        seen_units.add(canonical)
        allowed_units.append(FleetUnit(unit=unit, env_file=env_file))

    raw_coordinated = d.get("coordinated_units")
    if raw_coordinated is None:
        raw_coordinated = []
    if not isinstance(raw_coordinated, list):
        raise ValueError("fleet.coordinated_units must be a list")
    coordinated_units: list[str] = []
    coordinated_seen: set[str] = set()
    for raw_unit in raw_coordinated:
        canonical = unit_key(str(raw_unit or "").strip())
        if canonical not in seen_units:
            raise ValueError(f"fleet coordinated unit is not allowed: {canonical}")
        if canonical in coordinated_seen:
            raise ValueError(f"duplicate fleet coordinated unit: {canonical}")
        coordinated_seen.add(canonical)
        coordinated_units.append(canonical)
    if coordinated_units and len(coordinated_units) < 2:
        raise ValueError("fleet.coordinated_units requires at least two units")

    return FleetBox(
        alias=_ssh_host(d.get("alias", ""), "fleet.alias"),
        hotkey=str(d.get("hotkey", "")).strip(),
        label=str(d.get("label", "")).strip(),
        color=str(d.get("color", "white")).strip(),
        unit=primary_unit,
        env_file=_expand(str(d.get("env_file") or "")) or None,
        allowed_units=allowed_units,
        coordinated_units=coordinated_units,
        ssh_key=d.get("ssh_key") or None,
    )


def _coerce_lab_box(d: dict[str, Any]) -> LabBox:
    if not isinstance(d, dict):
        raise ValueError("labs entries must be mappings")
    if str(d.get("hotkey") or "").strip():
        raise ValueError("labs entries must not define a hotkey")

    alias = _ssh_host(d.get("alias"), "labs.alias")
    label = str(d.get("label") or "").strip()
    unit = str(d.get("unit") or "").strip()
    evidence_db_raw = d.get("evidence_db")
    artifact_raw = d.get("selector_artifact_manifest")
    if not alias:
        raise ValueError("labs entries require a non-empty alias")
    if not label:
        raise ValueError("labs entries require a non-empty label")
    if unit and not _SYSTEMD_UNIT_RE.fullmatch(unit):
        raise ValueError("labs entries require a valid systemd unit")
    if unit and not unit.endswith(".service"):
        unit = f"{unit}.service"
    evidence_db = (
        _absolute_path(evidence_db_raw, "labs.evidence_db")
        if evidence_db_raw not in (None, "")
        else ""
    )
    selector_artifact_manifest = (
        _absolute_path(
            artifact_raw,
            "labs.selector_artifact_manifest",
        )
        if artifact_raw not in (None, "")
        else ""
    )
    if not evidence_db and not selector_artifact_manifest:
        raise ValueError(
            "labs entries require evidence_db or selector_artifact_manifest"
        )

    return LabBox(
        alias=alias,
        label=label,
        color=str(d.get("color", "white") or "white").strip(),
        unit=unit,
        evidence_db=evidence_db,
        selector_artifact_manifest=selector_artifact_manifest,
        source_manifest=_absolute_path(
            d.get(
                "source_manifest",
                "/srv/reliquary-miner-pro/state/source-manifest.env",
            ),
            "labs.source_manifest",
        ),
        ssh_key=d.get("ssh_key") or None,
    )


def load(path: Path | str = "config.yaml") -> Settings:
    """Load config.yaml + env overrides into the SETTINGS singleton."""
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(
            f"{cfg_path} not found. "
            f"Copy config.example.yaml -> {cfg_path} and edit before running."
        )
    if not cfg_path.is_file():
        raise ValueError(f"{cfg_path} must be a regular config file")
    if cfg_path.stat().st_size > _MAX_CONFIG_BYTES:
        raise ValueError(f"{cfg_path} exceeds the 1 MB config limit")
    try:
        raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"invalid YAML in {cfg_path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError("config root must be a mapping")

    v = _mapping(raw.get("validator"), "validator")
    vssh = _mapping(v.get("ssh"), "validator.ssh")
    SETTINGS.validator_url = _validator_base_url(v.get("url"))
    SETTINGS.validator_ssh_host = _ssh_host(
        vssh.get("host"),
        "validator.ssh.host",
    )
    try:
        SETTINGS.validator_ssh_port = int(vssh.get("port", 22))
    except (TypeError, ValueError) as exc:
        raise ValueError("validator.ssh.port must be an integer") from exc
    if not 1 <= SETTINGS.validator_ssh_port <= 65535:
        raise ValueError("validator.ssh.port must be in 1..65535")
    SETTINGS.validator_ssh_key = _expand(str(vssh.get("key") or "~/.ssh/id_ed25519"))
    SETTINGS.validator_container = str(
        v.get("container") or "reliquary-trainer"
    ).strip()

    SETTINGS.fleet = [_coerce_fleet_box(b) for b in _list(raw.get("fleet"), "fleet")]
    SETTINGS.labs = [_coerce_lab_box(b) for b in _list(raw.get("labs"), "labs")]
    labels = [box.label for box in SETTINGS.fleet] + [
        lab.label for lab in SETTINGS.labs
    ]
    if len(labels) != len(set(labels)):
        raise ValueError("fleet and labs labels must be unique")
    # Default per-box ssh_key falls back to the validator key. Doing
    # this at config-load time means every downstream call to a box
    # can use box.ssh_key without re-running the fallback logic.
    for box in SETTINGS.fleet:
        if not box.ssh_key:
            box.ssh_key = SETTINGS.validator_ssh_key
        else:
            box.ssh_key = _expand(box.ssh_key)
    for lab in SETTINGS.labs:
        if not lab.ssh_key:
            lab.ssh_key = SETTINGS.validator_ssh_key
        else:
            lab.ssh_key = _expand(lab.ssh_key)

    code = _mapping(raw.get("code_readiness"), "code_readiness")
    code_grader_unit = str(
        code.get("grader_unit", "reliquary-code-grader.service") or ""
    ).strip()
    if not _SYSTEMD_UNIT_RE.fullmatch(code_grader_unit):
        raise ValueError("code_readiness.grader_unit is not a valid systemd unit")
    SETTINGS.code_grader_unit = code_grader_unit
    SETTINGS.code_grader_socket = _absolute_path(
        code.get("grader_socket", "/tmp/reliquary-grader.sock"),
        "code_readiness.grader_socket",
    )
    SETTINGS.code_grader_bundle_link = _absolute_path(
        code.get("grader_bundle_link", "/opt/reliquary-code-grader/current"),
        "code_readiness.grader_bundle_link",
    )
    try:
        SETTINGS.code_grader_metrics_port = int(code.get("grader_metrics_port", 9876))
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "code_readiness.grader_metrics_port must be an integer"
        ) from exc
    if not 1 <= SETTINGS.code_grader_metrics_port <= 65535:
        raise ValueError("code_readiness.grader_metrics_port must be in 1..65535")
    SETTINGS.code_grader_socket_mode = _socket_mode(
        code.get("grader_socket_mode", "0660")
    )

    SETTINGS.starred_hotkeys_seed = []
    SETTINGS.starred_hotkey_labels = {}
    for item in _list(raw.get("starred_hotkeys"), "starred_hotkeys"):
        if isinstance(item, dict):
            hk = str(item.get("hotkey") or item.get("ss58") or "").strip()
            label = str(item.get("label") or "").strip()
        else:
            hk = str(item).strip()
            label = ""
        if not hk:
            continue
        hk = _ss58_hotkey(hk, "starred_hotkeys entry")
        SETTINGS.starred_hotkeys_seed.append(hk)
        if label:
            SETTINGS.starred_hotkey_labels[hk] = label

    try:
        SETTINGS.netuid = int(raw.get("netuid", 81))
    except (TypeError, ValueError) as exc:
        raise ValueError("netuid must be an integer") from exc
    if SETTINGS.netuid < 1:
        raise ValueError("netuid must be positive")

    # R2 — env wins over yaml. The legacy fleet.py read every value
    # from env, so this keeps that contract while letting yaml act as
    # a per-machine default.
    r2 = _mapping(raw.get("r2"), "r2")
    SETTINGS.r2_endpoint = str(
        os.environ.get("R2_ENDPOINT", r2.get("endpoint", "")) or ""
    ).strip()
    SETTINGS.r2_bucket = os.environ.get("R2_BUCKET", r2.get("bucket", "reliquary"))
    SETTINGS.r2_bucket = str(SETTINGS.r2_bucket or "").strip()
    SETTINGS.r2_access_key_id = os.environ.get(
        "AWS_ACCESS_KEY_ID", r2.get("access_key_id", "")
    )
    SETTINGS.r2_access_key_id = str(SETTINGS.r2_access_key_id or "").strip()
    SETTINGS.r2_secret_access_key = os.environ.get(
        "AWS_SECRET_ACCESS_KEY", r2.get("secret_access_key", "")
    )
    SETTINGS.r2_secret_access_key = str(SETTINGS.r2_secret_access_key or "").strip()
    SETTINGS.r2_public_base_url = _public_base_url(
        os.environ.get("R2_PUBLIC_BASE_URL", r2.get("public_base_url", ""))
    )
    SETTINGS.r2_allow_list_fallback = _bool(
        r2.get("allow_list_fallback", False),
        "r2.allow_list_fallback",
    )
    SETTINGS.r2_fetch_batch_size = _bounded_int(
        r2.get("fetch_batch_size", 8),
        "r2.fetch_batch_size",
        minimum=1,
        maximum=32,
    )
    SETTINGS.r2_fetch_workers = _bounded_int(
        r2.get("fetch_workers", 2),
        "r2.fetch_workers",
        minimum=1,
        maximum=8,
    )
    if SETTINGS.r2_fetch_workers > SETTINGS.r2_fetch_batch_size:
        raise ValueError("r2.fetch_workers must not exceed r2.fetch_batch_size")
    SETTINGS.r2_refresh_seconds = _bounded_seconds(
        r2.get("refresh_seconds", 60),
        "r2.refresh_seconds",
        minimum=30,
        maximum=3600,
    )

    dash = _mapping(raw.get("dashboard"), "dashboard")
    SETTINGS.dashboard_host = str(dash.get("host") or "127.0.0.1").strip()
    try:
        SETTINGS.dashboard_port = int(dash.get("port", 9091))
    except (TypeError, ValueError) as exc:
        raise ValueError("dashboard.port must be an integer") from exc
    if not 1 <= SETTINGS.dashboard_port <= 65535:
        raise ValueError("dashboard.port must be in 1..65535")
    SETTINGS.refresh_seconds = positive_finite_seconds(
        dash.get("refresh_seconds", 5.0),
        "dashboard.refresh_seconds",
    )
    SETTINGS.health_poll_stale_seconds = positive_finite_seconds(
        dash.get("health_poll_stale_seconds", 60.0),
        "dashboard.health_poll_stale_seconds",
    )
    try:
        SETTINGS.history_windows = int(dash.get("history_windows", 216))
    except (TypeError, ValueError) as exc:
        raise ValueError("dashboard.history_windows must be an integer") from exc
    if not 1 <= SETTINGS.history_windows <= 10_000:
        raise ValueError("dashboard.history_windows must be in 1..10000")

    upstream = _mapping(raw.get("upstream"), "upstream")
    SETTINGS.validator_state_seconds = _bounded_seconds(
        upstream.get("validator_state_seconds", 15),
        "upstream.validator_state_seconds",
        minimum=10,
        maximum=300,
    )
    SETTINGS.validator_health_seconds = _bounded_seconds(
        upstream.get("validator_health_seconds", 30),
        "upstream.validator_health_seconds",
        minimum=30,
        maximum=600,
    )
    SETTINGS.validator_verdict_seconds = _bounded_seconds(
        upstream.get("validator_verdict_seconds", 60),
        "upstream.validator_verdict_seconds",
        minimum=30,
        maximum=600,
    )
    SETTINGS.max_verdict_hotkeys = _bounded_int(
        upstream.get("max_verdict_hotkeys", 8),
        "upstream.max_verdict_hotkeys",
        minimum=1,
        maximum=64,
    )
    SETTINGS.chain_refresh_seconds = _bounded_seconds(
        upstream.get("chain_refresh_seconds", 300),
        "upstream.chain_refresh_seconds",
        minimum=120,
        maximum=3600,
    )
    SETTINGS.rtt_refresh_seconds = _bounded_seconds(
        upstream.get("rtt_refresh_seconds", 120),
        "upstream.rtt_refresh_seconds",
        minimum=60,
        maximum=1800,
    )
    SETTINGS.startup_jitter_seconds = _bounded_seconds(
        upstream.get("startup_jitter_seconds", 30),
        "upstream.startup_jitter_seconds",
        minimum=1,
        maximum=300,
    )

    return SETTINGS


def apply_to_fleet_module() -> None:
    """Patch the legacy `fleet` module globals from SETTINGS.

    Called by fleet_web.py after `load()`. Lets the historical
    references in fleet.py (`FLEET`, `OUR_SS58`, `VALIDATOR_URL`, etc.)
    work unchanged against operator-provided values.
    """
    import fleet  # local import — avoids circular cost when settings
    # is imported standalone (e.g. unit tests).

    def _ssh_target(box: FleetBox | LabBox) -> str:
        if box.ssh_key:
            return f"-i {shlex.quote(box.ssh_key)} -o IdentitiesOnly=yes {box.alias}"
        return box.alias

    fleet.FLEET = [
        (_ssh_target(b), b.hotkey, b.label, b.color, b.unit) for b in SETTINGS.fleet
    ]
    fleet.LABS = [
        (
            _ssh_target(b),
            b.label,
            b.color,
            b.unit,
            b.evidence_db,
            b.source_manifest,
            b.selector_artifact_manifest,
        )
        for b in SETTINGS.labs
    ]
    fleet.FLEET_ENV_FILES = {
        b.label: b.env_file for b in SETTINGS.fleet if b.label and b.env_file
    }
    fleet.FLEET_UNIT_CANDIDATES = {
        b.label: [
            *([(str(b.unit), str(b.env_file or ""))] if b.unit else []),
            *[(u.unit, str(u.env_file or "")) for u in b.allowed_units],
        ]
        for b in SETTINGS.fleet
        if b.label and b.allowed_units
    }
    fleet.FLEET_COORDINATED_UNITS = {
        b.label: list(b.coordinated_units)
        for b in SETTINGS.fleet
        if b.label and b.coordinated_units
    }
    host_units: dict[str, set[str]] = {}
    for box in SETTINGS.fleet:
        target = _ssh_target(box)
        configured = [
            *([str(box.unit)] if box.unit else []),
            *[str(item.unit) for item in box.allowed_units],
        ]
        for unit in configured:
            canonical = unit if unit.endswith(".service") else f"{unit}.service"
            host_units.setdefault(target, set()).add(canonical)
    fleet.FLEET_HOST_UNIT_ALLOWLIST = {
        box.label: sorted(host_units.get(_ssh_target(box), set()))
        for box in SETTINGS.fleet
        if box.label and host_units.get(_ssh_target(box))
    }
    fleet.CODE_GRADER_UNIT = SETTINGS.code_grader_unit
    fleet.CODE_GRADER_SOCKET = SETTINGS.code_grader_socket
    fleet.CODE_GRADER_BUNDLE_LINK = SETTINGS.code_grader_bundle_link
    fleet.CODE_GRADER_METRICS_PORT = SETTINGS.code_grader_metrics_port
    fleet.CODE_GRADER_SOCKET_MODE = SETTINGS.code_grader_socket_mode
    # OUR_SS58 maps hotkey -> human label. Fleet boxes auto-included.
    # Starred-seed hotkeys are added with their hotkey-prefix as label.
    our: dict[str, str] = {}
    for b in SETTINGS.fleet:
        # Several GPU services can share a hotkey; keep the first label as
        # the hotkey-level display name instead of letting later services
        # overwrite it and make charts jump around on config reorder.
        if b.hotkey and b.hotkey not in our:
            our[b.hotkey] = b.label
    for hk in SETTINGS.starred_hotkeys_seed:
        if hk and hk not in our:
            our[hk] = SETTINGS.starred_hotkey_labels.get(hk, hk[:10])
    fleet.OUR_SS58 = our

    fleet.VALIDATOR_URL = SETTINGS.validator_url
    fleet.SSH_KEY = SETTINGS.validator_ssh_key
    fleet.VALIDATOR_SSH = SETTINGS.validator_ssh_host
    fleet.VALIDATOR_PORT = SETTINGS.validator_ssh_port
    fleet.VALIDATOR_CONTAINER = SETTINGS.validator_container
    fleet.NETUID = SETTINGS.netuid
    fleet.VALIDATOR_MAX_VERDICT_HOTKEYS = SETTINGS.max_verdict_hotkeys
    fleet.VALIDATOR_VERDICT_STALE_SECONDS = max(
        120.0,
        SETTINGS.validator_verdict_seconds * 3.0,
    )
    fleet.R2_PUBLIC_BASE_URL = SETTINGS.r2_public_base_url
    fleet.R2_ALLOW_LIST_FALLBACK = SETTINGS.r2_allow_list_fallback
    fleet.R2_FETCH_BATCH_SIZE = SETTINGS.r2_fetch_batch_size
    fleet.R2_FETCH_WORKERS = SETTINGS.r2_fetch_workers
    # A reload in a long-lived test or embedded process must not retain the
    # prior operator's archive transport.
    fleet._r2_client = None
    with fleet._http_cache_lock:
        fleet._http_conditional_cache.clear()
    fleet._validator_retry_after_until = 0.0

    # R2 env vars expected by boto3 / the existing R2 code path.
    if SETTINGS.r2_endpoint:
        os.environ.setdefault("R2_ENDPOINT", SETTINGS.r2_endpoint)
    if SETTINGS.r2_bucket:
        os.environ.setdefault("R2_BUCKET", SETTINGS.r2_bucket)
    if SETTINGS.r2_access_key_id:
        os.environ.setdefault("AWS_ACCESS_KEY_ID", SETTINGS.r2_access_key_id)
    if SETTINGS.r2_secret_access_key:
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", SETTINGS.r2_secret_access_key)
    if SETTINGS.r2_public_base_url:
        os.environ["R2_PUBLIC_BASE_URL"] = SETTINGS.r2_public_base_url
    else:
        os.environ.pop("R2_PUBLIC_BASE_URL", None)
