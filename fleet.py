#!/usr/bin/env python3
"""SN81 fleet dashboard — single-pane live view of all miners.

Refreshes every REFRESH_S seconds. Polls each box over SSH for GPU/journal,
hits the validator's /state, pulls last-N R2 windows for slot share.

Layout (one terminal window):
  ┌── Fleet summary ───────────────────────────────────────────────┐
  │ box | hk | mem | util | status | last_acpt | acpt/30m | rej/30m│
  └────────────────────────────────────────────────────────────────┘
  ┌── Validator ──────────────────┐ ┌── Last 6 windows from R2 ───┐
  │ window=N state=open env fill  │ │ wN: ours/total rt=...       │
  └───────────────────────────────┘ └─────────────────────────────┘
  ┌── Live event tail (last 15 lines, color by box) ───────────────┐
  │ [staging1] 22:34:01 ACCEPTED window=115 prompt=441 ...          │
  └────────────────────────────────────────────────────────────────┘

Configuration lives in `config.yaml` (see config.example.yaml). The
web dashboard at `fleet_web.py` is the primary entry point; this
module is also runnable standalone in TUI mode for log-tail use cases.

Run:
  ./run.sh                            # → http://127.0.0.1:9091 (web)
  python3 fleet.py                    # legacy TUI
  python3 fleet.py --refresh 3
  python3 fleet.py --history 12       # last 12 R2 windows
"""

from __future__ import annotations

import argparse
import copy
import concurrent.futures
import gzip
import hashlib
import io
import json
import math
import os
import random
import re
import shlex
import stat
import subprocess
import sys
import threading
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Optional


_STATE_DIR = os.path.expanduser(
    os.environ.get("RELIQUARY_FLEET_STATE_DIR", "state")
)


def _state_file(name: str) -> str:
    return os.path.join(_STATE_DIR, name)


def configure_state_dir(path: str) -> None:
    """Redirect all local persistence before dashboard state is loaded."""
    global _STATE_DIR, BASELINE_FILE
    global _WINDOW_CACHE_FILE, _LEGACY_WINDOW_CACHE_FILE

    _STATE_DIR = os.path.abspath(os.path.expanduser(path))
    BASELINE_FILE = _state_file("baseline.jsonl")
    if "_WINDOW_CACHE_FILE" in globals():
        _WINDOW_CACHE_FILE = _state_file("window_cache_v2.json.gz")
        _LEGACY_WINDOW_CACHE_FILE = _state_file("window_cache.json.gz")


def _private_atomic_write(path: str, payload: bytes) -> None:
    """Atomically replace one cache file with owner-only permissions."""
    directory = os.path.dirname(path)
    os.makedirs(directory, mode=0o700, exist_ok=True)
    temporary = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _bounded_regular_file(path: str, max_bytes: int) -> bool:
    """Accept only bounded regular cache files, never symlinks or devices."""
    try:
        info = os.lstat(path)
    except OSError:
        return False
    return stat.S_ISREG(info.st_mode) and info.st_size <= max_bytes

# Soft deps — fail with hint if missing.
try:
    from rich.console import Console, Group
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
except ImportError:
    sys.exit("missing dep: pip install --user rich boto3 httpx")

try:
    import boto3
except ImportError:
    boto3 = None  # R2 panel will be disabled
try:
    import httpx
except ImportError:
    httpx = None  # validator panel will be disabled


# --- Operator-tunable values ------------------------------------------------
# These start empty and are populated by `settings.apply_to_fleet_module()`
# after `settings.load("config.yaml")` runs at process start. Reading any
# of them BEFORE that call yields the empty default — fine for the
# `import fleet` at module-load time, broken for any panel render.
#
#   FLEET                list of (alias, hotkey, label, color, systemd_unit)
#   OUR_SS58             hotkey -> label dict for "our" miners
#                        (union of fleet + starred + seeded)
#   VALIDATOR_URL        FastAPI url of the validator under observation
#   SSH_KEY              default ssh identity file (overridable per box)
#   VALIDATOR_SSH        user@host of the validator host
#   VALIDATOR_PORT       ssh port of the validator host (default 22)
#   VALIDATOR_CONTAINER  docker container name to tail logs from
#   NETUID               subnet number (default 81 / Reliquary mainnet)
FLEET: list = []
# Submit-disabled accelerator labs are an intentionally separate topology.
# Tuple shape: (ssh target, label, color, unit, evidence DB, source manifest,
# selector artifact manifest).
# No hotkey exists in this structure, so labs cannot enter OUR_SS58 or any
# submission/reward aggregation.
LABS: list = []
# Wallet-free accelerator components are separate from both full miner rows
# and offline labs. They have no hotkey and cannot enter reward accounting.
FLEET_COMPONENTS: list[dict[str, Any]] = []
FLEET_ENV_FILES: dict[str, str] = {}
FLEET_UNIT_CANDIDATES: dict[str, list[tuple[str, str]]] = {}
FLEET_CONTROLLER_CONFIG_PATHS: dict[str, dict[str, str]] = {}
FLEET_COORDINATED_UNITS: dict[str, list[str]] = {}
FLEET_STANDALONE_TELEMETRY: dict[str, dict[str, Any]] = {}
FLEET_STANDALONE_CERTIFICATION: dict[str, dict[str, str]] = {}
FLEET_RELIQUARY_ONE: dict[str, dict[str, Any]] = {}
# Per-row view of every explicitly configured miner service on the same SSH
# host. A row still collects only its own service (or replacement candidates),
# while the resolver permits sibling lanes in this host-wide allowlist and
# rejects any other active reliquary-miner-pro unit.
FLEET_HOST_UNIT_ALLOWLIST: dict[str, list[str]] = {}
VALIDATOR_URL: str = ""
OUR_SS58: dict[str, str] = {}
SSH_KEY: str = ""
VALIDATOR_SSH: str = ""
VALIDATOR_PORT: int = 22
VALIDATOR_CONTAINER: str = "reliquary-trainer"
NETUID: int = 81
CODE_GRADER_UNIT: str = "reliquary-code-grader.service"
CODE_GRADER_SOCKET: str = "/tmp/reliquary-grader.sock"
CODE_GRADER_BUNDLE_LINK: str = "/opt/reliquary-code-grader/current"
CODE_GRADER_METRICS_PORT: int = 9876
CODE_GRADER_SOCKET_MODE: int = 0o660
VALIDATOR_MAX_VERDICT_HOTKEYS: int = 8
VALIDATOR_VERDICT_STALE_SECONDS: float = 180.0
# Exhaustive archive listing is an explicit opt-in in every entry path.
R2_PUBLIC_BASE_URL: str = ""
R2_ALLOW_LIST_FALLBACK: bool = False
R2_FETCH_BATCH_SIZE: int = 8
R2_FETCH_WORKERS: int = 2
_SS58_HOTKEY_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{40,64}")


def _is_ss58_hotkey(value: object) -> bool:
    return bool(_SS58_HOTKEY_RE.fullmatch(str(value or "").strip()))


def merge_starred_into_our_ss58(extra_hotkeys: set[str]) -> None:
    """Idempotent merge of additional 'our' hotkeys into the global dict.

    Called by the dashboard whenever the live starred state changes
    (e.g. operator clicks ★ in the EMA leaderboard). Each new hotkey
    gets its 10-char prefix as a label so the dashboard's row tinting
    still works without a dedicated FLEET entry. Hotkeys already in
    OUR_SS58 keep their existing label.
    """
    for hk in extra_hotkeys:
        hk = str(hk or "").strip()
        if not _is_ss58_hotkey(hk):
            continue
        if hk not in OUR_SS58:
            OUR_SS58[hk] = hk[:10]


# --- Per-box state cached between refreshes ---------------------------------
@dataclass
class BoxState:
    alias: str
    hotkey: str
    label: str
    color: str
    unit: Optional[str]
    env_file: str = ""
    # When populated, these are the only systemd services allowed to represent
    # this logical box. Exactly one must be active unless ``coordinated_units``
    # explicitly names the complete multi-lane set allowed to run together.
    unit_candidates: tuple[tuple[str, str], ...] = ()
    unit_controller_config_paths: tuple[tuple[str, str], ...] = ()
    coordinated_units: tuple[str, ...] = ()
    host_unit_allowlist: tuple[str, ...] = ()
    standalone_telemetry_path: str = ""
    standalone_runtime_manifest_path: str = ""
    active_unit_registry_path: str = ""
    active_unit_registry: dict[str, object] = field(default_factory=dict)
    standalone_supervisor_status_path: str = ""
    standalone_telemetry_stale_seconds: float = 90.0
    miner_kind: str = "legacy"
    reliquary_one_state_root: str = ""
    reliquary_one: dict[str, Any] = field(default_factory=dict)
    reliquary_one_error: str = ""
    standalone_certification_config: dict[str, str] = field(
        default_factory=dict
    )
    operator: str = ""
    active_unit: str = ""
    active_lane: str = ""
    active_units: list[str] = field(default_factory=list)
    active_lanes: list[str] = field(default_factory=list)
    # Exact bounded systemd projection for every explicitly coordinated unit.
    # The legacy ``restart_count`` scalar is the sum of ``restarts`` across
    # this complete set; for a non-coordinated box it remains the selected
    # active unit's NRestarts value.
    coordinated_unit_statuses: list[dict[str, object]] = field(
        default_factory=list
    )
    # Host-wide, read-only inventory of active Reliquary services.  These rows
    # are systemd/process attestations only: discovery never grants mining
    # authority or bypasses the configured controller allowlist.
    service_inventory: list[dict[str, object]] = field(default_factory=list)
    active_environment: str = ""
    active_pid: int = 0
    unit_resolution_error: str = ""
    unexpected_active_units: list[str] = field(default_factory=list)
    env_file_ok: bool = False
    env_file_error: str = ""
    last_poll_s: float = 0.0
    # GPU
    gpu_mem_mb: int = 0
    gpu_total_mb: int = 1
    gpu_util: int = 0
    # Process
    proc_alive: bool = False
    proc_uptime_s: int = 0  # systemd ActiveEnter to now
    active_started_at: int = 0  # exact active unit/process start epoch
    # Events (last 30 min).
    #
    # IMPORTANT: `acpt_30m` / `acpt_60m` are VALIDATOR-CONFIRMED accept
    # counts — i.e. submissions that made it through worker GRAIL verify
    # and were appended to the active batcher's `_valid`. They are
    # repopulated from the validator events tail in a post-collect pass
    # (`recompute_validator_acpts`), NOT from the miner's local journal.
    #
    # The miner-side journal logs `ACCEPTED-PREGEN` every time the
    # picker enqueues a pre-generated rollout into the submission queue.
    # With the new queue depth (~2500 slots in flight) those lines fire
    # 20-30x more often than real validator accepts. Counting them as
    # "accepts" inflated the dashboard's throughput KPIs by the same
    # factor pre-2026-05-12. We now track them separately as
    # `pregen_30m` / `pregen_60m` for the "miner is alive and producing"
    # signal, while `acpt_30m` reflects actual earning activity.
    last_event_at: str = "—"
    acpt_30m: int | None = 0
    rej_30m: int | None = 0
    window_mismatch_30m: int = 0  # split out from generic rejects
    grail_fail_30m: int = 0
    bad_term_30m: int = 0
    last_reject_reason: str = ""
    # Throughput rolling (last 60 min, validator-confirmed).
    acpt_60m: int | None = 0
    # Miner-side pregen queue activity. ACCEPTED-PREGEN lines from
    # the miner's journal. Tells you the box is producing rollouts;
    # doesn't tell you the validator accepted any of them.
    pregen_30m: int = 0
    pregen_60m: int = 0
    # Validator-side late drops. These are submissions where the
    # validator HTTP layer accepted the POST (miner got 200) but the
    # batcher window had already advanced before the worker could pick
    # the submission up — so the submission was dropped before GRAIL
    # verify. A high `late_drops_30m` with a low `acpt_30m` means the
    # box is alive and producing, but always losing the FIFO race.
    # Populated by `recompute_validator_acpts()` from the validator
    # events tail (kind == "late_drop").
    late_drops_30m: int = 0
    late_drops_60m: int = 0
    # OOM tracking — biggest signal for stability
    last_oom_at: str = ""    # raw journal timestamp ("May 09 23:30:57")
    last_oom_age_s: int = -1   # -1 means "never seen" (or boot too old)
    oom_60m: int = 0
    # FIFO performance: how fast our ACCEPTED slots arrived (lower = better rank)
    mean_accept_t_s: float = 0.0
    # Reliability — non-OOM pregen failures + frontier-picker pre-skips
    pregen_ok_30m: int = 0
    pregen_fail_30m: int = 0  # GRAIL build errors that aren't OOM
    skip_30m: int = 0          # pre-skips (σ < threshold) — picker working
    # Fresh-miner pipeline telemetry. These are parsed from
    # native_miner_fresh.py logs and answer the operational question:
    # generating? caching? ready? firing? being blocked by batch_filled?
    miner_state: str = "?"
    miner_window: int = 0
    miner_valid: int = 0
    miner_inflight: int = 0
    miner_ready: int = 0
    miner_submitted_this_win: int = 0
    fresh_built_30m: int = 0
    cache_fwd_30m: int = 0
    prefinalized_30m: int = 0
    burst_30m: int = 0
    late_grace_30m: int = 0
    batch_filled_30m: int = 0
    checkpoint_restarts_60m: int = 0
    last_fresh_total_s: float = 0.0
    last_tune: str = ""
    # Reference-miner posture.  These are read from the exact env/source
    # manifest and durable quarantine sentinel, then corroborated with startup
    # log markers.  They intentionally coexist with the legacy frontier fields
    # so one dashboard can observe both generations during a migration.
    miner_environment: str = ""
    engine_mode: str = ""
    protocol_profile: str = ""
    runtime_parity_ok: bool = False
    reference_ready: bool = False
    # Non-secret, process-bound Code auction readiness facts. The complete
    # mapping is replaced on every successful SSH poll and cleared whenever a
    # named lane becomes ambiguous. Keeping one structured payload prevents a
    # partially upgraded probe from mixing old grader/ledger facts with a new
    # process identity.
    miner_unit_enablement: str = ""
    runtime_profile_hash: str = ""
    code_auction_probe: dict[str, Any] = field(default_factory=dict)
    # Cross-lane Code selector experiment truth. Unlike ``code_auction_probe``
    # this may describe a coordinated sibling (currently reserve2), so the
    # remote probe enumerates the resolver-approved active set and reads the
    # four activation variables from each live process, never from a stale
    # EnvironmentFile.
    code_selector_crossover_probe: dict[str, Any] = field(default_factory=dict)
    # Process-bound same-shard AB/BA contract plus latest durable assignment
    # per coordinated lane. Kept separate from the retired k2 crossover
    # because the two experiments have different activation/state schemas.
    code_overlap_abba_probe: dict[str, Any] = field(default_factory=dict)
    # Schema-normalized, freshness-bound snapshot from a standalone miner's
    # atomic dashboard JSON. Empty means this row uses the legacy probe.
    standalone_telemetry: dict[str, Any] = field(default_factory=dict)
    standalone_generated_at: float = 0.0
    standalone_age_s: float = -1.0
    standalone_fresh: bool = False
    standalone_error: str = ""
    standalone_controller_mode: str = ""
    standalone_controller_config_path: str = ""
    standalone_active_runtime_manifest_path: str = ""
    standalone_active_ledger_path: str = ""
    standalone_supervisor: dict[str, Any] = field(default_factory=dict)
    standalone_progress_age_s: float = -1.0
    # Exact, submit-disabled standalone certification is deliberately
    # separate from live miner process and funnel state. It is populated only
    # when no configured canary/mine unit is active.
    standalone_certification: dict[str, Any] = field(default_factory=dict)
    runtime_components: list[dict[str, Any]] = field(default_factory=list)
    miner_source_revision: str = ""
    reliquary_source_revision: str = ""
    # The validator image may contain private deployment-only commits while
    # ``reliquary_source_revision`` names the public protocol source closure.
    # Keep both identities: compatibility is certified against the latter,
    # whereas live deployment drift is checked against the former.
    observed_validator_image_revision: str = ""
    source_manifest_provisioned_ok: bool = False
    provisioned_model_kind: str = ""
    provisioned_checkpoint_n: int = -1
    provisioned_model_repo: str = ""
    provisioned_model_revision: str = ""
    base_model_repo: str = ""
    base_model_revision: str = ""
    quarantine_active: bool = False
    quarantine_path: str = ""
    quarantine_reason: str = ""
    quarantine_at: float = 0.0
    # ``acceptance_scope`` makes the ownership of a verdict counter explicit.
    # A validator endpoint is keyed by hotkey, so two host rows sharing one
    # hotkey must never receive a made-up per-host zero or a duplicated total.
    acceptance_source: str = "events"
    acceptance_scope: str = "per_hotkey"
    shared_hotkey_verdicts: dict[str, object] = field(default_factory=dict)
    final_accept_30m: int | None = 0
    final_accept_60m: int | None = 0
    final_reject_30m: int | None = 0
    final_reject_60m: int | None = 0
    last_final_reason: str = ""
    last_final_window: int = 0
    last_final_ts: float = 0.0
    drand_offset: int = 0
    picker_epsilon: float = 0.0
    picker_top_k: int = 0
    external_min_sigma: float = 0.0
    external_max_len: int = 0
    hash_max_leading_byte: int = -1
    burst_per_window: int = 0
    prompt_shard_id: int = -1
    prompt_shard_mod: int = 0
    frontier_path: str = ""
    frontier_entries: int = 0
    frontier_content_entries: int = 0
    frontier_age_s: float = -1.0
    frontier_newest_age_s: float = -1.0
    frontier_checkpoint_n: int = 0
    frontier_checkpoint_revision: str = ""
    frontier_ckpts: dict[str, int] = field(default_factory=dict)
    local_checkpoint_n: int = 0
    local_checkpoint_revision: str = ""
    # Runtime checkpoint attestation.  Unlike the provisioning manifest, this
    # can advance while one miner process stays alive.  It is populated only
    # from the current systemd invocation.  The strongest legacy path proves
    # exact resolution, two-model load, and structured generation; current
    # Math builds can instead emit one schema-versioned startup attestation
    # carrying the exact tuple and independently corroborated process PID.
    runtime_checkpoint_loaded: bool = False
    runtime_checkpoint_n: int = 0
    runtime_checkpoint_repo: str = ""
    runtime_checkpoint_revision: str = ""
    runtime_checkpoint_pid: int = 0
    runtime_checkpoint_started_at: int = 0
    runtime_checkpoint_evidence: str = ""
    state_relay_ok: bool = False
    state_relay_age_s: float = -1.0
    state_relay_upstream_ms: float = 0.0
    state_relay_failures: int = 0
    state_relay_error: str = ""
    watchdog_ok: bool = False
    watchdog_age_s: float = -1.0
    watchdog_stale_strikes: int = 0
    watchdog_restart_count: int = 0
    watchdog_last_action: str = ""
    watchdog_last_error: str = ""
    # Process / system health
    restart_count: int = 0     # coordinated sum or selected-unit NRestarts
    rss_mb: int = 0
    cpu_pct: int = 0
    disk_used_pct: int = 0     # /srv volume
    # Trends — last N samples (latest = rightmost). Used for sparklines.
    acpt_trend: deque = field(default_factory=lambda: deque(maxlen=20))
    oom_trend: deque = field(default_factory=lambda: deque(maxlen=20))
    mem_trend: deque = field(default_factory=lambda: deque(maxlen=20))
    # Recent journal events (for live tail panel)
    recent_lines: deque = field(default_factory=lambda: deque(maxlen=8))
    error: str = ""


@dataclass
class LabState:
    """Read-only telemetry for one wallet-free, submit-disabled GPU lab."""

    alias: str
    label: str
    color: str
    unit: str
    evidence_db: str
    source_manifest: str
    selector_artifact_manifest: str = ""
    last_poll_s: float = 0.0
    gpu_name: str = ""
    compute_capability: str = ""
    gpu_mem_mb: int = 0
    gpu_total_mb: int = 1
    gpu_util: int = 0
    unit_active_state: str = "unknown"
    unit_sub_state: str = "unknown"
    unit_enablement: str = "unknown"
    active_pid: int = 0
    restart_count: int = 0
    submit_disabled_attested: bool = False
    lane_start_disarmed: bool = False
    manifest_submit_disabled: bool = False
    manifest_provisioned_ok: bool = False
    miner_release: str = ""
    source_revision: str = ""
    checkpoint_repo_id: str = ""
    checkpoint_revision: str = ""
    checkpoint_n: int = 0
    environment: str = ""
    hardware_name: str = ""
    parity_status: str = ""
    evidence_miner_release: str = ""
    evidence_db_ok: bool = False
    evidence_schema_version: int = 0
    attempts: int = 0
    terminal_attempts: int = 0
    complete_attempts: int = 0
    censored_attempts: int = 0
    deadline_censored_attempts: int = 0
    token_censored_attempts: int = 0
    pending_attempts: int = 0
    error_attempts: int = 0
    first_window_n: int = 0
    last_window_n: int = 0
    last_attempt_at: float = 0.0
    evidence_error: str = ""
    artifact_manifest_ok: bool = False
    artifact_status: str = ""
    artifact_kind: str = ""
    artifact_model_version: str = ""
    artifact_digest: str = ""
    artifact_file_sha256: str = ""
    artifact_data_digest: str = ""
    artifact_source_revision: str = ""
    artifact_checkpoint_repo_id: str = ""
    artifact_checkpoint_revision: str = ""
    artifact_checkpoint_n: int = 0
    artifact_window_start: int = 0
    artifact_window_end: int = 0
    artifact_train_local_rows: int = 0
    artifact_train_population_rows: int = 0
    artifact_completion_terminal_rows: int = 0
    artifact_holdout_rows: int = 0
    artifact_holdout_window: int = 0
    artifact_activation_gate_passed: bool = False
    artifact_top_quintile_lift: float | None = None
    artifact_heldout_value_lift: float | None = None
    artifact_completion_brier: float | None = None
    artifact_selection_brier: float | None = None
    artifact_value_multiclass_brier: float | None = None
    artifact_payout_profile: str = ""
    artifact_economic_target: str = ""
    artifact_online_activation_allowed: bool | None = None
    artifact_expires_after_window: int | None = None
    artifact_objective: str = ""
    artifact_exploration_bps: int | None = None
    artifact_emitted_slot_lift: float | None = None
    artifact_emission_mse: float | None = None
    artifact_emission_base_mse: float | None = None
    artifact_shadow_promotion_gate_passed: bool = False
    artifact_activation_blocker: str = ""
    artifact_gpu_rolling_lift_mean: float | None = None
    artifact_gpu_final_lift: float | None = None
    artifact_gpu_positive_value_folds: int = 0
    artifact_gpu_rolling_folds: int = 0
    artifact_cpu_rolling_lift_mean: float | None = None
    artifact_cpu_positive_value_folds: int = 0
    artifact_cpu_rolling_folds: int = 0
    artifact_cpu_bitwise_reproducible: bool = False
    artifact_gpu_bitwise_reproducible: bool = False
    artifact_blockers: list[str] = field(default_factory=list)
    artifact_decision_reason: str = ""
    artifact_error: str = ""
    error: str = ""


@dataclass
class ComponentState:
    """Read-only truth for one wallet-free component of a full miner."""

    alias: str
    label: str
    color: str
    role: str
    controller_label: str
    generator_unit: str
    tunnel_unit: str
    runtime_manifest_path: str
    runtime_profile_path: str
    component_config_path: str
    evidence_alias: str
    evidence_dir: str
    evidence_runtime_manifest_path: str
    evidence_runtime_profile_path: str
    evidence_target_windows: int
    evidence_stale_seconds: float
    telemetry_stale_seconds: float
    last_poll_s: float = 0.0
    generator_active_state: str = "unknown"
    generator_sub_state: str = "unknown"
    generator_enablement: str = "unknown"
    generator_pid: int = 0
    generator_restarts: int = 0
    tunnel_active_state: str = "unknown"
    tunnel_sub_state: str = "unknown"
    tunnel_enablement: str = "unknown"
    tunnel_pid: int = 0
    tunnel_restarts: int = 0
    gpu_uuid: str = ""
    gpu_name: str = ""
    compute_capability: str = ""
    gpu_mem_mb: int = 0
    gpu_total_mb: int = 1
    gpu_util: int = 0
    gpu_power_w: float = 0.0
    gpu_power_limit_w: float = 0.0
    component_role: str = ""
    manifest_schema_version: int = 0
    manifest_sha256: str = ""
    manifest_profile_sha256: str = ""
    miner_source_revision: str = ""
    validator_source_revision: str = ""
    checkpoint_n: int = 0
    checkpoint_revision: str = ""
    model_repo: str = ""
    environment: str = ""
    runtime_profile_sha256: str = ""
    runtime_regime: str = ""
    profile_file_sha256: str = ""
    profile_source_revision: str = ""
    profile_checkpoint_revision: str = ""
    profile_gpu_uuid: str = ""
    identity_ok: bool = False
    wallet_absent_attested: bool = False
    submit_authority: bool = False
    evidence_valid_windows: int = 0
    evidence_complete_groups: int = 0
    evidence_first_window: int = 0
    evidence_last_window: int = 0
    evidence_latest_at: float = 0.0
    evidence_source_ok: bool = False
    evidence_fresh: bool = False
    evidence_certificate_bound: bool = False
    evidence_manifest_sha256: str = ""
    evidence_profile_sha256: str = ""
    evidence_error: str = ""
    # A live component can advance checkpoints independently from the static
    # rollback row in config.yaml.  These fields are populated only after the
    # running worker process, its immutable runtime manifest, and the current
    # controller manifest all agree on one exact component identity.
    resolution_source: str = "static_config"
    worker_state: str = ""
    worker_progress_fresh: bool = False
    worker_progress_age_s: float = -1.0
    worker_boot_id: str = ""
    worker_engine_epoch: str = ""
    worker_recovery_count: int = 0
    active_job_id: str = ""
    active_window_n: int = 0
    active_started_at: float = 0.0
    active_deadline_at: float = 0.0
    last_completed_job_id: str = ""
    last_completed_window_n: int = 0
    last_completed_at: float = 0.0
    error: str = ""


_OFFLINE_LAB_PROBE_SOURCE = r'''import hashlib
import json
import math
import os
import shlex
import sqlite3
import stat
import subprocess
import sys
import urllib.parse

unit, evidence_db, source_manifest, selector_artifact_manifest = sys.argv[1:5]
result = {
    "unit_active_state": "unknown",
    "unit_sub_state": "unknown",
    "unit_enablement": "unknown",
    "active_pid": 0,
    "restart_count": 0,
    "gpu_name": "",
    "compute_capability": "",
    "gpu_mem_mb": 0,
    "gpu_total_mb": 1,
    "gpu_util": 0,
    "submit_disabled_attested": False,
    "lane_start_disarmed": False,
    "manifest_submit_disabled": False,
    "manifest_provisioned_ok": False,
    "miner_release": "",
    "source_revision": "",
    "checkpoint_repo_id": "",
    "checkpoint_revision": "",
    "checkpoint_n": 0,
    "environment": "",
    "hardware_name": "",
    "parity_status": "",
    "evidence_miner_release": "",
    "evidence_db_ok": False,
    "evidence_schema_version": 0,
    "attempts": 0,
    "terminal_attempts": 0,
    "complete_attempts": 0,
    "censored_attempts": 0,
    "deadline_censored_attempts": 0,
    "token_censored_attempts": 0,
    "pending_attempts": 0,
    "error_attempts": 0,
    "first_window_n": 0,
    "last_window_n": 0,
    "last_attempt_at": 0.0,
    "evidence_error": "",
    "artifact_manifest_ok": False,
    "artifact_status": "",
    "artifact_kind": "",
    "artifact_model_version": "",
    "artifact_digest": "",
    "artifact_file_sha256": "",
    "artifact_data_digest": "",
    "artifact_source_revision": "",
    "artifact_checkpoint_repo_id": "",
    "artifact_checkpoint_revision": "",
    "artifact_checkpoint_n": 0,
    "artifact_window_start": 0,
    "artifact_window_end": 0,
    "artifact_train_local_rows": 0,
    "artifact_train_population_rows": 0,
    "artifact_completion_terminal_rows": 0,
    "artifact_holdout_rows": 0,
    "artifact_holdout_window": 0,
    "artifact_activation_gate_passed": False,
    "artifact_top_quintile_lift": None,
    "artifact_heldout_value_lift": None,
    "artifact_completion_brier": None,
    "artifact_selection_brier": None,
    "artifact_value_multiclass_brier": None,
    "artifact_payout_profile": "",
    "artifact_economic_target": "",
    "artifact_online_activation_allowed": None,
    "artifact_expires_after_window": None,
    "artifact_objective": "",
    "artifact_exploration_bps": None,
    "artifact_emitted_slot_lift": None,
    "artifact_emission_mse": None,
    "artifact_emission_base_mse": None,
    "artifact_shadow_promotion_gate_passed": False,
    "artifact_activation_blocker": "",
    "artifact_gpu_rolling_lift_mean": None,
    "artifact_gpu_final_lift": None,
    "artifact_gpu_positive_value_folds": 0,
    "artifact_gpu_rolling_folds": 0,
    "artifact_cpu_rolling_lift_mean": None,
    "artifact_cpu_positive_value_folds": 0,
    "artifact_cpu_rolling_folds": 0,
    "artifact_cpu_bitwise_reproducible": False,
    "artifact_gpu_bitwise_reproducible": False,
    "artifact_blockers": [],
    "artifact_decision_reason": "",
    "artifact_error": "",
    "error": "",
}


def run(argv):
    return subprocess.run(
        argv,
        text=True,
        capture_output=True,
        timeout=6,
        check=False,
    )


def parse_assignments(path):
    values = {}
    try:
        info = os.lstat(path)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            return values
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            for raw in handle:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if key in {
                    "PROVISIONED_OK",
                    "RELIQUARY_SUBMIT_DISABLED_LAB",
                    "MINER_PRO_SOURCE_REVISION",
                    "RELIQUARY_SOURCE_REVISION",
                    "RELIQUARY_PROVISIONED_CHECKPOINT_N",
                    "RELIQUARY_PROVISIONED_MODEL_REPO",
                    "RELIQUARY_PROVISIONED_MODEL_REVISION",
                }:
                    values[key] = value.strip().strip('"').strip("'")
    except (OSError, ValueError):
        return {}
    return values


if unit:
    try:
        shown = run([
            "systemctl",
            "show",
            unit,
            "--property=ActiveState,SubState,UnitFileState,MainPID,NRestarts",
        ])
        properties = {}
        for raw in shown.stdout.splitlines():
            if "=" in raw:
                key, value = raw.split("=", 1)
                properties[key] = value
        result["unit_active_state"] = properties.get("ActiveState") or "unknown"
        result["unit_sub_state"] = properties.get("SubState") or "unknown"
        result["unit_enablement"] = properties.get("UnitFileState") or "unknown"
        result["active_pid"] = int(properties.get("MainPID") or 0)
        result["restart_count"] = int(properties.get("NRestarts") or 0)
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        result["error"] = "unit_probe:" + type(exc).__name__
else:
    result["unit_active_state"] = "not_applicable"
    result["unit_sub_state"] = "completed_one_shot"
    result["unit_enablement"] = "not_applicable"

try:
    gpu = run([
        "nvidia-smi",
        "--query-gpu=name,compute_cap,memory.used,memory.total,utilization.gpu",
        "--format=csv,noheader,nounits",
    ])
    first = gpu.stdout.splitlines()[0] if gpu.returncode == 0 and gpu.stdout else ""
    parts = [part.strip() for part in first.split(",")]
    if len(parts) == 5:
        result["gpu_name"] = parts[0]
        result["compute_capability"] = parts[1]
        result["gpu_mem_mb"] = int(float(parts[2]))
        result["gpu_total_mb"] = max(1, int(float(parts[3])))
        result["gpu_util"] = int(float(parts[4]))
except (OSError, subprocess.SubprocessError, ValueError, IndexError):
    pass

manifest = parse_assignments(source_manifest)
result["manifest_submit_disabled"] = (
    manifest.get("RELIQUARY_SUBMIT_DISABLED_LAB") == "1"
)
result["manifest_provisioned_ok"] = manifest.get("PROVISIONED_OK") == "1"
result["miner_release"] = manifest.get("MINER_PRO_SOURCE_REVISION", "")
result["source_revision"] = manifest.get("RELIQUARY_SOURCE_REVISION", "")
result["checkpoint_repo_id"] = manifest.get(
    "RELIQUARY_PROVISIONED_MODEL_REPO", ""
)
result["checkpoint_revision"] = manifest.get(
    "RELIQUARY_PROVISIONED_MODEL_REVISION", ""
)
try:
    result["checkpoint_n"] = int(
        manifest.get("RELIQUARY_PROVISIONED_CHECKPOINT_N") or 0
    )
except ValueError:
    result["checkpoint_n"] = 0

process_environment = {}
pid = result["active_pid"]
if pid > 0:
    try:
        with open(f"/proc/{pid}/environ", "rb") as handle:
            for item in handle.read().split(b"\0"):
                key, separator, value = item.partition(b"=")
                if separator and key.decode("ascii", "ignore") in {
                    "RELIQUARY_SUBMIT_DISABLED_LAB",
                    "RELIQUARY_LANE_START_ALLOWED",
                }:
                    process_environment[key.decode("ascii")] = value.decode(
                        "utf-8", "replace"
                    )
    except OSError:
        pass
if unit and not process_environment:
    try:
        configured = run(["systemctl", "show", unit, "--property=Environment", "--value"])
        for item in shlex.split(configured.stdout):
            key, separator, value = item.partition("=")
            if separator and key in {
                "RELIQUARY_SUBMIT_DISABLED_LAB",
                "RELIQUARY_LANE_START_ALLOWED",
            }:
                process_environment[key] = value
    except (OSError, subprocess.SubprocessError, ValueError):
        pass
result["submit_disabled_attested"] = bool(
    pid > 0
    and result["manifest_submit_disabled"]
    and process_environment.get("RELIQUARY_SUBMIT_DISABLED_LAB") == "1"
)
result["lane_start_disarmed"] = (
    process_environment.get("RELIQUARY_LANE_START_ALLOWED") == "0"
)

required_columns = {
    "attempt_id",
    "environment",
    "window_n",
    "source_revision",
    "checkpoint_repo_id",
    "checkpoint_revision",
    "checkpoint_n",
    "miner_release",
    "hardware_name",
    "compute_capability",
    "parity_status",
    "attempt_started_at",
    "terminal_cause",
    "finished_at",
    "deadline_censored",
    "local_rewards_json",
    "error_type",
    "terminal_digest",
}
try:
    info = os.lstat(evidence_db)
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise RuntimeError("not_regular")
    uri = "file:" + urllib.parse.quote(evidence_db, safe="/") + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=3.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    application_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
    user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    quick_check = str(connection.execute("PRAGMA quick_check(1)").fetchone()[0])
    columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(attempts)")
    }
    if application_id != 0x524F5345:
        raise RuntimeError("application_id")
    if user_version != 1:
        raise RuntimeError("schema_version")
    if quick_check.lower() != "ok":
        raise RuntimeError("quick_check")
    if not required_columns.issubset(columns):
        raise RuntimeError("columns")
    counts = connection.execute(
        """
        SELECT
          COUNT(*) AS attempts,
          COALESCE(SUM(terminal_digest IS NOT NULL), 0) AS terminal_attempts,
          COALESCE(SUM(local_rewards_json IS NOT NULL), 0) AS complete_attempts,
          COALESCE(SUM(
            COALESCE(deadline_censored, 0) = 1 OR terminal_cause = 'token_limit'
          ), 0) AS censored_attempts,
          COALESCE(SUM(COALESCE(deadline_censored, 0) = 1), 0)
            AS deadline_censored_attempts,
          COALESCE(SUM(terminal_cause = 'token_limit'), 0)
            AS token_censored_attempts,
          COALESCE(SUM(terminal_digest IS NULL), 0) AS pending_attempts,
          COALESCE(SUM(error_type IS NOT NULL), 0) AS error_attempts,
          COALESCE(MIN(window_n), 0) AS first_window_n,
          COALESCE(MAX(window_n), 0) AS last_window_n,
          COALESCE(MAX(COALESCE(finished_at, attempt_started_at)), 0.0)
            AS last_attempt_at
        FROM attempts
        """
    ).fetchone()
    for key in counts.keys():
        result[key] = counts[key]
    identity = connection.execute(
        """
        SELECT environment, source_revision, checkpoint_repo_id,
               checkpoint_revision, checkpoint_n, miner_release,
               hardware_name, compute_capability, parity_status
        FROM attempts
        ORDER BY attempt_started_at DESC, attempt_id DESC
        LIMIT 1
        """
    ).fetchone()
    if identity is not None:
        for key in identity.keys():
            if key == "miner_release":
                result["evidence_miner_release"] = identity[key]
            else:
                result[key] = identity[key]
    result["evidence_schema_version"] = user_version
    result["evidence_db_ok"] = True
    connection.close()
except (OSError, sqlite3.Error, RuntimeError, ValueError) as exc:
    if evidence_db:
        result["evidence_error"] = str(exc)[:160] or type(exc).__name__


def finite_metric(value, *, brier=False):
    if value is None:
        return None
    if isinstance(value, bool):
        raise RuntimeError("artifact_metric")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise RuntimeError("artifact_metric")
    if brier and not 0.0 <= parsed <= 1.0:
        raise RuntimeError("artifact_brier")
    return parsed


def nonnegative_metric(value):
    parsed = finite_metric(value)
    if parsed is not None and parsed < 0.0:
        raise RuntimeError("artifact_metric")
    return parsed


def nonnegative_int(value):
    if isinstance(value, bool):
        raise RuntimeError("artifact_integer")
    parsed = int(value or 0)
    if parsed < 0 or parsed > 2**63 - 1:
        raise RuntimeError("artifact_integer")
    return parsed


if selector_artifact_manifest:
    try:
        info = os.lstat(selector_artifact_manifest)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise RuntimeError("artifact_not_regular")
        if stat.S_IMODE(info.st_mode) & 0o022:
            raise RuntimeError("artifact_mutable")
        with open(selector_artifact_manifest, "rb") as handle:
            raw = handle.read(1024 * 1024 + 1)
        if not raw or len(raw) > 1024 * 1024:
            raise RuntimeError("artifact_size")
        document = json.loads(raw)
        if not isinstance(document, dict) or document.get("schema_version") != 1:
            raise RuntimeError("artifact_schema")
        claimed = str(document.get("checksum") or "").lower()
        if len(claimed) != 64 or any(
            character not in "0123456789abcdef" for character in claimed
        ):
            raise RuntimeError("artifact_checksum")
        canonical_document = dict(document)
        canonical_document.pop("checksum", None)
        canonical = json.dumps(
            canonical_document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        if hashlib.sha256(canonical).hexdigest() != claimed:
            raise RuntimeError("artifact_checksum")
        report_kind = str(document.get("report_kind") or "")
        artifact_kind = str(document.get("artifact_kind") or report_kind)
        model_version = str(document.get("model_version") or "")
        identity = document.get("identity")
        extra = {
            "artifact_payout_profile": "",
            "artifact_economic_target": "",
            "artifact_online_activation_allowed": None,
            "artifact_expires_after_window": None,
            "artifact_objective": "",
            "artifact_exploration_bps": None,
            "artifact_emitted_slot_lift": None,
            "artifact_emission_mse": None,
            "artifact_emission_base_mse": None,
            "artifact_shadow_promotion_gate_passed": False,
            "artifact_activation_blocker": "",
            "artifact_gpu_rolling_lift_mean": None,
            "artifact_gpu_final_lift": None,
            "artifact_gpu_positive_value_folds": 0,
            "artifact_gpu_rolling_folds": 0,
            "artifact_cpu_rolling_lift_mean": None,
            "artifact_cpu_positive_value_folds": 0,
            "artifact_cpu_rolling_folds": 0,
            "artifact_cpu_bitwise_reproducible": False,
            "artifact_gpu_bitwise_reproducible": False,
            "artifact_blockers": [],
            "artifact_decision_reason": "",
        }
        policy_document = document.get("policy")
        if policy_document is None:
            policy_document = {}
        if not isinstance(policy_document, dict):
            raise RuntimeError("artifact_policy")
        online_activation_allowed = policy_document.get(
            "online_activation_allowed"
        )
        if (
            online_activation_allowed is not None
            and not isinstance(online_activation_allowed, bool)
        ):
            raise RuntimeError("artifact_online_activation_allowed")
        expires_after_window = policy_document.get("expires_after_window")
        if expires_after_window is not None:
            expires_after_window = nonnegative_int(expires_after_window)
            if expires_after_window <= 0:
                raise RuntimeError("artifact_expires_after_window")
        objective = policy_document.get("objective", "")
        if (
            not isinstance(objective, str)
            or len(objective) > 2048
            or any(ord(character) < 0x20 for character in objective)
        ):
            raise RuntimeError("artifact_objective")
        exploration_bps = policy_document.get("exploration_bps")
        if exploration_bps is not None:
            exploration_bps = nonnegative_int(exploration_bps)
            if exploration_bps > 10_000:
                raise RuntimeError("artifact_exploration_bps")
        extra.update(
            {
                "artifact_online_activation_allowed": (
                    online_activation_allowed
                ),
                "artifact_expires_after_window": expires_after_window,
                "artifact_objective": objective,
                "artifact_exploration_bps": exploration_bps,
            }
        )
        if report_kind == "reliquary_math_selector_bounded_search":
            cutoff = document.get("cutoff")
            dataset = document.get("dataset")
            decision = document.get("decision")
            cpu = document.get("deterministic_cpu_challenger")
            gpu = document.get("gpu_search")
            if not all(
                isinstance(section, dict)
                for section in (identity, cutoff, dataset, decision, cpu, gpu)
            ):
                raise RuntimeError("artifact_sections")
            validation = cpu.get("final_cutoff")
            rolling = gpu.get("rolling_summary")
            gpu_final = gpu.get("final_cutoff")
            cpu_rolling = cpu.get("rolling_summary")
            if (
                not isinstance(validation, dict)
                or not isinstance(rolling, dict)
                or not isinstance(gpu_final, dict)
            ):
                raise RuntimeError("artifact_sections")
            if str(cutoff.get("validator_image_revision") or "") != str(
                identity.get("public_source_revision") or ""
            ):
                raise RuntimeError("artifact_source")
            source = {
                "window_start": cutoff.get("window_start"),
                "window_end": cutoff.get("window_end"),
                "data_digest": dataset.get("manifest_sha256"),
            }
            training = {
                "train_local_rows": dataset.get("local_attempt_rows"),
                "train_population_rows": dataset.get("population_rows"),
                "completion_terminal_rows": 0,
            }
            artifact_status = str(decision.get("status") or "")
            model_version = "bounded_search_cpu_gpu"
            blockers = []
            if decision.get("completion_calibration_gate_passed") is not True:
                blockers.append("completion_calibration")
            if "rolling_cpu_folds" not in cpu:
                blockers.append("rolling_cpu_validation")
            if gpu.get("bitwise_reproducible") is not True:
                blockers.append("gpu_nondeterminism")
            extra = {
                **extra,
                "artifact_gpu_rolling_lift_mean": finite_metric(
                    rolling.get("selection_lift_mean")
                ),
                "artifact_gpu_final_lift": finite_metric(
                    gpu_final.get("top_quintile_selected_lift")
                ),
                "artifact_gpu_positive_value_folds": nonnegative_int(
                    rolling.get("folds_positive_value_lift")
                ),
                "artifact_gpu_rolling_folds": nonnegative_int(
                    rolling.get("folds")
                ),
                "artifact_cpu_rolling_lift_mean": (
                    finite_metric(cpu_rolling.get("selection_lift_mean"))
                    if isinstance(cpu_rolling, dict)
                    else None
                ),
                "artifact_cpu_positive_value_folds": (
                    nonnegative_int(cpu_rolling.get("folds_positive_value_lift"))
                    if isinstance(cpu_rolling, dict)
                    else 0
                ),
                "artifact_cpu_rolling_folds": (
                    nonnegative_int(cpu_rolling.get("folds"))
                    if isinstance(cpu_rolling, dict)
                    else 0
                ),
                "artifact_cpu_bitwise_reproducible": (
                    cpu.get("bitwise_reproducible") is True
                ),
                "artifact_gpu_bitwise_reproducible": (
                    gpu.get("bitwise_reproducible") is True
                ),
                "artifact_blockers": blockers,
                "artifact_decision_reason": str(decision.get("reason") or ""),
            }
        elif (
            artifact_kind == "reliquary_math_selector_catboost_challenger"
            and model_version == "catboost_v2_emitted_slots"
        ):
            source = document.get("source")
            training = document.get("training")
            validation = document.get("validation")
            policy = document.get("policy")
            if not all(
                isinstance(section, dict)
                for section in (identity, source, training, validation, policy)
            ):
                raise RuntimeError("artifact_sections")
            payout_profile = str(policy.get("payout_profile") or "")
            economic_target = str(policy.get("economic_target") or "")
            if payout_profile not in {"strict_top8", "boundary_fair_split"}:
                raise RuntimeError("artifact_payout_profile")
            if economic_target != "effective_full_slots":
                raise RuntimeError("artifact_economic_target")
            emitted_slot_lift = nonnegative_metric(
                validation.get("emitted_slot_lift")
            )
            emission_mse = nonnegative_metric(validation.get("emission_mse"))
            emission_base_mse = nonnegative_metric(
                validation.get("emission_base_mse")
            )
            if any(
                metric is None
                for metric in (emitted_slot_lift, emission_mse, emission_base_mse)
            ):
                raise RuntimeError("artifact_emission_metrics")
            shadow_gate = validation.get("shadow_promotion_gate_passed")
            if not isinstance(shadow_gate, bool):
                raise RuntimeError("artifact_shadow_gate")
            activation_blocker = validation.get("activation_blocker")
            if not isinstance(activation_blocker, str) or not activation_blocker:
                raise RuntimeError("artifact_activation_blocker")
            artifact_status = str(document.get("status") or "")
            extra = {
                **extra,
                "artifact_payout_profile": payout_profile,
                "artifact_economic_target": economic_target,
                "artifact_emitted_slot_lift": emitted_slot_lift,
                "artifact_emission_mse": emission_mse,
                "artifact_emission_base_mse": emission_base_mse,
                "artifact_shadow_promotion_gate_passed": shadow_gate,
                "artifact_activation_blocker": activation_blocker,
            }
        elif artifact_kind == "reliquary_code_selector_challenger":
            source = document.get("source")
            training_document = document.get("training")
            validation_document = document.get("validation")
            blockers = document.get("blockers")
            gates = document.get("gates")
            if not all(
                isinstance(section, dict)
                for section in (
                    identity,
                    source,
                    training_document,
                    validation_document,
                    blockers,
                    gates,
                )
            ):
                raise RuntimeError("artifact_sections")
            completion_source = source.get("completion")
            completion_validation = validation_document.get("completion")
            empirical_validation = validation_document.get("empirical")
            if not all(
                isinstance(section, dict)
                for section in (
                    completion_source,
                    completion_validation,
                    empirical_validation,
                )
            ):
                raise RuntimeError("artifact_sections")
            empirical_aggregate = empirical_validation.get("aggregate")
            empirical_folds = empirical_validation.get("folds")
            if not isinstance(empirical_aggregate, dict) or not isinstance(
                empirical_folds, list
            ):
                raise RuntimeError("artifact_sections")
            holdout_windows = [
                nonnegative_int(fold.get("holdout_window"))
                for fold in empirical_folds
                if isinstance(fold, dict)
            ]
            source = {
                "window_start": source.get("window_start"),
                "window_end": source.get("window_end"),
                "data_digest": source.get("dataset_manifest_sha256"),
            }
            training = {
                "train_local_rows": completion_source.get("rows"),
                "train_population_rows": document["source"].get(
                    "population_rows"
                ),
                "completion_terminal_rows": completion_source.get("rows"),
            }
            validation = {
                "activation_gate_passed": (
                    blockers.get("activation_gate_passed") is True
                ),
                "holdout_rows": empirical_aggregate.get("rows"),
                "holdout_window": max(holdout_windows, default=0),
                "top_quintile_lift": empirical_aggregate.get(
                    "exact_k2_lift"
                ),
                "heldout_value_lift": empirical_aggregate.get(
                    "heldout_value_lift"
                ),
                "completion_brier": completion_validation.get(
                    "completion_brier"
                ),
                "selection_brier": empirical_aggregate.get(
                    "selection_brier"
                ),
                "value_multiclass_brier": None,
            }
            artifact_status = str(document.get("status") or "")
            failed_gates = sorted(
                str(name) for name, passed in gates.items() if passed is False
            )
            extra = {
                **extra,
                "artifact_blockers": failed_gates,
                "artifact_decision_reason": str(blockers.get("reason") or ""),
            }
        else:
            artifact_status = str(document.get("status") or "")
            source = document.get("source")
            training = document.get("training")
            validation = document.get("validation")
        if artifact_status not in {"shadow_only", "rejected"}:
            raise RuntimeError("artifact_status")
        if not all(
            isinstance(section, dict)
            for section in (identity, source, training, validation)
        ):
            raise RuntimeError("artifact_sections")
        window_start = nonnegative_int(source.get("window_start"))
        window_end = nonnegative_int(source.get("window_end"))
        checkpoint_n = nonnegative_int(identity.get("checkpoint_n"))
        if window_start <= 0 or window_end < window_start or checkpoint_n <= 0:
            raise RuntimeError("artifact_scope")
        result.update({
            "artifact_manifest_ok": True,
            "artifact_status": artifact_status,
            "artifact_kind": artifact_kind,
            "artifact_model_version": model_version,
            "artifact_digest": claimed,
            "artifact_file_sha256": hashlib.sha256(raw).hexdigest(),
            "artifact_data_digest": str(source.get("data_digest") or ""),
            "artifact_source_revision": str(
                identity.get("public_source_revision") or ""
            ),
            "artifact_checkpoint_repo_id": str(
                identity.get("checkpoint_repository")
                or identity.get("model_repository")
                or ""
            ),
            "artifact_checkpoint_revision": str(
                identity.get("checkpoint_revision") or ""
            ),
            "artifact_checkpoint_n": checkpoint_n,
            "artifact_window_start": window_start,
            "artifact_window_end": window_end,
            "artifact_train_local_rows": nonnegative_int(
                training.get("train_local_rows")
            ),
            "artifact_train_population_rows": nonnegative_int(
                training.get("train_population_rows")
            ),
            "artifact_completion_terminal_rows": nonnegative_int(
                training.get("completion_terminal_rows")
            ),
            "artifact_holdout_rows": nonnegative_int(
                validation.get("holdout_rows")
            ),
            "artifact_holdout_window": nonnegative_int(
                validation.get("holdout_window")
            ),
            "artifact_activation_gate_passed": (
                validation.get("activation_gate_passed") is True
            ),
            "artifact_top_quintile_lift": finite_metric(
                validation.get(
                    "top_quintile_lift",
                    validation.get("top_quintile_selected_lift"),
                )
            ),
            "artifact_heldout_value_lift": finite_metric(
                validation.get("heldout_value_lift")
            ),
            "artifact_completion_brier": finite_metric(
                validation.get("completion_brier"), brier=True
            ),
            "artifact_selection_brier": finite_metric(
                validation.get("selection_brier"), brier=True
            ),
            "artifact_value_multiclass_brier": finite_metric(
                validation.get("value_multiclass_brier"), brier=True
            ),
            **extra,
        })
    except (
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        result["artifact_error"] = str(exc)[:160] or type(exc).__name__

print(json.dumps(result, separators=(",", ":"), sort_keys=True))
'''


_SYSTEMD_UNIT_RE = re.compile(r"^[A-Za-z0-9_.@-]+$")
_EXACT_CHECKPOINT_REVISION_RE = re.compile(r"^[0-9a-f]{40}$", re.IGNORECASE)
_CHECKPOINT_RESOLUTION_RE = re.compile(
    r"checkpoint (?:cache miss; falling back to Hub|resolved from local cache) "
    r"repo=([^\s]+) revision=([0-9a-f]{40})(?:\s|$)",
    re.IGNORECASE,
)
_CHECKPOINT_LOADED_RE = re.compile(
    r"Checkpoint\s+\S*/snapshots/([0-9a-f]{40})\s+loaded into both models",
    re.IGNORECASE,
)
_GENERATION_CHECKPOINT_EVENT_RE = re.compile(
    r"\b("
    r"(?:(?:math|code)_)?generation(?:_group)?_start(?:ed)?"
    r"|(?:math|code)_generation_group_abort"
    r")\s+(\{.*\})\s*$"
)
_MATH_AUCTION_READINESS_RE = re.compile(
    r"\bmath_auction_readiness\s+(\{.*\})\s*$"
)


# Executed on the miner host through ``sudo -n python3 -``. The probe reads
# only whitelisted, non-secret service settings and aggregate ledger metadata.
# In particular, it never sources the operator EnvironmentFile, never creates
# the SQLite database, and never emits prompts, completions, wallet material,
# or per-attempt identifiers.
_CODE_SELECTOR_CROSSOVER_PROBE_SOURCE = r"""
import hashlib
import json
import math
import os
import re
import sqlite3
import stat
import subprocess
import sys
import urllib.parse

SCHEMA_VERSION = 1
CROSSOVER_ROOT = "/srv/reliquary-miner-pro/state/code-selector-crossover"
CONTRACT_KIND = "reliquary_code_selector_crossover_contract"
ASSIGNMENT_KIND = "reliquary_code_selector_crossover_pair_assignment"
DECISION_KIND = "reliquary_code_selector_crossover_execution_decision"
ASSIGNMENT_ALGORITHM = "sha256_canonical_json_first_byte_lsb_paired_ab_ba_v1"
ASSIGNMENT_DOMAIN = "reliquary-miner-pro/code-selector-crossover/paired-ab-ba/v1"
SCORE_APPLICATION_ID = 0x524B3253
STATE_APPLICATION_ID = 0x52435352
STATE_SCHEMA_VERSION = 1
MAX_CONTRACT_BYTES = 64 * 1024
MAX_RECORD_BYTES = 64 * 1024
ENV_KEYS = (
    "RELIQUARY_CODE_SELECTOR_CROSSOVER_CONTRACT_PATH",
    "RELIQUARY_CODE_SELECTOR_CROSSOVER_CONTRACT_SHA256",
    "RELIQUARY_CODE_SELECTOR_CROSSOVER_SCORE_INDEX_PATH",
    "RELIQUARY_CODE_SELECTOR_CROSSOVER_STATE_PATH",
)
CONTRACT_KEYS = {
    "assignment_algorithm", "assignment_domain", "assignment_unit",
    "checkpoint_n", "checkpoint_repository", "checkpoint_revision",
    "control_policy_id", "end_window", "environment", "experiment_id",
    "kind", "miner_id", "miner_release_revision", "operator_id",
    "public_source_revision", "runtime_profile_sha256", "schema_version",
    "shard_id", "start_window", "topology_id",
    "treatment_artifact_sha256", "treatment_policy_id",
}
ASSIGNMENT_KEYS = {
    "assignment_algorithm", "assignment_material_sha256", "assignment_unit",
    "block_start_window", "contract_sha256", "kind", "public_randomness",
    "public_randomness_round", "randomness_window", "schema_version",
    "sequence", "windows",
}
DECISION_KEYS = {
    "assignment_sha256", "contract_sha256", "execution_arm",
    "fallback_to_control", "itt_arm", "kind", "reason", "schema_version",
    "validation_failure_codes", "validation_status", "window_n",
}
INDEX_METADATA_KEYS = {
    "artifact_checksum", "artifact_manifest_file_sha256",
    "artifact_model_version", "catboost_version", "checkpoint_n",
    "checkpoint_revision", "content_digest", "dataset_manifest_sha256",
    "environment", "exact_k2_model_file_sha256",
    "exact_k2_validation_rows", "feature_index_content_digest",
    "feature_index_file_sha256", "feature_vector_order", "index_kind",
    "model_repository", "population_rows", "prompt_repository",
    "prompt_revision", "public_source_revision", "rank_order", "row_count",
    "schema_version", "score_encoding", "training_window_end",
    "training_window_start",
}
HEX40 = re.compile(r"^[0-9a-f]{40}$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
UNIT = re.compile(r"^[A-Za-z0-9_.@:-]{1,128}\.service$")
TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+-]{0,255}$")


def canonical_json(value):
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def unique_object(pairs):
    out = {}
    for key, value in pairs:
        if key in out:
            raise ValueError("duplicate_json_key")
        out[key] = value
    return out


def reject_constant(_value):
    raise ValueError("nonfinite_json")


def strict_json(payload, maximum):
    if not isinstance(payload, bytes) or not payload or len(payload) > maximum:
        raise ValueError("record_size")
    value = json.loads(
        payload.decode("utf-8", "strict"),
        object_pairs_hook=unique_object,
        parse_constant=reject_constant,
    )
    if not isinstance(value, dict) or canonical_json(value) != payload:
        raise ValueError("record_not_canonical")
    return value


def exact_int(value, *, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError("integer_invalid")
    return value


def exact_text(value, *, maximum=512, token=False):
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or any(ord(character) < 0x20 or ord(character) > 0x7E for character in value)
    ):
        raise ValueError("text_invalid")
    if token and TOKEN.fullmatch(value) is None:
        raise ValueError("token_invalid")
    return value


def exact_sha(value, pattern=HEX64):
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValueError("digest_invalid")
    return value


def regular_file(path, *, maximum=None):
    before = os.lstat(path)
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ValueError("path_not_regular")
    if before.st_size <= 0 or (maximum is not None and before.st_size > maximum):
        raise ValueError("path_size")
    return before


def sha256_file(path):
    before = regular_file(path)
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    after = os.lstat(path)
    if (
        before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns
    ) != (
        after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns
    ):
        raise ValueError("path_changed")
    return digest.hexdigest()


def process_env(pid):
    values = {}
    try:
        raw = open(f"/proc/{pid}/environ", "rb").read()
        for item in raw.split(b"\0"):
            key, separator, value = item.partition(b"=")
            if not separator:
                continue
            decoded = key.decode("ascii", "strict")
            if decoded in ENV_KEYS:
                values[decoded] = value.decode("utf-8", "strict")
    except (OSError, UnicodeError):
        return {}
    return values


def main_pid(unit):
    try:
        result = subprocess.run(
            ["systemctl", "show", "-p", "MainPID", "--value", unit],
            capture_output=True,
            text=True,
            timeout=0.75,
            check=False,
        )
        value = result.stdout.strip()
        return int(value) if value.isdigit() else 0
    except (OSError, subprocess.SubprocessError, ValueError):
        return 0


def readonly_uri(path, *, immutable=False):
    suffix = "?mode=ro&immutable=1" if immutable else "?mode=ro"
    return "file:" + urllib.parse.quote(path, safe="/") + suffix


def validate_contract(path, expected_sha):
    exact_sha(expected_sha)
    expected_path = os.path.join(
        CROSSOVER_ROOT, "contracts", expected_sha + ".json"
    )
    if path != expected_path or os.path.realpath(path) != path:
        raise ValueError("contract_path")
    info = regular_file(path, maximum=MAX_CONTRACT_BYTES)
    with open(path, "rb") as handle:
        payload = handle.read(MAX_CONTRACT_BYTES + 1)
    after = os.lstat(path)
    if (
        info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns
    ) != (
        after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns
    ):
        raise ValueError("contract_changed")
    if hashlib.sha256(payload).hexdigest() != expected_sha:
        raise ValueError("contract_digest")
    record = strict_json(payload, MAX_CONTRACT_BYTES)
    if set(record) != CONTRACT_KEYS:
        raise ValueError("contract_keys")
    if (
        record["schema_version"] != 1
        or record["kind"] != CONTRACT_KIND
        or record["assignment_algorithm"] != ASSIGNMENT_ALGORITHM
        or record["assignment_domain"] != ASSIGNMENT_DOMAIN
        or record["assignment_unit"] != "whole_window"
        or record["environment"] != "opencodeinstruct"
    ):
        raise ValueError("contract_schema")
    for field in (
        "public_source_revision", "checkpoint_revision",
        "miner_release_revision",
    ):
        exact_sha(record[field], HEX40)
    for field in ("treatment_artifact_sha256", "runtime_profile_sha256"):
        exact_sha(record[field])
    for field in (
        "checkpoint_repository", "control_policy_id", "experiment_id",
        "miner_id", "operator_id", "shard_id", "topology_id",
        "treatment_policy_id",
    ):
        exact_text(record[field], token=field != "checkpoint_repository")
    if record["control_policy_id"] == record["treatment_policy_id"]:
        raise ValueError("contract_arms")
    exact_int(record["checkpoint_n"])
    start = exact_int(record["start_window"], minimum=1)
    end = exact_int(record["end_window"], minimum=1)
    if end < start or (end - start + 1) % 2:
        raise ValueError("contract_range")
    return record


def metadata_int(metadata, key, *, minimum):
    value = metadata.get(key)
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise ValueError("index_metadata_integer") from None
    if not isinstance(value, str) or str(parsed) != value or parsed < minimum:
        raise ValueError("index_metadata_integer")
    return parsed


def validate_index(path, contract):
    artifact_sha = contract["treatment_artifact_sha256"]
    expected_path = os.path.join(
        CROSSOVER_ROOT, "score-indexes", artifact_sha + ".sqlite3"
    )
    if path != expected_path or os.path.realpath(path) != path:
        raise ValueError("index_path")
    if sha256_file(path) != artifact_sha:
        raise ValueError("index_digest")
    connection = sqlite3.connect(
        readonly_uri(path, immutable=True), uri=True, isolation_level=None
    )
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        if int(connection.execute("PRAGMA application_id").fetchone()[0]) != SCORE_APPLICATION_ID:
            raise ValueError("index_application_id")
        if int(connection.execute("PRAGMA user_version").fetchone()[0]) != 1:
            raise ValueError("index_schema_version")
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        if tables != {"metadata", "prompt_scores"}:
            raise ValueError("index_tables")
        columns = {
            table: tuple(
                str(row[1])
                for row in connection.execute("PRAGMA table_info(" + table + ")")
            )
            for table in tables
        }
        if columns != {
            "metadata": ("key", "value"),
            "prompt_scores": ("prompt_idx", "score"),
        }:
            raise ValueError("index_columns")
        check = connection.execute("PRAGMA quick_check").fetchone()
        if check is None or str(check[0]).lower() != "ok":
            raise ValueError("index_integrity")
        rows = connection.execute(
            "SELECT key,value,typeof(key),typeof(value) FROM metadata ORDER BY key"
        ).fetchall()
        if any(row[2] != "text" or row[3] != "text" for row in rows):
            raise ValueError("index_metadata_types")
        metadata = {str(row[0]): str(row[1]) for row in rows}
        if len(metadata) != len(rows) or set(metadata) != INDEX_METADATA_KEYS:
            raise ValueError("index_metadata_keys")
        if (
            metadata["index_kind"] != "reliquary_code_exact_k2_score_index"
            or metadata["schema_version"] != "1"
            or metadata["environment"] != contract["environment"]
            or metadata["public_source_revision"] != contract["public_source_revision"]
            or metadata["model_repository"] != contract["checkpoint_repository"]
            or metadata["checkpoint_revision"] != contract["checkpoint_revision"]
            or metadata["checkpoint_n"] != str(contract["checkpoint_n"])
            or metadata["artifact_model_version"] != "catboost_conditional_value_v2"
            or metadata["rank_order"] != "score_desc_prompt_idx_asc"
            or metadata["score_encoding"]
            != "sqlite_real_ieee754_binary64_unquantized"
        ):
            raise ValueError("index_identity")
        for key in (
            "artifact_checksum", "artifact_manifest_file_sha256",
            "content_digest", "dataset_manifest_sha256",
            "exact_k2_model_file_sha256", "feature_index_content_digest",
            "feature_index_file_sha256",
        ):
            exact_sha(metadata[key])
        row_count = metadata_int(metadata, "row_count", minimum=1)
        training_start = metadata_int(
            metadata, "training_window_start", minimum=0
        )
        training_end = metadata_int(metadata, "training_window_end", minimum=0)
        if training_end < training_start or training_end >= contract["start_window"]:
            raise ValueError("index_training_range")
        metadata_int(metadata, "population_rows", minimum=1)
        metadata_int(metadata, "exact_k2_validation_rows", minimum=1)
        stats = connection.execute(
            "SELECT COUNT(*),MIN(prompt_idx),MAX(prompt_idx),"
            "SUM(CASE WHEN typeof(score)!='real' OR score<0.0 OR score>1.0 "
            "OR score!=score THEN 1 ELSE 0 END) FROM prompt_scores"
        ).fetchone()
        if (
            stats is None
            or int(stats[0] or 0) != row_count
            or int(stats[1] if stats[1] is not None else -1) != 0
            or int(stats[2] if stats[2] is not None else -1) != row_count - 1
            or int(stats[3] or 0) != 0
        ):
            raise ValueError("index_coverage")
        return {
            "file_sha256": artifact_sha,
            "row_count": row_count,
            "content_digest": metadata["content_digest"],
            "training_window_start": training_start,
            "training_window_end": training_end,
        }
    finally:
        connection.close()


def validate_assignment(payload, record_sha, contract):
    exact_sha(record_sha)
    if hashlib.sha256(payload).hexdigest() != record_sha:
        raise ValueError("assignment_digest")
    record = strict_json(payload, MAX_RECORD_BYTES)
    if set(record) != ASSIGNMENT_KEYS:
        raise ValueError("assignment_keys")
    if (
        record["schema_version"] != 1
        or record["kind"] != ASSIGNMENT_KIND
        or record["assignment_algorithm"] != ASSIGNMENT_ALGORITHM
        or record["assignment_unit"] != "whole_window"
        or record["contract_sha256"] != contract["_sha256"]
    ):
        raise ValueError("assignment_schema")
    block = exact_int(record["block_start_window"], minimum=1)
    if (
        block < contract["start_window"]
        or block + 1 > contract["end_window"]
        or (block - contract["start_window"]) % 2
        or record["randomness_window"] != block
    ):
        raise ValueError("assignment_range")
    exact_int(record["public_randomness_round"], minimum=1)
    for field in (
        "public_randomness", "assignment_material_sha256",
    ):
        exact_sha(record[field])
    sequence = record["sequence"]
    if sequence not in {"AB", "BA"}:
        raise ValueError("assignment_sequence")
    windows = record["windows"]
    if not isinstance(windows, list) or len(windows) != 2:
        raise ValueError("assignment_windows")
    expected_arms = (
        ("control", "treatment")
        if sequence == "AB"
        else ("treatment", "control")
    )
    clean_windows = []
    for period, window in enumerate(windows):
        if not isinstance(window, dict) or set(window) != {
            "itt_arm", "period", "window_n"
        }:
            raise ValueError("assignment_window_keys")
        if (
            window["period"] != period
            or window["window_n"] != block + period
            or window["itt_arm"] != expected_arms[period]
        ):
            raise ValueError("assignment_window_identity")
        clean_windows.append({
            "window_n": window["window_n"],
            "itt_arm": window["itt_arm"],
        })
    return {
        "block_start_window": block,
        "record_sha256": record_sha,
        "sequence": sequence,
        "public_randomness_round": record["public_randomness_round"],
        "windows": clean_windows,
    }, record


def validate_decision(payload, record_sha, contract, assignment_record, assignment_sha):
    exact_sha(record_sha)
    if hashlib.sha256(payload).hexdigest() != record_sha:
        raise ValueError("decision_digest")
    record = strict_json(payload, MAX_RECORD_BYTES)
    if set(record) != DECISION_KEYS:
        raise ValueError("decision_keys")
    if (
        record["schema_version"] != 1
        or record["kind"] != DECISION_KIND
        or record["contract_sha256"] != contract["_sha256"]
        or record["assignment_sha256"] != assignment_sha
    ):
        raise ValueError("decision_schema")
    window_n = exact_int(record["window_n"], minimum=1)
    assignment_arm = None
    for window in assignment_record["windows"]:
        if window["window_n"] == window_n:
            assignment_arm = window["itt_arm"]
    if assignment_arm is None or record["itt_arm"] != assignment_arm:
        raise ValueError("decision_assignment")
    itt = record["itt_arm"]
    execution = record["execution_arm"]
    status = record["validation_status"]
    failures = record["validation_failure_codes"]
    fallback = record["fallback_to_control"]
    reason = record["reason"]
    if (
        itt not in {"control", "treatment"}
        or execution not in {"control", "treatment"}
        or status not in {"not_required", "passed", "failed"}
        or not isinstance(failures, list)
        or len(failures) > 32
        or any(
            not isinstance(item, str)
            or TOKEN.fullmatch(item) is None
            for item in failures
        )
        or len(set(failures)) != len(failures)
        or not isinstance(fallback, bool)
        or not isinstance(reason, str)
        or TOKEN.fullmatch(reason) is None
    ):
        raise ValueError("decision_fields")
    if itt == "control":
        valid_semantics = (
            execution == "control"
            and status == "not_required"
            and not failures
            and fallback is False
            and reason == "itt_control"
        )
    elif status == "passed":
        valid_semantics = (
            execution == "treatment"
            and not failures
            and fallback is False
            and reason == "itt_treatment_validated"
        )
    else:
        valid_semantics = (
            execution == "control"
            and status == "failed"
            and bool(failures)
            and fallback is True
            and reason == "technical_validation_failed_control_fallback"
        )
    if not valid_semantics:
        raise ValueError("decision_semantics")
    return {
        "window_n": window_n,
        "record_sha256": record_sha,
        "assignment_sha256": assignment_sha,
        "itt_arm": itt,
        "execution_arm": execution,
        "validation_status": status,
        "validation_failure_codes": failures,
        "fallback_to_control": fallback,
        "reason": reason,
    }


def state_columns(connection, table):
    return tuple(
        str(row[1])
        for row in connection.execute("PRAGMA table_info(" + table + ")")
    )


def validate_state(path, contract):
    expected_path = os.path.join(
        CROSSOVER_ROOT, "state", contract["_sha256"] + ".sqlite3"
    )
    if path != expected_path or os.path.realpath(path) != path:
        raise ValueError("state_path")
    regular_file(path)
    connection = sqlite3.connect(
        readonly_uri(path), uri=True, isolation_level=None, timeout=2.0
    )
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=2000")
        connection.execute("BEGIN")
        application_id = int(
            connection.execute("PRAGMA application_id").fetchone()[0]
        )
        user_version = int(
            connection.execute("PRAGMA user_version").fetchone()[0]
        )
        if application_id != STATE_APPLICATION_ID:
            raise ValueError("state_application_id")
        if user_version != STATE_SCHEMA_VERSION:
            raise ValueError("state_schema_version")
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        if tables != {
            "runtime_metadata", "pair_assignments", "window_decisions"
        }:
            raise ValueError("state_tables")
        if (
            state_columns(connection, "runtime_metadata") != ("key", "value")
            or state_columns(connection, "pair_assignments")
            != ("block_start_window", "record_sha256", "record_json")
            or state_columns(connection, "window_decisions")
            != (
                "window_n", "block_start_window", "record_sha256",
                "record_json",
            )
        ):
            raise ValueError("state_columns")
        check = connection.execute("PRAGMA quick_check").fetchone()
        if check is None or str(check[0]).lower() != "ok":
            raise ValueError("state_integrity")
        metadata_rows = connection.execute(
            "SELECT key,value,typeof(key),typeof(value) "
            "FROM runtime_metadata ORDER BY key"
        ).fetchall()
        if (
            metadata_rows != [
                (
                    "contract_sha256", contract["_sha256"],
                    "text", "text",
                )
            ]
        ):
            raise ValueError("state_metadata")
        latest_assignment_row = connection.execute(
            "SELECT block_start_window,record_sha256,record_json "
            "FROM pair_assignments ORDER BY block_start_window DESC LIMIT 1"
        ).fetchone()
        latest_assignment = None
        if latest_assignment_row is not None:
            payload = bytes(latest_assignment_row[2])
            latest_assignment, assignment_record = validate_assignment(
                payload, str(latest_assignment_row[1]), contract
            )
            if latest_assignment["block_start_window"] != int(
                latest_assignment_row[0]
            ):
                raise ValueError("state_assignment_key")

        latest_decision_row = connection.execute(
            "SELECT window_n,block_start_window,record_sha256,record_json "
            "FROM window_decisions ORDER BY window_n DESC LIMIT 1"
        ).fetchone()
        latest_decision = None
        if latest_decision_row is not None:
            assignment_row = connection.execute(
                "SELECT record_sha256,record_json FROM pair_assignments "
                "WHERE block_start_window=?",
                (int(latest_decision_row[1]),),
            ).fetchone()
            if assignment_row is None:
                raise ValueError("state_decision_assignment_missing")
            assignment_sha = str(assignment_row[0])
            decision_assignment, assignment_record = validate_assignment(
                bytes(assignment_row[1]), assignment_sha, contract
            )
            if decision_assignment["block_start_window"] != int(
                latest_decision_row[1]
            ):
                raise ValueError("state_decision_assignment_key")
            latest_decision = validate_decision(
                bytes(latest_decision_row[3]),
                str(latest_decision_row[2]),
                contract,
                assignment_record,
                assignment_sha,
            )
            if latest_decision["window_n"] != int(latest_decision_row[0]):
                raise ValueError("state_decision_key")
        connection.execute("COMMIT")
        return {
            "application_id": application_id,
            "user_version": user_version,
            "contract_sha256": contract["_sha256"],
            "latest_assignment": latest_assignment,
            "latest_decision": latest_decision,
        }
    finally:
        connection.close()


def fail(out, code):
    out["valid"] = False
    out["error"] = str(code or "invalid")[:128]
    print(json.dumps(out, sort_keys=True, separators=(",", ":")))
    raise SystemExit(0)


out = {
    "schema_version": SCHEMA_VERSION,
    "configured": False,
    "valid": False,
    "settings_process_bound": False,
    "process_unit": "",
    "process_pid": 0,
    "lane": "",
    "contract": {},
    "score_index": {},
    "state": {},
    "error": "",
}

try:
    raw_specs = json.loads(sys.argv[1])
    if (
        not isinstance(raw_specs, list)
        or len(raw_specs) > 16
        or any(
            not isinstance(item, list)
            or len(item) != 2
            or not isinstance(item[0], str)
            or UNIT.fullmatch(item[0]) is None
            or not isinstance(item[1], str)
            or len(item[1]) > 1024
            for item in raw_specs
        )
    ):
        fail(out, "unit_specs_invalid")
except (IndexError, TypeError, ValueError, json.JSONDecodeError):
    fail(out, "unit_specs_invalid")

candidates = []
partial = False
for unit, _env_path in raw_specs:
    pid = main_pid(unit)
    live = process_env(pid) if pid > 0 else {}
    present = {key: value for key, value in live.items() if value}
    if present:
        out["configured"] = True
        if set(present) != set(ENV_KEYS):
            partial = True
        candidates.append((unit, pid, present))

if partial or len(candidates) > 1:
    fail(out, "process_activation_ambiguous")
if not candidates:
    out["valid"] = True
    print(json.dumps(out, sort_keys=True, separators=(",", ":")))
    raise SystemExit(0)

unit, pid, values = candidates[0]
if pid <= 0 or set(values) != set(ENV_KEYS):
    fail(out, "process_activation_incomplete")
out["settings_process_bound"] = True
out["process_unit"] = unit
out["process_pid"] = pid
out["lane"] = (
    unit.rsplit("@", 1)[1][:-len(".service")]
    if "@" in unit and unit.endswith(".service")
    else ""
)

try:
    contract_sha = values[
        "RELIQUARY_CODE_SELECTOR_CROSSOVER_CONTRACT_SHA256"
    ]
    contract = validate_contract(
        values["RELIQUARY_CODE_SELECTOR_CROSSOVER_CONTRACT_PATH"],
        contract_sha,
    )
    contract["_sha256"] = contract_sha
    index = validate_index(
        values["RELIQUARY_CODE_SELECTOR_CROSSOVER_SCORE_INDEX_PATH"],
        contract,
    )
    state = validate_state(
        values["RELIQUARY_CODE_SELECTOR_CROSSOVER_STATE_PATH"],
        contract,
    )
except (
    OSError, sqlite3.Error, UnicodeError, ValueError, TypeError,
    OverflowError, json.JSONDecodeError,
) as exc:
    fail(out, str(exc) or type(exc).__name__)

out["contract"] = {
    "sha256": contract_sha,
    "experiment_id": contract["experiment_id"],
    "control_policy_id": contract["control_policy_id"],
    "treatment_policy_id": contract["treatment_policy_id"],
    "treatment_artifact_sha256": contract["treatment_artifact_sha256"],
    "public_source_revision": contract["public_source_revision"],
    "checkpoint_repository": contract["checkpoint_repository"],
    "checkpoint_revision": contract["checkpoint_revision"],
    "checkpoint_n": contract["checkpoint_n"],
    "runtime_profile_sha256": contract["runtime_profile_sha256"],
    "miner_release_revision": contract["miner_release_revision"],
    "start_window": contract["start_window"],
    "end_window": contract["end_window"],
    "environment": contract["environment"],
}
out["score_index"] = index
out["state"] = state
out["valid"] = True
print(json.dumps(out, sort_keys=True, separators=(",", ":")))
""".strip()


_CODE_OVERLAP_ABBA_PROBE_SOURCE = r"""
import hashlib
import json
import math
import os
import re
import sqlite3
import stat
import subprocess
import sys
import urllib.parse

ROOT = "/srv/reliquary-miner-pro/state/code-overlap-abba"
LEDGER = "/srv/reliquary-miner-pro/state/code-auction.sqlite3"
ENV_KEYS = (
    "RELIQUARY_CODE_OVERLAP_ABBA_CONTRACT_PATH",
    "RELIQUARY_CODE_OVERLAP_ABBA_CONTRACT_SHA256",
    "RELIQUARY_CODE_SELECTOR_SHARD_SLOT",
    "RELIQUARY_CODE_SELECTOR_SHARD_COUNT",
    "RELIQUARY_CODE_OUTCOME_LEDGER",
)
CONTRACT_KEYS = {
    "assignment_algorithm", "assignment_domain", "assignment_unit",
    "checkpoint_n", "checkpoint_repository", "checkpoint_revision",
    "control_variant", "end_window", "environment", "experiment_id",
    "feature_index_content_digest", "feature_index_file_sha256",
    "finalized_at_window", "kind", "miner_pair_id",
    "miner_release_revision", "operator_id", "period0_treatment_slot",
    "public_source_revision", "runtime_profile_sha256", "schema_version",
    "selector_policy_id", "selector_policy_version", "selector_strategy",
    "shard_count", "start_window", "topology_id", "treatment_variant",
}
HEX40 = re.compile(r"^[0-9a-f]{40}$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
UNIT = re.compile(r"^[A-Za-z0-9_.@:-]{1,128}\.service$")


def canonical(value):
    return json.dumps(
        value, allow_nan=False, ensure_ascii=True,
        separators=(",", ":"), sort_keys=True,
    ).encode()


def unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_json_key")
        result[key] = value
    return result


def process_env(pid):
    values = {}
    try:
        payload = open(f"/proc/{pid}/environ", "rb").read()
        for item in payload.split(b"\0"):
            key, separator, value = item.partition(b"=")
            if separator and key.decode("ascii", "strict") in ENV_KEYS:
                values[key.decode()] = value.decode("utf-8", "strict")
    except (OSError, UnicodeError):
        return None
    return values


def main_pid(unit):
    try:
        result = subprocess.run(
            ["systemctl", "show", "-p", "MainPID", "--value", unit],
            capture_output=True, text=True, timeout=0.75, check=False,
        )
        value = result.stdout.strip()
        return int(value) if value.isdigit() else 0
    except (OSError, subprocess.SubprocessError, ValueError):
        return 0


def fail(out, reason):
    out["error"] = str(reason or "invalid")[:128]
    print(json.dumps(out, sort_keys=True, separators=(",", ":")))
    raise SystemExit(0)


def regular(path, maximum=0):
    info = os.lstat(path)
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise ValueError("path_not_regular")
    if info.st_size <= 0 or (maximum and info.st_size > maximum):
        raise ValueError("path_size")
    return info


def load_contract(path, digest):
    if HEX64.fullmatch(digest or "") is None:
        raise ValueError("contract_digest")
    expected = os.path.join(ROOT, "contracts", digest + ".json")
    if path != expected or os.path.realpath(path) != path:
        raise ValueError("contract_path")
    before = regular(path, 1024 * 1024)
    payload = open(path, "rb").read()
    after = os.lstat(path)
    if (
        before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns
    ) != (
        after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns
    ):
        raise ValueError("contract_changed")
    if hashlib.sha256(payload).hexdigest() != digest:
        raise ValueError("contract_digest")
    record = json.loads(
        payload.decode("utf-8", "strict"),
        object_pairs_hook=unique,
        parse_constant=lambda _value: (_ for _ in ()).throw(
            ValueError("nonfinite_json")
        ),
    )
    if (
        not isinstance(record, dict)
        or set(record) != CONTRACT_KEYS
        or canonical(record) != payload
        or record["schema_version"] != 1
        or record["kind"] != "reliquary_code_overlap_abba_contract"
        or record["assignment_algorithm"]
        != "period0_slot_complementary_two_shard_ab_ba_v1"
        or record["assignment_domain"]
        != "reliquary-miner-pro/code-overlap-abba/v1"
        or record["assignment_unit"] != "window_shard"
        or record["control_variant"] != "overlap_first_disabled"
        or record["treatment_variant"] != "overlap_first_enabled"
        or record["environment"] != "opencodeinstruct"
        or record["selector_strategy"] != "arithmetic"
        or type(record["shard_count"]) is not int
        or record["shard_count"] != 2
        or type(record["period0_treatment_slot"]) is not int
        or record["period0_treatment_slot"] not in {0, 1}
    ):
        raise ValueError("contract_schema")
    for field in (
        "public_source_revision", "checkpoint_revision",
        "miner_release_revision",
    ):
        if (
            not isinstance(record[field], str)
            or HEX40.fullmatch(record[field]) is None
        ):
            raise ValueError("contract_identity")
    for field in (
        "feature_index_content_digest", "feature_index_file_sha256",
        "runtime_profile_sha256",
    ):
        if (
            not isinstance(record[field], str)
            or HEX64.fullmatch(record[field]) is None
        ):
            raise ValueError("contract_identity")
    for field in (
        "checkpoint_n", "finalized_at_window", "selector_policy_version",
        "start_window", "end_window",
    ):
        if type(record[field]) is not int or record[field] < 0:
            raise ValueError("contract_range")
    if (
        record["start_window"] <= record["finalized_at_window"]
        or record["end_window"] < record["start_window"]
        or (record["end_window"] - record["start_window"] + 1) % 2
    ):
        raise ValueError("contract_range")
    return record


def clean_attempt(row, contract, digest, lane):
    if (
        row["model_repository"] != contract["checkpoint_repository"]
        or row["checkpoint_revision"] != contract["checkpoint_revision"]
        or row["checkpoint_n"] != contract["checkpoint_n"]
        or row["public_source_revision"] != contract["public_source_revision"]
        or row["runtime_profile_hash"] != contract["runtime_profile_sha256"]
        or row["environment"] != contract["environment"]
        or type(row["window_n"]) is not int
        or not contract["start_window"] <= row["window_n"] <= contract["end_window"]
        or HEX64.fullmatch(str(row["assignment_digest"])) is None
        or row["pair_period"] not in {0, 1}
        or row["itt_arm"] not in {"control", "treatment"}
        or row["execution_arm"] not in {"control", "treatment"}
        or row["selector_policy"] not in {"exploit", "explore"}
        or type(row["overlap_candidate_count"]) is not int
        or row["overlap_candidate_count"] < 0
        or row["selected_overlap"] not in {0, 1}
        or row["activated"] not in {0, 1}
        or not isinstance(row["fallback_reason"], str)
        or len(row["fallback_reason"]) > 256
        or any(
            ord(character) < 0x20 or ord(character) > 0x7E
            for character in row["fallback_reason"]
        )
        or not math.isfinite(float(row["assigned_at"]))
    ):
        raise ValueError("assignment_identity")
    offset = row["window_n"] - contract["start_window"]
    treatment_slot = contract["period0_treatment_slot"] ^ (offset % 2)
    expected_arm = (
        "treatment" if lane["shard_slot"] == treatment_slot else "control"
    )
    if (
        row["pair_index"] != offset // 2
        or row["pair_period"] != offset % 2
        or row["itt_arm"] != expected_arm
        or (expected_arm == "control" and row["execution_arm"] != "control")
    ):
        raise ValueError("assignment_semantics")
    expected_digest = hashlib.sha256(canonical({
        "assignment_algorithm": "period0_slot_complementary_two_shard_ab_ba_v1",
        "assignment_unit": "window_shard",
        "contract_sha256": digest,
        "inside_experiment": True,
        "itt_arm": expected_arm,
        "kind": "reliquary_code_overlap_abba_assignment",
        "pair_index": offset // 2,
        "period": offset % 2,
        "reason": (
            "assigned_treatment"
            if expected_arm == "treatment"
            else "assigned_control"
        ),
        "schema_version": 1,
        "shard_slot": lane["shard_slot"],
        "treatment_slot": treatment_slot,
        "window_n": row["window_n"],
    })).hexdigest()
    if row["assignment_digest"] != expected_digest:
        raise ValueError("assignment_digest")
    activated = bool(
        expected_arm == "treatment"
        and row["execution_arm"] == "treatment"
        and row["selector_policy"] == "exploit"
        and row["overlap_candidate_count"] > 0
        and bool(row["selected_overlap"])
        and row["fallback_reason"] == ""
    )
    if bool(row["activated"]) != activated:
        raise ValueError("assignment_activation")
    return {
        "activated": activated,
        "assigned_at": float(row["assigned_at"]),
        "assignment_sha256": row["assignment_digest"],
        "execution_arm": row["execution_arm"],
        "fallback_reason": row["fallback_reason"],
        "itt_arm": row["itt_arm"],
        "overlap_candidate_count": row["overlap_candidate_count"],
        "pair_index": row["pair_index"],
        "period": row["pair_period"],
        "selected_overlap": bool(row["selected_overlap"]),
        "selector_policy": row["selector_policy"],
        "treatment_slot": treatment_slot,
        "window_n": row["window_n"],
    }


def latest_window(connection, contract, digest, lane):
    query = (
        "SELECT assigned_at,model_repository,checkpoint_revision,checkpoint_n,"
        "public_source_revision,runtime_profile_hash,environment,window_n,"
        "assignment_digest,pair_index,pair_period,itt_arm,execution_arm,"
        "selector_policy,overlap_candidate_count,selected_overlap,"
        "activated,fallback_reason FROM selector_start_assignments "
        "WHERE contract_digest=? AND lane=? AND shard_slot=? AND shard_count=2 "
        "AND window_n=(SELECT MAX(window_n) FROM selector_start_assignments "
        "WHERE contract_digest=? AND lane=? AND shard_slot=? AND shard_count=2) "
        "ORDER BY window_n DESC,assigned_at DESC,attempt_key DESC LIMIT 257"
    )
    identity = (digest, lane["lane"], lane["shard_slot"])
    rows = connection.execute(query, identity + identity).fetchall()
    if not rows:
        return None
    if len(rows) > 256:
        raise ValueError("assignment_window_too_large")
    attempts = [clean_attempt(row, contract, digest, lane) for row in rows]
    fallback_counts = {}
    for attempt in attempts:
        reason = attempt["fallback_reason"] or "none"
        fallback_counts[reason] = fallback_counts.get(reason, 0) + 1
    return {
        "activation_count": sum(attempt["activated"] for attempt in attempts),
        "attempt_count": len(attempts),
        "fallback_counts": dict(sorted(fallback_counts.items())),
        "latest_attempt": attempts[0],
        "selected_overlap_count": sum(
            attempt["selected_overlap"] for attempt in attempts
        ),
        "window_n": attempts[0]["window_n"],
    }


out = {
    "schema_version": 1, "configured": False, "valid": False,
    "settings_process_bound": False, "contract": {}, "lanes": [], "error": "",
}
try:
    specs = json.loads(sys.argv[1])
    if (
        not isinstance(specs, list)
        or len(specs) > 16
        or any(
            not isinstance(item, list) or len(item) != 2
            or not isinstance(item[0], str) or UNIT.fullmatch(item[0]) is None
            for item in specs
        )
    ):
        fail(out, "unit_specs_invalid")
except (IndexError, TypeError, ValueError, json.JSONDecodeError):
    fail(out, "unit_specs_invalid")

configured = []
for unit, _env_path in specs:
    pid = main_pid(unit)
    if pid <= 0:
        out["configured"] = True
        fail(out, "process_identity_unavailable")
    values = process_env(pid)
    if values is None:
        out["configured"] = True
        fail(out, "process_environment_unavailable")
    present = {
        key for key in ENV_KEYS[:2] if str(values.get(key) or "")
    }
    if not present:
        continue
    out["configured"] = True
    if present != set(ENV_KEYS[:2]) or any(not values.get(key) for key in ENV_KEYS):
        fail(out, "process_activation_incomplete")
    try:
        slot = int(values["RELIQUARY_CODE_SELECTOR_SHARD_SLOT"])
        count = int(values["RELIQUARY_CODE_SELECTOR_SHARD_COUNT"])
    except ValueError:
        fail(out, "process_shard_invalid")
    lane = unit.rsplit("@", 1)[-1].removesuffix(".service")
    if pid <= 0 or slot not in {0, 1} or count != 2 or not lane:
        fail(out, "process_identity_invalid")
    configured.append({
        "lane": lane, "process_pid": pid, "process_unit": unit,
        "shard_count": count, "shard_slot": slot, "values": values,
    })

if not out["configured"]:
    out["valid"] = True
    print(json.dumps(out, sort_keys=True, separators=(",", ":")))
    raise SystemExit(0)
if (
    len(configured) != 2
    or {lane["shard_slot"] for lane in configured} != {0, 1}
    or any(
        lane["values"][key] != configured[0]["values"][key]
        for lane in configured[1:]
        for key in (ENV_KEYS[0], ENV_KEYS[1], ENV_KEYS[4])
    )
):
    fail(out, "process_pair_invalid")

values = configured[0]["values"]
try:
    contract = load_contract(values[ENV_KEYS[0]], values[ENV_KEYS[1]])
    ledger_path = values[ENV_KEYS[4]]
    if ledger_path != LEDGER or os.path.realpath(ledger_path) != ledger_path:
        raise ValueError("ledger_path")
    regular(ledger_path)
    connection = sqlite3.connect(
        "file:" + urllib.parse.quote(ledger_path, safe="/") + "?mode=ro",
        uri=True, isolation_level=None, timeout=2.0,
    )
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        if (
            int(connection.execute("PRAGMA application_id").fetchone()[0])
            != 1380139340
            or int(connection.execute("PRAGMA user_version").fetchone()[0]) != 1
        ):
            raise ValueError("ledger_identity")
        for lane in configured:
            lane["latest_window"] = latest_window(
                connection, contract, values[ENV_KEYS[1]], lane
            )
    finally:
        connection.close()
except (
    OSError, sqlite3.Error, UnicodeError, ValueError, TypeError,
    OverflowError, json.JSONDecodeError,
) as exc:
    fail(out, str(exc) or type(exc).__name__)

out["contract"] = {
    key: contract[key] for key in (
        "checkpoint_n", "checkpoint_repository", "checkpoint_revision",
        "end_window", "experiment_id", "feature_index_content_digest",
        "feature_index_file_sha256", "miner_release_revision",
        "period0_treatment_slot", "public_source_revision",
        "runtime_profile_sha256", "selector_policy_id",
        "selector_policy_version", "start_window",
    )
}
out["contract"]["sha256"] = values[ENV_KEYS[1]]
out["lanes"] = [
    {key: lane[key] for key in (
        "lane", "latest_window", "process_pid", "process_unit",
        "shard_count", "shard_slot",
    )}
    for lane in sorted(configured, key=lambda row: row["shard_slot"])
]
out["settings_process_bound"] = True
out["valid"] = True
print(json.dumps(out, sort_keys=True, separators=(",", ":")))
""".strip()


_CODE_AUCTION_READINESS_PROBE_SOURCE = r"""
import json
import os
import re
import sqlite3
import stat
import subprocess
import sys
import urllib.parse
import urllib.request

env_path = sys.argv[1] if len(sys.argv) > 1 else ""
miner_unit = sys.argv[2] if len(sys.argv) > 2 else ""
grader_unit = sys.argv[3] if len(sys.argv) > 3 else ""
grader_socket = sys.argv[4] if len(sys.argv) > 4 else ""
grader_bundle = sys.argv[5] if len(sys.argv) > 5 else ""
metrics_port = int(sys.argv[6]) if len(sys.argv) > 6 else 9876
expected_socket_mode = int(sys.argv[7]) if len(sys.argv) > 7 else 0o660

watched_keys = (
    "RELIQUARY_ENVIRONMENT_NAME",
    "RELIQUARY_ENVIRONMENTS",
    "RELIQUARY_ENGINE_MODE",
    "RELIQUARY_LANE_ID",
    "RELIQUARY_REFERENCE_CODE_PRESCREEN_ENABLED",
    "RELIQUARY_CODE_AUCTION_POLICY",
    "RELIQUARY_CODE_OUTCOME_LEDGER",
)


def parse_env_file(path):
    values = {}
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                value = value.strip()
                if (
                    len(value) >= 2
                    and value[0] == value[-1]
                    and value[0] in "'\""
                ):
                    value = value[1:-1]
                if key.strip() in watched_keys:
                    values[key.strip()] = value
    except Exception:
        pass
    return values


def process_env(pid):
    values = {}
    try:
        raw = open(f"/proc/{pid}/environ", "rb").read()
        for item in raw.split(b"\0"):
            if b"=" not in item:
                continue
            key, value = item.split(b"=", 1)
            decoded_key = key.decode("utf-8", "replace")
            if decoded_key in watched_keys:
                values[decoded_key] = value.decode("utf-8", "replace")
    except Exception:
        pass
    return values


def systemctl(action, unit):
    if not unit:
        return ""
    try:
        result = subprocess.run(
            ["systemctl", action, unit],
            capture_output=True,
            text=True,
            timeout=0.75,
            check=False,
        )
        return (result.stdout or result.stderr).strip().splitlines()[0]
    except Exception:
        return ""


def main_pid(unit):
    if not unit:
        return 0
    try:
        result = subprocess.run(
            ["systemctl", "show", "-p", "MainPID", "--value", unit],
            capture_output=True,
            text=True,
            timeout=0.75,
            check=False,
        )
        return int(result.stdout.strip() or 0)
    except Exception:
        return 0


def unit_property(unit, name):
    if not unit:
        return ""
    try:
        result = subprocess.run(
            ["systemctl", "show", "-p", name, "--value", unit],
            capture_output=True,
            text=True,
            timeout=0.75,
            check=False,
        )
        return result.stdout.strip().splitlines()[0]
    except Exception:
        return ""


def bounded_text(value, limit):
    return str(value or "")[:limit]


file_values = parse_env_file(env_path)
pid = main_pid(miner_unit)
live_values = process_env(pid) if pid > 0 else {}

# The lane is injected by the templated systemd unit, rather than duplicated
# in each lane EnvironmentFile.  Bind it to the exact selected unit instance;
# every other watched setting must still match the running process byte for
# byte.  If an operator does put RELIQUARY_LANE_ID in the EnvironmentFile, it
# remains authoritative and must match the process like any other setting.
expected_unit_lane = ""
if "@" in miner_unit and miner_unit.endswith(".service"):
    expected_unit_lane = miner_unit.rsplit("@", 1)[1][:-len(".service")]
file_settings_bound = all(
    file_values.get(key, "") == live_values.get(key, "")
    for key in watched_keys
    if key != "RELIQUARY_LANE_ID"
)
if "RELIQUARY_LANE_ID" in file_values:
    lane_process_bound = (
        file_values.get("RELIQUARY_LANE_ID", "")
        == live_values.get("RELIQUARY_LANE_ID", "")
    )
elif expected_unit_lane:
    lane_process_bound = (
        live_values.get("RELIQUARY_LANE_ID", "") == expected_unit_lane
    )
else:
    lane_process_bound = not live_values.get("RELIQUARY_LANE_ID", "")
settings_process_bound = bool(
    pid > 0 and file_settings_bound and lane_process_bound
)
truthy = {"1", "true", "yes", "on"}
effective_values = live_values if pid > 0 else file_values

out = {
    "schema_version": 1,
    "miner_unit": miner_unit,
    "miner_unit_enablement": systemctl("is-enabled", miner_unit),
    "process_pid": pid,
    "invocation_id": unit_property(miner_unit, "InvocationID"),
    "settings_process_bound": settings_process_bound,
    "environment": (
        effective_values.get("RELIQUARY_ENVIRONMENT_NAME")
        or effective_values.get("RELIQUARY_ENVIRONMENTS")
        or ""
    ),
    "engine_mode": effective_values.get("RELIQUARY_ENGINE_MODE") or "",
    "lane": effective_values.get("RELIQUARY_LANE_ID") or "",
    "prescreen_enabled": (
        effective_values.get("RELIQUARY_REFERENCE_CODE_PRESCREEN_ENABLED", "")
        .strip()
        .lower()
        in truthy
    ),
    "auction_policy": effective_values.get("RELIQUARY_CODE_AUCTION_POLICY", ""),
    "ledger_path": effective_values.get("RELIQUARY_CODE_OUTCOME_LEDGER", ""),
}

if (
    str(out["environment"]).strip().lower() != "opencodeinstruct"
    or str(out["engine_mode"]).strip().lower() != "reference"
):
    print(json.dumps(out, sort_keys=True, separators=(",", ":")))
    raise SystemExit(0)

grader = {
    "unit": grader_unit,
    "active_state": systemctl("is-active", grader_unit),
    "enablement": systemctl("is-enabled", grader_unit),
    "socket_path": grader_socket,
    "socket_ok": False,
    "socket_uid": -1,
    "socket_gid": -1,
    "socket_mode": "",
    "expected_socket_mode": f"{expected_socket_mode:04o}",
    "bundle_link": grader_bundle,
    "bundle_ok": False,
    "bundle_target": "",
    "bundle_source_revision": "",
    "metrics_ok": False,
    "canary_eval_ok_total": 0,
    "canary_case_passed_total": 0,
}
try:
    socket_stat = os.lstat(grader_socket)
    grader["socket_ok"] = stat.S_ISSOCK(socket_stat.st_mode)
    grader["socket_uid"] = int(socket_stat.st_uid)
    grader["socket_gid"] = int(socket_stat.st_gid)
    grader["socket_mode"] = f"{stat.S_IMODE(socket_stat.st_mode):04o}"
except Exception:
    pass

try:
    target = os.path.realpath(grader_bundle)
    grader["bundle_target"] = target
    bundle_root = os.path.realpath(
        os.path.join(os.path.dirname(grader_bundle), "bundles")
    )
    confined = os.path.commonpath([bundle_root, target]) == bundle_root
    grader["bundle_ok"] = bool(
        os.path.islink(grader_bundle) and os.path.isdir(target) and confined
    )
    match = re.match(r"^([0-9a-fA-F]{40})(?:-|$)", os.path.basename(target))
    if match:
        grader["bundle_source_revision"] = match.group(1).lower()
except Exception:
    pass

try:
    with urllib.request.urlopen(
        f"http://127.0.0.1:{metrics_port}/metrics", timeout=0.75
    ) as response:
        metrics = response.read(131072).decode("utf-8", "replace")
    grader["metrics_ok"] = True
    eval_match = re.search(
        r'^grader_eval_total\{status="ok"\}\s+([0-9.eE+-]+)\s*$',
        metrics,
        re.MULTILINE,
    )
    case_match = re.search(
        r'^grader_case_total\{status="passed"\}\s+([0-9.eE+-]+)\s*$',
        metrics,
        re.MULTILINE,
    )
    grader["canary_eval_ok_total"] = int(float(eval_match.group(1))) if eval_match else 0
    grader["canary_case_passed_total"] = int(float(case_match.group(1))) if case_match else 0
except Exception:
    pass
out["grader"] = grader

ledger_path = out["ledger_path"]
ledger = {
    "configured": bool(ledger_path),
    "readonly_ok": False,
    "schema_ok": False,
    "application_id": 0,
    "user_version": 0,
    "quick_check": "",
    "journal_mode": "",
    "partitions": [],
    "generation_outcomes": {
        "table_present": False,
        "schema_ok": False,
        "summaries": [],
        "error": "",
    },
    "terminal_funnel": {
        "extension_table_present": False,
        "terminal_events_table_present": False,
        "schema_ok": False,
        "api_version": 0,
        "storage_mode": "",
        "summaries": [],
        "error": "",
    },
    "error": "",
}
connection = None
if ledger_path:
    try:
        state_root = os.path.realpath("/srv/reliquary-miner-pro/state")
        normalized_path = os.path.realpath(ledger_path)
        if (
            not os.path.isabs(ledger_path)
            or os.path.commonpath([state_root, normalized_path]) != state_root
        ):
            raise ValueError("path_not_absolute")
        quoted_path = urllib.parse.quote(normalized_path, safe="/")
        connection = sqlite3.connect(
            f"file:{quoted_path}?mode=ro",
            uri=True,
            timeout=0.5,
            isolation_level=None,
        )
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=500")
        ledger["application_id"] = int(
            connection.execute("PRAGMA application_id").fetchone()[0]
        )
        ledger["user_version"] = int(
            connection.execute("PRAGMA user_version").fetchone()[0]
        )
        # A full quick/integrity scan on every five-second poll would duplicate
        # work across Code lanes and contend with the live WAL writer. Exact
        # application/schema IDs, table metadata, journal mode, and the
        # partition SELECT below provide the bounded fast-poll health check.
        ledger["quick_check"] = "not_run_fast_poll"
        ledger["journal_mode"] = str(
            connection.execute("PRAGMA journal_mode").fetchone()[0]
        ).lower()
        ledger["readonly_ok"] = True

        attempts_required = {
            "model_repository",
            "checkpoint_revision",
            "checkpoint_n",
            "public_source_revision",
            "runtime_profile_hash",
            "environment",
            "window_n",
            "created_at",
        }
        events_required = {
            "model_repository",
            "checkpoint_revision",
            "checkpoint_n",
            "public_source_revision",
            "runtime_profile_hash",
            "environment",
            "window_n",
            "observed_at",
        }
        attempts_columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(attempts)")
        }
        events_columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(events)")
        }
        partitions_required = {
            "model_repository",
            "checkpoint_revision",
            "checkpoint_n",
            "public_source_revision",
            "runtime_profile_hash",
            "environment",
            "registered_at",
            "miner_source_revision",
        }
        partitions_columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(partitions)")
        }
        ledger["schema_ok"] = bool(
            ledger["application_id"] == 1380139340
            and ledger["user_version"] == 1
            and ledger["journal_mode"] == "wal"
            and partitions_required <= partitions_columns
            and attempts_required <= attempts_columns
            and events_required <= events_columns
        )
        if ledger["schema_ok"]:
            rows = connection.execute(
                "SELECT p.model_repository, p.checkpoint_revision, "
                "p.checkpoint_n, p.public_source_revision, "
                "p.runtime_profile_hash, p.environment, p.registered_at, "
                "p.miner_source_revision FROM partitions AS p "
                "ORDER BY p.registered_at DESC LIMIT 32"
            ).fetchall()
            ledger["partitions"] = [
                {
                    "model_repository": bounded_text(row[0], 512),
                    "checkpoint_revision": bounded_text(row[1], 64),
                    "checkpoint_n": int(row[2] or 0),
                    "public_source_revision": bounded_text(row[3], 64),
                    "runtime_profile_hash": bounded_text(row[4], 128),
                    "environment": bounded_text(row[5], 128),
                    "registered_at": float(row[6] or 0.0),
                    "miner_source_revision": bounded_text(row[7], 128),
                }
                for row in rows
            ]
            generation = ledger["generation_outcomes"]
            generation["table_present"] = bool(
                connection.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='generation_outcomes'"
                ).fetchone()
            )
            if generation["table_present"]:
                generation_required = {
                    "observed_at",
                    "model_repository",
                    "checkpoint_revision",
                    "checkpoint_n",
                    "public_source_revision",
                    "runtime_profile_hash",
                    "environment",
                    "window_n",
                    "lane",
                    "termination",
                }
                generation_columns = {
                    str(row[1])
                    for row in connection.execute(
                        "PRAGMA table_info(generation_outcomes)"
                    )
                }
                generation["schema_ok"] = bool(
                    generation_required <= generation_columns
                )
                if not generation["schema_ok"]:
                    generation["error"] = "schema_incompatible"
                else:
                    try:
                        summary_rows = connection.execute(
                            "SELECT model_repository, checkpoint_revision, "
                            "checkpoint_n, public_source_revision, "
                            "runtime_profile_hash, environment, lane, "
                            "MIN(window_n), MAX(window_n), MAX(observed_at), "
                            "SUM(CASE WHEN termination='complete' "
                            "THEN 1 ELSE 0 END), "
                            "SUM(CASE WHEN termination='local_token_limit' "
                            "THEN 1 ELSE 0 END), "
                            "SUM(CASE WHEN termination='safe_deadline' "
                            "THEN 1 ELSE 0 END) "
                            "FROM generation_outcomes "
                            "WHERE termination IN "
                            "('complete','local_token_limit','safe_deadline') "
                            "AND lane=? AND (model_repository, "
                            "checkpoint_revision, checkpoint_n, "
                            "public_source_revision, runtime_profile_hash, "
                            "environment) IN (SELECT model_repository, "
                            "checkpoint_revision, checkpoint_n, "
                            "public_source_revision, runtime_profile_hash, "
                            "environment FROM partitions "
                            "ORDER BY registered_at DESC LIMIT 32) "
                            "GROUP BY model_repository, checkpoint_revision, "
                            "checkpoint_n, public_source_revision, "
                            "runtime_profile_hash, environment, lane "
                            "ORDER BY MAX(observed_at) DESC LIMIT 32",
                            (str(out["lane"]),),
                        ).fetchall()
                        generation["summaries"] = [
                            {
                                "model_repository": bounded_text(row[0], 512),
                                "checkpoint_revision": bounded_text(row[1], 64),
                                "checkpoint_n": int(row[2] or 0),
                                "public_source_revision": bounded_text(row[3], 64),
                                "runtime_profile_hash": bounded_text(row[4], 128),
                                "environment": bounded_text(row[5], 128),
                                "lane": bounded_text(row[6], 128),
                                "first_window_n": int(row[7] or 0),
                                "last_window_n": int(row[8] or 0),
                                "last_observed_at": float(row[9] or 0.0),
                                "natural_eos_complete": int(row[10] or 0),
                                "local_token_limit": int(row[11] or 0),
                                "safe_deadline": int(row[12] or 0),
                            }
                            for row in summary_rows
                        ]
                    except (OSError, sqlite3.Error) as exc:
                        generation["error"] = type(exc).__name__

            # The terminal-funnel consumer is an additive API over storage
            # user_version=1.  Absence of the extension table identifies a
            # legacy ledger and remains compatible during the miner cutover.
            # Once the table exists, however, the exact versioned contract is
            # mandatory: never interpret a nearby schema as pool/selection or
            # reward evidence.
            terminal = ledger["terminal_funnel"]
            terminal["extension_table_present"] = bool(
                connection.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='ledger_extensions'"
                ).fetchone()
            )
            terminal["terminal_events_table_present"] = bool(
                connection.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='terminal_events'"
                ).fetchone()
            )
            if terminal["extension_table_present"]:
                extension_columns = {
                    str(row[1])
                    for row in connection.execute(
                        "PRAGMA table_info(ledger_extensions)"
                    )
                }
                terminal_columns = {
                    str(row[1])
                    for row in connection.execute(
                        "PRAGMA table_info(terminal_events)"
                    )
                }
                terminal_generation_columns = {
                    str(row[1])
                    for row in connection.execute(
                        "PRAGMA table_info(generation_outcomes)"
                    )
                }
                terminal_required = {
                    "event_key",
                    "observed_at",
                    "model_repository",
                    "checkpoint_revision",
                    "checkpoint_n",
                    "public_source_revision",
                    "runtime_profile_hash",
                    "environment",
                    "attempt_key",
                    "window_n",
                    "stage",
                    "merkle_root",
                    "http_provisional",
                    "accepted_into_pool",
                    "selected",
                    "rewarded",
                }
                terminal_schema_ready = bool(
                    {"extension_name", "api_version", "storage_mode"}
                    <= extension_columns
                    and terminal["terminal_events_table_present"]
                    and terminal_required <= terminal_columns
                    and "attempt_key" in attempts_columns
                    and {
                        "attempt_key",
                        "model_repository",
                        "checkpoint_revision",
                        "checkpoint_n",
                        "public_source_revision",
                        "runtime_profile_hash",
                        "environment",
                    }
                    <= terminal_generation_columns
                    and {
                        "event_key",
                        "attempt_key",
                        "merkle_root",
                        "lifecycle",
                        "accepted",
                        "accepted_into_pool",
                        "selected_for_batch",
                        "rewarded",
                        "reason_code",
                    }
                    <= events_columns
                )
                if not terminal_schema_ready:
                    terminal["error"] = "schema_incompatible"
                else:
                    extension = connection.execute(
                        "SELECT api_version, storage_mode "
                        "FROM ledger_extensions WHERE extension_name=?",
                        ("terminal_funnel",),
                    ).fetchone()
                    if extension is not None:
                        terminal["api_version"] = int(extension[0] or 0)
                        terminal["storage_mode"] = bounded_text(
                            extension[1], 64
                        )
                    if extension is None or tuple(extension) != (
                        2,
                        "v1_additive_compat",
                    ):
                        terminal["error"] = "metadata_incompatible"
                    else:
                        try:
                            summary_query = '''
                                WITH attempt_keys AS (
                                    SELECT attempt_key FROM attempts
                                     WHERE model_repository=?
                                       AND checkpoint_revision=?
                                       AND checkpoint_n=?
                                       AND public_source_revision=?
                                       AND runtime_profile_hash=?
                                       AND environment=?
                                       AND attempt_key != ''
                                    UNION
                                    SELECT attempt_key FROM events
                                     WHERE model_repository=?
                                       AND checkpoint_revision=?
                                       AND checkpoint_n=?
                                       AND public_source_revision=?
                                       AND runtime_profile_hash=?
                                       AND environment=?
                                       AND attempt_key != ''
                                    UNION
                                    SELECT attempt_key FROM generation_outcomes
                                     WHERE model_repository=?
                                       AND checkpoint_revision=?
                                       AND checkpoint_n=?
                                       AND public_source_revision=?
                                       AND runtime_profile_hash=?
                                       AND environment=?
                                       AND attempt_key != ''
                                    UNION
                                    SELECT attempt_key FROM terminal_events
                                     WHERE model_repository=?
                                       AND checkpoint_revision=?
                                       AND checkpoint_n=?
                                       AND public_source_revision=?
                                       AND runtime_profile_hash=?
                                       AND environment=?
                                       AND attempt_key != ''
                                       AND attempt_key NOT LIKE 'r2:%'
                                ), metric_rows AS (
                                    SELECT
                                        COALESCE(
                                            NULLIF(attempt_key, ''),
                                            NULLIF(merkle_root, ''),
                                            event_key
                                        ) AS evidence_key,
                                        CASE WHEN http_provisional=1
                                            THEN 1 ELSE 0 END AS http_provisional,
                                        CASE WHEN stage='receipt_accepted'
                                            THEN 1 ELSE 0 END AS receipt_reserved,
                                        CASE WHEN stage='reveal_sent'
                                            THEN 1 ELSE 0 END AS reveal_sent,
                                        CASE WHEN stage='pool_accepted'
                                                  OR accepted_into_pool=1
                                            THEN 1 ELSE 0 END AS pool_accepted,
                                        CASE WHEN stage='selected' OR selected=1
                                            THEN 1 ELSE 0 END AS selected,
                                        CASE WHEN stage='rewarded' OR rewarded=1
                                            THEN 1 ELSE 0 END AS rewarded,
                                        CASE WHEN stage='terminal_rejected'
                                            THEN 1 ELSE 0 END AS terminal_rejected,
                                        CASE WHEN stage='terminal_unresolved'
                                            THEN 1 ELSE 0 END AS terminal_unresolved,
                                        0 AS definitely_pre_network_expiry,
                                        CASE WHEN stage IN (
                                                'precommit_sent',
                                                'receipt_accepted',
                                                'reveal_sent',
                                                'immediate_response',
                                                'pool_accepted',
                                                'pool_rejected',
                                                'ranked',
                                                'validator_proof_attempted',
                                                'validator_proof_passed',
                                                'validator_proof_failed',
                                                'selected',
                                                'not_selected',
                                                'rewarded',
                                                'not_rewarded',
                                                'terminal_rejected'
                                            )
                                              OR http_provisional=1
                                              OR accepted_into_pool IS NOT NULL
                                              OR selected IS NOT NULL
                                              OR rewarded IS NOT NULL
                                            THEN 1 ELSE 0 END AS network_stage
                                    FROM terminal_events
                                    WHERE model_repository=?
                                      AND checkpoint_revision=?
                                      AND checkpoint_n=?
                                      AND public_source_revision=?
                                      AND runtime_profile_hash=?
                                      AND environment=?
                                      AND attempt_key NOT LIKE 'r2:%'
                                    UNION ALL
                                    SELECT
                                        COALESCE(
                                            NULLIF(attempt_key, ''),
                                            NULLIF(merkle_root, ''),
                                            event_key
                                        ) AS evidence_key,
                                        CASE WHEN lifecycle='immediate'
                                                  AND accepted=1
                                            THEN 1 ELSE 0 END AS http_provisional,
                                        0 AS receipt_reserved,
                                        0 AS reveal_sent,
                                        CASE WHEN accepted_into_pool=1
                                            THEN 1 ELSE 0 END AS pool_accepted,
                                        CASE WHEN lifecycle='auction_final'
                                                  AND selected_for_batch=1
                                            THEN 1 ELSE 0 END AS selected,
                                        CASE WHEN lifecycle='auction_final'
                                                  AND rewarded=1
                                            THEN 1 ELSE 0 END AS rewarded,
                                        0 AS terminal_rejected,
                                        CASE WHEN lifecycle='transport_error'
                                            THEN 1 ELSE 0 END AS terminal_unresolved,
                                        CASE WHEN lifecycle='transport_error'
                                                  AND reason_code=
                                                      '_DrandSendTicketExpired'
                                            THEN 1 ELSE 0 END
                                            AS definitely_pre_network_expiry,
                                        CASE WHEN lifecycle IN (
                                                'precommit_sent',
                                                'receipt_accepted',
                                                'reveal_sent',
                                                'immediate',
                                                'pool_admission',
                                                'auction_final',
                                                'terminal_rejected'
                                            )
                                              OR accepted_into_pool IS NOT NULL
                                              OR selected_for_batch IS NOT NULL
                                              OR rewarded IS NOT NULL
                                            THEN 1 ELSE 0 END AS network_stage
                                    FROM events
                                    WHERE model_repository=?
                                      AND checkpoint_revision=?
                                      AND checkpoint_n=?
                                      AND public_source_revision=?
                                      AND runtime_profile_hash=?
                                      AND environment=?
                                ), metric_rollup AS (
                                    SELECT
                                        evidence_key,
                                        MAX(http_provisional) AS http_provisional,
                                        MAX(receipt_reserved) AS receipt_reserved,
                                        MAX(reveal_sent) AS reveal_sent,
                                        MAX(pool_accepted) AS pool_accepted,
                                        MAX(selected) AS selected,
                                        MAX(rewarded) AS rewarded,
                                        MAX(terminal_rejected) AS terminal_rejected,
                                        MAX(terminal_unresolved) AS terminal_unresolved,
                                        MAX(definitely_pre_network_expiry)
                                            AS definitely_pre_network_expiry,
                                        MAX(network_stage) AS network_stage
                                    FROM metric_rows
                                    WHERE evidence_key IS NOT NULL
                                    GROUP BY evidence_key
                                )
                                SELECT
                                    (SELECT COUNT(*) FROM attempt_keys) AS attempts,
                                    COALESCE(SUM(http_provisional), 0),
                                    COALESCE(SUM(receipt_reserved), 0),
                                    COALESCE(SUM(reveal_sent), 0),
                                    COALESCE(SUM(pool_accepted), 0),
                                    COALESCE(SUM(selected), 0),
                                    COALESCE(SUM(rewarded), 0),
                                    COALESCE(SUM(terminal_rejected), 0),
                                    COALESCE(SUM(
                                        CASE
                                            WHEN terminal_unresolved=1
                                              AND NOT (
                                                definitely_pre_network_expiry=1
                                                AND network_stage=0
                                              )
                                            THEN 1
                                            ELSE 0
                                        END
                                    ), 0)
                                FROM metric_rollup
                            '''
                            for partition_row in ledger["partitions"]:
                                identity = (
                                    partition_row["model_repository"],
                                    partition_row["checkpoint_revision"],
                                    partition_row["checkpoint_n"],
                                    partition_row["public_source_revision"],
                                    partition_row["runtime_profile_hash"],
                                    partition_row["environment"],
                                )
                                summary = connection.execute(
                                    summary_query,
                                    (*identity, *identity, *identity,
                                     *identity, *identity, *identity),
                                ).fetchone()
                                terminal["summaries"].append(
                                    {
                                        **{
                                            key: partition_row[key]
                                            for key in (
                                                "model_repository",
                                                "checkpoint_revision",
                                                "checkpoint_n",
                                                "public_source_revision",
                                                "runtime_profile_hash",
                                                "environment",
                                            )
                                        },
                                        "attempts": int(summary[0] or 0),
                                        "http_provisional": int(summary[1] or 0),
                                        "receipt_reserved": int(summary[2] or 0),
                                        "reveal_sent": int(summary[3] or 0),
                                        "pool_accepted": int(summary[4] or 0),
                                        "selected": int(summary[5] or 0),
                                        "rewarded": int(summary[6] or 0),
                                        "terminal_rejected": int(summary[7] or 0),
                                        "terminal_unresolved": int(summary[8] or 0),
                                    }
                                )
                            terminal["schema_ok"] = True
                        except (OSError, sqlite3.Error) as exc:
                            terminal["error"] = type(exc).__name__
                            terminal["schema_ok"] = False
    except Exception as exc:
        ledger["error"] = type(exc).__name__
    finally:
        if connection is not None:
            connection.close()
out["ledger"] = ledger

print(json.dumps(out, sort_keys=True, separators=(",", ":")))
""".strip()


def _parse_code_selector_crossover_probe(
    lines: list[str],
) -> dict[str, Any]:
    """Parse only the bounded, non-secret crossover projection.

    The remote source does the expensive artifact/state validation. This
    second boundary deliberately rebuilds a fixed output schema rather than
    forwarding arbitrary JSON into HTTP exports.
    """
    if not lines:
        return {}
    encoded = str(lines[0] or "").encode("utf-8", "replace")
    if len(encoded) > 65_536:
        return {}
    try:
        raw = json.loads(encoded.decode("utf-8", "strict") or "{}")
    except (UnicodeError, ValueError, OverflowError, json.JSONDecodeError):
        return {}
    expected_root = {
        "schema_version",
        "configured",
        "valid",
        "settings_process_bound",
        "process_unit",
        "process_pid",
        "lane",
        "contract",
        "score_index",
        "state",
        "error",
    }
    if not isinstance(raw, dict) or set(raw) != expected_root:
        return {}
    if (
        type(raw.get("schema_version")) is not int
        or raw["schema_version"] != 1
        or type(raw.get("configured")) is not bool
        or type(raw.get("valid")) is not bool
        or type(raw.get("settings_process_bound")) is not bool
    ):
        return {}

    def bounded_text(value: Any, limit: int) -> str | None:
        if not isinstance(value, str) or len(value) > limit:
            return None
        if any(ord(character) < 0x20 for character in value):
            return None
        return value

    def exact_int(value: Any, minimum: int = 0) -> int | None:
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < minimum
            or value > 2**63 - 1
        ):
            return None
        return value

    def exact_sha(value: Any, length: int = 64) -> str | None:
        if (
            not isinstance(value, str)
            or len(value) != length
            or re.fullmatch(rf"[0-9a-f]{{{length}}}", value) is None
        ):
            return None
        return value

    configured = raw["configured"]
    valid = raw["valid"]
    error = bounded_text(raw.get("error"), 128)
    if error is None:
        return {}
    base: dict[str, Any] = {
        "schema_version": 1,
        "configured": configured,
        "valid": valid,
        "settings_process_bound": raw["settings_process_bound"],
        "process_unit": "",
        "process_pid": 0,
        "lane": "",
        "contract": {},
        "score_index": {},
        "state": {},
        "error": error,
    }
    if not configured:
        # Absence is a valid control-only posture; no process or artifact facts
        # are meaningful in this branch.
        if not valid or raw["settings_process_bound"] or error:
            return {}
        return base

    process_unit = bounded_text(raw.get("process_unit"), 128)
    process_pid = exact_int(raw.get("process_pid"), 1)
    lane = bounded_text(raw.get("lane"), 128)
    if (
        process_unit is None
        or process_pid is None
        or lane is None
        or not process_unit
        or _SYSTEMD_UNIT_RE.fullmatch(process_unit) is None
    ):
        return {}
    base.update(
        {
            "process_unit": process_unit,
            "process_pid": process_pid,
            "lane": lane,
        }
    )
    if not valid:
        # Preserve the fact that a live process requested crossover activation,
        # but never forward partially validated artifacts or decisions.
        if not error:
            return {}
        return base
    if error or not raw["settings_process_bound"]:
        return {}

    contract_raw = raw.get("contract")
    expected_contract = {
        "sha256",
        "experiment_id",
        "control_policy_id",
        "treatment_policy_id",
        "treatment_artifact_sha256",
        "public_source_revision",
        "checkpoint_repository",
        "checkpoint_revision",
        "checkpoint_n",
        "runtime_profile_sha256",
        "miner_release_revision",
        "start_window",
        "end_window",
        "environment",
    }
    if not isinstance(contract_raw, dict) or set(contract_raw) != expected_contract:
        return {}
    contract: dict[str, Any] = {}
    for key, limit in (
        ("experiment_id", 256),
        ("control_policy_id", 256),
        ("treatment_policy_id", 256),
        ("checkpoint_repository", 512),
        ("environment", 128),
    ):
        value = bounded_text(contract_raw.get(key), limit)
        if value is None or not value:
            return {}
        contract[key] = value
    for key in (
        "sha256",
        "treatment_artifact_sha256",
        "runtime_profile_sha256",
    ):
        value = exact_sha(contract_raw.get(key))
        if value is None:
            return {}
        contract[key] = value
    for key in (
        "public_source_revision",
        "checkpoint_revision",
        "miner_release_revision",
    ):
        value = exact_sha(contract_raw.get(key), 40)
        if value is None:
            return {}
        contract[key] = value
    for key, minimum in (
        ("checkpoint_n", 0),
        ("start_window", 1),
        ("end_window", 1),
    ):
        value = exact_int(contract_raw.get(key), minimum)
        if value is None:
            return {}
        contract[key] = value
    if (
        contract["environment"] != "opencodeinstruct"
        or contract["end_window"] < contract["start_window"]
        or (contract["end_window"] - contract["start_window"] + 1) % 2
    ):
        return {}

    index_raw = raw.get("score_index")
    expected_index = {
        "file_sha256",
        "row_count",
        "content_digest",
        "training_window_start",
        "training_window_end",
    }
    if not isinstance(index_raw, dict) or set(index_raw) != expected_index:
        return {}
    index_sha = exact_sha(index_raw.get("file_sha256"))
    content_digest = exact_sha(index_raw.get("content_digest"))
    row_count = exact_int(index_raw.get("row_count"), 1)
    training_start = exact_int(index_raw.get("training_window_start"), 0)
    training_end = exact_int(index_raw.get("training_window_end"), 0)
    if (
        index_sha is None
        or content_digest is None
        or row_count is None
        or training_start is None
        or training_end is None
        or index_sha != contract["treatment_artifact_sha256"]
        or training_end < training_start
        or training_end >= contract["start_window"]
    ):
        return {}
    score_index = {
        "file_sha256": index_sha,
        "row_count": row_count,
        "content_digest": content_digest,
        "training_window_start": training_start,
        "training_window_end": training_end,
    }

    def assignment(value: Any) -> dict[str, Any] | None:
        if value is None:
            return None
        expected = {
            "block_start_window",
            "record_sha256",
            "sequence",
            "public_randomness_round",
            "windows",
        }
        if not isinstance(value, dict) or set(value) != expected:
            raise ValueError("assignment")
        block = exact_int(value.get("block_start_window"), 1)
        digest = exact_sha(value.get("record_sha256"))
        sequence = bounded_text(value.get("sequence"), 2)
        round_n = exact_int(value.get("public_randomness_round"), 1)
        windows = value.get("windows")
        if (
            block is None
            or digest is None
            or sequence not in {"AB", "BA"}
            or round_n is None
            or not isinstance(windows, list)
            or len(windows) != 2
        ):
            raise ValueError("assignment")
        clean_windows = []
        for period, row in enumerate(windows):
            if not isinstance(row, dict) or set(row) != {"window_n", "itt_arm"}:
                raise ValueError("assignment")
            window_n = exact_int(row.get("window_n"), 1)
            arm = row.get("itt_arm")
            if (
                window_n != block + period
                or arm not in {"control", "treatment"}
            ):
                raise ValueError("assignment")
            clean_windows.append({"window_n": window_n, "itt_arm": arm})
        return {
            "block_start_window": block,
            "record_sha256": digest,
            "sequence": sequence,
            "public_randomness_round": round_n,
            "windows": clean_windows,
        }

    def decision(value: Any) -> dict[str, Any] | None:
        if value is None:
            return None
        expected = {
            "window_n",
            "record_sha256",
            "assignment_sha256",
            "itt_arm",
            "execution_arm",
            "validation_status",
            "validation_failure_codes",
            "fallback_to_control",
            "reason",
        }
        if not isinstance(value, dict) or set(value) != expected:
            raise ValueError("decision")
        window_n = exact_int(value.get("window_n"), 1)
        record_sha = exact_sha(value.get("record_sha256"))
        assignment_sha = exact_sha(value.get("assignment_sha256"))
        itt = value.get("itt_arm")
        execution = value.get("execution_arm")
        status = value.get("validation_status")
        failures = value.get("validation_failure_codes")
        fallback = value.get("fallback_to_control")
        reason = bounded_text(value.get("reason"), 256)
        if (
            window_n is None
            or record_sha is None
            or assignment_sha is None
            or itt not in {"control", "treatment"}
            or execution not in {"control", "treatment"}
            or status not in {"not_required", "passed", "failed"}
            or not isinstance(failures, list)
            or len(failures) > 32
            or any(
                bounded_text(item, 256) is None or not item for item in failures
            )
            or len(set(failures)) != len(failures)
            or not isinstance(fallback, bool)
            or reason is None
            or not reason
        ):
            raise ValueError("decision")
        return {
            "window_n": window_n,
            "record_sha256": record_sha,
            "assignment_sha256": assignment_sha,
            "itt_arm": itt,
            "execution_arm": execution,
            "validation_status": status,
            "validation_failure_codes": list(failures),
            "fallback_to_control": fallback,
            "reason": reason,
        }

    state_raw = raw.get("state")
    expected_state = {
        "application_id",
        "user_version",
        "contract_sha256",
        "latest_assignment",
        "latest_decision",
    }
    if not isinstance(state_raw, dict) or set(state_raw) != expected_state:
        return {}
    application_id = exact_int(state_raw.get("application_id"), 1)
    user_version = exact_int(state_raw.get("user_version"), 1)
    state_contract = exact_sha(state_raw.get("contract_sha256"))
    try:
        latest_assignment = assignment(state_raw.get("latest_assignment"))
        latest_decision = decision(state_raw.get("latest_decision"))
    except ValueError:
        return {}
    if (
        application_id != 0x52435352
        or user_version != 1
        or state_contract != contract["sha256"]
    ):
        return {}
    if latest_decision is not None:
        if (
            latest_decision["window_n"] < contract["start_window"]
            or latest_decision["window_n"] > contract["end_window"]
        ):
            return {}
    state = {
        "application_id": application_id,
        "user_version": user_version,
        "contract_sha256": state_contract,
        "latest_assignment": latest_assignment,
        "latest_decision": latest_decision,
    }
    base.update(
        {
            "contract": contract,
            "score_index": score_index,
            "state": state,
        }
    )
    return base


def _parse_code_overlap_abba_probe(lines: list[str]) -> dict[str, Any]:
    """Whitelist the bounded process/contract/assignment projection."""
    if not lines:
        return {}
    encoded = str(lines[0] or "").encode("utf-8", "replace")
    if len(encoded) > 65_536:
        return {}
    try:
        raw = json.loads(encoded.decode("utf-8", "strict") or "{}")
    except (UnicodeError, ValueError, OverflowError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict) or set(raw) != {
        "schema_version", "configured", "valid", "settings_process_bound",
        "contract", "lanes", "error",
    }:
        return {}
    if (
        type(raw.get("schema_version")) is not int
        or raw["schema_version"] != 1
        or type(raw.get("configured")) is not bool
        or type(raw.get("valid")) is not bool
        or type(raw.get("settings_process_bound")) is not bool
        or not isinstance(raw.get("error"), str)
        or len(raw["error"]) > 128
    ):
        return {}
    base = {
        "schema_version": 1,
        "configured": raw["configured"],
        "valid": raw["valid"],
        "settings_process_bound": raw["settings_process_bound"],
        "contract": {},
        "lanes": [],
        "error": raw["error"],
    }
    if not raw["configured"]:
        return (
            base
            if raw["valid"]
            and not raw["settings_process_bound"]
            and raw["contract"] == {}
            and raw["lanes"] == []
            and not raw["error"]
            else {}
        )
    if not raw["valid"]:
        return (
            base
            if not raw["settings_process_bound"]
            and raw["contract"] == {}
            and raw["lanes"] == []
            and bool(raw["error"])
            else {}
        )
    if raw["error"] or not raw["settings_process_bound"]:
        return {}

    sha40 = re.compile(r"^[0-9a-f]{40}$")
    sha64 = re.compile(r"^[0-9a-f]{64}$")
    contract = raw.get("contract")
    if not isinstance(contract, dict) or set(contract) != {
        "checkpoint_n", "checkpoint_repository", "checkpoint_revision",
        "end_window", "experiment_id", "feature_index_content_digest",
        "feature_index_file_sha256", "miner_release_revision",
        "period0_treatment_slot", "public_source_revision",
        "runtime_profile_sha256", "selector_policy_id",
        "selector_policy_version", "sha256", "start_window",
    }:
        return {}
    for key in (
        "checkpoint_repository", "experiment_id", "selector_policy_id",
    ):
        if (
            not isinstance(contract.get(key), str)
            or not contract[key]
            or len(contract[key]) > 512
        ):
            return {}
    for key in (
        "feature_index_content_digest", "feature_index_file_sha256",
        "runtime_profile_sha256", "sha256",
    ):
        if not isinstance(contract.get(key), str) or not sha64.fullmatch(contract[key]):
            return {}
    for key in (
        "checkpoint_revision", "miner_release_revision",
        "public_source_revision",
    ):
        if not isinstance(contract.get(key), str) or not sha40.fullmatch(contract[key]):
            return {}
    for key in (
        "checkpoint_n", "end_window", "period0_treatment_slot",
        "selector_policy_version", "start_window",
    ):
        if type(contract.get(key)) is not int or contract[key] < 0:
            return {}
    if (
        contract["period0_treatment_slot"] not in {0, 1}
        or contract["start_window"] <= 0
        or contract["end_window"] < contract["start_window"]
        or (contract["end_window"] - contract["start_window"] + 1) % 2
    ):
        return {}

    clean_lanes = []
    lanes = raw.get("lanes")
    if not isinstance(lanes, list) or len(lanes) != 2:
        return {}
    for lane in lanes:
        if not isinstance(lane, dict) or set(lane) != {
            "lane", "latest_window", "process_pid", "process_unit",
            "shard_count", "shard_slot",
        }:
            return {}
        if (
            not isinstance(lane.get("lane"), str)
            or not lane["lane"]
            or not isinstance(lane.get("process_unit"), str)
            or _SYSTEMD_UNIT_RE.fullmatch(lane["process_unit"]) is None
            or type(lane.get("process_pid")) is not int
            or lane["process_pid"] <= 0
            or type(lane.get("shard_slot")) is not int
            or lane["shard_slot"] not in {0, 1}
            or type(lane.get("shard_count")) is not int
            or lane["shard_count"] != 2
            or lane["lane"]
            != lane["process_unit"].rsplit("@", 1)[-1].removesuffix(
                ".service"
            )
        ):
            return {}

        latest_window = lane.get("latest_window")
        if latest_window is not None:
            if not isinstance(latest_window, dict) or set(latest_window) != {
                "activation_count", "attempt_count", "fallback_counts",
                "latest_attempt", "selected_overlap_count", "window_n",
            }:
                return {}
            activation_count = latest_window.get("activation_count")
            attempt_count = latest_window.get("attempt_count")
            selected_overlap_count = latest_window.get(
                "selected_overlap_count"
            )
            window_n = latest_window.get("window_n")
            fallback_counts = latest_window.get("fallback_counts")
            latest = latest_window.get("latest_attempt")
            if (
                type(activation_count) is not int
                or activation_count < 0
                or type(attempt_count) is not int
                or not 1 <= attempt_count <= 256
                or activation_count > attempt_count
                or type(selected_overlap_count) is not int
                or not 0 <= selected_overlap_count <= attempt_count
                or type(window_n) is not int
                or not contract["start_window"]
                <= window_n
                <= contract["end_window"]
                or not isinstance(fallback_counts, dict)
                or not 1 <= len(fallback_counts) <= 256
                or not isinstance(latest, dict)
            ):
                return {}
            fallback_total = 0
            for reason, count in fallback_counts.items():
                if (
                    not isinstance(reason, str)
                    or not reason
                    or len(reason) > 256
                    or any(
                        ord(character) < 0x20 or ord(character) > 0x7E
                        for character in reason
                    )
                    or type(count) is not int
                    or count <= 0
                ):
                    return {}
                fallback_total += count
            if fallback_total != attempt_count:
                return {}
            if set(latest) != {
                "activated", "assigned_at", "assignment_sha256",
                "execution_arm", "fallback_reason", "itt_arm", "pair_index",
                "overlap_candidate_count", "period", "selected_overlap",
                "selector_policy", "treatment_slot", "window_n",
            }:
                return {}
            if (
                type(latest.get("activated")) is not bool
                or not isinstance(latest.get("assigned_at"), (int, float))
                or isinstance(latest["assigned_at"], bool)
                or not math.isfinite(float(latest["assigned_at"]))
                or not isinstance(latest.get("assignment_sha256"), str)
                or not sha64.fullmatch(latest["assignment_sha256"])
                or latest.get("execution_arm") not in {"control", "treatment"}
                or latest.get("itt_arm") not in {"control", "treatment"}
                or not isinstance(latest.get("fallback_reason"), str)
                or len(latest["fallback_reason"]) > 256
                or any(
                    ord(character) < 0x20 or ord(character) > 0x7E
                    for character in latest["fallback_reason"]
                )
                or type(latest.get("overlap_candidate_count")) is not int
                or latest["overlap_candidate_count"] < 0
                or type(latest.get("pair_index")) is not int
                or latest["pair_index"] < 0
                or type(latest.get("period")) is not int
                or latest["period"] not in {0, 1}
                or type(latest.get("selected_overlap")) is not bool
                or latest.get("selector_policy") not in {"exploit", "explore"}
                or type(latest.get("treatment_slot")) is not int
                or latest["treatment_slot"] not in {0, 1}
                or type(latest.get("window_n")) is not int
                or latest["window_n"] != window_n
            ):
                return {}
            offset = latest["window_n"] - contract["start_window"]
            expected_slot = contract["period0_treatment_slot"] ^ (offset % 2)
            expected_arm = (
                "treatment"
                if lane["shard_slot"] == expected_slot
                else "control"
            )
            expected_assignment_sha = hashlib.sha256(
                json.dumps(
                    {
                        "assignment_algorithm":
                            "period0_slot_complementary_two_shard_ab_ba_v1",
                        "assignment_unit": "window_shard",
                        "contract_sha256": contract["sha256"],
                        "inside_experiment": True,
                        "itt_arm": expected_arm,
                        "kind":
                            "reliquary_code_overlap_abba_assignment",
                        "pair_index": offset // 2,
                        "period": offset % 2,
                        "reason": (
                            "assigned_treatment"
                            if expected_arm == "treatment"
                            else "assigned_control"
                        ),
                        "schema_version": 1,
                        "shard_slot": lane["shard_slot"],
                        "treatment_slot": expected_slot,
                        "window_n": latest["window_n"],
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
            ).hexdigest()
            if (
                offset < 0
                or latest["window_n"] > contract["end_window"]
                or latest["pair_index"] != offset // 2
                or latest["period"] != offset % 2
                or latest["treatment_slot"] != expected_slot
                or latest["itt_arm"] != expected_arm
                or latest["assignment_sha256"] != expected_assignment_sha
                or (
                    expected_arm == "control"
                    and latest["execution_arm"] != "control"
                )
                or (
                    latest["activated"]
                    != (
                        expected_arm == "treatment"
                        and latest["execution_arm"] == "treatment"
                        and latest["selector_policy"] == "exploit"
                        and latest["overlap_candidate_count"] > 0
                        and latest["selected_overlap"]
                        and latest["fallback_reason"] == ""
                    )
                )
                or int(latest["activated"]) > activation_count
                or int(latest["selected_overlap"]) > selected_overlap_count
                or fallback_counts.get(
                    latest["fallback_reason"] or "none", 0
                ) < 1
            ):
                return {}
            latest_window = {
                "activation_count": activation_count,
                "attempt_count": attempt_count,
                "fallback_counts": dict(fallback_counts),
                "latest_attempt": dict(latest),
                "selected_overlap_count": selected_overlap_count,
                "window_n": window_n,
            }
        clean_lanes.append({**lane, "latest_window": latest_window})
    clean_lanes.sort(key=lambda lane: lane["shard_slot"])
    if (
        {lane["shard_slot"] for lane in clean_lanes} != {0, 1}
        or len({lane["process_unit"] for lane in clean_lanes}) != 2
        or len({lane["process_pid"] for lane in clean_lanes}) != 2
    ):
        return {}
    return {**base, "contract": dict(contract), "lanes": clean_lanes}


def _parse_code_auction_probe(lines: list[str]) -> dict[str, Any]:
    """Return a bounded, JSON-safe readiness payload from one SSH section."""
    if not lines:
        return {}
    encoded = str(lines[0] or "").encode("utf-8", "replace")
    if len(encoded) > 131_072:
        return {}
    try:
        raw = json.loads(encoded.decode("utf-8", "replace") or "{}")
    except (
        TypeError,
        ValueError,
        OverflowError,
        json.JSONDecodeError,
    ):
        return {}
    if not isinstance(raw, dict):
        return {}

    def text_value(mapping: dict, key: str, limit: int = 512) -> str:
        return str(mapping.get(key) or "")[:limit]

    def int_value(
        mapping: dict,
        key: str,
        default: int = 0,
        minimum: int | None = None,
    ) -> int:
        value = mapping.get(key, default)
        if isinstance(value, bool):
            return default
        try:
            parsed = int(value)
        except (TypeError, ValueError, OverflowError):
            return default
        if minimum is not None and parsed < minimum:
            return default
        if abs(parsed) > 2**63 - 1:
            return default
        return parsed

    def float_value(
        mapping: dict,
        key: str,
        default: float = 0.0,
        minimum: float | None = None,
    ) -> float:
        value = mapping.get(key, default)
        if isinstance(value, bool):
            return default
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return default
        if not math.isfinite(parsed):
            return default
        if minimum is not None and parsed < minimum:
            return default
        return parsed

    grader_raw = raw.get("grader")
    grader_raw = grader_raw if isinstance(grader_raw, dict) else {}
    grader = {
        "unit": text_value(grader_raw, "unit", 128),
        "active_state": text_value(grader_raw, "active_state", 64),
        "enablement": text_value(grader_raw, "enablement", 64),
        "socket_path": text_value(grader_raw, "socket_path"),
        "socket_ok": grader_raw.get("socket_ok") is True,
        "socket_uid": int_value(grader_raw, "socket_uid", -1, -1),
        "socket_gid": int_value(grader_raw, "socket_gid", -1, -1),
        "socket_mode": text_value(grader_raw, "socket_mode", 8),
        "expected_socket_mode": text_value(
            grader_raw, "expected_socket_mode", 8
        ),
        "bundle_link": text_value(grader_raw, "bundle_link"),
        "bundle_ok": grader_raw.get("bundle_ok") is True,
        "bundle_target": text_value(grader_raw, "bundle_target"),
        "bundle_source_revision": text_value(
            grader_raw, "bundle_source_revision", 64
        ),
        "metrics_ok": grader_raw.get("metrics_ok") is True,
        "canary_eval_ok_total": int_value(
            grader_raw, "canary_eval_ok_total", 0, 0
        ),
        "canary_case_passed_total": int_value(
            grader_raw, "canary_case_passed_total", 0, 0
        ),
    }

    ledger_raw = raw.get("ledger")
    ledger_raw = ledger_raw if isinstance(ledger_raw, dict) else {}
    partitions: list[dict[str, Any]] = []
    raw_partitions = ledger_raw.get("partitions")
    if isinstance(raw_partitions, list):
        for row in raw_partitions[:32]:
            if not isinstance(row, dict):
                continue
            try:
                partitions.append(
                    {
                        "model_repository": text_value(
                            row, "model_repository", 512
                        ),
                        "checkpoint_revision": text_value(
                            row, "checkpoint_revision", 64
                        ),
                        "checkpoint_n": int_value(
                            row, "checkpoint_n", -1, 0
                        ),
                        "public_source_revision": text_value(
                            row, "public_source_revision", 64
                        ),
                        "runtime_profile_hash": text_value(
                            row, "runtime_profile_hash", 128
                        ),
                        "environment": text_value(row, "environment", 128),
                        "registered_at": float_value(
                            row, "registered_at", 0.0, 0.0
                        ),
                        "miner_source_revision": text_value(
                            row, "miner_source_revision", 128
                        ),
                    }
                )
            except (TypeError, ValueError):
                continue
    generation_raw = ledger_raw.get("generation_outcomes")
    generation_raw = generation_raw if isinstance(generation_raw, dict) else {}
    generation_summaries: list[dict[str, Any]] = []
    raw_generation_summaries = generation_raw.get("summaries")
    if isinstance(raw_generation_summaries, list):
        for row in raw_generation_summaries[:32]:
            if not isinstance(row, dict):
                continue
            first_window_n = int_value(row, "first_window_n", -1, 0)
            last_window_n = int_value(row, "last_window_n", -1, 0)
            if first_window_n < 0 or last_window_n < first_window_n:
                continue
            natural_eos_complete = int_value(
                row, "natural_eos_complete", 0, 0
            )
            local_token_limit = int_value(row, "local_token_limit", 0, 0)
            safe_deadline = int_value(row, "safe_deadline", 0, 0)
            decisive_attempts = natural_eos_complete + local_token_limit
            generation_summaries.append(
                {
                    "model_repository": text_value(
                        row, "model_repository", 512
                    ),
                    "checkpoint_revision": text_value(
                        row, "checkpoint_revision", 64
                    ),
                    "checkpoint_n": int_value(
                        row, "checkpoint_n", -1, 0
                    ),
                    "public_source_revision": text_value(
                        row, "public_source_revision", 64
                    ),
                    "runtime_profile_hash": text_value(
                        row, "runtime_profile_hash", 128
                    ),
                    "environment": text_value(row, "environment", 128),
                    "lane": text_value(row, "lane", 128),
                    "first_window_n": first_window_n,
                    "last_window_n": last_window_n,
                    "last_observed_at": float_value(
                        row, "last_observed_at", 0.0, 0.0
                    ),
                    "attempts": (
                        natural_eos_complete
                        + local_token_limit
                        + safe_deadline
                    ),
                    "decisive_attempts": decisive_attempts,
                    "natural_eos_complete": natural_eos_complete,
                    "local_token_limit": local_token_limit,
                    "safe_deadline": safe_deadline,
                    "natural_eos_before_bound_rate": (
                        natural_eos_complete / decisive_attempts
                        if decisive_attempts > 0
                        else None
                    ),
                }
            )
    generation_outcomes = {
        "table_present": generation_raw.get("table_present") is True,
        "schema_ok": generation_raw.get("schema_ok") is True,
        "summaries": generation_summaries,
        "error": text_value(generation_raw, "error", 128),
    }
    terminal_raw = ledger_raw.get("terminal_funnel")
    terminal_raw = terminal_raw if isinstance(terminal_raw, dict) else {}
    terminal_summaries: list[dict[str, Any]] = []
    raw_terminal_summaries = terminal_raw.get("summaries")
    if isinstance(raw_terminal_summaries, list):
        for row in raw_terminal_summaries[:32]:
            if not isinstance(row, dict):
                continue
            terminal_summaries.append(
                {
                    "model_repository": text_value(
                        row, "model_repository", 512
                    ),
                    "checkpoint_revision": text_value(
                        row, "checkpoint_revision", 64
                    ),
                    "checkpoint_n": int_value(
                        row, "checkpoint_n", -1, 0
                    ),
                    "public_source_revision": text_value(
                        row, "public_source_revision", 64
                    ),
                    "runtime_profile_hash": text_value(
                        row, "runtime_profile_hash", 128
                    ),
                    "environment": text_value(row, "environment", 128),
                    "attempts": int_value(row, "attempts", 0, 0),
                    "http_provisional": int_value(
                        row, "http_provisional", 0, 0
                    ),
                    "receipt_reserved": int_value(
                        row, "receipt_reserved", 0, 0
                    ),
                    "reveal_sent": int_value(row, "reveal_sent", 0, 0),
                    "pool_accepted": int_value(
                        row, "pool_accepted", 0, 0
                    ),
                    "selected": int_value(row, "selected", 0, 0),
                    "rewarded": int_value(row, "rewarded", 0, 0),
                    "terminal_rejected": int_value(
                        row, "terminal_rejected", 0, 0
                    ),
                    "terminal_unresolved": int_value(
                        row, "terminal_unresolved", 0, 0
                    ),
                }
            )
    terminal_funnel = {
        "extension_table_present": (
            terminal_raw.get("extension_table_present") is True
        ),
        "terminal_events_table_present": (
            terminal_raw.get("terminal_events_table_present") is True
        ),
        "schema_ok": terminal_raw.get("schema_ok") is True,
        "api_version": int_value(terminal_raw, "api_version", 0, 0),
        "storage_mode": text_value(terminal_raw, "storage_mode", 64),
        "summaries": terminal_summaries,
        "error": text_value(terminal_raw, "error", 128),
    }
    ledger = {
        "configured": ledger_raw.get("configured") is True,
        "readonly_ok": ledger_raw.get("readonly_ok") is True,
        "schema_ok": ledger_raw.get("schema_ok") is True,
        "application_id": int_value(ledger_raw, "application_id", 0, 0),
        "user_version": int_value(ledger_raw, "user_version", 0, 0),
        "quick_check": text_value(ledger_raw, "quick_check", 64),
        "journal_mode": text_value(ledger_raw, "journal_mode", 32).lower(),
        "partitions": partitions,
        "generation_outcomes": generation_outcomes,
        "terminal_funnel": terminal_funnel,
        "error": text_value(ledger_raw, "error", 128),
    }
    try:
        schema_version = int_value(raw, "schema_version", 0, 0)
        process_pid = int_value(raw, "process_pid", 0, 0)
    except (TypeError, ValueError, OverflowError):
        return {}
    return {
        "schema_version": schema_version,
        "miner_unit": text_value(raw, "miner_unit", 128),
        "miner_unit_enablement": text_value(
            raw, "miner_unit_enablement", 64
        ),
        "process_pid": process_pid,
        "invocation_id": text_value(raw, "invocation_id", 64),
        "settings_process_bound": raw.get("settings_process_bound") is True,
        "environment": text_value(raw, "environment", 128),
        "engine_mode": text_value(raw, "engine_mode", 64),
        "lane": text_value(raw, "lane", 128),
        "prescreen_enabled": raw.get("prescreen_enabled") is True,
        "auction_policy": text_value(raw, "auction_policy", 128),
        "ledger_path": text_value(raw, "ledger_path"),
        "grader": grader,
        "ledger": ledger,
    }


def _parse_runtime_checkpoint_evidence(
    lines: list[str],
    *,
    repo_hint: str = "",
    expected_process_pid: int = 0,
    expected_public_source_revision: str = "",
) -> dict[str, object] | None:
    """Return the newest independently attested dynamic checkpoint.

    The probe deliberately supplies sparse checkpoint and generation lines
    from the *current* unit invocation.  The strongest legacy path requires a
    matching resolver, load-success, and structured generation event.  A
    structured ``generation_start``/``generation_started`` (or
    generation-abort) event also proves that the active engine began using
    its stated checkpoint.  Older miner
    builds omit the repository from that event, so an exact, atomically
    provisioned manifest repository may be supplied as ``repo_hint``; its
    stale checkpoint number and revision are never reused.  Validator
    telemetry is never an input here.  A structured
    ``math_auction_readiness`` line is also sufficient before the first
    generation begins, but only when its explicit PID matches the active PID
    independently read from systemd.
    """
    resolved_repos: dict[str, str] = {}
    loaded_revisions: set[str] = set()
    generation_events: list[tuple[int, str, str, str]] = []
    readiness_events: list[tuple[int, str, str, int]] = []

    repo_hint = str(repo_hint or "").strip()
    if not repo_hint or any(char.isspace() for char in repo_hint):
        repo_hint = ""
    try:
        expected_process_pid = int(expected_process_pid)
    except (TypeError, ValueError, OverflowError):
        expected_process_pid = 0
    if expected_process_pid <= 0:
        expected_process_pid = 0
    expected_public_source_revision = str(
        expected_public_source_revision or ""
    ).lower()
    if not _EXACT_CHECKPOINT_REVISION_RE.fullmatch(
        expected_public_source_revision
    ):
        expected_public_source_revision = ""

    for raw in lines:
        readiness = _MATH_AUCTION_READINESS_RE.search(raw)
        if readiness:
            try:
                payload = json.loads(readiness.group(1))
                if not isinstance(payload, dict):
                    raise ValueError("readiness payload is not an object")
                schema_version_raw = payload.get("schema_version")
                schema_version = int(schema_version_raw)
                checkpoint_n_raw = payload.get("checkpoint_n")
                checkpoint_n = int(checkpoint_n_raw)
                process_pid_raw = payload.get("process_pid")
                process_pid = int(process_pid_raw)
                repo = str(payload.get("checkpoint_repo_id") or "").strip()
                revision = str(
                    payload.get("checkpoint_revision") or ""
                ).lower()
                public_source_revision = str(
                    payload.get("public_source_revision") or ""
                ).lower()
                runtime_profile_hash = str(
                    payload.get("runtime_profile_hash") or ""
                ).lower()
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
            else:
                if (
                    schema_version == 1
                    and type(schema_version_raw) is int
                    and type(checkpoint_n_raw) is int
                    and checkpoint_n > 0
                    and type(process_pid_raw) is int
                    and expected_process_pid > 0
                    and process_pid == expected_process_pid
                    and repo
                    and not any(char.isspace() for char in repo)
                    and _EXACT_CHECKPOINT_REVISION_RE.fullmatch(revision)
                    and _EXACT_CHECKPOINT_REVISION_RE.fullmatch(
                        public_source_revision
                    )
                    and expected_public_source_revision
                    and public_source_revision
                    == expected_public_source_revision
                    and re.fullmatch(r"[0-9a-f]{64}", runtime_profile_hash)
                    and str(payload.get("environment") or "").lower()
                    == "openmathinstruct"
                    and str(payload.get("auction_policy") or "")
                    == "deadline_aware"
                ):
                    readiness_events.append(
                        (checkpoint_n, repo, revision, process_pid)
                    )

        resolution = _CHECKPOINT_RESOLUTION_RE.search(raw)
        if resolution:
            repo = resolution.group(1).strip()
            revision = resolution.group(2).lower()
            if repo and not any(char.isspace() for char in repo):
                resolved_repos[revision] = repo

        loaded = _CHECKPOINT_LOADED_RE.search(raw)
        if loaded:
            loaded_revisions.add(loaded.group(1).lower())

        generation_event = _GENERATION_CHECKPOINT_EVENT_RE.search(raw)
        if not generation_event:
            continue
        try:
            payload = json.loads(generation_event.group(2))
            event = str(payload.get("event") or "")
            expected_event = generation_event.group(1)
            checkpoint_n_raw = payload.get("checkpoint_n")
            checkpoint_n = int(checkpoint_n_raw)
            revision = str(payload.get("checkpoint_revision") or "").lower()
            event_repo = str(
                payload.get("checkpoint_repo_id")
                or payload.get("checkpoint_repo")
                or ""
            ).strip()
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if (
            event != expected_event
            or isinstance(checkpoint_n_raw, bool)
            or checkpoint_n <= 0
            or _EXACT_CHECKPOINT_REVISION_RE.fullmatch(revision) is None
            or (event_repo and any(char.isspace() for char in event_repo))
        ):
            continue
        generation_events.append((checkpoint_n, revision, event, event_repo))

    for checkpoint_n, revision, event, event_repo in reversed(generation_events):
        resolved_repo = resolved_repos.get(revision, "")
        if resolved_repo and revision in loaded_revisions:
            return {
                "checkpoint_n": checkpoint_n,
                "repo": resolved_repo,
                "revision": revision,
                "evidence": f"loaded+{event}",
            }
        # The structured event is emitted only after the active engine has
        # bound the checkpoint tuple to a real generation group.  Prefer a
        # repository carried by that live event; otherwise use only the
        # repository (not n/revision) from the trusted local manifest hint.
        runtime_repo = event_repo or repo_hint
        if runtime_repo:
            return {
                "checkpoint_n": checkpoint_n,
                "repo": runtime_repo,
                "revision": revision,
                "evidence": f"generation+{event}",
            }
    for checkpoint_n, repo, revision, process_pid in reversed(readiness_events):
        return {
            "checkpoint_n": checkpoint_n,
            "repo": repo,
            "revision": revision,
            "process_pid": process_pid,
            "evidence": "readiness+math_auction_readiness",
        }
    return None


def _canonical_systemd_unit(unit: str) -> str:
    value = str(unit or "").strip()
    if not value or not _SYSTEMD_UNIT_RE.fullmatch(value):
        raise ValueError(f"invalid systemd unit name: {value!r}")
    return value if value.endswith(".service") else f"{value}.service"


_DYNAMIC_CERTIFICATION_TOKENS = (
    "--submit-disabled",
    "certif",
    "capture",
    "benchmark",
    "-proof-",
    "-download",
    "materialize",
    "alias-verify",
    "-validation-",
    "preflight",
    "postprocess",
)
_DYNAMIC_GENERATOR_TOKENS = ("generator", " generator.server")
_DYNAMIC_CONTROLLER_UNIT_TOKENS = ("canary",)
_DYNAMIC_CONTROLLER_UNIT_SUFFIXES = ("-mine.service", "@mine.service")
_DYNAMIC_CONTROLLER_LANE_TOKENS = ("code", "math", "miner-pro")
_DYNAMIC_SUPPORT_TOKENS = (
    "grader",
    "tunnel",
    "sentinel",
    "watchdog",
    "chrony",
    "fail-closed",
    "fence",
    "dashboard",
    "r2-sync",
    "memory",
    "coordinator",
)


def _classify_dynamic_service_role(unit: str, command: str) -> str:
    """Classify one discovered service using the exact remote-probe contract."""
    lowered_unit = unit.lower()
    combined = lowered_unit + " " + command.lower()
    if any(token in combined for token in _DYNAMIC_CERTIFICATION_TOKENS):
        return "certification"
    if any(token in combined for token in _DYNAMIC_GENERATOR_TOKENS):
        return "wallet_free_generator"
    if (
        any(token in lowered_unit for token in _DYNAMIC_CONTROLLER_UNIT_TOKENS)
        or any(
            lowered_unit.endswith(suffix)
            for suffix in _DYNAMIC_CONTROLLER_UNIT_SUFFIXES
        )
    ) and any(token in lowered_unit for token in _DYNAMIC_CONTROLLER_LANE_TOKENS):
        return "mining_controller"
    if any(token in combined for token in _DYNAMIC_SUPPORT_TOKENS):
        return "support"
    return "unknown"


def _unit_lane(unit: str) -> str:
    canonical = _canonical_systemd_unit(unit)
    if "@" in canonical:
        return canonical.split("@", 1)[1].removesuffix(".service")
    if canonical == "reliquary-miner-pro.service":
        return "single"
    return canonical.removesuffix(".service")


def _configured_unit_specs(state: BoxState) -> list[tuple[str, str]]:
    raw_specs = list(state.unit_candidates)
    if not raw_specs and state.unit:
        raw_specs = [(state.unit, state.env_file)]
    specs: list[tuple[str, str]] = []
    seen: set[str] = set()
    for unit, env_file in raw_specs:
        canonical = _canonical_systemd_unit(unit)
        if canonical in seen:
            raise ValueError(f"duplicate configured systemd unit: {canonical}")
        seen.add(canonical)
        specs.append((canonical, str(env_file or "")))
    return specs


def _configured_controller_config_path(
    state: BoxState,
    unit: str,
) -> str:
    """Return the declared controller TOML for one exact allowed unit."""
    canonical = _canonical_systemd_unit(unit)
    matches = [
        str(path or "")
        for configured_unit, path in state.unit_controller_config_paths
        if _canonical_systemd_unit(configured_unit) == canonical
    ]
    if len(matches) > 1:
        raise ValueError(f"duplicate controller config path: {canonical}")
    return matches[0] if matches else ""


def _refresh_active_unit_registry(state: BoxState) -> bool:
    """Load one root-owned checkpoint-independent controller registry.

    The registry is deployment authority, not service discovery.  Its three
    content digests are checked on the miner host before any unit or telemetry
    path can replace the static rollback configuration.
    """

    path = str(state.active_unit_registry_path or "")
    if not path:
        state.active_unit_registry = {}
        return True
    command = " ".join(("sudo -n python3 -", shlex.quote(path))) + """ <<'PYREGISTRY'
import hashlib
import json
import math
import os
import re
import stat
import subprocess
import sys

path = sys.argv[1]
required_top = {'schema_version', 'generated_at', 'active', 'rollback'}
required_entry = {
    'unit', 'mode', 'controller_config_path', 'telemetry_path',
    'runtime_manifest_path', 'checkpoint_revision', 'source_revision',
    'unit_fragment_sha256', 'controller_config_sha256',
    'runtime_manifest_sha256',
}
hex64 = re.compile(r'[0-9a-f]{64}')
hex40 = re.compile(r'[0-9a-f]{40}')
unit_re = re.compile(r'[A-Za-z0-9_.@:-]+\\.service')

def digest_file(filename, *, limit):
    st = os.stat(filename, follow_symlinks=False)
    if not stat.S_ISREG(st.st_mode) or st.st_size > limit:
        raise ValueError('attested file is not a bounded regular file')
    digest = hashlib.sha256()
    with open(filename, 'rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()

def digest_systemctl_cat(unit, *, limit):
    '''Hash systemd's canonical unit projection, including source headers.

    Some deployment writers intentionally bind ``systemctl cat`` instead of
    the raw ``FragmentPath``.  For transient units those byte streams differ
    only because ``systemctl cat`` prepends the fragment path header.  Keeping
    this as an exact-byte fallback accepts that documented canonicalization
    without turning a genuine fragment mutation into a soft warning.
    '''
    rendered = subprocess.run(
        ['systemctl', 'cat', '--no-pager', unit],
        capture_output=True, timeout=4, check=False,
    )
    payload = rendered.stdout
    if rendered.returncode != 0 or not payload or len(payload) > limit:
        return ''
    return hashlib.sha256(payload).hexdigest()

def normalize_entry(raw, *, require_live_fragment):
    if not isinstance(raw, dict) or set(raw) != required_entry:
        raise ValueError('registry entry schema mismatch')
    entry = {key: str(value or '') for key, value in raw.items()}
    if not unit_re.fullmatch(entry['unit']):
        raise ValueError('registry unit is invalid')
    if entry['mode'] not in {
        'certifying', 'canary', 'mine', 'fenced', 'stalled', 'idle'
    }:
        raise ValueError('registry mode is invalid')
    for name in (
        'controller_config_path', 'telemetry_path', 'runtime_manifest_path'
    ):
        value = entry[name]
        if not os.path.isabs(value) or '\\x00' in value:
            raise ValueError(f'registry {name} is not absolute')
    for name in (
        'unit_fragment_sha256', 'controller_config_sha256',
        'runtime_manifest_sha256'
    ):
        if not hex64.fullmatch(entry[name]):
            raise ValueError(f'registry {name} is invalid')
    for name in ('checkpoint_revision', 'source_revision'):
        if not hex40.fullmatch(entry[name]):
            raise ValueError(f'registry {name} is invalid')
    controller_digest = digest_file(entry['controller_config_path'], limit=1024 * 1024)
    runtime_digest = digest_file(entry['runtime_manifest_path'], limit=4 * 1024 * 1024)
    if controller_digest != entry['controller_config_sha256']:
        raise ValueError('controller config digest mismatch')
    if runtime_digest != entry['runtime_manifest_sha256']:
        raise ValueError('runtime manifest digest mismatch')
    show = subprocess.run(
        ['systemctl', 'show', entry['unit'], '--property=FragmentPath'],
        capture_output=True, text=True, timeout=4, check=False,
    )
    fragment = ''
    for line in show.stdout.splitlines():
        if line.startswith('FragmentPath='):
            fragment = line.split('=', 1)[1]
    fragment_verification = 'unloaded'
    if fragment:
        raw_digest = digest_file(fragment, limit=1024 * 1024)
        if raw_digest == entry['unit_fragment_sha256']:
            fragment_verification = 'raw_fragment'
        elif (
            digest_systemctl_cat(entry['unit'], limit=1024 * 1024)
            == entry['unit_fragment_sha256']
        ):
            fragment_verification = 'systemctl_cat'
        else:
            raise ValueError('systemd unit fragment digest mismatch')
    elif require_live_fragment:
        raise ValueError('systemd unit fragment digest mismatch')
    return entry, fragment_verification

root_st = os.stat(path, follow_symlinks=False)
if (
    not stat.S_ISREG(root_st.st_mode)
    or root_st.st_uid != 0
    or root_st.st_mode & 0o022
    or root_st.st_size > 65536
):
    raise ValueError('registry must be root-owned, bounded and non-writable')
with open(path, encoding='utf-8') as handle:
    raw = json.load(handle)
if not isinstance(raw, dict) or set(raw) != required_top:
    raise ValueError('registry top-level schema mismatch')
if raw['schema_version'] != 1:
    raise ValueError('registry schema version is unsupported')
generated_at = float(raw['generated_at'])
if not math.isfinite(generated_at) or generated_at <= 0:
    raise ValueError('registry generated_at is invalid')
if not isinstance(raw['rollback'], list) or len(raw['rollback']) > 8:
    raise ValueError('registry rollback set is invalid')
normalized = [normalize_entry(raw['active'], require_live_fragment=True)]
normalized.extend(
    normalize_entry(item, require_live_fragment=False)
    for item in raw['rollback']
)
entries = [entry for entry, _verification in normalized]
units = [entry['unit'] for entry in entries]
if len(units) != len(set(units)):
    raise ValueError('registry units are duplicated')
print(json.dumps({
    'schema_version': 1,
    'generated_at': generated_at,
    'registry_sha256': digest_file(path, limit=65536),
    'active': entries[0],
    'rollback': entries[1:],
    'fragment_verification': {
        entry['unit']: verification
        for entry, verification in normalized
    },
}, separators=(',', ':'), sort_keys=True))
PYREGISTRY"""
    rc, out, err = ssh_run(state.alias, command, timeout_s=12)
    if rc != 0 or not out.strip():
        state.active_unit_registry = {
            "path": path,
            "error": str(err or f"rc={rc}")[:160],
        }
        state.unit_resolution_error = "active_unit_registry_invalid"
        return False
    try:
        registry = json.loads(out.strip().splitlines()[-1])
        active = registry["active"]
        rollback = registry["rollback"]
        entries = [active, *rollback]
        state.active_unit_registry = {
            **registry,
            "path": path,
            "error": "",
        }
        state.unit = str(active["unit"])
        state.unit_candidates = tuple(
            (str(entry["unit"]), "") for entry in entries
        )
        state.unit_controller_config_paths = tuple(
            (str(entry["unit"]), str(entry["controller_config_path"]))
            for entry in entries
        )
        state.coordinated_units = ()
        state.host_unit_allowlist = tuple(
            str(entry["unit"]) for entry in entries
        )
        state.standalone_telemetry_path = str(active["telemetry_path"])
        state.standalone_runtime_manifest_path = str(
            active["runtime_manifest_path"]
        )
        return True
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        state.active_unit_registry = {
            "path": path,
            "error": str(exc)[:160],
        }
        state.unit_resolution_error = "active_unit_registry_invalid"
        return False


def _select_active_unit_registry_entry(state: BoxState, unit: str) -> bool:
    """Bind dynamic telemetry paths to the exact selected registry unit."""

    if not state.active_unit_registry_path:
        return True
    registry = state.active_unit_registry
    entries = [registry.get("active"), *list(registry.get("rollback") or [])]
    matches = [
        entry
        for entry in entries
        if isinstance(entry, dict)
        and _canonical_systemd_unit(str(entry.get("unit") or ""))
        == _canonical_systemd_unit(unit)
    ]
    if len(matches) != 1:
        state.unit_resolution_error = "active_unit_registry_unit_mismatch"
        return False
    selected = matches[0]
    state.standalone_telemetry_path = str(selected["telemetry_path"])
    state.standalone_runtime_manifest_path = str(
        selected["runtime_manifest_path"]
    )
    return True


def resolve_box_env_file(state: BoxState, unit: str | None = None) -> str:
    """Return the exact service env path without guessing over an override.

    The safe reference unit is deliberately non-templated and reads
    ``miner-pro.env``.  Legacy templated units retain their historical
    ``miner-pro-<instance>.env`` convention.
    """
    selected = unit or state.active_unit or state.unit or ""
    try:
        canonical = _canonical_systemd_unit(selected) if selected else ""
        for configured_unit, configured_env in _configured_unit_specs(state):
            if configured_unit == canonical and configured_env:
                return configured_env
    except ValueError:
        canonical = ""
    if not unit and not state.active_unit and state.env_file:
        return state.env_file
    unit_name = canonical.removesuffix(".service")
    if unit_name == "reliquary-miner-pro":
        return "/srv/reliquary-miner-pro/state/miner-pro.env"
    if "@" in unit_name:
        instance = unit_name.split("@", 1)[1]
        return f"/srv/reliquary-miner-pro/state/miner-pro-{instance}.env"
    if unit_name:
        return f"/srv/reliquary-miner-pro/state/miner-pro-{unit_name}.env"
    return ""


def _clear_current_lane_telemetry(state: BoxState) -> None:
    """Clear values that are authoritative only for a selected live lane.

    A resolver or follow-up probe failure must not leave the previous Code or
    Math process painted as current. Historical validator verdicts and trend
    deques remain useful, but process, GPU, manifest, frontier, quarantine, and
    lane-local pipeline values become unknown until a complete probe succeeds.
    """
    state.active_unit = ""
    state.active_lane = ""
    state.active_units = []
    state.active_lanes = []
    state.coordinated_unit_statuses = []
    state.service_inventory = []
    state.active_environment = ""
    state.active_pid = 0
    state.env_file_ok = False
    state.env_file_error = ""
    state.gpu_mem_mb = 0
    state.gpu_total_mb = 1
    state.gpu_util = 0
    state.proc_alive = False
    state.proc_uptime_s = 0
    state.active_started_at = 0
    state.last_event_at = "—"
    state.last_oom_at = ""
    state.last_oom_age_s = -1
    state.oom_60m = 0
    state.mean_accept_t_s = 0.0
    state.pregen_30m = 0
    state.pregen_60m = 0
    state.pregen_ok_30m = 0
    state.pregen_fail_30m = 0
    state.skip_30m = 0
    state.miner_state = "?"
    state.miner_window = 0
    state.miner_valid = 0
    state.miner_inflight = 0
    state.miner_ready = 0
    state.miner_submitted_this_win = 0
    state.fresh_built_30m = 0
    state.cache_fwd_30m = 0
    state.prefinalized_30m = 0
    state.burst_30m = 0
    state.late_grace_30m = 0
    state.batch_filled_30m = 0
    state.checkpoint_restarts_60m = 0
    state.last_fresh_total_s = 0.0
    state.last_tune = ""
    state.miner_environment = ""
    state.engine_mode = ""
    state.protocol_profile = ""
    state.runtime_parity_ok = False
    state.reference_ready = False
    state.miner_unit_enablement = ""
    state.runtime_profile_hash = ""
    state.code_auction_probe = {}
    state.code_selector_crossover_probe = {}
    state.code_overlap_abba_probe = {}
    state.standalone_telemetry = {}
    state.standalone_generated_at = 0.0
    state.standalone_age_s = -1.0
    state.standalone_fresh = False
    state.standalone_error = ""
    state.standalone_controller_mode = ""
    state.standalone_controller_config_path = ""
    state.standalone_active_runtime_manifest_path = ""
    state.standalone_active_ledger_path = ""
    state.standalone_supervisor = {}
    state.standalone_progress_age_s = -1.0
    state.standalone_certification = {}
    state.runtime_components = []
    state.miner_source_revision = ""
    state.reliquary_source_revision = ""
    state.observed_validator_image_revision = ""
    state.source_manifest_provisioned_ok = False
    state.provisioned_model_kind = ""
    state.provisioned_checkpoint_n = -1
    state.provisioned_model_repo = ""
    state.provisioned_model_revision = ""
    state.base_model_repo = ""
    state.base_model_revision = ""
    state.quarantine_active = False
    state.quarantine_path = ""
    state.quarantine_reason = ""
    state.quarantine_at = 0.0
    state.drand_offset = 0
    state.picker_epsilon = 0.0
    state.picker_top_k = 0
    state.external_min_sigma = 0.0
    state.external_max_len = 0
    state.hash_max_leading_byte = -1
    state.burst_per_window = 0
    state.prompt_shard_id = -1
    state.prompt_shard_mod = 0
    state.frontier_path = ""
    state.frontier_entries = 0
    state.frontier_content_entries = 0
    state.frontier_age_s = -1.0
    state.frontier_newest_age_s = -1.0
    state.frontier_checkpoint_n = 0
    state.frontier_checkpoint_revision = ""
    state.frontier_ckpts = {}
    state.local_checkpoint_n = 0
    state.local_checkpoint_revision = ""
    state.runtime_checkpoint_loaded = False
    state.runtime_checkpoint_n = 0
    state.runtime_checkpoint_repo = ""
    state.runtime_checkpoint_revision = ""
    state.runtime_checkpoint_pid = 0
    state.runtime_checkpoint_started_at = 0
    state.runtime_checkpoint_evidence = ""
    state.state_relay_ok = False
    state.state_relay_age_s = -1.0
    state.state_relay_upstream_ms = 0.0
    state.state_relay_failures = 0
    state.state_relay_error = ""
    state.watchdog_ok = False
    state.watchdog_age_s = -1.0
    state.watchdog_stale_strikes = 0
    state.watchdog_restart_count = 0
    state.watchdog_last_action = ""
    state.watchdog_last_error = ""
    state.restart_count = 0
    state.rss_mb = 0
    state.cpu_pct = 0
    state.disk_used_pct = 0
    state.recent_lines.clear()


def _resolve_allowed_active_unit(state: BoxState) -> tuple[str, str] | None:
    """Resolve this row's unit while enforcing the host-wide allowlist.

    An exact ``coordinated_units`` set may intentionally run on one GPU. The
    row collects detailed telemetry from its first configured active service
    while retaining every coordinated lane identity for display. Any other
    concurrent, transitional, or unexpected process remains fail-closed.
    """
    state.active_unit = ""
    state.active_lane = ""
    state.active_units = []
    state.active_lanes = []
    state.coordinated_unit_statuses = []
    state.service_inventory = []
    state.active_environment = ""
    state.active_pid = 0
    state.unexpected_active_units = []
    try:
        specs = _configured_unit_specs(state)
        collectable = [unit for unit, _env in specs]
        if state.host_unit_allowlist:
            host_allowed = sorted({
                _canonical_systemd_unit(unit)
                for unit in state.host_unit_allowlist
            })
            missing = sorted(set(collectable).difference(host_allowed))
            if missing:
                raise ValueError(
                    "collection unit absent from host allowlist: "
                    + ",".join(missing)
                )
        else:
            host_allowed = list(collectable)
        coordinated = [
            _canonical_systemd_unit(unit)
            for unit in state.coordinated_units
        ]
        if coordinated and (
            len(coordinated) < 2
            or len(coordinated) != len(set(coordinated))
            or not set(coordinated).issubset(collectable)
        ):
            raise ValueError("invalid coordinated unit set")
    except ValueError as exc:
        state.unit_resolution_error = f"config:{exc}"
        return None
    payload = shlex.quote(json.dumps({
        "collectable": collectable,
        "host_allowed": host_allowed,
        "coordinated": coordinated,
        "certification_tokens": _DYNAMIC_CERTIFICATION_TOKENS,
        "generator_tokens": _DYNAMIC_GENERATOR_TOKENS,
        "controller_unit_tokens": _DYNAMIC_CONTROLLER_UNIT_TOKENS,
        "controller_unit_suffixes": _DYNAMIC_CONTROLLER_UNIT_SUFFIXES,
        "controller_lane_tokens": _DYNAMIC_CONTROLLER_LANE_TOKENS,
        "support_tokens": _DYNAMIC_SUPPORT_TOKENS,
    }))
    command = f"""python3 - {payload} <<'PYUNIT'
import hashlib
import json
import os
import subprocess
import sys

config = json.loads(sys.argv[1])
collectable = set(config['collectable'])
collectable_order = list(config['collectable'])
allowed = list(config['host_allowed'])
coordinated_order = list(config['coordinated'])
coordinated = set(coordinated_order)

def properties(unit):
    proc = subprocess.run(
        [
            'systemctl', 'show', unit,
            '--property=LoadState', '--property=ActiveState',
            '--property=SubState', '--property=MainPID',
            '--property=NRestarts', '--property=InvocationID',
            '--property=ExecStart', '--property=FragmentPath',
            '--property=User',
        ],
        capture_output=True, text=True, timeout=4, check=False,
    )
    values = {{}}
    for line in proc.stdout.splitlines():
        if '=' in line:
            key, value = line.split('=', 1)
            values[key] = value
    return {{
        'unit': unit,
        'load_state': values.get('LoadState', ''),
        'active_state': values.get('ActiveState', ''),
        'sub_state': values.get('SubState', ''),
        'pid': int(values.get('MainPID') or 0),
        'restarts': int(values.get('NRestarts') or 0),
        'invocation_id': values.get('InvocationID', ''),
        'exec_start': values.get('ExecStart', ''),
        'fragment_path': values.get('FragmentPath', ''),
        'user': values.get('User', ''),
    }}

def digest_text(value):
    return hashlib.sha256(value.encode('utf-8', 'replace')).hexdigest()

def fragment_digest(path):
    try:
        if not path or not os.path.isabs(path):
            return ''
        with open(path, 'rb') as handle:
            payload = handle.read(1048577)
        if len(payload) > 1048576:
            return ''
        return hashlib.sha256(payload).hexdigest()
    except (OSError, ValueError):
        return ''

def service_role(row):
    unit = row['unit'].lower()
    command = row['exec_start'].lower()
    combined = unit + ' ' + command
    if any(token in combined for token in config['certification_tokens']):
        return 'certification'
    if any(token in combined for token in config['generator_tokens']):
        return 'wallet_free_generator'
    if (
        (
            any(token in unit for token in config['controller_unit_tokens'])
            or any(
                unit.endswith(suffix)
                for suffix in config['controller_unit_suffixes']
            )
        )
        and any(token in unit for token in config['controller_lane_tokens'])
    ):
        return 'mining_controller'
    if any(token in combined for token in config['support_tokens']):
        return 'support'
    return 'unknown'

def inventory_row(row):
    command = row.pop('exec_start', '')
    fragment = row.pop('fragment_path', '')
    role = service_role({{**row, 'exec_start': command}})
    return {{
        'unit': row['unit'],
        'active_state': row['active_state'],
        'sub_state': row['sub_state'],
        'pid': row['pid'],
        'restarts': row['restarts'],
        'invocation_id': row.get('invocation_id', ''),
        'service_user': row.get('user', ''),
        'role': role,
        'submit_disabled': '--submit-disabled' in command,
        'exec_start_sha256': digest_text(command) if command else '',
        'fragment_sha256': fragment_digest(fragment),
    }}

def is_selectable(row):
    return (
        row['active_state'] == 'active'
        and row['sub_state'] == 'running'
        and row['pid'] > 0
    )

def is_contender(row):
    return row['pid'] > 0 or row['active_state'] in (
        'active', 'activating', 'deactivating', 'reloading', 'refreshing',
    )

rows = [properties(unit) for unit in allowed]
active = [
    row for row in rows
    if row['unit'] in collectable and is_selectable(row)
]
contenders = [
    row for row in rows
    if row['unit'] in collectable and is_contender(row)
]
unexpected = []
proc = subprocess.run(
    [
        'systemctl', 'list-units', '--type=service', '--all',
        '--no-legend', '--plain', 'reliquary*.service',
    ],
    capture_output=True, text=True, timeout=4, check=False,
)
observed_units = sorted({{
    line.split()[0] for line in proc.stdout.splitlines()
    if line.split() and line.split()[0].startswith('reliquary')
}})
observed_rows = [properties(unit) for unit in observed_units]
inventory = [inventory_row(dict(row)) for row in observed_rows if is_contender(row)]
unexpected = sorted(
    row['unit'] for row in inventory
    if row['role'] == 'mining_controller'
    and row['unit'] not in allowed
    and (row['pid'] > 0 or row['sub_state'] == 'running')
)

error = ''
if unexpected:
    error = 'unexpected_active_units'
elif len(contenders) > 1:
    contender_units = {{row['unit'] for row in contenders}}
    active_units = {{row['unit'] for row in active}}
    if not coordinated or contender_units != coordinated or active_units != coordinated:
        error = 'multiple_allowed_units_active'
elif len(active) == 0:
    error = 'no_allowed_unit_active'
active_by_unit = {{row['unit']: row for row in active}}
primary = next(
    (active_by_unit[unit] for unit in collectable_order if unit in active_by_unit),
    None,
)
rows_by_unit = {{row['unit']: row for row in rows}}
print(json.dumps({{
    'active': primary,
    'active_units': active,
    'coordinated_unit_statuses': [
        rows_by_unit[unit] for unit in coordinated_order
        if unit in rows_by_unit
    ],
    'service_inventory': inventory,
    'candidates': rows,
    'unexpected': unexpected,
    'error': error,
}}, separators=(',', ':')))
PYUNIT"""
    rc, out, err = ssh_run(state.alias, command, timeout_s=12)
    if rc != 0 or not out.strip():
        state.unit_resolution_error = f"probe:{(err or f'rc={rc}')[:120]}"
        return None
    try:
        result = json.loads(out.strip().splitlines()[-1])
        state.unexpected_active_units = [
            str(unit) for unit in result.get("unexpected", [])
        ]
        inventory_rows = result.get("service_inventory")
        if inventory_rows is None:
            inventory_rows = []
        if not isinstance(inventory_rows, list):
            raise ValueError("service inventory missing")
        parsed_inventory: list[dict[str, object]] = []
        for raw_service in inventory_rows[:128]:
            if not isinstance(raw_service, dict):
                raise ValueError("service inventory row is not an object")
            unit = _canonical_systemd_unit(str(raw_service.get("unit") or ""))
            role = str(raw_service.get("role") or "")
            if role not in {
                "mining_controller",
                "wallet_free_generator",
                "certification",
                "support",
                "unknown",
            }:
                raise ValueError("service inventory role is invalid")
            pid = int(raw_service.get("pid") or 0)
            restarts = int(raw_service.get("restarts") or 0)
            if pid < 0 or restarts < 0:
                raise ValueError("service inventory counter is negative")
            digests: dict[str, str] = {}
            for name in ("exec_start_sha256", "fragment_sha256"):
                value = str(raw_service.get(name) or "").lower()
                if value and re.fullmatch(r"[0-9a-f]{64}", value) is None:
                    raise ValueError("service inventory digest is invalid")
                digests[name] = value
            parsed_inventory.append({
                "unit": unit,
                "active_state": str(raw_service.get("active_state") or "")[:32],
                "sub_state": str(raw_service.get("sub_state") or "")[:32],
                "pid": pid,
                "restarts": restarts,
                "invocation_id": str(
                    raw_service.get("invocation_id") or ""
                )[:128],
                "service_user": str(raw_service.get("service_user") or "")[:128],
                "role": role,
                "submit_disabled": raw_service.get("submit_disabled") is True,
                **digests,
            })
        state.service_inventory = parsed_inventory
        if coordinated:
            status_rows = result.get("coordinated_unit_statuses")
            if not isinstance(status_rows, list):
                raise ValueError("coordinated unit statuses missing")
            status_by_unit: dict[str, dict[str, object]] = {}
            for raw_status in status_rows:
                if not isinstance(raw_status, dict):
                    raise ValueError("coordinated unit status is not an object")
                status_unit = _canonical_systemd_unit(
                    str(raw_status.get("unit") or "")
                )
                if status_unit in status_by_unit:
                    raise ValueError("duplicate coordinated unit status")
                pid = int(raw_status.get("pid") or 0)
                restarts = int(raw_status.get("restarts") or 0)
                if pid < 0 or restarts < 0:
                    raise ValueError("negative coordinated unit counter")
                status_by_unit[status_unit] = {
                    "unit": status_unit,
                    "active_state": str(
                        raw_status.get("active_state") or ""
                    )[:32],
                    "sub_state": str(raw_status.get("sub_state") or "")[:32],
                    "pid": pid,
                    "restarts": restarts,
                }
            if set(status_by_unit) != set(coordinated):
                raise ValueError("incomplete coordinated unit statuses")
            state.coordinated_unit_statuses = [
                status_by_unit[unit] for unit in coordinated
            ]
            state.restart_count = sum(
                int(row["restarts"])
                for row in state.coordinated_unit_statuses
            )
        error = str(result.get("error") or "")
        active = result.get("active")
        if error or not isinstance(active, dict):
            detail = ",".join(state.unexpected_active_units)
            state.unit_resolution_error = f"{error}:{detail}".rstrip(":")
            return None
        active_unit = _canonical_systemd_unit(str(active.get("unit") or ""))
        active_rows = result.get("active_units")
        if not isinstance(active_rows, list) or not active_rows:
            active_rows = [active]
        active_units = [
            _canonical_systemd_unit(str(row.get("unit") or ""))
            for row in active_rows
            if isinstance(row, dict)
        ]
        if active_unit not in active_units:
            raise ValueError("primary unit absent from active units")
        env_by_unit = dict(specs)
        state.active_unit = active_unit
        state.active_lane = _unit_lane(active_unit)
        state.active_units = active_units
        state.active_lanes = [_unit_lane(unit) for unit in active_units]
        state.active_pid = int(active.get("pid") or 0)
        if not state.coordinated_unit_statuses:
            state.restart_count = int(active.get("restarts") or 0)
        state.unit_resolution_error = ""
        return active_unit, env_by_unit.get(active_unit, "")
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        state.unit_resolution_error = f"parse:{type(exc).__name__}"
        return None


def ssh_run(alias: str, cmd: str, timeout_s: int = 8) -> tuple[int, str, str]:
    """Run a command on a box over ssh. Returns (rc, stdout, stderr)."""
    try:
        target_args = shlex.split(alias)
    except ValueError as exc:
        return 2, "", f"invalid ssh target: {exc}"
    if not target_args:
        return 2, "", "empty ssh target"
    full = [
        "ssh",
        "-o", "ConnectTimeout=4",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ControlMaster=auto",
        "-o", "ControlPersist=120",
        "-o", "ControlPath=~/.ssh/reliquary-fleet-%C",
        *target_args,
        cmd,
    ]
    try:
        r = subprocess.run(
            full, capture_output=True, text=True,
            timeout=timeout_s,
        )
        # A long-lived OpenSSH mux can occasionally reject a new channel on
        # macOS with ``sendmsg(2): Message too long``.  That is a local
        # control-socket failure, not a remote miner failure.  Retry the same
        # read-only probe once over a fresh direct connection so the dashboard
        # does not paint a healthy lane stale until the master expires.
        if r.returncode != 0 and (
            "mux_client_request_session" in r.stderr
            or "sendmsg(2): Message too long" in r.stderr
        ):
            direct = list(full)
            for old, new in (
                ("ControlMaster=auto", "ControlMaster=no"),
                ("ControlPersist=120", "ControlPersist=no"),
                ("ControlPath=~/.ssh/reliquary-fleet-%C", "ControlPath=none"),
            ):
                direct[direct.index(old)] = new
            r = subprocess.run(
                direct,
                capture_output=True,
                text=True,
                timeout=timeout_s,
            )
        return r.returncode, r.stdout, r.stderr
    except subprocess.TimeoutExpired:
        return 124, "", "timeout"


def _collect_lab_in_place(state: LabState) -> None:
    """Read one lab's isolation, accelerator and aggregate evidence state.

    The remote probe has no wallet-facing inputs and opens the evidence store
    read-only.  It emits aggregate counters plus public build identity only;
    prompt, completion and attempt identifiers never cross SSH.
    """
    command = (
        "sudo -n python3 - "
        f"{shlex.quote(state.unit)} "
        f"{shlex.quote(state.evidence_db)} "
        f"{shlex.quote(state.source_manifest)} "
        f"{shlex.quote(state.selector_artifact_manifest)} "
        "<<'PYLAB'\n"
        f"{_OFFLINE_LAB_PROBE_SOURCE}\n"
        "PYLAB"
    )
    rc, out, err = ssh_run(state.alias, command, timeout_s=20)
    state.last_poll_s = time.time()
    if rc != 0 or not out.strip():
        detail = (err or f"rc={rc}").strip().replace("\n", " ")
        state.error = f"probe:{detail[:160]}"
        return
    try:
        payload = json.loads(out.strip().splitlines()[-1])
        if not isinstance(payload, dict):
            raise ValueError("probe payload is not an object")
        for field_name in (
            "gpu_name",
            "compute_capability",
            "unit_active_state",
            "unit_sub_state",
            "unit_enablement",
            "miner_release",
            "source_revision",
            "checkpoint_repo_id",
            "checkpoint_revision",
            "environment",
            "hardware_name",
            "parity_status",
            "evidence_miner_release",
            "evidence_error",
            "artifact_status",
            "artifact_kind",
            "artifact_model_version",
            "artifact_digest",
            "artifact_file_sha256",
            "artifact_data_digest",
            "artifact_source_revision",
            "artifact_checkpoint_repo_id",
            "artifact_checkpoint_revision",
            "artifact_decision_reason",
            "artifact_payout_profile",
            "artifact_economic_target",
            "artifact_objective",
            "artifact_activation_blocker",
            "artifact_error",
            "error",
        ):
            setattr(state, field_name, str(payload.get(field_name) or ""))
        for field_name in (
            "gpu_mem_mb",
            "gpu_total_mb",
            "gpu_util",
            "active_pid",
            "restart_count",
            "checkpoint_n",
            "evidence_schema_version",
            "attempts",
            "terminal_attempts",
            "complete_attempts",
            "censored_attempts",
            "deadline_censored_attempts",
            "token_censored_attempts",
            "pending_attempts",
            "error_attempts",
            "first_window_n",
            "last_window_n",
            "artifact_checkpoint_n",
            "artifact_window_start",
            "artifact_window_end",
            "artifact_train_local_rows",
            "artifact_train_population_rows",
            "artifact_completion_terminal_rows",
            "artifact_holdout_rows",
            "artifact_holdout_window",
            "artifact_gpu_positive_value_folds",
            "artifact_gpu_rolling_folds",
            "artifact_cpu_positive_value_folds",
            "artifact_cpu_rolling_folds",
        ):
            setattr(state, field_name, int(payload.get(field_name) or 0))
        state.gpu_total_mb = max(1, state.gpu_total_mb)
        state.last_attempt_at = float(payload.get("last_attempt_at") or 0.0)
        online_activation_allowed = payload.get(
            "artifact_online_activation_allowed"
        )
        if (
            online_activation_allowed is not None
            and not isinstance(online_activation_allowed, bool)
        ):
            raise ValueError(
                "artifact online activation policy is not boolean"
            )
        state.artifact_online_activation_allowed = (
            online_activation_allowed
        )
        for field_name in (
            "artifact_expires_after_window",
            "artifact_exploration_bps",
        ):
            value = payload.get(field_name)
            if isinstance(value, bool):
                raise ValueError(f"{field_name} is not an integer")
            setattr(state, field_name, None if value is None else int(value))
        for field_name in (
            "submit_disabled_attested",
            "lane_start_disarmed",
            "manifest_submit_disabled",
            "manifest_provisioned_ok",
            "evidence_db_ok",
            "artifact_manifest_ok",
            "artifact_activation_gate_passed",
            "artifact_cpu_bitwise_reproducible",
            "artifact_gpu_bitwise_reproducible",
            "artifact_shadow_promotion_gate_passed",
        ):
            setattr(state, field_name, bool(payload.get(field_name, False)))
        for field_name in (
            "artifact_top_quintile_lift",
            "artifact_heldout_value_lift",
            "artifact_completion_brier",
            "artifact_selection_brier",
            "artifact_value_multiclass_brier",
            "artifact_emitted_slot_lift",
            "artifact_emission_mse",
            "artifact_emission_base_mse",
            "artifact_gpu_rolling_lift_mean",
            "artifact_gpu_final_lift",
            "artifact_cpu_rolling_lift_mean",
        ):
            value = payload.get(field_name)
            setattr(state, field_name, None if value is None else float(value))
        blockers = payload.get("artifact_blockers", [])
        if not isinstance(blockers, list) or not all(
            isinstance(item, str) for item in blockers
        ):
            raise ValueError("artifact blockers are not a string list")
        state.artifact_blockers = list(blockers)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        state.error = f"parse:{type(exc).__name__}"


def collect_lab_snapshot(state: LabState) -> LabState:
    """Return a fresh lab snapshot without retaining stale probe values."""
    snapshot = LabState(
        alias=state.alias,
        label=state.label,
        color=state.color,
        unit=state.unit,
        evidence_db=state.evidence_db,
        source_manifest=state.source_manifest,
        selector_artifact_manifest=state.selector_artifact_manifest,
    )
    _collect_lab_in_place(snapshot)
    return snapshot


def collect_lab(state: LabState) -> None:
    """Compatibility adapter that atomically updates caller-owned lab state."""
    snapshot = collect_lab_snapshot(state)
    state.__dict__ = snapshot.__dict__


_COMPONENT_PROBE_SOURCE = r'''import glob
import hashlib
import json
import os
import pwd
import re
import stat
import subprocess
import sys
import time

config = json.loads(sys.argv[1])
result = {"schema_version": 1, "error": "", "evidence_error": ""}


def regular_json(path, *, max_bytes=1024 * 1024):
    before = os.lstat(path)
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ValueError("not_regular:" + path)
    if before.st_size <= 0 or before.st_size > max_bytes:
        raise ValueError("size:" + path)
    if stat.S_IMODE(before.st_mode) & 0o022:
        raise ValueError("mutable:" + path)
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        opened = os.fstat(descriptor)
        raw = os.read(descriptor, max_bytes + 1)
    finally:
        os.close(descriptor)
    if (
        opened.st_dev,
        opened.st_ino,
        opened.st_size,
        opened.st_mtime_ns,
    ) != (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ):
        raise ValueError("changed:" + path)
    if not raw or len(raw) > max_bytes:
        raise ValueError("size:" + path)
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("not_object:" + path)
    return value, hashlib.sha256(raw).hexdigest(), before.st_mtime


def unit_state(unit):
    proc = subprocess.run(
        [
            "systemctl", "show", unit,
            "--property=ActiveState,SubState,UnitFileState,MainPID,NRestarts,"
            "User,ProtectHome,ExecStart,Environment",
        ],
        text=True,
        capture_output=True,
        timeout=6,
        check=False,
    )
    values = {}
    for raw in proc.stdout.splitlines():
        if "=" in raw:
            key, value = raw.split("=", 1)
            values[key] = value
    return {
        "active_state": values.get("ActiveState") or "unknown",
        "sub_state": values.get("SubState") or "unknown",
        "enablement": values.get("UnitFileState") or "unknown",
        "pid": int(values.get("MainPID") or 0),
        "restarts": int(values.get("NRestarts") or 0),
        "user": values.get("User") or "",
        "protect_home": values.get("ProtectHome") or "",
        "exec_start": values.get("ExecStart") or "",
        "environment": values.get("Environment") or "",
    }


def assignments(path):
    before = os.lstat(path)
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ValueError("config_not_regular")
    if before.st_size <= 0 or before.st_size > 1024 * 1024:
        raise ValueError("config_size")
    if stat.S_IMODE(before.st_mode) & 0o022:
        raise ValueError("config_mutable")
    values = {}
    with open(path, "r", encoding="utf-8", errors="strict") as handle:
        for raw in handle:
            line = raw.split("#", 1)[0].strip()
            if not line or line.startswith("[") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip().lower()] = value.strip().strip('"').strip("'")
    return values


def process_argv(pid):
    with open("/proc/" + str(pid) + "/cmdline", "rb") as handle:
        return [
            item.decode("utf-8")
            for item in handle.read(64 * 1024).split(b"\0")
            if item
        ]


def active_service_units():
    listed = subprocess.run(
        [
            "systemctl", "list-units", "--type=service", "--state=running",
            "--no-legend", "--no-pager",
        ],
        text=True,
        capture_output=True,
        timeout=6,
        check=False,
    )
    units = []
    for raw in listed.stdout.splitlines():
        fields = raw.split()
        if fields and re.fullmatch(r"[A-Za-z0-9_.@:-]+\.service", fields[0]):
            units.append(fields[0])
    return sorted(set(units))


def dynamic_component(expected, *, gpu_uuid):
    required = {
        "role", "component_id", "manifest_path", "manifest_file_sha256",
        "runtime_payload_sha256", "runtime_profile_sha256",
        "miner_source_revision", "validator_source_revision",
        "checkpoint_revision", "controller_manifest_path",
    }
    if not isinstance(expected, dict) or set(expected) != required:
        raise ValueError("controller_component_schema")
    for name in (
        "manifest_file_sha256", "runtime_payload_sha256",
        "runtime_profile_sha256",
    ):
        if not re.fullmatch(r"[0-9a-f]{64}", str(expected[name])):
            raise ValueError("controller_component_" + name)
    for name in (
        "miner_source_revision", "validator_source_revision",
        "checkpoint_revision",
    ):
        if not re.fullmatch(r"[0-9a-f]{40}", str(expected[name])):
            raise ValueError("controller_component_" + name)
    if expected["role"] != config["role"] or not str(
        expected["manifest_path"]
    ).startswith("/"):
        raise ValueError("controller_component_identity")
    if not str(expected["controller_manifest_path"]).startswith("/"):
        raise ValueError("controller_component_controller_manifest")
    normalize_uuid = lambda value: str(value).removeprefix("GPU-").lower()
    if normalize_uuid(expected["component_id"]) != normalize_uuid(gpu_uuid):
        raise ValueError("controller_component_gpu")

    matches = []
    running_units = active_service_units()
    for unit in running_units:
        row = unit_state(unit)
        if row["active_state"] != "active" or row["sub_state"] != "running":
            continue
        try:
            argv = process_argv(row["pid"])
        except (OSError, UnicodeDecodeError, ValueError):
            continue
        if "reliquary_miner_pro_code.generator.server" not in argv:
            continue
        positions = [index for index, value in enumerate(argv) if value == "--config"]
        if len(positions) != 1 or positions[0] + 1 >= len(argv):
            continue
        config_path = argv[positions[0] + 1]
        if not config_path.startswith("/"):
            continue
        try:
            configured = assignments(config_path)
            manifest_path = configured.get("runtime_manifest", "")
            manifest, manifest_sha, _manifest_mtime = regular_json(manifest_path)
            identity = manifest.get("identity")
            if not isinstance(identity, dict):
                continue
            profile_matches = []
            for profile_path in glob.glob(
                os.path.join(os.path.dirname(manifest_path), "*.json")
            ):
                try:
                    profile, profile_sha, _profile_mtime = regular_json(profile_path)
                except (OSError, ValueError, TypeError, json.JSONDecodeError):
                    continue
                if (
                    profile.get("schema_version") == 1
                    and profile.get("role") == expected["role"]
                    and profile.get("manifest_sha256")
                    == expected["runtime_payload_sha256"]
                    and profile.get("source_revision")
                    == expected["validator_source_revision"]
                    and profile.get("checkpoint_revision")
                    == expected["checkpoint_revision"]
                    and normalize_uuid(profile.get("gpu_uuid"))
                    == normalize_uuid(expected["component_id"])
                ):
                    profile_matches.append((profile_path, profile_sha))
            environment = row.get("environment", "")
            if (
                manifest_sha != expected["manifest_file_sha256"]
                or manifest.get("schema_version") != 5
                or manifest.get("component_role") != expected["role"]
                or manifest.get("miner_source_revision")
                != expected["miner_source_revision"]
                or identity.get("validator_source_revision")
                != expected["validator_source_revision"]
                or identity.get("checkpoint_revision")
                != expected["checkpoint_revision"]
                or configured.get("runtime_profile_sha256")
                != expected["runtime_profile_sha256"]
                or configured.get("gpu_uuid")
                != expected["component_id"]
                or len(profile_matches) != 1
                or (
                    "RELIQUARY_CODE_RUNTIME_MANIFEST=" + manifest_path
                    not in environment
                )
            ):
                continue
            matches.append({
                "unit": unit,
                "row": row,
                "config_path": config_path,
                "configured": configured,
                "manifest_path": manifest_path,
                "profile_path": profile_matches[0][0],
            })
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue
    if len(matches) != 1:
        raise ValueError("controller_component_generator_" + str(len(matches)))
    selected = matches[0]
    configured = selected["configured"]
    if configured.get("listen_host") != "127.0.0.1":
        raise ValueError("controller_component_listen_host")
    try:
        listen_port = int(configured.get("listen_port") or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("controller_component_listen_port") from exc
    if not 1 <= listen_port <= 65535:
        raise ValueError("controller_component_listen_port")

    tunnels = []
    local_suffix = ":127.0.0.1:" + str(listen_port)
    for unit in running_units:
        row = unit_state(unit)
        surface = row.get("exec_start", "") + " " + row.get("environment", "")
        if (
            row["active_state"] == "active"
            and row["sub_state"] == "running"
            and "autossh" in surface
            and local_suffix in surface
        ):
            tunnels.append((unit, row))
    if len(tunnels) != 1:
        raise ValueError("controller_component_tunnel_" + str(len(tunnels)))

    progress_path = configured.get("progress_path", "")
    progress = {}
    progress_mtime = 0.0
    for _attempt in range(2):
        try:
            progress, _progress_sha, progress_mtime = regular_json(progress_path)
            break
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            progress = {}
    if progress:
        if progress.get("schema_version") != 1:
            raise ValueError("controller_component_progress_schema")
        if int(progress.get("worker_pid") or 0) != selected["row"]["pid"]:
            raise ValueError("controller_component_progress_pid")
        worker_state = str(progress.get("state") or "")
        if worker_state not in {"loading", "ready", "active", "recovering", "failed"}:
            raise ValueError("controller_component_progress_state")
        updated_at = float(progress.get("updated_at_unix") or progress_mtime)
        heartbeat = max(1.0, float(configured.get("job_heartbeat_interval_seconds") or 5.0))
        progress_age = max(0.0, time.time() - updated_at)
        timestamp_sane = updated_at <= time.time() + 30.0
        # A READY worker is an idle, process-bound state rather than a
        # heartbeat promise.  Its PID, service, config and exact runtime
        # identity are independently re-attested above on every poll.  Active,
        # loading and recovery states still require a fresh progress record.
        progress_fresh = bool(
            timestamp_sane
            and (
                worker_state == "ready"
                or progress_age <= max(15.0, heartbeat * 3.0)
            )
        )
    else:
        worker_state = ""
        progress_age = -1.0
        progress_fresh = False

    active_job_id = str(progress.get("active_job_id") or "")
    active_window_n = int(progress.get("active_window_n") or 0)
    active_started_at = float(progress.get("active_started_at_unix") or 0.0)
    active_deadline_at = float(progress.get("active_deadline_at_unix") or 0.0)
    if worker_state == "active" and not (
        progress_fresh
        and re.fullmatch(r"[0-9a-f]{64}", active_job_id)
        and active_window_n > 0
        and 0 < active_started_at <= time.time() + 30.0
        and active_deadline_at > active_started_at
        and time.time() <= active_deadline_at + 5.0
    ):
        progress_fresh = False

    return {
        "generator_unit": selected["unit"],
        "generator": selected["row"],
        "tunnel_unit": tunnels[0][0],
        "tunnel": tunnels[0][1],
        "runtime_manifest_path": selected["manifest_path"],
        "runtime_profile_path": selected["profile_path"],
        "component_config_path": selected["config_path"],
        "evidence_dir": os.path.dirname(selected["manifest_path"]),
        "worker_state": worker_state,
        "worker_progress_fresh": progress_fresh,
        "worker_progress_age_s": progress_age,
        "worker_boot_id": str(progress.get("boot_id") or ""),
        "worker_engine_epoch": str(progress.get("engine_epoch") or ""),
        "worker_recovery_count": int(progress.get("recovery_count") or 0),
        "active_job_id": active_job_id,
        "active_window_n": active_window_n,
        "active_started_at": active_started_at,
        "active_deadline_at": active_deadline_at,
        "last_completed_job_id": str(progress.get("last_completed_job_id") or ""),
        "last_completed_window_n": int(progress.get("last_completed_window_n") or 0),
        "last_completed_at": float(progress.get("last_completed_at_unix") or 0.0),
    }


generator = unit_state(config["generator_unit"])
tunnel = unit_state(config["tunnel_unit"])
for prefix, row in (("generator", generator), ("tunnel", tunnel)):
    for key in ("active_state", "sub_state", "enablement", "pid", "restarts"):
        result[prefix + "_" + key] = row[key]

gpu_uuid = ""
try:
    gpu = subprocess.run(
        [
            "nvidia-smi",
            "--query-gpu=uuid,name,compute_cap,memory.used,memory.total,"
            "utilization.gpu,power.draw,power.limit",
            "--format=csv,noheader,nounits",
        ],
        text=True,
        capture_output=True,
        timeout=6,
        check=False,
    )
    parts = [
        value.strip()
        for value in (gpu.stdout.splitlines()[0] if gpu.stdout else "").split(",")
    ]
    if len(parts) == 8:
        gpu_uuid = parts[0]
        result.update({
            "gpu_uuid": gpu_uuid,
            "gpu_name": parts[1],
            "compute_capability": parts[2],
            "gpu_mem_mb": int(float(parts[3])),
            "gpu_total_mb": max(1, int(float(parts[4]))),
            "gpu_util": int(float(parts[5])),
            "gpu_power_w": float(parts[6]),
            "gpu_power_limit_w": float(parts[7]),
        })
except (OSError, subprocess.SubprocessError, ValueError, IndexError):
    pass

try:
    expected = config.get("controller_component")
    if expected:
        resolved = dynamic_component(expected, gpu_uuid=gpu_uuid)
        config.update({
            "generator_unit": resolved["generator_unit"],
            "tunnel_unit": resolved["tunnel_unit"],
            "runtime_manifest_path": resolved["runtime_manifest_path"],
            "runtime_profile_path": resolved["runtime_profile_path"],
            "component_config_path": resolved["component_config_path"],
            "evidence_dir": resolved["evidence_dir"],
        })
        generator = resolved["generator"]
        tunnel = resolved["tunnel"]
        for prefix, row in (("generator", generator), ("tunnel", tunnel)):
            for key in ("active_state", "sub_state", "enablement", "pid", "restarts"):
                result[prefix + "_" + key] = row[key]
        result.update({
            "resolution_source": "controller_manifest",
            "resolved_generator_unit": resolved["generator_unit"],
            "resolved_tunnel_unit": resolved["tunnel_unit"],
            "resolved_runtime_manifest_path": resolved["runtime_manifest_path"],
            "resolved_runtime_profile_path": resolved["runtime_profile_path"],
            "resolved_component_config_path": resolved["component_config_path"],
            "worker_state": resolved["worker_state"],
            "worker_progress_fresh": resolved["worker_progress_fresh"],
            "worker_progress_age_s": resolved["worker_progress_age_s"],
            "worker_boot_id": resolved["worker_boot_id"],
            "worker_engine_epoch": resolved["worker_engine_epoch"],
            "worker_recovery_count": resolved["worker_recovery_count"],
            "active_job_id": resolved["active_job_id"],
            "active_window_n": resolved["active_window_n"],
            "active_started_at": resolved["active_started_at"],
            "active_deadline_at": resolved["active_deadline_at"],
            "last_completed_job_id": resolved["last_completed_job_id"],
            "last_completed_window_n": resolved["last_completed_window_n"],
            "last_completed_at": resolved["last_completed_at"],
        })
    else:
        result["resolution_source"] = "static_config"
except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
    result["error"] = type(exc).__name__ + ":" + str(exc)[:160]

try:
    if result.get("error"):
        raise ValueError(result["error"])
    manifest, manifest_sha, _manifest_mtime = regular_json(
        config["runtime_manifest_path"]
    )
    profile, profile_sha, _profile_mtime = regular_json(
        config["runtime_profile_path"]
    )
    configured = assignments(config["component_config_path"])
    identity = manifest.get("identity")
    if not isinstance(identity, dict):
        raise ValueError("manifest_identity")
    result.update({
        "component_role": str(manifest.get("component_role") or ""),
        "manifest_schema_version": int(manifest.get("schema_version") or 0),
        "manifest_sha256": manifest_sha,
        "manifest_profile_sha256": str(profile.get("manifest_sha256") or ""),
        "miner_source_revision": str(manifest.get("miner_source_revision") or ""),
        "validator_source_revision": str(
            identity.get("validator_source_revision") or ""
        ),
        "checkpoint_n": int(identity.get("checkpoint_n") or 0),
        "checkpoint_revision": str(identity.get("checkpoint_revision") or ""),
        "model_repo": str(identity.get("model_repo") or ""),
        "environment": str(identity.get("environment") or ""),
        "runtime_profile_sha256": str(
            configured.get("runtime_profile_sha256") or ""
        ),
        "runtime_regime": str(configured.get("runtime_regime") or ""),
        "profile_file_sha256": profile_sha,
        "profile_source_revision": str(profile.get("source_revision") or ""),
        "profile_checkpoint_revision": str(
            profile.get("checkpoint_revision") or ""
        ),
        "profile_gpu_uuid": str(profile.get("gpu_uuid") or ""),
    })
    normalize_uuid = lambda value: str(value).removeprefix("GPU-").lower()
    result["identity_ok"] = bool(
        manifest.get("schema_version") == 5
        and manifest.get("component_role") == config["role"]
        and re.fullmatch(
            r"[0-9a-f]{40}", str(manifest.get("miner_source_revision") or "")
        )
        and re.fullmatch(
            r"[0-9a-f]{40}",
            str(identity.get("validator_source_revision") or ""),
        )
        and re.fullmatch(
            r"[0-9a-f]{40}", str(identity.get("checkpoint_revision") or "")
        )
        and profile.get("schema_version") == 1
        and profile.get("role") == config["role"]
        and profile.get("source_revision")
        == identity.get("validator_source_revision")
        and profile.get("checkpoint_revision")
        == identity.get("checkpoint_revision")
        and normalize_uuid(profile.get("gpu_uuid"))
        == normalize_uuid(gpu_uuid)
        and configured.get("runtime_manifest")
        == config["runtime_manifest_path"]
        and re.fullmatch(
            r"[0-9a-f]{64}",
            str(configured.get("runtime_profile_sha256") or ""),
        )
        and re.fullmatch(
            r"[0-9a-f]{64}",
            str(configured.get("runtime_regime") or ""),
        )
    )

    forbidden = re.compile(
        r"(?:^|_)(?:hotkey|coldkey|wallet|seed|validator_url|"
        r"submit_url|subtensor|chain_endpoint)(?:_|$)",
        re.IGNORECASE,
    )
    forbidden_config = any(forbidden.search(key) for key in configured)
    process_surface = " ".join(
        [
            generator.get("exec_start", ""),
            generator.get("environment", ""),
        ]
    )
    forbidden_process = bool(forbidden.search(process_surface))
    service_user = generator.get("user", "")
    wallet_home_exists = True
    try:
        home = pwd.getpwnam(service_user).pw_dir
        wallet_home_exists = any(
            os.path.exists(path)
            for path in (
                os.path.join(home, ".bittensor", "wallets"),
                os.path.join(home, ".bittensor", "wallet"),
            )
        )
    except (KeyError, OSError):
        wallet_home_exists = True
    result["wallet_absent_attested"] = bool(
        service_user
        and service_user != "root"
        and str(generator.get("protect_home", "")).lower() in {"yes", "true"}
        and not forbidden_config
        and not forbidden_process
        and not wallet_home_exists
    )
    result["submit_authority"] = False

    valid_windows = 0
    complete_groups = 0
    evidence_windows = []
    latest_at = 0.0
    for path in sorted(
        glob.glob(os.path.join(config["evidence_dir"], "generation-*.audit.json"))
    ):
        try:
            audit, _digest, mtime = regular_json(path)
            inputs = audit.get("inputs")
            details = audit.get("details")
            if (
                audit.get("schema_version") != 1
                or audit.get("submit_disabled") is not True
                or audit.get("wallet_free") is not True
                or not isinstance(inputs, dict)
                or not isinstance(details, dict)
                or inputs.get(config["runtime_profile_path"]) != profile_sha
            ):
                continue
            window_n = int(details.get("window_n") or 0)
            groups = int(details.get("completed_groups") or 0)
            if window_n <= 0 or groups < 0:
                continue
            valid_windows += 1
            complete_groups += groups
            evidence_windows.append(window_n)
            latest_at = max(latest_at, float(mtime))
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue
    result.update({
        "evidence_valid_windows": valid_windows,
        "evidence_complete_groups": complete_groups,
        "evidence_first_window": min(evidence_windows) if evidence_windows else 0,
        "evidence_last_window": max(evidence_windows) if evidence_windows else 0,
        "evidence_latest_at": latest_at,
    })
except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
    result["error"] = type(exc).__name__ + ":" + str(exc)[:160]

print(json.dumps(result, sort_keys=True, separators=(",", ":")))
'''


_COMPONENT_EVIDENCE_PROBE_SOURCE = r'''import glob
import hashlib
import json
import os
import re
import stat
import sys

config = json.loads(sys.argv[1])
result = {"schema_version": 1, "source_ok": False, "error": ""}


def regular_bytes(path, *, max_bytes):
    before = os.lstat(path)
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ValueError("not_regular")
    if before.st_size <= 0 or before.st_size > max_bytes:
        raise ValueError("size")
    if stat.S_IMODE(before.st_mode) & 0o022:
        raise ValueError("mutable")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        opened = os.fstat(descriptor)
        chunks = []
        remaining = max_bytes + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
    finally:
        os.close(descriptor)
    raw = b"".join(chunks)
    after = os.lstat(path)
    if (
        opened.st_dev,
        opened.st_ino,
        opened.st_size,
        opened.st_mtime_ns,
    ) != (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) or (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ) != (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ):
        raise ValueError("changed")
    if not raw or len(raw) > max_bytes:
        raise ValueError("size")
    return raw, before.st_mtime


def regular_json(path):
    raw, mtime = regular_bytes(path, max_bytes=1024 * 1024)
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("not_object")
    return value, hashlib.sha256(raw).hexdigest(), mtime


def contained(path, root):
    return os.path.commonpath(
        [os.path.realpath(path), os.path.realpath(root)]
    ) == os.path.realpath(root)


def certificate_bound_evidence_dir(config, evidence_root):
    """Resolve the one capture bound by the active controller certificate.

    Recovery retries may leave several ``live-capture*`` directories beside a
    runtime manifest.  Directory names and mtimes are not authority.  The
    process-bound controller manifest names an immutable certificate; that
    certificate names the exact parity artifact digest, and the certification
    audit binds that digest to one capture directory.
    """
    controller_path = str(config.get("controller_manifest_path") or "")
    if not controller_path:
        return config["evidence_dir"], False
    if not contained(controller_path, evidence_root):
        raise ValueError("controller_manifest_outside_evidence_root")
    controller, _controller_sha, _controller_mtime = regular_json(
        controller_path
    )
    controller_identity = controller.get("identity")
    components = controller.get("components")
    if not isinstance(controller_identity, dict) or not isinstance(
        components, list
    ):
        raise ValueError("controller_manifest_contract")
    normalize_uuid = lambda value: str(value).removeprefix("GPU-").lower()
    component_matches = [
        item
        for item in components
        if isinstance(item, dict)
        and item.get("role") == config["role"]
        and normalize_uuid(item.get("component_id"))
        == normalize_uuid(config["expected_gpu_uuid"])
        and item.get("manifest_path") == config["runtime_manifest_path"]
        and item.get("manifest_file_sha256")
        == config["expected_manifest_sha256"]
        and item.get("runtime_profile_sha256")
        == config["expected_runtime_profile_sha256"]
    ]
    if (
        len(component_matches) != 1
        or controller.get("miner_source_revision")
        != config["expected_miner_source_revision"]
        or controller_identity.get("validator_source_revision")
        != config["expected_validator_source_revision"]
        or controller_identity.get("checkpoint_revision")
        != config["expected_checkpoint_revision"]
    ):
        raise ValueError("controller_manifest_identity")

    certificate_path = str(controller.get("certificate_path") or "")
    certificate_sha = str(controller.get("certificate_sha256") or "")
    if (
        not certificate_path
        or not contained(certificate_path, evidence_root)
        or not re.fullmatch(r"[0-9a-f]{64}", certificate_sha)
    ):
        raise ValueError("controller_certificate_binding")
    certificate, observed_certificate_sha, _certificate_mtime = regular_json(
        certificate_path
    )
    if observed_certificate_sha != certificate_sha:
        raise ValueError("controller_certificate_digest")
    certificate_binding = certificate.get("runtime_binding")
    refresh = certificate.get("checkpoint_refresh")
    if not isinstance(certificate_binding, dict) or not isinstance(refresh, dict):
        raise ValueError("controller_certificate_contract")
    artifact_sha = str(refresh.get("evidence_sha256") or "")
    if (
        certificate_binding.get("validator_source_revision")
        != config["expected_validator_source_revision"]
        or certificate_binding.get("checkpoint_revision")
        != config["expected_checkpoint_revision"]
        or not re.fullmatch(r"[0-9a-f]{64}", artifact_sha)
    ):
        raise ValueError("controller_certificate_identity")

    matches = []
    for candidate in sorted(glob.glob(os.path.join(evidence_root, "live-capture*"))):
        try:
            candidate_info = os.lstat(candidate)
            if stat.S_ISLNK(candidate_info.st_mode) or not stat.S_ISDIR(
                candidate_info.st_mode
            ):
                continue
            audit_path = os.path.join(candidate, "certification-evidence.audit.json")
            audit, _audit_sha, _audit_mtime = regular_json(audit_path)
            details = audit.get("details")
            output_path = str(audit.get("output") or "")
            output_sha = str(audit.get("output_sha256") or "")
            if (
                audit.get("schema_version") != 1
                or audit.get("submit_disabled") is not True
                or audit.get("wallet_free") is not True
                or not isinstance(details, dict)
                or details.get("artifact_sha256") != artifact_sha
                or not output_path
                or not contained(output_path, candidate)
                or not re.fullmatch(r"[0-9a-f]{64}", output_sha)
            ):
                continue
            output, observed_output_sha, _output_mtime = regular_json(output_path)
            output_binding = output.get("runtime_binding")
            output_refresh = output.get("checkpoint_refresh")
            if (
                observed_output_sha != output_sha
                or not isinstance(output_binding, dict)
                or not isinstance(output_refresh, dict)
                or output_binding.get("validator_source_revision")
                != config["expected_validator_source_revision"]
                or output_binding.get("checkpoint_revision")
                != config["expected_checkpoint_revision"]
                or output_refresh.get("evidence_sha256") != artifact_sha
                or int(output_refresh.get("groups") or 0) < 1
            ):
                continue
            matches.append(candidate)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue
    if len(matches) != 1:
        raise ValueError("certificate_evidence_directory_" + str(len(matches)))
    return matches[0], True


try:
    configured_evidence = config["evidence_dir"]
    evidence_root = (
        configured_evidence
        if os.path.basename(configured_evidence.rstrip("/"))
        != "live-capture"
        else os.path.dirname(configured_evidence.rstrip("/"))
    )
    evidence_dir, certificate_bound = certificate_bound_evidence_dir(
        config, evidence_root
    )
    root_info = os.lstat(evidence_dir)
    if (
        stat.S_ISLNK(root_info.st_mode)
        or not stat.S_ISDIR(root_info.st_mode)
        # The controller's checkpoint-scoped live-capture directory is
        # intentionally group-writable while its one canary is assembled.
        # Every consumed file below is independently O_NOFOLLOW/read-stable,
        # digest-bound, and non-writable; only world-writable roots are unsafe.
        or stat.S_IMODE(root_info.st_mode) & 0o002
    ):
        raise ValueError("evidence_dir")
    evidence_root = os.path.dirname(evidence_dir.rstrip("/"))
    manifest_path = config["runtime_manifest_path"]
    profile_path = config["runtime_profile_path"]
    if not contained(manifest_path, evidence_root) or not contained(
        profile_path, evidence_root
    ):
        raise ValueError("identity_outside_evidence_root")
    manifest, manifest_sha, _manifest_mtime = regular_json(manifest_path)
    profile, profile_sha, _profile_mtime = regular_json(profile_path)
    identity = manifest.get("identity")
    if not isinstance(identity, dict):
        raise ValueError("manifest_identity")

    normalize_uuid = lambda value: str(value).removeprefix("GPU-").lower()
    if (
        manifest_sha != config["expected_manifest_sha256"]
        or manifest.get("schema_version") != 5
        or manifest.get("component_role") != config["role"]
        or manifest.get("miner_source_revision")
        != config["expected_miner_source_revision"]
        or identity.get("validator_source_revision")
        != config["expected_validator_source_revision"]
        or identity.get("checkpoint_revision")
        != config["expected_checkpoint_revision"]
        or profile.get("schema_version") != 1
        or profile.get("role") != config["role"]
        or profile.get("source_revision")
        != config["expected_validator_source_revision"]
        or profile.get("checkpoint_revision")
        != config["expected_checkpoint_revision"]
        or normalize_uuid(profile.get("gpu_uuid"))
        != normalize_uuid(config["expected_gpu_uuid"])
    ):
        raise ValueError("evidence_identity")

    windows = {}
    complete_groups = 0
    latest_completion_at = 0.0
    audit_paths = sorted(
        glob.glob(os.path.join(evidence_dir, "generation-*.audit.json"))
    )
    for audit_path in audit_paths:
        audit, _audit_sha, audit_mtime = regular_json(audit_path)
        inputs = audit.get("inputs")
        details = audit.get("details")
        output_path = str(audit.get("output") or "")
        if (
            audit.get("schema_version") != 1
            or audit.get("submit_disabled") is not True
            or audit.get("wallet_free") is not True
            or not isinstance(inputs, dict)
            or not isinstance(details, dict)
            or inputs.get(profile_path) != profile_sha
            or not output_path
            or not contained(output_path, evidence_dir)
        ):
            raise ValueError("audit_contract")
        # Historical component campaigns directly named the component
        # manifest as an input.  Current split-GPU canary captures instead
        # name the controller manifest; that controller manifest binds the
        # exact RTX component manifest, GPU and runtime profile.  Accept both
        # shapes, but never infer the binding from filenames alone.
        direct_manifest_binding = inputs.get(manifest_path) == manifest_sha
        controller_manifest_binding = False
        if not direct_manifest_binding:
            controller_matches = 0
            for input_path, expected_sha in inputs.items():
                if (
                    not isinstance(input_path, str)
                    or not isinstance(expected_sha, str)
                    or not contained(input_path, evidence_root)
                ):
                    raise ValueError("audit_input")
                try:
                    candidate, candidate_sha, _candidate_mtime = regular_json(
                        input_path
                    )
                except (OSError, ValueError, TypeError, json.JSONDecodeError):
                    continue
                if candidate_sha != expected_sha:
                    raise ValueError("audit_input_digest")
                candidate_identity = candidate.get("identity")
                components = candidate.get("components")
                if not isinstance(candidate_identity, dict) or not isinstance(
                    components, list
                ):
                    continue
                matches = [
                    item
                    for item in components
                    if isinstance(item, dict)
                    and item.get("role") == config["role"]
                    and normalize_uuid(item.get("component_id"))
                    == normalize_uuid(config["expected_gpu_uuid"])
                    and item.get("manifest_path") == manifest_path
                    and item.get("manifest_file_sha256") == manifest_sha
                    and item.get("runtime_profile_sha256")
                    == config["expected_runtime_profile_sha256"]
                ]
                if (
                    len(matches) == 1
                    and candidate.get("miner_source_revision")
                    == config["expected_miner_source_revision"]
                    and candidate_identity.get("validator_source_revision")
                    == config["expected_validator_source_revision"]
                    and candidate_identity.get("checkpoint_revision")
                    == config["expected_checkpoint_revision"]
                ):
                    controller_matches += 1
            controller_manifest_binding = controller_matches == 1
        if not direct_manifest_binding and not controller_manifest_binding:
            raise ValueError("audit_component_binding")
        for input_path, expected_sha in inputs.items():
            if (
                not isinstance(input_path, str)
                or not isinstance(expected_sha, str)
                or not contained(input_path, evidence_root)
            ):
                raise ValueError("audit_input")
            raw, _mtime = regular_bytes(input_path, max_bytes=32 * 1024 * 1024)
            if hashlib.sha256(raw).hexdigest() != expected_sha:
                raise ValueError("audit_input_digest")
        output, output_mtime = regular_bytes(
            output_path, max_bytes=32 * 1024 * 1024
        )
        if (
            hashlib.sha256(output).hexdigest()
            != str(audit.get("output_sha256") or "")
        ):
            raise ValueError("audit_output_digest")
        if direct_manifest_binding:
            window_n = int(details.get("window_n") or 0)
            groups = int(details.get("completed_groups") or 0)
        else:
            capture = json.loads(output)
            if not isinstance(capture, dict):
                raise ValueError("capture_output")
            trials = capture.get("generation_trials")
            if not isinstance(trials, list):
                raise ValueError("capture_output")
            complete_trials = [
                trial
                for trial in trials
                if isinstance(trial, dict)
                and isinstance(trial.get("group"), dict)
                and isinstance(trial["group"].get("rollouts"), list)
                and len(trial["group"]["rollouts"]) == 8
                and int(trial["group"].get("prompt_idx") or 0) > 0
            ]
            if len(complete_trials) != len(trials):
                raise ValueError("capture_incomplete_group")
            window_n = int(capture.get("observed_window_n") or 0)
            groups = len(complete_trials)
        if window_n <= 0 or groups < 0 or window_n in windows:
            raise ValueError("audit_window")
        windows[window_n] = True
        complete_groups += groups
        # Completion truth comes from the immutable output and its audit,
        # never from pre-created plan files.
        latest_completion_at = max(
            latest_completion_at,
            float(output_mtime),
            float(audit_mtime),
        )
    ordered = sorted(windows)
    result.update({
        "source_ok": True,
        "certificate_bound": certificate_bound,
        "evidence_dir": evidence_dir,
        "manifest_sha256": manifest_sha,
        "profile_sha256": profile_sha,
        "valid_windows": len(ordered),
        "complete_groups": complete_groups,
        "first_window": ordered[0] if ordered else 0,
        "last_window": ordered[-1] if ordered else 0,
        "latest_completion_at": latest_completion_at,
    })
except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
    result["error"] = type(exc).__name__ + ":" + str(exc)[:120]

print(json.dumps(result, sort_keys=True, separators=(",", ":")))
'''


def _collect_component_in_place(
    state: ComponentState,
    controller_component: dict[str, Any] | None = None,
) -> None:
    """Collect one component without reading wallet or submission material."""
    payload = {
        "role": state.role,
        "generator_unit": state.generator_unit,
        "tunnel_unit": state.tunnel_unit,
        "runtime_manifest_path": state.runtime_manifest_path,
        "runtime_profile_path": state.runtime_profile_path,
        "component_config_path": state.component_config_path,
        "evidence_dir": state.evidence_dir,
    }
    if controller_component is not None:
        payload["controller_component"] = controller_component
    command = (
        "sudo -n python3 - "
        + shlex.quote(json.dumps(payload, separators=(",", ":")))
        + " <<'PYCOMPONENT'\n"
        + _COMPONENT_PROBE_SOURCE
        + "\nPYCOMPONENT\n"
    )
    rc, out, err = ssh_run(state.alias, command, timeout_s=20)
    state.last_poll_s = time.time()
    if rc != 0 or not out.strip():
        detail = (err or f"rc={rc}").strip().replace("\n", " ")
        state.error = f"probe:{detail[:160]}"
        return
    try:
        payload = json.loads(out.strip().splitlines()[-1])
        if not isinstance(payload, dict) or payload.get("schema_version") != 1:
            raise ValueError("probe_schema")
        string_fields = (
            "generator_active_state",
            "generator_sub_state",
            "generator_enablement",
            "tunnel_active_state",
            "tunnel_sub_state",
            "tunnel_enablement",
            "gpu_uuid",
            "gpu_name",
            "compute_capability",
            "component_role",
            "manifest_sha256",
            "manifest_profile_sha256",
            "miner_source_revision",
            "validator_source_revision",
            "checkpoint_revision",
            "model_repo",
            "environment",
            "runtime_profile_sha256",
            "runtime_regime",
            "profile_file_sha256",
            "profile_source_revision",
            "profile_checkpoint_revision",
            "profile_gpu_uuid",
            "resolution_source",
            "worker_state",
            "worker_boot_id",
            "worker_engine_epoch",
            "active_job_id",
            "last_completed_job_id",
            "evidence_error",
            "error",
        )
        for name in string_fields:
            setattr(state, name, str(payload.get(name) or ""))
        integer_fields = (
            "generator_pid",
            "generator_restarts",
            "tunnel_pid",
            "tunnel_restarts",
            "gpu_mem_mb",
            "gpu_total_mb",
            "gpu_util",
            "manifest_schema_version",
            "checkpoint_n",
            "evidence_valid_windows",
            "evidence_complete_groups",
            "evidence_first_window",
            "evidence_last_window",
            "worker_recovery_count",
            "active_window_n",
            "last_completed_window_n",
        )
        for name in integer_fields:
            value = payload.get(name, 0)
            if isinstance(value, bool):
                raise ValueError(f"{name}_boolean")
            parsed = int(value or 0)
            if parsed < 0:
                raise ValueError(f"{name}_negative")
            setattr(state, name, parsed)
        state.gpu_total_mb = max(1, state.gpu_total_mb)
        for name in (
            "gpu_power_w",
            "gpu_power_limit_w",
            "evidence_latest_at",
            "worker_progress_age_s",
            "active_started_at",
            "active_deadline_at",
            "last_completed_at",
        ):
            value = payload.get(name, 0.0)
            if isinstance(value, bool):
                raise ValueError(f"{name}_boolean")
            parsed = float(value or 0.0)
            if not math.isfinite(parsed) or (
                parsed < 0 and name != "worker_progress_age_s"
            ):
                raise ValueError(f"{name}_invalid")
            setattr(state, name, parsed)
        state.identity_ok = payload.get("identity_ok") is True
        state.wallet_absent_attested = (
            payload.get("wallet_absent_attested") is True
        )
        state.submit_authority = payload.get("submit_authority") is True
        state.worker_progress_fresh = (
            payload.get("worker_progress_fresh") is True
        )
        if state.resolution_source == "controller_manifest":
            state.generator_unit = str(
                payload.get("resolved_generator_unit") or ""
            )
            state.tunnel_unit = str(
                payload.get("resolved_tunnel_unit") or ""
            )
            state.runtime_manifest_path = str(
                payload.get("resolved_runtime_manifest_path") or ""
            )
            state.runtime_profile_path = str(
                payload.get("resolved_runtime_profile_path") or ""
            )
            state.component_config_path = str(
                payload.get("resolved_component_config_path") or ""
            )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        state.error = f"parse:{type(exc).__name__}:{str(exc)[:120]}"


def _collect_component_evidence_in_place(
    state: ComponentState,
    *,
    controller_manifest_path: str = "",
) -> None:
    """Read certification evidence from its authoritative assembly host."""
    payload = {
        "role": state.role,
        "evidence_dir": state.evidence_dir,
        "runtime_manifest_path": state.evidence_runtime_manifest_path,
        "runtime_profile_path": state.evidence_runtime_profile_path,
        "expected_manifest_sha256": state.manifest_sha256,
        "expected_miner_source_revision": state.miner_source_revision,
        "expected_validator_source_revision": state.validator_source_revision,
        "expected_checkpoint_revision": state.checkpoint_revision,
        "expected_gpu_uuid": state.gpu_uuid,
        "expected_runtime_profile_sha256": state.runtime_profile_sha256,
        "controller_manifest_path": controller_manifest_path,
    }
    command = (
        "sudo -n python3 - "
        + shlex.quote(json.dumps(payload, separators=(",", ":")))
        + " <<'PYCOMPONENTEVIDENCE'\n"
        + _COMPONENT_EVIDENCE_PROBE_SOURCE
        + "\nPYCOMPONENTEVIDENCE\n"
    )
    rc, out, err = ssh_run(state.evidence_alias, command, timeout_s=20)
    if rc != 0 or not out.strip():
        detail = (err or f"rc={rc}").strip().replace("\n", " ")
        state.evidence_error = f"probe:{detail[:160]}"
        state.evidence_source_ok = False
        state.evidence_fresh = False
        return
    try:
        payload = json.loads(out.strip().splitlines()[-1])
        if not isinstance(payload, dict) or payload.get("schema_version") != 1:
            raise ValueError("probe_schema")
        state.evidence_error = str(payload.get("error") or "")
        state.evidence_source_ok = payload.get("source_ok") is True
        state.evidence_certificate_bound = (
            payload.get("certificate_bound") is True
        )
        selected_evidence_dir = str(payload.get("evidence_dir") or "")
        if selected_evidence_dir:
            state.evidence_dir = selected_evidence_dir
        state.evidence_manifest_sha256 = str(
            payload.get("manifest_sha256") or ""
        )
        state.evidence_profile_sha256 = str(
            payload.get("profile_sha256") or ""
        )
        for target, source in (
            ("evidence_valid_windows", "valid_windows"),
            ("evidence_complete_groups", "complete_groups"),
            ("evidence_first_window", "first_window"),
            ("evidence_last_window", "last_window"),
        ):
            value = payload.get(source, 0)
            if isinstance(value, bool):
                raise ValueError(f"{source}_boolean")
            parsed = int(value or 0)
            if parsed < 0:
                raise ValueError(f"{source}_negative")
            setattr(state, target, parsed)
        latest = payload.get("latest_completion_at", 0.0)
        if isinstance(latest, bool):
            raise ValueError("latest_completion_at_boolean")
        state.evidence_latest_at = float(latest or 0.0)
        if (
            not math.isfinite(state.evidence_latest_at)
            or state.evidence_latest_at < 0
        ):
            raise ValueError("latest_completion_at_invalid")
        complete = (
            state.evidence_valid_windows
            >= state.evidence_target_windows
        )
        age = (
            max(0.0, time.time() - state.evidence_latest_at)
            if state.evidence_latest_at
            else math.inf
        )
        state.evidence_fresh = bool(
            state.evidence_source_ok
            and (
                state.evidence_certificate_bound
                or complete
                or age <= state.evidence_stale_seconds
            )
        )
        if (
            state.evidence_source_ok
            and not state.evidence_fresh
            and not state.evidence_error
        ):
            state.evidence_error = "stale_incomplete_certification"
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        state.evidence_error = (
            f"parse:{type(exc).__name__}:{str(exc)[:120]}"
        )
        state.evidence_source_ok = False
        state.evidence_fresh = False


def collect_component_snapshot(
    state: ComponentState,
    controller_component: dict[str, Any] | None = None,
) -> ComponentState:
    snapshot = ComponentState(
        alias=state.alias,
        label=state.label,
        color=state.color,
        role=state.role,
        controller_label=state.controller_label,
        generator_unit=state.generator_unit,
        tunnel_unit=state.tunnel_unit,
        runtime_manifest_path=state.runtime_manifest_path,
        runtime_profile_path=state.runtime_profile_path,
        component_config_path=state.component_config_path,
        evidence_alias=state.evidence_alias,
        evidence_dir=state.evidence_dir,
        evidence_runtime_manifest_path=(
            state.evidence_runtime_manifest_path
        ),
        evidence_runtime_profile_path=(
            state.evidence_runtime_profile_path
        ),
        evidence_target_windows=state.evidence_target_windows,
        evidence_stale_seconds=state.evidence_stale_seconds,
        telemetry_stale_seconds=state.telemetry_stale_seconds,
    )
    _collect_component_in_place(snapshot, controller_component)
    if controller_component is not None:
        manifest_path = str(controller_component.get("manifest_path") or "")
        if manifest_path.startswith("/"):
            snapshot.evidence_runtime_manifest_path = manifest_path
            evidence_root = os.path.dirname(manifest_path)
            snapshot.evidence_runtime_profile_path = os.path.join(
                evidence_root, "generation-profile-rtx.json"
            )
            snapshot.evidence_dir = evidence_root
    controller_manifest_path = (
        str(controller_component.get("controller_manifest_path") or "")
        if controller_component is not None
        else ""
    )
    _collect_component_evidence_in_place(
        snapshot,
        controller_manifest_path=controller_manifest_path,
    )
    return snapshot


_STANDALONE_CERTIFICATION_PROBE_SOURCE = r"""
import csv
import glob
import hashlib
import io
import json
import os
import re
import stat
import subprocess
import sys
import time

MAX_BYTES = 8 * 1024 * 1024
SHA256 = re.compile(r"[0-9a-f]{64}")
GPU_UUID = re.compile(
    r"GPU-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}"
    r"-[0-9a-fA-F]{12}"
)

def absolute(value, name):
    value = str(value or "")
    if not value.startswith("/"):
        raise ValueError(name)
    return value

def read_regular(path, max_bytes=MAX_BYTES):
    before = os.lstat(path)
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ValueError("not_regular")
    if before.st_size <= 0 or before.st_size > max_bytes:
        raise ValueError("size")
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        opened = os.fstat(descriptor)
        if (
            opened.st_dev, opened.st_ino, opened.st_size,
            opened.st_mtime_ns,
        ) != (
            before.st_dev, before.st_ino, before.st_size,
            before.st_mtime_ns,
        ):
            raise ValueError("changed_before_read")
        raw = os.read(descriptor, max_bytes + 1)
        if not raw or len(raw) > max_bytes:
            raise ValueError("size")
        after = os.lstat(path)
        if (
            after.st_dev, after.st_ino, after.st_size,
            after.st_mtime_ns,
        ) != (
            before.st_dev, before.st_ino, before.st_size,
            before.st_mtime_ns,
        ):
            raise ValueError("changed_during_read")
        return raw
    finally:
        os.close(descriptor)

def read_json(path):
    value = json.loads(read_regular(path).decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("json_object")
    return value

def cmdline(pid):
    try:
        raw = open(
            "/proc/" + str(pid) + "/cmdline", "rb", buffering=0
        ).read(65537)
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return []
    if not raw or len(raw) > 65536:
        return []
    try:
        return [
            item.decode("utf-8")
            for item in raw.split(b"\0")
            if item
        ]
    except UnicodeDecodeError:
        return []

def ppid(pid):
    try:
        with open(
            "/proc/" + str(pid) + "/status",
            "r",
            encoding="utf-8",
        ) as handle:
            for line in handle:
                if line.startswith("PPid:"):
                    return int(line.split(":", 1)[1].strip())
    except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError):
        pass
    return 0

def started_at(pid):
    with open(
        "/proc/" + str(pid) + "/stat",
        "r",
        encoding="utf-8",
    ) as handle:
        raw = handle.read(65536)
    if not raw or len(raw) >= 65536:
        raise ValueError("process_stat")
    tail = raw.rsplit(")", 1)[1].strip().split()
    ticks = int(tail[19])
    clock = os.sysconf("SC_CLK_TCK")
    uptime = float(open("/proc/uptime", encoding="utf-8").read().split()[0])
    return time.time() - uptime + ticks / clock

def option(argv, name):
    positions = [
        index for index, value in enumerate(argv) if value == name
    ]
    if len(positions) != 1 or positions[0] + 1 >= len(argv):
        raise ValueError("option:" + name)
    return argv[positions[0] + 1]

def run(command):
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )

def systemd_properties(unit):
    result = run([
        "systemctl", "show", unit, "--no-pager",
        "--property=LoadState,ActiveState,SubState,MainPID,User,"
        "NoNewPrivileges",
    ])
    if result.returncode != 0:
        raise ValueError("systemd_show")
    return dict(
        line.split("=", 1)
        for line in result.stdout.splitlines()
        if "=" in line
    )

def gpu_telemetry(expected_uuid):
    result = run([
        "nvidia-smi",
        "--query-gpu=uuid,name,utilization.gpu,memory.used,memory.total,"
        "power.draw,power.limit",
        "--format=csv,noheader,nounits",
    ])
    if result.returncode != 0:
        raise ValueError("nvidia_smi")
    for row in csv.reader(io.StringIO(result.stdout)):
        if len(row) != 7 or row[0].strip() != expected_uuid:
            continue
        return {
            "uuid": row[0].strip(),
            "name": row[1].strip(),
            "utilization_pct": int(float(row[2].strip())),
            "memory_used_mb": int(float(row[3].strip())),
            "memory_total_mb": int(float(row[4].strip())),
            "power_w": float(row[5].strip()),
            "power_limit_w": float(row[6].strip()),
        }
    raise ValueError("gpu_missing")

def gpu_pids(expected_uuid):
    result = run([
        "nvidia-smi",
        "--query-compute-apps=pid,gpu_uuid,used_memory",
        "--format=csv,noheader,nounits",
    ])
    observed = {}
    if result.returncode != 0:
        return observed
    for row in csv.reader(io.StringIO(result.stdout)):
        if len(row) != 3 or row[1].strip() != expected_uuid:
            continue
        try:
            observed[int(row[0].strip())] = int(float(row[2].strip()))
        except ValueError:
            continue
    return observed

def artifact_state(directory):
    names = (
        "reference-proof.json",
        "candidate-proof.json",
        "parity.json",
        "validator-replay.json",
        "runtime-certificate-v3.canary.json",
        "runtime-certificate-v3.canary.report.json",
        "runtime.bound.json",
    )
    return {name: os.path.isfile(directory + "/" + name) for name in names}

out = {
    "schema_version": 1,
    "generated_at": time.time(),
    "active": False,
    "exact": False,
    "error": "",
}
try:
    config = json.loads(sys.argv[1])
    if not isinstance(config, dict):
        raise ValueError("config")
    runner = absolute(config.get("runner_path"), "runner_path")
    artifact_dir = absolute(config.get("artifact_dir"), "artifact_dir")
    manifest_path = absolute(
        config.get("runtime_manifest_path"), "runtime_manifest_path"
    )
    profile_path = absolute(
        config.get("proof_profile_path"), "proof_profile_path"
    )
    checkpoint_path = absolute(
        config.get("checkpoint_path"), "checkpoint_path"
    )
    expected_gpu = str(config.get("gpu_uuid") or "")
    expected_sha = str(config.get("runner_sha256") or "").lower()
    service_unit = str(config.get("service_unit") or "")
    service_user = str(config.get("service_user") or "")
    service_config_path = str(config.get("service_config_path") or "")
    service_config_sha256 = str(
        config.get("service_config_sha256") or ""
    ).lower()
    progress_path = str(config.get("progress_path") or "")
    service_kind = str(
        config.get("service_kind")
        or ("wallet_free_generator" if service_unit else "")
    )
    if not SHA256.fullmatch(expected_sha) or not GPU_UUID.fullmatch(
        expected_gpu
    ):
        raise ValueError("config_identity")
    if not profile_path.startswith(artifact_dir.rstrip("/") + "/") or (
        not service_unit
        and not runner.startswith(artifact_dir.rstrip("/") + "/")
    ):
        raise ValueError("artifact_scope")
    runner_raw = read_regular(runner, 64 * 1024 * 1024)
    if hashlib.sha256(runner_raw).hexdigest() != expected_sha:
        raise ValueError("runner_sha256")
    if service_unit:
        service_config_path = absolute(
            service_config_path, "service_config_path"
        )
        if (
            not SHA256.fullmatch(service_config_sha256)
            or hashlib.sha256(read_regular(service_config_path)).hexdigest()
            != service_config_sha256
        ):
            raise ValueError("service_config_sha256")
    manifest = read_json(manifest_path)
    profile = read_json(profile_path)
    identity = manifest.get("identity")
    if not isinstance(identity, dict):
        raise ValueError("manifest_identity")
    miner_source = str(manifest.get("miner_source_revision") or "").lower()
    validator_source = str(
        identity.get("validator_source_revision") or ""
    ).lower()
    checkpoint_revision = str(
        identity.get("checkpoint_revision") or ""
    ).lower()
    if (
        not re.fullmatch(r"[0-9a-f]{40}", miner_source)
        or not re.fullmatch(r"[0-9a-f]{40}", validator_source)
        or not re.fullmatch(r"[0-9a-f]{40}", checkpoint_revision)
        or (
            "GPU-" + str(profile.get("gpu_uuid") or "").removeprefix("GPU-")
        ) != expected_gpu
        or str(profile.get("source_revision") or "").lower()
        != validator_source
        or str(profile.get("checkpoint_revision") or "").lower()
        != checkpoint_revision
    ):
        raise ValueError("runtime_profile_identity")

    process_rows = {}
    for proc_path in glob.glob("/proc/[0-9]*"):
        pid = int(proc_path.rsplit("/", 1)[1])
        argv = cmdline(pid)
        if argv:
            process_rows[pid] = {"argv": argv, "ppid": ppid(pid)}

    def descendants_of(root_pid):
        result = set()
        changed = True
        while changed:
            changed = False
            for pid, row in process_rows.items():
                if pid in result or pid == root_pid:
                    continue
                if row["ppid"] == root_pid or row["ppid"] in result:
                    result.add(pid)
                    changed = True
        return result

    certification_worker_pid = 0
    if service_unit:
        properties = systemd_properties(service_unit)
        runner_pid = int(properties.get("MainPID") or 0)
        argv = process_rows.get(runner_pid, {}).get("argv", [])
        if (
            properties.get("LoadState") != "loaded"
            or properties.get("ActiveState") != "active"
            or properties.get("SubState") != "running"
            or properties.get("User") != service_user
            or properties.get("NoNewPrivileges") != "yes"
        ):
            raise ValueError("wallet_free_service_identity")
        if service_kind == "wallet_free_generator":
            generation_only = (
                "worker" in argv
                or (
                    "-m" in argv
                    and "reliquary_miner_pro_code.generator.server" in argv
                )
            )
            if (
                (
                    runner not in argv
                    and (not argv or os.path.realpath(argv[0]) != runner)
                )
                or option(argv, "--config") != service_config_path
                or not generation_only
                or "submit" in argv
                or "mine" in argv
            ):
                raise ValueError("wallet_free_service_identity")
        elif service_kind == "submit_disabled_certification":
            candidates = []
            for pid in sorted(descendants_of(runner_pid)):
                child_argv = process_rows[pid]["argv"]
                if not child_argv or os.path.realpath(child_argv[0]) != runner:
                    continue
                if (
                    "reliquary_miner_pro_math.evidence_producer"
                    not in child_argv
                    or "capture-public-oracle-proof" not in child_argv
                    or child_argv.count("--submit-disabled") != 1
                    or option(child_argv, "--manifest") != manifest_path
                    or option(child_argv, "--proof-profile") != profile_path
                    or option(child_argv, "--checkpoint") != checkpoint_path
                    or "submit" in child_argv
                    or "mine" in child_argv
                ):
                    continue
                output_path = str(option(child_argv, "--output") or "")
                audit_path = str(option(child_argv, "--audit") or "")
                prefix = artifact_dir.rstrip("/") + "/"
                if not output_path.startswith(prefix) or not audit_path.startswith(
                    prefix
                ):
                    continue
                candidates.append(pid)
            if len(candidates) != 1:
                raise ValueError("submit_disabled_service_identity")
            certification_worker_pid = candidates[0]
        else:
            raise ValueError("service_kind")
        runners = [runner_pid]
    else:
        runners = [
            pid for pid, row in process_rows.items()
            if row["argv"] in (
                ["/bin/bash", runner],
                ["/usr/bin/bash", runner],
            )
        ]
    if not runners:
        out.update({
            "error": "not_running",
            "runner_path": runner,
            "artifact_dir": artifact_dir,
        })
    else:
        if len(runners) != 1:
            raise ValueError("runner_count")
        runner_pid = runners[0]
        descendants = descendants_of(runner_pid)

        phases = []
        worker_pid = 0
        for pid in sorted(descendants) if not service_unit else []:
            argv = process_rows[pid]["argv"]
            if "capture-proof" in argv:
                if argv.count("--submit-disabled") != 1:
                    raise ValueError("capture_not_submit_disabled")
                label = option(argv, "--label")
                if label not in {"reference", "candidate"}:
                    raise ValueError("capture_label")
                if (
                    option(argv, "--manifest") != manifest_path
                    or option(argv, "--proof-profile") != profile_path
                    or option(argv, "--checkpoint") != checkpoint_path
                    or option(argv, "--corpus")
                    != artifact_dir + "/corpus-128.json"
                    or option(argv, "--output")
                    != artifact_dir + "/" + label + "-proof.json"
                    or option(argv, "--audit")
                    != artifact_dir + "/" + label + "-proof.audit.json"
                ):
                    raise ValueError("capture_paths")
                phases.append((label + "_proof", pid))
            elif "assemble-parity" in argv:
                phases.append(("parity_assembly", pid))
            elif "assemble-validator-replay" in argv:
                phases.append(("validator_replay_assembly", pid))
            elif "build" in argv and any(
                "reliquary-miner-pro-math-certify" in value
                for value in argv
            ):
                phases.append(("certificate_build", pid))
            elif "certify" in argv and any(
                value.endswith("/reliquary-miner-pro-math")
                for value in argv
            ):
                phases.append(("canary_gate", pid))
            elif "attach-certificate" in argv:
                phases.append(("attach_certificate", pid))
        if service_unit and service_kind == "submit_disabled_certification":
            phase = "public_oracle_proof"
            worker_pid = certification_worker_pid
        elif service_unit:
            phase = "generation_ready"
            if progress_path and os.path.isfile(progress_path):
                progress = read_json(absolute(progress_path, "progress_path"))
                if any(
                    progress.get(key) not in (None, identity.get(identity_key))
                    for key, identity_key in (
                        ("checkpoint_revision", "checkpoint_revision"),
                        ("generation_profile_id", "generation_profile_id"),
                        (
                            "generation_contract_sha256",
                            "generation_contract_sha256",
                        ),
                    )
                ):
                    raise ValueError("progress_identity")
                if progress.get("pid") not in (None, runner_pid):
                    raise ValueError("progress_pid")
                phase = {
                    "loading": "generation_loading",
                    "ready": "generation_ready",
                    "active": "generation_capture",
                    "recovering": "generation_recovering",
                    "failed": "generation_failed",
                }.get(str(progress.get("state") or ""), phase)
            compute_pids = gpu_pids(expected_gpu)
            bound = sorted((descendants | {runner_pid}) & set(compute_pids))
            worker_pid = bound[0] if bound else runner_pid
        elif phases:
            unique = {phase for phase, _pid in phases}
            if unique == {"parity_assembly", "validator_replay_assembly"}:
                phase = "parity_and_replay"
            else:
                phase = phases[0][0]
            worker_pid = phases[0][1]
        else:
            phase = "transition"

        gpu = gpu_telemetry(expected_gpu)
        compute_pids = gpu_pids(expected_gpu)
        runner_started_at = started_at(runner_pid)
        out.update({
            "active": True,
            "exact": True,
            "submit_disabled_attested": True,
            "wallet_free_attested": bool(
                service_unit and service_kind == "wallet_free_generator"
            ),
            "runner_pid": runner_pid,
            "worker_pid": worker_pid,
            "phase": phase,
            "runner_path": runner,
            "runner_sha256": expected_sha,
            "artifact_dir": artifact_dir,
            "runtime_manifest_path": manifest_path,
            "proof_profile_path": profile_path,
            "checkpoint_path": checkpoint_path,
            "service_unit": service_unit,
            "service_user": service_user,
            "service_config_path": service_config_path,
            "service_config_sha256": service_config_sha256,
            "progress_path": progress_path,
            "service_kind": service_kind,
            "started_at": runner_started_at,
            "elapsed_s": max(0.0, time.time() - runner_started_at),
            "gpu": gpu,
            "gpu_process_bound": bool(
                worker_pid and worker_pid in compute_pids
            ),
            "gpu_worker_memory_mb": compute_pids.get(worker_pid, 0),
            "identity": {
                "miner_source_revision": miner_source,
                "validator_source_revision": validator_source,
                "checkpoint_n": identity.get("checkpoint_n"),
                "checkpoint_revision": checkpoint_revision,
                "model_repo": identity.get("model_repo")
                or identity.get("checkpoint_repo_id"),
                "environment": identity.get("environment"),
            },
            "artifacts": artifact_state(artifact_dir),
        })
except Exception as exc:
    out["error"] = type(exc).__name__ + ":" + str(exc)[:120]
print(json.dumps(out, separators=(",", ":"), sort_keys=True))
"""


_STANDALONE_TELEMETRY_PROBE_SOURCE = r"""
import hashlib
import json
import os
import re
import sqlite3
import stat
import sys
import urllib.parse

MAX_BYTES = 1024 * 1024
GPU_UUID = re.compile(
    r"(?:GPU-)?([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
)

def read_bytes(path, max_bytes=MAX_BYTES, allow_zero_size=False):
    before = os.lstat(path)
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ValueError("not_regular")
    if (
        (before.st_size <= 0 and not allow_zero_size)
        or before.st_size > max_bytes
    ):
        raise ValueError("size")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
        ) != (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ):
            raise ValueError("changed_before_read")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            value = handle.read(max_bytes + 1)
        if not value or len(value) > max_bytes:
            raise ValueError("size")
        after = os.lstat(path)
        if (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ) != (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ):
            raise ValueError("changed_during_read")
        return value, before.st_mtime
    finally:
        if descriptor >= 0:
            os.close(descriptor)

def read_object(path):
    raw, mtime = read_bytes(path)
    value = json.loads(raw.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("not_object")
    return value, mtime, hashlib.sha256(raw).hexdigest()

def toml_string_values(path):
    raw, _mtime = read_bytes(path)
    section = ""
    values = {}
    for raw_line in raw.decode("utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip()
            continue
        if "=" not in line:
            continue
        key, encoded = (part.strip() for part in line.split("=", 1))
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        if not encoded.startswith('"'):
            continue
        try:
            value = json.loads(encoded)
        except json.JSONDecodeError as exc:
            raise ValueError("config_string") from exc
        if not isinstance(value, str):
            raise ValueError("config_string")
        compound = (section + "." + key).lstrip(".")
        if compound in values:
            raise ValueError("config_duplicate")
        values[compound] = value
    return values

def absolute_path(value, name):
    if not isinstance(value, str) or not value.startswith("/"):
        raise ValueError(name)
    return value

def canonical_gpu_id(value):
    match = GPU_UUID.fullmatch(str(value or "").strip())
    if not match:
        raise ValueError("physical_gpu_id")
    return "GPU-" + match.group(1).lower()

def controller_paths(config_path, active_pid):
    config_path = absolute_path(config_path, "controller_config_path")
    try:
        pid = int(active_pid)
    except (TypeError, ValueError) as exc:
        raise ValueError("active_pid") from exc
    if pid <= 0:
        raise ValueError("active_pid")
    raw_cmdline, _mtime = read_bytes(
        "/proc/" + str(pid) + "/cmdline",
        max_bytes=64 * 1024,
        allow_zero_size=True,
    )
    argv = [
        item.decode("utf-8")
        for item in raw_cmdline.split(b"\0")
        if item
    ]
    positions = [
        index for index, value in enumerate(argv) if value == "--config"
    ]
    if (
        len(positions) != 1
        or positions[0] + 1 >= len(argv)
        or argv[positions[0] + 1] != config_path
    ):
        raise ValueError("process_config_mismatch")
    modes = [value for value in argv if value in {"canary", "mine"}]
    if len(modes) != 1:
        raise ValueError("controller_mode")
    values = toml_string_values(config_path)
    if values.get("mode") != modes[0]:
        raise ValueError("config_mode_mismatch")
    return {
        "mode": modes[0],
        "config_path": config_path,
        "runtime_manifest": absolute_path(
            values.get("paths.runtime_manifest"),
            "runtime_manifest",
        ),
        "dashboard_json": absolute_path(
            values.get("paths.dashboard_json"),
            "dashboard_json",
        ),
        "ledger_db": absolute_path(
            values.get("paths.ledger_db"),
            "ledger_db",
        ),
    }

def ledger_summary(path, since, until):
    info = os.lstat(path)
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise ValueError("ledger_not_regular")
    if info.st_size <= 0 or info.st_size > 2 * 1024 * 1024 * 1024:
        raise ValueError("ledger_size")
    uri = "file:" + urllib.parse.quote(path, safe="/") + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=2.0)
    try:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(attempts)")
        }
        code_attempt_columns = {
            "physical_gpu_id", "window_n", "natural_complete", "created_at"
        }
        interval_columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(gpu_intervals)")
        }
        code_interval_columns = {
            "physical_gpu_id", "started_at", "ended_at"
        }
        generated_columns = (
            {
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(generated_groups)"
                )
            }
            if "generated_groups" in tables
            else set()
        )
        math_attempt_columns = {"window_n", "prompt_idx", "created_at"}
        math_generated_columns = {
            "window_n",
            "prompt_idx",
            "physical_gpu_id",
            "natural_complete_m8",
        }
        math_interval_columns = {
            "gpu_uuid", "started_at", "completed_at"
        }
        code_schema = (
            code_attempt_columns.issubset(columns)
            and code_interval_columns.issubset(interval_columns)
        )
        math_schema = (
            math_attempt_columns.issubset(columns)
            and math_generated_columns.issubset(generated_columns)
            and math_interval_columns.issubset(interval_columns)
        )
        if not code_schema and not math_schema:
            raise ValueError("ledger_schema")
        attempts = {}
        if code_schema:
            rows = connection.execute(
                "SELECT physical_gpu_id, window_n, natural_complete "
                "FROM attempts WHERE created_at >= ? AND created_at <= ?",
                (since, until),
            )
        else:
            # Math records raw GPU generation before CPU grading and creates an
            # attempt only for a candidate that survives construction. Join on
            # the immutable window/prompt identity so an H100/RTX-style
            # attribution is never guessed or collapsed onto the controller.
            rows = connection.execute(
                "SELECT g.physical_gpu_id,a.window_n,g.natural_complete_m8 "
                "FROM attempts a JOIN generated_groups g "
                "ON g.window_n=a.window_n AND g.prompt_idx=a.prompt_idx "
                "WHERE a.created_at >= ? AND a.created_at <= ?",
                (since, until),
            )
        for raw_gpu, raw_window, raw_natural in rows:
            gpu = canonical_gpu_id(raw_gpu)
            row = attempts.setdefault(gpu, {
                "aliases": set(),
                "attempts": 0,
                "natural_complete_m8": 0,
                "windows": set(),
                "natural_windows": set(),
            })
            row["aliases"].add(str(raw_gpu))
            row["attempts"] += 1
            row["windows"].add(int(raw_window))
            if int(raw_natural) == 1:
                row["natural_complete_m8"] += 1
                row["natural_windows"].add(int(raw_window))
        intervals = {}
        if code_schema:
            rows = connection.execute(
                "SELECT physical_gpu_id, started_at, ended_at "
                "FROM gpu_intervals WHERE ended_at > ? AND started_at < ?",
                (since, until),
            )
        else:
            rows = connection.execute(
                "SELECT gpu_uuid, started_at, completed_at "
                "FROM gpu_intervals "
                "WHERE completed_at > ? AND started_at < ?",
                (since, until),
            )
        for raw_gpu, raw_start, raw_end in rows:
            gpu = canonical_gpu_id(raw_gpu)
            start = max(float(raw_start), since)
            end = min(float(raw_end), until)
            if not end > start:
                continue
            intervals.setdefault(gpu, []).append((start, end))
            attempts.setdefault(gpu, {
                "aliases": set(),
                "attempts": 0,
                "natural_complete_m8": 0,
                "windows": set(),
                "natural_windows": set(),
            })["aliases"].add(str(raw_gpu))
        result = {}
        for gpu in sorted(attempts):
            source = attempts[gpu]
            merged = []
            for start, end in sorted(intervals.get(gpu, [])):
                if merged and start <= merged[-1][1]:
                    merged[-1][1] = max(merged[-1][1], end)
                else:
                    merged.append([start, end])
            windows = source["windows"]
            natural_windows = source["natural_windows"]
            result[gpu] = {
                "aliases": sorted(source["aliases"]),
                "attempts": source["attempts"],
                "natural_complete_m8": source["natural_complete_m8"],
                "generated_windows": len(windows),
                "natural_complete_windows": len(natural_windows),
                "first_window": min(windows) if windows else 0,
                "last_window": max(windows) if windows else 0,
                "last_natural_window": (
                    max(natural_windows) if natural_windows else 0
                ),
                "physical_gpu_hours": sum(
                    end - start for start, end in merged
                ) / 3600.0,
            }
        return {
            "schema_version": 1,
            "per_gpu": result,
            "physical_gpu_hours": sum(
                row["physical_gpu_hours"] for row in result.values()
            ),
        }
    finally:
        connection.close()

out = {"schema_version": 1}
try:
    telemetry_path = absolute_path(sys.argv[1], "telemetry_path")
    fallback_manifest = absolute_path(sys.argv[2], "fallback_manifest")
    config_path = sys.argv[3] if len(sys.argv) > 3 else ""
    active_unit = sys.argv[4] if len(sys.argv) > 4 else ""
    active_pid = sys.argv[5] if len(sys.argv) > 5 else "0"
    supervisor_path = sys.argv[6] if len(sys.argv) > 6 else ""
    active = None
    manifest_path = fallback_manifest
    ledger = None
    if config_path:
        active = controller_paths(config_path, active_pid)
        if active["dashboard_json"] != telemetry_path:
            raise ValueError("active_dashboard_path_mismatch")
        manifest_path = active["runtime_manifest"]
    telemetry, telemetry_mtime, telemetry_sha256 = read_object(telemetry_path)
    manifest, manifest_mtime, manifest_sha256 = read_object(manifest_path)
    supervisor = None
    supervisor_mtime = None
    supervisor_error = ""
    if supervisor_path:
        supervisor_path = absolute_path(supervisor_path, "supervisor_path")
        if os.path.exists(supervisor_path):
            supervisor, supervisor_mtime, _supervisor_sha256 = read_object(
                supervisor_path
            )
        else:
            supervisor_error = "missing"
    if active is not None:
        raw_since = telemetry.get("since")
        raw_until = telemetry.get("until")
        if raw_since is None and raw_until is None:
            # Math publishes a lifetime ledger plus release/latest-six scopes,
            # not Code's timestamp-bounded top-level interval.
            since = 0.0
            until = float(telemetry.get("generated_at"))
            if not until > 0:
                raise ValueError("telemetry_interval")
        else:
            since = float(raw_since)
            until = float(raw_until)
            if (
                not since > 0
                or not until > since
                or until - since > 172800.0
            ):
                raise ValueError("telemetry_interval")
        ledger = ledger_summary(active["ledger_db"], since, until)
    identity = manifest.get("identity")
    if not isinstance(identity, dict):
        raise ValueError("manifest_identity")
    raw_components = manifest.get("components", [])
    if raw_components is None:
        raw_components = []
    if not isinstance(raw_components, list):
        raise ValueError("manifest_components")
    components = []
    for raw_component in raw_components:
        if not isinstance(raw_component, dict):
            raise ValueError("manifest_component")
        components.append({
            key: raw_component.get(key)
            for key in (
                "role",
                "component_id",
                "manifest_path",
                "manifest_file_sha256",
                "runtime_payload_sha256",
                "runtime_profile_sha256",
                "health_gpu_uuid_required",
            )
        })
    out.update({
        "telemetry": telemetry,
        "telemetry_mtime": telemetry_mtime,
        "telemetry_sha256": telemetry_sha256,
        "manifest_mtime": manifest_mtime,
        "manifest_sha256": manifest_sha256,
        "manifest": {
            "schema_version": manifest.get("schema_version"),
            "miner_source_revision": manifest.get("miner_source_revision"),
            "validator_source_revision": identity.get(
                "validator_source_revision"
            ),
            "observed_validator_image_revision": identity.get(
                "observed_validator_image_revision"
            ),
            "checkpoint_n": identity.get("checkpoint_n"),
            "checkpoint_revision": identity.get("checkpoint_revision"),
            "protocol_version": identity.get("protocol_version"),
            "generation_profile_id": identity.get("generation_profile_id"),
            "generation_contract_sha256": identity.get(
                "generation_contract_sha256"
            ),
            "checkpoint_profile_sha256": identity.get(
                "checkpoint_profile_sha256"
            ),
            "runtime_fingerprint_sha256": identity.get(
                "runtime_fingerprint_sha256"
            ),
            "model_repo": identity.get("model_repo"),
            "environment": identity.get("environment"),
            "components": components,
        },
        "controller": {
            "active_unit": active_unit,
            "mode": active["mode"] if active is not None else "",
            "config_path": (
                active["config_path"] if active is not None else ""
            ),
            "runtime_manifest_path": manifest_path,
            "ledger_path": active["ledger_db"] if active is not None else "",
        },
        "ledger": ledger,
        "supervisor": supervisor,
        "supervisor_mtime": supervisor_mtime,
        "supervisor_error": supervisor_error,
    })
except Exception as exc:
    out["error"] = type(exc).__name__ + ":" + str(exc)[:120]
print(json.dumps(out, separators=(",", ":"), sort_keys=True))
"""


def _standalone_counter(
    mapping: dict[str, Any],
    *names: str,
    required: bool = False,
) -> int | None:
    for name in names:
        if name not in mapping:
            continue
        value = mapping[name]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"invalid_counter:{name}")
        return value
    if required:
        raise ValueError(f"missing_counter:{names[0]}")
    return None


def _standalone_nonnegative_float(
    mapping: dict[str, Any],
    *names: str,
    required: bool = False,
) -> float | None:
    for name in names:
        if name not in mapping:
            continue
        value = mapping[name]
        if value is None and not required:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"invalid_float:{name}")
        parsed = float(value)
        if not math.isfinite(parsed) or parsed < 0:
            raise ValueError(f"invalid_float:{name}")
        return parsed
    if required:
        raise ValueError(f"missing_float:{names[0]}")
    return None


_PHYSICAL_GPU_UUID_RE = re.compile(
    r"(?:GPU-)?([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
)


def _canonical_physical_gpu_id(value: object) -> str:
    """Collapse NVIDIA UUID aliases without conflating non-UUID identities."""
    match = _PHYSICAL_GPU_UUID_RE.fullmatch(str(value or "").strip())
    if not match:
        raise ValueError("physical_gpu_id")
    return "GPU-" + match.group(1).lower()


def _standalone_ledger_gpu_summary(
    raw: object,
) -> tuple[dict[str, dict[str, object]], float | None]:
    """Validate process-bound, alias-normalized generation-only ledger data."""
    if raw is None:
        return {}, None
    if not isinstance(raw, dict) or raw.get("schema_version") != 1:
        raise ValueError("ledger_schema")
    per_gpu_raw = raw.get("per_gpu")
    if not isinstance(per_gpu_raw, dict):
        raise ValueError("ledger_per_gpu")
    per_gpu: dict[str, dict[str, object]] = {}
    for raw_gpu, raw_row in per_gpu_raw.items():
        gpu = _canonical_physical_gpu_id(raw_gpu)
        if gpu != raw_gpu or gpu in per_gpu or not isinstance(raw_row, dict):
            raise ValueError("ledger_gpu_identity")
        attempts = _standalone_counter(raw_row, "attempts", required=True)
        natural = _standalone_counter(
            raw_row, "natural_complete_m8", required=True
        )
        windows = _standalone_counter(
            raw_row, "generated_windows", required=True
        )
        natural_windows = _standalone_counter(
            raw_row, "natural_complete_windows", required=True
        )
        first_window = _standalone_counter(
            raw_row, "first_window", required=True
        )
        last_window = _standalone_counter(
            raw_row, "last_window", required=True
        )
        last_natural_window = _standalone_counter(
            raw_row, "last_natural_window"
        )
        # Older dashboard probes did not publish the natural-only frontier.
        # The aggregate last window is an exact fallback only when every
        # recorded attempt completed naturally; otherwise retain unknown.
        if last_natural_window is None and natural == attempts:
            last_natural_window = last_window
        gpu_hours = _standalone_nonnegative_float(
            raw_row, "physical_gpu_hours", required=True
        )
        aliases = raw_row.get("aliases")
        if (
            natural > attempts
            or windows > attempts
            or natural_windows > windows
            or natural_windows > natural
            or not isinstance(aliases, list)
            or not aliases
            or not all(isinstance(alias, str) for alias in aliases)
            or any(
                _canonical_physical_gpu_id(alias) != gpu
                for alias in aliases
            )
        ):
            raise ValueError("ledger_gpu_counters")
        if attempts:
            if first_window <= 0 or last_window < first_window:
                raise ValueError("ledger_gpu_windows")
        elif first_window or last_window or windows or natural_windows:
            raise ValueError("ledger_gpu_windows")
        if natural_windows:
            if (
                last_natural_window is not None
                and not (
                    first_window
                    <= last_natural_window
                    <= last_window
                )
            ):
                raise ValueError("ledger_gpu_natural_window")
        elif last_natural_window not in {None, 0}:
            raise ValueError("ledger_gpu_natural_window")
        per_gpu[gpu] = {
            "aliases": list(aliases),
            "attempts": attempts,
            "natural_complete_m8": natural,
            "generated_windows": windows,
            "natural_complete_windows": natural_windows,
            "first_window": first_window,
            "last_window": last_window,
            "last_natural_window": last_natural_window,
            "physical_gpu_hours": gpu_hours,
        }
    physical_gpu_hours = _standalone_nonnegative_float(
        raw, "physical_gpu_hours", required=True
    )
    observed = sum(
        float(row["physical_gpu_hours"]) for row in per_gpu.values()
    )
    if not math.isclose(
        physical_gpu_hours,
        observed,
        rel_tol=1e-9,
        abs_tol=1e-9,
    ):
        raise ValueError("ledger_gpu_hours")
    return per_gpu, physical_gpu_hours


def _standalone_comparison_scopes(raw: object) -> dict[str, dict[str, Any]]:
    """Validate optional release/latest-window GPU comparison telemetry."""

    def nullable_float(
        mapping: dict[str, Any],
        name: str,
    ) -> float | None:
        if mapping.get(name) is None:
            return None
        return _standalone_nonnegative_float(
            mapping, name, required=True
        )

    def timing_map(
        raw_timings: object,
    ) -> dict[str, dict[str, int | float | None]]:
        if not isinstance(raw_timings, dict):
            raise ValueError("comparison_timings")
        timings: dict[str, dict[str, int | float | None]] = {}
        for stage, raw_timing in raw_timings.items():
            if (
                not isinstance(stage, str)
                or not stage
                or not isinstance(raw_timing, dict)
            ):
                raise ValueError("comparison_timing")
            timings[stage] = {
                "count": _standalone_counter(
                    raw_timing, "count", required=True
                ),
                "p50": nullable_float(raw_timing, "p50"),
                "p95": nullable_float(raw_timing, "p95"),
                "max": nullable_float(raw_timing, "max"),
            }
        return timings

    def quota_map(
        raw_quota: object,
        *,
        error_name: str,
    ) -> dict[str, int | float | None]:
        if not isinstance(raw_quota, dict):
            raise ValueError(error_name)
        quota: dict[str, int | float | None] = {
            "capacity_per_hotkey": _standalone_counter(
                raw_quota, "capacity_per_hotkey", required=True
            ),
            "peak_effective": _standalone_counter(
                raw_quota, "peak_effective", required=True
            ),
            "peak_occupancy": nullable_float(
                raw_quota, "peak_occupancy"
            ),
            "dropped_solely_for_quota": _standalone_counter(
                raw_quota, "dropped_solely_for_quota", required=True
            ),
        }
        capacity = int(quota["capacity_per_hotkey"] or 0)
        peak = int(quota["peak_effective"] or 0)
        occupancy = quota["peak_occupancy"]
        if (
            peak > capacity
            or (
                isinstance(occupancy, float)
                and occupancy > 1.0
            )
        ):
            raise ValueError(error_name)
        return quota

    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError("comparison_scopes")
    allowed_scopes = {"lifetime", "release", "latest_six", "active_profile"}
    if not set(raw).issubset(allowed_scopes):
        raise ValueError("comparison_scope_name")
    result: dict[str, dict[str, Any]] = {}
    funnel_names = (
        "attempts",
        "generation_complete",
        "natural_complete_m8",
        "local_eligible",
        "precommit_accepted",
        "pool_accepted",
        "selected",
        "rewarded",
        "raw_k2",
        "malformed_k2",
        "distribution_dropped_k2",
        "exact_preflight_passing_k2",
    )
    gpu_counter_names = (
        "generated_natural_m8",
        "raw_k2",
        "malformed_k2",
        "distribution_dropped_k2",
        "exact_preflight_passing_k2",
        "precommit_accepted",
        "pool_accepted",
        "selected",
        "rewarded",
    )
    for scope_name, raw_scope in raw.items():
        if not isinstance(raw_scope, dict):
            raise ValueError("comparison_scope")
        window_start = (
            None
            if raw_scope.get("window_start") is None
            else _standalone_counter(
                raw_scope, "window_start", required=True
            )
        )
        window_end = (
            None
            if raw_scope.get("window_end") is None
            else _standalone_counter(
                raw_scope, "window_end", required=True
            )
        )
        window_count = _standalone_counter(
            raw_scope, "window_count", required=True
        )
        if (
            (window_start is None) != (window_end is None)
            or (
                window_start is not None
                and (
                    window_end < window_start
                    or window_count != window_end - window_start + 1
                )
            )
        ):
            raise ValueError("comparison_windows")
        raw_funnel = raw_scope.get("funnel")
        if not isinstance(raw_funnel, dict):
            raise ValueError("comparison_funnel")
        funnel = {
            name: _standalone_counter(raw_funnel, name, required=True)
            for name in funnel_names
        }
        attempts = int(funnel["attempts"])
        if (
            int(funnel["natural_complete_m8"])
            > int(funnel["generation_complete"])
            or int(funnel["local_eligible"]) > attempts
            or int(funnel["raw_k2"]) > attempts
            or int(funnel["malformed_k2"]) > int(funnel["raw_k2"])
            or int(funnel["distribution_dropped_k2"])
            > int(funnel["raw_k2"])
            or int(funnel["exact_preflight_passing_k2"])
            > int(funnel["raw_k2"])
            or int(funnel["precommit_accepted"])
            > int(funnel["local_eligible"])
            or int(funnel["pool_accepted"])
            > int(funnel["precommit_accepted"])
            or int(funnel["rewarded"]) > int(funnel["selected"])
            or int(funnel["selected"]) > int(funnel["pool_accepted"])
        ):
            raise ValueError("comparison_funnel_order")

        quota = quota_map(
            raw_scope.get("quota"),
            error_name="comparison_quota",
        )

        raw_per_gpu = raw_scope.get("per_gpu")
        if not isinstance(raw_per_gpu, dict):
            raise ValueError("comparison_per_gpu")
        per_gpu: dict[str, dict[str, Any]] = {}
        for raw_gpu, raw_row in raw_per_gpu.items():
            gpu = _canonical_physical_gpu_id(raw_gpu)
            if (
                gpu != raw_gpu
                or gpu in per_gpu
                or not isinstance(raw_row, dict)
            ):
                raise ValueError("comparison_gpu_identity")
            row = {
                name: _standalone_counter(
                    raw_row, name, required=True
                )
                for name in gpu_counter_names
            }
            row["physical_gpu_hours"] = _standalone_nonnegative_float(
                raw_row, "physical_gpu_hours", required=True
            )
            row["selected_slots_per_physical_gpu_hour"] = nullable_float(
                raw_row,
                "selected_slots_per_physical_gpu_hour",
            )
            row["rewarded_slots_per_physical_gpu_hour"] = nullable_float(
                raw_row,
                "rewarded_slots_per_physical_gpu_hour",
            )
            raw_gpu_timings = raw_row.get("timings_ms")
            row["timings_ms"] = (
                {}
                if raw_gpu_timings is None
                else timing_map(raw_gpu_timings)
            )
            if "quota" in raw_row:
                row["quota"] = quota_map(
                    raw_row["quota"],
                    error_name="comparison_gpu_quota",
                )
            if (
                int(row["rewarded"]) > int(row["selected"])
                or int(row["selected"]) > int(row["pool_accepted"])
            ):
                raise ValueError("comparison_gpu_order")
            per_gpu[gpu] = row

        timings = timing_map(raw_scope.get("timings_ms"))

        total_gpu_hours = _standalone_nonnegative_float(
            raw_scope, "physical_gpu_hours", required=True
        )
        observed_gpu_hours = sum(
            float(row["physical_gpu_hours"]) for row in per_gpu.values()
        )
        if not math.isclose(
            float(total_gpu_hours),
            observed_gpu_hours,
            rel_tol=1e-9,
            abs_tol=1e-9,
        ):
            raise ValueError("comparison_gpu_hours")
        result[scope_name] = {
            "scope_kind": str(raw_scope.get("scope_kind") or ""),
            "identity_scope": str(raw_scope.get("identity_scope") or ""),
            "gpu_identity_filter": (
                dict(raw_scope["gpu_identity_filter"])
                if isinstance(raw_scope.get("gpu_identity_filter"), dict)
                else {}
            ),
            "release_id": (
                str(raw_scope["release_id"])
                if raw_scope.get("release_id") is not None
                else None
            ),
            "window_start": window_start,
            "window_end": window_end,
            "window_count": window_count,
            "funnel": funnel,
            "timings_ms": timings,
            "quota": quota,
            "physical_gpu_hours": total_gpu_hours,
            "selected_slots_per_physical_gpu_hour": nullable_float(
                raw_scope,
                "selected_slots_per_physical_gpu_hour",
            ),
            "rewarded_slots_per_physical_gpu_hour": nullable_float(
                raw_scope,
                "rewarded_slots_per_physical_gpu_hour",
            ),
            "per_gpu": per_gpu,
        }
    return result


def _standalone_window_lifecycle(raw: object) -> dict[str, Any]:
    """Normalize optional Math controller liveness without inventing activity."""

    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError("window_lifecycle")
    status = str(raw.get("status") or "").upper()
    if status not in {"READY", "ACTIVE", "RECOVERING", "STALLED"}:
        raise ValueError("window_lifecycle_status")

    def optional_counter(name: str) -> int | None:
        value = raw.get(name)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"window_lifecycle_{name}")
        return value

    def optional_float(name: str) -> float | None:
        value = raw.get(name)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"window_lifecycle_{name}")
        parsed = float(value)
        if not math.isfinite(parsed) or parsed < 0:
            raise ValueError(f"window_lifecycle_{name}")
        return parsed

    latest_run_raw = raw.get("latest_run")
    latest_run: dict[str, Any] | None = None
    if latest_run_raw is not None:
        if not isinstance(latest_run_raw, dict):
            raise ValueError("window_lifecycle_latest_run")
        run_status = str(latest_run_raw.get("status") or "").lower()
        if run_status not in {"started", "completed", "failed"}:
            raise ValueError("window_lifecycle_run_status")
        window_n = latest_run_raw.get("window_n")
        if (
            isinstance(window_n, bool)
            or not isinstance(window_n, int)
            or window_n < 0
        ):
            raise ValueError("window_lifecycle_run_window")
        latest_run = {
            "window_n": window_n,
            "status": run_status,
            "started_at": latest_run_raw.get("started_at"),
            "updated_at": latest_run_raw.get("updated_at"),
            "completed_at": latest_run_raw.get("completed_at"),
            "failure_stage": str(latest_run_raw.get("failure_stage") or ""),
            "failure_type": str(latest_run_raw.get("failure_type") or ""),
            "failure_message": str(
                latest_run_raw.get("failure_message") or ""
            )[:1_000],
        }
        for name in (
            "started_at",
            "updated_at",
            "completed_at",
        ):
            value = latest_run[name]
            if value is None:
                continue
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) < 0
            ):
                raise ValueError(f"window_lifecycle_run_{name}")
            latest_run[name] = float(value)
        for name in (
            "generated_groups",
            "locally_eligible",
            "precommitted",
            "http_provisional",
        ):
            value = latest_run_raw.get(name, 0)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"window_lifecycle_run_{name}")
            latest_run[name] = value

    validator_state = raw.get("validator_state")
    if validator_state is not None and not isinstance(validator_state, str):
        raise ValueError("window_lifecycle_validator_state")
    return {
        "status": status,
        "heartbeat_at": optional_float("heartbeat_at"),
        "heartbeat_age_seconds": optional_float("heartbeat_age_seconds"),
        "validator_window_n": optional_counter("validator_window_n"),
        "validator_state": validator_state,
        "latest_run": latest_run,
        "latest_generated_window": optional_counter(
            "latest_generated_window"
        ),
        "latest_attempt_window": optional_counter("latest_attempt_window"),
    }


def _standalone_active_generation(
    lifecycle: dict[str, Any],
) -> int | None:
    """Return the current window while an attested atomic generation is active.

    Standalone generators publish completed groups atomically, so the economic
    funnel legitimately remains at zero while a worker is generating.  The
    controller lifecycle is the independent liveness signal for that interval.
    Require one current ``ACTIVE``/``started`` run with no completed group or
    submission progress; older or terminal runs must not make an idle lane look
    busy.
    """

    if str(lifecycle.get("status") or "").upper() != "ACTIVE":
        return None
    latest_run = lifecycle.get("latest_run")
    if not isinstance(latest_run, dict):
        return None
    if str(latest_run.get("status") or "").lower() != "started":
        return None
    run_window = latest_run.get("window_n")
    validator_window = lifecycle.get("validator_window_n")
    if (
        isinstance(run_window, bool)
        or not isinstance(run_window, int)
        or run_window <= 0
        or run_window != validator_window
    ):
        return None
    if any(
        int(latest_run.get(name, 0) or 0) > 0
        for name in (
            "generated_groups",
            "locally_eligible",
            "precommitted",
            "http_provisional",
        )
    ):
        return None
    return run_window


def _standalone_profile_identity(raw: object) -> dict[str, Any] | None:
    """Normalize only the non-secret identity fields shared by both supervisors."""

    if not isinstance(raw, dict):
        return None
    protocol = raw.get("protocol_version")
    if protocol is not None and (
        isinstance(protocol, bool) or not isinstance(protocol, int) or protocol < 2
    ):
        raise ValueError("supervisor_protocol_version")
    profile = raw.get("generation_profile_id")
    if profile is not None and (not isinstance(profile, str) or not profile):
        raise ValueError("supervisor_profile_id")

    def digest(name: str, size: int) -> str | None:
        value = raw.get(name)
        if value is None:
            return None
        value = str(value).lower()
        if not re.fullmatch(rf"[0-9a-f]{{{size}}}", value):
            raise ValueError(f"supervisor_{name}")
        return value

    checkpoint_repo = raw.get("checkpoint_repo_id")
    if checkpoint_repo is not None and (
        not isinstance(checkpoint_repo, str) or not checkpoint_repo
    ):
        raise ValueError("supervisor_checkpoint_repo_id")
    validator_image = (
        raw.get("validator_image_revision")
        or raw.get("observed_validator_image_revision")
    )
    if validator_image is not None:
        validator_image = str(validator_image).lower()
        if not re.fullmatch(r"[0-9a-f]{40}", validator_image):
            raise ValueError("supervisor_validator_image_revision")
    result = {
        "protocol_version": protocol,
        "generation_profile_id": profile,
        "generation_contract_sha256": digest(
            "generation_contract_sha256", 64
        ),
        "checkpoint_repo_id": checkpoint_repo,
        "checkpoint_revision": digest("checkpoint_revision", 40),
        "checkpoint_profile_sha256": digest(
            "checkpoint_profile_sha256", 64
        ),
        "validator_image_revision": validator_image,
        "window_n": raw.get("window_n")
        if isinstance(raw.get("window_n"), int)
        and not isinstance(raw.get("window_n"), bool)
        else None,
    }
    if all(
        result.get(name) is None
        for name in (
            "protocol_version",
            "generation_profile_id",
            "generation_contract_sha256",
            "checkpoint_repo_id",
            "checkpoint_revision",
            "checkpoint_profile_sha256",
            "validator_image_revision",
        )
    ):
        return None
    return result


def _standalone_supervisor_status(
    raw: object,
    *,
    configured: bool,
    read_error: object,
    current: float,
    stale_seconds: float,
) -> dict[str, Any]:
    """Normalize Code schema 2 and Math schema 1 without granting authority."""

    if raw is None:
        return {
            "configured": configured,
            "available": False,
            "fresh": False,
            "submission_enabled": False,
            "error": str(read_error or "missing")[:120] if configured else "",
        }
    if not isinstance(raw, dict) or raw.get("schema_version") not in {1, 2}:
        raise ValueError("supervisor_schema")
    updated_raw = raw.get("updated_at", raw.get("updated_unix"))
    if (
        isinstance(updated_raw, bool)
        or not isinstance(updated_raw, (int, float))
        or not math.isfinite(float(updated_raw))
        or float(updated_raw) <= 0
    ):
        raise ValueError("supervisor_updated_at")
    updated_at = float(updated_raw)
    if updated_at > current + 30.0:
        raise ValueError("supervisor_updated_at_future")
    age_s = max(0.0, current - updated_at)
    phase = str(raw.get("phase") or raw.get("state") or "UNKNOWN")
    submission_enabled = raw.get("submission_enabled") is True
    advertised_raw = (
        raw.get("advertised")
        or raw.get("advertised_target")
        or raw.get("target")
    )
    advertised = _standalone_profile_identity(advertised_raw)
    active_raw = raw.get("active")
    if not isinstance(active_raw, dict):
        active_raw = {
            "protocol_version": raw.get(
                "protocol_version", raw.get("active_protocol")
            ),
            "generation_profile_id": raw.get(
                "generation_profile_id", raw.get("active_profile")
            ),
            "generation_contract_sha256": raw.get(
                "generation_contract_sha256"
            ),
            "checkpoint_repo_id": raw.get("active_checkpoint_repo_id")
            or raw.get("checkpoint_repo_id"),
            "checkpoint_revision": raw.get("active_checkpoint")
            or raw.get("checkpoint_revision"),
            "checkpoint_profile_sha256": raw.get(
                "checkpoint_profile_sha256"
            ),
            "observed_validator_image_revision": raw.get(
                "observed_validator_image_revision"
            ),
        }
    active = _standalone_profile_identity(active_raw)
    active_bundle = raw.get("active_bundle")
    if active_bundle is None:
        active_bundle = (
            active_raw.get("bundle_id")
            if isinstance(active_raw, dict)
            else None
        )
    compared_fields = (
        "protocol_version",
        "generation_profile_id",
        "generation_contract_sha256",
        "checkpoint_repo_id",
        "checkpoint_revision",
        "checkpoint_profile_sha256",
    )
    mismatches: dict[str, dict[str, Any]] = {}
    identity_match: bool | None = None
    if active is not None and advertised is not None:
        mismatches = {
            name: {
                "active": active.get(name),
                "advertised": advertised.get(name),
            }
            for name in compared_fields
            if active.get(name) != advertised.get(name)
        }
        identity_match = not mismatches
    blockers = [
        str(value)
        for value in (
            raw.get("canary_blocker"),
            raw.get("promotion_blocker"),
            raw.get("mine_blocker"),
            raw.get("blocker"),
            raw.get("reason") if raw.get("state") == "PROFILE_BLOCKED" else None,
        )
        if isinstance(value, str) and value
    ]
    return {
        "configured": configured,
        "available": True,
        "state": str(raw.get("state") or phase),
        "phase": phase,
        "updated_at": updated_at,
        "age_s": age_s,
        "fresh": age_s <= stale_seconds,
        "submission_enabled": submission_enabled,
        "active_bundle": str(active_bundle) if active_bundle is not None else None,
        "active": active,
        "advertised": advertised,
        "identity_match": identity_match,
        "identity_mismatches": mismatches,
        "unsafe_submission_state": bool(
            submission_enabled and identity_match is not True
        ),
        "blockers": list(dict.fromkeys(blockers)),
        "error": "",
    }


def _standalone_registry_controller_authority(
    state: BoxState,
    wrapper: dict[str, Any],
    *,
    controller: dict[str, Any],
    miner_source_revision: str,
    validator_source_revision: str,
    observed_validator_image_revision: str,
    checkpoint_n: int,
    checkpoint_repo_id: str,
    checkpoint_revision: str,
    protocol_version: int | None,
    generation_profile_id: str,
    generation_contract_sha256: str,
    checkpoint_profile_sha256: str,
) -> dict[str, object]:
    """Bind a live controller to the digest-verified active registry entry.

    The optional supervisor file describes transition intent and can lag a
    completed cutover.  A configured active-unit registry instead names the
    process that currently owns submission authority and pins its unit,
    controller configuration, telemetry, and runtime manifest.  Only return
    authority after all of those independently probed values agree.  Any
    mismatch is an error so callers fail closed instead of falling back to a
    previous static deployment tuple.
    """

    if not state.active_unit_registry_path:
        return {}
    registry = state.active_unit_registry
    if (
        not isinstance(registry, dict)
        or registry.get("error")
        or registry.get("schema_version") != 1
        or not re.fullmatch(
            r"[0-9a-f]{64}", str(registry.get("registry_sha256") or "")
        )
    ):
        raise ValueError("active_unit_registry_invalid")
    entry = registry.get("active")
    if not isinstance(entry, dict):
        raise ValueError("active_unit_registry_entry")
    required = {
        "unit",
        "mode",
        "controller_config_path",
        "telemetry_path",
        "runtime_manifest_path",
        "checkpoint_revision",
        "source_revision",
        "unit_fragment_sha256",
        "controller_config_sha256",
        "runtime_manifest_sha256",
    }
    if set(entry) != required:
        raise ValueError("active_unit_registry_entry")
    mode = str(entry.get("mode") or "").lower()
    if mode not in {"canary", "mine"}:
        raise ValueError("active_unit_registry_not_controller")
    manifest_sha256 = str(wrapper.get("manifest_sha256") or "").lower()
    expected_manifest_sha256 = str(
        entry.get("runtime_manifest_sha256") or ""
    ).lower()
    checks = (
        _canonical_systemd_unit(str(entry.get("unit") or ""))
        == _canonical_systemd_unit(state.active_unit),
        str(entry.get("controller_config_path") or "")
        == str(controller.get("config_path") or ""),
        str(controller.get("mode") or "") == mode,
        str(entry.get("telemetry_path") or "")
        == state.standalone_telemetry_path,
        str(entry.get("runtime_manifest_path") or "")
        == str(controller.get("runtime_manifest_path") or ""),
        str(entry.get("runtime_manifest_path") or "")
        == state.standalone_runtime_manifest_path,
        str(entry.get("checkpoint_revision") or "").lower()
        == checkpoint_revision,
        str(entry.get("source_revision") or "").lower()
        == miner_source_revision,
        bool(re.fullmatch(r"[0-9a-f]{40}", validator_source_revision)),
        bool(
            re.fullmatch(
                r"[0-9a-f]{40}", observed_validator_image_revision
            )
        ),
        isinstance(protocol_version, int),
        bool(generation_profile_id),
        bool(re.fullmatch(r"[0-9a-f]{64}", generation_contract_sha256)),
        bool(re.fullmatch(r"[0-9a-f]{64}", checkpoint_profile_sha256)),
        bool(manifest_sha256),
        manifest_sha256 == expected_manifest_sha256,
    )
    if not all(checks):
        raise ValueError("active_unit_registry_binding")
    return {
        "source": "active_unit_registry",
        "mode": mode,
        "unit": str(entry["unit"]),
        "protocol_version": protocol_version,
        "generation_profile_id": generation_profile_id,
        "generation_contract_sha256": generation_contract_sha256,
        "checkpoint_n": checkpoint_n,
        "checkpoint_repo_id": checkpoint_repo_id,
        "checkpoint_revision": checkpoint_revision,
        "source_revision": miner_source_revision,
        "validator_source_revision": validator_source_revision,
        "observed_validator_image_revision": (
            observed_validator_image_revision
        ),
        "checkpoint_profile_sha256": checkpoint_profile_sha256,
        "registry_sha256": str(registry["registry_sha256"]),
        "runtime_manifest_sha256": manifest_sha256,
    }


def _standalone_sanitize_observability(
    raw: object,
    controller_authority: dict[str, object],
) -> dict[str, object]:
    """Fence transition telemetry superseded by live controller authority.

    A producer can atomically publish a new, digest-bound controller registry
    before its optional transition-observability snapshot is refreshed.  Do
    not expose the older advertised tuple or supervisor assertions as current
    fleet truth when they contradict that stronger authority.
    """

    observability: dict[str, object] = dict(raw) if isinstance(raw, dict) else {}
    if not controller_authority:
        return observability

    supervisor = observability.get("supervisor")
    advertised_candidates: list[dict[str, object]] = []
    advertised = observability.get("validator_advertised")
    if isinstance(advertised, dict):
        advertised_candidates.append(advertised)
    if isinstance(supervisor, dict):
        for key in ("validator_advertised", "advertised"):
            nested = supervisor.get(key)
            if isinstance(nested, dict):
                advertised_candidates.append(nested)

    authority_fields = {
        "protocol_version": "protocol_version",
        "generation_profile_id": "generation_profile_id",
        "generation_contract_sha256": "generation_contract_sha256",
        "checkpoint_repo_id": "checkpoint_repo_id",
        "checkpoint_revision": "checkpoint_revision",
        "validator_image_revision": "observed_validator_image_revision",
    }
    mismatch_fields: set[str] = set()
    for candidate in advertised_candidates:
        for advertised_field, authority_field in authority_fields.items():
            candidate_value = candidate.get(advertised_field)
            if candidate_value in (None, ""):
                continue
            authority_value = controller_authority.get(authority_field)
            if advertised_field == "protocol_version":
                mismatch = candidate_value != authority_value
            else:
                mismatch = str(candidate_value).lower() != str(
                    authority_value or ""
                ).lower()
            if mismatch:
                mismatch_fields.add(advertised_field)

    if not mismatch_fields:
        return observability

    # These values are all produced from the same optional transition
    # snapshot.  Remove them together so no stale checkpoint, window, or
    # submission assertion survives beside the newer signed registry tuple.
    for key in (
        "validator_advertised",
        "active_bundle",
        "identity_match",
        "identity_mismatches",
        "unsafe_submission_state",
    ):
        observability.pop(key, None)
    observability["supervisor"] = {
        "status": "FENCED_STALE",
        "reason": "superseded_by_active_unit_registry",
        "authority_source": "active_unit_registry",
        "mismatch_fields": sorted(mismatch_fields),
    }
    return observability


def _apply_standalone_telemetry_probe(
    state: BoxState,
    lines: list[str],
    *,
    now: float | None = None,
) -> None:
    """Apply one complete standalone telemetry+identity read fail-closed."""
    state.standalone_telemetry = {}
    state.standalone_generated_at = 0.0
    state.standalone_age_s = -1.0
    state.standalone_fresh = False
    state.standalone_error = ""
    state.runtime_components = []
    state.standalone_controller_mode = ""
    state.standalone_controller_config_path = ""
    state.standalone_active_runtime_manifest_path = ""
    state.standalone_active_ledger_path = ""
    if not state.standalone_telemetry_path:
        return
    try:
        if len(lines) != 1:
            raise ValueError("probe_output")
        wrapper = json.loads(lines[0])
        if not isinstance(wrapper, dict) or wrapper.get("schema_version") != 1:
            raise ValueError("probe_schema")
        if wrapper.get("error"):
            raise ValueError(str(wrapper["error"]))
        telemetry = wrapper.get("telemetry")
        manifest = wrapper.get("manifest")
        if not isinstance(telemetry, dict) or not isinstance(manifest, dict):
            raise ValueError("payload_shape")
        generated_at_raw = telemetry.get("generated_at")
        if (
            isinstance(generated_at_raw, bool)
            or not isinstance(generated_at_raw, (int, float))
        ):
            raise ValueError("generated_at")
        generated_at = float(generated_at_raw)
        current = time.time() if now is None else float(now)
        if not math.isfinite(generated_at) or generated_at <= 0:
            raise ValueError("generated_at")
        if generated_at > current + 30.0:
            raise ValueError("generated_at_future")
        age_s = max(0.0, current - generated_at)
        supervisor = _standalone_supervisor_status(
            wrapper.get("supervisor"),
            configured=bool(state.standalone_supervisor_status_path),
            read_error=wrapper.get("supervisor_error"),
            current=current,
            stale_seconds=state.standalone_telemetry_stale_seconds,
        )

        funnel = telemetry.get("funnel")
        if not isinstance(funnel, dict):
            raise ValueError("funnel")
        attempts = _standalone_counter(funnel, "attempts", required=True)
        generation_complete = _standalone_counter(
            funnel, "generation_complete", required=True
        )
        natural_complete = _standalone_counter(
            funnel, "natural_complete_m8"
        )
        local_eligible = _standalone_counter(
            funnel, "local_eligible", "locally_eligible", required=True
        )
        precommit = _standalone_counter(
            funnel, "precommit_accepted", "receipt_accepted"
        )
        reveal = _standalone_counter(funnel, "reveal_accepted")
        pool = _standalone_counter(
            funnel, "pool_accepted", required=True
        )
        selected = _standalone_counter(
            funnel, "selected", required=True
        )
        rewarded = _standalone_counter(
            funnel, "rewarded", required=True
        )
        terminal = _standalone_counter(funnel, "terminal_final")
        unresolved = _standalone_counter(
            funnel, "terminal_unresolved"
        )
        network_proof = _standalone_counter(
            funnel, "network_proof_passed"
        )
        for name, value in (
            ("generation_complete", generation_complete),
            ("natural_complete_m8", natural_complete),
            ("local_eligible", local_eligible),
            ("precommit_accepted", precommit),
            ("reveal_accepted", reveal),
            ("pool_accepted", pool),
            ("selected", selected),
            ("rewarded", rewarded),
            ("terminal_final", terminal),
            ("terminal_unresolved", unresolved),
            ("network_proof_passed", network_proof),
        ):
            if value is not None and value > attempts:
                raise ValueError(f"counter_exceeds_attempts:{name}")
        if selected > pool or rewarded > selected:
            raise ValueError("funnel_order")

        published_physical_gpu_hours = _standalone_nonnegative_float(
            telemetry, "physical_gpu_hours", required=True
        )
        selected_rate_required = not (
            published_physical_gpu_hours == 0.0 and selected == 0
        )
        published_selected_per_gpu_hour = _standalone_nonnegative_float(
            telemetry,
            "selected_slots_per_physical_gpu_hour",
            "slots_per_physical_gpu_hour",
            required=selected_rate_required,
        )
        if published_selected_per_gpu_hour is None:
            published_selected_per_gpu_hour = 0.0
        published_rewarded_per_gpu_hour = _standalone_nonnegative_float(
            telemetry,
            "rewarded_slots_per_physical_gpu_hour",
        )
        if published_rewarded_per_gpu_hour is None:
            published_rewarded_per_gpu_hour = (
                published_selected_per_gpu_hour
                if rewarded == selected
                else 0.0
            )
        per_gpu, normalized_gpu_hours = _standalone_ledger_gpu_summary(
            wrapper.get("ledger")
        )
        comparison_scopes = _standalone_comparison_scopes(
            telemetry.get("comparison_scopes")
        )
        if sum(int(row["attempts"]) for row in per_gpu.values()) > attempts:
            raise ValueError("ledger_attempts")
        if (
            natural_complete is not None
            and sum(
                int(row["natural_complete_m8"])
                for row in per_gpu.values()
            )
            > natural_complete
        ):
            raise ValueError("ledger_natural_complete")
        physical_gpu_hours = (
            normalized_gpu_hours
            if normalized_gpu_hours is not None
            and normalized_gpu_hours > 0
            else published_physical_gpu_hours
        )
        selected_per_gpu_hour = (
            selected / physical_gpu_hours if physical_gpu_hours > 0 else 0.0
        )
        rewarded_per_gpu_hour = (
            rewarded / physical_gpu_hours if physical_gpu_hours > 0 else 0.0
        )

        window_lifecycle = _standalone_window_lifecycle(
            telemetry.get("window_lifecycle")
        )
        latest_window_raw = telemetry.get(
            "latest_window",
            telemetry.get("latest_attempt_window"),
        )
        if latest_window_raw is None:
            zero_history = (
                attempts == 0
                and physical_gpu_hours == 0.0
                and all(
                    value in {None, 0}
                    for value in (
                        generation_complete,
                        natural_complete,
                        local_eligible,
                        precommit,
                        reveal,
                        pool,
                        network_proof,
                        selected,
                        rewarded,
                        terminal,
                        unresolved,
                    )
                )
            )
            if not window_lifecycle and not zero_history:
                raise ValueError("missing_counter:latest_window")
            latest_window = None
        else:
            latest_window = _standalone_counter(
                {"latest_window": latest_window_raw},
                "latest_window",
                required=True,
            )
        hotkeys = telemetry.get("hotkeys", [])
        if hotkeys is not None and not isinstance(hotkeys, list):
            raise ValueError("hotkeys")
        if hotkeys and (
            not all(isinstance(value, str) for value in hotkeys)
            or state.hotkey not in hotkeys
        ):
            raise ValueError("hotkey_mismatch")

        miner_source = str(manifest.get("miner_source_revision") or "").lower()
        public_source = str(
            manifest.get("validator_source_revision") or ""
        ).lower()
        observed_validator_image = str(
            manifest.get("observed_validator_image_revision") or ""
        ).lower()
        checkpoint_revision = str(
            manifest.get("checkpoint_revision") or ""
        ).lower()
        protocol_version_raw = manifest.get("protocol_version")
        if protocol_version_raw is not None and (
            isinstance(protocol_version_raw, bool)
            or not isinstance(protocol_version_raw, int)
            or protocol_version_raw < 2
        ):
            raise ValueError("runtime_protocol_version")
        generation_profile_id = str(
            manifest.get("generation_profile_id") or ""
        )
        generation_contract_sha256 = str(
            manifest.get("generation_contract_sha256") or ""
        ).lower()
        checkpoint_profile_sha256 = str(
            manifest.get("checkpoint_profile_sha256") or ""
        ).lower()
        runtime_fingerprint_sha256 = str(
            manifest.get("runtime_fingerprint_sha256") or ""
        ).lower()
        for name, value in (
            ("generation_contract_sha256", generation_contract_sha256),
            ("checkpoint_profile_sha256", checkpoint_profile_sha256),
            ("runtime_fingerprint_sha256", runtime_fingerprint_sha256),
        ):
            if value and not re.fullmatch(r"[0-9a-f]{64}", value):
                raise ValueError(f"runtime_{name}")
        model_repo = str(manifest.get("model_repo") or "").strip()
        environment = str(manifest.get("environment") or "").strip()
        checkpoint_n_raw = manifest.get("checkpoint_n")
        raw_components = manifest.get("components", [])
        if not isinstance(raw_components, list):
            raise ValueError("runtime_components")
        runtime_components: list[dict[str, Any]] = []
        for component in raw_components:
            if not isinstance(component, dict):
                raise ValueError("runtime_component")
            role = str(component.get("role") or "")
            component_id = str(component.get("component_id") or "")
            manifest_path = str(component.get("manifest_path") or "")
            manifest_file_sha256 = str(
                component.get("manifest_file_sha256") or ""
            ).lower()
            runtime_payload_sha256 = str(
                component.get("runtime_payload_sha256") or ""
            ).lower()
            runtime_profile_sha256 = str(
                component.get("runtime_profile_sha256") or ""
            ).lower()
            if (
                role not in {"generation", "proof", "grader"}
                or not component_id
                or (
                    manifest_path
                    and (
                        not manifest_path.startswith("/")
                        or "\x00" in manifest_path
                    )
                )
                or not re.fullmatch(r"[0-9a-f]{64}", manifest_file_sha256)
                or not re.fullmatch(r"[0-9a-f]{64}", runtime_payload_sha256)
                or not re.fullmatch(r"[0-9a-f]{64}", runtime_profile_sha256)
                or not isinstance(
                    component.get("health_gpu_uuid_required"), bool
                )
            ):
                raise ValueError("runtime_component")
            normalized_component = {
                "role": role,
                "component_id": component_id,
                "manifest_file_sha256": manifest_file_sha256,
                "runtime_payload_sha256": runtime_payload_sha256,
                "runtime_profile_sha256": runtime_profile_sha256,
                "health_gpu_uuid_required": component[
                    "health_gpu_uuid_required"
                ],
            }
            if manifest_path:
                normalized_component["manifest_path"] = manifest_path
            runtime_components.append(normalized_component)
        controller = wrapper.get("controller", {})
        if not isinstance(controller, dict):
            raise ValueError("controller")
        controller_mode = str(controller.get("mode") or "")
        controller_config_path = str(controller.get("config_path") or "")
        active_manifest_path = str(
            controller.get("runtime_manifest_path") or ""
        )
        active_ledger_path = str(controller.get("ledger_path") or "")
        controller_unit = str(controller.get("active_unit") or "")
        expected_config_path = ""
        if state.active_unit:
            expected_config_path = _configured_controller_config_path(
                state, state.active_unit
            )
        if expected_config_path:
            if (
                controller_mode not in {"canary", "mine"}
                or controller_config_path != expected_config_path
                or controller_unit != state.active_unit
                or not active_manifest_path.startswith("/")
                or not active_ledger_path.startswith("/")
            ):
                raise ValueError("active_controller_binding")
        elif any((
            controller_mode,
            controller_config_path,
            active_ledger_path,
        )):
            raise ValueError("unexpected_controller_binding")
        if (
            not re.fullmatch(r"[0-9a-f]{40}", miner_source)
            or not re.fullmatch(r"[0-9a-f]{40}", public_source)
            or (
                observed_validator_image
                and not re.fullmatch(
                    r"[0-9a-f]{40}", observed_validator_image
                )
            )
            or not re.fullmatch(r"[0-9a-f]{40}", checkpoint_revision)
            or isinstance(checkpoint_n_raw, bool)
            or not isinstance(checkpoint_n_raw, int)
            or checkpoint_n_raw <= 0
            or not model_repo
            or not environment
        ):
            raise ValueError("runtime_identity")

        controller_authority = _standalone_registry_controller_authority(
            state,
            wrapper,
            controller=controller,
            miner_source_revision=miner_source,
            validator_source_revision=public_source,
            observed_validator_image_revision=observed_validator_image,
            checkpoint_n=checkpoint_n_raw,
            checkpoint_repo_id=model_repo,
            checkpoint_revision=checkpoint_revision,
            protocol_version=protocol_version_raw,
            generation_profile_id=generation_profile_id,
            generation_contract_sha256=generation_contract_sha256,
            checkpoint_profile_sha256=checkpoint_profile_sha256,
        )

        historical_funnel = {
            "attempts": attempts,
            "generation_complete": generation_complete,
            "natural_complete_m8": natural_complete,
            "local_eligible": local_eligible,
            "precommit_accepted": precommit,
            "reveal_accepted": reveal,
            "pool_accepted": pool,
            "network_proof_passed": network_proof,
            "selected": selected,
            "rewarded": rewarded,
            "terminal_final": terminal,
            "terminal_unresolved": unresolved,
        }
        current_scope = comparison_scopes.get("active_profile")
        if state.proc_alive and isinstance(current_scope, dict):
            current_funnel = dict(current_scope["funnel"])
            current_physical_gpu_hours = float(
                current_scope["physical_gpu_hours"]
            )
            current_selected_per_gpu_hour = float(
                current_scope.get("selected_slots_per_physical_gpu_hour") or 0.0
            )
            current_rewarded_per_gpu_hour = float(
                current_scope.get("rewarded_slots_per_physical_gpu_hour") or 0.0
            )
            current_latest_window = current_scope.get("window_end")
            metrics_posture = "active_profile"
        elif not state.proc_alive:
            current_funnel = {
                name: 0
                for name in (
                    "attempts",
                    "generation_complete",
                    "natural_complete_m8",
                    "local_eligible",
                    "precommit_accepted",
                    "reveal_accepted",
                    "pool_accepted",
                    "network_proof_passed",
                    "selected",
                    "rewarded",
                    "terminal_final",
                    "terminal_unresolved",
                )
            }
            current_physical_gpu_hours = 0.0
            current_selected_per_gpu_hour = 0.0
            current_rewarded_per_gpu_hour = 0.0
            current_latest_window = None
            metrics_posture = "historical_fenced"
        else:
            # Backward compatibility for a live v2 producer that predates the
            # additive active_profile scope.
            current_funnel = historical_funnel
            current_physical_gpu_hours = physical_gpu_hours
            current_selected_per_gpu_hour = selected_per_gpu_hour
            current_rewarded_per_gpu_hour = rewarded_per_gpu_hour
            current_latest_window = latest_window
            metrics_posture = "legacy_live"

        raw_staleness = telemetry.get("staleness_seconds")
        staleness_seconds = (
            {
                str(name): float(value)
                for name, value in raw_staleness.items()
                if isinstance(value, (int, float))
                and not isinstance(value, bool)
                and math.isfinite(float(value))
                and float(value) >= 0
            }
            if isinstance(raw_staleness, dict)
            else {}
        )
        heartbeat_age = window_lifecycle.get("heartbeat_age_seconds")
        ledger_age = staleness_seconds.get("ledger_update")
        # The atomic snapshot timestamp is the telemetry liveness authority.
        # Producer-specific ages describe independent data products: an idle
        # selector memory or an R2 reconciliation cursor may legitimately be
        # old while the controller is actively generating.  Taking the maximum
        # of all of them therefore mislabeled a healthy live miner as stale.
        # Prefer an explicit controller heartbeat for the displayed progress
        # age, then ledger activity, while preserving every raw age below for
        # diagnosis.  Neither auxiliary age can invalidate a fresh snapshot.
        if isinstance(heartbeat_age, (int, float)) and not isinstance(
            heartbeat_age, bool
        ):
            progress_age_s = max(0.0, float(heartbeat_age))
        elif isinstance(ledger_age, (int, float)) and not isinstance(
            ledger_age, bool
        ):
            progress_age_s = max(0.0, float(ledger_age))
        else:
            progress_age_s = age_s
        observability = _standalone_sanitize_observability(
            telemetry.get("observability"), controller_authority
        )
        runtime_observability = telemetry.get("runtime")
        runtime_observability = (
            runtime_observability
            if isinstance(runtime_observability, dict)
            else {}
        )
        state.standalone_telemetry = {
            "schema_version": int(telemetry.get("schema_version") or 1),
            "latest_window": current_latest_window,
            "historical_latest_window": latest_window,
            "funnel": current_funnel,
            "historical_funnel": historical_funnel,
            "metrics_posture": metrics_posture,
            "physical_gpu_hours": current_physical_gpu_hours,
            "published_physical_gpu_hours": published_physical_gpu_hours,
            "selected_slots_per_physical_gpu_hour": current_selected_per_gpu_hour,
            "rewarded_slots_per_physical_gpu_hour": current_rewarded_per_gpu_hour,
            "published_selected_slots_per_physical_gpu_hour": (
                published_selected_per_gpu_hour
            ),
            "published_rewarded_slots_per_physical_gpu_hour": (
                published_rewarded_per_gpu_hour
            ),
            "per_gpu": per_gpu,
            "comparison_scopes": comparison_scopes,
            "window_lifecycle": window_lifecycle,
            "staleness_seconds": staleness_seconds,
            "observability": observability,
            "runtime": runtime_observability,
            "controller_authority": controller_authority,
            "manifest_profile": {
                "protocol_version": protocol_version_raw,
                "generation_profile_id": generation_profile_id or None,
                "generation_contract_sha256": (
                    generation_contract_sha256 or None
                ),
                "checkpoint_profile_sha256": (
                    checkpoint_profile_sha256 or None
                ),
                "runtime_fingerprint_sha256": (
                    runtime_fingerprint_sha256 or None
                ),
                "checkpoint_repo_id": model_repo,
                "checkpoint_revision": checkpoint_revision,
                "validator_source_revision": public_source,
                "observed_validator_image_revision": (
                    observed_validator_image or None
                ),
            },
            "raw_hotkeys": list(hotkeys or []),
        }
        state.standalone_supervisor = supervisor
        state.standalone_progress_age_s = progress_age_s
        state.standalone_generated_at = generated_at
        state.standalone_age_s = age_s
        state.standalone_fresh = (
            age_s <= state.standalone_telemetry_stale_seconds
            and state.proc_alive
        )
        if not state.proc_alive:
            state.standalone_error = "no_mining_unit"
        elif not state.standalone_fresh:
            state.standalone_error = "stale"

        state.miner_source_revision = miner_source
        state.runtime_components = runtime_components
        state.standalone_controller_mode = controller_mode
        state.standalone_controller_config_path = controller_config_path
        state.standalone_active_runtime_manifest_path = active_manifest_path
        state.standalone_active_ledger_path = active_ledger_path
        state.reliquary_source_revision = public_source
        state.observed_validator_image_revision = observed_validator_image
        state.source_manifest_provisioned_ok = True
        state.provisioned_model_kind = "validator_checkpoint"
        state.provisioned_checkpoint_n = checkpoint_n_raw
        state.provisioned_model_repo = model_repo
        state.provisioned_model_revision = checkpoint_revision
        state.runtime_checkpoint_loaded = True
        state.runtime_checkpoint_n = checkpoint_n_raw
        state.runtime_checkpoint_repo = model_repo
        state.runtime_checkpoint_revision = checkpoint_revision
        state.runtime_checkpoint_pid = state.active_pid
        state.runtime_checkpoint_started_at = state.active_started_at
        state.runtime_checkpoint_evidence = "standalone_runtime_manifest"
        state.local_checkpoint_n = checkpoint_n_raw
        state.local_checkpoint_revision = checkpoint_revision
        state.miner_environment = environment
        state.active_environment = environment if state.proc_alive else ""
        state.engine_mode = "standalone"
        supervisor_active = supervisor.get("active")
        supervisor_active = (
            supervisor_active if isinstance(supervisor_active, dict) else {}
        )
        state.protocol_profile = str(
            generation_profile_id
            if controller_authority
            else supervisor_active.get("generation_profile_id")
            or generation_profile_id
            or "historical-v2-unscoped"
        )
        state.runtime_parity_ok = bool(
            state.proc_alive
            and (
                bool(controller_authority)
                or (
                    supervisor.get("identity_match") is not False
                    and not supervisor.get("unsafe_submission_state")
                )
            )
        )
        state.reference_ready = bool(
            state.proc_alive and state.standalone_fresh
        )
        active_generation_window = (
            _standalone_active_generation(window_lifecycle)
            if state.reference_ready
            and state.runtime_parity_ok
            and bool(controller_authority)
            else None
        )
        if active_generation_window is not None:
            state.miner_state = "active_generation"
            state.miner_window = active_generation_window
            state.miner_ready = 0
            state.miner_inflight = 1
        else:
            state.miner_state = (
                "live"
                if state.reference_ready
                else "fenced"
                if not state.proc_alive
                else "telemetry-stale"
            )
            state.miner_window = current_latest_window
            state.miner_ready = int(state.reference_ready)
            state.miner_inflight = 0
        state.miner_submitted_this_win = int(
            state.proc_alive
            and current_funnel.get("terminal_final") is not None
            and int(current_funnel["terminal_final"])
            < int(current_funnel["attempts"])
        )
        state.acceptance_source = (
            "standalone_active_profile"
            if metrics_posture == "active_profile"
            else "standalone_historical"
            if metrics_posture == "historical_fenced"
            else "standalone_atomic"
        )
    except (TypeError, ValueError, OverflowError, json.JSONDecodeError) as exc:
        state.standalone_error = str(exc)[:240]
        state.reference_ready = False
        state.runtime_parity_ok = False
        state.miner_ready = 0
        state.miner_state = "telemetry-error"


def _apply_standalone_certification_probe(
    state: BoxState,
    line: str,
    *,
    now: float | None = None,
) -> bool:
    """Replace exact non-mining certification state from one live probe."""
    state.standalone_certification = {}
    config = state.standalone_certification_config
    if not config:
        return False
    try:
        raw = json.loads(line)
        if not isinstance(raw, dict) or raw.get("schema_version") != 1:
            raise ValueError("probe_schema")
        generated_at = float(raw.get("generated_at") or 0.0)
        current = time.time() if now is None else float(now)
        if (
            not math.isfinite(generated_at)
            or generated_at <= 0
            or generated_at > current + 30.0
            or current - generated_at > 30.0
        ):
            raise ValueError("probe_stale")
        if not raw.get("active"):
            state.standalone_certification = {
                "active": False,
                "fresh": True,
                "generated_at": generated_at,
                "error": str(raw.get("error") or "not_running")[:160],
            }
            return False
        phase = str(raw.get("phase") or "")
        allowed_phases = {
            "reference_proof",
            "candidate_proof",
            "parity_assembly",
            "validator_replay_assembly",
            "parity_and_replay",
            "certificate_build",
            "canary_gate",
            "attach_certificate",
            "transition",
            "generation_loading",
            "generation_ready",
            "generation_capture",
            "generation_recovering",
            "generation_failed",
            "public_oracle_proof",
        }
        runner_pid = int(raw.get("runner_pid") or 0)
        worker_pid = int(raw.get("worker_pid") or 0)
        if (
            raw.get("exact") is not True
            or raw.get("submit_disabled_attested") is not True
            or phase not in allowed_phases
            or runner_pid <= 0
            or worker_pid < 0
            or (
                config.get("service_unit")
                and config.get("service_kind")
                != "submit_disabled_certification"
                and raw.get("wallet_free_attested") is not True
            )
        ):
            raise ValueError("process_identity")
        for key in (
            "runner_path",
            "runner_sha256",
            "artifact_dir",
            "runtime_manifest_path",
            "proof_profile_path",
            "checkpoint_path",
            "service_unit",
            "service_user",
            "service_config_path",
            "service_config_sha256",
            "progress_path",
            "service_kind",
        ):
            if str(raw.get(key) or "") != str(config.get(key) or ""):
                raise ValueError(f"config_mismatch:{key}")
        gpu = raw.get("gpu")
        identity = raw.get("identity")
        artifacts = raw.get("artifacts")
        if (
            not isinstance(gpu, dict)
            or not isinstance(identity, dict)
            or not isinstance(artifacts, dict)
            or str(gpu.get("uuid") or "") != config.get("gpu_uuid")
        ):
            raise ValueError("payload_shape")
        normalized_gpu = {
            "uuid": str(gpu["uuid"]),
            "name": str(gpu.get("name") or ""),
            "utilization_pct": int(gpu.get("utilization_pct") or 0),
            "memory_used_mb": int(gpu.get("memory_used_mb") or 0),
            "memory_total_mb": int(gpu.get("memory_total_mb") or 0),
            "power_w": float(gpu.get("power_w") or 0.0),
            "power_limit_w": float(gpu.get("power_limit_w") or 0.0),
        }
        if (
            not 0 <= normalized_gpu["utilization_pct"] <= 100
            or normalized_gpu["memory_used_mb"] < 0
            or normalized_gpu["memory_total_mb"] <= 0
            or normalized_gpu["memory_used_mb"]
            > normalized_gpu["memory_total_mb"]
        ):
            raise ValueError("gpu")
        started_at_value = float(raw.get("started_at") or 0.0)
        elapsed_s = float(raw.get("elapsed_s") or 0.0)
        if (
            not math.isfinite(started_at_value)
            or started_at_value <= 0
            or started_at_value > current + 30.0
            or not math.isfinite(elapsed_s)
            or elapsed_s < 0
        ):
            raise ValueError("process_time")
        state.standalone_certification = {
            "active": True,
            "fresh": True,
            "generated_at": generated_at,
            "exact": True,
            "submit_disabled_attested": True,
            "wallet_free_attested": bool(raw.get("wallet_free_attested")),
            "runner_pid": runner_pid,
            "worker_pid": worker_pid,
            "phase": phase,
            "started_at": started_at_value,
            "elapsed_s": elapsed_s,
            "runner_path": str(raw["runner_path"]),
            "runner_sha256": str(raw["runner_sha256"]),
            "artifact_dir": str(raw["artifact_dir"]),
            "runtime_manifest_path": str(raw["runtime_manifest_path"]),
            "proof_profile_path": str(raw["proof_profile_path"]),
            "checkpoint_path": str(raw["checkpoint_path"]),
            "service_unit": str(raw.get("service_unit") or ""),
            "service_user": str(raw.get("service_user") or ""),
            "service_config_path": str(
                raw.get("service_config_path") or ""
            ),
            "service_config_sha256": str(
                raw.get("service_config_sha256") or ""
            ),
            "progress_path": str(raw.get("progress_path") or ""),
            "service_kind": str(raw.get("service_kind") or ""),
            "gpu": normalized_gpu,
            "gpu_process_bound": bool(raw.get("gpu_process_bound")),
            "gpu_worker_memory_mb": int(
                raw.get("gpu_worker_memory_mb") or 0
            ),
            "identity": {
                "miner_source_revision": str(
                    identity.get("miner_source_revision") or ""
                ),
                "validator_source_revision": str(
                    identity.get("validator_source_revision") or ""
                ),
                "checkpoint_n": int(identity.get("checkpoint_n") or 0),
                "checkpoint_revision": str(
                    identity.get("checkpoint_revision") or ""
                ),
                "model_repo": str(identity.get("model_repo") or ""),
                "environment": str(identity.get("environment") or ""),
            },
            "artifacts": {
                str(key): bool(value)
                for key, value in artifacts.items()
                if isinstance(key, str)
            },
            "error": "",
        }
        return True
    except (
        TypeError,
        ValueError,
        OverflowError,
        json.JSONDecodeError,
    ) as exc:
        state.standalone_certification = {
            "active": False,
            "fresh": False,
            "generated_at": 0.0,
            "error": str(exc)[:160],
        }
        return False


def _collect_standalone_certification_after_unit_failure(
    state: BoxState,
) -> bool:
    """Probe a configured exact certifier without making it a miner."""
    state.standalone_certification = {}
    if not state.standalone_certification_config:
        return False
    payload = shlex.quote(json.dumps(
        state.standalone_certification_config,
        separators=(",", ":"),
        sort_keys=True,
    ))
    command = (
        f"sudo -n python3 - {payload} <<'PYSTANDALONECERT'\n"
        f"{_STANDALONE_CERTIFICATION_PROBE_SOURCE}\n"
        "PYSTANDALONECERT\n"
    )
    rc, out, err = ssh_run(state.alias, command, timeout_s=12)
    if rc != 0 or not out.strip():
        state.standalone_certification = {
            "active": False,
            "fresh": False,
            "generated_at": 0.0,
            "error": "probe:" + str(err or f"rc={rc}")[:120],
        }
        return False
    return _apply_standalone_certification_probe(
        state, out.strip().splitlines()[-1]
    )


def _collect_standalone_files_after_unit_failure(state: BoxState) -> None:
    """Retain fail-stale telemetry truth even when no allowed unit is live."""
    if not state.standalone_telemetry_path:
        return
    command = " ".join(
        [
            "sudo -n python3 -",
            shlex.quote(state.standalone_telemetry_path),
            shlex.quote(state.standalone_runtime_manifest_path),
            shlex.quote(""),
            shlex.quote(""),
            "0",
            shlex.quote(state.standalone_supervisor_status_path),
        ]
    )
    command += (
        " <<'PYSTANDALONE'\n"
        + _STANDALONE_TELEMETRY_PROBE_SOURCE
        + "\nPYSTANDALONE\n"
    )
    rc, out, err = ssh_run(state.alias, command, timeout_s=12)
    if rc != 0 or not out.strip():
        state.standalone_error = (
            "probe:" + str(err or f"rc={rc}")[:120]
        )
        state.standalone_fresh = False
        return
    _apply_standalone_telemetry_probe(
        state, [out.strip().splitlines()[-1]]
    )


def _mark_reliquary_one_stale(
    state: BoxState,
    *,
    error: str,
    now: float,
) -> None:
    """Retain the last safe projection while failing current readiness stale."""

    previous = dict(state.reliquary_one or {})
    previous["fresh"] = False
    previous["collector_error"] = error
    previous.setdefault("errors", [])
    previous["errors"] = list(
        dict.fromkeys([*previous.get("errors", []), error])
    )
    state.reliquary_one = previous
    state.reliquary_one_error = error
    state.error = error[:120]
    state.last_poll_s = now
    state.reference_ready = False
    state.runtime_parity_ok = False


def _collect_reliquary_one_in_place(state: BoxState) -> None:
    """Collect one complete redacted ``reliquary-one`` snapshot over SSH."""

    from reliquary_one import build_remote_probe_command, normalize_probe

    now = time.time()
    try:
        command = build_remote_probe_command(
            state.reliquary_one_state_root,
            state.unit or "",
        )
    except ValueError as exc:
        _mark_reliquary_one_stale(
            state,
            error=f"reliquary_one_config:{exc}",
            now=now,
        )
        return
    rc, out, err = ssh_run(state.alias, command, timeout_s=20)
    if rc != 0:
        detail = re.sub(r"\s+", " ", str(err or "ssh probe failed")).strip()
        _mark_reliquary_one_stale(
            state,
            error=f"reliquary_one_ssh:{detail[:80]}",
            now=now,
        )
        return
    if len(out.encode("utf-8", errors="replace")) > 2 * 1024 * 1024:
        _mark_reliquary_one_stale(
            state,
            error="reliquary_one_probe_oversized",
            now=now,
        )
        return
    try:
        raw = json.loads(out)
        normalized = normalize_probe(
            raw,
            now=now,
            stale_seconds=state.standalone_telemetry_stale_seconds,
        )
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        _mark_reliquary_one_stale(
            state,
            error=f"reliquary_one_probe:{type(exc).__name__}",
            now=now,
        )
        return

    service = normalized.get("service", {})
    binding = normalized.get("binding", {})
    gpus = normalized.get("gpu", [])
    gpu = gpus[0] if gpus else {}
    active = bool(service.get("active"))
    unit = str(service.get("unit") or state.unit or "")
    environment = str(binding.get("environment") or "")
    revision = str(binding.get("checkpoint_revision") or "")
    checkpoint_n = int(binding.get("checkpoint_number") or 0)

    state.reliquary_one = normalized
    state.reliquary_one_error = ""
    state.last_poll_s = now
    state.proc_alive = active
    state.active_unit = unit if active else ""
    state.active_units = [unit] if active and unit else []
    state.active_lane = str(binding.get("strategy") or "code") if active else ""
    state.active_lanes = [state.active_lane] if state.active_lane else []
    state.active_environment = environment
    state.active_pid = int(service.get("pid") or 0)
    state.restart_count = int(service.get("restarts") or 0)
    state.proc_uptime_s = int(float(service.get("uptime_seconds") or 0.0))
    state.gpu_util = int(float(gpu.get("utilization_pct") or 0.0))
    state.gpu_mem_mb = int(float(gpu.get("memory_used_mib") or 0.0))
    state.gpu_total_mb = max(
        1,
        int(float(gpu.get("memory_total_mib") or 1.0)),
    )
    state.env_file_ok = True
    state.env_file_error = ""
    state.unit_resolution_error = ""
    state.unexpected_active_units = []
    state.engine_mode = "reliquary_one"
    state.miner_environment = environment
    state.reference_ready = bool(normalized.get("fresh"))
    state.runtime_parity_ok = bool(normalized.get("fresh") and revision)
    state.runtime_checkpoint_loaded = bool(revision)
    state.runtime_checkpoint_n = checkpoint_n
    state.runtime_checkpoint_revision = revision
    state.runtime_checkpoint_pid = state.active_pid
    state.runtime_checkpoint_started_at = int(
        float(binding.get("activated_at") or 0.0)
    )
    state.runtime_checkpoint_evidence = "reliquary_one_active_binding"
    state.local_checkpoint_n = checkpoint_n
    state.local_checkpoint_revision = revision
    state.miner_source_revision = str(binding.get("release_sha") or "")
    state.acceptance_source = "reliquary_one+verdicts+r2"
    state.error = "" if normalized.get("fresh") else "reliquary_one_stale"


def _collect_box_in_place(state: BoxState) -> None:
    """Poll a single box for GPU + journal metrics. Mutates state in place.

    The metrics are bundled into one SSH round-trip. Named-lane configs first
    use a small resolver round-trip to select one primary allowed systemd unit;
    an explicitly coordinated set is retained for display while the primary
    unit and EnvironmentFile are fixed for the detailed metrics probe.
    """
    if state.miner_kind == "reliquary_one":
        _collect_reliquary_one_in_place(state)
        return

    state.unit_resolution_error = ""
    state.unexpected_active_units = []
    selected_unit = state.unit or ""
    selected_env_file = state.env_file
    selected_controller_config_path = ""
    initial_selected_unit = ""
    initial_selected_pid = 0
    lane_resolver_enabled = bool(
        state.unit_candidates
        or state.host_unit_allowlist
        or state.active_unit_registry_path
    )
    if not lane_resolver_enabled:
        state.active_unit = ""
        state.active_lane = ""
        state.active_units = []
        state.active_lanes = []
        state.active_environment = ""
        state.active_pid = 0
    if lane_resolver_enabled:
        _clear_current_lane_telemetry(state)
        if not _refresh_active_unit_registry(state):
            state.last_poll_s = time.time()
            state.proc_alive = False
            state.reference_ready = False
            state.runtime_parity_ok = False
            state.env_file_ok = False
            state.env_file_error = state.unit_resolution_error
            state.error = state.unit_resolution_error[:120]
            return
        resolved = _resolve_allowed_active_unit(state)
        if resolved is None:
            state.last_poll_s = time.time()
            state.proc_alive = False
            state.reference_ready = False
            state.runtime_parity_ok = False
            state.env_file_ok = False
            state.env_file_error = state.unit_resolution_error
            state.error = state.unit_resolution_error[:120]
            # A submit-disabled certifier and the last immutable mining
            # snapshot answer different questions.  Always retain the
            # profile-scoped historical/supervisor truth before probing a
            # certifier so the latter cannot hide that the current mining
            # funnel is fenced and zero.
            _collect_standalone_files_after_unit_failure(state)
            if (
                state.unit_resolution_error == "no_allowed_unit_active"
                and _collect_standalone_certification_after_unit_failure(
                    state
                )
            ):
                # Certification is real compute but never a miner. Preserve
                # the empty active-unit/process/funnel state and the resolver
                # blocker while avoiding a misleading generic probe error.
                state.error = ""
                return
            return
        selected_unit, selected_env_file = resolved
        if not _select_active_unit_registry_entry(state, selected_unit):
            state.proc_alive = False
            state.reference_ready = False
            state.runtime_parity_ok = False
            state.error = state.unit_resolution_error[:120]
            return
        try:
            selected_controller_config_path = (
                _configured_controller_config_path(state, selected_unit)
            )
        except ValueError as exc:
            state.unit_resolution_error = f"config:{exc}"
            state.proc_alive = False
            state.reference_ready = False
            state.runtime_parity_ok = False
            state.error = state.unit_resolution_error[:120]
            return
        initial_selected_unit = selected_unit
        initial_selected_pid = state.active_pid

    # journalctl source — templated systemd unit when available, PID-based
    # fallback for one-off explorer processes. The current production fleet
    # runs `reliquary-miner-fresh@*.service`, so every process/GPU probe must
    # key off the configured unit instead of the old reliquary-miner-pro CLI.
    # Use a function so each call site that pipes through grep doesn't have to
    # worry about `|| true` swallowing the next pipe (subtle bash precedence
    # bug — `cmd || true | grep` parses as `cmd || (true | grep)`).
    if selected_unit:
        unit_q = shlex.quote(selected_unit)
        env_guess = selected_env_file or (
            ""
            if state.standalone_telemetry_path
            else resolve_box_env_file(state, selected_unit)
        )
        env_q = shlex.quote(env_guess)
        def jrange(window: str) -> str:
            return f"journalctl -u {unit_q} --no-pager --since '{window}'"
        jall = f"journalctl -u {unit_q} --no-pager"
        jreference = (
            "{ INVOCATION_ID=$(systemctl show -p InvocationID --value "
            f"{unit_q} 2>/dev/null); "
            "if [ -n \"$INVOCATION_ID\" ]; then "
            "journalctl _SYSTEMD_INVOCATION_ID=\"$INVOCATION_ID\" --no-pager; "
            f"else {jall}; fi; }}"
        )
        # Strict Code readiness must never search a previous invocation. An
        # empty InvocationID yields no lines and therefore no attestation.
        jreference_strict = (
            "{ INVOCATION_ID=$(systemctl show -p InvocationID --value "
            f"{unit_q} 2>/dev/null); "
            "if [ -n \"$INVOCATION_ID\" ]; then "
            "journalctl _SYSTEMD_INVOCATION_ID=\"$INVOCATION_ID\" --no-pager; "
            "fi; }"
        )
        proc_cmd = f"[ \"$(systemctl is-active {unit_q} 2>/dev/null)\" = active ] && echo 1 || echo 0"
        pstat_cmd = (
            f"PID=$(systemctl show -p MainPID --value {unit_q} 2>/dev/null); "
            "[ -n \"$PID\" ] && [ \"$PID\" != 0 ] && "
            "ps -o pcpu,rss --no-headers -p \"$PID\" 2>/dev/null | head -1 || true"
        )
        uptime_cmd = f"systemctl show -p ActiveEnterTimestamp --value {unit_q} 2>/dev/null"
        nrestarts_cmd = f"systemctl show -p NRestarts --value {unit_q} 2>/dev/null"
        pid_cmd = f"systemctl show -p MainPID --value {unit_q} 2>/dev/null"
        gpu_cmd = (
            "GPU_ID=$(sudo -n sed -n 's/^CUDA_VISIBLE_DEVICES=//p' "
            f"{env_q} 2>/dev/null | tail -1 | tr -cd '0-9,' | cut -d, -f1); "
            "[ -n \"$GPU_ID\" ] || GPU_ID=0; "
            "nvidia-smi -i \"$GPU_ID\" --query-gpu=memory.used,memory.total,utilization.gpu "
            "--format=csv,noheader 2>/dev/null | head -1"
        )
        env_dump_cmd = (
            f"sudo -n grep -E "
            "'^(DRAND_ROUND_OFFSET|PICKER_EPSILON|PICKER_TOP_K|"
            "EXTERNAL_FRONTIER_MIN_SIGMA|EXTERNAL_FRONTIER_MAX_LEN|"
            "HASH_MAX_LEADING_BYTE|BURST_PER_WINDOW|PROMPT_SHARD_ID|"
            "PROMPT_SHARD_MOD|RELIQUARY_ENVIRONMENTS|RELIQUARY_ENVIRONMENT_NAME|"
            "RELIQUARY_ENGINE_MODE|RELIQUARY_BASE_MODEL_REPO|"
            "RELIQUARY_BASE_MODEL_REVISION|"
            f"CUDA_VISIBLE_DEVICES)=' {env_q} 2>/dev/null || true; "
            "sudo -n test -r /srv/reliquary-miner-pro/state/source-manifest.env && "
            "sudo -n grep -E '^(MINER_PRO_SOURCE_REVISION|RELIQUARY_SOURCE_REVISION|"
            "PROVISIONED_OK|RELIQUARY_PROVISIONED_MODEL_KIND|"
            "RELIQUARY_PROVISIONED_CHECKPOINT_N|RELIQUARY_PROVISIONED_MODEL_REPO|"
            "RELIQUARY_PROVISIONED_MODEL_REVISION|RELIQUARY_BASE_MODEL_REPO|"
            "RELIQUARY_BASE_MODEL_REVISION)=' "
            "/srv/reliquary-miner-pro/state/source-manifest.env || true"
        )
        # Parse the root-only EnvironmentFile as inert KEY=VALUE data inside
        # the Python probes below.  Never source it in a privileged shell:
        # operator-controlled values must not become executable code.
        state_probe_cmd = f"sudo -n python3 - {env_q}"
    else:
        pid_expr = (
            "pgrep -f 'native_miner_fresh.py|vllm_frontier_explorer.py|"
            "reliquary-miner-pro --network' | head -1"
        )
        def jrange(window: str) -> str:
            return (f"{{ PID=$({pid_expr}); "
                    f"[ -n \"$PID\" ] && journalctl _PID=$PID --no-pager --since '{window}'; }} 2>/dev/null")
        jall = (f"{{ PID=$({pid_expr}); "
                "[ -n \"$PID\" ] && journalctl _PID=$PID --no-pager; } 2>/dev/null")
        jreference = jall
        jreference_strict = "true"
        proc_cmd = f"{pid_expr} | wc -l"
        pstat_cmd = f"PID=$({pid_expr}); [ -n \"$PID\" ] && ps -o pcpu,rss --no-headers -p \"$PID\" 2>/dev/null | head -1 || true"
        uptime_cmd = (f"PID=$({pid_expr}); "
                      "[ -n \"$PID\" ] && stat -c '%Y' /proc/$PID 2>/dev/null || true")
        # Templated unit name follows the hotkey for the RTX boxes too.
        nrestarts_cmd = f"systemctl show -p NRestarts --value reliquary-miner-pro@{state.hotkey} 2>/dev/null"
        pid_cmd = pid_expr
        gpu_cmd = "nvidia-smi --query-gpu=memory.used,memory.total,utilization.gpu --format=csv,noheader 2>/dev/null | head -1"
        env_dump_cmd = "true"
        state_probe_cmd = "python3 - ''"
    j30 = jrange("30 min ago")
    j60 = jrange("60 min ago")
    jall_oom = f"{jall} | grep OutOfMemoryError | tail -1"
    readiness_unit = (
        _canonical_systemd_unit(selected_unit) if selected_unit else ""
    )
    code_readiness_probe_cmd = " ".join(
        [
            state_probe_cmd,
            shlex.quote(readiness_unit),
            shlex.quote(CODE_GRADER_UNIT),
            shlex.quote(CODE_GRADER_SOCKET),
            shlex.quote(CODE_GRADER_BUNDLE_LINK),
            str(int(CODE_GRADER_METRICS_PORT)),
            str(int(CODE_GRADER_SOCKET_MODE)),
        ]
    )
    env_by_unit = dict(_configured_unit_specs(state))
    crossover_unit_specs = [
        [unit, env_by_unit.get(unit, "")]
        for unit in (
            state.active_units
            if state.active_units
            else ([readiness_unit] if readiness_unit else [])
        )
    ]
    crossover_probe_cmd = (
        "sudo -n python3 - "
        + shlex.quote(json.dumps(crossover_unit_specs, separators=(",", ":")))
    )
    overlap_abba_probe_cmd = (
        "sudo -n python3 - "
        + shlex.quote(json.dumps(crossover_unit_specs, separators=(",", ":")))
    )
    standalone_probe_cmd = " ".join(
        [
            "sudo -n python3 -",
            shlex.quote(state.standalone_telemetry_path),
            shlex.quote(state.standalone_runtime_manifest_path),
            shlex.quote(selected_controller_config_path),
            shlex.quote(readiness_unit),
            str(int(initial_selected_pid or state.active_pid or 0)),
            shlex.quote(state.standalone_supervisor_status_path),
        ]
    )

    one_shot = (
        "echo '===GPU==='; "
        f"{gpu_cmd}; "
        "echo '===PROC==='; "
        f"{proc_cmd}; "
        "echo '===PSTAT==='; "
        # CPU% + RSS for the running miner. ps prints header line we skip.
        f"{pstat_cmd}; "
        "echo '===DISK==='; "
        "df -P /srv 2>/dev/null | awk 'NR==2 {print $5}' | tr -d '%'; "
        "echo '===ENVFILE==='; "
        + (
            f"if sudo -n test -r {env_q}; then echo ok; else echo missing:{shlex.quote(env_guess)}; fi; "
            if selected_unit else "echo unmanaged; "
        ) +
        "echo '===MINERENV==='; "
        f"{env_dump_cmd}; "
        "echo '===CODEAUCTION==='; "
        f"{code_readiness_probe_cmd} <<'PYCODEAUCTION'\n"
        f"{_CODE_AUCTION_READINESS_PROBE_SOURCE}\n"
        "PYCODEAUCTION\n"
        "echo '===CODESELECTORCROSSOVER==='; "
        f"{crossover_probe_cmd} <<'PYCODESELECTORCROSSOVER'\n"
        f"{_CODE_SELECTOR_CROSSOVER_PROBE_SOURCE}\n"
        "PYCODESELECTORCROSSOVER\n"
        "echo '===CODEOVERLAPABBA==='; "
        f"{overlap_abba_probe_cmd} <<'PYCODEOVERLAPABBA'\n"
        f"{_CODE_OVERLAP_ABBA_PROBE_SOURCE}\n"
        "PYCODEOVERLAPABBA\n"
        "echo '===STANDALONE==='; "
        + (
            f"{standalone_probe_cmd} <<'PYSTANDALONE'\n"
            f"{_STANDALONE_TELEMETRY_PROBE_SOURCE}\n"
            "PYSTANDALONE\n"
            if state.standalone_telemetry_path
            else "printf '{}\\n'; "
        ) +
        "echo '===FRONTIER==='; "
        f"{state_probe_cmd} <<'PYFRONTIER'\n"
        "import glob, json, os, sys, time\n"
        "env_path = sys.argv[1] if len(sys.argv) > 1 else ''\n"
        "values = {}\n"
        "try:\n"
        "    for line in open(env_path):\n"
        "        line = line.strip()\n"
        "        if not line or line.startswith('#') or '=' not in line:\n"
        "            continue\n"
        "        key, value = line.split('=', 1)\n"
        "        value = value.strip()\n"
        "        if len(value) >= 2 and value[0] == value[-1] and value[0] in \"'\\\"\":\n"
        "            value = value[1:-1]\n"
        "        values[key.strip()] = value\n"
        "except Exception:\n"
        "    pass\n"
        "p = values.get('RELIQUARY_EXTERNAL_FRONTIER_PATH') or values.get('RELIQUARY_FRONTIER_STATE_PATH') or ''\n"
        "if not p and os.path.exists('/workspace/frontier.json'):\n"
        "    p = '/workspace/frontier.json'\n"
        # Auction-v2 deliberately writes into a profile-isolated state file so
        # observations from the legacy sampler can never contaminate it.  The
        # operator EnvironmentFile continues to name the logical/base path,
        # therefore resolve the active physical file here.  Startup installs
        # the current seed immediately, making the active file the newest one
        # when both legacy and auction-v2 state happen to exist.
        "auction_candidates = []\n"
        "if p:\n"
        "    auction_candidates = [p + '.reference-auction-v2']\n"
        "    auction_candidates += glob.glob(p + '.reference-auction-v2.public-*')\n"
        "auction_candidates = [candidate for candidate in auction_candidates if os.path.isfile(candidate)]\n"
        "if auction_candidates:\n"
        "    auction_p = max(auction_candidates, key=os.path.getmtime)\n"
        "    if not os.path.isfile(p) or os.path.getmtime(auction_p) >= os.path.getmtime(p):\n"
        "        p = auction_p\n"
        "out = {'path': p, 'entries': 0, 'content_entries': 0, 'age_s': -1, 'newest_age_s': -1, 'checkpoint_n': 0, 'checkpoint_revision': '', 'ckpts': {}}\n"
        "try:\n"
        "    if p and os.path.exists(p):\n"
        "        st = os.stat(p)\n"
        "        raw = json.load(open(p))\n"
        "        now = time.time()\n"
        "        reference_schema = isinstance(raw, dict) and isinstance(raw.get('stats'), dict)\n"
        "        rows = list(raw['stats'].values()) if reference_schema else list(raw.values()) if isinstance(raw, dict) else raw if isinstance(raw, list) else []\n"
        "        out['entries'] = len(rows)\n"
        "        out['age_s'] = round(now - st.st_mtime, 1)\n"
        "        if reference_schema:\n"
        "            out['checkpoint_n'] = int(raw.get('checkpoint_n') or 0)\n"
        "            out['newest_age_s'] = out['age_s']\n"
        "            content_path = p + '.reference-content.json'\n"
        "            if os.path.exists(content_path):\n"
        "                content = json.load(open(content_path))\n"
        "                content_stats = content.get('stats') if isinstance(content, dict) else {}\n"
        "                out['content_entries'] = len(content_stats) if isinstance(content_stats, dict) else 0\n"
        "                out['checkpoint_n'] = int(content.get('checkpoint_n') or out['checkpoint_n'])\n"
        "                bindings = content.get('protocol_bindings') if isinstance(content, dict) else {}\n"
        "                revision = bindings.get('checkpoint_revision') if isinstance(bindings, dict) else ''\n"
        "                if isinstance(revision, str):\n"
        "                    out['checkpoint_revision'] = revision\n"
        "                out['newest_age_s'] = min(out['newest_age_s'], round(now - os.stat(content_path).st_mtime, 1))\n"
        "            ckpt = out['checkpoint_revision'][:12] or ('n' + str(out['checkpoint_n']) if out['checkpoint_n'] else '')\n"
        "            if ckpt:\n"
        "                out['ckpts'] = {ckpt: len(rows)}\n"
        "        newest = 0.0\n"
        "        counts = {}\n"
        "        for row in ([] if reference_schema else rows):\n"
        "            if not isinstance(row, dict):\n"
        "                continue\n"
        "            ckpt = str(row.get('ckpt') or row.get('checkpoint_revision') or row.get('checkpoint') or '')[:12]\n"
        "            if ckpt:\n"
        "                counts[ckpt] = counts.get(ckpt, 0) + 1\n"
        "            ts = row.get('ts') or row.get('timestamp') or row.get('created_at') or row.get('updated_at') or 0\n"
        "            try:\n"
        "                newest = max(newest, float(ts))\n"
        "            except Exception:\n"
        "                pass\n"
        "        if not reference_schema:\n"
        "            out['newest_age_s'] = round(now - newest, 1) if newest else -1\n"
        "            out['ckpts'] = dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:6])\n"
        "except Exception as e:\n"
        "    out['error'] = type(e).__name__\n"
        "print(json.dumps(out, separators=(',', ':')))\n"
        "PYFRONTIER\n"
        "echo '===STATEHEALTH==='; "
        "curl -fsS --max-time 3 http://127.0.0.1:18080/health 2>/dev/null || printf '{}'; "
        "echo; "
        "echo '===WATCHDOG==='; "
        "for p in /workspace/frontier_watchdog.json /srv/reliquary-miner-pro/state/frontier_watchdog.json; do "
        "  if [ -r \"$p\" ]; then cat \"$p\"; found=1; break; fi; "
        "done; "
        "[ \"${found:-0}\" = 1 ] || printf '{}'; "
        "echo; "
        "echo '===QUARANTINE==='; "
        f"{state_probe_cmd} <<'PYQUARANTINE'\n"
        "import json, os, sys\n"
        "env_path = sys.argv[1] if len(sys.argv) > 1 else ''\n"
        "p = '/srv/reliquary-miner-pro/state/quarantine.json'\n"
        "try:\n"
        "    for line in open(env_path):\n"
        "        line = line.strip()\n"
        "        if line.startswith('RELIQUARY_QUARANTINE_PATH='):\n"
        "            value = line.split('=', 1)[1].strip()\n"
        "            if len(value) >= 2 and value[0] == value[-1] and value[0] in \"'\\\"\":\n"
        "                value = value[1:-1]\n"
        "            if value:\n"
        "                p = value\n"
        "except Exception:\n"
        "    pass\n"
        "out = {'path': p, 'active': False}\n"
        "try:\n"
        "    if os.path.exists(p):\n"
        "        out['active'] = True\n"
        "        raw = json.load(open(p))\n"
        "        out['reason'] = str(raw.get('reason') or raw.get('source') or 'quarantined')\n"
        "        out['quarantined_at'] = float(raw.get('quarantined_at') or 0)\n"
        "except Exception as e:\n"
        "    out['active'] = True\n"
        "    out['reason'] = 'unreadable:' + type(e).__name__\n"
        "print(json.dumps(out, separators=(',', ':')))\n"
        "PYQUARANTINE\n"
        # Capture one invocation-scoped journal snapshot and derive every
        # reference section from it.  In particular, keep sparse checkpoint
        # transition evidence independently of the noisy recent-event tail so
        # a long-lived process does not forget a successful dynamic load.
        "REFERENCE_LOG=$("
        f"{jreference}"
        "); "
        "CODE_REFERENCE_LOG=$("
        f"{jreference_strict}"
        "); "
        "echo '===REFERENCEPROFILE==='; "
        "printf '%s\n' \"$REFERENCE_LOG\" | grep -E 'validator protocol parity ok profile=|installed protocol-identical vectorized forced-seed sampler .*profile=|validator runtime parity ok|validator runtime telemetry enabled|pro miner ready' | tail -20; "
        "echo '===CODEREFERENCEPROFILE==='; "
        "printf '%s\n' \"$CODE_REFERENCE_LOG\" | grep -E 'validator runtime telemetry enabled|code_auction_readiness' | tail -8; "
        "echo '===RUNTIMECHECKPOINT==='; "
        "printf '%s\n' \"$REFERENCE_LOG\" | grep -E 'math_auction_readiness[[:space:]]+\\{' | tail -4; "
        "printf '%s\n' \"$REFERENCE_LOG\" | grep -E 'checkpoint (cache miss; falling back to Hub|resolved from local cache) repo=' | tail -4; "
        "printf '%s\n' \"$REFERENCE_LOG\" | grep -E 'Checkpoint .*/snapshots/[0-9a-fA-F]{40} loaded into both models' | tail -4; "
        "printf '%s\n' \"$REFERENCE_LOG\" | grep -E '(math|code)_generation_group_abort( |$)|((math|code)_)?generation(_group)?_start(ed)?( |$)' | tail -8; "
        "echo '===REFERENCE==='; "
        "printf '%s\n' \"$REFERENCE_LOG\" | grep -E 'starting pro miner|validator protocol parity ok|validator runtime parity ok|validator runtime telemetry enabled|pro miner ready|reactive attempt|reference Math .*frontier selected prompt=|reference Math frontier learned prompt=|reference Math frontier pre-screen dropped group before GRAIL|generated 0/8 .* skipping|reference Math frontier checkpoint advanced|checkpoint resolved from local cache|checkpoint cache miss|Loading checkpoint from|loaded into both models|SUBMIT-DIAG|submitted window=|submit failed:|protocol canary quarantined|persistent miner quarantine' | tail -40; "
        "echo '===NRESTARTS==='; "
        f"{nrestarts_cmd}; "
        "echo '===PID==='; "
        f"{pid_cmd}; "
        "echo '===UPTIME==='; "
        f"{uptime_cmd}; "
        "echo '===NOW==='; "
        "date -u +'%s'; "
        "echo '===EVENTS30==='; "
        f"{j30} | grep -E 'ACCEPTED|rejected|FINAL-VERDICT|ERROR|CRITICAL|window_mismatch|grail_fail|bad_term|pre-skip|pregen GRAIL build failed|reactive attempt|state=.*(ready=|ready_sketches=)|fresh sketch built|BURST window=|late-open grace active|verdicts learned|checkpoint advanced|tune:|(math|code)_generation_group_start \\{{|(math|code)_auction_score \\{{|submit precommit response|submitted window=' | tail -260; "
        "echo '===ACPT60==='; "
        f"{j60} | grep -c ACCEPTED; "
        "echo '===CKPT60==='; "
        f"{j60} | grep -c 'checkpoint advanced'; "
        "echo '===OOM30==='; "
        f"{j30} | grep -c OutOfMemoryError; "
        "echo '===OOM60==='; "
        f"{j60} | grep -c OutOfMemoryError; "
        "echo '===LASTOOM==='; "
        f"{jall_oom}"
    )
    rc, out, err = ssh_run(state.alias, one_shot, timeout_s=30)
    state.last_poll_s = time.time()
    if rc != 0 and not out:
        probe_error = (err or f"rc={rc}")[:60]
        if lane_resolver_enabled:
            _clear_current_lane_telemetry(state)
            state.unit_resolution_error = "selected_unit_probe_unavailable"
            state.env_file_error = state.unit_resolution_error
        state.error = probe_error
        # Do not keep a previously-live process painted green when the most
        # recent authoritative SSH probe failed.  The error text still
        # distinguishes an unreachable box from a stopped service.
        state.proc_alive = False
        state.reference_ready = False
        state.env_file_ok = False
        state.env_file_error = "probe unavailable"
        return
    state.error = ""

    # Section parser
    sect: dict[str, list[str]] = {
        "GPU": [], "PROC": [], "PSTAT": [], "DISK": [], "NRESTARTS": [], "PID": [],
        "UPTIME": [], "NOW": [],
        "ENVFILE": [], "MINERENV": [], "CODEAUCTION": [],
        "CODESELECTORCROSSOVER": [], "CODEOVERLAPABBA": [],
        "STANDALONE": [], "FRONTIER": [],
        "STATEHEALTH": [], "WATCHDOG": [],
        "QUARANTINE": [], "REFERENCEPROFILE": [],
        "CODEREFERENCEPROFILE": [], "RUNTIMECHECKPOINT": [],
        "REFERENCE": [],
        "EVENTS30": [], "ACPT60": [], "CKPT60": [], "OOM30": [], "OOM60": [], "LASTOOM": [],
    }
    cur = None
    for line in out.splitlines():
        s = line.strip()
        if s.startswith("===") and s.endswith("==="):
            cur = s.strip("=")
            continue
        if cur and cur in sect:
            sect[cur].append(s)

    # GPU
    if sect["GPU"]:
        try:
            parts = [p.strip() for p in sect["GPU"][0].split(",")]
            state.gpu_mem_mb = int(parts[0].split()[0])
            state.gpu_total_mb = max(int(parts[1].split()[0]), 1)
            state.gpu_util = int(parts[2].split()[0])
        except Exception:
            pass

    # Process alive
    if sect["PROC"]:
        try:
            state.proc_alive = int(sect["PROC"][0]) > 0
        except Exception:
            state.proc_alive = False

    if sect["PID"] and sect["PID"][0].isdigit():
        state.active_pid = int(sect["PID"][0])
    elif lane_resolver_enabled or not state.proc_alive:
        # A named-lane metrics probe must independently corroborate the PID
        # selected by the first resolver. Do not inherit that resolver PID
        # when the metrics response omitted or malformed its PID section.
        state.active_pid = 0
    if lane_resolver_enabled and (
        not state.proc_alive or state.active_pid <= 0
    ):
        _clear_current_lane_telemetry(state)
        state.unit_resolution_error = "selected_unit_became_inactive"
        state.env_file_error = state.unit_resolution_error
        state.error = state.unit_resolution_error
        return
    if lane_resolver_enabled and (
        state.active_unit != initial_selected_unit
        or state.active_pid != initial_selected_pid
    ):
        _clear_current_lane_telemetry(state)
        state.unit_resolution_error = "unit_selection_changed_during_probe"
        state.env_file_error = state.unit_resolution_error
        state.error = state.unit_resolution_error
        return
    if not lane_resolver_enabled:
        if state.proc_alive and selected_unit:
            try:
                state.active_unit = _canonical_systemd_unit(selected_unit)
                state.active_lane = _unit_lane(selected_unit)
                state.active_units = [state.active_unit]
                state.active_lanes = [state.active_lane]
            except ValueError:
                state.active_unit = ""
                state.active_lane = ""
                state.active_units = []
                state.active_lanes = []
        else:
            state.active_unit = ""
            state.active_lane = ""
            state.active_units = []
            state.active_lanes = []

    # CPU + RSS — `ps -o pcpu,rss` prints e.g. "  3.4 1234567" (rss in KB)
    if sect["PSTAT"] and sect["PSTAT"][0].strip():
        try:
            parts = sect["PSTAT"][0].split()
            state.cpu_pct = int(round(float(parts[0])))
            state.rss_mb = int(parts[1]) // 1024
        except Exception:
            pass

    # Disk usage on /srv (where frontier files + journal grow)
    if sect["DISK"] and sect["DISK"][0].isdigit():
        state.disk_used_pct = int(sect["DISK"][0])

    if selected_unit:
        if state.standalone_telemetry_path and not selected_env_file:
            state.env_file_ok = True
            state.env_file_error = ""
        else:
            env_status = (
                sect["ENVFILE"][0] if sect["ENVFILE"] else "missing:probe"
            )
            state.env_file_ok = env_status == "ok"
            state.env_file_error = "" if state.env_file_ok else env_status[:240]
    else:
        state.env_file_ok = True
        state.env_file_error = ""

    # The source manifest is replaced atomically by provisioning. Clear its
    # cached identity before parsing each successful probe so a downgraded,
    # incomplete, or removed manifest can never inherit a previous poll's
    # exact-model readiness. Base pins are read from the operator env first and
    # then overwritten by immutable source-manifest values when present; they
    # receive the same reset treatment.
    state.miner_source_revision = ""
    state.reliquary_source_revision = ""
    state.observed_validator_image_revision = ""
    state.source_manifest_provisioned_ok = False
    state.provisioned_model_kind = ""
    state.provisioned_checkpoint_n = -1
    state.provisioned_model_repo = ""
    state.provisioned_model_revision = ""
    state.base_model_repo = ""
    state.base_model_revision = ""
    # These values are lane-scoped. Clear them before reading the selected
    # unit's EnvironmentFile/journal so a Math -> Code handoff cannot retain a
    # stale environment, frontier identity, or quarantine path.
    state.miner_environment = ""
    state.engine_mode = ""
    state.frontier_path = ""
    state.frontier_entries = 0
    state.frontier_content_entries = 0
    state.frontier_age_s = -1.0
    state.frontier_newest_age_s = -1.0
    state.frontier_checkpoint_n = 0
    state.frontier_checkpoint_revision = ""
    state.frontier_ckpts = {}
    state.local_checkpoint_n = 0
    state.local_checkpoint_revision = ""
    state.runtime_checkpoint_loaded = False
    state.runtime_checkpoint_n = 0
    state.runtime_checkpoint_repo = ""
    state.runtime_checkpoint_revision = ""
    state.runtime_checkpoint_pid = 0
    state.runtime_checkpoint_started_at = 0
    state.runtime_checkpoint_evidence = ""
    state.miner_unit_enablement = ""
    state.runtime_profile_hash = ""
    state.code_auction_probe = {}
    state.code_selector_crossover_probe = {}
    state.code_overlap_abba_probe = {}
    state.proc_uptime_s = 0
    state.active_started_at = 0
    state.quarantine_path = ""

    for raw in sect["MINERENV"]:
        if "=" not in raw:
            continue
        k, v = raw.split("=", 1)
        v = v.strip()
        if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"":
            v = v[1:-1]
        try:
            if k == "DRAND_ROUND_OFFSET":
                state.drand_offset = int(v)
            elif k == "PICKER_EPSILON":
                state.picker_epsilon = float(v)
            elif k == "PICKER_TOP_K":
                state.picker_top_k = int(v)
            elif k == "EXTERNAL_FRONTIER_MIN_SIGMA":
                state.external_min_sigma = float(v)
            elif k == "EXTERNAL_FRONTIER_MAX_LEN":
                state.external_max_len = int(v)
            elif k == "HASH_MAX_LEADING_BYTE":
                state.hash_max_leading_byte = int(v)
            elif k == "BURST_PER_WINDOW":
                state.burst_per_window = int(v)
            elif k == "PROMPT_SHARD_ID":
                state.prompt_shard_id = int(v)
            elif k == "PROMPT_SHARD_MOD":
                state.prompt_shard_mod = int(v)
            elif k in ("RELIQUARY_ENVIRONMENTS", "RELIQUARY_ENVIRONMENT_NAME"):
                state.miner_environment = v.strip()
            elif k == "RELIQUARY_ENGINE_MODE":
                state.engine_mode = v.strip()
            elif k == "RELIQUARY_BASE_MODEL_REPO":
                state.base_model_repo = v.strip()
            elif k == "RELIQUARY_BASE_MODEL_REVISION":
                state.base_model_revision = v.strip()
            elif k == "RELIQUARY_QUARANTINE_PATH":
                state.quarantine_path = v.strip()
            elif k == "MINER_PRO_SOURCE_REVISION":
                state.miner_source_revision = v.strip()
            elif k == "RELIQUARY_SOURCE_REVISION":
                state.reliquary_source_revision = v.strip()
            elif k == "PROVISIONED_OK":
                state.source_manifest_provisioned_ok = v.strip() == "1"
            elif k == "RELIQUARY_PROVISIONED_MODEL_KIND":
                state.provisioned_model_kind = v.strip()
            elif k == "RELIQUARY_PROVISIONED_CHECKPOINT_N":
                state.provisioned_checkpoint_n = int(v)
            elif k == "RELIQUARY_PROVISIONED_MODEL_REPO":
                state.provisioned_model_repo = v.strip()
            elif k == "RELIQUARY_PROVISIONED_MODEL_REVISION":
                state.provisioned_model_revision = v.strip()
        except Exception:
            pass

    state.code_auction_probe = _parse_code_auction_probe(sect["CODEAUCTION"])
    state.code_selector_crossover_probe = (
        _parse_code_selector_crossover_probe(
            sect["CODESELECTORCROSSOVER"]
        )
    )
    state.code_overlap_abba_probe = _parse_code_overlap_abba_probe(
        sect["CODEOVERLAPABBA"]
    )
    state.miner_unit_enablement = str(
        state.code_auction_probe.get("miner_unit_enablement") or ""
    )

    if sect["FRONTIER"]:
        try:
            f = json.loads(sect["FRONTIER"][0])
            state.frontier_path = str(f.get("path") or "")
            state.frontier_entries = int(f.get("entries") or 0)
            state.frontier_content_entries = int(f.get("content_entries") or 0)
            state.frontier_age_s = float(f.get("age_s", -1))
            state.frontier_newest_age_s = float(f.get("newest_age_s", -1))
            state.frontier_checkpoint_n = int(f.get("checkpoint_n") or 0)
            state.frontier_checkpoint_revision = str(
                f.get("checkpoint_revision") or ""
            )
            ckpts = f.get("ckpts") if isinstance(f, dict) else {}
            state.frontier_ckpts = {
                str(k): int(v) for k, v in (ckpts or {}).items()
                if str(k)
            }
        except Exception:
            pass

    if sect["STATEHEALTH"]:
        try:
            h = json.loads(sect["STATEHEALTH"][0] or "{}")
            state.state_relay_ok = bool(h.get("ok"))
            state.state_relay_age_s = float(h.get("age_s", -1))
            state.state_relay_upstream_ms = float(h.get("upstream_ms", 0))
            state.state_relay_failures = int(h.get("upstream_failures", 0) or 0)
            state.state_relay_error = str(h.get("last_error") or "")[:160]
        except Exception:
            state.state_relay_ok = False
            state.state_relay_error = "parse"

    if sect["WATCHDOG"]:
        try:
            w = json.loads(sect["WATCHDOG"][0] or "{}")
            ts = int(w.get("ts") or 0)
            state.watchdog_ok = bool(w.get("ok"))
            state.watchdog_age_s = max(time.time() - ts, 0.0) if ts else -1.0
            state.watchdog_stale_strikes = int(w.get("stale_strikes", 0) or 0)
            state.watchdog_restart_count = int(w.get("restart_count", 0) or 0)
            state.watchdog_last_action = str(w.get("last_action") or "")[:160]
            state.watchdog_last_error = str(w.get("last_error") or "")[:160]
        except Exception:
            state.watchdog_ok = False
            state.watchdog_last_error = "parse"

    # The reference miner writes a durable quarantine sentinel before
    # stopping on protocol-fatal verdicts.  It is more authoritative than a
    # transient journal line and survives service/dashboard restarts.
    state.quarantine_active = False
    state.quarantine_reason = ""
    state.quarantine_at = 0.0
    quarantine_probe_ok = False
    if sect["QUARANTINE"]:
        try:
            q = json.loads(sect["QUARANTINE"][0] or "{}")
            quarantine_probe_ok = True
            state.quarantine_active = bool(q.get("active"))
            state.quarantine_path = str(q.get("path") or state.quarantine_path)
            state.quarantine_reason = str(q.get("reason") or "")[:240]
            state.quarantine_at = float(q.get("quarantined_at") or 0.0)
        except Exception:
            state.quarantine_active = True
            state.quarantine_reason = "sentinel_parse_error"

    # Startup markers are intentionally read from the complete unit journal:
    # they describe compatibility gates that normally occur only once per
    # process.  Recompute booleans on each successful probe so a restarted
    # service cannot inherit readiness from an older process.
    advertised_profile = ""
    installed_sampler_profile = ""
    runtime_attestation: dict[str, Any] = {}
    for raw in [*sect["REFERENCEPROFILE"], *sect["REFERENCE"]]:
        advertised = re.search(
            r"validator protocol parity ok profile=([^\s]+)", raw
        )
        if advertised:
            advertised_profile = advertised.group(1)
        installed = re.search(
            r"installed protocol-identical vectorized forced-seed sampler "
            r".*?\bprofile=([^\s]+)",
            raw,
        )
        if installed:
            installed_sampler_profile = installed.group(1)

    # Code runtime identity and readiness must come exclusively from the
    # current systemd InvocationID section. The shell leaves this section empty
    # when InvocationID is unavailable; it never falls back to a whole-unit
    # journal that could contain an earlier process's startup attestation.
    for raw in sect["CODEREFERENCEPROFILE"]:
        runtime_profile = re.search(
            r"(?:validator_profile|profile_hash)=([0-9a-fA-F]{64})(?:\s|$)",
            raw,
        )
        if runtime_profile:
            state.runtime_profile_hash = runtime_profile.group(1).lower()
        attestation_match = re.search(
            r"\bcode_auction_readiness\s+(\{.*\})\s*$", raw
        )
        if attestation_match:
            try:
                attestation_raw = json.loads(attestation_match.group(1))
                if isinstance(attestation_raw, dict):
                    runtime_attestation = {
                        "schema_version": (
                            int(attestation_raw.get("schema_version") or 0)
                            if not isinstance(
                                attestation_raw.get("schema_version"), bool
                            )
                            else 0
                        ),
                        "prescreen_enabled": (
                            attestation_raw.get("prescreen_enabled") is True
                        ),
                        "auction_policy": str(
                            attestation_raw.get("auction_policy") or ""
                        )[:128],
                        "ledger_path": str(
                            attestation_raw.get("ledger_path") or ""
                        )[:512],
                        "ledger_available": (
                            attestation_raw.get("ledger_available") is True
                        ),
                        "ledger_partition_registered": (
                            attestation_raw.get("ledger_partition_registered")
                            is True
                        ),
                        "ledger_schema_version": (
                            int(
                                attestation_raw.get("ledger_schema_version")
                                or 0
                            )
                            if not isinstance(
                                attestation_raw.get("ledger_schema_version"),
                                bool,
                            )
                            else 0
                        ),
                        "grader_socket": str(
                            attestation_raw.get("grader_socket") or ""
                        )[:512],
                        "grader_source_revision": str(
                            attestation_raw.get("grader_source_revision") or ""
                        )[:64],
                        "process_pid": (
                            int(attestation_raw.get("process_pid") or 0)
                            if not isinstance(
                                attestation_raw.get("process_pid"), bool
                            )
                            else 0
                        ),
                        "miner_source_revision": str(
                            attestation_raw.get("miner_source_revision") or ""
                        )[:64],
                        "public_source_revision": str(
                            attestation_raw.get("public_source_revision") or ""
                        )[:64],
                        "runtime_profile_hash": str(
                            attestation_raw.get("runtime_profile_hash") or ""
                        )[:128],
                    }
            except (TypeError, ValueError, OverflowError, json.JSONDecodeError):
                runtime_attestation = {}
    if state.code_auction_probe:
        state.code_auction_probe["runtime_attestation"] = runtime_attestation
    # The installed adapter marker is emitted after the fail-closed profile
    # gate and names the sampler actually patched into the running process.
    # Prefer it when journald did not retain the earlier CLI parity line.
    state.protocol_profile = installed_sampler_profile or advertised_profile
    state.runtime_parity_ok = False
    state.reference_ready = False
    state.miner_state = "ready" if state.proc_alive else "down"
    state.miner_inflight = 0
    state.miner_ready = 1 if state.proc_alive else 0
    state.miner_submitted_this_win = 0
    latest_submit_window = 0
    # REFERENCE is a noisy tail and can lose the one-shot startup markers on a
    # productive long-lived process. REFERENCEPROFILE is a separate sparse
    # invocation-wide capture, so readiness must also be recovered from it.
    for raw in sect["REFERENCEPROFILE"]:
        if "validator runtime parity ok" in raw:
            state.runtime_parity_ok = True
        if "pro miner ready" in raw:
            state.reference_ready = state.proc_alive
        if "validator runtime telemetry enabled" in raw:
            state.runtime_parity_ok = True
            state.reference_ready = state.proc_alive
    for raw in sect["REFERENCE"]:
        m = re.search(r"starting pro miner .*\benv=([^\s]+)\s+engine_mode=([^\s]+)", raw)
        if m:
            state.miner_environment = m.group(1)
            state.engine_mode = m.group(2)
        if "validator runtime parity ok" in raw:
            state.runtime_parity_ok = True
        checkpoint_match = re.search(
            r"(?:revision=|snapshots/)([0-9a-f]{40})", raw
        )
        if checkpoint_match:
            state.local_checkpoint_revision = checkpoint_match.group(1)
        checkpoint_n_match = re.search(
            r"reference Math frontier checkpoint advanced n=(\d+)", raw
        )
        if checkpoint_n_match:
            state.local_checkpoint_n = int(checkpoint_n_match.group(1))
        if re.search(r"reference Math .*frontier selected prompt=", raw):
            state.miner_state = "generating"
            state.miner_window = latest_submit_window
            state.miner_inflight = 1
            state.miner_ready = 0
            state.miner_submitted_this_win = int(latest_submit_window > 0)
        elif "reference Math frontier learned prompt=" in raw:
            state.miner_state = "proving"
            state.miner_window = latest_submit_window
            state.miner_inflight = 1
            state.miner_ready = 0
            state.miner_submitted_this_win = int(latest_submit_window > 0)
        elif (
            "reference Math frontier pre-screen dropped group before GRAIL" in raw
            or re.search(r"generated 0/8 .* skipping", raw)
        ):
            state.miner_state = "waiting"
            state.miner_window = latest_submit_window
            state.miner_inflight = 0
            state.miner_ready = 1 if state.proc_alive else 0
            state.miner_submitted_this_win = int(latest_submit_window > 0)
        submit_match = re.search(r"SUBMIT-DIAG .*\bwindow=(\d+)", raw)
        if submit_match:
            latest_submit_window = int(submit_match.group(1))
            state.miner_state = "submitted"
            state.miner_window = latest_submit_window
            state.miner_inflight = 0
            state.miner_ready = 1
            state.miner_submitted_this_win = 1
        submitted_match = re.search(r"submitted window=(\d+)", raw)
        if submitted_match:
            latest_submit_window = int(submitted_match.group(1))
            state.miner_state = "submitted"
            state.miner_window = latest_submit_window
            state.miner_inflight = 0
            state.miner_ready = 1
            state.miner_submitted_this_win = 1
        sealed_match = re.search(
            r"submit failed: submission window sealed .*\bwindow=(\d+)", raw
        )
        if sealed_match:
            state.miner_state = "waiting"
            state.miner_window = int(sealed_match.group(1))
            state.miner_inflight = 0
            state.miner_ready = 1
            state.miner_submitted_this_win = 0
        if "pro miner ready" in raw:
            state.reference_ready = state.proc_alive
        if (
            "validator runtime telemetry enabled" in raw
            or "reactive attempt" in raw
            or "SUBMIT-DIAG" in raw
            or "submitted window=" in raw
            or "submit failed:" in raw
            or "generated 0/8" in raw
        ):
            # These lines are emitted only after the fail-closed protocol and
            # runtime gates have completed and the reference engine has
            # entered its mining loop.  They are a robust fallback on hosts
            # where journald coalesces the one-shot CLI readiness lines.
            state.runtime_parity_ok = True
            state.reference_ready = state.proc_alive
            if not state.protocol_profile:
                state.protocol_profile = "validator-gated"
        if (
            not quarantine_probe_ok
            and ("protocol canary quarantined" in raw or "persistent miner quarantine" in raw)
        ):
            state.quarantine_active = True
            if not state.quarantine_reason:
                state.quarantine_reason = raw[-240:]

    # For coordinated lanes the resolver's complete set is authoritative and
    # ``restart_count`` is its sum. The detailed primary-lane probe must not
    # overwrite that aggregate with one service's NRestarts value.
    if state.coordinated_unit_statuses:
        state.restart_count = sum(
            int(row.get("restarts", 0))
            for row in state.coordinated_unit_statuses
        )
    elif sect["NRESTARTS"] and sect["NRESTARTS"][0].isdigit():
        state.restart_count = int(sect["NRESTARTS"][0])

    # Process uptime — systemd ActiveEnterTimestamp is "Sat 2026-05-10 00:23:25 UTC".
    # Non-systemd path returns a unix epoch from /proc/<pid>'s mtime.
    now_epoch = int(sect["NOW"][0]) if sect["NOW"] and sect["NOW"][0].isdigit() else 0
    if sect["UPTIME"] and now_epoch:
        raw = sect["UPTIME"][0].strip()
        try:
            started_at = 0
            if raw.isdigit():
                started_at = int(raw)
            else:
                # systemd format: "Sat 2026-05-10 00:23:25 UTC"
                bits = raw.split()
                if len(bits) >= 4:
                    ts = datetime.strptime(f"{bits[1]} {bits[2]}", "%Y-%m-%d %H:%M:%S")
                    ts = ts.replace(tzinfo=timezone.utc)
                    started_at = int(ts.timestamp())
            if 0 < started_at <= now_epoch + 1:
                state.active_started_at = started_at
                state.proc_uptime_s = max(now_epoch - started_at, 0)
        except (TypeError, ValueError, OverflowError):
            pass

    # A dynamic checkpoint may supersede the immutable provisioning manifest
    # without restarting the miner.  Attest it only from this active process's
    # invocation-scoped journal and bind the result to the independently
    # observed PID + start epoch.  Missing process metadata fails closed.
    runtime_repo_hint = ""
    if (
        state.source_manifest_provisioned_ok
        and state.provisioned_model_kind == "validator_checkpoint"
        and state.provisioned_checkpoint_n > 0
        and _EXACT_CHECKPOINT_REVISION_RE.fullmatch(
            state.provisioned_model_revision
        )
    ):
        runtime_repo_hint = state.provisioned_model_repo
    runtime_checkpoint = _parse_runtime_checkpoint_evidence(
        sect["RUNTIMECHECKPOINT"],
        repo_hint=runtime_repo_hint,
        expected_process_pid=state.active_pid,
        expected_public_source_revision=state.reliquary_source_revision,
    )
    runtime_checkpoint_pid = (
        int(runtime_checkpoint.get("process_pid") or state.active_pid)
        if runtime_checkpoint is not None
        else 0
    )
    if (
        runtime_checkpoint is not None
        and state.proc_alive
        and state.active_pid > 0
        and runtime_checkpoint_pid == state.active_pid
        and state.active_started_at > 0
    ):
        state.runtime_checkpoint_loaded = True
        state.runtime_checkpoint_n = int(runtime_checkpoint["checkpoint_n"])
        state.runtime_checkpoint_repo = str(runtime_checkpoint["repo"])
        state.runtime_checkpoint_revision = str(runtime_checkpoint["revision"])
        state.runtime_checkpoint_pid = runtime_checkpoint_pid
        state.runtime_checkpoint_started_at = state.active_started_at
        state.runtime_checkpoint_evidence = str(runtime_checkpoint["evidence"])
        # Preserve the legacy observed-journal fields as diagnostics, but make
        # them coherent when stronger runtime evidence is available.
        state.local_checkpoint_n = state.runtime_checkpoint_n
        state.local_checkpoint_revision = state.runtime_checkpoint_revision

    # Last OOM (whole-journal lookup) — used to compute "OOM-free streak"
    if sect["LASTOOM"] and sect["LASTOOM"][0].strip():
        raw = sect["LASTOOM"][0]
        # Journal: "May 09 23:30:57 host reliquary-miner-pro[..]: ... OutOfMemoryError"
        bits = raw.split(maxsplit=3)
        if len(bits) >= 3:
            state.last_oom_at = f"{bits[0]} {bits[1]} {bits[2]}"
            try:
                # journal omits year — assume current year, fall back if Dec→Jan
                year = datetime.now(timezone.utc).year
                ts = datetime.strptime(f"{year} {bits[0]} {bits[1]} {bits[2]}", "%Y %b %d %H:%M:%S")
                ts = ts.replace(tzinfo=timezone.utc)
                age = now_epoch - int(ts.timestamp()) if now_epoch else -1
                # If "future" by more than a day, assume previous year (Dec→Jan rollover)
                if age < -86400:
                    ts = ts.replace(year=year - 1)
                    age = now_epoch - int(ts.timestamp())
                state.last_oom_age_s = age
            except Exception:
                state.last_oom_age_s = -1
    else:
        # Empty LASTOOM section → grep found nothing → no OOM in this miner's
        # journal at all → treat as "very long time" so UI shows it green.
        state.last_oom_at = ""
        state.last_oom_age_s = 999999

    # OOM count rolling
    state.oom_60m = int(sect["OOM60"][0]) if sect["OOM60"] and sect["OOM60"][0].isdigit() else 0

    # 60-min PREGEN rolling. Renamed from acpt_60m semantically: the
    # `grep -c ACCEPTED` against the miner journal counts ACCEPTED-PREGEN
    # lines, NOT validator-confirmed accepts. We surface this as the
    # box's pregen rate (miner-side queue activity) and let
    # `recompute_validator_acpts()` populate the real acpt_60m from the
    # validator events feed.
    state.pregen_60m = int(sect["ACPT60"][0]) if sect["ACPT60"] and sect["ACPT60"][0].isdigit() else 0

    # 30-min event breakdown — count everything in one pass for cheapness.
    # `acpt` here is the LOCAL pregen count from the miner's journal; it
    # lands in `pregen_30m`, not `acpt_30m` (see field-level comment on
    # BoxState for the distinction).
    acpt = rej = wm = gf = bt = oom = pregen_fail = skip = 0
    final_accept = final_reject = 0
    fresh_built = cache_fwd = prefinalized = burst = late_grace = batch_filled = 0
    last_reason = ""
    for raw in sect["EVENTS30"]:
        if "FINAL-VERDICT" in raw:
            accepted_match = re.search(r"\baccepted=(true|false|1|0)\b", raw, re.IGNORECASE)
            accepted = bool(
                accepted_match
                and accepted_match.group(1).lower() in {"true", "1"}
            )
            if accepted:
                final_accept += 1
            else:
                final_reject += 1
            reason_match = re.search(r"\breason=([^\s]+)", raw)
            window_match = re.search(r"\bwindow=(\d+)", raw)
            if reason_match:
                state.last_final_reason = reason_match.group(1)
            if window_match:
                state.last_final_window = int(window_match.group(1))
                if (
                    state.miner_submitted_this_win
                    and state.miner_window > 0
                    and state.last_final_window >= state.miner_window
                ):
                    state.miner_submitted_this_win = 0
                    if state.miner_state == "submitted":
                        state.miner_state = "waiting"
                        state.miner_inflight = 0
                        state.miner_ready = 1 if state.proc_alive else 0
        elif "reactive attempt" in raw:
            m = re.search(r"\bwindow=(\d+).*\benv=([^\s]+)", raw)
            if m:
                state.miner_state = "reactive"
                state.miner_window = int(m.group(1))
                state.miner_environment = m.group(2)
                state.miner_inflight = max(state.miner_inflight, 1)
        elif "ACCEPTED" in raw:
            acpt += 1
        elif "OutOfMemoryError" in raw:
            oom += 1
        elif "pregen GRAIL build failed" in raw:
            pregen_fail += 1
        elif "pre-skip" in raw:
            skip += 1
        elif "fresh sketch built" in raw:
            fresh_built += 1
            if "[CACHE+FWD]" in raw:
                cache_fwd += 1
            if "PREFINALIZE" in raw:
                prefinalized += 1
            if "total=" in raw:
                try:
                    state.last_fresh_total_s = float(raw.split("total=", 1)[1].split("s", 1)[0])
                except Exception:
                    pass
        elif "BURST window=" in raw:
            burst += 1
        elif "late-open grace active" in raw:
            late_grace += 1
        elif "verdicts learned:" in raw and "batch_filled_quarantine=" in raw:
            try:
                batch_filled += int(raw.split("batch_filled_quarantine=", 1)[1].split()[0])
            except Exception:
                pass
        elif "submitted window=" in raw:
            reason_match = re.search(r"\breason=([^\s]+)", raw)
            if reason_match and reason_match.group(1) == "batch_filled":
                batch_filled += 1
        elif "tune:" in raw:
            state.last_tune = raw.split("tune:", 1)[1].strip()[:280]
        elif "state=" in raw and " submitted_this_win=" in raw:
            m = re.search(
                r"state=(\w+)\s+win=(\d+)\s+valid=(\d+)\s+"
                r"(?:(?:inflight=(\d+)\s+ready=(\d+))|"
                r"(?:sketch_inflight=(\d+)\s+prebuild_inflight=(\d+)\s+ready_sketches=(\d+)))"
                r"\s+submitted_this_win=(\d+)",
                raw,
            )
            if m:
                state.miner_state = m.group(1)
                state.miner_window = int(m.group(2))
                state.miner_valid = int(m.group(3))
                if m.group(4) is not None:
                    state.miner_inflight = int(m.group(4))
                    state.miner_ready = int(m.group(5))
                else:
                    state.miner_inflight = int(m.group(6)) + int(m.group(7))
                    state.miner_ready = int(m.group(8))
                state.miner_submitted_this_win = int(m.group(9))
        elif "rejected" in raw or "ERROR" in raw:
            rej += 1
            if "window_mismatch" in raw:
                wm += 1
            if "grail_fail" in raw:
                gf += 1
            if "bad_term" in raw:
                bt += 1
            if "reason=" in raw:
                tail = raw.split("reason=", 1)[1].split(" ", 1)[0]
                last_reason = tail
        # Keep most recent 8 events for the live tail panel.
        state.recent_lines.append(raw)
    # Miner-side pregen counters — `acpt` here is the ACCEPTED-PREGEN
    # line count from the journal, NOT validator-confirmed accepts.
    state.pregen_30m = acpt
    state.final_accept_30m = final_accept
    state.final_reject_30m = final_reject
    state.pregen_ok_30m = acpt  # successful pregen runs (alias)
    state.fresh_built_30m = fresh_built
    state.cache_fwd_30m = cache_fwd
    state.prefinalized_30m = prefinalized
    state.burst_30m = burst
    state.late_grace_30m = late_grace
    state.batch_filled_30m = batch_filled
    state.checkpoint_restarts_60m = (
        int(sect["CKPT60"][0]) if sect["CKPT60"] and sect["CKPT60"][0].isdigit() else 0
    )
    # `acpt_30m` is OVERWRITTEN in `recompute_validator_acpts()` after
    # collect_box returns. We DON'T leave the old pregen count in the
    # field as a transient default because that would briefly flash
    # inflated numbers on dashboard cold start before the validator
    # events buffer fills.
    state.acpt_30m = 0
    state.rej_30m = rej
    state.window_mismatch_30m = wm
    state.grail_fail_30m = gf
    state.bad_term_30m = bt
    state.pregen_fail_30m = pregen_fail
    state.skip_30m = skip
    if last_reason:
        state.last_reject_reason = last_reason

    # Mean response time on our ACCEPTED slots (lower = better FIFO position).
    # Journal line: "ACCEPTED window=131 prompt=10 k=2 σ=0.433 t=208.22s (slots_total≈1)"
    rts: list[float] = []
    for raw in sect["EVENTS30"]:
        if "ACCEPTED" in raw and " t=" in raw:
            try:
                t_str = raw.split(" t=", 1)[1].split("s", 1)[0]
                rts.append(float(t_str))
            except Exception:
                pass
    state.mean_accept_t_s = sum(rts) / len(rts) if rts else 0.0

    # Last event timestamp for the table column.
    if sect["EVENTS30"]:
        last = sect["EVENTS30"][-1]
        bits = last.split(maxsplit=3)
        if len(bits) >= 3:
            state.last_event_at = bits[2]

    # Update trends (one sample per poll). The poller calls this every ~5s but
    # we only push to the deque every ~60s so the sparkline reflects minutes
    # not raw samples. Cheaper proxy: push every call, deque cap=20 keeps the
    # last ~100s. For a 1-hour sparkline use the rolling 60m counts directly.
    #
    # NOTE: acpt_trend is appended in `recompute_validator_acpts()` AFTER
    # this function returns — so the trend reflects validator-confirmed
    # accepts, not the always-zero default we leave in `state.acpt_30m`
    # here.
    _apply_standalone_telemetry_probe(state, sect["STANDALONE"])
    state.active_environment = state.miner_environment if state.proc_alive else ""
    if lane_resolver_enabled:
        observed_environment = state.active_environment
        final_resolution = _resolve_allowed_active_unit(state)
        if (
            final_resolution is None
            or final_resolution[0] != initial_selected_unit
            or state.active_pid != initial_selected_pid
        ):
            resolution_error = (
                state.unit_resolution_error
                or "unit_selection_changed_during_probe"
            )
            _clear_current_lane_telemetry(state)
            state.unit_resolution_error = resolution_error
            state.env_file_error = resolution_error
            state.error = resolution_error
            return
        # The final resolver intentionally clears lane-derived fields before
        # selecting. Restore the environment parsed from the exact selected
        # unit only after unit+PID exclusivity is corroborated.
        state.active_environment = observed_environment

    # Only retain trend samples after the final unit/PID exclusivity guard.
    # A lane that became ambiguous during the metrics probe is not an
    # authoritative source for either process OOMs or GPU memory.
    state.oom_trend.append(state.oom_60m)
    state.mem_trend.append(state.gpu_mem_mb // 1024)  # GB


def collect_box_snapshot(state: BoxState) -> BoxState:
    """Return one complete probe result without mutating the published state."""
    snapshot = copy.deepcopy(state)
    _collect_box_in_place(snapshot)
    return snapshot


def collect_box(state: BoxState) -> None:
    """Compatibility adapter that updates a caller-owned ``BoxState``.

    Resolver and metrics probes are sequential. Mutating the dashboard's live
    ``BoxState`` between them would expose a half-selected lane. Work on a
    private deep copy and update this object only after every probe has
    finished. Owners with concurrent readers, such as the web dashboard, use
    ``collect_box_snapshot`` and replace the owned object reference under their
    reader lock instead.
    """
    snapshot = collect_box_snapshot(state)
    state.__dict__ = snapshot.__dict__


# --- Validator + R2 polling --------------------------------------------------
@dataclass
class ValidatorState:
    state: str = "?"
    window: int = 0
    valid: int = 0
    error: str = ""
    # v2.3 surface from /state (added 2026-05). All optional / empty
    # strings if the validator is on an older build — the renderer
    # treats blank values as "—" rather than failing.
    randomness: str = ""           # current window's drand seed (hex)
    checkpoint_n: int = 0           # how many checkpoints the validator has published
    checkpoint_repo_id: str = ""    # e.g. "R0mAI/reliquary-sn-v23"
    checkpoint_revision: str = ""   # HF commit sha for the active checkpoint
    env_name: str = ""              # e.g. "openmathinstruct"
    cooldown_prompts_count: int = 0 # cardinality of the cooldown set on /state
    anchor_block: int = 0
    state_raw: dict[str, Any] = field(default_factory=dict)
    last_fetch_at: float = 0.0
    health_last_fetch_at: float = 0.0
    health_error: str = ""
    health_status: str = ""
    health_raw: dict[str, Any] = field(default_factory=dict)
    image_revision: str = ""
    protocol_version: int = 0
    generation_profile_id: str = ""
    generation_contract: dict[str, Any] = field(default_factory=dict)
    generation_contract_sha256: str = ""
    checkpoint_profile_sha256: str = ""
    app_started_at: float = 0.0
    batch_size: int = 0
    queue_depth: int = 0
    queue_depth_by_environment: dict[str, int] = field(default_factory=dict)
    admission_workers_by_environment: dict[str, int] = field(default_factory=dict)
    proof_admission_count: int = 0
    proof_admission_limit: int = 0
    proof_verification_inflight: int = 0
    proof_verification_inflight_by_environment: dict[str, int] = field(
        default_factory=dict
    )
    pending_proof_reservations: int = 0
    inflight_proof_reservations: int = 0
    # Auction-v2 production-liveness telemetry is additive. Empty mappings
    # and ``reported=False`` deliberately mean "older validator / not
    # reported", not a measured zero. This lets one dashboard binary follow
    # both sides of the validator rollout without inventing healthy samples.
    liveness_telemetry_reported: bool = False
    event_loop_lag_ms: dict[str, float] = field(default_factory=dict)
    endpoint_latency_ms: dict[str, dict[str, float]] = field(default_factory=dict)
    admission_latency_ms_by_environment: dict[
        str, dict[str, dict[str, float]]
    ] = field(default_factory=dict)
    seal_drain_by_environment: dict[str, dict[str, Any]] = field(
        default_factory=dict
    )
    current_quicknet_drand_round: int = 0
    current_window_open_drand_round: int = 0
    forced_seed_enforced: bool = False
    forced_seed_cdf_enforced: bool = False
    runtime_fingerprint: dict[str, Any] = field(default_factory=dict)
    window_environments: dict[str, dict[str, Any]] = field(default_factory=dict)
    environment_targets: dict[str, int] = field(default_factory=dict)
    prompt_sources: dict[str, dict[str, Any]] = field(default_factory=dict)
    difficulty_auction_shadow_enabled: bool = False
    difficulty_auction_shadow_environments: list[str] = field(default_factory=list)
    recent_reject_counts: dict[str, int] = field(default_factory=dict)
    archive_queue_depth: int = 0
    archive_continuity_reported: bool = False
    archive_last_uploaded_window: Optional[int] = None
    archive_last_failed_window: Optional[int] = None
    archive_uploads_succeeded_total: Optional[int] = None
    archive_upload_failures_total: Optional[int] = None
    archive_last_enqueued_window: Optional[int] = None
    archive_archives_enqueued_total: Optional[int] = None
    archive_enqueue_gaps_total: Optional[int] = None
    archive_last_enqueue_gap: Any = None
    verdicts_by_hotkey: dict[str, dict[str, Any]] = field(default_factory=dict)
    verdicts_last_fetch_at: float = 0.0
    verdicts_error: str = ""
    verdicts_warning: str = ""


# --- Chain RPC poller (slow, separate thread) ------------------------------
# Loading the metagraph takes ~5–10s; we run it on its own thread at 5-min
# cadence and the HTTP renderer reads cached state.

@dataclass
class HotkeyChainEntry:
    label: str
    hotkey: str
    uid: int
    stake: float        # τ
    emission: float     # per-block (raw value from metagraph)


@dataclass
class ChainState:
    hotkeys: list = field(default_factory=list)  # list[HotkeyChainEntry]
    total_stake: float = 0.0
    # Sum of I across our hotkeys = our share of subnet emissions (0–1).
    # Multiply by the daily subnet emission to get τ/day; we don't fetch that
    # because it's dynamic (TAO subnet weights), so we just expose the share.
    subnet_share: float = 0.0
    netuid_size: int = 0
    last_fetch_at: float = 0.0
    error: str = ""
    source_alias: str = ""
    probe_s: float = 0.0


def _chain_as_float(v) -> float:
    if hasattr(v, "item"):
        v = v.item()
    return float(v)


def _local_chain_payload() -> dict:
    if not os.environ.get("SSL_CERT_FILE") or not os.environ.get("REQUESTS_CA_BUNDLE"):
        try:
            import certifi
            os.environ.setdefault("SSL_CERT_FILE", certifi.where())
            os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())
        except Exception:
            pass

    try:
        import bittensor as bt
    except Exception as e:
        raise RuntimeError(f"import bittensor failed: {type(e).__name__}: {e}") from e

    try:
        Subtensor = getattr(bt, "Subtensor")
    except AttributeError:
        try:
            from bittensor.core.subtensor import Subtensor
        except Exception as e:
            raise RuntimeError(f"Subtensor unavailable: {type(e).__name__}: {e}") from e

    try:
        sub = Subtensor(network="finney")
        mg = sub.metagraph(netuid=NETUID)
    except Exception as e:
        raise RuntimeError(f"metagraph failed: {type(e).__name__}: {e}") from e

    hotkeys_raw = getattr(mg, "hotkeys", None)
    hotkeys = list(hotkeys_raw) if hotkeys_raw is not None else []
    stakes = getattr(mg, "S", None)
    if stakes is None:
        stakes = []
    emissions = getattr(mg, "I", None)
    if emissions is None:
        emissions = []
    try:
        n = int(_chain_as_float(getattr(mg, "n", len(hotkeys))))
    except Exception:
        n = len(hotkeys)

    out = {"hotkeys": [], "total": 0.0, "n": n, "emit": 0.0}
    for uid, hk in enumerate(hotkeys):
        if hk in OUR_SS58:
            s = _chain_as_float(stakes[uid]) if uid < len(stakes) else 0.0
            e = _chain_as_float(emissions[uid]) if uid < len(emissions) else 0.0
            out["hotkeys"].append({
                "label": OUR_SS58[hk],
                "hotkey": hk,
                "uid": uid,
                "stake": s,
                "emission": e,
            })
            out["total"] += s
            out["emit"] += e
    return out


def _apply_chain_payload(cs: ChainState, d: dict, source_alias: str, started: float) -> None:
    cs.source_alias = source_alias
    cs.hotkeys = [
        HotkeyChainEntry(
            label=h["label"], hotkey=h.get("hotkey", ""),
            uid=h["uid"], stake=h["stake"], emission=h["emission"],
        ) for h in d.get("hotkeys", [])
    ]
    cs.total_stake = d.get("total", 0.0)
    # mg.I[uid] is "incentive" in [0, 1] = fraction of subnet emission to
    # that UID. Sum across our hotkeys = our slice of the subnet's daily
    # τ payout. Multiply by absolute subnet emission (not fetched here)
    # to get actual τ/day.
    cs.subnet_share = d.get("emit", 0.0)
    cs.netuid_size = d.get("n", 0)
    cs.last_fetch_at = time.time()
    cs.error = ""
    cs.probe_s = time.time() - started


def fetch_chain_state(cs: ChainState, ssh_alias: str | None = None) -> None:
    """Pull metagraph stats for our hotkeys and cache them into ``cs``.

    The default `ssh_alias=None` tries local bittensor first, then each
    unique SSH target in FLEET. This keeps the chain panel fresh when a
    retired or moved server is still present in the config ahead of a
    working box. Override when one specific box is faster or has a
    fresher metagraph cache.

    Slow operation (5–15s); only run from a 5-min poller thread.
    """
    started = time.time()
    failures = []
    if ssh_alias is None:
        try:
            _apply_chain_payload(cs, _local_chain_payload(), "local", started)
            return
        except Exception as e:
            failures.append(f"local: {type(e).__name__}: {str(e)[:120]}")

    if ssh_alias:
        candidates = [ssh_alias]
    else:
        candidates = []
        seen = set()
        for alias, *_rest in FLEET:
            if alias and alias not in seen:
                candidates.append(alias)
                seen.add(alias)

    if not candidates:
        cs.error = "no fleet boxes configured"
        cs.source_alias = ""
        cs.probe_s = 0.0
        return
    # Heredoc-based snippet — multi-line Python is fine when piped to stdin
    # via `python - <<EOF`. Avoids the `python -c "..."` shell-escape rabbit
    # hole where `;` blocks `for` loops and `\n` doesn't reach the interpreter.
    #
    # The OUR dict and the netuid are templated from the runtime config so
    # the remote box runs against the operator's hotkeys + subnet, not a
    # hardcoded fleet. json.dumps prevents any odd character in a label or
    # hotkey from breaking the heredoc.
    # Hardened hosts commonly keep the production venv root-only, so each
    # fixed interpreter path also gets a non-interactive sudo attempt.
    our_dict_literal = json.dumps(OUR_SS58)
    netuid_literal = json.dumps(NETUID)
    snippet = f"""set -u
probe_python() {{
    "$@" - <<'PYEOF'
import json
OUR = {our_dict_literal}
NETUID = {netuid_literal}

try:
    import bittensor as bt
except Exception as e:
    raise SystemExit(f"import bittensor failed: {{type(e).__name__}}: {{e}}")

try:
    Subtensor = getattr(bt, "Subtensor")
except AttributeError:
    try:
        from bittensor.core.subtensor import Subtensor
    except Exception as e:
        raise SystemExit(f"Subtensor unavailable: {{type(e).__name__}}: {{e}}")

def as_float(v):
    if hasattr(v, "item"):
        v = v.item()
    return float(v)

try:
    sub = Subtensor(network="finney")
    mg = sub.metagraph(netuid=NETUID)
except Exception as e:
    raise SystemExit(f"metagraph failed: {{type(e).__name__}}: {{e}}")

hotkeys_raw = getattr(mg, "hotkeys", None)
hotkeys = list(hotkeys_raw) if hotkeys_raw is not None else []
stakes = getattr(mg, "S", None)
if stakes is None:
    stakes = []
emissions = getattr(mg, "I", None)
if emissions is None:
    emissions = []
try:
    n = int(as_float(getattr(mg, "n", len(hotkeys))))
except Exception:
    n = len(hotkeys)

out = {{"hotkeys": [], "total": 0.0, "n": n, "emit": 0.0}}
for uid, hk in enumerate(hotkeys):
    if hk in OUR:
        s = as_float(stakes[uid]) if uid < len(stakes) else 0.0
        e = as_float(emissions[uid]) if uid < len(emissions) else 0.0
        out["hotkeys"].append({{
          "label": OUR[hk], "hotkey": hk, "uid": uid, "stake": s, "emission": e,
        }})
        out["total"] += s
        out["emit"] += e
print(json.dumps(out, separators=(",", ":")))
PYEOF
}}

last_rc=127
for py in /opt/miner-venv-py312/bin/python3.12 /opt/miner-venv-py312/bin/python3 /opt/reliquary-venv/bin/python python3; do
    if [ -x "$py" ] || command -v "$py" >/dev/null 2>&1; then
        probe_python "$py"
        last_rc=$?
    elif sudo -n test -x "$py" >/dev/null 2>&1; then
        probe_python sudo -n "$py"
        last_rc=$?
    else
        continue
    fi
    if [ "$last_rc" -eq 0 ]; then
        exit 0
    fi
    echo "candidate $py failed rc=$last_rc" >&2
done
echo "chain probe failed on all python candidates" >&2
exit ${{last_rc:-1}}
"""
    for alias in candidates:
        try:
            source_alias = shlex.split(alias)[-1]
        except Exception:
            source_alias = alias
        cs.source_alias = source_alias

        rc, stdout, stderr = ssh_run(alias, snippet, timeout_s=30)
        if rc != 0 or not stdout.strip():
            lines = [
                ln.strip()
                for ln in (stderr or stdout or "").splitlines()
                if ln.strip()
            ]
            causes = [
                line
                for line in lines
                if line.startswith(
                    ("metagraph failed:", "import bittensor failed:", "Subtensor unavailable:")
                )
            ]
            msg_lines = causes[:4] or lines[-4:]
            msg = "; ".join(msg_lines) if msg_lines else f"chain probe failed rc={rc}"
            failures.append(f"{source_alias}: {msg[:120]}")
            continue

        try:
            last_line = [ln for ln in stdout.strip().splitlines() if ln.startswith("{")][-1]
            _apply_chain_payload(cs, json.loads(last_line), source_alias, started)
            return
        except Exception as e:
            failures.append(f"{source_alias}: parse {type(e).__name__}: {str(e)[:60]}")

    cs.error = ("all chain probes failed: " + "; ".join(failures))[:240]
    cs.probe_s = time.time() - started


# --- Baseline / drift detector ---------------------------------------------
# Persists 1-min samples of total fleet ACPT/30m to disk so we can compute a
# rolling 6h baseline that survives dashboard restarts. The /loop spec says
# "diagnose if ACC/hr drops > 30% from rolling 6h baseline" — we automate it.

# Local persistence — kept inside the repo's state/ dir (gitignored) so
# fresh clones start clean and operators can `rm -rf state/` to reset.
BASELINE_FILE = _state_file("baseline.jsonl")
BASELINE_KEEP = 60 * 24  # 24h × 60 samples = enough for 24h plotting


@dataclass
class BaselineState:
    samples: list = field(default_factory=list)  # list[(unix_ts, total_acpt_30m)]
    baseline_6h: float = 0.0
    current: int = 0
    drift_pct: float = 0.0  # negative = degrading, positive = improving
    alert: bool = False     # True if current < 0.7 × baseline_6h


def baseline_load(state: BaselineState) -> None:
    if not _bounded_regular_file(BASELINE_FILE, 1_000_000):
        return
    try:
        with open(BASELINE_FILE) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                ts, val = line.split(",", 1)
                state.samples.append((int(ts), int(val)))
    except Exception:
        pass
    # Keep at most BASELINE_KEEP latest samples
    if len(state.samples) > BASELINE_KEEP:
        state.samples = state.samples[-BASELINE_KEEP:]


def baseline_record(state: BaselineState, current: int) -> None:
    """Append a new sample and rewrite the file (cheap — capped to ~1440 lines)."""
    now = int(time.time())
    state.samples.append((now, current))
    state.current = current
    if len(state.samples) > BASELINE_KEEP:
        state.samples = state.samples[-BASELINE_KEEP:]

    # Compute 6h-rolling baseline = median of samples ≤ 6h old.
    cutoff = now - 6 * 3600
    recent = [v for ts, v in state.samples if ts >= cutoff]
    if recent:
        recent_sorted = sorted(recent)
        state.baseline_6h = recent_sorted[len(recent_sorted) // 2]
    else:
        state.baseline_6h = current

    if state.baseline_6h > 0:
        state.drift_pct = (current - state.baseline_6h) / state.baseline_6h * 100
        state.alert = current < 0.7 * state.baseline_6h
    else:
        state.drift_pct = 0.0
        state.alert = False

    # Persist (rewrite cheap; we cap rows).
    try:
        payload = "".join(f"{ts},{v}\n" for ts, v in state.samples).encode()
        _private_atomic_write(BASELINE_FILE, payload)
    except Exception:
        pass


# --- Network RTT prober ----------------------------------------------------
# How fast can each box reach the validator? Critical because window_mismatch
# is partly latency-driven. Run on a slower 60s thread.

@dataclass
class RTTState:
    # ms; key = box label or "mac"
    rtt_ms: dict = field(default_factory=dict)
    last_fetch_at: float = 0.0


def probe_rtt(boxes: list, vs_url: str, rtt: RTTState) -> None:
    # mac → validator. Use GET (HEAD sometimes 405s on FastAPI). 6s timeout —
    # validator is occasionally 2–3s under load.
    if httpx:
        try:
            t0 = time.time()
            _http_json("state", timeout=6.0)
            rtt.rtt_ms["mac"] = int((time.time() - t0) * 1000)
        except Exception:
            rtt.rtt_ms["mac"] = -1

    # Each box → validator (curl timed inside the box). Run in parallel.
    def _probe_box(b):
        # curl's -w gives time_total; we also capture http_code so a slow
        # but successful response counts (3s validator response shouldn't
        # appear as "down").
        state_url = shlex.quote(f"{vs_url.rstrip('/')}/state")
        cmd = (f"curl -s -o /dev/null -w '%{{time_total}} %{{http_code}}' "
               f"--max-time 6 {state_url} 2>/dev/null")
        rc, out, _ = ssh_run(b.alias, cmd, timeout_s=10)
        if rc == 0 and out.strip():
            try:
                parts = out.strip().split()
                t = float(parts[0])
                code = parts[1] if len(parts) > 1 else "000"
                rtt.rtt_ms[b.label] = int(t * 1000) if code == "200" else -1
            except Exception:
                rtt.rtt_ms[b.label] = -1
        else:
            rtt.rtt_ms[b.label] = -1

    threads = [threading.Thread(target=_probe_box, args=(b,)) for b in boxes]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    rtt.last_fetch_at = time.time()


def _validator_ssh_args(remote_command: str, *, server_alive: bool = False) -> list[str]:
    """Build one correctly-ordered validator SSH command.

    OpenSSH stops parsing client options once it reaches the destination.
    Keeping every ``-o``, ``-i`` and ``-p`` before ``VALIDATOR_SSH`` avoids
    silently executing ``-p`` as part of the remote command (the old tail and
    deployment probes did exactly that).
    """
    cmd = [
        "ssh",
        "-o", "ConnectTimeout=4",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
    ]
    if server_alive:
        cmd.extend(["-o", "ServerAliveInterval=30"])
    if SSH_KEY:
        cmd.extend(["-i", os.path.expanduser(SSH_KEY)])
    cmd.extend(["-p", str(VALIDATOR_PORT), VALIDATOR_SSH, remote_command])
    return cmd


def _validator_json_via_ssh(path: str) -> dict | None:
    """Fetch a validator JSON endpoint from inside its host."""
    if not VALIDATOR_SSH:
        return None
    url = f"http://127.0.0.1:8080/{path.lstrip('/')}"
    cmd = _validator_ssh_args(f"curl -fsS --max-time 5 {shlex.quote(url)}")
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=9)
        if r.returncode == 0 and r.stdout.strip():
            payload = json.loads(r.stdout)
            return payload if isinstance(payload, dict) else None
    except Exception:
        pass
    return None


def _validator_state_via_ssh() -> dict | None:
    """Backward-compatible wrapper used by the state poller and tests."""
    return _validator_json_via_ssh("state")


_http_client_instance = None
_http_client_lock = threading.Lock()
_http_cache_lock = threading.Lock()
_http_conditional_cache: dict[str, tuple[str, dict[str, Any]]] = {}
_validator_retry_after_until = 0.0
_MAX_VALIDATOR_RESPONSE_BYTES = 32 * 1024 * 1024


def _http_client():
    """Return one process-wide keep-alive client for read-only upstreams."""
    global _http_client_instance
    if httpx is None:
        raise RuntimeError("httpx missing")
    if _http_client_instance is None:
        with _http_client_lock:
            if _http_client_instance is None:
                _http_client_instance = httpx.Client(
                    timeout=httpx.Timeout(8.0, connect=4.0),
                    limits=httpx.Limits(
                        max_connections=16,
                        max_keepalive_connections=8,
                        keepalive_expiry=30.0,
                    ),
                    follow_redirects=False,
                )
    return _http_client_instance


def _retry_after_seconds(response: Any) -> float:
    raw = str(getattr(response, "headers", {}).get("retry-after", "") or "").strip()
    if not raw:
        return 0.0
    try:
        return max(0.0, min(float(raw), 900.0))
    except (TypeError, ValueError):
        pass
    try:
        parsed = parsedate_to_datetime(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(
            0.0,
            min((parsed - datetime.now(timezone.utc)).total_seconds(), 900.0),
        )
    except (TypeError, ValueError, OverflowError):
        return 0.0


def validator_retry_after_remaining(*, now: float | None = None) -> float:
    """Seconds left in the latest validator-provided Retry-After window."""
    current = time.time() if now is None else float(now)
    return max(0.0, _validator_retry_after_until - current)


def _http_json(path: str, *, timeout: float = 8.0) -> dict:
    global _validator_retry_after_until
    normalized_path = path.lstrip("/")
    cacheable = normalized_path in {"state", "health"}
    headers: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": "reliquary-fleet",
    }
    cached_payload: dict[str, Any] | None = None
    if cacheable:
        with _http_cache_lock:
            cached = _http_conditional_cache.get(normalized_path)
        if cached:
            etag, cached_payload = cached
            if etag:
                headers["If-None-Match"] = etag
    response = _http_client().get(
        f"{VALIDATOR_URL.rstrip('/')}/{normalized_path}",
        timeout=timeout,
        headers=headers,
    )
    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code == 304 and cached_payload is not None:
        return dict(cached_payload)
    if status_code in {429, 503}:
        retry_after = _retry_after_seconds(response)
        if retry_after > 0:
            _validator_retry_after_until = max(
                _validator_retry_after_until,
                time.time() + retry_after,
            )
    raise_for_status = getattr(response, "raise_for_status", None)
    if callable(raise_for_status):
        raise_for_status()
    content = getattr(response, "content", b"")
    if isinstance(content, (bytes, bytearray)) and len(content) > _MAX_VALIDATOR_RESPONSE_BYTES:
        raise ValueError("validator response exceeds 32 MiB")
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("validator response is not an object")
    if cacheable:
        etag = str(getattr(response, "headers", {}).get("etag", "") or "")
        with _http_cache_lock:
            _http_conditional_cache[normalized_path] = (etag, dict(payload))
    return payload


def _apply_validator_state(vs: ValidatorState, d: dict, *, source: str) -> None:
    """Apply a successful `/state` payload while preserving omitted fields.

    An explicit null checkpoint identity is meaningful: it is how a validator
    advertises an intentional base-model reset at ``checkpoint_n=0``.  Clear a
    previously observed revision in that case, but retain it when an older or
    sparse endpoint omits the key entirely.
    """
    vs.state_raw = dict(d)
    new_state = d.get("state", "?")
    try:
        new_window = int(d.get("window_n") or d.get("current_round") or 0)
    except Exception:
        new_window = 0
    if new_window <= 0:
        vs.error = "stale"
        return

    vs.state = str(new_state or "?")
    vs.window = new_window
    try:
        vs.valid = int(d.get("valid_submissions") or 0)
    except Exception:
        pass
    randomness = d.get("randomness")
    if isinstance(randomness, str):
        vs.randomness = randomness
    for attr, key in (
        ("checkpoint_n", "checkpoint_n"),
        ("anchor_block", "anchor_block"),
    ):
        try:
            value = d.get(key)
            if value is not None:
                setattr(vs, attr, int(value))
        except Exception:
            pass
    for attr, key in (
        ("checkpoint_repo_id", "checkpoint_repo_id"),
        ("checkpoint_revision", "checkpoint_revision"),
    ):
        if key not in d:
            continue
        value = d[key]
        if value is None:
            setattr(vs, attr, "")
        elif isinstance(value, str):
            setattr(vs, attr, value)
    protocol = d.get("protocol_version")
    if isinstance(protocol, int) and not isinstance(protocol, bool) and protocol >= 2:
        vs.protocol_version = protocol
    profile_id = d.get("generation_profile_id")
    if isinstance(profile_id, str):
        vs.generation_profile_id = profile_id
    generation_contract = d.get("generation_contract")
    if isinstance(generation_contract, dict):
        try:
            encoded_contract = json.dumps(
                generation_contract,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError):
            pass
        else:
            vs.generation_contract = dict(generation_contract)
            vs.generation_contract_sha256 = hashlib.sha256(
                encoded_contract
            ).hexdigest()
    checkpoint_profile = d.get("checkpoint_profile_sha256")
    if isinstance(checkpoint_profile, str):
        vs.checkpoint_profile_sha256 = checkpoint_profile
    env_name = d.get("environment_name") or d.get("env_name")
    if isinstance(env_name, str):
        vs.env_name = env_name
    cooldown = d.get("cooldown_prompts")
    if isinstance(cooldown, list):
        vs.cooldown_prompts_count = len(cooldown)
    vs.last_fetch_at = time.time()
    vs.error = "ssh-state" if source == "ssh" else ""


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


_LATENCY_PERCENTILES = ("p50", "p95", "p99", "max")


def _optional_nonnegative_int(value: Any) -> Optional[int]:
    """Return a trustworthy counter or ``None`` for absent/malformed data."""
    if isinstance(value, bool):
        return None
    if isinstance(value, float) and (
        not math.isfinite(value) or not value.is_integer()
    ):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed >= 0 else None


def _optional_nonnegative_float(value: Any) -> Optional[float]:
    """Return one finite latency/duration sample without coercing booleans."""
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0 else None


def _normalize_counter_map(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, int] = {}
    for name, raw in value.items():
        parsed = _optional_nonnegative_int(raw)
        if parsed is not None:
            normalized[str(name)] = parsed
    return normalized


def _normalize_latency_percentiles(value: Any) -> dict[str, float]:
    """Normalize the validator's bounded p50/p95/p99/max latency shape."""
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, float] = {}
    for name in _LATENCY_PERCENTILES:
        if name not in value:
            continue
        parsed = _optional_nonnegative_float(value.get(name))
        if parsed is not None:
            normalized[name] = parsed
    return normalized


def _normalize_latency_map(value: Any) -> dict[str, dict[str, float]]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, dict[str, float]] = {}
    for name, raw in value.items():
        percentiles = _normalize_latency_percentiles(raw)
        if percentiles:
            normalized[str(name)] = percentiles
    return normalized


def _normalize_admission_latency_map(
    value: Any,
) -> dict[str, dict[str, dict[str, float]]]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, dict[str, dict[str, float]]] = {}
    for environment, raw_metrics in value.items():
        metrics = _normalize_latency_map(raw_metrics)
        if metrics:
            normalized[str(environment)] = metrics
    return normalized


def _normalize_seal_drain(value: Any) -> dict[str, Any]:
    """Normalize one immutable seal snapshot without fabricating defaults."""
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, Any] = {}
    elapsed = _optional_nonnegative_float(value.get("elapsed_seconds"))
    if elapsed is not None:
        normalized["elapsed_seconds"] = elapsed
    if isinstance(value.get("timed_out"), bool):
        normalized["timed_out"] = value["timed_out"]
    for name in (
        "queue_depth_at_snapshot",
        "inflight_workers_at_snapshot",
        "pending_reservations_at_snapshot",
        "inflight_reservations_at_snapshot",
        # Retain forward-compatible counters if a newer liveness build emits
        # the names from the production design directly.
        "completed_during_drain",
        "dropped_at_snapshot",
        "completed_during_drain_count",
        "dropped_at_snapshot_count",
    ):
        if name not in value:
            continue
        parsed = _optional_nonnegative_int(value.get(name))
        if parsed is not None:
            normalized[name] = parsed
    return normalized


def _normalize_archive_gap(value: Any) -> Any:
    """Keep the validator's JSON gap identity while rejecting odd objects."""
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, list):
        return list(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (str, int)) and not isinstance(value, bool):
        return value
    return None


def _apply_validator_health(vs: ValidatorState, d: dict) -> None:
    """Normalize current `/health` while retaining the complete payload."""
    vs.health_raw = dict(d)
    vs.health_status = str(d.get("status") or "unknown")
    vs.image_revision = str(d.get("image_revision") or vs.image_revision)
    protocol = d.get("protocol_version")
    if isinstance(protocol, int) and not isinstance(protocol, bool) and protocol >= 2:
        vs.protocol_version = protocol
    profile_id = d.get("generation_profile_id")
    if isinstance(profile_id, str):
        vs.generation_profile_id = profile_id
    generation_contract = d.get("generation_contract")
    if isinstance(generation_contract, dict):
        try:
            encoded_contract = json.dumps(
                generation_contract,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError):
            pass
        else:
            vs.generation_contract = dict(generation_contract)
            vs.generation_contract_sha256 = hashlib.sha256(
                encoded_contract
            ).hexdigest()
    checkpoint_profile = d.get("checkpoint_profile_sha256")
    if isinstance(checkpoint_profile, str):
        vs.checkpoint_profile_sha256 = checkpoint_profile
    vs.app_started_at = _as_float(d.get("app_started_at"), vs.app_started_at)
    vs.batch_size = _as_int(d.get("batch_size"), vs.batch_size)
    vs.queue_depth = _as_int(d.get("queue_depth"), vs.queue_depth)
    vs.queue_depth_by_environment = _normalize_counter_map(
        d.get("queue_depth_by_environment")
    )
    vs.admission_workers_by_environment = _normalize_counter_map(
        d.get("admission_workers_by_environment")
    )
    vs.proof_admission_count = _as_int(
        d.get("proof_admission_count"), vs.proof_admission_count
    )
    vs.proof_admission_limit = _as_int(
        d.get("post_trigger_proof_admission_limit"), vs.proof_admission_limit
    )
    vs.proof_verification_inflight = _as_int(
        d.get("proof_verification_inflight"), vs.proof_verification_inflight
    )
    vs.proof_verification_inflight_by_environment = _normalize_counter_map(
        d.get("proof_verification_inflight_by_environment")
    )
    vs.pending_proof_reservations = _as_int(
        d.get("pending_proof_reservations"), vs.pending_proof_reservations
    )
    vs.inflight_proof_reservations = _as_int(
        d.get("inflight_proof_reservations"), vs.inflight_proof_reservations
    )
    liveness_keys = {
        "event_loop_lag_ms",
        "endpoint_latency_ms",
        "admission_latency_ms_by_environment",
        "queue_depth_by_environment",
        "admission_workers_by_environment",
        "proof_verification_inflight_by_environment",
    }
    vs.liveness_telemetry_reported = any(key in d for key in liveness_keys)
    vs.event_loop_lag_ms = _normalize_latency_percentiles(
        d.get("event_loop_lag_ms")
    )
    vs.endpoint_latency_ms = _normalize_latency_map(
        d.get("endpoint_latency_ms")
    )
    vs.admission_latency_ms_by_environment = _normalize_admission_latency_map(
        d.get("admission_latency_ms_by_environment")
    )
    vs.current_quicknet_drand_round = _as_int(
        d.get("current_quicknet_drand_round"), vs.current_quicknet_drand_round
    )
    vs.current_window_open_drand_round = _as_int(
        d.get("current_window_open_drand_round"),
        vs.current_window_open_drand_round,
    )
    vs.forced_seed_enforced = bool(d.get("forced_seed_enforced"))
    vs.forced_seed_cdf_enforced = bool(d.get("forced_seed_cdf_enforced"))
    runtime = d.get("runtime_fingerprint")
    if isinstance(runtime, dict):
        vs.runtime_fingerprint = dict(runtime)
    envs = d.get("window_environments")
    if isinstance(envs, dict):
        vs.window_environments = {
            str(name): dict(row) for name, row in envs.items()
            if isinstance(row, dict)
        }
    else:
        vs.window_environments = {}
    vs.seal_drain_by_environment = {}
    for name, row in vs.window_environments.items():
        drain = _normalize_seal_drain(row.get("auction_seal_drain"))
        if drain:
            vs.seal_drain_by_environment[name] = drain
    targets = d.get("training_accumulator_targets")
    if isinstance(targets, dict):
        vs.environment_targets = {
            str(name): _as_int(value) for name, value in targets.items()
        }
    sources = d.get("prompt_sources")
    if isinstance(sources, dict):
        vs.prompt_sources = {
            str(name): dict(row) for name, row in sources.items()
            if isinstance(row, dict)
        }
    vs.difficulty_auction_shadow_enabled = bool(
        d.get("difficulty_auction_shadow_enabled")
    )
    difficulty_envs = d.get("difficulty_auction_shadow_environments")
    if isinstance(difficulty_envs, list):
        vs.difficulty_auction_shadow_environments = [str(x) for x in difficulty_envs]
    reject_counts = d.get("recent_reject_counts_by_reason")
    if isinstance(reject_counts, dict):
        vs.recent_reject_counts = {
            str(reason): _as_int(count) for reason, count in reject_counts.items()
        }
    vs.archive_queue_depth = _as_int(
        d.get("archive_queue_depth"), vs.archive_queue_depth
    )
    archive_continuity_keys = {
        "archive_last_uploaded_window",
        "archive_last_failed_window",
        "archive_uploads_succeeded_total",
        "archive_upload_failures_total",
        "archive_last_enqueued_window",
        "archive_archives_enqueued_total",
        "archive_enqueue_gaps_total",
        "archive_last_enqueue_gap",
    }
    vs.archive_continuity_reported = any(
        key in d for key in archive_continuity_keys
    )
    for attr, key in (
        ("archive_last_uploaded_window", "archive_last_uploaded_window"),
        ("archive_last_failed_window", "archive_last_failed_window"),
        ("archive_uploads_succeeded_total", "archive_uploads_succeeded_total"),
        ("archive_upload_failures_total", "archive_upload_failures_total"),
        ("archive_last_enqueued_window", "archive_last_enqueued_window"),
        ("archive_archives_enqueued_total", "archive_archives_enqueued_total"),
        ("archive_enqueue_gaps_total", "archive_enqueue_gaps_total"),
    ):
        setattr(vs, attr, _optional_nonnegative_int(d.get(key)))
    vs.archive_last_enqueue_gap = _normalize_archive_gap(
        d.get("archive_last_enqueue_gap")
    )
    if "checkpoint_n" in d and d.get("checkpoint_n") is not None:
        vs.checkpoint_n = _as_int(d.get("checkpoint_n"), vs.checkpoint_n)
    for attr, key in (
        ("checkpoint_repo_id", "checkpoint_repo_id"),
        ("checkpoint_revision", "checkpoint_revision"),
    ):
        if key not in d:
            continue
        value = d[key]
        if value is None:
            setattr(vs, attr, "")
        elif isinstance(value, str):
            setattr(vs, attr, value)
    health_window = _as_int(d.get("current_window_n"))
    if health_window > 0 and health_window >= vs.window:
        vs.window = health_window
        vs.state = str(d.get("current_validator_state") or vs.state)
        vs.valid = _as_int(d.get("valid_submissions_count"), vs.valid)
    vs.health_last_fetch_at = time.time()
    vs.health_error = ""


def summarize_verdicts(verdicts: list[dict], *, now: float | None = None) -> dict[str, Any]:
    """Summarize admission and seal-final verdict stages without conflation.

    Auction validators publish two records for one Merkle root: pool admission,
    then a seal-final record carrying ``selected_for_batch`` and ``rewarded``.
    The latter must not inflate the pool admission counters. Legacy single-stage
    records remain both an admission outcome and, when present, a final outcome.
    """
    now = time.time() if now is None else now
    rows = [dict(v) for v in verdicts if isinstance(v, dict)]
    rows.sort(key=lambda v: _as_float(v.get("ts")))
    summary: dict[str, Any] = {
        "accepted_30m": 0, "accepted_60m": 0,
        "rejected_30m": 0, "rejected_60m": 0,
        "finalized_30m": 0, "finalized_60m": 0,
        "final_rejected_30m": 0, "final_rejected_60m": 0,
        "selected_30m": 0, "selected_60m": 0,
        "rewarded_30m": 0, "rewarded_60m": 0,
        "rewarded_not_selected_30m": 0, "rewarded_not_selected_60m": 0,
        "fractional_rewarded_30m": 0, "fractional_rewarded_60m": 0,
        "reward_amount_30m": None, "reward_amount_60m": None,
        "reward_amount_observations_30m": 0,
        "reward_amount_observations_60m": 0,
        "effective_full_slots_30m": None,
        "effective_full_slots_60m": None,
        "effective_full_slots_observations_30m": 0,
        "effective_full_slots_observations_60m": 0,
        "full_slot_reward": 0.0625,
        "reason_counts_30m": {}, "final_reason_counts_30m": {},
        "by_environment": {},
        "last": rows[-1] if rows else {}, "recent": rows[-200:],
    }
    admissions: dict[str, dict] = {}
    finals: dict[str, dict] = {}
    for index, verdict in enumerate(rows):
        root = str(verdict.get("merkle_root") or "").strip().lower()
        window_value = verdict.get("window_n")
        if isinstance(window_value, int) and not isinstance(window_value, bool):
            window_n = window_value if window_value >= 0 else None
        elif isinstance(window_value, str) and window_value.strip().isdigit():
            window_n = int(window_value.strip())
        else:
            window_n = None
        # Merkle roots can legitimately recur in a later window. The feed is
        # already per-hotkey, so window + normalized root is the lifecycle
        # identity; incomplete legacy rows must remain unique rather than
        # accidentally collapsing into one another.
        key = (
            f"{window_n}:{root}"
            if window_n is not None and root
            else f"legacy-row:{index}"
        )
        has_final_fields = (
            verdict.get("selected_for_batch") is not None
            or verdict.get("rewarded") is not None
        )
        is_auction_final = (
            verdict.get("accepted_into_pool") is not None
            and has_final_fields
        )
        if is_auction_final:
            finals[key] = verdict
            continue
        # Assignment deliberately keeps the newest duplicate for one lifecycle.
        admissions[key] = verdict
        if has_final_fields:
            # Backward compatibility for pre-auction, single-stage verdicts.
            finals[f"legacy-final:{key}"] = verdict

    admission_reasons: Counter = Counter()
    final_reasons: Counter = Counter()
    by_env: dict[str, Counter] = {}
    reward_amount_sums = {"30m": 0.0, "60m": 0.0}
    effective_slot_sums = {"30m": 0.0, "60m": 0.0}

    def optional_nonnegative_number(value: Any) -> float | None:
        if isinstance(value, bool) or value is None:
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return parsed if math.isfinite(parsed) and parsed >= 0.0 else None

    for verdict in admissions.values():
        ts = _as_float(verdict.get("ts"))
        age = now - ts
        if ts <= 0 or age < -60 or age > 3600:
            continue
        accepted = bool(verdict.get("accepted"))
        prefix = "accepted" if accepted else "rejected"
        summary[f"{prefix}_60m"] += 1
        env_name = str(verdict.get("env_name") or "unknown")
        env_counter = by_env.setdefault(env_name, Counter())
        env_counter[f"{prefix}_60m"] += 1
        if age <= 1800:
            summary[f"{prefix}_30m"] += 1
            env_counter[f"{prefix}_30m"] += 1
            if not accepted:
                admission_reasons[str(
                    verdict.get("reject_reason")
                    or verdict.get("reason")
                    or "unknown"
                )] += 1

    for verdict in finals.values():
        ts = _as_float(verdict.get("ts"))
        age = now - ts
        if ts <= 0 or age < -60 or age > 3600:
            continue
        accepted = bool(verdict.get("accepted"))
        env_name = str(verdict.get("env_name") or "unknown")
        env_counter = by_env.setdefault(env_name, Counter())
        summary["finalized_60m"] += 1
        env_counter["finalized_60m"] += 1
        if not accepted:
            summary["final_rejected_60m"] += 1
            env_counter["final_rejected_60m"] += 1
        selected = verdict.get("selected_for_batch") is True
        rewarded = verdict.get("rewarded") is True
        if selected:
            summary["selected_60m"] += 1
            env_counter["selected_60m"] += 1
        if rewarded:
            summary["rewarded_60m"] += 1
            env_counter["rewarded_60m"] += 1
        if rewarded and not selected:
            summary["rewarded_not_selected_60m"] += 1
            env_counter["rewarded_not_selected_60m"] += 1
        reward_amount = optional_nonnegative_number(verdict.get("reward_amount"))
        effective_slots = optional_nonnegative_number(
            verdict.get("effective_full_slots")
        )
        if effective_slots is None and reward_amount is not None:
            effective_slots = reward_amount / summary["full_slot_reward"]
        if reward_amount is not None:
            summary["reward_amount_observations_60m"] += 1
            env_counter["reward_amount_observations_60m"] += 1
            reward_amount_sums["60m"] += reward_amount
            env_counter["reward_amount_60m"] += reward_amount
            if rewarded and 0.0 < reward_amount < summary["full_slot_reward"] - 1e-12:
                summary["fractional_rewarded_60m"] += 1
                env_counter["fractional_rewarded_60m"] += 1
        if effective_slots is not None:
            summary["effective_full_slots_observations_60m"] += 1
            env_counter["effective_full_slots_observations_60m"] += 1
            effective_slot_sums["60m"] += effective_slots
            env_counter["effective_full_slots_60m"] += effective_slots
        if age <= 1800:
            summary["finalized_30m"] += 1
            env_counter["finalized_30m"] += 1
            if not accepted:
                summary["final_rejected_30m"] += 1
                env_counter["final_rejected_30m"] += 1
                final_reasons[str(
                    verdict.get("reject_reason")
                    or verdict.get("reason")
                    or "unknown"
                )] += 1
            if selected:
                summary["selected_30m"] += 1
                env_counter["selected_30m"] += 1
            if rewarded:
                summary["rewarded_30m"] += 1
                env_counter["rewarded_30m"] += 1
            if rewarded and not selected:
                summary["rewarded_not_selected_30m"] += 1
                env_counter["rewarded_not_selected_30m"] += 1
            if reward_amount is not None:
                summary["reward_amount_observations_30m"] += 1
                env_counter["reward_amount_observations_30m"] += 1
                reward_amount_sums["30m"] += reward_amount
                env_counter["reward_amount_30m"] += reward_amount
                if rewarded and 0.0 < reward_amount < summary["full_slot_reward"] - 1e-12:
                    summary["fractional_rewarded_30m"] += 1
                    env_counter["fractional_rewarded_30m"] += 1
            if effective_slots is not None:
                summary["effective_full_slots_observations_30m"] += 1
                env_counter["effective_full_slots_observations_30m"] += 1
                effective_slot_sums["30m"] += effective_slots
                env_counter["effective_full_slots_30m"] += effective_slots

    for period in ("30m", "60m"):
        if summary[f"reward_amount_observations_{period}"]:
            summary[f"reward_amount_{period}"] = reward_amount_sums[period]
        if summary[f"effective_full_slots_observations_{period}"]:
            summary[f"effective_full_slots_{period}"] = effective_slot_sums[period]

    summary["reason_counts_30m"] = dict(admission_reasons)
    summary["final_reason_counts_30m"] = dict(final_reasons)
    summary["by_environment"] = {
        name: dict(counts) for name, counts in by_env.items()
    }
    return summary


_verdict_cache_lock = threading.Lock()
_verdict_rows_by_hotkey: dict[str, list[dict[str, Any]]] = {}
_verdict_cursor_by_hotkey: dict[str, float] = {}
_VERDICT_OVERLAP_SECONDS = 120.0
_VERDICT_CACHE_LIMIT = 5_000


def _selected_verdict_hotkeys() -> tuple[list[str], list[str]]:
    """Prioritize configured miner keys, then cap additional watched keys."""
    configured = [
        str(row[1])
        for row in FLEET
        if len(row) > 1 and _is_ss58_hotkey(row[1])
    ]
    watched = [str(hotkey) for hotkey in OUR_SS58 if _is_ss58_hotkey(hotkey)]
    ordered = list(dict.fromkeys([*configured, *watched]))
    limit = max(1, int(VALIDATOR_MAX_VERDICT_HOTKEYS))
    return ordered[:limit], ordered[limit:]


def verdict_rows_for_hotkey(hotkey: str) -> list[dict[str, Any]]:
    """Thread-safe copy of cached public verdict rows for UI reconciliation."""

    if not _is_ss58_hotkey(hotkey):
        return []
    with _verdict_cache_lock:
        return [dict(row) for row in _verdict_rows_by_hotkey.get(hotkey, [])]


def _verdict_identity(row: dict[str, Any]) -> str:
    try:
        return json.dumps(
            row,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            default=str,
        )
    except (TypeError, ValueError):
        return repr(sorted((str(key), repr(value)) for key, value in row.items()))


def _merge_verdict_rows(
    existing: list[dict[str, Any]],
    incoming: list[Any],
    *,
    now: float,
) -> list[dict[str, Any]]:
    cutoff = now - 3660.0
    future_limit = now + 60.0
    merged: dict[str, dict[str, Any]] = {}
    for raw in [*existing, *incoming]:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        ts = _as_float(row.get("ts"))
        if ts > 0 and not cutoff <= ts <= future_limit:
            continue
        merged[_verdict_identity(row)] = row
    rows = list(merged.values())
    rows.sort(key=lambda row: _as_float(row.get("ts")))
    return rows[-_VERDICT_CACHE_LIMIT:]


def _fetch_validator_verdicts(vs: ValidatorState) -> None:
    hotkeys, omitted = _selected_verdict_hotkeys()
    vs.verdicts_warning = (
        f"watch limit: polling {len(hotkeys)} of {len(hotkeys) + len(omitted)} hotkeys"
        if omitted
        else ""
    )
    if not hotkeys:
        vs.verdicts_error = "no hotkeys configured"
        return
    now = time.time()
    updated = {
        hotkey: summary
        for hotkey, summary in vs.verdicts_by_hotkey.items()
        if hotkey in hotkeys
    }
    errors: list[str] = []
    successes = 0
    for hk in hotkeys:
        try:
            with _verdict_cache_lock:
                existing = list(_verdict_rows_by_hotkey.get(hk, []))
                cursor = float(_verdict_cursor_by_hotkey.get(hk, 0.0) or 0.0)
            if (
                not existing
                or cursor <= 0
                or cursor < now - 7200.0
                or cursor > now + 60.0
            ):
                since = now - 3600.0
            else:
                since = max(now - 3600.0, cursor - _VERDICT_OVERLAP_SECONDS)
            payload = _http_json(
                f"verdicts/{hk}?since={since:.3f}", timeout=6.0
            )
            rows = payload.get("verdicts")
            if not isinstance(rows, list):
                raise ValueError("verdicts missing")
            merged = _merge_verdict_rows(existing, rows, now=now)
            with _verdict_cache_lock:
                _verdict_rows_by_hotkey[hk] = merged
                # Advancing to request time plus a two-minute overlap bounds
                # traffic without losing verdicts published slightly late.
                _verdict_cursor_by_hotkey[hk] = now
            summary = summarize_verdicts(merged, now=now)
            summary["_fetched_at"] = now
            updated[hk] = summary
            successes += 1
        except Exception as exc:
            errors.append(f"{hk[:12]}:{type(exc).__name__}")
    vs.verdicts_by_hotkey = updated
    vs.verdicts_error = "; ".join(errors)[:240]
    if successes:
        vs.verdicts_last_fetch_at = now


def fetch_validator(
    vs: ValidatorState,
    *,
    poll_state: bool = True,
    poll_health: bool = True,
    poll_verdicts: bool = True,
) -> None:
    """Poll independent state, health and final-verdict authority surfaces."""
    if not httpx:
        vs.error = "httpx missing"
        vs.health_error = "httpx missing"
        vs.verdicts_error = "httpx missing"
        return

    if poll_state:
        try:
            state_payload = _http_json("state", timeout=8.0)
            _apply_validator_state(vs, state_payload, source="http")
        except Exception as exc:
            state_payload = _validator_state_via_ssh()
            if state_payload is None:
                vs.error = type(exc).__name__
            else:
                _apply_validator_state(vs, state_payload, source="ssh")

    if poll_health and validator_retry_after_remaining() <= 0:
        try:
            _apply_validator_health(vs, _http_json("health", timeout=8.0))
        except Exception as exc:
            # Preserve last-good health fields; freshness/error communicates
            # that they are no longer authoritative.
            vs.health_error = type(exc).__name__

    if poll_verdicts and validator_retry_after_remaining() <= 0:
        _fetch_validator_verdicts(vs)


# ---------------------------------------------------------------------------
# Validator-side event tail (live `docker logs reliquary-trainer` stream)
# ---------------------------------------------------------------------------
# The miner journal events panel shows the miner-side perspective: every
# ACCEPTED-PREGEN line is what the MINER logged after the validator's HTTP
# layer returned ``accepted=true`` on enqueue. That's PROVISIONAL — the
# validator's worker still needs to dequeue and actually run GRAIL verify
# before the submission counts toward emission.
#
# This tail panel pulls VALIDATOR-side events directly via SSH + docker
# logs. Each accepted/rejected line here is a worker-confirmed outcome,
# the closest thing to "what's actually earning emission right now". When
# you see ``accepted prompt=X hotkey=5D76Wk...`` here it means the
# validator's _submit_worker ran GRAIL on that submission, the result
# passed, and it was appended to the active batcher's ``_valid``.


@dataclass
class ValidatorEvent:
    ts: str         # "07:54:28" — the wall-clock time from the validator's log line
    ts_epoch: float = 0.0  # seconds since unix epoch for time-window aggregation
    msg: str = ""   # the rest of the log line after the timestamp
    ours: bool = False  # True iff the hotkey matches one of OUR_SS58
    hotkey12: str = ""  # 12-char prefix for compact display
    kind: str = ""  # "accept" | "reject" | "selected" | "reward" |
                    # "late_drop" | "seal" | "fail" |
                    # "worker_fail" | "window_timeout" |
                    # "randomness_retry" | "randomness_ok"
    reject_reason: str = ""  # extracted reason= field on rejects, empty otherwise
    prompt_idx: int = -1
    drand_round: int = 0
    window_n: int = 0
    env_name: str = ""
    reward_amount: float = 0.0


# Bumped from 1500 → 6000 (2026-05): the new /logs page lets operators
# scroll back through the entire buffer with filters, so a 30 min
# rundown ceiling isn't enough — we want at least an hour of raw
# events available for forensic inspection. At ~50 events/min worst
# case (busy window + late_drops fully captured) 6000 entries covers
# ~2 h of history. Memory cost: ~6000 × ~300 B/entry ≈ 1.8 MB, well
# inside the host's RAM budget.
_validator_events: deque = deque(maxlen=6000)
_validator_events_lock = threading.Lock()
_validator_tail_proc: Optional[subprocess.Popen] = None

# Dedup state for the tail loop. When the SSH `docker logs -f --since 1m`
# subprocess dies and reconnects (network blip, validator restart, etc.)
# the new connection's --since 1m re-emits any log line emitted in the
# previous minute — which we've usually already captured. Without
# deduping, every reconnect inflates the accept/reject counters and
# the throughput baselines compute against ghost events.
#
# Key shape: (ts, msg). `ts` is per-second so we still admit two
# legitimately-distinct events that happened in the same second with
# different messages. The set is bounded by the deque maxlen so memory
# is O(maxlen) total.
_seen_event_keys: deque = deque(maxlen=1500)
_seen_event_keys_set: set[tuple[str, str]] = set()

# The validator has used three seal markers across deployed revisions.  Keep
# the old single-batcher message while also accepting the current
# multi-environment and liveness-breaker forms.
_VALIDATOR_SEAL_MARKERS = (
    " sealed (",
    "batcher(s) sealed",
    "sealed by liveness breaker",
)

_VALIDATOR_TAIL_GREP_EXPR = (
    "accepted prompt=|rejected prompt=|"
    "validator_submit_lifecycle|"
    "dropping late submission|"
    "Window iteration failed|sealed \\(B valid received\\)|"
    "batcher\\(s\\) sealed|sealed by liveness breaker|"
    "submission worker|submission_worker|"
    "timed out|_derive_randomness|derived on attempt"
)


def _classify_validator_line(line: str) -> Optional[ValidatorEvent]:
    """Parse one validator docker-logs line into a typed event.

    Lines look like (with the docker --timestamps prefix stripped by the
    validator's own logger):
      ``2026-05-11 07:54:28 | reliquary.validator.server | INFO | accepted prompt=861 hotkey=5XXXXXX...``
      ``2026-05-11 07:54:28 | reliquary.validator.server | WARNING | rejected prompt=861 hotkey=... reason=cooldown rewards=[...]``
      ``2026-05-11 07:54:28 | reliquary.validator.service | INFO | Window 453 sealed (B valid received)``
      ``2026-05-11 07:46:54 | reliquary.validator.service | ERROR | Window iteration failed``

    Returns None for lines we don't care about (DEBUG noise, /state polls).
    """
    # Cheap pre-filter so we don't bother regex'ing every line.
    # Order matches frequency: accept/reject/late-drop dominate, the rest are rare.
    PATTERNS = (
        "accepted prompt=", "rejected prompt=",
        "validator_submit_lifecycle",
        "dropping late submission",
        "Window iteration failed", *_VALIDATOR_SEAL_MARKERS,
        "submission worker", "submission_worker",
        "timed out", "_derive_randomness", "derived on attempt",
    )
    if not any(s in line for s in PATTERNS):
        return None

    # Timestamp — first 19 chars are "YYYY-MM-DD HH:MM:SS".
    ts = line[11:19] if len(line) >= 19 else ""
    # Parse into epoch seconds so the rundown panel can filter by
    # timeframe without re-parsing the string on every render.
    ts_epoch = 0.0
    if len(line) >= 19:
        try:
            ts_epoch = datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc
            ).timestamp()
        except ValueError:
            ts_epoch = 0.0

    body = line.split(" | ", 3)[-1] if " | " in line else line

    def _int_field(name: str, default: int = 0) -> int:
        m = re.search(rf"\b{name}=(\d+)", line)
        if not m:
            return default
        try:
            return int(m.group(1))
        except Exception:
            return default

    if "validator_submit_lifecycle" in line:
        raw_json = body
        if "{" in raw_json:
            raw_json = raw_json[raw_json.find("{"):]
        try:
            payload = json.loads(raw_json)
        except Exception:
            payload = {}
        if isinstance(payload, dict) and payload.get("event") == "validator_submit_lifecycle":
            stage = str(payload.get("stage") or "")
            # The modern validator logs both proof_finished and candidate_*
            # events for the same submission. Count only the pool decision line
            # so dashboard counters are not doubled.
            if stage in (
                "candidate_accepted", "candidate_rejected",
                "final_batch_selected", "reward_assigned",
            ):
                hk = str(payload.get("hotkey") or "")
                ours = hk in OUR_SS58 if hk else False
                prompt_idx = _as_int(payload.get("prompt_idx"), -1)
                window_n = _as_int(payload.get("window_n"), 0)
                env_name = str(payload.get("env_name") or "")
                drand_round = int(
                    payload.get("submitted_drand_round")
                    or payload.get("arrival_drand_round")
                    or 0
                )
                if stage == "candidate_accepted":
                    return ValidatorEvent(
                        ts=ts, ts_epoch=ts_epoch, msg=body, ours=ours,
                        hotkey12=hk[:12], kind="accept",
                        prompt_idx=prompt_idx, drand_round=drand_round,
                        window_n=window_n, env_name=env_name,
                    )
                if stage == "final_batch_selected":
                    return ValidatorEvent(
                        ts=ts, ts_epoch=ts_epoch, msg=body, ours=ours,
                        hotkey12=hk[:12],
                        kind="selected" if payload.get("selected_for_batch") else "not_selected",
                        prompt_idx=prompt_idx, drand_round=drand_round,
                        window_n=window_n, env_name=env_name,
                        reward_amount=_as_float(payload.get("reward_amount")),
                    )
                if stage == "reward_assigned":
                    return ValidatorEvent(
                        ts=ts, ts_epoch=ts_epoch, msg=body, ours=ours,
                        hotkey12=hk[:12], kind="reward",
                        prompt_idx=prompt_idx, drand_round=drand_round,
                        window_n=window_n, env_name=env_name,
                        reward_amount=_as_float(payload.get("reward_amount")),
                    )
                reason = str(
                    payload.get("reject_reason")
                    or payload.get("batch_filled_reason")
                    or payload.get("reason")
                    or ""
                )
                return ValidatorEvent(
                    ts=ts, ts_epoch=ts_epoch, msg=body, ours=ours,
                    hotkey12=hk[:12], kind="reject", reject_reason=reason,
                    prompt_idx=prompt_idx, drand_round=drand_round,
                    window_n=window_n, env_name=env_name,
                )

    if "accepted prompt=" in line:
        hk = ""
        if "hotkey=" in line:
            hk = line.split("hotkey=", 1)[1].split()[0]
        ours = hk[:12] in {k[:12] for k in OUR_SS58}
        return ValidatorEvent(
            ts=ts, ts_epoch=ts_epoch, msg=body, ours=ours,
            hotkey12=hk[:12], kind="accept",
            prompt_idx=_int_field("prompt", -1),
            drand_round=_int_field("drand_round", 0),
        )
    if "rejected prompt=" in line:
        hk = ""
        if "hotkey=" in line:
            hk = line.split("hotkey=", 1)[1].split()[0]
        ours = hk[:12] in {k[:12] for k in OUR_SS58}
        # Extract reason= for the rundown panel's reject-reasons
        # breakdown. Validator log convention: "reason=out_of_zone rewards=..."
        reason = ""
        if "reason=" in line:
            reason = line.split("reason=", 1)[1].split()[0]
        return ValidatorEvent(
            ts=ts, ts_epoch=ts_epoch, msg=body, ours=ours,
            hotkey12=hk[:12], kind="reject", reject_reason=reason,
            prompt_idx=_int_field("prompt", -1),
            drand_round=_int_field("drand_round", 0),
        )
    if "dropping late submission" in line:
        # "dropping late submission prompt=X hotkey=Y (batcher window=N no longer active)"
        # — fires when the HTTP layer accepts a POST but the batcher's
        # window has already advanced before the submission reaches the
        # worker. The submission never gets GRAIL-verified and never
        # appears in batch/runners-up/rejected. Without this branch the
        # dashboard misses these entirely — a miner can spend hours
        # submitting and look "silent" if they always lose the FIFO race.
        hk = ""
        if "hotkey=" in line:
            hk = line.split("hotkey=", 1)[1].split()[0]
        ours = hk[:12] in {k[:12] for k in OUR_SS58}
        return ValidatorEvent(
            ts=ts, ts_epoch=ts_epoch, msg=body, ours=ours,
            hotkey12=hk[:12], kind="late_drop",
        )
    if any(marker in line for marker in _VALIDATOR_SEAL_MARKERS):
        window_match = re.search(r"\bWindow\s+(\d+)\b", line)
        return ValidatorEvent(
            ts=ts, ts_epoch=ts_epoch, msg=body, kind="seal",
            window_n=int(window_match.group(1)) if window_match else 0,
        )
    if "Window iteration failed" in line:
        return ValidatorEvent(
            ts=ts, ts_epoch=ts_epoch, msg=body, kind="fail",
        )
    # PR #8 retry instrumentation: WARNING line + INFO line per retry pair.
    if "_derive_randomness" in line and "failed" in line:
        return ValidatorEvent(
            ts=ts, ts_epoch=ts_epoch, msg=body, kind="randomness_retry",
        )
    if "derived on attempt" in line:
        return ValidatorEvent(
            ts=ts, ts_epoch=ts_epoch, msg=body, kind="randomness_ok",
        )
    # Worker / window-timeout errors — capture broadly so any future
    # message variant still surfaces in the rundown's error counts.
    if ("submission worker" in line.lower() or "submission_worker" in line) and (
        "failed" in line.lower() or "error" in line.lower()
    ):
        return ValidatorEvent(
            ts=ts, ts_epoch=ts_epoch, msg=body, kind="worker_fail",
        )
    if "timed out" in line.lower() and ("window" in line.lower() or "iteration" in line.lower()):
        return ValidatorEvent(
            ts=ts, ts_epoch=ts_epoch, msg=body, kind="window_timeout",
        )
    return None


def validator_tail_loop():
    """Maintain a persistent ``docker logs -f`` stream against the validator.

    Restarts the subprocess automatically if SSH or docker drops it. Pushes
    parsed ValidatorEvent rows into ``_validator_events``. Cheap on the
    wire — the grep happens server-side so only matching lines come back.

    Started once at dashboard boot from ``init_state``.
    """
    global _validator_tail_proc
    if not VALIDATOR_SSH:
        return
    # The grep mirrors the prefilter in ``_classify_validator_line``. Keeping
    # the patterns in sync is fine: each pattern is the unique log-line
    # substring for an outcome we render.
    # Expanded grep mirrors the substrings checked in
    # `_classify_validator_line`. Adding new patterns here AND there together
    # keeps the wire-side filter tight while letting the rundown panel
    # surface the full taxonomy of validator-side events.
    remote_command = (
        f"docker logs {shlex.quote(VALIDATOR_CONTAINER)} -f --since 2m 2>&1 | "
        f"grep --line-buffered -E {shlex.quote(_VALIDATOR_TAIL_GREP_EXPR)}"
    )
    cmd = _validator_ssh_args(remote_command, server_alive=True)
    failures = 0
    while True:
        connected_at = time.monotonic()
        try:
            _validator_tail_proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                bufsize=1,
            )
            for line in _validator_tail_proc.stdout:
                line = line.rstrip("\n")
                if not line:
                    continue
                ev = _classify_validator_line(line)
                if ev is None:
                    continue
                # Dedup. SSH reconnects (and the bounded `--since` re-read on
                # restart) duplicate any event in the lookback window; we
                # skip the second copy here so downstream counters don't
                # double up. Modern validator logs can emit both the JSON
                # candidate_rejected event and the legacy "rejected prompt="
                # line for the same submission, so accept/reject keys use the
                # hotkey/prompt/reason shape rather than the raw message text.
                if ev.kind in ("accept", "reject", "late_drop"):
                    key = (
                        ev.kind,
                        ev.hotkey12,
                        str(ev.prompt_idx),
                        ev.reject_reason,
                        str(int(ev.ts_epoch // 10) if ev.ts_epoch else ev.ts),
                    )
                else:
                    key = (ev.ts, ev.msg)
                with _validator_events_lock:
                    if key in _seen_event_keys_set:
                        continue
                    # Bound the dedup set the same way as the events
                    # deque — when the oldest tracked key falls off, we
                    # drop it from the set too so the membership check
                    # doesn't grow unbounded.
                    if len(_seen_event_keys) == _seen_event_keys.maxlen:
                        _seen_event_keys_set.discard(_seen_event_keys[0])
                    _seen_event_keys.append(key)
                    _seen_event_keys_set.add(key)
                    _validator_events.append(ev)
        except Exception:
            pass
        finally:
            process = _validator_tail_proc
            if process is not None and process.poll() is None:
                try:
                    process.terminate()
                except Exception:
                    pass
        lived_s = max(0.0, time.monotonic() - connected_at)
        failures = 0 if lived_s >= 60.0 else failures + 1
        ceiling = min(300.0, 5.0 * (2 ** min(max(failures - 1, 0), 6)))
        # Equal jitter avoids reconnect waves while retaining a lower bound.
        time.sleep(random.uniform(ceiling * 0.5, ceiling))


def validator_events_snapshot(limit: int = 50) -> list[ValidatorEvent]:
    """Thread-safe copy of the tail buffer for the renderer."""
    with _validator_events_lock:
        # deque slicing isn't supported — reverse to newest-first via list.
        return list(_validator_events)[-limit:]


def validator_events_query(
    kinds: Optional[set[str]] = None,
    reason_substr: Optional[str] = None,
    hotkey_substr: Optional[str] = None,
    ours_only: bool = False,
    since_epoch: float = 0.0,
    limit: int = 500,
) -> list[ValidatorEvent]:
    """Filtered view onto the validator events deque for the /logs page.

    Filters compose as AND. Pass ``None`` to disable a filter. ``limit``
    is applied last (newest entries kept). This is the only query that
    supports pulling more than ~60 events at once — the /logs page is
    operator-facing forensic UI and benefits from showing the full
    buffer, while the rundown panel still uses ``_in_window`` for
    timeframe slicing.
    """
    with _validator_events_lock:
        events = list(_validator_events)
    out: list[ValidatorEvent] = []
    needle_reason = reason_substr.lower() if reason_substr else None
    needle_hk = hotkey_substr.lower() if hotkey_substr else None
    for e in events:
        if kinds and e.kind not in kinds:
            continue
        if since_epoch > 0 and e.ts_epoch < since_epoch:
            continue
        if ours_only and not e.ours:
            continue
        if needle_reason and needle_reason not in (e.reject_reason or "").lower():
            continue
        if needle_hk and needle_hk not in (e.hotkey12 or "").lower():
            continue
        out.append(e)
    return out[-limit:]


def validator_events_in_window(seconds: float) -> list[ValidatorEvent]:
    """All events with `ts_epoch >= now - seconds`. Used by the rundown
    panel to slice the deque to a configurable timeframe (30 min, 60 min,
    or "all" when seconds <= 0). Events with `ts_epoch == 0` (parse
    failures from a malformed log line) are dropped — they'd otherwise
    appear in every window because their epoch is the unix zero point.
    """
    if seconds <= 0:
        cutoff = 0.0
    else:
        cutoff = time.time() - seconds
    with _validator_events_lock:
        return [e for e in _validator_events if e.ts_epoch > cutoff]


def _reliquary_one_v5_fleet_events(
    box: BoxState,
) -> dict[str, int | None] | None:
    """Return observed host-scoped V5 admission counts.

    V5's ``fleet-events.jsonl`` lives beneath the configured state root for
    one host, so ``accepted=True`` rows retain host attribution that the
    validator's shared-hotkey `/verdicts` response cannot provide.  The
    stream does not provide a complete rejected-outcome source, so rejection
    counts remain unavailable even when the rolling event coverage is good.
    """

    if getattr(box, "miner_kind", "") != "reliquary_one":
        return None
    miner = getattr(box, "reliquary_one", {})
    if not isinstance(miner, dict):
        return None
    v5 = miner.get("v5_telemetry")
    v5 = v5 if isinstance(v5, dict) else {}
    events = v5.get("fleet_events")
    if not isinstance(events, dict) or events.get("available") is not True:
        return None
    if events.get("complete_30m") is not True:
        return None

    def count(name: str, *, required: bool = False) -> int | None:
        value = events.get(name)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            return value
        return None if required else 0

    accepted_30m = count("verdict_accepted_30m", required=True)
    if accepted_30m is None:
        return None
    complete_60m = events.get("complete_60m") is True
    return {
        "accepted_30m": accepted_30m,
        "accepted_60m": (
            count("verdict_accepted_60m", required=True)
            if complete_60m else None
        ),
        "rejected_30m": None,
        "rejected_60m": None,
    }


def recompute_validator_acpts(
    boxes: list[BoxState], validator_state: ValidatorState | None = None
) -> None:
    """Repopulate each box's validator-derived counters from the events
    tail. Three signals are written in a single pass over the buffer:

      `acpt_30m` / `acpt_60m`        worker-confirmed accepts (GRAIL passed)
      `rej_30m`                      worker-confirmed rejects for this hotkey
      `late_drops_30m` / `_60m`      submissions dropped by the batcher
                                     because the window already advanced
                                     before the worker could pick them up

    The first one was misleading on the previous build (we were counting
    miner-side ACCEPTED-PREGEN lines from the journal — 20-30x inflated).
    The second one is the brand-new "submission landed in HTTP but lost
    the FIFO race" signal that the dashboard was missing entirely.

    The match is by hotkey-prefix: ValidatorEvent.hotkey12 is the first
    12 chars of the on-chain SS58, and each box's `state.hotkey` is the
    LABEL we store in OUR_SS58 — so we invert OUR_SS58 once to get a
    label->ss58 map and compare prefixes.

    All four counters are filled in a single pass to keep this cheap
    (<1 ms for 1500 events × N boxes).
    """
    if not boxes:
        return
    # Invert OUR_SS58 once. Skip empty labels just in case.
    label_to_ss58 = {label: ss58 for ss58, label in OUR_SS58.items() if label}
    now = time.time()
    cutoff_30 = now - 30 * 60
    cutoff_60 = now - 60 * 60
    with _validator_events_lock:
        evs = list(_validator_events)
    # Pre-index events by hotkey prefix so we don't re-scan the entire
    # buffer per box. Parallel dicts (30/60 min, accept/reject/late_drop)
    # are populated in one pass.
    acpt_30: dict[str, int] = {}
    acpt_60: dict[str, int] = {}
    reject_30: dict[str, int] = {}
    late_30: dict[str, int] = {}
    late_60: dict[str, int] = {}
    for ev in evs:
        if ev.kind not in ("accept", "reject", "late_drop"):
            continue
        if ev.ts_epoch <= cutoff_60:
            continue
        pref = ev.hotkey12[:12]
        if ev.kind == "accept":
            acpt_60[pref] = acpt_60.get(pref, 0) + 1
            if ev.ts_epoch > cutoff_30:
                acpt_30[pref] = acpt_30.get(pref, 0) + 1
        elif ev.kind == "reject":
            if ev.ts_epoch > cutoff_30:
                reject_30[pref] = reject_30.get(pref, 0) + 1
        else:  # late_drop
            late_60[pref] = late_60.get(pref, 0) + 1
            if ev.ts_epoch > cutoff_30:
                late_30[pref] = late_30.get(pref, 0) + 1
    seen_hotkeys: set[str] = set()
    verdicts_globally_fresh = bool(
        validator_state
        and validator_state.verdicts_last_fetch_at
        and now - validator_state.verdicts_last_fetch_at
        <= VALIDATOR_VERDICT_STALE_SECONDS
    )
    resolved_hotkeys = {
        id(box): label_to_ss58.get(box.hotkey) or box.hotkey
        for box in boxes
    }
    hotkey_box_counts = Counter(
        ss58 for ss58 in resolved_hotkeys.values() if ss58
    )

    def shared_hotkey_aggregate(ss58: str) -> dict[str, object]:
        """Return an aggregate verdict summary without assigning it to a host."""

        pref = ss58[:12]
        aggregate: dict[str, object] = {
            "accepted_30m": acpt_30.get(pref, 0),
            "accepted_60m": acpt_60.get(pref, 0),
            "rejected_30m": reject_30.get(pref, 0),
            "rejected_60m": 0,
            "source": "validator_events",
        }
        if not verdicts_globally_fresh or validator_state is None:
            return aggregate
        candidate = validator_state.verdicts_by_hotkey.get(ss58)
        if not isinstance(candidate, dict):
            return aggregate
        fetched_at = _as_float(
            candidate.get(
                "_fetched_at",
                validator_state.verdicts_last_fetch_at,
            )
        )
        if (
            fetched_at <= 0
            or now - fetched_at > VALIDATOR_VERDICT_STALE_SECONDS
        ):
            return aggregate
        return {
            "accepted_30m": _as_int(candidate.get("accepted_30m")),
            "accepted_60m": _as_int(candidate.get("accepted_60m")),
            "rejected_30m": _as_int(candidate.get("rejected_30m")),
            "rejected_60m": _as_int(candidate.get("rejected_60m")),
            "source": "verdicts",
        }

    for box in boxes:
        ss58 = resolved_hotkeys[id(box)]
        if not ss58:
            # Box's label not in OUR_SS58 — leave the field at zero
            # rather than guessing. The operator will see "0" on the
            # box row and know to fix their FLEET / OUR_SS58 mapping.
            box.acpt_30m = 0
            box.acpt_60m = 0
            box.rej_30m = 0
            box.late_drops_30m = 0
            box.late_drops_60m = 0
            box.acceptance_source = "unmapped"
            box.acceptance_scope = "unmapped"
            box.shared_hotkey_verdicts = {}
            continue
        v5_host_events = _reliquary_one_v5_fleet_events(box)
        if v5_host_events is not None:
            # The V5 state root is one host's own append-only event stream.
            # Prefer it over a shared SS58 verdict counter, which cannot say
            # which of two boxes produced the admitted candidate.
            box.acpt_30m = v5_host_events["accepted_30m"]
            box.acpt_60m = v5_host_events["accepted_60m"]
            box.rej_30m = v5_host_events["rejected_30m"]
            box.final_accept_30m = v5_host_events["accepted_30m"]
            box.final_accept_60m = v5_host_events["accepted_60m"]
            box.final_reject_30m = v5_host_events["rejected_30m"]
            box.final_reject_60m = v5_host_events["rejected_60m"]
            box.late_drops_30m = 0
            box.late_drops_60m = 0
            box.acceptance_source = "v5_fleet_events_admissions_observed"
            box.acceptance_scope = "per_host_admissions_observed"
            box.shared_hotkey_verdicts = (
                shared_hotkey_aggregate(ss58)
                if hotkey_box_counts[ss58] > 1 else {}
            )
            box.acpt_trend.append(int(box.acpt_30m or 0))
            continue
        if (
            getattr(box, "miner_kind", "") == "reliquary_one"
            and hotkey_box_counts[ss58] > 1
        ):
            # Do not turn the second V5 host into a zero or allocate the
            # whole shared-hotkey total to the first.  Preserve the aggregate
            # beside an explicit unavailable per-host value for the UI/API.
            box.acpt_30m = None
            box.acpt_60m = None
            box.rej_30m = None
            box.final_accept_30m = None
            box.final_accept_60m = None
            box.final_reject_30m = None
            box.final_reject_60m = None
            box.late_drops_30m = 0
            box.late_drops_60m = 0
            box.acceptance_source = "shared_hotkey_aggregate_unattributable"
            box.acceptance_scope = "shared_hotkey_aggregate"
            box.shared_hotkey_verdicts = shared_hotkey_aggregate(ss58)
            box.acpt_trend.clear()
            continue
        if ss58 in seen_hotkeys:
            # Multiple services may share one hotkey (current RTX8 mesh).
            # Validator events are keyed only by hotkey, not by service, so
            # duplicating the same accept count on every GPU row would inflate
            # fleet totals. Attribute hotkey-level accepts to the first row;
            # service-level production still appears in the pipeline metrics.
            box.acpt_30m = 0
            box.acpt_60m = 0
            box.rej_30m = 0
            box.late_drops_30m = 0
            box.late_drops_60m = 0
            box.final_accept_30m = 0
            box.final_accept_60m = 0
            box.final_reject_30m = 0
            box.final_reject_60m = 0
            box.acceptance_source = "duplicate-hotkey"
            box.acceptance_scope = "shared_hotkey_duplicate"
            box.shared_hotkey_verdicts = shared_hotkey_aggregate(ss58)
            box.acpt_trend.append(0)
            continue
        seen_hotkeys.add(ss58)
        pref = ss58[:12]
        box.acpt_30m = acpt_30.get(pref, 0)
        box.acpt_60m = acpt_60.get(pref, 0)
        box.rej_30m = reject_30.get(pref, 0)
        box.late_drops_30m = late_30.get(pref, 0)
        box.late_drops_60m = late_60.get(pref, 0)
        box.acceptance_source = "events"
        box.acceptance_scope = "per_hotkey"
        box.shared_hotkey_verdicts = {}

        # `/verdicts/{full-hotkey}` is exact-keyed and represents the final
        # worker decision, unlike miner-side provisional submit responses or
        # a best-effort SSH log tail.  When fresh, it overrides the fallback
        # event counters (including an authoritative empty result).
        verdict_summary = None
        if verdicts_globally_fresh and validator_state is not None:
            candidate = validator_state.verdicts_by_hotkey.get(ss58)
            if isinstance(candidate, dict):
                fetched_at = _as_float(
                    candidate.get(
                        "_fetched_at",
                        validator_state.verdicts_last_fetch_at,
                    )
                )
                if (
                    fetched_at > 0
                    and now - fetched_at <= VALIDATOR_VERDICT_STALE_SECONDS
                ):
                    verdict_summary = candidate
        if isinstance(verdict_summary, dict):
            box.acpt_30m = _as_int(verdict_summary.get("accepted_30m"))
            box.acpt_60m = _as_int(verdict_summary.get("accepted_60m"))
            box.rej_30m = _as_int(verdict_summary.get("rejected_30m"))
            box.final_accept_30m = box.acpt_30m
            box.final_accept_60m = box.acpt_60m
            box.final_reject_30m = box.rej_30m
            box.final_reject_60m = _as_int(verdict_summary.get("rejected_60m"))
            reasons = verdict_summary.get("reason_counts_30m") or {}
            if isinstance(reasons, dict):
                box.window_mismatch_30m = _as_int(reasons.get("window_mismatch"))
                box.grail_fail_30m = sum(
                    _as_int(v) for k, v in reasons.items()
                    if "grail" in str(k).lower()
                )
                box.bad_term_30m = sum(
                    _as_int(v) for k, v in reasons.items()
                    if "term" in str(k).lower()
                )
                box.late_drops_30m = sum(
                    _as_int(v) for k, v in reasons.items()
                    if str(k) in {"worker_dropped", "window_not_active"}
                )
                if reasons:
                    box.last_reject_reason = max(
                        reasons.items(), key=lambda item: _as_int(item[1])
                    )[0]
            last = verdict_summary.get("last") or {}
            if isinstance(last, dict):
                box.last_final_reason = str(
                    last.get("reject_reason") or last.get("reason") or ""
                )
                box.last_final_window = _as_int(last.get("window_n"))
                box.last_final_ts = _as_float(last.get("ts"))
                if (
                    box.miner_submitted_this_win
                    and box.miner_window > 0
                    and box.last_final_window >= box.miner_window
                ):
                    box.miner_submitted_this_win = 0
                    if box.miner_state == "submitted":
                        box.miner_state = "waiting"
                        box.miner_inflight = 0
                        box.miner_ready = 1 if box.proc_alive else 0
            box.acceptance_source = "verdicts"
            box.acceptance_scope = "per_hotkey"
        # Reference frontier activity logs do not carry a window number. Once
        # an older submission has received its final verdict, attribute the
        # live ready/generating/proving/waiting state to the validator's
        # current window instead of leaving the completed submit window on the
        # dashboard. Outstanding submissions retain their exact logged window.
        if (
            validator_state is not None
            and validator_state.window > 0
            and box.proc_alive
            and not box.miner_submitted_this_win
            and box.miner_state in {"ready", "waiting", "generating", "proving"}
        ):
            box.miner_window = validator_state.window
        # Keep the trend sparkline pointing at the corrected counter.
        box.acpt_trend.append(box.acpt_30m)


def current_window_accept_count() -> int:
    """Count of `accept` events since the last `seal` event in the tail
    buffer. This is the live "X / B" gauge the dashboard's "valid"
    KPI used to show — the validator's /state.valid_submissions field
    now only updates at seal time on this build, so polling the events
    feed is the only way to surface real-time slot progression.

    If no seal event is in the buffer (cold start, deque just rotated
    past the last seal), counts all accepts in the buffer.
    """
    with _validator_events_lock:
        evs = list(_validator_events)
    count = 0
    # Walk newest-first so we can stop at the most recent seal.
    for ev in reversed(evs):
        if ev.kind == "seal":
            break
        if ev.kind == "accept":
            count += 1
    return count


# --- Validator deployment probe -------------------------------------------
# Captures the image SHA, start time, and uptime so the rundown panel
# can pin every observation to a specific deployment ("PR #8 retry logic
# live since 09:29:24Z"). Polled on its own thread at slow cadence
# because docker inspect over SSH is ~600 ms — we don't want it on the
# request path.

@dataclass
class DeploymentState:
    image_ref: str = ""           # ghcr.io/.../reliquary-validator:latest
    image_sha: str = ""           # 12-char short hash from sha256:<64>
    image_sha_full: str = ""
    started_at: str = ""          # ISO8601 from docker inspect
    started_epoch: float = 0.0
    status: str = ""              # "running" / "restarting" / "exited"
    last_fetch_at: float = 0.0
    error: str = ""

    @property
    def uptime_s(self) -> int:
        if self.started_epoch <= 0:
            return -1
        return max(0, int(time.time() - self.started_epoch))


def fetch_deployment_state(ds: DeploymentState) -> None:
    """Single docker-inspect over SSH to fill the DeploymentState fields.

    Cheap and idempotent — meant to be called from a slow poller thread,
    NOT inline in a request handler. The validator image SHA is what
    fingerprints "which PR is running"; everything else is contextual.
    """
    cmd = _validator_ssh_args(
        f"docker inspect {shlex.quote(VALIDATOR_CONTAINER)} "
        "--format '{{.Config.Image}}|{{.Image}}|{{.State.StartedAt}}|{{.State.Status}}'"
    )
    try:
        out = subprocess.check_output(
            cmd, stderr=subprocess.DEVNULL, timeout=10, text=True
        ).strip()
        parts = out.split("|")
        if len(parts) != 4:
            ds.error = f"unexpected format: {out[:80]}"
            return
        image_ref, image_full, started_at, status = parts
        # image_full looks like "sha256:b3a60fc2700d…"; trim to 12 hex
        # chars matching what the user pastes from `docker images`.
        sha_full = image_full.split("sha256:", 1)[-1]
        sha_short = sha_full[:12]
        # docker iso timestamp: "2026-05-11T09:29:24.327324125Z"
        started_epoch = 0.0
        try:
            # Truncate fractional seconds beyond 6 digits — datetime can't
            # parse nanosecond precision in older Python builds.
            iso = started_at
            if "." in iso and iso.endswith("Z"):
                head, frac = iso[:-1].split(".", 1)
                iso = f"{head}.{frac[:6]}Z"
            started_epoch = datetime.strptime(
                iso, "%Y-%m-%dT%H:%M:%S.%fZ"
            ).replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            try:
                started_epoch = datetime.strptime(
                    started_at, "%Y-%m-%dT%H:%M:%SZ"
                ).replace(tzinfo=timezone.utc).timestamp()
            except ValueError:
                started_epoch = 0.0
        ds.image_ref = image_ref
        ds.image_sha = sha_short
        ds.image_sha_full = sha_full
        ds.started_at = started_at
        ds.started_epoch = started_epoch
        ds.status = status
        ds.last_fetch_at = time.time()
        ds.error = ""
    except subprocess.TimeoutExpired:
        ds.error = "ssh timeout"
    except subprocess.CalledProcessError as e:
        ds.error = f"ssh exit {e.returncode}"
    except Exception as e:  # noqa: BLE001 — diagnostic surface
        ds.error = type(e).__name__


@dataclass
class WindowSummary:
    n: int
    ours: int
    total_batch: int
    rt_first: float
    rejects: dict
    reward_ours: float = 0.0
    reward_total: float = 0.0
    rewards_by_hotkey: dict[str, float] = field(default_factory=dict)
    # Future archive lifecycle: legacy objects are completed; aborted
    # tombstones prove numeric continuity but are never reward/EMA evidence.
    window_status: str = "completed"
    terminal_data_present: bool = True
    failure_stage: str = ""
    failure_type: str = ""
    lifecycle_explicit: bool = False
    archive_schema_version: int = 0
    auction_seal_drain_by_environment: dict[str, dict[str, Any]] = field(
        default_factory=dict
    )
    # Distinguishes a real zero-reward archive from an old dashboard cache
    # row that discarded the map.  Exact EMA replay must include the former
    # (it decays prior weights) and refetch/ignore the latter.
    reward_data_present: bool = False
    environments: list[str] = field(default_factory=list)
    environment_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    ours_by_environment: dict[str, int] = field(default_factory=dict)
    # The archive's canonical batch is grouped by environment.  Keep the
    # actual minimum selected response time for each environment instead of
    # treating the first row of the combined list as the global winner.
    rt_first_by_environment: dict[str, float] = field(default_factory=dict)
    reward_ours_by_environment: dict[str, float] = field(default_factory=dict)
    # Current archives intentionally expose only the authoritative aggregate
    # ``rewards_by_hotkey`` map.  Per-row ``reward_amount`` is absent, so an
    # empty map with ``exact=False`` means unavailable (not zero).
    reward_ours_by_environment_exact: bool = False
    # Raw batch + rejected entries kept so aggregator panels (slot rank, σ
    # histogram, competitor leaderboard) can compute on demand without re-
    # downloading from R2. ~5KB per window × 12 windows = ~60KB total — fits
    # comfortably in cached state.
    batch: list = field(default_factory=list)
    runners_up: list = field(default_factory=list)
    rejected: list = field(default_factory=list)
    force_seal_reason: str = ""
    validator_hotkey: str = ""
    randomness: str = ""
    rewarded_but_not_selected_by_hotkey: dict[str, float] = field(default_factory=dict)
    late_drops: dict[str, dict[str, int]] = field(default_factory=dict)
    reject_summary: dict[str, int] = field(default_factory=dict)
    grader_failures: dict[str, int] = field(default_factory=dict)
    training_quarantine: dict[str, Any] = field(default_factory=dict)
    difficulty_auction_shadow: dict[str, Any] = field(default_factory=dict)
    server_reject_summary: dict[str, Any] = field(default_factory=dict)
    logical_group_dedup: dict[str, Any] = field(default_factory=dict)
    training_accumulator: dict[str, Any] = field(default_factory=dict)


# Window-level cache: once a window is sealed in R2 it's immutable, so we
# keep a per-window dict and only download windows we haven't seen yet. R2
# publishes sealed windows behind the live validator window, so published
# objects are immutable and don't need a speculative re-fetch every cycle.
_window_cache: dict[int, "WindowSummary"] = {}

# Disk persistence — survives restarts so we don't re-download 216 archives.
# The v2 filename prevents an already-running pre-v2 dashboard from racing
# this process and repeatedly overwriting exact reward rows with legacy rows.
_WINDOW_CACHE_FILE = _state_file("window_cache_v2.json.gz")
_LEGACY_WINDOW_CACHE_FILE = _state_file("window_cache.json.gz")
_WINDOW_CACHE_SCHEMA = 3
_WINDOW_CACHE_LIFECYCLE_SCHEMA = 4

_R2_LAST_ATTEMPT_AT = 0.0
_R2_LAST_SUCCESS_AT = 0.0
_R2_LAST_ERROR = ""
_R2_LATEST_WINDOW = 0
_R2_LATEST_WINDOW_SEEN_AT = 0.0
_R2_DISCOVERY_SOURCE = ""
_R2_COVERAGE_REQUESTED = 0
_R2_COVERAGE_NUMERIC_EXPECTED = 0
_R2_COVERAGE_EXPECTED = 0
_R2_COVERAGE_CACHED = 0
_R2_COVERAGE_EXACT = 0
_R2_COVERAGE_REWARD_EXPECTED = 0
_R2_COVERAGE_TERMINAL = 0
_R2_COVERAGE_ABORTED: list[int] = []
_R2_COVERAGE_ABSENT: list[int] = []
_R2_COVERAGE_MISSING: list[int] = []
_R2_COVERAGE_REWARD_MISSING: list[int] = []
_R2_COVERAGE_COMPLETE = False
_R2_COVERAGE_LIFECYCLE_EXPLICIT = False


def r2_status() -> dict[str, Any]:
    """Read-only R2/cache status used by `/healthz` and JSON export."""
    coverage: dict[str, Any] = {
        "requested": _R2_COVERAGE_REQUESTED,
        "numeric_expected": _R2_COVERAGE_NUMERIC_EXPECTED,
        "archived_expected": _R2_COVERAGE_EXPECTED,
        # Preserve the historical meaning for older consumers: the numeric
        # lookback width, including windows that were never archived.
        "expected": _R2_COVERAGE_NUMERIC_EXPECTED,
        "cached": _R2_COVERAGE_CACHED,
        "exact_rewards": _R2_COVERAGE_EXACT,
        "absent_windows": list(_R2_COVERAGE_ABSENT),
        "missing_windows": list(_R2_COVERAGE_MISSING),
        "complete": _R2_COVERAGE_COMPLETE,
        "source": _R2_DISCOVERY_SOURCE,
    }
    if _R2_COVERAGE_LIFECYCLE_EXPLICIT:
        coverage.update(
            {
                "terminal_present": _R2_COVERAGE_TERMINAL,
                "reward_expected": _R2_COVERAGE_REWARD_EXPECTED,
                "aborted_windows": list(_R2_COVERAGE_ABORTED),
                "reward_missing_windows": list(_R2_COVERAGE_REWARD_MISSING),
            }
        )
    result = {
        "last_attempt_at": _R2_LAST_ATTEMPT_AT,
        "last_success_at": _R2_LAST_SUCCESS_AT,
        "error": _R2_LAST_ERROR,
        "latest_window": _R2_LATEST_WINDOW,
        "latest_window_seen_at": _R2_LATEST_WINDOW_SEEN_AT,
        "discovery_source": _R2_DISCOVERY_SOURCE,
        "coverage_complete": _R2_COVERAGE_COMPLETE,
        "coverage": coverage,
        "cache_entries": len(_window_cache),
        "exact_reward_entries": sum(
            1 for window in _window_cache.values() if window.reward_data_present
        ),
    }
    if _R2_COVERAGE_LIFECYCLE_EXPLICIT:
        result.update(
            {
                "terminal_entries": sum(
                    1
                    for window in _window_cache.values()
                    if window.terminal_data_present
                ),
                "completed_entries": sum(
                    1
                    for window in _window_cache.values()
                    if window.terminal_data_present
                    and window.window_status == "completed"
                ),
                "aborted_entries": sum(
                    1
                    for window in _window_cache.values()
                    if window.terminal_data_present
                    and window.window_status == "aborted"
                ),
            }
        )
    return result


def _window_cache_row(w: WindowSummary) -> dict[str, Any]:
    row = {
        name: getattr(w, name)
        for name in WindowSummary.__dataclass_fields__
    }
    if not w.lifecycle_explicit:
        for name in (
            "window_status",
            "terminal_data_present",
            "failure_stage",
            "failure_type",
            "lifecycle_explicit",
            "archive_schema_version",
            "auction_seal_drain_by_environment",
        ):
            row.pop(name, None)
    return row


def _infer_single_environment_rewards(
    rewards_by_hotkey: Any,
    rows: Any,
    *,
    reward_data_present: bool,
) -> tuple[dict[str, float], bool]:
    """Attribute authoritative rewards when each owned key has one exact env.

    Current R2 archives omit per-row ``reward_amount`` while retaining both
    the authoritative ``rewards_by_hotkey`` map and ``rewarded`` rows with an
    explicit ``env_name``.  An owned hotkey's aggregate reward is therefore
    attributable exactly only when every rewarded row for that key names the
    same environment.  Any missing row/environment or cross-environment key
    fails closed.
    """
    if not reward_data_present or not isinstance(rewards_by_hotkey, dict):
        return {}, False

    authoritative: dict[str, float] = {}
    for hotkey, raw_amount in rewards_by_hotkey.items():
        hotkey = str(hotkey)
        if hotkey not in OUR_SS58:
            continue
        try:
            amount = float(raw_amount)
        except (TypeError, ValueError):
            return {}, False
        if not math.isfinite(amount):
            return {}, False
        authoritative[hotkey] = amount
    if not authoritative:
        return {}, False

    environments_by_hotkey: dict[str, set[str]] = {}
    if not isinstance(rows, list):
        return {}, False
    for row in rows:
        if not isinstance(row, dict) or row.get("rewarded") is not True:
            continue
        hotkey = str(row.get("hotkey") or "")
        if hotkey not in OUR_SS58:
            continue
        # A rewarded owned row absent from the authoritative map cannot be
        # assigned an amount exactly, even if its environment is known.
        if hotkey not in authoritative:
            return {}, False
        env_name = str(row.get("env_name") or "").strip()
        if not env_name:
            return {}, False
        environments_by_hotkey.setdefault(hotkey, set()).add(env_name)

    attributed: dict[str, float] = {}
    attributed_any = False
    for hotkey, amount in authoritative.items():
        environments = environments_by_hotkey.get(hotkey, set())
        if not environments:
            # Explicit zeroes do not need an environment to preserve the
            # aggregate; any non-zero reward does.
            if math.isclose(amount, 0.0, rel_tol=0.0, abs_tol=1e-12):
                continue
            return {}, False
        if len(environments) != 1:
            return {}, False
        env_name = next(iter(environments))
        attributed[env_name] = attributed.get(env_name, 0.0) + amount
        attributed_any = True

    if not attributed_any or not math.isclose(
        sum(attributed.values()),
        sum(authoritative.values()),
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        return {}, False
    return attributed, True


def _window_from_cache_row(row: dict[str, Any], *, legacy: bool) -> WindowSummary:
    known = WindowSummary.__dataclass_fields__
    kwargs = {name: row[name] for name in known if name in row}
    kwargs["n"] = _as_int(row.get("n"))
    kwargs["ours"] = _as_int(row.get("ours"))
    kwargs["total_batch"] = _as_int(row.get("total_batch"))
    kwargs["rt_first"] = _as_float(row.get("rt_first"))
    kwargs["rejects"] = dict(row.get("rejects") or {})
    kwargs["reward_ours"] = _as_float(row.get("reward_ours"))
    kwargs["reward_total"] = _as_float(row.get("reward_total"))
    raw_status = "completed" if legacy else row.get("window_status", "completed")
    if raw_status not in {"completed", "aborted"}:
        raise ValueError("cached R2 window_status is incompatible")
    kwargs["window_status"] = raw_status
    kwargs["terminal_data_present"] = bool(
        not legacy and row.get("terminal_data_present", True)
    )
    kwargs["failure_stage"] = str(row.get("failure_stage") or "")
    kwargs["failure_type"] = str(row.get("failure_type") or "")
    kwargs["lifecycle_explicit"] = bool(
        not legacy and row.get("lifecycle_explicit", False)
    )
    kwargs["archive_schema_version"] = (
        _optional_nonnegative_int(row.get("archive_schema_version")) or 0
    )
    kwargs["auction_seal_drain_by_environment"] = {}
    raw_seal_drains = row.get("auction_seal_drain_by_environment")
    if isinstance(raw_seal_drains, dict):
        for environment, raw_drain in raw_seal_drains.items():
            drain = _normalize_seal_drain(raw_drain)
            if drain:
                kwargs["auction_seal_drain_by_environment"][
                    str(environment)
                ] = drain
    kwargs["reward_data_present"] = bool(
        not legacy
        and raw_status == "completed"
        and row.get("reward_data_present", False)
    )
    kwargs["reward_ours_by_environment_exact"] = bool(
        not legacy and row.get("reward_ours_by_environment_exact", False)
    )
    if not kwargs["reward_ours_by_environment_exact"]:
        # Schema-v2 rows populated absent reward_amount fields through
        # ``_as_float(None) == 0``.  Never carry those fabricated zeroes into
        # the new explicit availability contract.
        kwargs["reward_ours_by_environment"] = {}
        inferred, inferred_exact = _infer_single_environment_rewards(
            kwargs.get("rewards_by_hotkey"),
            list(kwargs.get("batch") or [])
            + list(kwargs.get("runners_up") or [])
            + list(kwargs.get("rejected") or []),
            reward_data_present=kwargs["reward_data_present"],
        )
        if inferred_exact:
            kwargs["reward_ours_by_environment"] = inferred
            kwargs["reward_ours_by_environment_exact"] = True
    return WindowSummary(**kwargs)


def window_cache_save() -> None:
    """Persist the window cache to disk (gzipped JSON). ~1MB for 216 windows."""
    try:
        rows = [_window_cache_row(w) for w in _window_cache.values()]
        schema = (
            _WINDOW_CACHE_LIFECYCLE_SCHEMA
            if any(window.lifecycle_explicit for window in _window_cache.values())
            else _WINDOW_CACHE_SCHEMA
        )
        payload = json.dumps({
            "schema_version": schema,
            "rows": rows,
        }).encode()
        _private_atomic_write(
            _WINDOW_CACHE_FILE,
            gzip.compress(payload, compresslevel=3),
        )
    except Exception:
        pass


def window_cache_load() -> int:
    """Load window cache from disk. Returns # of entries loaded."""
    path = _WINDOW_CACHE_FILE
    if not _bounded_regular_file(path, 50_000_000):
        path = _LEGACY_WINDOW_CACHE_FILE
    if not _bounded_regular_file(path, 50_000_000):
        return 0
    try:
        with open(path, "rb") as f:
            raw = gzip.decompress(f.read())
        payload = json.loads(raw)
        legacy = isinstance(payload, list)
        rows = payload if legacy else payload.get("rows", [])
        schema = 0 if legacy else _as_int(payload.get("schema_version"))
        # Schema 3 is the existing exact-reward cache and must remain byte-for-
        # byte reusable. Schema 4 adds lifecycle fields without invalidating
        # those reward maps.
        legacy = legacy or schema < 3
        _window_cache.clear()
        for r in rows:
            if not isinstance(r, dict):
                continue
            window = _window_from_cache_row(r, legacy=legacy)
            if window.n > 0:
                _window_cache[window.n] = window
        return len(_window_cache)
    except Exception:
        return 0

_r2_client = None
_MAX_R2_COMPRESSED_BYTES = 8 * 1024 * 1024
_MAX_R2_DECOMPRESSED_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class _PublicR2Client:
    """Marker for a cacheable, credential-free immutable archive origin."""

    base_url: str


class _R2HTTPError(RuntimeError):
    """Bounded curl transport failure with a parsed HTTP status, if any."""

    def __init__(self, returncode: int, http_status: int = 0) -> None:
        super().__init__(f"R2 curl failed rc={returncode}")
        self.returncode = int(returncode)
        self.http_status = int(http_status)


class _R2ObjectNotFound(FileNotFoundError):
    """An R2 object is authoritatively absent (HTTP/S3 404)."""


def r2():
    global _r2_client
    if _r2_client is None:
        public_base_url = str(
            R2_PUBLIC_BASE_URL or os.environ.get("R2_PUBLIC_BASE_URL", "")
        ).strip().rstrip("/")
        if public_base_url:
            _r2_client = _PublicR2Client(public_base_url)
            return _r2_client
    if _r2_client is None and boto3:
        endpoint = os.environ.get("R2_ENDPOINT")
        if not endpoint:
            # Fall back to a local `.env` file if present — operators
            # who prefer keeping secrets out of yaml can keep them in
            # an `R2_*=...` shell-style env file (gitignored).
            envpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
            if os.path.exists(envpath):
                with open(envpath) as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("export "):
                            line = line[len("export "):]
                        if "=" in line and not line.startswith("#"):
                            k, v = line.split("=", 1)
                            os.environ.setdefault(k, v.strip())
                endpoint = os.environ.get("R2_ENDPOINT")
        if endpoint:
            # A dashboard refresh must not be able to wedge forever on a
            # half-open R2 connection.  Keep retries bounded; the caller
            # already preserves and serves the last-good on-disk cache.
            from botocore.config import Config

            _r2_client = boto3.client(
                "s3", endpoint_url=endpoint, region_name="auto",
                config=Config(
                    connect_timeout=3,
                    read_timeout=8,
                    # The R2 worker retries every 30 seconds and serves its
                    # last-good cache in between. Botocore-level retries turn
                    # one slow GET into a multi-minute dashboard stall.
                    retries={"total_max_attempts": 1, "mode": "standard"},
                ),
            )
    return _r2_client


def _r2_curl_bytes(url: str, *, timeout_s: int = 10) -> bytes:
    """Fetch a presigned R2 URL with a hard wall-clock deadline.

    Botocore's read timeout is an inactivity timeout, so a slow/trickling R2
    response can still occupy the dashboard worker for minutes. curl's
    ``--max-time`` is a total deadline. Supplying the signed URL over stdin
    also keeps credentials/signatures out of the process list.
    """
    escaped = url.replace("\\", "\\\\").replace('"', '\\"')
    config = f'url = "{escaped}"\n'.encode()
    result = subprocess.run(
        [
            "/usr/bin/curl", "--fail", "--silent", "--show-error",
            "--location", "--max-time", str(timeout_s),
            "--max-filesize", str(_MAX_R2_COMPRESSED_BYTES),
            "--config", "-",
        ],
        input=config,
        capture_output=True,
        timeout=timeout_s + 2,
    )
    if result.returncode != 0:
        # With --fail, curl exits 22 and reports only the HTTP status on
        # stderr. Preserve the status as typed data without leaking a signed
        # URL or response body into logs.
        status_match = re.search(
            rb"(?:error|status)(?::| code)?\s+(\d{3})\b",
            bytes(result.stderr or b""),
            flags=re.IGNORECASE,
        )
        status = int(status_match.group(1)) if status_match else 0
        raise _R2HTTPError(result.returncode, status)
    if len(result.stdout) > _MAX_R2_COMPRESSED_BYTES:
        raise ValueError("R2 object exceeds 8 MiB")
    return result.stdout


def _r2_list_objects(cli: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    """Bounded LIST transport; fake/unit-test clients retain native calls."""
    presign = getattr(cli, "generate_presigned_url", None)
    if not callable(presign):
        return cli.list_objects_v2(**kwargs)

    import xml.etree.ElementTree as ET
    from urllib.parse import unquote

    url = presign(
        "list_objects_v2",
        Params=kwargs,
        ExpiresIn=60,
    )
    root = ET.fromstring(_r2_curl_bytes(url))

    def local_name(element: Any) -> str:
        return str(element.tag).rsplit("}", 1)[-1]

    def child_text(element: Any, name: str) -> str:
        for child in element:
            if local_name(child) == name:
                return str(child.text or "")
        return ""

    contents = []
    is_truncated = False
    next_token = ""
    for element in root.iter():
        name = local_name(element)
        if name == "Contents":
            key = child_text(element, "Key")
            if key:
                # Botocore requests EncodingType=url for LIST and normally
                # decodes this on our behalf. The bounded XML transport must
                # mirror that behavior before using the key in GET requests.
                contents.append({"Key": unquote(key)})
        elif name == "IsTruncated":
            is_truncated = str(element.text or "").strip().lower() == "true"
        elif name == "NextContinuationToken":
            next_token = str(element.text or "")
    return {
        "Contents": contents,
        "IsTruncated": is_truncated,
        "NextContinuationToken": next_token,
    }


def _r2_get_object(cli: Any, *, bucket: str, key: str) -> bytes:
    """Bounded GET transport; fake/unit-test clients retain native calls."""
    if isinstance(cli, _PublicR2Client):
        from urllib.parse import quote

        if _window_number_from_key(key) is None:
            raise ValueError("unsupported public archive key")
        url = f"{cli.base_url}/{quote(key, safe='/-._')}"
        response = _http_client().get(
            url,
            timeout=10.0,
            headers={
                "Accept": "application/gzip, application/octet-stream",
                "User-Agent": "reliquary-fleet",
            },
        )
        if response.status_code == 404:
            raise _R2ObjectNotFound(key)
        response.raise_for_status()
        raw = bytes(response.content)
        if len(raw) > _MAX_R2_COMPRESSED_BYTES:
            raise ValueError("R2 object exceeds 8 MiB")
        return raw
    presign = getattr(cli, "generate_presigned_url", None)
    if not callable(presign):
        try:
            body = cli.get_object(Bucket=bucket, Key=key)["Body"]
            raw = body.read(_MAX_R2_COMPRESSED_BYTES + 1)
            if len(raw) > _MAX_R2_COMPRESSED_BYTES:
                raise ValueError("R2 object exceeds 8 MiB")
            return raw
        except Exception as exc:
            response = getattr(exc, "response", {})
            error = response.get("Error", {}) if isinstance(response, dict) else {}
            code = str(error.get("Code") or "") if isinstance(error, dict) else ""
            status = response.get("ResponseMetadata", {}) if isinstance(response, dict) else {}
            http_status = (
                status.get("HTTPStatusCode") if isinstance(status, dict) else None
            )
            if code in {"NoSuchKey", "NotFound", "404"} or http_status == 404:
                raise _R2ObjectNotFound(key) from exc
            raise
    url = presign(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=60,
    )
    try:
        return _r2_curl_bytes(url)
    except _R2HTTPError as exc:
        if exc.http_status == 404:
            raise _R2ObjectNotFound(key) from exc
        raise


def _parse_archive_lifecycle(data: Any, *, window_n: int) -> dict[str, Any]:
    """Classify an R2 terminal before any reward/slot fields are read."""
    if not isinstance(data, dict):
        raise ValueError("R2 archive must be an object")
    explicit = "window_status" in data
    status = data.get("window_status") if explicit else "completed"
    if status not in {"completed", "aborted"}:
        raise ValueError(f"unknown R2 window_status: {status!r}")
    if explicit:
        raw_window = data.get("window_start")
        if (
            isinstance(raw_window, bool)
            or not isinstance(raw_window, int)
            or raw_window != window_n
        ):
            raise ValueError("explicit R2 lifecycle window identity is invalid")
    elif data.get("window_start") is not None and data.get("window_start") != window_n:
        raise ValueError("legacy R2 window identity is invalid")

    failure_stage = data.get("failure_stage")
    failure_type = data.get("failure_type")
    if failure_stage is not None and not isinstance(failure_stage, str):
        raise ValueError("R2 failure_stage must be a string or null")
    if failure_type is not None and not isinstance(failure_type, str):
        raise ValueError("R2 failure_type must be a string or null")
    if explicit and status == "completed":
        if not isinstance(data.get("batch"), list) or not isinstance(
            data.get("rewards_by_hotkey"), dict
        ):
            raise ValueError("completed R2 archive is missing legacy reward invariants")
    archive_schema_version = _optional_nonnegative_int(
        data.get("archive_schema_version")
    )
    return {
        "window_status": status,
        "terminal_data_present": True,
        "reward_data_present": bool(
            status == "completed"
            and isinstance(data.get("rewards_by_hotkey"), dict)
        ),
        "failure_stage": str(failure_stage or ""),
        "failure_type": str(failure_type or ""),
        "lifecycle_explicit": explicit,
        "archive_schema_version": archive_schema_version or 0,
    }


def _parse_window_object(key: str, raw: bytes) -> Optional[WindowSummary]:
    """Decode a single R2 window object into a WindowSummary.

    We strip every field we don't actually use — raw R2 archives include
    full proof/rollout payloads (~180 KB per window). We need only:
      batch[i]: hotkey, response_time, prompt_idx, sigma, signed_round
      rejected[i]: hotkey, reason
    Result: ~1 KB per window vs 180 KB → 99.5% reduction in cache size.
    """
    try:
        if len(raw) > _MAX_R2_COMPRESSED_BYTES:
            raise ValueError("R2 object exceeds compressed size limit")
        if key.endswith(".gz"):
            with gzip.GzipFile(fileobj=io.BytesIO(raw), mode="rb") as archive:
                raw = archive.read(_MAX_R2_DECOMPRESSED_BYTES + 1)
            if len(raw) > _MAX_R2_DECOMPRESSED_BYTES:
                raise ValueError("R2 object exceeds decompressed size limit")
        elif len(raw) > _MAX_R2_DECOMPRESSED_BYTES:
            raise ValueError("R2 object exceeds decompressed size limit")
        d = json.loads(raw)
        n = int(key.split("window-")[1].split(".")[0])
        lifecycle = _parse_archive_lifecycle(d, window_n=n)
        if lifecycle["window_status"] == "aborted":
            return WindowSummary(
                n=n,
                ours=0,
                total_batch=0,
                rt_first=0.0,
                rejects={},
                window_status="aborted",
                terminal_data_present=True,
                reward_data_present=False,
                failure_stage=lifecycle["failure_stage"],
                failure_type=lifecycle["failure_type"],
                lifecycle_explicit=lifecycle["lifecycle_explicit"],
                archive_schema_version=lifecycle["archive_schema_version"],
            )
        full_batch = d.get("batch", []) if isinstance(d.get("batch"), list) else []
        full_runners = d.get("runners_up", []) if isinstance(d.get("runners_up"), list) else []
        full_rejected = d.get("rejected", []) if isinstance(d.get("rejected"), list) else []

        # Keep proof/selection observability, but discard heavyweight prompt,
        # completion text, token and rollout payloads.
        entry_fields = (
            "hotkey", "response_time", "prompt_idx", "env_name", "sigma",
            "merkle_root", "arrival_ts", "decision_ts",
            "submitted_drand_round", "arrival_drand_round", "drand_delta",
            "seal_trigger_round", "prompt_hash_lead", "canonical_rank",
            "accepted_into_pool", "selected_for_batch", "rewarded",
            "reward_amount", "batch_filled_reason", "reject_stage",
            "reject_reason", "reason", "reward_vector", "truncated_count",
            "reward_shape", "sketch_diff_max", "lp_dev_max", "dist_q10_min",
            "claimed_checkpoint_hash", "rollout_hashes",
        )

        def _slim(entry: Any) -> dict[str, Any]:
            if not isinstance(entry, dict):
                return {}
            return {name: entry.get(name) for name in entry_fields if name in entry}

        slim_batch = [row for row in (_slim(b) for b in full_batch) if row]
        slim_runners = [row for row in (_slim(b) for b in full_runners) if row]
        slim_rejected = [row for row in (_slim(r) for r in full_rejected) if row]
        for row in slim_rejected:
            row["reason"] = str(
                row.get("reason") or row.get("reject_reason") or "?"
            )
        ours = sum(1 for b in slim_batch if b.get("hotkey") in OUR_SS58)
        rejects = Counter(
            r["reason"] for r in slim_rejected if r["hotkey"] in OUR_SS58
        )
        reward_data_present = lifecycle["reward_data_present"]
        rewards_raw = d.get("rewards_by_hotkey") or {}
        rewards: dict[str, float] = {}
        reward_ours = 0.0
        reward_total = 0.0
        if isinstance(rewards_raw, dict):
            for hk, value in rewards_raw.items():
                try:
                    v = float(value)
                except Exception:
                    continue
                rewards[str(hk)] = v
                reward_total += v
                if hk in OUR_SS58:
                    reward_ours += v

        envs_raw = d.get("environments")
        if isinstance(envs_raw, list):
            environments = [str(x) for x in envs_raw if str(x)]
        else:
            legacy_env = str(d.get("environment") or "")
            environments = [legacy_env] if legacy_env else []
        for row in slim_batch + slim_runners + slim_rejected:
            env = str(row.get("env_name") or "")
            if env and env not in environments:
                environments.append(env)

        environment_counts: dict[str, dict[str, int]] = {}
        ours_by_environment: dict[str, int] = {}
        rt_first_by_environment: dict[str, float] = {}
        explicit_reward_ours_by_environment: dict[str, float] = {}
        our_reward_rows = 0
        explicit_reward_amounts_complete = True
        for env in environments:
            environment_counts[env] = {
                "batch": 0, "runners_up": 0, "rejected": 0,
                "selected": 0, "rewarded": 0,
            }
            ours_by_environment[env] = 0
        for name, rows in (
            ("batch", slim_batch),
            ("runners_up", slim_runners),
            ("rejected", slim_rejected),
        ):
            for row in rows:
                env = str(row.get("env_name") or (environments[0] if environments else "unknown"))
                counts = environment_counts.setdefault(
                    env,
                    {"batch": 0, "runners_up": 0, "rejected": 0,
                     "selected": 0, "rewarded": 0},
                )
                counts[name] += 1
                if row.get("selected_for_batch") is True:
                    counts["selected"] += 1
                if row.get("rewarded") is True:
                    counts["rewarded"] += 1
                if row.get("hotkey") in OUR_SS58:
                    if name == "batch":
                        ours_by_environment[env] = ours_by_environment.get(env, 0) + 1
                    if row.get("rewarded") is True:
                        our_reward_rows += 1
                        amount = row.get("reward_amount")
                        if amount is None:
                            explicit_reward_amounts_complete = False
                        else:
                            try:
                                explicit_reward_ours_by_environment[env] = (
                                    explicit_reward_ours_by_environment.get(env, 0.0)
                                    + float(amount)
                                )
                            except (TypeError, ValueError):
                                explicit_reward_amounts_complete = False
                if name == "batch":
                    try:
                        response_time = float(row["response_time"])
                    except (KeyError, TypeError, ValueError):
                        continue
                    if response_time < 0:
                        continue
                    previous = rt_first_by_environment.get(env)
                    if previous is None or response_time < previous:
                        rt_first_by_environment[env] = response_time

        explicit_reward_total = sum(explicit_reward_ours_by_environment.values())
        reward_ours_by_environment_exact = bool(
            reward_data_present
            and our_reward_rows > 0
            and explicit_reward_amounts_complete
            and abs(explicit_reward_total - reward_ours) <= 1e-9
        )
        reward_ours_by_environment = (
            explicit_reward_ours_by_environment
            if reward_ours_by_environment_exact
            else {}
        )
        if not reward_ours_by_environment_exact:
            (
                reward_ours_by_environment,
                reward_ours_by_environment_exact,
            ) = _infer_single_environment_rewards(
                rewards,
                slim_batch + slim_runners + slim_rejected,
                reward_data_present=reward_data_present,
            )

        def _float_map(value: Any) -> dict[str, float]:
            if not isinstance(value, dict):
                return {}
            return {str(k): _as_float(v) for k, v in value.items()}

        def _int_map(value: Any) -> dict[str, int]:
            if not isinstance(value, dict):
                return {}
            return {str(k): _as_int(v) for k, v in value.items()}

        seal_drain_by_environment: dict[str, dict[str, Any]] = {}
        raw_seal_drains = d.get("auction_seal_drain_by_environment")
        if isinstance(raw_seal_drains, dict):
            for environment, raw_drain in raw_seal_drains.items():
                drain = _normalize_seal_drain(raw_drain)
                if drain:
                    seal_drain_by_environment[str(environment)] = drain

        rt_first = (
            round(min(rt_first_by_environment.values()), 1)
            if rt_first_by_environment else 0.0
        )
        return WindowSummary(
            n=n, ours=ours, total_batch=len(slim_batch), rt_first=rt_first,
            rejects=dict(rejects), reward_ours=reward_ours,
            reward_total=reward_total, rewards_by_hotkey=rewards,
            reward_data_present=reward_data_present,
            window_status=lifecycle["window_status"],
            terminal_data_present=lifecycle["terminal_data_present"],
            failure_stage=lifecycle["failure_stage"],
            failure_type=lifecycle["failure_type"],
            lifecycle_explicit=lifecycle["lifecycle_explicit"],
            archive_schema_version=lifecycle["archive_schema_version"],
            auction_seal_drain_by_environment=seal_drain_by_environment,
            environments=environments, environment_counts=environment_counts,
            ours_by_environment=ours_by_environment,
            rt_first_by_environment=rt_first_by_environment,
            reward_ours_by_environment=reward_ours_by_environment,
            reward_ours_by_environment_exact=reward_ours_by_environment_exact,
            batch=slim_batch, runners_up=slim_runners,
            rejected=slim_rejected,
            force_seal_reason=str(d.get("force_seal_reason") or ""),
            validator_hotkey=str(d.get("validator_hotkey") or ""),
            randomness=str(d.get("randomness") or ""),
            rewarded_but_not_selected_by_hotkey=_float_map(
                d.get("rewarded_but_not_selected_by_hotkey")
            ),
            late_drops=dict(d.get("late_drops") or {})
                if isinstance(d.get("late_drops"), dict) else {},
            reject_summary=_int_map(d.get("reject_summary")),
            grader_failures=_int_map(d.get("grader_failures")),
            training_quarantine=dict(d.get("training_quarantine") or {})
                if isinstance(d.get("training_quarantine"), dict) else {},
            difficulty_auction_shadow=dict(d.get("difficulty_auction_shadow") or {})
                if isinstance(d.get("difficulty_auction_shadow"), dict) else {},
            server_reject_summary=dict(d.get("server_reject_summary") or {})
                if isinstance(d.get("server_reject_summary"), dict) else {},
            logical_group_dedup=dict(d.get("logical_group_dedup") or {})
                if isinstance(d.get("logical_group_dedup"), dict) else {},
            training_accumulator=dict(d.get("training_accumulator") or {})
                if isinstance(d.get("training_accumulator"), dict) else {},
        )
    except Exception:
        return None


def _window_number_from_key(key: Any) -> Optional[int]:
    match = re.fullmatch(r"reliquary/dataset/window-(\d+)\.json\.gz", str(key))
    return int(match.group(1)) if match else None


def _list_all_window_candidates(
    cli: Any, *, bucket: str, prefix: str
) -> dict[int, str]:
    """Exhaustively LIST the archive prefix and return numeric key mappings.

    S3 ordering is lexicographic, so a numeric early-stop heuristic can skip
    live keys at digit boundaries.  This deliberately follows every
    continuation token and is used only when the validator cannot provide its
    authoritative ``archive_last_uploaded_window`` health field.
    """
    by_window: dict[int, str] = {}
    token = ""
    seen_tokens: set[str] = set()
    while True:
        kwargs: dict[str, Any] = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": 1000,
        }
        if token:
            kwargs["ContinuationToken"] = token
        response = _r2_list_objects(cli, kwargs)
        for item in response.get("Contents", []) or []:
            if not isinstance(item, dict):
                continue
            key = str(item.get("Key") or "")
            window_n = _window_number_from_key(key)
            if window_n is not None:
                by_window[window_n] = key
        if not response.get("IsTruncated"):
            break
        next_token = str(response.get("NextContinuationToken") or "")
        if not next_token or next_token in seen_tokens:
            raise RuntimeError("R2 LIST pagination stalled")
        seen_tokens.add(next_token)
        token = next_token
    return by_window


def _record_r2_coverage(
    history: int,
    numeric_numbers: list[int],
    *,
    source: str,
    absent_numbers: Optional[set[int]] = None,
) -> None:
    global _R2_DISCOVERY_SOURCE, _R2_COVERAGE_REQUESTED
    global _R2_COVERAGE_NUMERIC_EXPECTED, _R2_COVERAGE_EXPECTED
    global _R2_COVERAGE_CACHED, _R2_COVERAGE_EXACT
    global _R2_COVERAGE_REWARD_EXPECTED, _R2_COVERAGE_TERMINAL
    global _R2_COVERAGE_ABORTED, _R2_COVERAGE_ABSENT
    global _R2_COVERAGE_MISSING, _R2_COVERAGE_REWARD_MISSING
    global _R2_COVERAGE_COMPLETE, _R2_COVERAGE_LIFECYCLE_EXPLICIT
    numeric = list(dict.fromkeys(numeric_numbers))
    numeric_set = set(numeric)
    absent = sorted(numeric_set & set(absent_numbers or set()))
    absent_set = set(absent)
    expected = [n for n in numeric if n not in absent_set]
    cached = [n for n in expected if n in _window_cache]
    terminal = [
        n for n in cached if _window_cache[n].terminal_data_present
    ]
    aborted = sorted(
        n for n in terminal if _window_cache[n].window_status == "aborted"
    )
    reward_expected = [
        n for n in terminal if _window_cache[n].window_status == "completed"
    ]
    exact = [n for n in reward_expected if _window_cache[n].reward_data_present]
    terminal_set = set(terminal)
    exact_set = set(exact)
    lifecycle_explicit = any(
        _window_cache[n].lifecycle_explicit for n in terminal
    )
    _R2_DISCOVERY_SOURCE = source
    _R2_COVERAGE_REQUESTED = history
    _R2_COVERAGE_NUMERIC_EXPECTED = len(numeric)
    _R2_COVERAGE_EXPECTED = len(expected)
    _R2_COVERAGE_CACHED = len(cached)
    _R2_COVERAGE_TERMINAL = len(terminal)
    _R2_COVERAGE_REWARD_EXPECTED = len(reward_expected)
    _R2_COVERAGE_EXACT = len(exact)
    _R2_COVERAGE_ABORTED = aborted
    _R2_COVERAGE_ABSENT = absent
    _R2_COVERAGE_LIFECYCLE_EXPLICIT = lifecycle_explicit
    if lifecycle_explicit:
        _R2_COVERAGE_MISSING = [n for n in expected if n not in terminal_set]
        _R2_COVERAGE_REWARD_MISSING = [
            n for n in reward_expected if n not in exact_set
        ]
        _R2_COVERAGE_COMPLETE = bool(
            history > 0
            and len(numeric) == history
            and len(terminal) == len(expected)
        )
    else:
        # Preserve the complete legacy status/API exactly: before terminal
        # lifecycle existed, completeness meant every archive had exact
        # reward data and the missing list reflected that same contract.
        _R2_COVERAGE_MISSING = [n for n in expected if n not in exact_set]
        _R2_COVERAGE_REWARD_MISSING = []
        _R2_COVERAGE_COMPLETE = bool(
            history > 0
            and len(numeric) == history
            and len(exact) == len(expected)
        )


_ARCHIVE_HEALTH_UNSET = object()


def fetch_recent_windows(
    history: int,
    *,
    archive_health: Any = _ARCHIVE_HEALTH_UNSET,
    allow_list_fallback: bool | None = None,
) -> list[WindowSummary]:
    """Return the latest configured archive history with explicit coverage.

    The validator's health endpoint is the primary index: its
    ``archive_last_uploaded_window`` value lets us construct exact numeric R2
    keys and avoid S3's lexicographic ordering entirely. Exhaustive LIST is
    available only when explicitly enabled; package configs otherwise serve
    the last-good cache until the index returns. Immutable parsed windows stay
    cached, while cold/schema upgrades warm in a bounded batch.
    """
    global _R2_LAST_ATTEMPT_AT, _R2_LAST_SUCCESS_AT, _R2_LAST_ERROR
    global _R2_LATEST_WINDOW, _R2_LATEST_WINDOW_SEEN_AT
    history = max(0, _as_int(history))
    list_fallback_enabled = (
        R2_ALLOW_LIST_FALLBACK
        if allow_list_fallback is None
        else bool(allow_list_fallback)
    )
    _R2_LAST_ATTEMPT_AT = time.time()
    if history <= 0:
        _record_r2_coverage(0, [], source="disabled")
        return []

    cli = r2()
    if not cli:
        _R2_LAST_ERROR = "R2 unconfigured"
        latest_cached = max(_window_cache, default=0)
        expected = list(range(max(0, latest_cached - history + 1), latest_cached + 1))
        _record_r2_coverage(history, expected, source="cache_only")
        return sorted(_window_cache.values(), key=lambda w: -w.n)[:history]

    try:
        bucket = os.environ["R2_BUCKET"]
        prefix = "reliquary/dataset/window-"
        latest_n = 0
        archive_queue_depth: Optional[int] = None
        archive_queue_oldest_window: Optional[int] = None
        confirmed_absent: set[int] = set()
        try:
            if archive_health is _ARCHIVE_HEALTH_UNSET:
                health = _http_json("health", timeout=3.0)
            elif isinstance(archive_health, dict):
                health = archive_health
            else:
                health = {}
            latest_n = _as_int(health.get("archive_last_uploaded_window"))
            if "archive_queue_depth" in health:
                queue_depth = _as_int(health.get("archive_queue_depth"), -1)
                if queue_depth >= 0:
                    archive_queue_depth = queue_depth
            if "archive_queue_oldest_window" in health:
                oldest_window = _as_int(
                    health.get("archive_queue_oldest_window"), -1
                )
                if oldest_window >= 0:
                    archive_queue_oldest_window = oldest_window
        except Exception:
            latest_n = 0

        if latest_n > 0:
            source = "validator_health"
            expected_numbers = list(
                range(max(0, latest_n - history + 1), latest_n + 1)
            )
            candidates = [
                (f"{prefix}{window_n}.json.gz", window_n)
                for window_n in reversed(expected_numbers)
            ]
        else:
            if isinstance(cli, _PublicR2Client):
                list_fallback_enabled = False
            if not list_fallback_enabled:
                source = "cache_without_archive_index"
                latest_cached = max(_window_cache, default=0)
                expected_numbers = list(
                    range(
                        max(0, latest_cached - history + 1),
                        latest_cached + 1,
                    )
                )
                _R2_LAST_ERROR = "archive index unavailable; serving cache"
                _record_r2_coverage(history, expected_numbers, source=source)
                return sorted(
                    _window_cache.values(), key=lambda window: -window.n
                )[:history]
            source = "r2_list_exhaustive"
            listed = _list_all_window_candidates(
                cli, bucket=bucket, prefix=prefix
            )
            if not listed:
                _R2_LAST_ERROR = "no window archives"
                _record_r2_coverage(history, [], source=source)
                return sorted(_window_cache.values(), key=lambda w: -w.n)[:history]
            latest_n = max(listed)
            expected_numbers = list(
                range(max(0, latest_n - history + 1), latest_n + 1)
            )
            expected_set = set(expected_numbers)
            listed_in_range = expected_set & set(listed)
            # LIST is authoritative for the snapshot. Preserve any cached row
            # if a transiently inconsistent LIST omitted it; only a number
            # absent from both LIST and cache is a confirmed archive gap.
            confirmed_absent = (
                expected_set - listed_in_range - set(_window_cache)
            )
            candidates = [
                (listed[window_n], window_n)
                for window_n in reversed(expected_numbers)
                if window_n in listed
            ]
        _R2_LATEST_WINDOW = latest_n
    except Exception as exc:
        # Network/auth error — return what's already cached so the dashboard
        # doesn't go blank, while keeping readiness explicitly degraded.
        _R2_LAST_ERROR = type(exc).__name__
        latest_cached = max(_window_cache, default=0)
        expected = list(range(max(0, latest_cached - history + 1), latest_cached + 1))
        _record_r2_coverage(history, expected, source="cache_after_error")
        return sorted(_window_cache.values(), key=lambda w: -w.n)[:history]

    # Fetch anything absent, not terminal, or missing a completed archive's
    # exact reward map. Aborted tombstones are immutable terminal objects and
    # must not be downloaded forever just because rewards are intentionally
    # absent. Limit cold/legacy
    # migration to a bounded chunk; later 30-second refreshes continue where
    # this one stopped. Newest-first makes live observability useful early.
    pending_fetch = [
        (k, n) for k, n in candidates
        if (
            n not in _window_cache
            or not _window_cache[n].terminal_data_present
            or (
                _window_cache[n].window_status == "completed"
                and not _window_cache[n].reward_data_present
            )
        )
    ]
    to_fetch = pending_fetch[: max(1, int(R2_FETCH_BATCH_SIZE))]
    fetch_errors: list[str] = []
    not_found: list[int] = []
    fetched_count = 0

    def fetch_one(
        item: tuple[str, int],
    ) -> tuple[int, Optional[WindowSummary], str, bool]:
        key, n = item
        try:
            raw = _r2_get_object(cli, bucket=bucket, key=key)
            ws = _parse_window_object(key, raw)
            if ws is not None:
                return n, ws, "", False
            return n, None, f"w{n}:parse", False
        except _R2ObjectNotFound:
            return n, None, "", True
        except Exception as exc:
            return n, None, f"w{n}:{type(exc).__name__}", False

    if to_fetch:
        workers = min(max(1, int(R2_FETCH_WORKERS)), len(to_fetch))
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            for n, ws, error, object_absent in executor.map(fetch_one, to_fetch):
                if ws is not None:
                    _window_cache[n] = ws
                    fetched_count += 1
                elif object_absent:
                    not_found.append(n)
                elif error:
                    fetch_errors.append(error)

    if source == "validator_health":
        for window_n in not_found:
            # A legacy/inexact cache row proves the object existed, so a new
            # 404 must remain unresolved. For uncached rows, queue telemetry
            # distinguishes a permanent skipped window from an upload still
            # pending. When queue telemetry is absent, fail closed.
            if window_n in _window_cache:
                continue
            # The validator explicitly advertised latest_n as successfully
            # uploaded. A 404 for that exact object is an inconsistent feed,
            # not evidence of an intentionally skipped window.
            if window_n >= latest_n:
                continue
            if archive_queue_depth == 0:
                confirmed_absent.add(window_n)
            elif (
                archive_queue_depth is not None
                and archive_queue_depth > 0
                and archive_queue_oldest_window is not None
                and window_n < archive_queue_oldest_window
            ):
                confirmed_absent.add(window_n)

    _record_r2_coverage(
        history,
        expected_numbers,
        source=source,
        absent_numbers=confirmed_absent,
    )
    remaining_after_batch = len(_R2_COVERAGE_MISSING)
    reward_missing_after_batch = len(_R2_COVERAGE_REWARD_MISSING)
    now = time.time()
    if fetch_errors:
        _R2_LAST_ERROR = "; ".join(fetch_errors[:4])
    elif remaining_after_batch or reward_missing_after_batch:
        _R2_LAST_SUCCESS_AT = now
        _R2_LATEST_WINDOW_SEEN_AT = now
        if _R2_COVERAGE_LIFECYCLE_EXPLICIT:
            _R2_LAST_ERROR = (
                f"terminal cache warming:{remaining_after_batch} remaining;"
                f"reward data warming:{reward_missing_after_batch} remaining"
            )
        else:
            _R2_LAST_ERROR = f"reward cache warming:{remaining_after_batch} remaining"
    else:
        _R2_LAST_SUCCESS_AT = now
        _R2_LATEST_WINDOW_SEEN_AT = now
        _R2_LAST_ERROR = ""

    # Trim cache to 220 entries — matches the validator's archive lookback
    # (216) so our locally-replayed EMA tracks the real on-chain numbers.
    # 216 windows × ~5 KB raw batch = ~1 MB, fits comfortably.
    cache_limit = max(220, history + 4)
    if len(_window_cache) > cache_limit:
        for old_n in sorted(_window_cache.keys())[:-cache_limit]:
            _window_cache.pop(old_n, None)

    # Persist updated cache to disk so dashboard restarts skip cold warmup.
    # Only save if we actually downloaded new content this round.
    if fetched_count:
        window_cache_save()

    # Return the configured numeric range from cache.  In exhaustive-fallback
    # mode this also preserves an immutable cached archive if a transient LIST
    # response omitted its key.
    return [
        _window_cache[n]
        for n in reversed(expected_numbers)
        if n in _window_cache
    ]


# --- Renderers ---------------------------------------------------------------
def render_fleet_table(boxes: list[BoxState]) -> Table:
    t = Table(
        show_header=True, header_style="bold",
        title="Fleet — per-box health (refreshed continuously)",
        title_style="bold white",
        expand=True,
    )
    t.add_column("box",       style="bold", width=6)
    t.add_column("hotkey",    width=10)
    t.add_column("alive",     width=5, justify="center")
    t.add_column("GPU mem",   width=18)
    t.add_column("util",      width=5, justify="right")
    t.add_column("last evt",  width=10)
    t.add_column("acpt/30m",  width=9, justify="right")
    t.add_column("rej/30m",   width=8, justify="right")
    t.add_column("last rej",  width=20)
    t.add_column("err",       width=18)
    for b in boxes:
        # Color memory by % full
        pct = b.gpu_mem_mb * 100 // max(b.gpu_total_mb, 1)
        mem_color = (
            "red" if pct >= 95 else
            "yellow" if pct >= 88 else
            "green"
        )
        mem_txt = Text(f"{b.gpu_mem_mb//1024:>3}/{b.gpu_total_mb//1024} GB ({pct:>2}%)", style=mem_color)
        alive_mark = Text("●", style="green") if b.proc_alive else Text("○", style="red")
        # ACPT highlighting
        acpt_txt = Text(str(b.acpt_30m), style="bold green" if b.acpt_30m >= 5 else ("yellow" if b.acpt_30m >= 1 else "red"))
        rej_txt = Text(str(b.rej_30m), style="red" if b.rej_30m >= 3 else ("yellow" if b.rej_30m >= 1 else ""))
        t.add_row(
            Text(b.label, style=b.color),
            b.hotkey,
            alive_mark,
            mem_txt,
            f"{b.gpu_util}%",
            b.last_event_at,
            acpt_txt,
            rej_txt,
            b.last_reject_reason or "—",
            Text(b.error, style="red") if b.error else "",
        )
    return t


def render_validator(vs: ValidatorState) -> Panel:
    if vs.error and vs.error != "ssh-state":
        body = Text(f"validator unreachable: {vs.error}", style="red")
    else:
        if vs.window_environments:
            valid = sum(
                _as_int(row.get("valid_submissions_count"))
                for row in vs.window_environments.values()
            )
        else:
            valid = vs.valid
        target = sum(vs.environment_targets.values()) or vs.batch_size or 8
        body = Text.assemble(
            ("window ", "dim"), (str(vs.window), "bold yellow"),
            ("  state=", "dim"), (vs.state, "bold cyan"),
            ("  valid=", "dim"), (f"{valid}/{target}", "bold magenta"),
        )
    return Panel(body, title="[bold]Validator[/bold]", border_style="blue")


def render_windows(history: list[WindowSummary]) -> Panel:
    if not history:
        return Panel(Text("R2 unavailable (set R2_ENDPOINT/R2_BUCKET)", style="dim"),
                     title="[bold]Recent windows[/bold]", border_style="blue")
    rows = []
    completed = [w for w in history if w.window_status == "completed"]
    total_ours = sum(w.ours for w in completed)
    total_slots = sum(w.total_batch for w in completed)
    pct = total_ours * 100 // max(total_slots, 1)
    for w in history:
        if w.window_status == "aborted":
            failure = "/".join(
                value for value in (w.failure_stage, w.failure_type) if value
            ) or "unspecified"
            rows.append(
                f"w{w.n}: [bold red]aborted[/] [dim]{failure} "
                "(terminal; no reward evidence)[/]"
            )
            continue
        pct_w = w.ours * 100 // max(w.total_batch, 1)
        share_color = (
            "green" if pct_w >= 60 else
            "yellow" if pct_w >= 30 else
            "red"
        )
        rej_str = (
            " " + " ".join(f"[red]{k}={v}[/red]" for k, v in w.rejects.items())
            if w.rejects else ""
        )
        rows.append(
            f"w{w.n}: [bold {share_color}]{w.ours}/{w.total_batch}[/] "
            f"({pct_w:>3}%)  rt₀=[dim]{w.rt_first:>5.1f}s[/]{rej_str}"
        )
    rows.append("")
    rows.append(
        f"[bold]TOTAL: {total_ours}/{total_slots} = "
        f"{'[green]' if pct >= 50 else '[yellow]' if pct >= 30 else '[red]'}{pct}%[/][/]"
    )
    return Panel(
        "\n".join(rows),
        title=f"[bold]Last {len(history)} windows[/bold]",
        border_style="blue",
    )


def render_event_tail(boxes: list[BoxState], limit: int = 18) -> Panel:
    """Merge recent events from all boxes, sorted by line text (timestamp prefix
    sorts naturally), keep last `limit`."""
    merged = []
    for b in boxes:
        for line in b.recent_lines:
            merged.append((b.label, b.color, line))
    # Sort by the timestamp embedded in the journal line.
    def keyfn(rec):
        line = rec[2]
        bits = line.split(maxsplit=3)
        if len(bits) >= 3:
            return bits[0] + " " + bits[1] + " " + bits[2]
        return line
    merged.sort(key=keyfn)
    tail = merged[-limit:]
    if not tail:
        body = Text("(no events yet)", style="dim")
    else:
        out = []
        for label, color, line in tail:
            # Strip the host prefix (everything up to and including ": " on a
            # line like "May 09 22:29:58 host reliquary-miner-pro[27481]: msg")
            msg = line
            if ": " in msg:
                msg = msg.split(": ", 1)[1] if msg.count(":") >= 3 else msg
            # Try to keep the inner timestamp (the python "2026-05-09 22:29:58") for context.
            color_token = "green" if "ACCEPTED" in msg else (
                "red" if "ERROR" in msg or "rejected" in msg or "FAIL" in msg
                else "white"
            )
            out.append(f"[{color}][{label:>5}][/{color}]  [{color_token}]{msg[:180]}[/{color_token}]")
        body = "\n".join(out)
    return Panel(body, title=f"[bold]Live event tail (latest {limit})[/bold]",
                 border_style="white")


def build_layout(boxes, vs, windows) -> Group:
    fleet_panel = Panel(render_fleet_table(boxes), border_style="white")
    # Table needs: 1 title + 2 border + 2 header + N rows + panel borders = N + 7
    top = Layout(name="top", size=len(boxes) + 7)
    top.update(fleet_panel)

    mid = Layout(name="mid", size=12)
    mid.split_row(
        Layout(render_validator(vs), ratio=1),
        Layout(render_windows(windows), ratio=2),
    )

    tail = Layout(name="tail", minimum_size=12)
    tail.update(render_event_tail(boxes))

    root = Layout()
    root.split_column(top, mid, tail)
    return root


# --- Main loop ---------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", type=float, default=5.0,
                    help="seconds between full polls (default 5)")
    ap.add_argument("--history", type=int, default=8,
                    help="how many recent R2 windows to show (default 8)")
    args = ap.parse_args()

    boxes = [
        BoxState(
            alias=alias, hotkey=hk, label=label, color=color, unit=unit,
            env_file=FLEET_ENV_FILES.get(label, ""),
            unit_candidates=tuple(
                tuple(spec) for spec in FLEET_UNIT_CANDIDATES.get(label, [])
            ),
            unit_controller_config_paths=tuple(
                (str(unit), str(path))
                for unit, path in FLEET_CONTROLLER_CONFIG_PATHS.get(
                    label, {}
                ).items()
            ),
            coordinated_units=tuple(
                FLEET_COORDINATED_UNITS.get(label, [])
            ),
            host_unit_allowlist=tuple(
                FLEET_HOST_UNIT_ALLOWLIST.get(label, [])
            ),
            standalone_telemetry_path=str(
                FLEET_STANDALONE_TELEMETRY.get(label, {}).get(
                    "telemetry_path"
                ) or ""
            ),
            standalone_runtime_manifest_path=str(
                FLEET_STANDALONE_TELEMETRY.get(label, {}).get(
                    "runtime_manifest_path"
                ) or ""
            ),
            active_unit_registry_path=str(
                FLEET_STANDALONE_TELEMETRY.get(label, {}).get(
                    "active_unit_registry_path"
                ) or ""
            ),
            standalone_supervisor_status_path=str(
                FLEET_STANDALONE_TELEMETRY.get(label, {}).get(
                    "supervisor_status_path"
                ) or ""
            ),
            standalone_telemetry_stale_seconds=float(
                FLEET_RELIQUARY_ONE.get(label, {}).get("stale_seconds")
                or FLEET_STANDALONE_TELEMETRY.get(label, {}).get(
                    "stale_seconds"
                )
                or 90.0
            ),
            miner_kind=(
                "reliquary_one"
                if label in FLEET_RELIQUARY_ONE
                else "legacy"
            ),
            reliquary_one_state_root=str(
                FLEET_RELIQUARY_ONE.get(label, {}).get("state_root") or ""
            ),
            standalone_certification_config=dict(
                FLEET_STANDALONE_CERTIFICATION.get(label, {})
            ),
            operator=str(
                FLEET_STANDALONE_TELEMETRY.get(label, {}).get("operator")
                or ""
            ),
        )
        for alias, hk, label, color, unit in FLEET
    ]
    vs = ValidatorState()
    windows: list[WindowSummary] = []

    console = Console()

    def poll_all():
        # Poll the boxes in parallel — SSH round-trip dominates wall clock.
        snapshots = list(boxes)

        def collect_snapshot(index: int, box: BoxState) -> None:
            snapshots[index] = collect_box_snapshot(box)

        threads = [
            threading.Thread(target=collect_snapshot, args=(index, box))
            for index, box in enumerate(boxes)
        ]
        for t in threads:
            t.start()
        # Poll validator + R2 in parallel with the SSH calls.
        v_thread = threading.Thread(target=fetch_validator, args=(vs,))
        v_thread.start()
        nonlocal windows
        new_windows = fetch_recent_windows(args.history)
        if new_windows:
            windows = new_windows
        for t in threads:
            t.join()
        v_thread.join()
        boxes[:] = snapshots
        recompute_validator_acpts(boxes, vs)

    # Initial poll (synchronous so first frame has data).
    poll_all()

    with Live(build_layout(boxes, vs, windows), console=console, refresh_per_second=4, screen=True) as live:
        last_poll = time.time()
        try:
            while True:
                live.update(build_layout(boxes, vs, windows))
                time.sleep(0.25)
                if time.time() - last_poll >= args.refresh:
                    poll_all()
                    last_poll = time.time()
        except KeyboardInterrupt:
            pass
    return 0


# --- R2 aggregators (called from web dashboard render path) ----------------
# All functions in this section operate on the cached `WindowSummary` list.
# They are pure CPU work over ~60KB of data — measured <1ms each, safe for
# the HTTP request path.

def aggregate_slot_ranks_by_environment(
    windows: list[WindowSummary],
) -> dict[str, dict[str, list[int]]]:
    """Return exact 1-based canonical-rank histograms split by environment."""
    out: dict[str, dict[str, list[int]]] = {
        label: {} for label in OUR_SS58.values()
    }
    for w in windows:
        for b in w.batch:
            label = OUR_SS58.get(b.get("hotkey", ""))
            if not label:
                continue
            rank = _as_int(b.get("canonical_rank"))
            if not 1 <= rank <= 8:
                # Do not guess from the combined batch index: multi-env
                # archives concatenate independent 8-slot canonical orders.
                continue
            env = str(b.get("env_name") or "unknown")
            dist = out[label].setdefault(env, [0] * 8)
            dist[rank - 1] += 1
    return out


def aggregate_slot_ranks(windows: list[WindowSummary]) -> dict[str, list[int]]:
    """Per-hotkey canonical-rank histogram, merged after per-env counting.

    Each environment resets ``canonical_rank`` to 1.  We first count those
    independent orders and only then merge their same-rank buckets for the
    compact dashboard panel.
    """
    by_environment = aggregate_slot_ranks_by_environment(windows)
    out: dict[str, list[int]] = {
        label: [0] * 8 for label in OUR_SS58.values()
    }
    for label, environments in by_environment.items():
        for dist in environments.values():
            for index, count in enumerate(dist):
                out[label][index] += count
    return out


def aggregate_sigma_histogram(windows: list[WindowSummary]) -> dict[str, int]:
    """Histogram of σ values across our ACCEPTED slots.

    σ is the prompt's expected reward (correct/M). Common buckets in DAPO/MATH:
      σ=0.433 (k=2 or 6), σ=0.484 (k=3 or 5), σ=0.5 (k=4). Skewed → prompt
    selection issue.
    """
    out: dict[str, int] = {}
    for w in windows:
        for b in w.batch:
            if b.get("hotkey") in OUR_SS58:
                key = f"{b.get('sigma', 0):.3f}"
                out[key] = out.get(key, 0) + 1
    return out


def _reward_vector_correct_count(value: Any) -> Optional[int]:
    """Count positive rollout rewards without guessing from symmetric sigma."""
    if isinstance(value, str):
        bits = value.strip()
        if not bits or any(bit not in "01" for bit in bits):
            return None
        return bits.count("1")
    if isinstance(value, (list, tuple)):
        count = 0
        for item in value:
            if isinstance(item, bool):
                count += int(item)
                continue
            try:
                count += int(float(item) > 0.0)
            except (TypeError, ValueError):
                return None
        return count
    return None


def aggregate_k_histogram_by_environment(
    windows: list[WindowSummary],
) -> dict[str, dict[int | str, int]]:
    """Exact k_correct histograms from reward vectors, split by environment."""
    out: dict[str, dict[int | str, int]] = {}
    for w in windows:
        for b in w.batch:
            if b.get("hotkey") in OUR_SS58:
                env = str(b.get("env_name") or "unknown")
                k: int | str = _reward_vector_correct_count(
                    b.get("reward_vector")
                )
                if k is None:
                    k = "unknown"
                histogram = out.setdefault(env, {})
                histogram[k] = histogram.get(k, 0) + 1
    return out


def aggregate_k_histogram(
    windows: list[WindowSummary],
) -> dict[int | str, int]:
    """Exact k_correct histogram; absent/malformed vectors stay unknown."""
    out: dict[int | str, int] = {}
    for histogram in aggregate_k_histogram_by_environment(windows).values():
        for k, count in histogram.items():
            out[k] = out.get(k, 0) + count
    return out


def aggregate_competitors(windows: list[WindowSummary], top_n: int = 8) -> list[dict]:
    """Other miners' performance — ranked by total batch slots over the window
    sample. Each entry: hotkey (truncated), uid (if available), slot_count,
    mean_rt, last_seen_window.
    """
    by_hk: dict[str, dict] = {}
    for w in windows:
        for b in w.batch:
            hk = b.get("hotkey", "")
            if not hk or hk in OUR_SS58:
                continue
            ent = by_hk.setdefault(hk, {
                "hotkey": hk,
                "slot_count": 0,
                "rt_sum": 0.0,
                "rt_n": 0,
                "last_seen": 0,
            })
            ent["slot_count"] += 1
            ent["rt_sum"] += b.get("response_time", 0)
            ent["rt_n"] += 1
            if w.n > ent["last_seen"]:
                ent["last_seen"] = w.n
    out = []
    for ent in by_hk.values():
        ent["mean_rt"] = ent["rt_sum"] / max(ent["rt_n"], 1)
        del ent["rt_sum"]
        out.append(ent)
    out.sort(key=lambda e: -e["slot_count"])
    return out[:top_n]


# --- EMA leaderboard ---------------------------------------------------------
# Mirrors the validator's `_replay_ema` exactly so the dashboard shows the
# *actual* weights that will be set on chain at the next 72-block submission,
# not just our short-term slot share.
#
# Constants pulled directly from Reliquary main. ``B_BATCH`` remains the
# legacy/single-environment UI fallback; exact EMA replay no longer divides by
# it because archive rewards are already normalized emission fractions.
EMA_ALPHA = 2.0 / (72 + 1)   # ≈ 0.0274
B_BATCH = 8                  # max slots per window
EMA_PRUNE = 1e-6             # validator drops miners below this


def compute_ema_leaderboard(windows: list[WindowSummary]) -> list[dict]:
    """Run the validator's exact EMA algorithm over our cached windows.

    Returns a sorted list of {hotkey, ema, slot_share_72, slot_count_72,
    slot_count_recent12, rank}. Higher rank = higher on-chain weight.

    Note: validator pulls up to 216 archives but the alpha-blend converges
    well within ~72 windows. We use whatever's in our cache (capped at 216).
    """
    # Sort archives oldest→newest exactly like the validator. Legacy cache
    # rows that discarded rewards cannot be reconstructed from ``batch``
    # under same-prompt splits/boundary fairness, so ignore them until the R2
    # fetcher replaces them with schema-v2 rows.
    sorted_windows = sorted(
        (w for w in windows if w.reward_data_present), key=lambda w: w.n
    )
    ema: dict[str, float] = {}
    slot_counts_total: dict[str, int] = {}
    total_slots = 0
    for w in sorted_windows:
        for entry in w.batch:
            hk = entry.get("hotkey", "")
            if not hk:
                continue
            slot_counts_total[hk] = slot_counts_total.get(hk, 0) + 1
        total_slots += w.total_batch
        contribs = {
            str(hk): _as_float(reward)
            for hk, reward in w.rewards_by_hotkey.items()
            if str(hk)
        }
        # Per-window alpha update — needs to touch ALL known hotkeys, not
        # just those in this window, so absent miners decay.
        all_hk = set(ema) | set(contribs)
        for hk in all_hk:
            fraction = contribs.get(hk, 0.0)
            ema[hk] = EMA_ALPHA * fraction + (1 - EMA_ALPHA) * ema.get(hk, 0.0)
        ema = {hk: v for hk, v in ema.items() if v > EMA_PRUNE}

    # Slots in the last 12 windows = a "recent" signal vs the full 72-window EMA
    last_12 = sorted_windows[-12:] if len(sorted_windows) >= 12 else sorted_windows
    recent_counts: dict[str, int] = {}
    for w in last_12:
        for entry in w.batch:
            hk = entry.get("hotkey", "")
            if hk:
                recent_counts[hk] = recent_counts.get(hk, 0) + 1

    out = []
    for hk, ema_val in ema.items():
        slots_total = slot_counts_total.get(hk, 0)
        slot_share_72 = slots_total / max(total_slots, 1)
        out.append({
            "hotkey": hk,
            "ema": ema_val,
            "slot_count_total": slots_total,
            "slot_share_72": slot_share_72,
            "slot_count_recent12": recent_counts.get(hk, 0),
        })
    out.sort(key=lambda r: -r["ema"])
    for i, r in enumerate(out):
        r["rank"] = i + 1
    return out


def aggregate_validator_health(windows: list[WindowSummary]) -> dict:
    """Coarse health view of the validator: total submissions seen across windows,
    mean batch size, mean number of rejects, top reject reason."""
    if not windows:
        return {}
    n = len(windows)
    total_batch = sum(w.total_batch for w in windows)
    total_rej = sum(len(w.rejected) for w in windows)
    reasons: dict[str, int] = {}
    for w in windows:
        for r in w.rejected:
            reasons[r.get("reason", "?")] = reasons.get(r.get("reason", "?"), 0) + 1
    top_rejects = sorted(reasons.items(), key=lambda kv: -kv[1])[:5]
    return {
        "windows": n,
        "mean_batch_size": total_batch / n,
        "mean_rejects": total_rej / n,
        "top_rejects": top_rejects,
    }


if __name__ == "__main__":
    sys.exit(main())
