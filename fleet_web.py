#!/usr/bin/env python3
"""SN81 fleet web dashboard built on FastAPI with local static assets.

Same data sources as fleet.py (SSH probes, validator /state, R2 window
history) but served as a self-refreshing HTML page on localhost. Open
http://localhost:9091 in any browser. One visibility-aware local snapshot
request refreshes the complete page without full-page reloads.

A background poller thread keeps the data warm so HTTP requests return
in milliseconds instead of waiting on SSH round trips.

Run (from the repo root, after copying config.example.yaml -> config.yaml):
  ./run.sh
  ./run.sh --port 9091 --refresh 5
  python3 fleet_web.py --config /alt/path/to/config.yaml
"""

from __future__ import annotations

import argparse
import hashlib
import html
import ipaddress
import json
import math
import os
import random
import re
import shlex
import socket
import sys
import threading
import time
import webbrowser
from datetime import datetime, timezone
from importlib import resources as package_resources
from pathlib import Path

from reliquary_fleet import __version__

# Reuse the polling primitives from the TUI module.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from fleet import (  # type: ignore
        FLEET, OUR_SS58, BoxState, LabState, ComponentState, ValidatorState,
        collect_box_snapshot, collect_lab_snapshot, collect_component_snapshot,
        fetch_validator, fetch_recent_windows,
        recompute_validator_acpts,
        ChainState, RTTState, BaselineState,
        fetch_chain_state, probe_rtt, baseline_load, baseline_record,
        aggregate_slot_ranks, aggregate_sigma_histogram,
        aggregate_k_histogram, aggregate_competitors,
        compute_ema_leaderboard, window_cache_load,
        validator_tail_loop, validator_events_snapshot,
        VALIDATOR_SSH,
        # New: validator-rundown plumbing.
        current_window_accept_count, validator_events_in_window,
        validator_events_query,
        DeploymentState, fetch_deployment_state,
        _canonical_physical_gpu_id,
        verdict_rows_for_hotkey,
        B_BATCH,
    )
    from collections import Counter
except ImportError as e:
    sys.exit(f"could not import fleet.py from same dir: {e}")

try:
    from fastapi import FastAPI, Header, HTTPException, Request
    from fastapi.responses import HTMLResponse, JSONResponse, Response
    from starlette.middleware.gzip import GZipMiddleware
    import uvicorn
except ImportError:
    sys.exit("missing dep: pip install --user fastapi 'uvicorn[standard]'")


# --- Shared state ----------------------------------------------------------
_lock = threading.Lock()
_boxes: list[BoxState] = []
_labs: list[LabState] = []
_components: list[ComponentState] = []
_vs = ValidatorState()
_windows: list = []
_last_poll_at: float = 0.0
_poll_count: int = 0
# Slow-cadence subsystems each get their own state object + their own thread.
_chain = ChainState()
_rtt = RTTState()
_baseline = BaselineState()
_chain_last: float = 0.0
_rtt_last: float = 0.0
_baseline_last: float = 0.0
# Cached EMA leaderboard — recomputed only when the window cache changes.
_ema_cache: list = []
_ema_window_count: int = 0
# Validator deployment fingerprint (image SHA + start time). Refreshed on
# its own SSH-backed thread; used by the rundown panel.
_deployment = DeploymentState()
_deployment_last: float = 0.0
_instance_lock_handle = None
_STATIC_ROOT = package_resources.files("reliquary_fleet").joinpath("static")
_STATIC_MEDIA_TYPES = {
    "brand/mark-outline.svg": "image/svg+xml",
    "brand/relic.svg": "image/svg+xml",
    "dashboard.css": "text/css",
    "dashboard.js": "text/javascript",
    "fonts/geist-sans.woff2": "font/woff2",
    "fonts/jetbrains-mono.woff2": "font/woff2",
    "htmx.min.js": "text/javascript",
    "logs.css": "text/css",
    "logs.js": "text/javascript",
}
_STATIC_ASSETS = {
    name: _STATIC_ROOT.joinpath(name).read_bytes()
    for name in _STATIC_MEDIA_TYPES
}
_STATIC_ASSET_VERSION = (
    f"{__version__}-"
    + hashlib.sha256(
        b"\0".join(
            name.encode("utf-8") + b"\0" + _STATIC_ASSETS[name]
            for name in sorted(_STATIC_ASSETS)
        )
    ).hexdigest()[:12]
)
_RNG = random.SystemRandom()


def _is_loopback_bind(host: str) -> bool:
    value = str(host or "").strip().strip("[]").lower()
    if value == "localhost":
        return True
    try:
        return ipaddress.ip_address(value).is_loopback
    except ValueError:
        return False


def _acquire_instance_lock(state_dir: str, port: int) -> tuple[bool, str]:
    """Hold a process lock so one configured dashboard cannot run twice."""
    global _instance_lock_handle
    try:
        import fcntl

        lock_path = Path(state_dir) / f"dashboard-{port}.lock"
        handle = lock_path.open("a+", encoding="utf-8")
        os.chmod(lock_path, 0o600)
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.seek(0)
            owner = handle.read().strip()
            handle.close()
            return False, owner
        handle.seek(0)
        handle.truncate()
        handle.write(str(os.getpid()))
        handle.flush()
        _instance_lock_handle = handle
        return True, ""
    except (ImportError, OSError) as exc:
        return False, type(exc).__name__


def _port_available(host: str, port: int) -> tuple[bool, str]:
    """Preflight the listener before any background probes are started."""
    try:
        addresses = socket.getaddrinfo(
            host,
            port,
            type=socket.SOCK_STREAM,
            flags=socket.AI_PASSIVE,
        )
    except socket.gaierror as exc:
        return False, str(exc)
    last_error = "no bindable address"
    for family, socktype, protocol, _, address in addresses:
        candidate = socket.socket(family, socktype, protocol)
        try:
            candidate.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            candidate.bind(address)
            return True, ""
        except OSError as exc:
            last_error = str(exc)
        finally:
            candidate.close()
    return False, last_error


def _release_instance_lock() -> None:
    global _instance_lock_handle
    if _instance_lock_handle is not None:
        try:
            _instance_lock_handle.close()
        finally:
            _instance_lock_handle = None


def _start_collectors_after_ready(
    server,
    collectors: list[tuple],
    *,
    on_ready=None,
    wait_s: float = 0.02,
) -> bool:
    """Start remote work only after Uvicorn has bound its local socket."""
    while not bool(getattr(server, "started", False)):
        if bool(getattr(server, "should_exit", False)):
            return False
        time.sleep(wait_s)
    for collector in collectors:
        name, target, thread_args = collector[:3]
        startup_jitter_s = float(collector[3]) if len(collector) > 3 else 0.0
        threading.Thread(
            name=f"reliquary-fleet-{name}",
            target=_run_collector_after_delay,
            args=(target, thread_args, startup_jitter_s),
            daemon=True,
        ).start()
    if on_ready is not None:
        on_ready()
    return True


def _jittered_delay(base_s: float, ratio: float = 0.2) -> float:
    """Bound one recurring cadence while desynchronizing installations."""
    base = max(0.05, float(base_s))
    spread = max(0.0, min(float(ratio), 0.5))
    return base * _RNG.uniform(1.0 - spread, 1.0 + spread)


def _backoff_delay(
    base_s: float,
    failures: int,
    *,
    cap_s: float = 900.0,
) -> float:
    """Equal-jitter exponential backoff; never returns a hot-looping zero."""
    exponent = max(0, min(int(failures) - 1, 6))
    ceiling = min(float(cap_s), max(0.1, float(base_s)) * (2 ** exponent))
    return _RNG.uniform(ceiling * 0.5, ceiling)


def _run_collector_after_delay(
    target: object,
    thread_args: tuple,
    startup_jitter_s: float,
) -> None:
    delay = _RNG.uniform(0.0, max(0.0, float(startup_jitter_s)))
    if delay:
        time.sleep(delay)
    target(*thread_args)


# Auction payout semantics are an exact-source contract.  PR156 introduced a
# paying boundary tier, so a candidate may be rewarded without entering the
# training batch.  Do not infer that contract from observed counts: a future
# source must be reviewed before the dashboard relaxes the strict invariant.
_BOUNDARY_FAIR_SPLIT_SOURCES = frozenset(
    {
        "21ae0c9cd7cfa346f4746fbf64fcc702fa954ff2",
        "8835a95e5a7aa6065eef4691760716be004f88cc",
    }
)


def _auction_payout_profile(source_revision: str) -> str:
    source = str(source_revision or "").strip().lower()
    if source in _BOUNDARY_FAIR_SPLIT_SOURCES:
        return "boundary_fair_split"
    return "strict_top8"


def sync_fleet_globals() -> None:
    """Refresh names imported from fleet.py after config/star changes.

    `settings.apply_to_fleet_module()` reassigns fleet.FLEET/OUR_SS58 after
    this module is imported. Without syncing, renderers keep seeing the import
    time empty defaults, which makes live hotkeys look invisible.
    """
    import fleet as _fleet_mod

    global FLEET, OUR_SS58, VALIDATOR_URL, SSH_KEY
    global VALIDATOR_SSH, VALIDATOR_PORT, VALIDATOR_CONTAINER, NETUID
    FLEET = _fleet_mod.FLEET
    OUR_SS58 = _fleet_mod.OUR_SS58
    VALIDATOR_URL = _fleet_mod.VALIDATOR_URL
    SSH_KEY = _fleet_mod.SSH_KEY
    VALIDATOR_SSH = _fleet_mod.VALIDATOR_SSH
    VALIDATOR_PORT = _fleet_mod.VALIDATOR_PORT
    VALIDATOR_CONTAINER = _fleet_mod.VALIDATOR_CONTAINER
    NETUID = _fleet_mod.NETUID


def deployment_loop(period_s: float = 60.0):
    """Slow poll — SSH + docker inspect every 60 s for image/uptime data.

    The validator's container restart is the operational event the
    rundown panel cares about (each restart flips the "live since" line
    and the running PR fingerprint). 60 s cadence is fast enough that
    operator-visible restarts surface in under a minute, slow enough
    that the SSH round trips don't compete with the fast box-probe loop.
    """
    global _deployment_last
    failures = 0
    while True:
        try:
            fetch_deployment_state(_deployment)
        except Exception:
            _deployment.error = "probe failed"
        _deployment_last = time.time()
        if _deployment.error:
            failures += 1
            delay = _backoff_delay(period_s, failures)
        else:
            failures = 0
            delay = _jittered_delay(period_s)
        time.sleep(delay)


def init_state():
    """Build the per-box state list from the post-config FLEET.

    Reads `fleet.FLEET` directly rather than the import-time alias because
    `settings.apply_to_fleet_module()` mutates the fleet module's globals
    AFTER this module's top-level `from fleet import FLEET` ran.
    """
    import fleet as _fleet_mod
    global _boxes, _labs, _components
    _boxes = [
        BoxState(
            alias=a,
            hotkey=hk,
            label=label,
            color=c,
            unit=u,
            env_file=_fleet_mod.FLEET_ENV_FILES.get(label, ""),
            unit_candidates=tuple(
                tuple(spec)
                for spec in _fleet_mod.FLEET_UNIT_CANDIDATES.get(label, [])
            ),
            unit_controller_config_paths=tuple(
                (str(unit), str(path))
                for unit, path in _fleet_mod.FLEET_CONTROLLER_CONFIG_PATHS.get(
                    label, {}
                ).items()
            ),
            coordinated_units=tuple(
                _fleet_mod.FLEET_COORDINATED_UNITS.get(label, [])
            ),
            host_unit_allowlist=tuple(
                _fleet_mod.FLEET_HOST_UNIT_ALLOWLIST.get(label, [])
            ),
            standalone_telemetry_path=str(
                _fleet_mod.FLEET_STANDALONE_TELEMETRY.get(
                    label, {}
                ).get("telemetry_path") or ""
            ),
            standalone_runtime_manifest_path=str(
                _fleet_mod.FLEET_STANDALONE_TELEMETRY.get(
                    label, {}
                ).get("runtime_manifest_path") or ""
            ),
            active_unit_registry_path=str(
                _fleet_mod.FLEET_STANDALONE_TELEMETRY.get(
                    label, {}
                ).get("active_unit_registry_path") or ""
            ),
            standalone_supervisor_status_path=str(
                _fleet_mod.FLEET_STANDALONE_TELEMETRY.get(
                    label, {}
                ).get("supervisor_status_path") or ""
            ),
            standalone_telemetry_stale_seconds=float(
                _fleet_mod.FLEET_RELIQUARY_ONE.get(label, {}).get(
                    "stale_seconds"
                )
                or _fleet_mod.FLEET_STANDALONE_TELEMETRY.get(
                    label, {}
                ).get("stale_seconds")
                or 90.0
            ),
            miner_kind=(
                "reliquary_one"
                if label in _fleet_mod.FLEET_RELIQUARY_ONE
                else "legacy"
            ),
            reliquary_one_state_root=str(
                _fleet_mod.FLEET_RELIQUARY_ONE.get(label, {}).get(
                    "state_root"
                ) or ""
            ),
            standalone_certification_config=dict(
                _fleet_mod.FLEET_STANDALONE_CERTIFICATION.get(label, {})
            ),
            operator=str(
                _fleet_mod.FLEET_STANDALONE_TELEMETRY.get(
                    label, {}
                ).get("operator") or ""
            ),
        )
        for a, hk, label, c, u in _fleet_mod.FLEET
    ]
    _labs = [
        LabState(
            alias=alias,
            label=label,
            color=color,
            unit=unit,
            evidence_db=evidence_db,
            source_manifest=source_manifest,
            selector_artifact_manifest=selector_artifact_manifest,
        )
        for (
            alias,
            label,
            color,
            unit,
            evidence_db,
            source_manifest,
            selector_artifact_manifest,
        )
        in _fleet_mod.LABS
    ]
    _components = [
        ComponentState(**dict(component))
        for component in _fleet_mod.FLEET_COMPONENTS
    ]


def _publish_box_snapshots(
    boxes: list[BoxState], *, labs: list[LabState] | None = None,
    components: list[ComponentState] | None = None,
    polled_at: float | None = None,
) -> None:
    """Replace the owned box objects under the same lock readers snapshot.

    Renderers copy references from ``_boxes`` while holding ``_lock`` and then
    read those immutable, no-longer-mutated instances after releasing it.
    Replacing the list here therefore prevents one response from combining
    fields from two different poll generations.
    """
    global _boxes, _labs, _components, _last_poll_at, _poll_count
    with _lock:
        _boxes = list(boxes)
        if labs is not None:
            _labs = list(labs)
        if components is not None:
            _components = list(components)
        if polled_at is not None:
            _last_poll_at = float(polled_at)
            _poll_count += 1


def _controller_component_expectation(
    component: ComponentState,
    boxes: list[BoxState],
) -> dict[str, object] | None:
    """Return one exact live controller-manifest component contract."""
    controller = next(
        (box for box in boxes if box.label == component.controller_label),
        None,
    )
    if not (
        controller is not None
        and controller.proc_alive
        and controller.standalone_fresh
        and controller.runtime_parity_ok
        and controller.standalone_controller_mode in {"canary", "mine"}
    ):
        return None
    matches = [
        row
        for row in getattr(controller, "runtime_components", [])
        if isinstance(row, dict) and row.get("role") == component.role
    ]
    if len(matches) != 1:
        return None
    row = matches[0]
    return {
        "role": str(row.get("role") or ""),
        "component_id": str(row.get("component_id") or ""),
        "manifest_path": str(row.get("manifest_path") or ""),
        "manifest_file_sha256": str(
            row.get("manifest_file_sha256") or ""
        ),
        "runtime_payload_sha256": str(
            row.get("runtime_payload_sha256") or ""
        ),
        "runtime_profile_sha256": str(
            row.get("runtime_profile_sha256") or ""
        ),
        "miner_source_revision": str(
            controller.miner_source_revision or ""
        ),
        "validator_source_revision": str(
            controller.reliquary_source_revision or ""
        ),
        "checkpoint_revision": str(
            controller.runtime_checkpoint_revision
            or controller.provisioned_model_revision
            or ""
        ),
        "controller_manifest_path": str(
            controller.standalone_active_runtime_manifest_path or ""
        ),
    }


def _apply_component_active_generation(
    boxes: list[BoxState],
    components: list[ComponentState],
    validator: ValidatorState,
) -> None:
    """Project a fresh worker job onto its controller, never its funnel."""
    for component in components:
        controller = next(
            (box for box in boxes if box.label == component.controller_label),
            None,
        )
        if controller is None:
            continue
        binding = _component_controller_binding(component, controller)
        if not (
            binding.get("attached") is True
            and component.resolution_source == "controller_manifest"
            and component.generator_active_state == "active"
            and component.generator_sub_state == "running"
            and component.tunnel_active_state == "active"
            and component.tunnel_sub_state == "running"
            and component.identity_ok
            and component.wallet_absent_attested
            and component.submit_authority is False
            and component.worker_state == "active"
            and component.worker_progress_fresh
            and component.active_job_id
            and component.active_window_n > 0
            and component.active_window_n == int(validator.window or 0)
            and controller.proc_alive
            and controller.standalone_fresh
            and controller.runtime_parity_ok
        ):
            continue
        controller.miner_state = "active_generation"
        controller.miner_window = component.active_window_n
        controller.miner_ready = 0
        controller.miner_inflight = 1


def poller_loop(refresh_s: float, history: int):
    """Fast loop — SSH probes + validator state. R2 runs on its own thread
    (windows only change every ~5 min, so 5s cadence here was wasted work).

    After each round of `collect_box` finishes we run
    `recompute_validator_acpts(_boxes, _vs)` which projects the exact final
    `/verdicts` feed onto each box, with the validator event tail as a
    short-lived fallback. Without this step the dashboard would show pregen counts
    (~225/30m per box on a busy fleet) where the operator expects
    actual validator pool/proof admissions (~5-15/30m per box).
    """
    import fleet as _fleet_mod
    from settings import SETTINGS as _settings

    periods = {
        "state": float(_settings.validator_state_seconds),
        "health": float(_settings.validator_health_seconds),
        "verdicts": float(_settings.validator_verdict_seconds),
    }
    startup_now = time.monotonic()
    startup_spread = float(_settings.startup_jitter_seconds)
    first_poll_spread = {
        "state": min(5.0, startup_spread),
        "health": min(15.0, startup_spread),
        "verdicts": startup_spread,
    }
    next_due = {
        name: startup_now
        + _RNG.uniform(0.0, first_poll_spread[name])
        for name in periods
    }
    failures = {name: 0 for name in periods}
    while True:
        with _lock:
            current_boxes = list(_boxes)
            current_labs = list(_labs)
            current_components = list(_components)
        snapshots = list(current_boxes)
        lab_snapshots = list(current_labs)
        component_snapshots = list(current_components)

        def collect_snapshot(
            index: int,
            box: BoxState,
            target: list[BoxState] = snapshots,
        ) -> None:
            target[index] = collect_box_snapshot(box)

        def collect_lab(
            index: int,
            lab: LabState,
            target: list[LabState] = lab_snapshots,
        ) -> None:
            target[index] = collect_lab_snapshot(lab)

        def collect_component(
            index: int,
            component: ComponentState,
            target: list[ComponentState] = component_snapshots,
        ) -> None:
            expectation = _controller_component_expectation(
                component, snapshots
            )
            target[index] = collect_component_snapshot(
                component, expectation
            )

        threads = [
            threading.Thread(target=collect_snapshot, args=(index, box))
            for index, box in enumerate(current_boxes)
        ]
        lab_threads = [
            threading.Thread(target=collect_lab, args=(index, lab))
            for index, lab in enumerate(current_labs)
        ]
        component_threads = [
            threading.Thread(
                target=collect_component,
                args=(index, component),
            )
            for index, component in enumerate(current_components)
        ]
        now_mono = time.monotonic()
        retry_after = _fleet_mod.validator_retry_after_remaining()
        if retry_after > 0:
            for name in next_due:
                next_due[name] = max(next_due[name], now_mono + retry_after)
        due = {
            name: now_mono >= next_due[name]
            for name in periods
        }
        previous_fetch = {
            "state": float(_vs.last_fetch_at or 0.0),
            "health": float(_vs.health_last_fetch_at or 0.0),
            "verdicts": float(_vs.verdicts_last_fetch_at or 0.0),
        }
        v_thread = None
        if any(due.values()):
            v_thread = threading.Thread(
                target=fetch_validator,
                args=(_vs,),
                kwargs={
                    "poll_state": due["state"],
                    "poll_health": due["health"],
                    "poll_verdicts": due["verdicts"],
                },
            )
        active_threads = threads + lab_threads + (
            [v_thread] if v_thread is not None else []
        )
        for t in active_threads:
            t.start()
        # Wait for every private snapshot's bounded SSH/HTTP work to finish;
        # publishing only complete generations prevents overlapping rounds and
        # keeps the previous generation stable for concurrent HTTP readers.
        for t in threads + lab_threads:
            t.join()
        # Component resolution is controller-manifest driven.  Collect it
        # only after this poll's controller snapshots are complete so a
        # checkpoint transition can never mix old and new identities.
        for t in component_threads:
            t.start()
        for t in component_threads:
            t.join()
        if v_thread is not None:
            v_thread.join()
            fetched_at = {
                "state": float(_vs.last_fetch_at or 0.0),
                "health": float(_vs.health_last_fetch_at or 0.0),
                "verdicts": float(_vs.verdicts_last_fetch_at or 0.0),
            }
            surface_error = {
                "state": _vs.error not in ("", "ssh-state"),
                "health": bool(_vs.health_error),
                "verdicts": bool(_vs.verdicts_error),
            }
            scheduled_at = time.monotonic()
            for name, was_due in due.items():
                if not was_due:
                    continue
                succeeded = (
                    fetched_at[name] > previous_fetch[name]
                    and fetched_at[name] > 0
                    and not surface_error[name]
                )
                if succeeded:
                    failures[name] = 0
                    delay = _jittered_delay(periods[name])
                else:
                    failures[name] += 1
                    delay = _backoff_delay(periods[name], failures[name])
                retry_after = _fleet_mod.validator_retry_after_remaining()
                next_due[name] = scheduled_at + max(delay, retry_after)
        # Reproject final validator verdicts onto each box. Runs after
        # the SSH-collect joins so we have the latest box list and the
        # validator-events deque is up to date (the tail subprocess
        # streams events independently).
        try:
            recompute_validator_acpts(snapshots, _vs)
        except Exception:
            pass
        _apply_component_active_generation(
            snapshots, component_snapshots, _vs
        )
        _publish_box_snapshots(
            snapshots,
            labs=lab_snapshots,
            components=component_snapshots,
            polled_at=time.time(),
        )
        time.sleep(_jittered_delay(refresh_s, ratio=0.1))


def r2_loop(history: int, period_s: float = 60.0):
    """Refresh sealed windows on a bounded configured cadence.

    Cached fetches download only new immutable windows. The validator health
    snapshot supplies the exact archive index, so normal operation performs
    neither a duplicate health request nor a bucket LIST.

    Also recomputes the EMA leaderboard when new windows arrive — this is the
    only place EMA gets recomputed, keeping the /api/ema endpoint at <1ms.
    """
    import fleet as _fleet_mod

    global _windows, _ema_cache, _ema_window_count
    failures = 0
    while True:
        health_snapshot = (
            dict(_vs.health_raw) if isinstance(_vs.health_raw, dict) else {}
        )
        if (
            not health_snapshot
            and not _fleet_mod.R2_ALLOW_LIST_FALLBACK
        ):
            # The health collector supplies the exact immutable archive index.
            # Waiting here performs no network work and avoids a false
            # "index unavailable" warning when the R2 thread wins startup.
            time.sleep(_jittered_delay(min(5.0, period_s), ratio=0.1))
            continue
        try:
            new_windows = fetch_recent_windows(
                history,
                archive_health=health_snapshot,
            )
        except Exception:
            new_windows = None
        if new_windows:
            with _lock:
                _windows = new_windows
            # Recompute the EMA leaderboard outside the lock — pure CPU work
            # over cached data, ~1ms even on 200+ windows. Then update cache
            # under the lock.
            try:
                lb = compute_ema_leaderboard(new_windows)
                with _lock:
                    _ema_cache = lb
                    _ema_window_count = len(new_windows)
            except Exception:
                pass
        status = _r2_status_snapshot()
        error = str(status.get("error", status.get("last_error", "")) or "")
        hard_error = bool(error and "warming:" not in error)
        if hard_error:
            failures += 1
            delay = _backoff_delay(period_s, failures)
        else:
            failures = 0
            delay = _jittered_delay(period_s)
        time.sleep(delay)


def chain_loop(period_s: float = 300.0):
    """Pull metagraph stake / emission every 5 min."""
    global _chain_last
    failures = 0
    while True:
        try:
            fetch_chain_state(_chain)
            _chain_last = time.time()
        except Exception as e:
            _chain.error = type(e).__name__
        if _chain.error:
            failures += 1
            delay = _backoff_delay(period_s, failures, cap_s=1800.0)
        else:
            failures = 0
            delay = _jittered_delay(period_s)
        time.sleep(delay)


def rtt_loop(period_s: float = 60.0):
    """Probe mac→validator and box→validator latency on a slow cadence.

    Uses fleet.VALIDATOR_URL (populated by settings) so the probe targets
    the operator-configured validator host instead of a hardcoded IP.
    """
    import fleet as _fleet_mod
    global _rtt_last
    failures = 0
    while True:
        succeeded = False
        retry_after = _fleet_mod.validator_retry_after_remaining()
        if retry_after <= 0 and _boxes and _fleet_mod.VALIDATOR_URL:
            try:
                probe_rtt(_boxes, _fleet_mod.VALIDATOR_URL, _rtt)
                _rtt_last = time.time()
                succeeded = any(value >= 0 for value in _rtt.rtt_ms.values())
            except Exception:
                pass
        if succeeded:
            failures = 0
            delay = _jittered_delay(period_s)
        else:
            failures += 1
            delay = _backoff_delay(period_s, failures)
        time.sleep(max(delay, retry_after))


def _sum_available_counts(values) -> int | None:
    """Sum counters only when every contributing scope is attributable."""

    total = 0
    for value in values:
        if value is None:
            return None
        total += value
    return total


def baseline_loop(period_s: float = 60.0):
    """Sample fleet ACPT/30m every minute, persist to disk for 6h baseline."""
    global _baseline_last
    while True:
        time.sleep(_jittered_delay(period_s, ratio=0.1))
        with _lock:
            total = _sum_available_counts(b.acpt_30m for b in _boxes)
        if total is None:
            continue
        baseline_record(_baseline, total)
        _baseline_last = time.time()


def _collector_specs(runtime_settings, refresh_s: float, history: int) -> list[tuple]:
    """Build the one-process collector topology from validated settings."""
    startup_jitter = float(runtime_settings.startup_jitter_seconds)
    collectors: list[tuple] = [
        (
            "poller",
            poller_loop,
            (refresh_s, history),
            min(5.0, startup_jitter),
        ),
        (
            "chain",
            chain_loop,
            (runtime_settings.chain_refresh_seconds,),
            startup_jitter,
        ),
        (
            "rtt",
            rtt_loop,
            (runtime_settings.rtt_refresh_seconds,),
            startup_jitter,
        ),
        ("baseline", baseline_loop, (60,), 0.0),
        (
            "r2",
            r2_loop,
            (history, runtime_settings.r2_refresh_seconds),
            startup_jitter,
        ),
    ]
    if runtime_settings.validator_ssh_host:
        collectors.extend(
            [
                (
                    "validator-tail",
                    validator_tail_loop,
                    (),
                    startup_jitter,
                ),
                (
                    "deployment",
                    deployment_loop,
                    (60,),
                    startup_jitter,
                ),
            ]
        )
    return collectors


def _request_budget(refresh_s: float) -> dict[str, float | int]:
    """Conservative steady-state request envelope for one visible install."""
    from settings import SETTINGS as runtime

    watched = min(
        len({hotkey for hotkey in OUR_SS58 if hotkey}),
        int(runtime.max_verdict_hotkeys),
    )
    boxes = len(_boxes)
    validator_per_minute = (
        60.0 / float(runtime.validator_state_seconds)
        + 60.0 / float(runtime.validator_health_seconds)
        + watched * 60.0 / float(runtime.validator_verdict_seconds)
        + (boxes + 1) * 60.0 / float(runtime.rtt_refresh_seconds)
    )
    browser_interval = max(5.0, float(refresh_s))
    return {
        "watched_verdict_hotkeys": watched,
        "miner_boxes": boxes,
        "validator_requests_per_minute_upper_bound": round(
            validator_per_minute,
            3,
        ),
        "browser_local_requests_per_minute": round(
            60.0 / browser_interval,
            3,
        ),
        "r2_cold_batch_objects": int(runtime.r2_fetch_batch_size),
        "r2_cold_max_concurrency": int(runtime.r2_fetch_workers),
    }


# --- v2.3 reject-reason metadata -------------------------------------------
# Mirrors reliquary/protocol/submission.py::RejectReason. Each entry
# carries a short human label, a one-sentence explanation surfaced as
# the row's `title=` attribute, and a colour-class hint for the /logs
# page so an operator can scan severity at a glance.
#
# `severity` matches the colour-token scheme on the reliquary-web side:
#   ok        → amber  (success sentinel)
#   miner     → reject (the miner produced invalid work)
#   timing    → amber  (miner was at the wrong place in time)
#   race      → stone  (miner lost a race they couldn't win)
#   saturate  → ledger (the validator was overloaded)
REJECT_REASON_META: dict[str, dict[str, str | bool]] = {
    "accepted": {"label": "accepted", "sev": "ok",
                  "explain": "The candidate entered the validator pool; auction proof and final selection/reward are determined at seal."},
    "submitted": {"label": "submitted", "sev": "ok",
                  "explain": "Placed on the worker queue; real verdict surfaces later."},
    "bad_signature": {"label": "bad_signature", "sev": "miner",
                      "explain": "A rollout commitment signature did not verify for the claimed miner hotkey; the candidate was rejected during proof validation."},
    "bad_envelope_signature": {"label": "bad_envelope_signature", "sev": "miner",
                               "explain": "The signed submission envelope was missing or invalid; the request was rejected before hotkey quota or candidate proof admission."},
    "bad_prompt_idx": {"label": "bad_prompt_idx", "sev": "timing",
                       "explain": "prompt_idx outside the window's range — stale window or off-by-one."},
    "prompt_mismatch": {"label": "prompt_mismatch", "sev": "miner",
                        "explain": "Prompt text the miner ran doesn't match the validator's prompt at that index."},
    "distribution_suspicious": {"label": "distribution_suspicious", "sev": "miner",
                                "explain": "min-of-q10 below 0.10 — per-token probability mass collapsed."},
    "prompt_in_cooldown": {"label": "prompt_in_cooldown", "sev": "race", "v23": True,
                           "explain": "You already submitted on this prompt. v2.3 cooldown=1_000_000 windows (effectively single-use)."},
    "superseded": {"label": "superseded", "sev": "race", "deprecated": True,
                   "explain": "v2.3 no longer emits this — drand_round ordering replaced FIFO per-prompt claim."},
    "prompt_full": {"label": "prompt_full", "sev": "race", "v23": True,
                    "explain": "MAX_SUBMISSIONS_PER_PROMPT (10) reached — diversify to less-saturated prompts."},
    "grail_fail": {"label": "grail_fail", "sev": "miner",
                   "explain": "sketch_diff_max exceeded GRAIL tolerance (5000 + 5√P)."},
    "hash_duplicate": {"label": "hash_duplicate", "sev": "miner",
                       "explain": "Same merkle_root seen within 10 000 windows — copy-paste or replay."},
    "logprob_mismatch": {"label": "logprob_mismatch", "sev": "miner",
                         "explain": "Per-token log-prob deviation > 0.10 — claimed sampling distribution mismatch."},
    "reward_mismatch": {"label": "reward_mismatch", "sev": "miner",
                        "explain": "Miner-claimed reward != validator-recomputed reward."},
    "reward_distribution": {"label": "reward_distribution", "sev": "miner", "deprecated": True,
                            "explain": "Historical anomaly code for an invalid reward distribution; current validators use distribution_suspicious plus quarantine telemetry."},
    "out_of_zone": {"label": "out_of_zone", "sev": "timing",
                    "explain": "σ below the zone threshold (0.43 steady, 0.33 bootstrap) — DAPO filter discarded."},
    "rate_limited": {"label": "rate_limited", "sev": "saturate",
                     "explain": "Validator throttled this hotkey. Back off and retry."},
    "batch_filled": {"label": "batch_filled", "sev": "race",
                     "explain": "Validator proof cutoff already met — eligible-by-drand_round queue closed before this submission."},
    "wrong_rollout_count": {"label": "wrong_rollout_count", "sev": "miner",
                            "explain": "Group did not carry M_ROLLOUTS samples — incomplete or padded."},
    "window_mismatch": {"label": "window_mismatch", "sev": "timing",
                        "explain": "window_start in the request doesn't match the validator's current window — miner clock stale."},
    "window_not_active": {"label": "window_not_active", "sev": "timing",
                          "explain": "Validator in TRAINING or PUBLISHING — submissions only accepted during OPEN."},
    "bad_schema": {"label": "bad_schema", "sev": "miner",
                   "explain": "Pydantic validation failed — malformed BatchSubmissionRequest."},
    "bad_tokens": {"label": "bad_tokens", "sev": "miner",
                   "explain": "Token IDs out of vocab or mismatched length — re-encode against the active tokenizer."},
    "tokens_mismatch": {"label": "tokens_mismatch", "sev": "miner",
                        "explain": "rollout.tokens differed from commit.tokens; the protocol invariant failed before decode, reward grading, or GRAIL."},
    "bad_termination": {"label": "bad_termination", "sev": "miner",
                        "explain": "Rollout did not end with EOS — generation cut short."},
    "boxed_answer_tampered": {"label": "boxed_answer_tampered", "sev": "miner",
                              "explain": "The proof probabilities did not support the claimed boxed-answer tokens; the candidate failed proof validation."},
    "token_tampered": {"label": "token_tampered", "sev": "miner",
                       "explain": "Token-authenticity checks found manufactured token probabilities; the candidate is rejected when enforcement is enabled."},
    "malformed_final_answer": {"label": "malformed_final_answer", "sev": "miner",
                               "explain": "A zero-reward rollout used an empty, special-token, or unclosed final box; the candidate was rejected before GRAIL."},
    "reward_shape_suspicious": {"label": "reward_shape_suspicious", "sev": "miner", "deprecated": True,
                                "explain": "Historical reward-order/length heuristic; current validators retain shape telemetry for quarantine but do not reject on it."},
    "wrong_checkpoint": {"label": "wrong_checkpoint", "sev": "miner",
                         "explain": "claimed_checkpoint_hash != active checkpoint_revision — pull latest weights."},
    "wrong_randomness": {"label": "wrong_randomness", "sev": "timing", "v23": True,
                         "explain": "GRAIL seed used during generation doesn't match state.randomness — read from /state, don't derive locally."},
    "worker_dropped": {"label": "worker_dropped", "sev": "saturate",
                       "explain": "Worker queue back-pressured — submission dropped. Retry next window."},
    "stale_round": {"label": "stale_round", "sev": "timing", "v23": True,
                    "explain": "drand_round trailing the window's allowed range — beacon round too old."},
    "future_round": {"label": "future_round", "sev": "timing", "v23": True,
                     "explain": "drand_round ahead of accept ceiling — beacon round hasn't matured."},
    "empty_randomness": {"label": "empty_randomness", "sev": "saturate",
                         "explain": "Validator couldn't derive randomness for the window — historical PR #8 bug."},
}


def reject_meta(reason: str) -> dict:
    """Get metadata for a reject reason, with a generic fallback for unknowns."""
    if reason in REJECT_REASON_META:
        return REJECT_REASON_META[reason]
    return {
        "label": reason or "?",
        "sev": "miner",
        "explain": f"Unknown reject code '{reason}' — newer validator than this dashboard knows.",
    }


def reject_color(sev: str) -> str:
    """Map a severity tier to a CSS variable."""
    return {
        "ok": "var(--amber)",
        "miner": "var(--reject)",
        "timing": "var(--amber)",
        "race": "var(--stone)",
        "saturate": "var(--ledger)",
    }.get(sev, "var(--bone)")


# --- HTML helpers ----------------------------------------------------------
def color_for_pct(pct: int, hi: int = 95, mid: int = 88) -> str:
    if pct >= hi:
        return "var(--red)"
    if pct >= mid:
        return "var(--yellow)"
    return "var(--green)"


def color_acpt(n: int) -> str:
    if n >= 5:
        return "var(--green)"
    if n >= 1:
        return "var(--yellow)"
    return "var(--red)"


def color_rej(n: int) -> str:
    if n >= 3:
        return "var(--red)"
    if n >= 1:
        return "var(--yellow)"
    return "var(--dim)"


def fmt_age(seconds: int | float) -> str:
    """Compact age string: '94min', '3h 12m', '2d', or '—' if unknown."""
    try:
        seconds = int(seconds)
    except (TypeError, ValueError, OverflowError):
        return "—"
    if seconds < 0:
        return "—"
    if seconds >= 999_000:
        return "—"  # treat as "no record"
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}min"
    if seconds < 86400:
        h, m = divmod(seconds, 3600)
        return f"{h}h {m // 60}m"
    return f"{seconds // 86400}d"


def color_oom_age(s: int) -> str:
    """Time-since-OOM color: red <5min, yellow <1h, green ≥1h."""
    if s < 0 or s >= 999_000:
        return "var(--green)"
    if s < 300:
        return "var(--red)"
    if s < 3600:
        return "var(--yellow)"
    return "var(--green)"


def sparkline(values, width: int = 18) -> str:
    """Unicode-block-character sparkline from a sequence of numbers.

    Renders rightmost = most recent. Empty → dim placeholder."""
    blocks = "▁▂▃▄▅▆▇█"
    arr = list(values)[-width:]
    if not arr:
        return "<span class='dim'>—</span>"
    lo = min(arr)
    hi = max(arr)
    span = max(hi - lo, 1)
    out = []
    for v in arr:
        idx = int(round((v - lo) / span * (len(blocks) - 1)))
        out.append(blocks[idx])
    color = "var(--green)" if hi >= 1 else "var(--dim)"
    return f"<span class='spark' style='color:{color}'>{''.join(out)}</span>"


def _coordinated_unit_status_payload(box: BoxState) -> list[dict[str, object]]:
    """Return the bounded systemd projection retained by the lane resolver."""
    return [
        {
            "unit": str(row.get("unit") or ""),
            "active_state": str(row.get("active_state") or ""),
            "sub_state": str(row.get("sub_state") or ""),
            "pid": int(row.get("pid") or 0),
            "restarts": int(row.get("restarts") or 0),
        }
        for row in (
            getattr(box, "coordinated_unit_statuses", []) or []
        )
        if isinstance(row, dict)
    ]


def _service_inventory_payload(box: BoxState) -> list[dict[str, object]]:
    """Return bounded systemd/process attestations without promoting a unit."""

    allowed_roles = {
        "mining_controller",
        "wallet_free_generator",
        "certification",
        "support",
        "unknown",
    }
    result: list[dict[str, object]] = []
    for row in getattr(box, "service_inventory", []) or []:
        if not isinstance(row, dict):
            continue
        role = str(row.get("role") or "")
        if role not in allowed_roles:
            continue
        result.append({
            "unit": str(row.get("unit") or ""),
            "active_state": str(row.get("active_state") or ""),
            "sub_state": str(row.get("sub_state") or ""),
            "pid": int(row.get("pid") or 0),
            "restarts": int(row.get("restarts") or 0),
            "invocation_id": str(row.get("invocation_id") or ""),
            "service_user": str(row.get("service_user") or ""),
            "role": role,
            "submit_disabled": row.get("submit_disabled") is True,
            "exec_start_sha256": str(row.get("exec_start_sha256") or ""),
            "fragment_sha256": str(row.get("fragment_sha256") or ""),
        })
    return result


def _registry_active_entry(box: BoxState) -> dict[str, object]:
    """Return only an active entry from a successfully verified registry."""

    registry = getattr(box, "active_unit_registry", {}) or {}
    if not (
        isinstance(registry, dict)
        and not registry.get("error")
        and registry.get("schema_version") == 1
    ):
        return {}
    active = registry.get("active")
    return active if isinstance(active, dict) else {}


def _registry_controller_authority(box: BoxState) -> dict[str, object]:
    """Return the process-bound registry authority normalized by the probe."""

    telemetry = getattr(box, "standalone_telemetry", {}) or {}
    if not isinstance(telemetry, dict):
        return {}
    authority = telemetry.get("controller_authority")
    if not isinstance(authority, dict):
        return {}
    mode = str(authority.get("mode") or "").lower()
    unit = str(authority.get("unit") or "")
    if (
        authority.get("source") != "active_unit_registry"
        or mode not in {"canary", "mine"}
        or not unit
        or unit != str(getattr(box, "active_unit", "") or "")
    ):
        return {}
    return authority


def _reliquary_one_expected_lane(box: BoxState) -> tuple[str, str]:
    """Bind each configured miner service to its one admissible lane identity."""

    unit = str(getattr(box, "unit", "") or "").lower()
    if unit.endswith("@math.service"):
        return "math", "openmathinstruct"
    if unit.endswith("@code.service"):
        return "code", "opencodeinstruct"
    if unit == "reliquary-one.service":
        state_root = str(
            getattr(box, "reliquary_one_state_root", "") or ""
        )
        if state_root.endswith("/math/state"):
            return "math", "openmathinstruct"
        if state_root.endswith("/code/state"):
            return "code", "opencodeinstruct"
    return "", ""


def _box_lane_status(
    box: BoxState,
    *,
    validator_state: ValidatorState | None = None,
) -> dict[str, object]:
    """Project mining state without equating GPU work with submissions.

    Service discovery is diagnostic only.  Only a configured active controller
    plus fresh, process-bound standalone telemetry can be called ``CANARY`` or
    ``MINING``.  A digest-bound registry mode distinguishes quota-one canary
    from unrestricted mining when present; legacy lanes fall back to the unit
    name.  A live canary must never be presented as unrestricted mining.
    A wallet-free generator is useful capacity, but remains explicitly
    unattached and contributes no submission/selection/reward counters.
    """

    if getattr(box, "miner_kind", "legacy") == "reliquary_one":
        miner = getattr(box, "reliquary_one", {})
        miner = miner if isinstance(miner, dict) else {}
        funnel = miner.get("funnel")
        funnel = funnel if isinstance(funnel, dict) else {}
        issues = _box_readiness_issues(box, validator_state=validator_state)
        if not issues:
            status = "MINING"
            reason = "configured V4 service and exact active binding are current"
        elif any(issue.startswith("checkpoint_") for issue in issues):
            status = "FENCED"
            reason = "; ".join(issues)
        elif "down" in issues:
            status = "STOPPED"
            reason = "; ".join(issues)
        elif any("stale" in issue or "missing" in issue for issue in issues):
            status = "STALE"
            reason = "; ".join(issues)
        else:
            status = "BLOCKED"
            reason = "; ".join(issues)
        return {
            "status": status,
            "reason": reason,
            "funnel_authoritative": not issues,
            "funnel_scope": "redacted_v4_events" if not issues else "none",
            "funnel": (
                {
                    name: int(funnel.get(name) or 0)
                    for name in (
                        "generated_groups",
                        "protocol_valid_groups",
                        "locally_eligible_groups",
                        "signed_precommits",
                        "validator_ranked_candidates",
                        "selected_slots",
                    )
                }
                if not issues
                else {}
            ),
            "active_generator_units": [],
            "discovered_controller_units": [],
            "service_inventory": [],
        }

    inventory = _service_inventory_payload(box)
    generators = [
        row for row in inventory
        if row["role"] == "wallet_free_generator"
        and int(row["pid"] or 0) > 0
        and row["active_state"] == "active"
    ]
    certifications = [
        row for row in inventory
        if row["role"] == "certification"
        and int(row["pid"] or 0) > 0
        and row["active_state"] in {"active", "activating"}
    ]
    dynamic_controllers = [
        row for row in inventory
        if row["role"] == "mining_controller"
        and int(row["pid"] or 0) > 0
    ]
    telemetry = getattr(box, "standalone_telemetry", {}) or {}
    active_unit = str(getattr(box, "active_unit", "") or "").lower()
    funnel = telemetry.get("funnel") if isinstance(telemetry, dict) else None
    authoritative_funnel = bool(
        box.proc_alive
        and getattr(box, "standalone_fresh", False)
        and isinstance(funnel, dict)
    )
    safe_funnel = (
        {
            name: int(funnel.get(name, 0) or 0)
            for name in (
                "attempts",
                "generation_complete",
                "natural_complete_m8",
                "local_eligible",
                "precommit_accepted",
                "reveal_accepted",
                "pool_accepted",
                "selected",
                "rewarded",
                "terminal_final",
                "terminal_unresolved",
            )
        }
        if authoritative_funnel
        else {}
    )

    registry_active = _registry_active_entry(box)
    registry_mode = str(registry_active.get("mode") or "").lower()
    controller_authority = _registry_controller_authority(box)
    if getattr(box, "active_unit_registry_path", "") and not registry_active:
        return {
            "status": "BLOCKED",
            "reason": str(
                getattr(box, "unit_resolution_error", "")
                or "active_unit_registry_invalid"
            ),
            "funnel_authoritative": False,
            "funnel_scope": "none",
            "funnel": {},
            "active_generator_units": [
                str(row["unit"]) for row in generators
            ],
            "discovered_controller_units": [
                str(row["unit"]) for row in dynamic_controllers
            ],
            "service_inventory": inventory,
        }
    expected_checkpoint = str(
        _validator_model_identity(validator_state).get("revision") or ""
    ).lower()
    active_checkpoint = str(
        registry_active.get("checkpoint_revision") or ""
    ).lower()
    if (
        registry_mode in {"canary", "mine"}
        and expected_checkpoint
        and active_checkpoint != expected_checkpoint
    ):
        return {
            "status": "FENCED",
            "reason": (
                "registry_checkpoint_mismatch:"
                f"active={active_checkpoint[:12] or 'unknown'}:"
                f"validator={expected_checkpoint[:12]}"
            ),
            "funnel_authoritative": False,
            "funnel_scope": "none",
            "funnel": {},
            "active_generator_units": [
                str(row["unit"]) for row in generators
            ],
            "discovered_controller_units": [
                str(row["unit"]) for row in dynamic_controllers
            ],
            "service_inventory": inventory,
        }
    if registry_mode == "certifying":
        if (
            expected_checkpoint
            and active_checkpoint != expected_checkpoint
        ):
            return {
                "status": "FENCED",
                "reason": (
                    "certification_checkpoint_mismatch:"
                    f"active={active_checkpoint[:12] or 'unknown'}:"
                    f"validator={expected_checkpoint[:12]}"
                ),
                "funnel_authoritative": False,
                "funnel_scope": "none",
                "funnel": {},
                "active_generator_units": [
                    str(row["unit"]) for row in generators
                ],
                "discovered_controller_units": [
                    str(row["unit"]) for row in dynamic_controllers
                ],
                "service_inventory": inventory,
            }
        if box.proc_alive or generators or certifications:
            status = "CERTIFYING"
            reason = (
                "attested submit-disabled certification is active; "
                "no mining authority"
            )
        else:
            status = "STALLED"
            reason = "attested certification has no active service"
        return {
            "status": status,
            "reason": reason,
            "funnel_authoritative": False,
            "funnel_scope": "none",
            "funnel": {},
            "active_generator_units": [str(row["unit"]) for row in generators],
            "discovered_controller_units": [
                str(row["unit"]) for row in dynamic_controllers
            ],
            "service_inventory": inventory,
        }
    if registry_mode in {"canary", "mine"} and not box.proc_alive:
        return {
            "status": "STALLED",
            "reason": f"attested {registry_mode} controller is not active",
            "funnel_authoritative": False,
            "funnel_scope": "none",
            "funnel": {},
            "active_generator_units": [
                str(row["unit"]) for row in generators
            ],
            "discovered_controller_units": [
                str(row["unit"]) for row in dynamic_controllers
            ],
            "service_inventory": inventory,
        }

    identity_drift: list[str] = []
    if (
        validator_state is not None
        and box.proc_alive
        and bool(getattr(box, "standalone_telemetry_path", ""))
    ):
        expected_model = _validator_model_identity(validator_state)
        active_model = _active_model_identity(box)
        if expected_model.get("known") and not (
            active_model.get("valid")
            and _same_validator_model_identity(active_model, expected_model)
        ):
            identity_drift.append(
                "checkpoint_mismatch:"
                f"active={str(active_model.get('revision') or 'unknown')[:12]}:"
                f"validator={str(expected_model.get('revision') or 'unknown')[:12]}"
            )
        observed_image = str(
            getattr(box, "observed_validator_image_revision", "")
            or getattr(box, "reliquary_source_revision", "")
            or ""
        ).lower()
        advertised_image = str(
            getattr(validator_state, "image_revision", "") or ""
        ).lower()
        if advertised_image and observed_image != advertised_image:
            identity_drift.append(
                "validator_source_mismatch:"
                f"active={observed_image[:12] or 'unknown'}:"
                f"validator={advertised_image[:12]}"
            )

    if identity_drift:
        return {
            "status": "FENCED",
            "reason": "; ".join(identity_drift),
            "funnel_authoritative": False,
            "funnel_scope": "none",
            "funnel": {},
            "active_generator_units": [str(row["unit"]) for row in generators],
            "discovered_controller_units": [
                str(row["unit"]) for row in dynamic_controllers
            ],
            "service_inventory": inventory,
        }

    certification = getattr(box, "standalone_certification", {}) or {}
    if isinstance(certification, dict) and certification.get("active") is True:
        status = "CERTIFYING"
        reason = "submit-disabled certification is active; no mining authority"
    elif registry_mode == "fenced":
        active_checkpoint = str(
            registry_active.get("checkpoint_revision") or "unknown"
        )
        expected_checkpoint = str(
            _validator_model_identity(validator_state).get("revision")
            or "unknown"
        )
        status = "FENCED"
        reason = (
            "attested submission fence is active"
            f" (active checkpoint {active_checkpoint[:12]},"
            f" validator checkpoint {expected_checkpoint[:12]})"
        )
    elif registry_mode == "stalled":
        status = "STALLED"
        reason = "attested controller is stalled and has no live submission authority"
    elif registry_mode == "idle":
        status = "IDLE"
        reason = "attested lane is intentionally idle and has no mining authority"
    elif (
        getattr(box, "unit_resolution_error", "")
        and not (
            (generators or certifications)
            and str(getattr(box, "unit_resolution_error", "")).startswith(
                "no_allowed_unit_active"
            )
        )
    ):
        status = "BLOCKED"
        reason = str(getattr(box, "unit_resolution_error", ""))
    elif box.proc_alive:
        if authoritative_funnel and getattr(box, "runtime_parity_ok", False):
            lifecycle = telemetry.get("window_lifecycle")
            lifecycle = lifecycle if isinstance(lifecycle, dict) else {}
            lifecycle_status = str(lifecycle.get("status") or "").upper()
            if lifecycle_status in {"RECOVERING", "STALLED"}:
                status = lifecycle_status
                latest_run = lifecycle.get("latest_run")
                latest_run = latest_run if isinstance(latest_run, dict) else {}
                failure = ":".join(
                    part
                    for part in (
                        str(latest_run.get("failure_stage") or ""),
                        str(latest_run.get("failure_type") or ""),
                    )
                    if part
                )
                reason = (
                    f"current controller lifecycle is {lifecycle_status.lower()}"
                    + (f" ({failure})" if failure else "")
                )
            elif str(controller_authority.get("mode") or "") == "canary":
                status = "CANARY"
                reason = (
                    "registry-attested quota-one canary controller and "
                    "process-bound telemetry are current"
                )
            elif str(controller_authority.get("mode") or "") == "mine":
                status = "MINING"
                reason = (
                    "registry-attested mining controller and process-bound "
                    "telemetry are current"
                )
            elif "canary" in active_unit:
                status = "CANARY"
                reason = (
                    "quota-one canary controller and process-bound telemetry "
                    "are current"
                )
            else:
                status = "MINING"
                reason = "configured controller and process-bound telemetry are current"
        elif getattr(box, "standalone_telemetry_path", ""):
            status = "STALE"
            reason = str(getattr(box, "standalone_error", "") or "telemetry unavailable")
        else:
            status = "CONTROLLER_ACTIVE_UNATTESTED"
            reason = "controller PID is active without current standalone attestation"
    elif dynamic_controllers:
        status = "BLOCKED"
        reason = "unconfigured mining-capable controller discovered"
    elif generators:
        status = "READY_UNATTACHED"
        reason = "wallet-free generator active; no controller or submission authority"
    elif certifications:
        status = "CERTIFYING"
        reason = "submit-disabled certification service active"
    else:
        status = "STOPPED"
        reason = "no active mining controller or generator"

    return {
        "status": status,
        "reason": reason,
        "funnel_authoritative": authoritative_funnel,
        "funnel_scope": (
            str(telemetry.get("metrics_posture") or "active_profile")
            if authoritative_funnel
            else "none"
        ),
        "funnel": safe_funnel,
        "active_generator_units": [str(row["unit"]) for row in generators],
        "discovered_controller_units": [
            str(row["unit"]) for row in dynamic_controllers
        ],
        "service_inventory": inventory,
    }


def _box_restart_count_semantics(box: BoxState) -> str:
    """Name the backwards-compatible restart scalar's exact denominator."""
    if getattr(box, "coordinated_units", ()) or (
        getattr(box, "coordinated_unit_statuses", []) or []
    ):
        return "sum_systemd_nrestarts_across_coordinated_units"
    return "active_unit_systemd_nrestarts"


def _active_standalone_certification(
    box: BoxState,
) -> dict[str, object]:
    """Return only a fresh exact submit-disabled certification process.

    Certification is observability for real GPU work, never mining liveness.
    Keeping this accessor separate from ``proc_alive`` and standalone mining
    telemetry makes it difficult for renderers to accidentally turn a proof
    certification worker into attempts, admissions, selected slots, or reward.
    """
    raw = getattr(box, "standalone_certification", {})
    if not isinstance(raw, dict):
        return {}
    if not (
        raw.get("active") is True
        and raw.get("fresh") is True
        and raw.get("exact") is True
        and raw.get("submit_disabled_attested") is True
    ):
        return {}
    gpu = raw.get("gpu")
    if not isinstance(gpu, dict):
        return {}
    return raw


def _box_rolling_pregen(
    box: BoxState,
) -> tuple[int | None, int | None, str]:
    """Return only an authoritative rolling generation counter.

    Historical miners publish ``ACCEPTED-PREGEN`` journal lines, which are
    counted over exact 30/60 minute journal slices.  Standalone miners publish
    an atomic, profile-scoped funnel, but that snapshot does not currently
    expose an exact rolling 30/60 minute generation contract.  Treating its
    checkpoint/lifetime natural-M8 total as a rolling value would be false, so
    the old column is explicitly unavailable until the producer adds one.
    """

    if getattr(box, "miner_kind", "") == "reliquary_one":
        details = _box_v5_host_telemetry(box)
        events = details.get("fleet_events", {})
        events = events if isinstance(events, dict) else {}
        generation_30m = events.get("generation_30m")
        generation_60m = events.get("generation_60m")
        if isinstance(generation_30m, int) and not isinstance(
            generation_30m, bool
        ):
            return (
                generation_30m,
                (
                    generation_60m
                    if isinstance(generation_60m, int)
                    and not isinstance(generation_60m, bool)
                    else None
                ),
                "v5_fleet_events_generation",
            )
        return None, None, "v5_fleet_events_generation_unavailable"
    if getattr(box, "standalone_telemetry_path", ""):
        return None, None, "standalone_rolling_generation_unavailable"
    return (
        int(getattr(box, "pregen_30m", 0) or 0),
        int(getattr(box, "pregen_60m", 0) or 0),
        "legacy_journal_accepted_pregen",
    )


def _box_v5_host_telemetry(box: BoxState) -> dict[str, object]:
    """Return the redacted V5 per-host sidecar projection, if configured."""

    if getattr(box, "miner_kind", "") != "reliquary_one":
        return {"applicable": False}
    miner = getattr(box, "reliquary_one", {})
    miner = miner if isinstance(miner, dict) else {}
    telemetry = miner.get("v5_telemetry")
    telemetry = telemetry if isinstance(telemetry, dict) else {}
    fleet_events = telemetry.get("fleet_events")
    fleet_events = fleet_events if isinstance(fleet_events, dict) else {}
    economic_ledger = telemetry.get("economic_ledger")
    economic_ledger = (
        economic_ledger if isinstance(economic_ledger, dict) else {}
    )
    return {
        "applicable": True,
        "fleet_events": fleet_events,
        "economic_ledger": economic_ledger,
    }


def _standalone_auction_funnel_display(
    box: BoxState,
    validator_state: ValidatorState | None,
) -> dict[str, object]:
    """Render the normalized standalone funnel for either auction environment.

    Code has additional prescreen and grader readiness checks, but Math still
    publishes the same authoritative generated-to-rewarded funnel.  Keeping
    this fallback environment-neutral prevents a healthy Math controller from
    being rendered as ``non-Code`` with its real work hidden.
    """

    if not (
        getattr(box, "proc_alive", False)
        and getattr(box, "standalone_telemetry_path", "")
    ):
        return {"applicable": False}
    environment = str(
        getattr(box, "active_environment", "")
        or getattr(box, "miner_environment", "")
        or ""
    ).strip().lower()
    if environment not in {"openmathinstruct", "opencodeinstruct"}:
        return {"applicable": False}
    telemetry = getattr(box, "standalone_telemetry", {})
    telemetry = telemetry if isinstance(telemetry, dict) else {}
    funnel = telemetry.get("funnel")
    if not isinstance(funnel, dict):
        return {"applicable": False}
    issues = _standalone_readiness_issues(box, validator_state=validator_state)
    controller_mode = str(
        getattr(box, "standalone_controller_mode", "") or "unknown"
    )
    return {
        "applicable": True,
        "ok": not issues,
        "issues": issues,
        "environment": environment,
        "controller_mode": controller_mode,
        "funnel": funnel,
    }


def render_fleet_html() -> str:
    with _lock:
        boxes = list(_boxes)
        last = _last_poll_at
        validator_state = _vs
    age = (time.time() - last) if last else 0
    rows = []
    for b in boxes:
        v5_host_telemetry = _box_v5_host_telemetry(b)
        pregen_30m, _pregen_60m, pregen_source = _box_rolling_pregen(b)
        pregen_30m_text = "—" if pregen_30m is None else str(pregen_30m)
        pregen_title = (
            "Exact rolling generation from this V5 host's fleet-events sidecar; "
            "this is not the shared-hotkey validator verdict total"
            if pregen_source == "v5_fleet_events_generation"
            else "V5 fleet-events does not cover a complete rolling generation "
            "window yet; no zero is inferred"
            if pregen_source == "v5_fleet_events_generation_unavailable"
            else
            "Exact rolling generation is unavailable for this standalone "
            "miner; use the profile-scoped natural-M8 generation funnel"
            if pregen_30m is None
            else "Miner-side pregen rate (queue activity, not validator accepts)"
        )
        advertised_protocol = int(
            getattr(validator_state, "protocol_version", 0) or 0
        )
        advertised_profile_id = str(
            getattr(validator_state, "generation_profile_id", "")
            or "unknown"
        )
        profile_status = (
            f"advertised v{advertised_protocol} {advertised_profile_id}"
            if advertised_protocol
            else "advertised profile unknown"
        )
        posture_sub = ""
        certification = _active_standalone_certification(b)
        certification_identity = certification.get("identity", {})
        certification_identity = (
            certification_identity
            if isinstance(certification_identity, dict)
            else {}
        )
        certification_gpu = certification.get("gpu", {})
        certification_gpu = (
            certification_gpu
            if isinstance(certification_gpu, dict)
            else {}
        )
        display_gpu_mem_mb = (
            int(certification_gpu.get("memory_used_mb") or 0)
            if certification
            else b.gpu_mem_mb
        )
        display_gpu_total_mb = (
            int(certification_gpu.get("memory_total_mb") or 1)
            if certification
            else b.gpu_total_mb
        )
        display_gpu_util = (
            int(certification_gpu.get("utilization_pct") or 0)
            if certification
            else b.gpu_util
        )
        display_uptime_s = (
            int(float(certification.get("elapsed_s") or 0.0))
            if certification
            else b.proc_uptime_s
        )
        pct = (
            display_gpu_mem_mb
            * 100
            // max(display_gpu_total_mb, 1)
        )
        mem_color = color_for_pct(pct)
        alive = "●" if b.proc_alive else "○"
        alive_color = "var(--green)" if b.proc_alive else "var(--red)"
        acpt_value = getattr(b, "acpt_30m", None)
        acpt_color = (
            color_acpt(int(acpt_value))
            if isinstance(acpt_value, int) and not isinstance(acpt_value, bool)
            else "var(--dim)"
        )
        hk = _short_hotkey(b.hotkey)
        # Reject breakdown in last reject column
        rej_breakdown = []
        if b.window_mismatch_30m:
            rej_breakdown.append(f"<span class='small dim'>wm={b.window_mismatch_30m}</span>")
        if b.grail_fail_30m:
            rej_breakdown.append(f"<span class='small red'>gf={b.grail_fail_30m}</span>")
        if b.bad_term_30m:
            rej_breakdown.append(f"<span class='small red'>bt={b.bad_term_30m}</span>")
        rej_html = " ".join(rej_breakdown) if rej_breakdown else "<span class='dim'>—</span>"
        oom_age_color = color_oom_age(b.last_oom_age_s)
        oom_age_txt = fmt_age(b.last_oom_age_s) if b.last_oom_age_s >= 0 else "—"
        quarantined = bool(getattr(b, "quarantine_active", False))
        if quarantined:
            posture = "QUARANTINED"
            posture_color = "var(--red)"
            posture_title = (
                f"{getattr(b, 'quarantine_reason', '') or 'durable quarantine active'} · "
                f"{getattr(b, 'quarantine_path', '')}"
            )
        elif certification:
            posture = "CERTIFYING"
            posture_color = "var(--yellow)"
            posture_sub = (
                f"cert c{int(certification_identity.get('checkpoint_n') or 0)} · "
                f"source {str(certification_identity.get('miner_source_revision') or '')[:10]} · "
                f"{profile_status} · current attempts/selected/rewarded 0/0/0"
            )
            posture_title = (
                (
                    "exact wallet-free generation worker · non-mining · "
                    if certification.get("service_unit")
                    else "exact standalone certification · submit-disabled · "
                )
                +
                f"phase={certification.get('phase') or 'transition'} · "
                f"runner pid={int(certification.get('runner_pid') or 0)} · "
                f"worker pid={int(certification.get('worker_pid') or 0)} · "
                f"GPU-bound={bool(certification.get('gpu_process_bound'))}"
            )
        elif getattr(b, "standalone_telemetry_path", ""):
            if not b.proc_alive:
                supervisor = getattr(b, "standalone_supervisor", {})
                supervisor = supervisor if isinstance(supervisor, dict) else {}
                phase = str(
                    supervisor.get("phase")
                    or supervisor.get("state")
                    or ""
                )
                posture = "FENCED" + (f" · {phase}" if phase else "")
                posture_color = "var(--yellow)"
                posture_sub = (
                    f"{profile_status} · active none · "
                    "current attempts/selected/rewarded 0/0/0"
                )
            elif getattr(b, "standalone_fresh", False):
                posture = "standalone live"
                posture_color = "var(--green)"
            else:
                posture = "TELEMETRY STALE"
                posture_color = "var(--red)"
            posture_title = (
                f"atomic={getattr(b, 'standalone_telemetry_path', '')} · "
                f"age={getattr(b, 'standalone_age_s', -1.0):.1f}s · "
                f"{getattr(b, 'standalone_error', '') or 'fresh'}"
            )
        elif v5_host_telemetry.get("applicable"):
            v5_events = v5_host_telemetry.get("fleet_events", {})
            v5_events = v5_events if isinstance(v5_events, dict) else {}
            v5_ledger = v5_host_telemetry.get("economic_ledger", {})
            v5_ledger = v5_ledger if isinstance(v5_ledger, dict) else {}
            if getattr(b, "reference_ready", False) and v5_events.get(
                "available"
            ) is True:
                posture = "V5 host telemetry"
                posture_color = "var(--green)"
            elif getattr(b, "reference_ready", False):
                posture = "V5 sidecars unavailable"
                posture_color = "var(--yellow)"
            else:
                posture = "V5 telemetry stale"
                posture_color = "var(--red)"
            posture_sub = (
                "fleet-events="
                + (
                    "current"
                    if v5_events.get("complete_30m") is True
                    else "partial"
                    if v5_events.get("available") is True
                    else "missing"
                )
                + " · economic-ledger="
                + (
                    "tail linked"
                    if v5_ledger.get("tail_chain_linked") is True
                    else "observed"
                    if v5_ledger.get("available") is True
                    else "missing"
                )
            )
            posture_title = (
                "Per-host V5 state-root telemetry. Host-local "
                "accepted=True events are observed admissions, not a complete "
                "acceptance rate; rejected outcomes remain unavailable. "
                "Shared-hotkey validator verdicts are never allocated to this "
                "host without a local fleet-events window."
            )
        elif getattr(b, "reference_ready", False):
            posture = "reference ready"
            posture_color = "var(--green)"
            posture_title = (
                f"protocol={getattr(b, 'protocol_profile', '') or '—'} · "
                f"runtime parity={bool(getattr(b, 'runtime_parity_ok', False))}"
            )
        elif getattr(b, "protocol_profile", ""):
            posture = "parity pending"
            posture_color = "var(--yellow)"
            posture_title = f"protocol={getattr(b, 'protocol_profile', '')}"
        else:
            posture = "legacy/unknown"
            posture_color = "var(--dim)"
            posture_title = "reference-miner startup markers not observed"
        model_details = _box_model_details(b, validator_state)
        model = model_details["active"]
        provisioned_model = model_details["provisioned"]
        model_issues = (
            _box_model_readiness_issues(b, validator_state)
            if getattr(b, "engine_mode", "") == "reference"
            else []
        )
        if model["valid"]:
            model_kind = (
                "clean base"
                if model["kind"] == "clean_base"
                else "checkpoint"
            )
            model_label = f"{model_kind} n{model['checkpoint_n']}"
            model_repo = str(model["repo"])
            model_revision = str(model["revision"])
            model_sub = f"{model_repo.rsplit('/', 1)[-1]}@{model_revision[:10]}"
            model_color = "var(--yellow)" if model_issues else "var(--green)"
            model_source = str(model.get("source") or "unknown")
            model_title = (
                f"active ({model_source}) {model['kind']} n={model['checkpoint_n']} "
                f"{model_repo}@{model_revision}"
            )
            if model_source == "runtime_journal":
                model_title += (
                    f" · pid {model.get('pid')} start "
                    f"{model.get('process_started_at')} · {model.get('evidence')}"
                )
                if provisioned_model["valid"]:
                    model_title += (
                        " · provisioned baseline "
                        f"n={provisioned_model['checkpoint_n']} "
                        f"{provisioned_model['repo']}@"
                        f"{str(provisioned_model['revision'])[:12]}"
                    )
            if model_issues:
                model_title += " · " + ", ".join(
                    _readiness_issue_label(issue) for issue in model_issues
                )
        else:
            model_label = "unknown"
            model_sub = "manifest missing" if not model["complete"] else "manifest invalid"
            model_color = "var(--yellow)"
            model_title = model_sub
        if (
            getattr(b, "standalone_telemetry_path", "")
            and not b.proc_alive
            and model["valid"]
        ):
            model_label = f"historical {model_label}"
            model_sub = (
                f"historical v2 · {model_sub} · "
                f"{profile_status} n{int(getattr(validator_state, 'checkpoint_n', 0) or 0)}"
            )
            model_color = "var(--yellow)"
        if certification:
            posture_title += (
                " · exact cert "
                f"c{int(certification_identity.get('checkpoint_n') or 0)} "
                f"{certification_identity.get('model_repo') or 'unknown'}@"
                f"{certification_identity.get('checkpoint_revision') or 'unknown'} · "
                f"miner source {certification_identity.get('miner_source_revision') or 'unknown'} · "
                f"validator source {certification_identity.get('validator_source_revision') or 'unknown'}"
            )
        else:
            posture_title += f" · {model_title}"
        code_auction = _box_code_auction_details(b, validator_state)
        if certification:
            cert_environment = str(
                certification_identity.get("environment") or "unknown"
            )
            auction_label = (
                "Code certifying"
                if cert_environment == "opencodeinstruct"
                else "Math certifying"
                if cert_environment == "openmathinstruct"
                else "certifying"
            )
            auction_sub = (
                f"non-mining · {cert_environment} · "
                f"c{int(certification_identity.get('checkpoint_n') or 0)}"
            )
            auction_generation_sub = ""
            auction_terminal_sub = ""
            auction_color = "var(--yellow)"
            auction_title = (
                "Certification identity only; no auction readiness, "
                "submission, selection, or reward is implied"
            )
        elif v5_host_telemetry.get("applicable"):
            v5_events = v5_host_telemetry.get("fleet_events", {})
            v5_events = v5_events if isinstance(v5_events, dict) else {}
            v5_ledger = v5_host_telemetry.get("economic_ledger", {})
            v5_ledger = v5_ledger if isinstance(v5_ledger, dict) else {}
            generation_30m = v5_events.get("generation_30m")
            generation_60m = v5_events.get("generation_60m")
            accepted_30m = v5_events.get("verdict_accepted_30m")
            if v5_events.get("complete_30m") is True:
                auction_label = "V5 host telemetry"
                auction_color = "var(--green)"
            elif v5_events.get("available") is True:
                auction_label = "V5 telemetry partial"
                auction_color = "var(--yellow)"
            else:
                auction_label = "V5 telemetry unavailable"
                auction_color = "var(--yellow)"
            auction_sub = (
                "fleet-events="
                + (
                    "30m event coverage"
                    if v5_events.get("complete_30m") is True
                    else "insufficient coverage"
                )
                + " · economic-ledger="
                + (
                    "tail linked"
                    if v5_ledger.get("tail_chain_linked") is True
                    else "observed"
                    if v5_ledger.get("available") is True
                    else "unavailable"
                )
            )
            if isinstance(generation_30m, int) and not isinstance(
                generation_30m, bool
            ):
                generation_60m_text = (
                    str(generation_60m)
                    if isinstance(generation_60m, int)
                    and not isinstance(generation_60m, bool)
                    else "—"
                )
                auction_generation_sub = (
                    "host-local generation: 30m="
                    f"{generation_30m} · 60m={generation_60m_text}"
                )
            else:
                auction_generation_sub = (
                    "host-local generation: unavailable until fleet-events "
                    "covers the complete rolling window"
                )
            accepted_30m_text = (
                str(accepted_30m)
                if isinstance(accepted_30m, int)
                and not isinstance(accepted_30m, bool)
                else "—"
            )
            auction_terminal_sub = (
                "observed host-local admissions: 30m accepted/rejected="
                f"{accepted_30m_text}/—"
            )
            auction_title = (
                "V5 fleet-events and economic-ledger telemetry are host "
                "scoped. This is not legacy Code-auction readiness, and it "
                "does not claim a complete acceptance rate, rejected-outcome "
                "count, canonical selection, or reward."
            )
        elif code_auction["applicable"]:
            auction_ok = bool(code_auction["ok"])
            auction_label = "auction ready" if auction_ok else "auction blocked"
            auction_color = "var(--green)" if auction_ok else "var(--red)"
            ledger_details = code_auction.get("ledger", {})
            grader_details = code_auction.get("grader", {})
            current_partition = (
                ledger_details.get("current_partition")
                if isinstance(ledger_details, dict)
                else None
            )
            generation_details = (
                ledger_details.get("generation_outcomes", {})
                if isinstance(ledger_details, dict)
                else {}
            )
            generation_details = (
                generation_details
                if isinstance(generation_details, dict)
                else {}
            )
            generation_current = generation_details.get("current")
            terminal_details = (
                ledger_details.get("terminal_funnel", {})
                if isinstance(ledger_details, dict)
                else {}
            )
            terminal_details = (
                terminal_details
                if isinstance(terminal_details, dict)
                else {}
            )
            terminal_current = terminal_details.get("current")
            auction_sub = (
                f"pre={'on' if code_auction.get('prescreen_enabled') else 'off'} · "
                f"policy={code_auction.get('auction_policy') or '—'} · "
                f"ledger={'current' if current_partition else 'missing'} · "
                f"grader={'ready' if isinstance(grader_details, dict) and grader_details.get('metrics_ok') else 'blocked'}"
            )
            if isinstance(generation_current, dict):
                if generation_details.get("source") == "standalone_atomic":
                    auction_generation_sub = (
                        "generation: attempts="
                        f"{int(generation_current.get('attempts', 0) or 0)}"
                        " · complete="
                        f"{int(generation_current.get('generation_complete', 0) or 0)}"
                        " · natural M8="
                        f"{int(generation_current.get('natural_complete_m8', 0) or 0)}"
                        " · local eligible="
                        f"{int(generation_current.get('local_eligible', 0) or 0)}"
                    )
                else:
                    natural_rate = generation_current.get(
                        "natural_eos_before_bound_rate"
                    )
                    natural_rate_text = (
                        f"{float(natural_rate) * 100.0:.1f}%"
                        if isinstance(natural_rate, (float, int))
                        else "—"
                    )
                    auction_generation_sub = (
                        "generation: completed="
                        f"{int(generation_current.get('natural_eos_complete', 0) or 0)}"
                        " · local_token_limit="
                        f"{int(generation_current.get('local_token_limit', 0) or 0)}"
                        " · safe_deadline="
                        f"{int(generation_current.get('safe_deadline', 0) or 0)}"
                        f" · natural-EOS/decisive={natural_rate_text}"
                    )
            elif generation_details.get("error"):
                auction_generation_sub = "generation outcomes: extension unavailable"
            elif generation_details.get("schema_ok"):
                auction_generation_sub = (
                    "generation outcomes: awaiting exact current partition/lane"
                )
            elif generation_details.get("table_present"):
                auction_generation_sub = "generation outcomes: extension unavailable"
            else:
                auction_generation_sub = "generation outcomes: legacy ledger"
            if isinstance(terminal_current, dict):
                if terminal_details.get("source") == "standalone_atomic":
                    auction_terminal_sub = (
                        "terminal funnel: attempts="
                        f"{int(terminal_current.get('attempts', 0) or 0)}"
                        " · terminal="
                        f"{int(terminal_current.get('terminal_final', 0) or 0)}"
                        " · unresolved="
                        f"{int(terminal_current.get('terminal_unresolved', 0) or 0)}"
                        " · precommit/reveal="
                        f"{int(terminal_current.get('precommit_accepted', 0) or 0)}/"
                        f"{int(terminal_current.get('reveal_accepted', 0) or 0)}"
                        " · pool="
                        f"{int(terminal_current.get('pool_accepted', 0) or 0)}"
                        " · selected/rewarded="
                        f"{int(terminal_current.get('selected', 0) or 0)}/"
                        f"{int(terminal_current.get('rewarded', 0) or 0)}"
                    )
                else:
                    terminal_schema = int(
                        terminal_details.get("api_version", 0) or 0
                    )
                    schema_label = (
                        f" (schema v{terminal_schema})"
                        if terminal_schema
                        else ""
                    )
                    auction_terminal_sub = (
                        f"terminal funnel{schema_label}: attempts="
                        f"{int(terminal_current.get('attempts', 0) or 0)}"
                        " · HTTP provisional="
                        f"{int(terminal_current.get('http_provisional', 0) or 0)}"
                        " · pool accepted="
                        f"{int(terminal_current.get('pool_accepted', 0) or 0)}"
                        " · selected="
                        f"{int(terminal_current.get('selected', 0) or 0)}"
                        " · rewarded="
                        f"{int(terminal_current.get('rewarded', 0) or 0)}"
                    )
            elif terminal_details.get("extension_table_present"):
                auction_terminal_sub = (
                    "terminal funnel: awaiting compatible exact-current partition"
                )
            else:
                auction_terminal_sub = (
                    "terminal funnel: legacy ledger (compatible until cutover)"
                )
            auction_title = " · ".join(
                _readiness_issue_label(str(issue))
                for issue in code_auction.get("issues", [])
            ) or "exact Code pre-screen, policy, ledger, grader, and source ready"
            auction_title += (
                " · safe_deadline is right-censored and excluded from the "
                "natural-EOS decisive denominator"
            )
        else:
            standalone_auction = _standalone_auction_funnel_display(
                b, validator_state
            )
            if standalone_auction.get("applicable"):
                auction_ok = bool(standalone_auction.get("ok"))
                funnel = standalone_auction.get("funnel", {})
                funnel = funnel if isinstance(funnel, dict) else {}
                environment = str(standalone_auction.get("environment") or "")
                controller_mode = str(
                    standalone_auction.get("controller_mode") or "unknown"
                )
                auction_label = (
                    "auction ready" if auction_ok else "auction blocked"
                )
                auction_color = "var(--green)" if auction_ok else "var(--red)"
                auction_sub = (
                    f"env={environment} · controller={controller_mode} · "
                    "telemetry=standalone_atomic"
                )
                auction_generation_sub = (
                    "generation: attempts="
                    f"{int(funnel.get('attempts', 0) or 0)}"
                    " · complete="
                    f"{int(funnel.get('generation_complete', 0) or 0)}"
                    " · natural M8="
                    f"{int(funnel.get('natural_complete_m8', 0) or 0)}"
                    " · local eligible="
                    f"{int(funnel.get('local_eligible', 0) or 0)}"
                )
                auction_terminal_sub = (
                    "terminal funnel: attempts="
                    f"{int(funnel.get('attempts', 0) or 0)}"
                    " · terminal="
                    f"{int(funnel.get('terminal_final', 0) or 0)}"
                    " · unresolved="
                    f"{int(funnel.get('terminal_unresolved', 0) or 0)}"
                    " · precommit/reveal="
                    f"{int(funnel.get('precommit_accepted', 0) or 0)}/"
                    f"{int(funnel.get('reveal_accepted', 0) or 0)}"
                    " · pool="
                    f"{int(funnel.get('pool_accepted', 0) or 0)}"
                    " · selected/rewarded="
                    f"{int(funnel.get('selected', 0) or 0)}/"
                    f"{int(funnel.get('rewarded', 0) or 0)}"
                )
                auction_title = " · ".join(
                    _readiness_issue_label(str(issue))
                    for issue in standalone_auction.get("issues", [])
                ) or "exact profile-bound standalone auction funnel is current"
            else:
                auction_label = "—"
                auction_sub = "non-Code"
                auction_generation_sub = ""
                auction_terminal_sub = ""
                auction_color = "var(--dim)"
                auction_title = "No exact standalone auction funnel is available"
        auction_generation_html = "".join(
            "<div class='dim small'>" + html.escape(detail) + "</div>"
            for detail in (auction_generation_sub, auction_terminal_sub)
            if detail
        )
        acceptance_source = getattr(b, "acceptance_source", "events") or "events"
        acceptance_scope = getattr(b, "acceptance_scope", "per_hotkey") or "per_hotkey"
        final_accept_value = getattr(b, "final_accept_30m", b.acpt_30m)
        final_reject_value = getattr(b, "final_reject_30m", b.rej_30m)
        final_accept = (
            int(final_accept_value)
            if isinstance(final_accept_value, int)
            and not isinstance(final_accept_value, bool)
            else None
        )
        final_reject = (
            int(final_reject_value)
            if isinstance(final_reject_value, int)
            and not isinstance(final_reject_value, bool)
            else None
        )
        final_accept_text = "—" if final_accept is None else str(final_accept)
        final_reject_text = "—" if final_reject is None else str(final_reject)
        acceptance_source_label = {
            "v5_fleet_events_admissions_observed": "V5 host admissions observed",
            "shared_hotkey_aggregate_unattributable": "shared aggregate · unallocated",
            "duplicate-hotkey": "shared aggregate · duplicate",
        }.get(acceptance_source, acceptance_source)
        shared_hotkey_verdicts = getattr(b, "shared_hotkey_verdicts", {})
        shared_hotkey_verdicts = (
            shared_hotkey_verdicts
            if isinstance(shared_hotkey_verdicts, dict)
            else {}
        )
        shared_aggregate_detail = ""
        if acceptance_scope == "shared_hotkey_aggregate":
            shared_aggregate_detail = (
                " · shared validator total "
                f"{shared_hotkey_verdicts.get('accepted_30m', '—')}/"
                f"{shared_hotkey_verdicts.get('rejected_30m', '—')}"
            )
        final_reason = getattr(b, "last_final_reason", "") or "—"
        unit_error = str(getattr(b, "unit_resolution_error", "") or "")
        lane_status = _box_lane_status(b, validator_state=validator_state)
        active_unit = str(getattr(b, "active_unit", "") or "")
        active_lane = str(getattr(b, "active_lane", "") or "")
        active_units = list(getattr(b, "active_units", []) or [])
        active_lanes = list(getattr(b, "active_lanes", []) or [])
        active_environment = str(getattr(b, "active_environment", "") or "")
        configured_units = [
            str(spec[0]) for spec in getattr(b, "unit_candidates", ())
        ] or ([str(b.unit)] if b.unit else [])
        coordinated_statuses = _coordinated_unit_status_payload(b)
        restart_semantics = _box_restart_count_semantics(b)
        restart_marker = "rΣ" if coordinated_statuses else "r"
        unexpected_units = list(getattr(b, "unexpected_active_units", []) or [])
        if certification:
            lane_label = "certifying (non-mining)"
            lane_color = "var(--yellow)"
            lane_sub = (
                f"{certification.get('phase') or 'transition'} · "
                f"worker pid {int(certification.get('worker_pid') or 0)}"
            )
        elif lane_status["status"] == "READY_UNATTACHED":
            lane_label = "generator ready"
            lane_color = "var(--yellow)"
            lane_sub = str(lane_status["reason"])
        elif lane_status["status"] in {"RECOVERING", "STALLED", "FENCED"}:
            lane_label = str(lane_status["status"]).lower()
            lane_color = (
                "var(--yellow)"
                if lane_status["status"] == "RECOVERING"
                else "var(--red)"
            )
            lane_sub = str(lane_status["reason"])
        elif unit_error:
            lane_label = "blocked"
            lane_color = "var(--red)"
            lane_sub = unit_error
        else:
            lane_label = " + ".join(active_lanes) or active_lane or "—"
            lane_color = "var(--green)" if lane_label != "—" else "var(--dim)"
            lane_sub = active_environment or "no active service"
            if len(active_lanes) > 1:
                lane_sub += f" · {len(active_lanes)} coordinated lanes"
        lane_title = (
            f"status={lane_status['status']} reason={lane_status['reason']}; "
            f"active={active_unit or 'none'} pid={int(getattr(b, 'active_pid', 0) or 0)} "
            f"restarts={int(getattr(b, 'restart_count', 0) or 0)} "
            f"({restart_semantics}); "
            f"allowed={','.join(configured_units) or 'unmanaged'}"
        )
        if active_units:
            lane_title += f"; active_set={','.join(active_units)}"
        if coordinated_statuses:
            lane_title += "; unit_statuses=" + ",".join(
                f"{row['unit']}[{row['active_state']}/{row['sub_state']},"
                f"pid={row['pid']},r={row['restarts']}]"
                for row in coordinated_statuses
            )
        if unexpected_units:
            lane_title += f"; unexpected={','.join(unexpected_units)}"
        if certification:
            lane_title += (
                "; exact submit-disabled certification only; "
                f"runner_pid={int(certification.get('runner_pid') or 0)}; "
                f"worker_pid={int(certification.get('worker_pid') or 0)}; "
                "no active mining unit"
            )
        rows.append(f"""
<tr>
  <td><button class="fleet-detail-button" type="button" data-box-label="{html.escape(b.label)}" aria-label="Open details for {html.escape(b.label)}"><span class="lbl" style="color:{b.color}">{html.escape(b.label)}</span></button></td>
  <td class="mono" title="{html.escape(b.hotkey)}">{html.escape(hk)}</td>
  <td style="color:{alive_color};text-align:center;font-size:1.4em">{alive}</td>
  <td class="mono" style="color:{lane_color}" title="{html.escape(lane_title)}">{html.escape(lane_label)}<div class="dim small">{html.escape(lane_sub)} · pid {int(getattr(b, 'active_pid', 0) or 0)} · {restart_marker}{int(getattr(b, 'restart_count', 0) or 0)}</div></td>
  <td class="mono dim">{fmt_age(display_uptime_s)}</td>
  <td class="mono" style="color:{posture_color}" title="{html.escape(posture_title)}">{html.escape(posture)}<div class="dim small">{html.escape(posture_sub)}</div></td>
  <td class="mono" style="color:{auction_color}" title="{html.escape(auction_title)}">{html.escape(auction_label)}<div class="dim small">{html.escape(auction_sub)}</div>{auction_generation_html}</td>
  <td class="mono" style="color:{model_color}" title="{html.escape(model_title)}">{html.escape(model_label)}<div class="dim small">{html.escape(model_sub)}</div></td>
  <td>
    <span style="color:{mem_color}">{display_gpu_mem_mb//1024}/{display_gpu_total_mb//1024}&nbsp;GB</span>
    <span class="dim">({pct}%)</span>
  </td>
  <td class="num">{display_gpu_util}%</td>
  <td class="num" style="color:{acpt_color};font-weight:700">{'—' if b.acpt_30m is None else b.acpt_30m}</td>
  <td class="num dim">{'—' if b.acpt_60m is None else b.acpt_60m}</td>
  <td class="mono" title="Last pool/proof verdict reason: {html.escape(final_reason)}{html.escape(shared_aggregate_detail)}"><b style="color:{color_acpt(final_accept or 0) if final_accept is not None else 'var(--dim)'}">{final_accept_text}</b><span class="dim">/{final_reject_text} · {html.escape(acceptance_source_label)}{html.escape(shared_aggregate_detail)}</span></td>
  <td class="num dim" data-pregen-source="{html.escape(pregen_source)}" title="{html.escape(pregen_title)}">{html.escape(pregen_30m_text)}</td>
  <td class="num" style="color:{'var(--red)' if b.late_drops_30m > 0 else 'var(--dim)'}" title="Late drops — submissions lost to FIFO race before validation">{b.late_drops_30m}</td>
  <td class="num">{sparkline(b.acpt_trend, width=14)}</td>
  <td class="num dim">{b.mean_accept_t_s:.0f}s</td>
  <td>{rej_html}</td>
  <td class="mono" style="color:{oom_age_color}">{oom_age_txt}</td>
  <td class="num" style="color:{'var(--red)' if b.oom_60m else 'var(--dim)'}">{b.oom_60m}</td>
  <td class="mono dim">{html.escape(b.last_event_at)}</td>
  <td class="mono dim">{html.escape(b.error)}</td>
</tr>
""")
    poll_meta = (
        f"<span class='dim'>polled {int(age)}s ago · "
        f"#{_poll_count}</span>"
    )
    return f"""
<div class="panel">
  <h2>Fleet — per-box health {poll_meta}</h2>
  <table class="grid">
    <thead>
      <tr>
        <th>box</th><th>hotkey</th><th>alive</th><th title="Exactly one configured legacy or named systemd lane may be active">lane / env</th><th>uptime</th><th title="Reference-miner readiness and durable quarantine state">posture</th><th title="Profile-bound generated-to-rewarded funnel; Code additionally includes prescreen, grader, and source readiness">auction funnel</th><th title="Exact model identity recorded by the atomic source manifest and compared with live validator state">model</th>
        <th>GPU mem</th><th>util</th>
        <th title="Pool/proof admissions in the last 30 min. A shared-hotkey V5 row uses observed host-local admitted events when its fleet-events tail covers the window; otherwise the value is unavailable rather than assigned from /verdicts.">pool/30m</th>
        <th title="Pool/proof admissions in the last 60 min. V5 values are observed host-local admissions, not a complete acceptance rate; this is not R2 selection or reward.">pool/60m</th>
        <th title="Pool/proof accept/reject counts and attribution scope. V5 shows observed host-local admissions as accepted/— because fleet-events does not provide a complete rejected-outcome source. Shared-hotkey validator totals are displayed only as explicitly unallocated aggregates.">pool a/r</th>
        <th title="Exact rolling miner-side generation when available. V5 lanes use host-local fleet-events; legacy lanes use ACCEPTED-PREGEN journal lines. This is never validator admission.">pregen/30m</th>
        <th title="Validator-side late drops in last 30 min — submissions that reached the validator HTTP but the batcher window had already advanced before the worker could pick them up. Non-zero means this box is losing the FIFO race; rollouts never reach GRAIL verify.">late/30m</th>
        <th title="Sparkline of validator pool/30m across the last ~20 polls">trend</th>
        <th title="Mean response time of pool/proof admissions — lower = earlier FIFO arrival; this is not a selected slot">mean rt</th>
        <th title="Reject breakdown — wm=window_mismatch, gf=grail_fail, bt=bad_term">rejects</th>
        <th title="Time since last OutOfMemoryError">last OOM</th>
        <th title="OOMs in last 60 min">oom/60m</th>
        <th>last evt</th><th>err</th>
      </tr>
    </thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</div>
"""


def _lab_is_running(lab: LabState) -> bool:
    return (
        lab.unit_active_state == "active"
        and lab.unit_sub_state == "running"
        and lab.active_pid > 0
    )


def _lab_isolation_status(lab: LabState) -> tuple[str, str, str]:
    """Describe isolation without inventing a process for an idle lab."""
    if _lab_is_running(lab):
        if lab.submit_disabled_attested and lab.lane_start_disarmed:
            return (
                "live process isolated",
                "var(--green)",
                "process + manifest attested · lane-start disarmed",
            )
        return (
            "live isolation blocked",
            "var(--red)",
            "running process lacks submit-disabled or lane-start attestation",
        )
    if lab.manifest_submit_disabled and lab.manifest_provisioned_ok:
        return (
            "offline manifest attested",
            "var(--green)",
            "wallet-free dashboard row · no live process to attest",
        )
    return (
        "offline manifest unverified",
        "var(--red)",
        "submit-disabled source manifest is missing or unprovisioned",
    )


def _lab_artifact_lifecycle(
    lab: LabState,
    *,
    validator_window: int | None,
) -> dict[str, object]:
    """Compare an immutable artifact cutoff with an observed validator window."""
    try:
        observed_window = int(validator_window or 0)
        expires_after = int(lab.artifact_expires_after_window or 0)
    except (TypeError, ValueError, OverflowError):
        observed_window = 0
        expires_after = 0
    comparable = bool(
        lab.artifact_manifest_ok
        and observed_window > 0
        and expires_after > 0
    )
    expired = observed_window > expires_after if comparable else None
    return {
        "artifact_current": (not expired) if comparable else None,
        "expired": expired,
        "validator_window": observed_window or None,
    }


def _lab_status(
    lab: LabState,
    *,
    validator_window: int | None = None,
) -> tuple[str, str, str]:
    if lab.selector_artifact_manifest:
        if not lab.artifact_manifest_ok:
            return (
                "ARTIFACT INVALID",
                "var(--red)",
                lab.artifact_error or "selector artifact unavailable",
            )
        if lab.artifact_status == "shadow_only":
            lifecycle = _lab_artifact_lifecycle(
                lab,
                validator_window=validator_window,
            )
            if lifecycle["expired"] is True:
                return (
                    "SELECTOR EXPIRED",
                    "var(--red)",
                    "immutable shadow artifact expired after "
                    f"w{lab.artifact_expires_after_window}; validator "
                    f"w{lifecycle['validator_window']}",
                )
            return (
                "SELECTOR SHADOW",
                "var(--yellow)",
                lab.artifact_decision_reason
                or "completed immutable challenger; activation gate not claimed",
            )
        return (
            "SELECTOR REJECTED",
            "var(--red)",
            "completed immutable challenger rejected by its activation policy",
        )
    running = _lab_is_running(lab)
    isolated = lab.submit_disabled_attested and lab.lane_start_disarmed
    if running and isolated:
        return "OFFLINE ACTIVE", "var(--green)", "wallet-free isolation attested"
    if running:
        return "ISOLATION BLOCKED", "var(--red)", "live process isolation unverified"
    if lab.unit_active_state in {"activating", "reloading"}:
        return "OFFLINE STARTING", "var(--yellow)", lab.unit_sub_state or "starting"
    if lab.unit_active_state == "failed":
        return "OFFLINE FAILED", "var(--red)", lab.unit_sub_state or "failed"
    if lab.manifest_submit_disabled and lab.manifest_provisioned_ok:
        return (
            "OFFLINE INACTIVE",
            "var(--dim)",
            "submit-disabled manifest attested; no live process",
        )
    return "OFFLINE INACTIVE", "var(--dim)", lab.unit_sub_state or "inactive"


def _lab_payload(
    lab: LabState,
    *,
    now: float | None = None,
    validator_window: int | None = None,
) -> dict:
    """Public lab telemetry with no live-miner/accounting dimensions."""
    now = time.time() if now is None else float(now)
    status, _color, status_detail = _lab_status(
        lab,
        validator_window=validator_window,
    )
    isolation_status, _isolation_color, isolation_detail = _lab_isolation_status(lab)
    age_s = max(0.0, now - lab.last_poll_s) if lab.last_poll_s else None
    artifact_lifecycle = _lab_artifact_lifecycle(
        lab,
        validator_window=validator_window,
    )
    exploration_rate = (
        lab.artifact_exploration_bps / 10_000
        if lab.artifact_exploration_bps is not None
        else None
    )
    return {
        "label": lab.label,
        "host": _display_ssh_alias(lab.alias),
        "role": "submit-disabled-lab",
        "status": status.lower().replace(" ", "_"),
        "status_detail": status_detail,
        "last_poll_s": lab.last_poll_s,
        "age_s": age_s,
        "unit": {
            "name": lab.unit,
            "active_state": lab.unit_active_state,
            "sub_state": lab.unit_sub_state,
            "enablement": lab.unit_enablement,
            "pid": lab.active_pid,
            "restarts": lab.restart_count,
        },
        "isolation": {
            "status": isolation_status.replace(" ", "_"),
            "detail": isolation_detail,
            "process_attestation_applicable": _lab_is_running(lab),
            "wallet_free_dashboard_row": True,
            "submit_disabled_attested": lab.submit_disabled_attested,
            "lane_start_disarmed": lab.lane_start_disarmed,
            "manifest_submit_disabled": lab.manifest_submit_disabled,
            "manifest_provisioned_ok": lab.manifest_provisioned_ok,
        },
        "gpu": {
            "name": lab.gpu_name,
            "compute_capability": lab.compute_capability,
            "memory_used_mb": lab.gpu_mem_mb,
            "memory_total_mb": lab.gpu_total_mb,
            "utilization_pct": lab.gpu_util,
        },
        "identity": {
            "release": lab.miner_release,
            "manifest_release": lab.miner_release,
            "evidence_release": lab.evidence_miner_release,
            "release_source": "manifest" if lab.miner_release else "unavailable",
            "source_revision": lab.source_revision,
            "checkpoint_repo_id": lab.checkpoint_repo_id,
            "checkpoint_revision": lab.checkpoint_revision,
            "checkpoint_n": lab.checkpoint_n,
            "environment": lab.environment,
            "hardware_name": lab.hardware_name,
            "parity_status": lab.parity_status,
        },
        "evidence": {
            "database": lab.evidence_db,
            "release": lab.evidence_miner_release,
            "ok": lab.evidence_db_ok,
            "schema_version": lab.evidence_schema_version,
            "attempts": lab.attempts,
            "terminal": lab.terminal_attempts,
            "locally_complete": lab.complete_attempts,
            "censored": lab.censored_attempts,
            "deadline_censored": lab.deadline_censored_attempts,
            "token_censored": lab.token_censored_attempts,
            "pending": lab.pending_attempts,
            "errors": lab.error_attempts,
            "first_window_n": lab.first_window_n,
            "last_window_n": lab.last_window_n,
            "last_attempt_at": lab.last_attempt_at,
            "error": lab.evidence_error,
        },
        "selector_artifact": {
            "configured": bool(lab.selector_artifact_manifest),
            "manifest": lab.selector_artifact_manifest,
            "ok": lab.artifact_manifest_ok,
            "status": lab.artifact_status,
            "kind": lab.artifact_kind,
            "model_version": lab.artifact_model_version,
            "digest": lab.artifact_digest,
            "file_sha256": lab.artifact_file_sha256,
            "data_digest": lab.artifact_data_digest,
            **artifact_lifecycle,
            "identity": {
                "source_revision": lab.artifact_source_revision,
                "checkpoint_repo_id": lab.artifact_checkpoint_repo_id,
                "checkpoint_revision": lab.artifact_checkpoint_revision,
                "checkpoint_n": lab.artifact_checkpoint_n,
            },
            "scope": {
                "window_start": lab.artifact_window_start,
                "window_end": lab.artifact_window_end,
                "train_local_rows": lab.artifact_train_local_rows,
                "train_population_rows": lab.artifact_train_population_rows,
                "completion_terminal_rows": (
                    lab.artifact_completion_terminal_rows
                ),
                "holdout_rows": lab.artifact_holdout_rows,
                "holdout_window": lab.artifact_holdout_window,
            },
            "policy": {
                "payout_profile": lab.artifact_payout_profile,
                "economic_target": lab.artifact_economic_target,
                "online_activation_allowed": (
                    lab.artifact_online_activation_allowed
                ),
                "expires_after_window": (
                    lab.artifact_expires_after_window
                ),
                "objective": lab.artifact_objective,
                "exploration_bps": lab.artifact_exploration_bps,
                "exploration_rate": exploration_rate,
            },
            "validation": {
                "activation_gate_passed": (
                    lab.artifact_activation_gate_passed
                ),
                "top_quintile_lift": lab.artifact_top_quintile_lift,
                "heldout_value_lift": lab.artifact_heldout_value_lift,
                "completion_brier": lab.artifact_completion_brier,
                "selection_brier": lab.artifact_selection_brier,
                "value_multiclass_brier": (
                    lab.artifact_value_multiclass_brier
                ),
                "emitted_slot_lift": lab.artifact_emitted_slot_lift,
                "emission_mse": lab.artifact_emission_mse,
                "emission_base_mse": lab.artifact_emission_base_mse,
                "shadow_promotion_gate_passed": (
                    lab.artifact_shadow_promotion_gate_passed
                ),
                "activation_blocker": lab.artifact_activation_blocker,
            },
            "search_report": {
                "gpu_rolling_selection_lift_mean": (
                    lab.artifact_gpu_rolling_lift_mean
                ),
                "gpu_final_selection_lift": lab.artifact_gpu_final_lift,
                "gpu_positive_value_folds": (
                    lab.artifact_gpu_positive_value_folds
                ),
                "gpu_rolling_folds": lab.artifact_gpu_rolling_folds,
                "cpu_rolling_selection_lift_mean": (
                    lab.artifact_cpu_rolling_lift_mean
                ),
                "cpu_positive_value_folds": (
                    lab.artifact_cpu_positive_value_folds
                ),
                "cpu_rolling_folds": lab.artifact_cpu_rolling_folds,
                "cpu_bitwise_reproducible": (
                    lab.artifact_cpu_bitwise_reproducible
                ),
                "gpu_bitwise_reproducible": (
                    lab.artifact_gpu_bitwise_reproducible
                ),
                "blockers": list(lab.artifact_blockers),
                "decision_reason": lab.artifact_decision_reason,
            },
            "error": lab.artifact_error,
        },
        "probe_error": lab.error,
    }


def _component_controller_binding(
    component: ComponentState,
    controller: BoxState | None,
) -> dict[str, object]:
    """Match a component only through the controller's immutable manifest."""
    if controller is None:
        return {"attached": False, "reason": "controller_missing"}
    bindings = getattr(controller, "runtime_components", [])
    if not isinstance(bindings, list) or not bindings:
        return {"attached": False, "reason": "controller_manifest_unbound"}
    matches = [
        row
        for row in bindings
        if isinstance(row, dict)
        and row.get("role") == component.role
        and row.get("manifest_file_sha256") == component.manifest_sha256
        and row.get("runtime_profile_sha256")
        == component.runtime_profile_sha256
    ]
    if len(matches) != 1:
        return {
            "attached": False,
            "reason": (
                "component_binding_missing"
                if not matches
                else "component_binding_ambiguous"
            ),
        }
    return {
        "attached": True,
        "reason": "exact_controller_manifest_binding",
        "component_id": str(matches[0].get("component_id") or ""),
    }


def _component_payload(
    component: ComponentState,
    *,
    boxes: list[BoxState],
    chain_entries: dict[str, object],
    now: float | None = None,
) -> dict[str, object]:
    """Return component truth without inventing a second mining funnel."""
    now = time.time() if now is None else float(now)
    controller = next(
        (
            box
            for box in boxes
            if box.label == component.controller_label
        ),
        None,
    )
    binding = _component_controller_binding(component, controller)
    generator_ok = bool(
        component.generator_active_state == "active"
        and component.generator_sub_state == "running"
        and component.generator_pid > 0
    )
    tunnel_ok = bool(
        component.tunnel_active_state == "active"
        and component.tunnel_sub_state == "running"
        and component.tunnel_pid > 0
    )
    poll_age = (
        max(0.0, now - component.last_poll_s)
        if component.last_poll_s
        else None
    )
    fresh = bool(
        poll_age is not None
        and poll_age <= component.telemetry_stale_seconds
    )
    services_ok = generator_ok and tunnel_ok and fresh
    isolation_ok = bool(
        component.wallet_absent_attested
        and component.submit_authority is False
    )
    evidence_complete = (
        component.evidence_valid_windows
        >= component.evidence_target_windows
    )
    evidence_ok = bool(
        component.evidence_source_ok and component.evidence_fresh
    )
    attached = bool(binding["attached"])
    controller_mode = str(
        getattr(controller, "standalone_controller_mode", "") or ""
    )
    generation: dict[str, object] | None = None
    if attached and controller is not None:
        try:
            component_gpu = _canonical_physical_gpu_id(
                component.gpu_uuid
            )
            bound_gpu = _canonical_physical_gpu_id(
                binding.get("component_id")
            )
        except ValueError:
            component_gpu = ""
            bound_gpu = ""
        per_gpu = getattr(controller, "standalone_telemetry", {}).get(
            "per_gpu", {}
        )
        if (
            component_gpu
            and component_gpu == bound_gpu
            and isinstance(per_gpu, dict)
            and isinstance(per_gpu.get(component_gpu), dict)
        ):
            generation = dict(per_gpu[component_gpu])
    if (
        not services_ok
        or not component.identity_ok
        or not isolation_ok
        or not evidence_ok
    ):
        status = "component_failed"
    elif attached:
        status = (
            f"attached_{controller_mode}"
            if controller_mode in {"canary", "mine"}
            else "live_attached"
        )
    elif evidence_complete:
        status = "ready_unattached"
    else:
        status = "certifying"
    controller_hotkey = controller.hotkey if controller is not None else ""
    chain_entry = chain_entries.get(controller_hotkey)
    uid = (
        int(getattr(chain_entry, "uid", -1))
        if chain_entry is not None
        else None
    )
    evidence_age = (
        max(0.0, now - component.evidence_latest_at)
        if component.evidence_latest_at
        else None
    )
    return {
        "label": component.label,
        "host": _display_ssh_alias(component.alias),
        "role": f"wallet-free-{component.role}-component",
        "status": status,
        "ok": (
            services_ok
            and component.identity_ok
            and isolation_ok
            and evidence_ok
        ),
        "fresh": fresh,
        "age_s": poll_age,
        "services": {
            "generator": {
                "name": component.generator_unit,
                "active_state": component.generator_active_state,
                "sub_state": component.generator_sub_state,
                "enablement": component.generator_enablement,
                "pid": component.generator_pid,
                "restarts": component.generator_restarts,
                "ok": generator_ok,
            },
            "tunnel": {
                "name": component.tunnel_unit,
                "active_state": component.tunnel_active_state,
                "sub_state": component.tunnel_sub_state,
                "enablement": component.tunnel_enablement,
                "pid": component.tunnel_pid,
                "restarts": component.tunnel_restarts,
                "ok": tunnel_ok,
            },
        },
        "authority": {
            "wallet_absent_attested": component.wallet_absent_attested,
            "submit_authority": component.submit_authority,
            "owns_hotkey": False,
        },
        "gpu": {
            "uuid": component.gpu_uuid,
            "name": component.gpu_name,
            "compute_capability": component.compute_capability,
            "memory_used_mb": component.gpu_mem_mb,
            "memory_total_mb": component.gpu_total_mb,
            "utilization_pct": component.gpu_util,
            "power_w": component.gpu_power_w,
            "power_limit_w": component.gpu_power_limit_w,
        },
        "identity": {
            "ok": component.identity_ok,
            "component_role": component.component_role,
            "manifest_schema_version": component.manifest_schema_version,
            "manifest_sha256": component.manifest_sha256,
            "manifest_profile_sha256": component.manifest_profile_sha256,
            "miner_source_revision": component.miner_source_revision,
            "validator_source_revision": component.validator_source_revision,
            "checkpoint_n": component.checkpoint_n,
            "checkpoint_revision": component.checkpoint_revision,
            "model_repo": component.model_repo,
            "environment": component.environment,
            "runtime_profile_sha256": component.runtime_profile_sha256,
            "runtime_regime": component.runtime_regime,
            "profile_file_sha256": component.profile_file_sha256,
            "profile_source_revision": component.profile_source_revision,
            "profile_checkpoint_revision": (
                component.profile_checkpoint_revision
            ),
            "profile_gpu_uuid": component.profile_gpu_uuid,
        },
        "evidence": {
            "host": _display_ssh_alias(component.evidence_alias),
            "directory": component.evidence_dir,
            "source_ok": component.evidence_source_ok,
            "certificate_bound": component.evidence_certificate_bound,
            "fresh": component.evidence_fresh,
            "stale_after_seconds": component.evidence_stale_seconds,
            "manifest_sha256": component.evidence_manifest_sha256,
            "profile_sha256": component.evidence_profile_sha256,
            "valid_windows": component.evidence_valid_windows,
            "target_windows": component.evidence_target_windows,
            "complete_groups": component.evidence_complete_groups,
            "first_window": component.evidence_first_window,
            "last_window": component.evidence_last_window,
            "latest_at": component.evidence_latest_at,
            "age_s": evidence_age,
            "complete": evidence_complete,
            "error": component.evidence_error,
        },
        "controller_binding": binding,
        "worker": {
            "resolution_source": component.resolution_source,
            "state": component.worker_state,
            "progress_fresh": component.worker_progress_fresh,
            "progress_age_s": component.worker_progress_age_s,
            "boot_id": component.worker_boot_id,
            "engine_epoch": component.worker_engine_epoch,
            "recovery_count": component.worker_recovery_count,
            "active_job_id": component.active_job_id,
            "active_window_n": component.active_window_n,
            "active_started_at": component.active_started_at,
            "active_deadline_at": component.active_deadline_at,
            "last_completed_job_id": component.last_completed_job_id,
            "last_completed_window_n": component.last_completed_window_n,
            "last_completed_at": component.last_completed_at,
        },
        "generation": generation,
        # There is deliberately no component attempt/reward funnel. Every
        # rollout leaving this worker is deduplicated, signed and submitted by
        # the one controller below, whose standalone row is authoritative.
        "accounting": {
            "controller_label": component.controller_label,
            "hotkey": controller_hotkey,
            "uid": uid,
            "shared_controller_funnel": True,
            "component_attempts": None,
            "component_selected_slots": None,
            "component_rewarded_slots": None,
            "component_slots_per_gpu_hour": None,
            "generated_attempts": (
                int(generation["attempts"]) if generation else None
            ),
            "natural_complete_m8": (
                int(generation["natural_complete_m8"])
                if generation
                else None
            ),
            "generated_windows": (
                int(generation["generated_windows"])
                if generation
                else None
            ),
            "natural_complete_windows": (
                int(generation["natural_complete_windows"])
                if generation
                else None
            ),
            "physical_gpu_hours": (
                float(generation["physical_gpu_hours"])
                if generation
                else None
            ),
        },
        "probe_error": component.error,
    }


def render_components_html() -> str:
    with _lock:
        components = list(_components)
        boxes = list(_boxes)
        chain_entries = {
            str(getattr(entry, "hotkey", "") or ""): entry
            for entry in _chain.hotkeys
        }
    if not components:
        return ""
    rows: list[str] = []
    now = time.time()
    for component in components:
        payload = _component_payload(
            component,
            boxes=boxes,
            chain_entries=chain_entries,
            now=now,
        )
        status = str(payload["status"]).replace("_", " ").upper()
        tone = (
            "var(--green)"
            if payload["status"] in {
                "live_attached",
                "attached_canary",
                "attached_mine",
            }
            else (
                "var(--yellow)"
                if payload["status"] in {"certifying", "ready_unattached"}
                else "var(--red)"
            )
        )
        services = payload["services"]
        generator = services["generator"]
        tunnel = services["tunnel"]
        gpu = payload["gpu"]
        identity = payload["identity"]
        evidence = payload["evidence"]
        binding = payload["controller_binding"]
        accounting = payload["accounting"]
        generated = (
            f'{int(accounting["natural_complete_m8"])} natural M=8 / '
            f'{int(accounting["generated_attempts"])} generated · '
            f'{int(accounting["natural_complete_windows"])} windows · '
            f'{float(accounting["physical_gpu_hours"]):.3f} GPUh'
            if accounting["generated_attempts"] is not None
            else "component generation ledger unavailable"
        )
        pct = int(gpu["memory_used_mb"]) * 100 // max(
            int(gpu["memory_total_mb"]), 1
        )
        rows.append(f"""
<tr>
  <td><span class="lbl" style="color:{html.escape(component.color)}">{html.escape(component.label)}</span>
    <div class="dim small">{html.escape(str(payload["host"]))}</div>
  </td>
  <td class="mono" style="color:{tone}"><b>{html.escape(status)}</b>
    <div class="dim small">poll {float(payload["age_s"] or 0.0):.1f}s</div>
  </td>
  <td class="mono">{html.escape(str(generator["active_state"]))}/{html.escape(str(generator["sub_state"]))}
    <div class="dim small">pid {int(generator["pid"])} · r{int(generator["restarts"])} · {html.escape(str(generator["enablement"]))}</div>
  </td>
  <td class="mono">{html.escape(str(tunnel["active_state"]))}/{html.escape(str(tunnel["sub_state"]))}
    <div class="dim small">pid {int(tunnel["pid"])} · r{int(tunnel["restarts"])} · {html.escape(str(tunnel["enablement"]))}</div>
  </td>
  <td style="color:{'var(--green)' if payload["authority"]["wallet_absent_attested"] and not payload["authority"]["submit_authority"] else 'var(--red)'}">NO WALLET · NO SUBMIT
    <div class="dim small">controller owns signing + transport</div>
  </td>
  <td title="{html.escape(str(gpu["uuid"]))}"><span style="color:{color_for_pct(pct)}">{html.escape(str(gpu["name"]) or "GPU unavailable")}</span>
    <div class="dim small">CC {html.escape(str(gpu["compute_capability"]) or "—")} · {int(gpu["memory_used_mb"]) // 1024}/{int(gpu["memory_total_mb"]) // 1024} GB · {int(gpu["utilization_pct"])}% · {float(gpu["power_w"]):.0f}/{float(gpu["power_limit_w"]):.0f} W</div>
  </td>
  <td class="mono" title="manifest {html.escape(str(identity["manifest_sha256"]))} · profile {html.escape(str(identity["runtime_profile_sha256"]))}">miner {html.escape(str(identity["miner_source_revision"])[:10] or "—")}
    <div class="dim small">validator {html.escape(str(identity["validator_source_revision"])[:10] or "—")} · manifest v{int(identity["manifest_schema_version"])} · {html.escape(str(identity["component_role"]) or "—")}</div>
  </td>
  <td class="mono">c{int(identity["checkpoint_n"])} · {html.escape(str(identity["checkpoint_revision"])[:10] or "—")}
    <div class="dim small">{html.escape(str(identity["model_repo"]) or "—")} · profile {html.escape(str(identity["runtime_profile_sha256"])[:10] or "—")}</div>
  </td>
  <td class="mono" title="{html.escape(str(evidence["directory"]))} · manifest {html.escape(str(evidence["manifest_sha256"]))} · profile {html.escape(str(evidence["profile_sha256"]))}">{int(evidence["valid_windows"])}/{int(evidence["target_windows"])} windows · {int(evidence["complete_groups"])} groups
    <div class="dim small">w{int(evidence["first_window"])}–w{int(evidence["last_window"])} · completed {float(evidence["age_s"] or 0.0):.0f}s ago · {'fresh' if evidence["fresh"] else 'STALE'}</div>
    <div class="dim small">{html.escape(str(evidence["host"]))}</div>
  </td>
  <td class="mono" style="color:{'var(--green)' if binding["attached"] else 'var(--yellow)'}">{html.escape(str(binding["reason"]).replace("_", " "))}
    <div class="dim small">{html.escape(str(accounting["controller_label"]))} · UID {accounting["uid"] if accounting["uid"] is not None else "—"} · {html.escape(str(accounting["hotkey"])[:12] or "—")}…</div>
    <div class="dim small">{html.escape(generated)}</div>
    <div class="dim small">attempts / selected / rewards counted once on controller row</div>
  </td>
</tr>""")
    return f"""
<div class="panel">
  <h2>Live accelerator components <span class="dim small">wallet-free workers · controller-attributed accounting</span></h2>
  <table class="grid compact">
    <thead><tr>
      <th>component / host</th><th>stage</th><th>generator</th><th>tunnel</th>
      <th>authority</th><th>GPU</th><th>component / validator source</th>
      <th>checkpoint / profile</th><th>evidence progress</th>
      <th>controller binding / accounting</th>
    </tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</div>
"""


def render_labs_html() -> str:
    with _lock:
        labs = list(_labs)
        last = _last_poll_at
        poll_count = _poll_count
        validator_window = int(getattr(_vs, "window", 0) or 0)
    if not labs:
        return render_components_html()
    rows = []
    now = time.time()
    for lab in labs:
        status, status_color, status_detail = _lab_status(
            lab,
            validator_window=validator_window,
        )
        artifact_lifecycle = _lab_artifact_lifecycle(
            lab,
            validator_window=validator_window,
        )
        isolation_text, isolation_color, isolation_sub = _lab_isolation_status(lab)
        pct = lab.gpu_mem_mb * 100 // max(lab.gpu_total_mb, 1)
        gpu_label = lab.gpu_name or "GPU unavailable"
        gpu_sub = (
            f"CC {lab.compute_capability or '—'} · "
            f"{lab.gpu_mem_mb // 1024}/{lab.gpu_total_mb // 1024} GB · "
            f"{lab.gpu_util}%"
        )
        unit_sub = (
            f"{lab.unit_active_state}/{lab.unit_sub_state} · "
            f"pid {lab.active_pid} · r{lab.restart_count} · "
            f"{lab.unit_enablement}"
        )
        role_sub = "SUBMIT-DISABLED · NO WALLET"
        unit_label = lab.unit or "completed one-shot"
        identity_prefix = "release"
        identity_sub = lab.environment or "awaiting evidence"
        release_sub = "source —"
        evidence_extra = ""
        if lab.selector_artifact_manifest:
            role_sub = (
                "EXPIRED SHADOW · NOT ACTIVATION ELIGIBLE"
                if artifact_lifecycle["expired"] is True
                else "SHADOW-ONLY · NOT ACTIVATION ELIGIBLE"
            )
            identity_prefix = "artifact"
            release = lab.artifact_model_version[:18] or "—"
            source = lab.artifact_source_revision[:10] or "—"
            manifest_release = lab.miner_release[:10] or "—"
            evidence_release = lab.evidence_miner_release[:10] or "—"
            checkpoint = (
                f"c{lab.artifact_checkpoint_n} · "
                f"{lab.artifact_checkpoint_revision[:10]}"
                if lab.artifact_checkpoint_n
                or lab.artifact_checkpoint_revision
                else "—"
            )
            identity_title = (
                f"kind={lab.artifact_kind or 'unknown'} · "
                f"source={lab.artifact_source_revision or 'unknown'} · "
                f"checkpoint={lab.artifact_checkpoint_repo_id or 'unknown'}@"
                f"{lab.artifact_checkpoint_revision or 'unknown'}"
            )
            identity_sub = (
                f"{lab.artifact_status or 'invalid'} · gate "
                f"{'passed' if lab.artifact_activation_gate_passed else 'failed'}"
            )
            release_sub = (
                f"live {manifest_release} · evidence {evidence_release}"
                if _lab_is_running(lab)
                else f"manifest {manifest_release} · evidence {evidence_release}"
            )
        else:
            release = lab.miner_release[:10] or "—"
            source = lab.source_revision[:10] or "—"
            evidence_release = lab.evidence_miner_release[:10] or "—"
            if _lab_is_running(lab):
                identity_prefix = "live release"
            release_sub = f"source {source} · evidence {evidence_release}"
            checkpoint = (
                f"c{lab.checkpoint_n} · {lab.checkpoint_revision[:10]}"
                if lab.checkpoint_n or lab.checkpoint_revision
                else "—"
            )
            identity_title = (
                f"manifest_release={lab.miner_release or 'unknown'} · "
                f"evidence_release={lab.evidence_miner_release or 'unknown'} · "
                f"source={lab.source_revision or 'unknown'} · "
                f"checkpoint={lab.checkpoint_repo_id or 'unknown'}@"
                f"{lab.checkpoint_revision or 'unknown'}"
            )
        if lab.artifact_manifest_ok:
            evidence_color = (
                "var(--green)"
                if lab.artifact_activation_gate_passed
                else "var(--yellow)"
            )
            evidence_main = (
                f"{lab.artifact_train_local_rows} local · "
                f"{lab.artifact_train_population_rows:,} population · "
                f"{lab.artifact_holdout_rows} holdout"
            )
            value_lift = lab.artifact_heldout_value_lift
            value_lift_text = (
                f"{value_lift * 100:+.2f}%"
                if value_lift is not None
                else "—"
            )
            selection_brier = lab.artifact_selection_brier
            selection_brier_text = (
                f"{selection_brier:.4f}"
                if selection_brier is not None
                else "—"
            )
            if (
                lab.artifact_kind
                == "reliquary_math_selector_catboost_challenger"
                and lab.artifact_model_version == "catboost_v2_emitted_slots"
            ):
                emission_lift = lab.artifact_emitted_slot_lift
                emission_lift_text = (
                    f"{emission_lift:.3f}×"
                    if emission_lift is not None
                    else "—"
                )
                emission_mse = lab.artifact_emission_mse
                emission_base_mse = lab.artifact_emission_base_mse
                emission_mse_text = (
                    f"{emission_mse:.5f}" if emission_mse is not None else "—"
                )
                emission_base_mse_text = (
                    f"{emission_base_mse:.5f}"
                    if emission_base_mse is not None
                    else "—"
                )
                payout_profile = (
                    lab.artifact_payout_profile.replace("_", " ") or "—"
                )
                economic_target = (
                    lab.artifact_economic_target.replace("_", " ") or "—"
                )
                evidence_sub = (
                    f"emitted-slot lift={emission_lift_text} · "
                    f"payout={payout_profile}"
                )
                evidence_extra = (
                    f"emission MSE={emission_mse_text} · "
                    f"base={emission_base_mse_text} · target={economic_target} · "
                    "shadow promotion="
                    f"{'passed' if lab.artifact_shadow_promotion_gate_passed else 'failed'}"
                )
            elif lab.artifact_kind == "reliquary_code_selector_challenger":
                exact_k2_lift = lab.artifact_top_quintile_lift
                exact_k2_lift_text = (
                    f"{exact_k2_lift:.3f}×"
                    if exact_k2_lift is not None
                    else "—"
                )
                completion_brier = lab.artifact_completion_brier
                completion_brier_text = (
                    f"{completion_brier:.4f}"
                    if completion_brier is not None
                    else "—"
                )
                evidence_sub = (
                    f"exact-k2 top lift={exact_k2_lift_text} · "
                    f"value lift={value_lift_text}"
                )
                evidence_extra = (
                    f"completion Brier={completion_brier_text} · "
                    f"selection Brier={selection_brier_text}"
                )
            elif lab.artifact_gpu_rolling_lift_mean is not None:
                cpu_lift = lab.artifact_top_quintile_lift
                cpu_lift_text = (
                    f"{cpu_lift:.3f}×" if cpu_lift is not None else "—"
                )
                if lab.artifact_cpu_rolling_lift_mean is not None:
                    gpu_final_text = (
                        f"{lab.artifact_gpu_final_lift:.3f}×"
                        if lab.artifact_gpu_final_lift is not None
                        else "—"
                    )
                    evidence_sub = (
                        "GPU forward mean="
                        f"{lab.artifact_gpu_rolling_lift_mean:.3f}× · "
                        f"final={gpu_final_text}"
                    )
                    evidence_extra = (
                        "CPU forward mean="
                        f"{lab.artifact_cpu_rolling_lift_mean:.3f}× · "
                        f"final={cpu_lift_text} · value+="
                        f"{lab.artifact_cpu_positive_value_folds}/"
                        f"{lab.artifact_cpu_rolling_folds} · repro CPU/GPU="
                        f"{'yes' if lab.artifact_cpu_bitwise_reproducible else 'no'}/"
                        f"{'yes' if lab.artifact_gpu_bitwise_reproducible else 'no'}"
                    )
                else:
                    evidence_sub = (
                        f"GPU rolling={lab.artifact_gpu_rolling_lift_mean:.3f}× · "
                        f"value+={lab.artifact_gpu_positive_value_folds}/"
                        f"{lab.artifact_gpu_rolling_folds} · "
                        f"final CPU={cpu_lift_text} · value={value_lift_text}"
                    )
            else:
                evidence_sub = (
                    f"value lift={value_lift_text} · "
                    f"selection Brier={selection_brier_text}"
                )
            window_sub = (
                f"w{lab.artifact_window_start}–w{lab.artifact_window_end}"
            )
            if lab.artifact_expires_after_window is not None:
                window_sub += (
                    f" · expires after w{lab.artifact_expires_after_window}"
                )
            age_detail = f"digest {lab.artifact_digest[:10] or '—'}"
        elif lab.selector_artifact_manifest:
            evidence_color = "var(--red)"
            evidence_main = "selector artifact unavailable"
            evidence_sub = lab.artifact_error or "manifest validation failed"
            window_sub = "no valid artifact scope"
            age_detail = "checksum unavailable"
        elif lab.evidence_db_ok:
            evidence_color = "var(--green)" if not lab.evidence_error else "var(--yellow)"
            evidence_main = (
                f"{lab.attempts} attempts · {lab.complete_attempts} complete · "
                f"{lab.censored_attempts} censored"
            )
            evidence_sub = (
                f"deadline={lab.deadline_censored_attempts} · "
                f"token={lab.token_censored_attempts} · pending={lab.pending_attempts} · "
                f"errors={lab.error_attempts}"
            )
            window_sub = (
                f"w{lab.first_window_n}–w{lab.last_window_n}"
                if lab.first_window_n or lab.last_window_n
                else "no completed evidence window"
            )
            age_detail = (
                f"last {fmt_age(int(max(0.0, now - lab.last_attempt_at)))} ago"
                if lab.last_attempt_at else "last — ago"
            )
        else:
            evidence_color = "var(--red)"
            evidence_main = "evidence unavailable"
            evidence_sub = lab.evidence_error or "database not created"
            window_sub = "no completed evidence window"
            age_detail = "last — ago"
        if lab.artifact_activation_blocker:
            probe_error = "BLOCKED: " + lab.artifact_activation_blocker.replace(
                "_", " "
            )
        elif lab.artifact_blockers:
            probe_error = "BLOCKED: " + " · ".join(
                blocker.replace("_", " ") for blocker in lab.artifact_blockers
            )
        else:
            probe_error = lab.artifact_error or lab.error or "—"
        evidence_extra_html = (
            f"<div class='dim small'>{html.escape(evidence_extra)}</div>"
            if evidence_extra
            else ""
        )
        rows.append(f"""
<tr>
  <td><span class="lbl" style="color:{html.escape(lab.color)}">{html.escape(lab.label)}</span><div class="dim small">{html.escape(_display_ssh_alias(lab.alias))}</div></td>
  <td class="mono" style="color:{status_color}" title="{html.escape(status_detail)}"><b>{status}</b><div class="dim small">{html.escape(role_sub)}</div></td>
  <td class="mono" title="{html.escape(lab.unit or 'artifact-only')}">{html.escape(unit_label)}<div class="dim small">{html.escape(unit_sub)}</div></td>
  <td style="color:{isolation_color}">{html.escape(isolation_text)}<div class="dim small">{html.escape(isolation_sub)}</div></td>
  <td title="{html.escape(gpu_label)}"><span style="color:{color_for_pct(pct)}">{html.escape(gpu_label)}</span><div class="dim small">{html.escape(gpu_sub)}</div></td>
  <td class="mono" title="{html.escape(identity_title)}">{html.escape(identity_prefix)} {html.escape(release)}<div class="dim small">{html.escape(release_sub)}</div></td>
  <td class="mono" title="{html.escape(identity_title)}">{html.escape(checkpoint)}<div class="dim small">{html.escape(identity_sub)}</div></td>
  <td class="mono" style="color:{evidence_color}">{html.escape(evidence_main)}<div class="dim small">{html.escape(evidence_sub)}</div>{evidence_extra_html}</td>
  <td class="mono dim">{html.escape(window_sub)}<div class="dim small">{html.escape(age_detail)}</div></td>
  <td class="mono dim">{html.escape(probe_error)}</td>
</tr>
""")
    age = int(max(0.0, now - last)) if last else 0
    return render_components_html() + f"""
<div class="panel">
  <h2>Accelerator labs — submit-disabled <span class="dim">polled {age}s ago · #{poll_count}</span></h2>
  <div class="small" style="margin-bottom:6px;color:var(--yellow)">OFFLINE ONLY · NO WALLET · EXCLUDED FROM LIVE MINER ACCOUNTING</div>
  <table class="grid">
    <thead><tr>
      <th>lab / host</th><th>role</th><th>service</th><th>isolation</th>
      <th>GPU</th><th>release / source</th><th>checkpoint</th>
      <th>offline evidence</th><th>window / age</th><th>probe</th>
    </tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</div>
"""


def _mission_current_events() -> list:
    """Events since the latest validator seal, oldest → newest."""
    events = validator_events_snapshot(6000)
    start = 0
    for i, ev in enumerate(events):
        if ev.kind == "seal":
            start = i + 1
    return events[start:]


def _short_hotkey(hk: str, chars: int = 12) -> str:
    return (hk[:chars] + "…") if hk else "—"


def _display_ssh_alias(alias: str) -> str:
    """Human label for an SSH target that may include option flags."""
    try:
        parts = shlex.split(alias)
    except ValueError:
        parts = alias.split()
    return parts[-1] if parts else alias


def _revisions_match(left: str, right: str) -> bool:
    """Compare full or safely abbreviated immutable revision identifiers."""
    left = str(left or "").strip().lower()
    right = str(right or "").strip().lower()
    if not left or not right:
        return False
    if left == right:
        return True
    return min(len(left), len(right)) >= 8 and (
        left.startswith(right) or right.startswith(left)
    )


_EXACT_GIT_REVISION_RE = re.compile(r"^[0-9a-f]{40}$", re.IGNORECASE)


def _explicit_validator_model_identity(
    payload: object,
) -> dict[str, object] | None:
    """Parse one endpoint's atomic checkpoint tuple when it publishes one."""
    if not isinstance(payload, dict):
        return None
    keys = ("checkpoint_n", "checkpoint_repo_id", "checkpoint_revision")
    if not all(key in payload for key in keys):
        return None
    try:
        checkpoint_n = int(payload.get("checkpoint_n") or 0)
    except (TypeError, ValueError):
        checkpoint_n = -1
    repo = str(payload.get("checkpoint_repo_id") or "").strip()
    revision = str(payload.get("checkpoint_revision") or "").strip()
    if checkpoint_n == 0 and not repo and not revision:
        kind = "clean_base"
        known = True
    elif (
        checkpoint_n > 0
        and repo
        and not any(char.isspace() for char in repo)
        and _EXACT_GIT_REVISION_RE.fullmatch(revision)
    ):
        kind = "validator_checkpoint"
        known = True
    else:
        kind = "unknown"
        known = False
    return {
        "known": known,
        "kind": kind,
        "checkpoint_n": checkpoint_n if checkpoint_n >= 0 else None,
        "repo": repo,
        "revision": revision,
    }


def _same_validator_model_identity(
    left: dict[str, object], right: dict[str, object]
) -> bool:
    return bool(
        left.get("known")
        and right.get("known")
        and left.get("kind") == right.get("kind")
        and left.get("checkpoint_n") == right.get("checkpoint_n")
        and str(left.get("repo") or "").casefold()
        == str(right.get("repo") or "").casefold()
        and str(left.get("revision") or "").casefold()
        == str(right.get("revision") or "").casefold()
    )


def _validator_model_identity(
    validator_state: ValidatorState | None,
) -> dict[str, object]:
    """Return only an authoritative live checkpoint/base-model identity.

    A blank default ``ValidatorState`` is not evidence of a clean-base reset.
    The latter is recognized only when a live payload explicitly carries
    ``checkpoint_n=0`` and null/blank repository and revision fields. This
    distinction prevents an initializing or temporarily sparse validator from
    being painted as model-compatible.
    """
    if validator_state is None:
        return {
            "known": False,
            "kind": "unknown",
            "checkpoint_n": None,
            "repo": "",
            "revision": "",
            "source": "",
        }

    try:
        checkpoint_n = int(getattr(validator_state, "checkpoint_n", 0) or 0)
    except (TypeError, ValueError):
        checkpoint_n = -1
    repo = str(getattr(validator_state, "checkpoint_repo_id", "") or "").strip()
    revision = str(
        getattr(validator_state, "checkpoint_revision", "") or ""
    ).strip()
    state_raw = getattr(validator_state, "state_raw", {})
    health_raw = getattr(validator_state, "health_raw", {})
    state_identity = _explicit_validator_model_identity(state_raw)
    health_identity = _explicit_validator_model_identity(health_raw)

    # `/health` is fetched after `/state`. If a future health schema publishes
    # the complete tuple, it can authoritatively advance or reset the identity
    # sampled just before it. Keep provenance honest when the two samples span
    # a transition instead of claiming both endpoints agreed.
    if health_identity is not None:
        if not health_identity["known"]:
            return {**health_identity, "source": ""}
        source = (
            "health/state"
            if state_identity is not None
            and _same_validator_model_identity(state_identity, health_identity)
            else "health"
        )
        return {**health_identity, "source": source}

    if state_identity is not None:
        if not state_identity["known"]:
            return {**state_identity, "source": ""}
        # The current health schema publishes repo/revision but not
        # checkpoint_n. It may corroborate an atomic `/state` tuple, but it
        # must never supply a new repo/revision that gets paired with the old
        # state checkpoint number during a transition.
        if isinstance(health_raw, dict) and any(
            key in health_raw
            for key in ("checkpoint_repo_id", "checkpoint_revision")
        ):
            if not all(
                key in health_raw
                for key in ("checkpoint_repo_id", "checkpoint_revision")
            ):
                return {
                    "known": False,
                    "kind": "unknown",
                    "checkpoint_n": None,
                    "repo": repo,
                    "revision": revision,
                    "source": "",
                }
            health_repo = str(
                health_raw.get("checkpoint_repo_id") or ""
            ).strip()
            health_revision = str(
                health_raw.get("checkpoint_revision") or ""
            ).strip()
            if (
                health_repo.casefold()
                != str(state_identity["repo"]).casefold()
                or health_revision.casefold()
                != str(state_identity["revision"]).casefold()
            ):
                return {
                    "known": False,
                    "kind": "unknown",
                    "checkpoint_n": None,
                    "repo": health_repo,
                    "revision": health_revision,
                    "source": "",
                }
            return {**state_identity, "source": "health/state"}
        return {**state_identity, "source": "state"}

    # Compatibility for normalized test/legacy state that predates raw-payload
    # retention. A live health repo/revision pair cannot bind to this fallback
    # checkpoint number because it was not published atomically with it.
    if isinstance(health_raw, dict) and any(
        key in health_raw for key in ("checkpoint_repo_id", "checkpoint_revision")
    ):
        return {
            "known": False,
            "kind": "unknown",
            "checkpoint_n": None,
            "repo": repo,
            "revision": revision,
            "source": "",
        }
    if (
        checkpoint_n > 0
        and repo
        and not any(char.isspace() for char in repo)
        and _EXACT_GIT_REVISION_RE.fullmatch(revision)
    ):
        return {
            "known": True,
            "kind": "validator_checkpoint",
            "checkpoint_n": checkpoint_n,
            "repo": repo,
            "revision": revision,
            "source": "state",
        }

    return {
        "known": False,
        "kind": "unknown",
        "checkpoint_n": None,
        "repo": repo,
        "revision": revision,
        "source": "",
    }


def _provisioned_model_identity(box: BoxState) -> dict[str, object]:
    """Normalize and validate the atomically published source manifest."""
    kind = str(getattr(box, "provisioned_model_kind", "") or "").strip()
    try:
        checkpoint_n = int(getattr(box, "provisioned_checkpoint_n", -1))
    except (TypeError, ValueError):
        checkpoint_n = -1
    repo = str(getattr(box, "provisioned_model_repo", "") or "").strip()
    revision = str(
        getattr(box, "provisioned_model_revision", "") or ""
    ).strip()
    provisioned_ok = bool(
        getattr(box, "source_manifest_provisioned_ok", False)
    )
    complete = bool(
        provisioned_ok
        and kind
        and checkpoint_n >= 0
        and repo
        and revision
    )
    valid = bool(
        complete
        and kind in {"validator_checkpoint", "clean_base"}
        and (
            (kind == "validator_checkpoint" and checkpoint_n > 0)
            or (kind == "clean_base" and checkpoint_n == 0)
        )
        and _EXACT_GIT_REVISION_RE.fullmatch(revision)
        and not any(char.isspace() for char in repo)
    )
    return {
        "provisioned_ok": provisioned_ok,
        "complete": complete,
        "valid": valid,
        "kind": kind,
        "checkpoint_n": checkpoint_n if checkpoint_n >= 0 else None,
        "repo": repo,
        "revision": revision,
    }


def _runtime_model_identity(box: BoxState) -> dict[str, object]:
    """Normalize checkpoint evidence bound to the currently active process."""
    try:
        checkpoint_n = int(getattr(box, "runtime_checkpoint_n", 0) or 0)
        active_pid = int(getattr(box, "active_pid", 0) or 0)
        evidence_pid = int(getattr(box, "runtime_checkpoint_pid", 0) or 0)
        active_started_at = int(getattr(box, "active_started_at", 0) or 0)
        evidence_started_at = int(
            getattr(box, "runtime_checkpoint_started_at", 0) or 0
        )
    except (TypeError, ValueError):
        checkpoint_n = active_pid = evidence_pid = 0
        active_started_at = evidence_started_at = 0
    repo = str(getattr(box, "runtime_checkpoint_repo", "") or "").strip()
    revision = str(
        getattr(box, "runtime_checkpoint_revision", "") or ""
    ).strip()
    evidence = str(
        getattr(box, "runtime_checkpoint_evidence", "") or ""
    ).strip()
    process_bound = bool(
        getattr(box, "proc_alive", False)
        and getattr(box, "runtime_checkpoint_loaded", False)
        and active_pid > 0
        and evidence_pid == active_pid
        and active_started_at > 0
        and evidence_started_at == active_started_at
    )
    valid = bool(
        process_bound
        and checkpoint_n > 0
        and repo
        and not any(char.isspace() for char in repo)
        and _EXACT_GIT_REVISION_RE.fullmatch(revision)
        and (
            evidence.startswith(("loaded+", "generation+"))
            or evidence == "readiness+math_auction_readiness"
        )
    )
    return {
        "known": valid,
        "valid": valid,
        "kind": "validator_checkpoint" if valid else "unknown",
        "checkpoint_n": checkpoint_n if checkpoint_n > 0 else None,
        "repo": repo,
        "revision": revision,
        "source": "runtime_journal" if valid else "",
        "process_bound": process_bound,
        "pid": evidence_pid if evidence_pid > 0 else None,
        "process_started_at": (
            evidence_started_at if evidence_started_at > 0 else None
        ),
        "evidence": evidence,
    }


def _active_model_identity(box: BoxState) -> dict[str, object]:
    """Prefer independently attested runtime weights over install baseline."""
    runtime = _runtime_model_identity(box)
    if runtime["valid"]:
        return runtime
    provisioned = _provisioned_model_identity(box)
    return {
        **provisioned,
        "known": bool(provisioned["valid"]),
        "source": "provisioned_manifest" if provisioned["valid"] else "",
    }


def _box_model_readiness_issues(
    box: BoxState,
    validator_state: ValidatorState | None,
) -> list[str]:
    """Compare provisioned identity with live validator truth.

    The provisioning manifest remains mandatory as the durable install/source
    baseline. A newer runtime identity may supersede its model tuple only when
    the active process independently attests an exact repo/revision, successful
    load or live generation, and structured checkpoint number. Validator
    telemetry is comparison truth only; it is never copied into miner runtime
    identity.
    """
    issues: list[str] = []
    provisioned = _provisioned_model_identity(box)
    if not provisioned["complete"]:
        return ["model_manifest_missing"]
    if not provisioned["valid"]:
        return ["model_manifest_invalid"]

    kind = str(provisioned["kind"])
    if kind == "clean_base":
        base_repo = str(getattr(box, "base_model_repo", "") or "").strip()
        base_revision = str(
            getattr(box, "base_model_revision", "") or ""
        ).strip()
        if not base_repo or not _EXACT_GIT_REVISION_RE.fullmatch(base_revision):
            issues.append("base_model_pin_missing")
        else:
            if base_repo.casefold() != str(provisioned["repo"]).casefold():
                issues.append("base_model_repo_mismatch")
            if base_revision.casefold() != str(provisioned["revision"]).casefold():
                issues.append("base_model_revision_mismatch")

    expected = _validator_model_identity(validator_state)
    if not expected["known"]:
        issues.append("validator_model_identity_pending")
        return issues
    active = _active_model_identity(box)
    if active["kind"] != expected["kind"]:
        issues.append("model_kind_mismatch")
    if active["checkpoint_n"] != expected["checkpoint_n"]:
        issues.append("model_checkpoint_n_mismatch")
    if expected["kind"] == "validator_checkpoint":
        if str(active["repo"]).casefold() != str(expected["repo"]).casefold():
            issues.append("model_repo_mismatch")
        if str(active["revision"]).casefold() != str(
            expected["revision"]
        ).casefold():
            issues.append("model_revision_mismatch")
    return issues


def _box_model_details(
    box: BoxState,
    validator_state: ValidatorState | None,
) -> dict[str, object]:
    """JSON-safe exact-model diagnostics shared by health and export."""
    applicable = getattr(box, "engine_mode", "") == "reference"
    issues = (
        _box_model_readiness_issues(box, validator_state)
        if applicable
        else []
    )
    validator = _validator_model_identity(validator_state)
    runtime = _runtime_model_identity(box)
    active = _active_model_identity(box)
    runtime_exact = bool(
        runtime["valid"]
        and validator["known"]
        and _same_validator_model_identity(runtime, validator)
    )
    return {
        "applicable": applicable,
        "ok": not issues if applicable else None,
        "issues": issues,
        "provisioned": _provisioned_model_identity(box),
        "runtime": {**runtime, "exact_validator_identity": runtime_exact},
        "active": active,
        "base_pin": {
            "repo": str(getattr(box, "base_model_repo", "") or ""),
            "revision": str(getattr(box, "base_model_revision", "") or ""),
        },
        "validator": validator,
        "observed_journal": {
            "checkpoint_n": int(getattr(box, "local_checkpoint_n", 0) or 0),
            "revision": str(
                getattr(box, "local_checkpoint_revision", "") or ""
            ),
        },
    }


def _configured_code_lane_intent(box: BoxState) -> bool:
    """Return Code intent from this row's configured service, not live env.

    Observed environment and engine mode are readiness evidence and may drift;
    using them to decide applicability would turn exactly that drift into a
    fail-open non-Code row. Named Code units and their explicit env-file names
    are stable configuration inputs.
    """
    raw_specs = list(getattr(box, "unit_candidates", ()) or ())
    if not raw_specs and getattr(box, "unit", None):
        raw_specs = [(str(box.unit), str(getattr(box, "env_file", "") or ""))]
    active_unit = str(getattr(box, "active_unit", "") or "")
    active_canonical = (
        active_unit if active_unit.endswith(".service") else
        f"{active_unit}.service" if active_unit else ""
    )
    selected: list[tuple[str, str]] = []
    for unit, env_file in raw_specs:
        unit_text = str(unit or "")
        canonical = (
            unit_text if unit_text.endswith(".service")
            else f"{unit_text}.service"
        )
        if not active_canonical or canonical == active_canonical:
            selected.append((canonical, str(env_file or "")))
    if not selected and active_canonical:
        selected = [(active_canonical, "")]
    marker = re.compile(r"(?:^|[-_.@/])code(?:[-_.@/]|$)", re.IGNORECASE)
    return any(marker.search(unit) or marker.search(env) for unit, env in selected)


def _box_code_auction_applicable(box: BoxState) -> bool:
    return bool(
        getattr(box, "proc_alive", False)
        and _configured_code_lane_intent(box)
    )


def _code_validator_health_details(
    validator_state: ValidatorState | None,
    *,
    now: float | None = None,
    stale_after_s: float | None = None,
) -> dict[str, object]:
    """Return freshness-bound health evidence for Code source/runtime gates."""
    current = time.time() if now is None else float(now)
    if stale_after_s is None:
        try:
            from settings import SETTINGS as _SETTINGS

            stale_after_s = float(_SETTINGS.health_poll_stale_seconds)
        except Exception:
            stale_after_s = 60.0
    if not math.isfinite(stale_after_s) or stale_after_s <= 0:
        stale_after_s = 60.0
    fetched_at = float(
        getattr(validator_state, "health_last_fetch_at", 0.0) or 0.0
    ) if validator_state is not None else 0.0
    age_s = max(0.0, current - fetched_at) if fetched_at else None
    status = str(
        getattr(validator_state, "health_status", "") or ""
    ) if validator_state is not None else ""
    error = str(
        getattr(validator_state, "health_error", "") or ""
    ) if validator_state is not None else ""
    response_fresh = bool(
        fetched_at
        and age_s is not None
        and age_s <= stale_after_s
        and not error
    )
    fresh = bool(response_fresh and status == "ok")
    return {
        "fresh": fresh,
        # A successful, recent health response remains authoritative for
        # immutable runtime identity even when the validator reports its
        # operational status as degraded.  Keep that distinct from ``fresh``
        # above: degraded health must still block readiness.
        "response_fresh": response_fresh,
        "status": status,
        "error": error,
        "last_fetch_at": fetched_at,
        "age_s": age_s,
        "stale_after_s": stale_after_s,
    }


def _validator_badge_health_details(
    validator_state: ValidatorState | None,
    *,
    poll_at: float = 0.0,
    now: float | None = None,
    stale_after_s: float | None = None,
) -> dict[str, object]:
    """Return display-only aggregate health for validator status badges.

    The detailed and rundown panels previously rendered the last successful
    ``/health`` status verbatim.  That made a cached ``ok`` look live while
    the poller was reporting endpoint errors or had stopped receiving fresh
    state.  Badge health therefore requires a fresh poll generation plus
    fresh ``/state``, ``/health`` and ``/verdicts`` observations.

    This helper is deliberately separate from immutable runtime identity.
    In particular, a recent successful ``/health`` response whose reported
    status is ``degraded`` remains authoritative for source/profile identity
    through :func:`_code_validator_health_details`; it simply cannot render an
    operationally healthy badge.
    """
    current = time.time() if now is None else float(now)
    if stale_after_s is None:
        try:
            from settings import SETTINGS as _SETTINGS

            stale_after_s = float(_SETTINGS.health_poll_stale_seconds)
        except Exception:
            stale_after_s = 60.0
    if not math.isfinite(stale_after_s) or stale_after_s <= 0:
        stale_after_s = 60.0

    if validator_state is None:
        return {
            "ok": False,
            "label": "warming",
            "tone": "warning",
            "issues": ["validator_state_missing"],
        }

    surfaces = {
        "poll": (float(poll_at or 0.0), ""),
        "state": (
            float(getattr(validator_state, "last_fetch_at", 0.0) or 0.0),
            str(getattr(validator_state, "error", "") or ""),
        ),
        "health": (
            float(
                getattr(validator_state, "health_last_fetch_at", 0.0) or 0.0
            ),
            str(getattr(validator_state, "health_error", "") or ""),
        ),
        "verdicts": (
            float(
                getattr(validator_state, "verdicts_last_fetch_at", 0.0) or 0.0
            ),
            str(getattr(validator_state, "verdicts_error", "") or ""),
        ),
    }
    missing = [name for name, (fetched_at, _error) in surfaces.items() if not fetched_at]
    stale = [
        name
        for name, (fetched_at, _error) in surfaces.items()
        if fetched_at and max(0.0, current - fetched_at) > stale_after_s
    ]
    errors = [
        name
        for name, (_fetched_at, error) in surfaces.items()
        if error and not (name == "state" and error == "ssh-state")
    ]
    fallbacks = [
        "state" if str(getattr(validator_state, "error", "") or "") == "ssh-state" else ""
    ]
    fallbacks = [name for name in fallbacks if name]
    reported = str(
        getattr(validator_state, "health_status", "") or ""
    ).strip().casefold()

    if errors:
        label, tone = "endpoint error", "error"
    elif stale:
        label, tone = "stale", "warning"
    elif missing:
        label, tone = "warming", "warning"
    elif fallbacks or reported != "ok":
        label, tone = "degraded", "warning"
    else:
        label, tone = "ok", "ok"

    issues = [
        *(f"{name}_error" for name in errors),
        *(f"{name}_stale" for name in stale),
        *(f"{name}_missing" for name in missing),
        *(f"{name}_fallback" for name in fallbacks),
    ]
    if reported and reported != "ok":
        issues.append(f"health_status_{reported}")
    return {
        "ok": label == "ok",
        "label": label,
        "tone": tone,
        "issues": issues,
    }


def _box_code_auction_details(
    box: BoxState,
    validator_state: ValidatorState | None,
    *,
    now: float | None = None,
    health_stale_s: float | None = None,
) -> dict[str, object]:
    """Derive fail-closed Code readiness from one atomic, non-secret probe."""
    if getattr(box, "miner_kind", "") == "reliquary_one":
        # V5 exposes a separate host-local event/ledger contract.  Running
        # the retired reference Code readiness gates against that service
        # would turn an unsupported probe into a false "auction blocked".
        return {
            "applicable": False,
            "ok": None,
            "issues": [],
            "v5_host_telemetry": _box_v5_host_telemetry(box),
        }
    applicable = _box_code_auction_applicable(box)
    if not applicable:
        return {"applicable": False, "ok": None, "issues": []}
    if getattr(box, "standalone_telemetry_path", ""):
        issues = _standalone_readiness_issues(
            box, validator_state=validator_state
        )
        telemetry = getattr(box, "standalone_telemetry", {})
        telemetry = telemetry if isinstance(telemetry, dict) else {}
        funnel = telemetry.get("funnel")
        funnel = funnel if isinstance(funnel, dict) else {}
        manifest_profile = telemetry.get("manifest_profile")
        manifest_profile = (
            manifest_profile if isinstance(manifest_profile, dict) else {}
        )
        return {
            "applicable": True,
            "ok": not issues,
            "issues": issues,
            "prescreen_enabled": True,
            "auction_policy": "standalone_atomic",
            "protocol_version": manifest_profile.get("protocol_version"),
            "ledger": {
                "current_partition": bool(funnel),
                "generation_outcomes": {
                    "source": "standalone_atomic",
                    "current": {
                        "attempts": int(funnel.get("attempts") or 0),
                        "generation_complete": int(
                            funnel.get("generation_complete") or 0
                        ),
                        "natural_complete_m8": int(
                            funnel.get("natural_complete_m8") or 0
                        ),
                        "local_eligible": int(
                            funnel.get("local_eligible") or 0
                        ),
                    },
                },
                "terminal_funnel": {
                    "source": "standalone_atomic",
                    "current": {
                        "attempts": int(funnel.get("attempts") or 0),
                        "precommit_accepted": int(
                            funnel.get("precommit_accepted") or 0
                        ),
                        "reveal_accepted": int(
                            funnel.get("reveal_accepted") or 0
                        ),
                        "pool_accepted": int(
                            funnel.get("pool_accepted") or 0
                        ),
                        "terminal_final": int(
                            funnel.get("terminal_final") or 0
                        ),
                        "terminal_unresolved": int(
                            funnel.get("terminal_unresolved") or 0
                        ),
                        "selected": int(funnel.get("selected") or 0),
                        "rewarded": int(funnel.get("rewarded") or 0),
                    }
                },
            },
            "grader": {"metrics_ok": True, "authority": "atomic_snapshot"},
        }

    issues: list[str] = []
    probe = getattr(box, "code_auction_probe", {})
    probe = probe if isinstance(probe, dict) else {}
    if int(probe.get("schema_version", 0) or 0) != 1:
        issues.append("code_auction:probe_missing")
    invocation_id = str(probe.get("invocation_id") or "").lower()
    if re.fullmatch(r"[0-9a-f]{32}", invocation_id) is None:
        issues.append("code_auction:invocation_id_missing")

    observed_environment = str(
        getattr(box, "active_environment", "")
        or getattr(box, "miner_environment", "")
        or ""
    ).strip().lower()
    probe_environment = str(probe.get("environment") or "").strip().lower()
    observed_engine_mode = str(
        getattr(box, "engine_mode", "") or ""
    ).strip().lower()
    probe_engine_mode = str(probe.get("engine_mode") or "").strip().lower()
    if (
        observed_environment != "opencodeinstruct"
        or probe_environment != "opencodeinstruct"
    ):
        issues.append("code_auction:environment_mismatch")
    if observed_engine_mode != "reference" or probe_engine_mode != "reference":
        issues.append("code_auction:engine_mode_mismatch")

    active_unit = str(getattr(box, "active_unit", "") or "")
    active_lane = str(getattr(box, "active_lane", "") or "")
    probe_unit = str(probe.get("miner_unit") or "")
    probe_pid = int(probe.get("process_pid", 0) or 0)
    active_pid = int(getattr(box, "active_pid", 0) or 0)
    unit_enablement = str(
        probe.get("miner_unit_enablement")
        or getattr(box, "miner_unit_enablement", "")
        or ""
    )
    if probe_unit != active_unit or probe_pid != active_pid or active_pid <= 0:
        issues.append("code_auction:process_identity_mismatch")
    if unit_enablement != "enabled":
        issues.append("code_auction:miner_unit_disabled")
    if not bool(probe.get("settings_process_bound")):
        issues.append("code_auction:settings_not_process_bound")
    if not bool(probe.get("prescreen_enabled")):
        issues.append("code_auction:prescreen_disabled")
    if str(probe.get("auction_policy") or "") != "deadline_aware":
        issues.append("code_auction:policy_mismatch")

    miner_source = str(getattr(box, "miner_source_revision", "") or "").lower()
    public_source = str(
        getattr(box, "reliquary_source_revision", "") or ""
    ).lower()
    validator_health = _code_validator_health_details(
        validator_state,
        now=now,
        stale_after_s=health_stale_s,
    )
    if validator_health["error"]:
        issues.append("code_auction:validator_health_error")
    if validator_health["status"] != "ok":
        issues.append("code_auction:validator_health_status")
    health_age = validator_health["age_s"]
    if (
        health_age is None
        or health_age > float(validator_health["stale_after_s"])
    ):
        issues.append("code_auction:validator_health_stale")
    retained_health_raw = (
        getattr(validator_state, "health_raw", {})
        if validator_state is not None
        else {}
    )
    health_raw = (
        retained_health_raw
        if validator_health["response_fresh"]
        and isinstance(retained_health_raw, dict)
        else {}
    )
    validator_source = str(health_raw.get("image_revision") or "").lower()
    source_manifest_valid = bool(
        getattr(box, "source_manifest_provisioned_ok", False)
        and _EXACT_GIT_REVISION_RE.fullmatch(miner_source)
        and _EXACT_GIT_REVISION_RE.fullmatch(public_source)
    )
    validator_source_valid = bool(
        _EXACT_GIT_REVISION_RE.fullmatch(validator_source)
    )
    if not source_manifest_valid:
        issues.append("code_auction:source_manifest_invalid")
    if not validator_source_valid:
        issues.append("code_auction:validator_source_pending")
    elif public_source != validator_source:
        issues.append("code_auction:validator_source_mismatch")

    validator_profile = ""
    runtime_fingerprint = health_raw.get("runtime_fingerprint") or {}
    if isinstance(runtime_fingerprint, dict):
        validator_profile = str(
            runtime_fingerprint.get("profile_hash") or ""
        ).lower()
    miner_profile = str(
        getattr(box, "runtime_profile_hash", "") or ""
    ).lower()
    exact_profile = re.compile(r"^[0-9a-f]{64}$")
    if not exact_profile.fullmatch(miner_profile) or not exact_profile.fullmatch(
        validator_profile
    ):
        issues.append("code_auction:runtime_profile_pending")
    elif miner_profile != validator_profile:
        issues.append("code_auction:runtime_profile_mismatch")

    attestation = probe.get("runtime_attestation")
    attestation = attestation if isinstance(attestation, dict) else {}
    attestation_ok = bool(
        int(attestation.get("schema_version", 0) or 0) == 1
        and attestation.get("prescreen_enabled") is True
        and str(attestation.get("auction_policy") or "") == "deadline_aware"
        and str(attestation.get("ledger_path") or "")
        == str(probe.get("ledger_path") or "")
        and attestation.get("ledger_available") is True
        and attestation.get("ledger_partition_registered") is True
        and int(attestation.get("ledger_schema_version", 0) or 0) == 1
        and int(attestation.get("process_pid", 0) or 0) == active_pid
        and str(attestation.get("miner_source_revision") or "").lower()
        == miner_source
        and str(attestation.get("public_source_revision") or "").lower()
        == public_source
        and str(attestation.get("runtime_profile_hash") or "").lower()
        == miner_profile
    )
    if not attestation_ok:
        issues.append("code_auction:runtime_attestation_missing")

    grader = probe.get("grader")
    grader = grader if isinstance(grader, dict) else {}
    grader_source = str(grader.get("bundle_source_revision") or "").lower()
    if str(grader.get("active_state") or "") != "active":
        issues.append("code_auction:grader_inactive")
    if str(grader.get("enablement") or "") != "enabled":
        issues.append("code_auction:grader_disabled")
    socket_ok = bool(
        grader.get("socket_ok")
        and int(grader.get("socket_uid", -1) or 0) == 0
        and str(grader.get("socket_mode") or "")
        == str(grader.get("expected_socket_mode") or "")
    )
    if not socket_ok:
        issues.append("code_auction:grader_socket_invalid")
    if not bool(grader.get("bundle_ok")):
        issues.append("code_auction:grader_bundle_invalid")
    if (
        not _EXACT_GIT_REVISION_RE.fullmatch(grader_source)
        or grader_source != public_source
        or (validator_source_valid and grader_source != validator_source)
    ):
        issues.append("code_auction:grader_source_mismatch")
    canary_ok = bool(
        grader.get("metrics_ok")
        and int(grader.get("canary_eval_ok_total", 0) or 0) >= 1
        and int(grader.get("canary_case_passed_total", 0) or 0) >= 1
    )
    if not canary_ok:
        issues.append("code_auction:grader_canary_missing")
    if (
        str(attestation.get("grader_socket") or "")
        != str(grader.get("socket_path") or "")
        or str(attestation.get("grader_source_revision") or "").lower()
        != grader_source
    ):
        issues.append("code_auction:grader_runtime_attestation_mismatch")

    ledger = probe.get("ledger")
    ledger = ledger if isinstance(ledger, dict) else {}
    ledger_path = str(probe.get("ledger_path") or "")
    if not ledger_path or not bool(ledger.get("configured")):
        issues.append("code_auction:ledger_not_configured")
    if not bool(ledger.get("readonly_ok")):
        issues.append("code_auction:ledger_unavailable")
    if not bool(ledger.get("schema_ok")):
        issues.append("code_auction:ledger_schema_invalid")

    validator_model = (
        _validator_model_identity(validator_state)
        if validator_health["response_fresh"]
        else {
            "known": False,
            "kind": "unknown",
            "checkpoint_n": None,
            "repo": "",
            "revision": "",
            "source": "",
        }
    )
    expected_partition = {
        "checkpoint_n": validator_model.get("checkpoint_n"),
        "model_repository": str(validator_model.get("repo") or ""),
        "checkpoint_revision": str(
            validator_model.get("revision") or ""
        ).lower(),
        "public_source_revision": validator_source,
        "runtime_profile_hash": validator_profile,
        "environment": "opencodeinstruct",
    }
    partition_identity_ready = bool(
        validator_model.get("known")
        and validator_source_valid
        and exact_profile.fullmatch(validator_profile)
    )
    current_partition: dict[str, object] | None = None
    partitions = ledger.get("partitions")
    if isinstance(partitions, list) and partition_identity_ready:
        for row in partitions:
            if not isinstance(row, dict):
                continue
            if (
                str(row.get("model_repository") or "").casefold()
                == expected_partition["model_repository"].casefold()
                and str(row.get("checkpoint_revision") or "").lower()
                == expected_partition["checkpoint_revision"]
                and int(row.get("checkpoint_n", -1))
                == expected_partition["checkpoint_n"]
                and str(row.get("public_source_revision") or "").lower()
                == expected_partition["public_source_revision"]
                and str(row.get("runtime_profile_hash") or "").lower()
                == expected_partition["runtime_profile_hash"]
                and str(row.get("environment") or "").lower()
                == expected_partition["environment"]
            ):
                current_partition = dict(row)
                break
    if not partition_identity_ready:
        issues.append("code_auction:ledger_partition_identity_pending")
    elif current_partition is None:
        # Ledger v1 has an explicit six-field registry. Never infer currentness
        # from attempt timestamps or a five-field near match: startup and each
        # dynamic checkpoint transition register the exact current tuple.
        issues.append("code_auction:ledger_current_partition_missing")

    generation_raw = ledger.get("generation_outcomes")
    generation_raw = generation_raw if isinstance(generation_raw, dict) else {}
    generation_summaries = generation_raw.get("summaries")
    generation_summaries = (
        generation_summaries if isinstance(generation_summaries, list) else []
    )
    process_lane = str(probe.get("lane") or "")
    lane_matches = bool(active_lane and process_lane == active_lane)
    current_generation: dict[str, object] | None = None

    def generation_int(
        row: dict, key: str, *, default: int = 0
    ) -> int:
        value = row.get(key, default)
        if isinstance(value, bool):
            return default
        try:
            parsed = int(value)
        except (TypeError, ValueError, OverflowError):
            return default
        return parsed if 0 <= parsed <= 2**63 - 1 else default

    def generation_float(row: dict, key: str) -> float:
        value = row.get(key, 0.0)
        if isinstance(value, bool):
            return 0.0
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return 0.0
        return parsed if math.isfinite(parsed) and parsed >= 0.0 else 0.0

    if (
        bool(generation_raw.get("schema_ok"))
        and partition_identity_ready
        and lane_matches
    ):
        for row in generation_summaries:
            if not isinstance(row, dict):
                continue
            if (
                str(row.get("model_repository") or "").casefold()
                != expected_partition["model_repository"].casefold()
                or str(row.get("checkpoint_revision") or "").lower()
                != expected_partition["checkpoint_revision"]
                or generation_int(row, "checkpoint_n", default=-1)
                != expected_partition["checkpoint_n"]
                or str(row.get("public_source_revision") or "").lower()
                != expected_partition["public_source_revision"]
                or str(row.get("runtime_profile_hash") or "").lower()
                != expected_partition["runtime_profile_hash"]
                or str(row.get("environment") or "").lower()
                != expected_partition["environment"]
                or str(row.get("lane") or "") != active_lane
            ):
                continue

            completed = generation_int(row, "natural_eos_complete")
            local_token_limit = generation_int(row, "local_token_limit")
            safe_deadline = generation_int(row, "safe_deadline")
            decisive_attempts = completed + local_token_limit
            current_generation = {
                "lane": active_lane,
                "first_window_n": generation_int(row, "first_window_n"),
                "last_window_n": generation_int(row, "last_window_n"),
                "last_observed_at": generation_float(row, "last_observed_at"),
                "attempts": completed + local_token_limit + safe_deadline,
                "decisive_attempts": decisive_attempts,
                "natural_eos_complete": completed,
                "local_token_limit": local_token_limit,
                "safe_deadline": safe_deadline,
                "natural_eos_before_bound_rate": (
                    completed / decisive_attempts
                    if decisive_attempts > 0
                    else None
                ),
            }
            break

    generation_outcomes = {
        "table_present": bool(generation_raw.get("table_present")),
        "schema_ok": bool(generation_raw.get("schema_ok")),
        "error": str(generation_raw.get("error") or ""),
        "observed_summary_count": len(generation_summaries),
        "expected_lane": active_lane,
        "process_lane": process_lane,
        "lane_matches": lane_matches,
        "current": current_generation,
    }

    terminal_raw = ledger.get("terminal_funnel")
    terminal_raw = terminal_raw if isinstance(terminal_raw, dict) else {}
    extension_present = bool(terminal_raw.get("extension_table_present"))
    terminal_schema_ok = bool(
        terminal_raw.get("schema_ok")
        and terminal_raw.get("terminal_events_table_present")
        and int(terminal_raw.get("api_version", 0) or 0) == 2
        and str(terminal_raw.get("storage_mode") or "")
        == "v1_additive_compat"
    )
    if extension_present and not terminal_schema_ok:
        issues.append("code_auction:terminal_funnel_incompatible")

    terminal_summaries = terminal_raw.get("summaries")
    terminal_summaries = (
        terminal_summaries if isinstance(terminal_summaries, list) else []
    )
    current_terminal: dict[str, int] | None = None

    def terminal_count(row: dict, key: str) -> int:
        value = row.get(key, 0)
        if isinstance(value, bool):
            return 0
        try:
            parsed = int(value)
        except (TypeError, ValueError, OverflowError):
            return 0
        return parsed if 0 <= parsed <= 2**63 - 1 else 0

    if terminal_schema_ok and partition_identity_ready:
        for row in terminal_summaries:
            if not isinstance(row, dict):
                continue
            if (
                str(row.get("model_repository") or "").casefold()
                != expected_partition["model_repository"].casefold()
                or str(row.get("checkpoint_revision") or "").lower()
                != expected_partition["checkpoint_revision"]
                or terminal_count(row, "checkpoint_n")
                != expected_partition["checkpoint_n"]
                or str(row.get("public_source_revision") or "").lower()
                != expected_partition["public_source_revision"]
                or str(row.get("runtime_profile_hash") or "").lower()
                != expected_partition["runtime_profile_hash"]
                or str(row.get("environment") or "").lower()
                != expected_partition["environment"]
            ):
                continue
            current_terminal = {
                name: terminal_count(row, name)
                for name in (
                    "attempts",
                    "http_provisional",
                    "receipt_reserved",
                    "reveal_sent",
                    "pool_accepted",
                    "selected",
                    "rewarded",
                    "terminal_rejected",
                    "terminal_unresolved",
                )
            }
            break
        if current_terminal is None:
            issues.append("code_auction:terminal_funnel_current_partition_missing")

    payout_profile = _auction_payout_profile(validator_source)
    terminal_counts_valid = True
    if current_terminal is not None:
        attempts = current_terminal["attempts"]
        payout_counts_valid = (
            current_terminal["selected"] <= current_terminal["rewarded"]
            if payout_profile == "boundary_fair_split"
            else current_terminal["rewarded"] <= current_terminal["selected"]
        )
        terminal_counts_valid = bool(
            all(
                current_terminal[name] <= attempts
                for name in current_terminal
                if name != "attempts"
            )
            and payout_counts_valid
            and current_terminal["rewarded"]
            <= current_terminal["pool_accepted"]
            and current_terminal["selected"]
            <= current_terminal["pool_accepted"]
        )
        if not terminal_counts_valid:
            issues.append("code_auction:terminal_funnel_counts_invalid")

    terminal_funnel = {
        "extension_table_present": extension_present,
        "legacy_compatible": not extension_present,
        "schema_ok": terminal_schema_ok,
        "api_version": int(terminal_raw.get("api_version", 0) or 0),
        "storage_mode": str(terminal_raw.get("storage_mode") or ""),
        "payout_profile": payout_profile,
        "error": str(terminal_raw.get("error") or ""),
        "observed_summary_count": len(terminal_summaries),
        "counts_valid": terminal_counts_valid,
        "current": current_terminal,
    }

    safe_ledger = {
        "path": ledger_path,
        "configured": bool(ledger.get("configured")),
        "readonly_ok": bool(ledger.get("readonly_ok")),
        "schema_ok": bool(ledger.get("schema_ok")),
        "application_id": int(ledger.get("application_id", 0) or 0),
        "user_version": int(ledger.get("user_version", 0) or 0),
        "quick_check": str(ledger.get("quick_check") or ""),
        "journal_mode": str(ledger.get("journal_mode") or ""),
        "error": str(ledger.get("error") or ""),
        "observed_partition_count": len(partitions)
        if isinstance(partitions, list)
        else 0,
        "expected_partition": expected_partition,
        "current_partition": current_partition,
        "generation_outcomes": generation_outcomes,
        "terminal_funnel": terminal_funnel,
    }
    return {
        "applicable": True,
        "ok": not issues,
        "issues": issues,
        "miner_unit": {
            "unit": active_unit,
            "enablement": unit_enablement,
            "process_pid": probe_pid,
            "invocation_id": invocation_id,
            "settings_process_bound": bool(
                probe.get("settings_process_bound")
            ),
        },
        "prescreen_enabled": bool(probe.get("prescreen_enabled")),
        "auction_policy": str(probe.get("auction_policy") or ""),
        "runtime_attestation": dict(attestation),
        "validator_health": validator_health,
        "source": {
            "provisioned_ok": bool(
                getattr(box, "source_manifest_provisioned_ok", False)
            ),
            "miner_revision": miner_source,
            "public_revision": public_source,
            "validator_revision": validator_source,
            "exact_match": bool(
                source_manifest_valid
                and validator_source_valid
                and public_source == validator_source
            ),
        },
        "runtime_profile": {
            "miner": miner_profile,
            "validator": validator_profile,
            "exact_match": bool(
                exact_profile.fullmatch(miner_profile)
                and exact_profile.fullmatch(validator_profile)
                and miner_profile == validator_profile
            ),
        },
        "grader": dict(grader),
        "ledger": safe_ledger,
    }


def _box_code_selector_crossover_details(
    box: BoxState,
    validator_state: ValidatorState | None,
    *,
    now: float | None = None,
    stale_after_s: float | None = None,
) -> dict[str, object]:
    """Project one validated persisted arm without relabeling stale history."""
    raw = getattr(box, "code_selector_crossover_probe", {})
    raw = raw if isinstance(raw, dict) else {}
    configured = bool(raw.get("configured"))
    latest: dict[str, object] | None = None
    state = raw.get("state")
    state = state if isinstance(state, dict) else {}
    latest_raw = state.get("latest_decision")
    if isinstance(latest_raw, dict):
        latest = dict(latest_raw)
    result: dict[str, object] = {
        "configured": configured,
        "ok": True if not configured else False,
        "issues": [],
        "process_unit": "",
        "process_pid": 0,
        "lane": "",
        "contract": {},
        "score_index": {},
        "latest_persisted": latest,
        "current": None,
        "current_status": "disabled" if not configured else "invalid",
    }
    if not configured:
        return result

    issues: list[str] = []
    if int(raw.get("schema_version", 0) or 0) != 1:
        issues.append("probe_missing")
    if raw.get("valid") is not True:
        issues.append(
            "artifact_invalid:"
            + str(raw.get("error") or "validation_failed")[:128]
        )
    if raw.get("settings_process_bound") is not True:
        issues.append("settings_not_process_bound")
    process_unit = str(raw.get("process_unit") or "")
    process_pid = int(raw.get("process_pid", 0) or 0)
    lane = str(raw.get("lane") or "")
    active_units = set(getattr(box, "active_units", []) or [])
    if not active_units and getattr(box, "active_unit", ""):
        active_units.add(str(getattr(box, "active_unit")))
    if process_pid <= 0 or process_unit not in active_units:
        issues.append("process_identity_mismatch")

    contract = raw.get("contract")
    contract = contract if isinstance(contract, dict) else {}
    score_index = raw.get("score_index")
    score_index = score_index if isinstance(score_index, dict) else {}
    if raw.get("valid") is True:
        active_model = _active_model_identity(box)
        identity_pairs = (
            (
                str(contract.get("public_source_revision") or ""),
                str(getattr(box, "reliquary_source_revision", "") or ""),
                "source_mismatch",
            ),
            (
                str(contract.get("miner_release_revision") or ""),
                str(getattr(box, "miner_source_revision", "") or ""),
                "miner_release_mismatch",
            ),
            (
                str(contract.get("runtime_profile_sha256") or ""),
                str(getattr(box, "runtime_profile_hash", "") or ""),
                "runtime_profile_mismatch",
            ),
            (
                str(contract.get("checkpoint_repository") or "").casefold(),
                str(active_model.get("repo") or "").casefold(),
                "checkpoint_repository_mismatch",
            ),
            (
                str(contract.get("checkpoint_revision") or ""),
                str(active_model.get("revision") or ""),
                "checkpoint_revision_mismatch",
            ),
        )
        for observed, expected, issue in identity_pairs:
            if not observed or not expected or observed != expected:
                issues.append(issue)
        try:
            contract_checkpoint_n = int(contract.get("checkpoint_n"))
            active_checkpoint_n = int(active_model.get("checkpoint_n"))
        except (TypeError, ValueError):
            issues.append("checkpoint_n_mismatch")
        else:
            if (
                not bool(active_model.get("valid"))
                or contract_checkpoint_n != active_checkpoint_n
            ):
                issues.append("checkpoint_n_mismatch")

    result.update(
        {
            "ok": not issues,
            "issues": issues,
            "process_unit": process_unit,
            "process_pid": process_pid,
            "lane": lane,
            "contract": dict(contract),
            "score_index": dict(score_index),
            "current_status": "pending",
        }
    )
    if issues:
        result["current_status"] = "invalid"
        return result

    current_time = time.time() if now is None else float(now)
    if stale_after_s is None:
        try:
            from settings import SETTINGS as runtime_settings

            configured_limit = float(
                getattr(
                    runtime_settings,
                    "health_poll_stale_seconds",
                    0.0,
                ) or 0.0
            )
            refresh = float(
                getattr(runtime_settings, "refresh_seconds", 5.0) or 5.0
            )
        except (ImportError, TypeError, ValueError):
            configured_limit = 0.0
            refresh = 5.0
        stale_after_s = max(15.0, refresh * 3.0, configured_limit)
    try:
        state_age = max(
            0.0,
            current_time
            - float(getattr(validator_state, "last_fetch_at", 0.0) or 0.0),
        )
        box_age = max(
            0.0,
            current_time - float(getattr(box, "last_poll_s", 0.0) or 0.0),
        )
    except (TypeError, ValueError, OverflowError):
        result["current_status"] = "validator_stale"
        return result
    direct_raw = (
        getattr(validator_state, "state_raw", {})
        if validator_state is not None
        else {}
    )
    direct_raw = direct_raw if isinstance(direct_raw, dict) else {}
    try:
        direct_window = int(
            direct_raw.get("window_n")
            or direct_raw.get("current_round")
            or 0
        )
        validator_window = int(
            getattr(validator_state, "window", 0) or 0
        )
    except (TypeError, ValueError, OverflowError):
        direct_window = validator_window = 0
    direct_state = str(direct_raw.get("state") or "").lower()
    observed_state = str(
        getattr(validator_state, "state", "") or ""
    ).lower()
    direct_fresh = bool(
        validator_state is not None
        and state_age <= float(stale_after_s)
        and box_age <= float(stale_after_s)
        and getattr(validator_state, "error", "") in {"", "ssh-state"}
        and direct_state == "open"
        and observed_state == "open"
        and direct_window > 0
        and direct_window == validator_window
    )
    if not direct_fresh:
        result["current_status"] = (
            "validator_not_open"
            if state_age <= float(stale_after_s)
            and box_age <= float(stale_after_s)
            and direct_state != "open"
            else "validator_stale"
        )
        return result
    try:
        start_window = int(contract.get("start_window"))
        end_window = int(contract.get("end_window"))
    except (TypeError, ValueError):
        result["current_status"] = "invalid"
        return result
    if not start_window <= validator_window <= end_window:
        result["current_status"] = "outside_contract"
        return result
    if (
        latest is None
        or int(latest.get("window_n", 0) or 0) != validator_window
    ):
        result["current_status"] = "decision_pending"
        return result
    result["current"] = {
        **latest,
        "validator_window": validator_window,
        "validator_state": "open",
    }
    result["current_status"] = "current"
    return result


def _box_code_overlap_abba_details(
    box: BoxState,
    validator_state: ValidatorState | None,
    *,
    now: float | None = None,
    stale_after_s: float | None = None,
) -> dict[str, object]:
    """Project the process-bound two-lane AB/BA contract and current arms."""
    raw = getattr(box, "code_overlap_abba_probe", {})
    raw = raw if isinstance(raw, dict) else {}
    configured = bool(raw.get("configured"))
    result: dict[str, object] = {
        "configured": configured,
        "ok": True if not configured else False,
        "issues": [],
        "contract": {},
        "lanes": [],
        "current": None,
        "current_status": "disabled" if not configured else "invalid",
    }
    if not configured:
        return result

    issues: list[str] = []
    if int(raw.get("schema_version", 0) or 0) != 1:
        issues.append("probe_missing")
    if raw.get("valid") is not True:
        issues.append(
            "artifact_invalid:"
            + str(raw.get("error") or "validation_failed")[:128]
        )
    if raw.get("settings_process_bound") is not True:
        issues.append("settings_not_process_bound")
    contract = raw.get("contract")
    contract = contract if isinstance(contract, dict) else {}
    lanes_raw = raw.get("lanes")
    lanes = [
        dict(lane)
        for lane in (lanes_raw if isinstance(lanes_raw, list) else [])
        if isinstance(lane, dict)
    ]

    status_by_unit = {
        str(row.get("unit") or ""): row
        for row in _coordinated_unit_status_payload(box)
    }
    active_units = set(getattr(box, "active_units", []) or [])
    for lane in lanes:
        unit = str(lane.get("process_unit") or "")
        pid = int(lane.get("process_pid", 0) or 0)
        status = status_by_unit.get(unit)
        if (
            unit not in active_units
            or status is None
            or str(status.get("active_state") or "") != "active"
            or int(status.get("pid", 0) or 0) != pid
        ):
            issues.append(
                "process_identity_mismatch:"
                + str(lane.get("shard_slot", "?"))
            )

    if raw.get("valid") is True:
        active_model = _active_model_identity(box)
        identity_pairs = (
            (
                str(contract.get("public_source_revision") or ""),
                str(getattr(box, "reliquary_source_revision", "") or ""),
                "source_mismatch",
            ),
            (
                str(contract.get("miner_release_revision") or ""),
                str(getattr(box, "miner_source_revision", "") or ""),
                "miner_release_mismatch",
            ),
            (
                str(contract.get("runtime_profile_sha256") or ""),
                str(getattr(box, "runtime_profile_hash", "") or ""),
                "runtime_profile_mismatch",
            ),
            (
                str(contract.get("checkpoint_repository") or "").casefold(),
                str(active_model.get("repo") or "").casefold(),
                "checkpoint_repository_mismatch",
            ),
            (
                str(contract.get("checkpoint_revision") or ""),
                str(active_model.get("revision") or ""),
                "checkpoint_revision_mismatch",
            ),
        )
        for observed, expected, issue in identity_pairs:
            if not observed or not expected or observed != expected:
                issues.append(issue)
        try:
            checkpoint_n = int(contract.get("checkpoint_n"))
            active_checkpoint_n = int(active_model.get("checkpoint_n"))
        except (TypeError, ValueError):
            issues.append("checkpoint_n_mismatch")
        else:
            if (
                not bool(active_model.get("valid"))
                or checkpoint_n != active_checkpoint_n
            ):
                issues.append("checkpoint_n_mismatch")

    result.update(
        {
            "ok": not issues,
            "issues": issues,
            "contract": dict(contract),
            "lanes": lanes,
            "current_status": "pending",
        }
    )
    if issues:
        result["current_status"] = "invalid"
        return result

    current_time = time.time() if now is None else float(now)
    if stale_after_s is None:
        try:
            from settings import SETTINGS as runtime_settings

            configured_limit = float(
                getattr(
                    runtime_settings,
                    "health_poll_stale_seconds",
                    0.0,
                ) or 0.0
            )
            refresh = float(
                getattr(runtime_settings, "refresh_seconds", 5.0) or 5.0
            )
        except (ImportError, TypeError, ValueError):
            configured_limit = 0.0
            refresh = 5.0
        stale_after_s = max(15.0, refresh * 3.0, configured_limit)
    try:
        state_age = max(
            0.0,
            current_time
            - float(getattr(validator_state, "last_fetch_at", 0.0) or 0.0),
        )
        box_age = max(
            0.0,
            current_time - float(getattr(box, "last_poll_s", 0.0) or 0.0),
        )
    except (TypeError, ValueError, OverflowError):
        result["current_status"] = "validator_stale"
        return result
    direct_raw = (
        getattr(validator_state, "state_raw", {})
        if validator_state is not None
        else {}
    )
    direct_raw = direct_raw if isinstance(direct_raw, dict) else {}
    try:
        direct_window = int(
            direct_raw.get("window_n")
            or direct_raw.get("current_round")
            or 0
        )
        validator_window = int(
            getattr(validator_state, "window", 0) or 0
        )
    except (TypeError, ValueError, OverflowError):
        direct_window = validator_window = 0
    direct_state = str(direct_raw.get("state") or "").lower()
    observed_state = str(
        getattr(validator_state, "state", "") or ""
    ).lower()
    fresh_open = bool(
        validator_state is not None
        and state_age <= float(stale_after_s)
        and box_age <= float(stale_after_s)
        and getattr(validator_state, "error", "") in {"", "ssh-state"}
        and direct_state == "open"
        and observed_state == "open"
        and direct_window > 0
        and direct_window == validator_window
    )
    if not fresh_open:
        result["current_status"] = (
            "validator_not_open"
            if state_age <= float(stale_after_s)
            and box_age <= float(stale_after_s)
            and direct_state != "open"
            else "validator_stale"
        )
        return result
    try:
        start_window = int(contract.get("start_window"))
        end_window = int(contract.get("end_window"))
    except (TypeError, ValueError):
        result["current_status"] = "invalid"
        return result
    if not start_window <= validator_window <= end_window:
        result["current_status"] = "outside_contract"
        return result
    if any(
        not isinstance(lane.get("latest_window"), dict)
        or int(lane["latest_window"].get("window_n", 0) or 0)
        != validator_window
        for lane in lanes
    ):
        result["current_status"] = "assignment_pending"
        return result
    result["current"] = [
        {
            "lane": str(lane.get("lane") or ""),
            "process_pid": int(lane.get("process_pid", 0) or 0),
            "process_unit": str(lane.get("process_unit") or ""),
            "shard_slot": int(lane.get("shard_slot", 0) or 0),
            "activation_count": int(
                lane["latest_window"].get("activation_count", 0) or 0
            ),
            "attempt_count": int(
                lane["latest_window"].get("attempt_count", 0) or 0
            ),
            "fallback_counts": dict(
                lane["latest_window"].get("fallback_counts") or {}
            ),
            "latest_attempt": dict(
                lane["latest_window"].get("latest_attempt") or {}
            ),
            "selected_overlap_count": int(
                lane["latest_window"].get(
                    "selected_overlap_count", 0
                ) or 0
            ),
            "window_n": int(
                lane["latest_window"].get("window_n", 0) or 0
            ),
        }
        for lane in lanes
    ]
    result["current_status"] = "current"
    return result


def _standalone_readiness_issues(
    box: BoxState,
    *,
    validator_state: ValidatorState | None,
) -> list[str]:
    if not getattr(box, "standalone_telemetry_path", ""):
        return []
    issues: list[str] = []
    error = str(getattr(box, "standalone_error", "") or "")
    if error:
        issues.append(f"standalone_telemetry:{error}")
    if not bool(getattr(box, "standalone_fresh", False)):
        issues.append("standalone_telemetry_stale")
    telemetry = getattr(box, "standalone_telemetry", {})
    if not isinstance(telemetry, dict) or not telemetry:
        issues.append("standalone_telemetry_missing")
    else:
        lifecycle = telemetry.get("window_lifecycle")
        if isinstance(lifecycle, dict):
            lifecycle_status = str(lifecycle.get("status") or "").upper()
            if lifecycle_status in {"RECOVERING", "STALLED"}:
                issues.append(
                    f"standalone_window_lifecycle:{lifecycle_status.lower()}"
                )
    if not bool(getattr(box, "runtime_parity_ok", False)):
        issues.append("standalone_runtime_identity_invalid")
    if not str(getattr(box, "operator", "") or ""):
        issues.append("standalone_operator_missing")

    if validator_state is not None:
        # Public source closure and the deployed validator image are separate
        # identities.  A private deployment commit must not make an otherwise
        # exact runtime look incompatible with its public protocol closure.
        # New standalone manifests attest the observed image explicitly;
        # retain the public-source fallback for older producers.
        observed_validator_image = str(
            getattr(box, "observed_validator_image_revision", "")
            or getattr(box, "reliquary_source_revision", "")
            or ""
        ).lower()
        validator_source = str(
            getattr(validator_state, "image_revision", "") or ""
        ).lower()
        if (
            validator_source
            and observed_validator_image != validator_source
        ):
            issues.append("standalone_validator_source_mismatch")
        expected = _validator_model_identity(validator_state)
        if expected.get("known"):
            if (
                int(getattr(box, "runtime_checkpoint_n", 0) or 0)
                != int(expected.get("checkpoint_n") or 0)
                or str(
                    getattr(box, "runtime_checkpoint_revision", "") or ""
                ).lower()
                != str(expected.get("revision") or "").lower()
                or str(
                    getattr(box, "runtime_checkpoint_repo", "") or ""
                )
                != str(expected.get("repo") or "")
            ):
                issues.append("standalone_checkpoint_mismatch")
        validator_window = int(
            getattr(validator_state, "window", 0) or 0
        )
        per_gpu = (
            telemetry.get("per_gpu")
            if isinstance(telemetry, dict)
            else None
        )
        if (
            bool(getattr(box, "standalone_fresh", False))
            and validator_window > 0
            and isinstance(per_gpu, dict)
        ):
            configured_generation_gpus: set[str] = set()
            for component in getattr(box, "runtime_components", []):
                if (
                    not isinstance(component, dict)
                    or component.get("role") != "generation"
                ):
                    continue
                try:
                    configured_generation_gpus.add(
                        _canonical_physical_gpu_id(
                            component.get("component_id")
                        )
                    )
                except ValueError:
                    continue
            for gpu in sorted(configured_generation_gpus):
                row = per_gpu.get(gpu)
                if not isinstance(row, dict):
                    # No process-bound generation record means there is no
                    # truthful natural-window frontier to compare yet.
                    continue
                last_natural = row.get("last_natural_window")
                if (
                    isinstance(last_natural, int)
                    and not isinstance(last_natural, bool)
                    and last_natural > 0
                    and validator_window - last_natural > 1
                ):
                    issues.append(
                        "standalone_gpu_generation_lag:"
                        f"{gpu}:last={last_natural}:"
                        f"validator={validator_window}"
                    )
    return list(dict.fromkeys(issues))


def _box_readiness_issues(
    box: BoxState,
    *,
    validator_state: ValidatorState | None = None,
    now: float | None = None,
    poll_limit: float | None = None,
) -> list[str]:
    """Return the shared operator/readiness blockers for one miner service."""
    issues: list[str] = []
    if poll_limit is not None:
        current = time.time() if now is None else float(now)
        last_poll = float(getattr(box, "last_poll_s", 0.0) or 0.0)
        box_age = max(0.0, current - last_poll) if last_poll else None
        if box_age is None or box_age > poll_limit:
            issues.append("stale")

    if getattr(box, "miner_kind", "legacy") == "reliquary_one":
        miner = getattr(box, "reliquary_one", {})
        miner = miner if isinstance(miner, dict) else {}
        if not miner:
            issues.append("reliquary_one_missing")
        if not bool(miner.get("fresh")):
            issues.append("reliquary_one_stale")
        collector_error = str(
            getattr(box, "reliquary_one_error", "")
            or miner.get("collector_error")
            or ""
        )
        if collector_error:
            issues.append(f"reliquary_one:{collector_error}")
        service = miner.get("service") if isinstance(miner, dict) else None
        service = service if isinstance(service, dict) else {}
        if not service.get("active"):
            issues.append("down")
        if not service.get("enabled"):
            issues.append("service_not_enabled")
        binding = miner.get("binding") if isinstance(miner, dict) else None
        binding = binding if isinstance(binding, dict) else {}
        expected_strategy, expected_environment = _reliquary_one_expected_lane(box)
        if not expected_strategy or not expected_environment:
            issues.append("configured_lane_unknown")
        if str(binding.get("environment") or "") != expected_environment:
            issues.append("environment_mismatch")
        if str(binding.get("strategy") or "") != expected_strategy:
            issues.append("strategy_mismatch")
        if not str(binding.get("checkpoint_revision") or ""):
            issues.append("checkpoint_binding_missing")
        gpus = miner.get("gpu") if isinstance(miner, dict) else None
        if not isinstance(gpus, list) or not gpus:
            issues.append("gpu_missing")
        if validator_state is not None:
            expected_n = int(
                getattr(validator_state, "checkpoint_n", 0) or 0
            )
            expected_revision = str(
                getattr(validator_state, "checkpoint_revision", "") or ""
            ).lower()
            observed_n = int(binding.get("checkpoint_number") or 0)
            observed_revision = str(
                binding.get("checkpoint_revision") or ""
            ).lower()
            if expected_n and observed_n != expected_n:
                issues.append("checkpoint_number_mismatch")
            if expected_revision and observed_revision != expected_revision:
                issues.append("checkpoint_revision_mismatch")
        if getattr(box, "quarantine_active", False):
            issues.append("quarantined")
        return list(dict.fromkeys(issues))

    unit_error = str(getattr(box, "unit_resolution_error", "") or "")
    if unit_error:
        issues.append(f"unit_resolution:{unit_error}")
    if _active_standalone_certification(box):
        # This is useful live GPU work but deliberately cannot satisfy miner
        # readiness: it has no active canary/mine unit or submission funnel.
        issues.append("certifying_non_mining")
    if not box.proc_alive:
        issues.append("down")
    if box.error and not unit_error:
        issues.append(f"probe:{box.error}")
    if (box.unit or box.unit_candidates) and not getattr(
        box, "env_file_ok", False
    ):
        issues.append(
            f"env_file:{getattr(box, 'env_file_error', '') or 'unreadable'}"
        )

    if getattr(box, "engine_mode", "") == "reference" and box.proc_alive:
        if not getattr(box, "protocol_profile", ""):
            issues.append("protocol_parity_pending")
        if not getattr(box, "runtime_parity_ok", False):
            issues.append("runtime_parity_pending")
        if not getattr(box, "reference_ready", False):
            issues.append("reference_not_ready")
        if not (
            getattr(box, "miner_source_revision", "")
            and getattr(box, "reliquary_source_revision", "")
        ):
            issues.append("source_manifest_missing")
        issues.extend(_box_model_readiness_issues(box, validator_state))

        expected_model = _validator_model_identity(validator_state)
        provisioned_model = _provisioned_model_identity(box)
        frontier_checkpoint = str(
            getattr(box, "frontier_checkpoint_revision", "") or ""
        )
        # The auction seed-v2 adapter intentionally leaves the v1 reference
        # prompt picker disabled. Its persisted frontier can therefore remain
        # on the last v1 checkpoint without saying anything about the active
        # model, whose exact revision is still enforced above.
        prompt_frontier_active = (
            getattr(box, "protocol_profile", "")
            != "forced_seed_v2_auction_legacy_wire"
        )
        if frontier_checkpoint and prompt_frontier_active:
            comparison_checkpoint = str(expected_model.get("revision") or "")
            if not comparison_checkpoint and provisioned_model["valid"]:
                comparison_checkpoint = str(provisioned_model["revision"])
            if comparison_checkpoint and not _revisions_match(
                frontier_checkpoint, comparison_checkpoint
            ):
                issues.append("checkpoint_frontier_mismatch")

    issues.extend(
        _standalone_readiness_issues(
            box, validator_state=validator_state
        )
    )

    issues.extend(_box_code_auction_details(
        box,
        validator_state,
        now=now,
        health_stale_s=poll_limit,
    )["issues"])
    crossover = _box_code_selector_crossover_details(
        box,
        validator_state,
        now=now,
        stale_after_s=poll_limit,
    )
    if crossover["configured"] and not crossover["ok"]:
        issues.extend(
            f"code_selector_crossover:{issue}"
            for issue in crossover["issues"]
        )
    overlap_abba = _box_code_overlap_abba_details(
        box,
        validator_state,
        now=now,
        stale_after_s=poll_limit,
    )
    if overlap_abba["configured"] and not overlap_abba["ok"]:
        issues.extend(
            f"code_overlap_abba:{issue}"
            for issue in overlap_abba["issues"]
        )

    if getattr(box, "quarantine_active", False):
        issues.append("quarantined")
    return issues


def _readiness_issue_label(issue: str) -> str:
    """Turn a machine-readable readiness issue into compact operator text."""
    key, _, detail = issue.partition(":")
    if key == "standalone_gpu_generation_lag":
        parts = detail.split(":")
        gpu = parts[0] if parts else "GPU"
        fields = dict(
            part.split("=", 1)
            for part in parts[1:]
            if "=" in part
        )
        short_gpu = (
            f"{gpu[:12]}…{gpu[-8:]}"
            if len(gpu) > 22
            else gpu
        )
        return (
            f"{short_gpu} natural generation stale "
            f"(w{fields.get('last', '?')} vs "
            f"w{fields.get('validator', '?')})"
        )
    if key == "code_selector_crossover":
        return "Code selector crossover " + (detail or "invalid").replace(
            "_", " "
        )
    if key == "code_overlap_abba":
        return "Code overlap AB/BA " + (detail or "invalid").replace(
            "_", " "
        )
    if key == "code_auction":
        code_labels = {
            "probe_missing": "Code readiness probe missing",
            "invocation_id_missing": "Code InvocationID missing",
            "environment_mismatch": "Code lane environment drift",
            "engine_mode_mismatch": "Code lane engine-mode drift",
            "process_identity_mismatch": "Code probe/process mismatch",
            "miner_unit_disabled": "Code miner not enabled at boot",
            "settings_not_process_bound": "Code settings not process-bound",
            "prescreen_disabled": "exact Code pre-screen disabled",
            "policy_mismatch": "deadline-aware Code policy missing",
            "source_manifest_invalid": "Code source manifest invalid",
            "validator_health_error": "validator health fetch failed",
            "validator_health_status": "validator health status not ready",
            "validator_health_stale": "validator health stale",
            "validator_source_pending": "validator source revision pending",
            "validator_source_mismatch": "validator source revision mismatch",
            "runtime_profile_pending": "runtime profile identity pending",
            "runtime_profile_mismatch": "runtime profile mismatch",
            "runtime_attestation_missing": "Code runtime attestation missing",
            "grader_inactive": "Code grader inactive",
            "grader_disabled": "Code grader not enabled at boot",
            "grader_socket_invalid": "Code grader socket invalid",
            "grader_bundle_invalid": "Code grader bundle invalid",
            "grader_source_mismatch": "Code grader source mismatch",
            "grader_canary_missing": "Code grader canary missing",
            "grader_runtime_attestation_mismatch": (
                "Code grader/runtime attestation mismatch"
            ),
            "ledger_not_configured": "Code outcome ledger not configured",
            "ledger_unavailable": "Code outcome ledger unavailable",
            "ledger_schema_invalid": "Code outcome ledger schema invalid",
            "ledger_partition_identity_pending": (
                "Code ledger partition identity pending"
            ),
            "ledger_current_partition_missing": (
                "current Code ledger partition missing"
            ),
            "terminal_funnel_incompatible": (
                "Code terminal-funnel API incompatible"
            ),
            "terminal_funnel_current_partition_missing": (
                "current Code terminal funnel missing"
            ),
            "terminal_funnel_counts_invalid": (
                "Code terminal-funnel counts invalid"
            ),
        }
        return code_labels.get(detail, f"Code auction {detail.replace('_', ' ')}")
    labels = {
        "probe": "probe failed",
        "unit_resolution": "unit selection failed",
        "env_file": "env file unreadable",
        "protocol_parity_pending": "protocol parity pending",
        "runtime_parity_pending": "runtime parity pending",
        "reference_not_ready": "reference not ready",
        "source_manifest_missing": "source manifest missing",
        "model_manifest_missing": "provisioned model manifest missing",
        "model_manifest_invalid": "provisioned model manifest invalid",
        "validator_model_identity_pending": "validator model identity pending",
        "model_kind_mismatch": "provisioned model kind mismatch",
        "model_checkpoint_n_mismatch": "provisioned checkpoint number mismatch",
        "model_repo_mismatch": "provisioned model repository mismatch",
        "model_revision_mismatch": "provisioned model revision mismatch",
        "base_model_pin_missing": "exact base-model pin missing",
        "base_model_repo_mismatch": "base-model repository mismatch",
        "base_model_revision_mismatch": "base-model revision mismatch",
        "checkpoint_frontier_mismatch": "frontier checkpoint mismatch",
    }
    label = labels.get(key, key.replace("_", " "))
    if detail and key not in {"probe", "env_file"}:
        return f"{label}: {detail}"
    return label


def _watched_hotkey_rows() -> list[dict[str, object]]:
    """Configured hotkeys the dashboard treats as ours."""
    labels_by_hotkey: dict[str, list[str]] = {}
    for _ssh_target, hk, fleet_label, _color, _unit in FLEET:
        if hk:
            labels_by_hotkey.setdefault(hk, []).append(fleet_label)

    rows: list[dict[str, object]] = []
    for hk, label in OUR_SS58.items():
        labels = labels_by_hotkey.get(hk) or [label]
        rows.append(
            {
                "label": label,
                "labels": labels,
                "hotkey": hk,
                "short": _short_hotkey(hk),
                "prefix": hk[:12],
            }
        )
    return rows


def _configured_target_rows(
    chain_hotkeys: set[str] | None = None,
    chain_seen: bool = False,
) -> list[dict[str, object]]:
    """Dashboard target hotkeys with fleet wiring and chain status."""
    chain_hotkeys = chain_hotkeys or set()
    fleet_by_hotkey: dict[str, list[dict[str, str]]] = {}
    for alias, hk, fleet_label, color, unit in FLEET:
        if not hk:
            continue
        fleet_by_hotkey.setdefault(hk, []).append(
            {
                "alias": _display_ssh_alias(alias),
                "label": fleet_label,
                "color": color,
                "unit": unit or "",
            }
        )

    rows: list[dict[str, object]] = []
    for row in _watched_hotkey_rows():
        hk = str(row["hotkey"])
        fleet_targets = fleet_by_hotkey.get(hk, [])
        registered: bool | None = None
        if chain_seen:
            registered = hk in chain_hotkeys
        rows.append(
            {
                "label": row["label"],
                "labels": row["labels"],
                "hotkey": hk,
                "hotkey_short": row["short"],
                "hotkey_prefix": row["prefix"],
                "registered": registered,
                "fleet": fleet_targets,
                "aliases": [target["alias"] for target in fleet_targets],
                "units": [target["unit"] for target in fleet_targets if target["unit"]],
            }
        )
    return rows


_NOISY_FEEDBACK_FIELD_RE = re.compile(
    r"(?:^|[\s,;|/·-]+)"
    r"(?:down[\s_-]*rank|up[\s_-]*rank|%?\s*bunk|%?\s*legit|score)"
    r"\s*[:=]?\s*[-+]?\d+(?:\.\d+)?%?",
    re.IGNORECASE,
)


def _clean_operator_msg(msg: str) -> str:
    """Drop low-value scoring/debug metrics from live log snippets."""
    labels = (
        r"down[\s_-]*rank",
        r"up[\s_-]*rank",
        r"%?\s*bunk",
        r"%?\s*legit",
        r"\bscore\b",
    )
    if sum(1 for pat in labels if re.search(pat, msg, re.IGNORECASE)) >= 3:
        return ""
    cleaned = _NOISY_FEEDBACK_FIELD_RE.sub(" ", msg)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned.strip(" ,;|/·-")


def _share_pct(part: float, whole: float) -> float:
    return (part / whole * 100.0) if whole > 0 else 0.0


def _fmt_pct(pct: float, whole: float, digits: int = 0) -> str:
    if whole <= 0:
        return "—"
    return f"{pct:.{digits}f}%"


def _proof_batch_target(windows: list) -> int:
    """Best current proof-batch denominator from recent sealed archives."""
    totals = [int(getattr(w, "total_batch", 0) or 0) for w in windows[:24]]
    totals = [n for n in totals if n > 0]
    return max([B_BATCH] + totals)


def _live_valid_context(vs: ValidatorState, windows: list) -> tuple[int, int, str]:
    """Return current valid count, proof target, and source label.

    Validator `/state` is the operational truth for current pressure. The
    event tail remains useful for hotkey-specific accepts/rejects, but on a
    dashboard cold start it can miss the latest seal and overcount old accepts.
    """
    # The current validator runs one batch target per environment (currently
    # math + code).  Summing the `/health` environment counters is therefore
    # the only gauge that cannot hide a starved environment behind the old
    # aggregate, single-batch `/state` value.
    environments = getattr(vs, "window_environments", {}) or {}
    targets = getattr(vs, "environment_targets", {}) or {}
    if isinstance(environments, dict) and environments:
        valid = 0
        saw_valid_counter = False
        for env_data in environments.values():
            if not isinstance(env_data, dict):
                continue
            for key in ("valid_submissions_count", "valid_count", "valid"):
                if key in env_data:
                    try:
                        valid += int(env_data.get(key) or 0)
                        saw_valid_counter = True
                    except (TypeError, ValueError):
                        pass
                    break
        target = 0
        if isinstance(targets, dict):
            for target_value in targets.values():
                try:
                    target += int(target_value or 0)
                except (TypeError, ValueError):
                    pass
        if target <= 0:
            target = int(getattr(vs, "batch_size", 0) or 0)
        if target <= 0:
            target = _proof_batch_target(windows)
        if saw_valid_counter:
            return valid, target, "/health per-env"

    target = _proof_batch_target(windows)
    state_valid = int(getattr(vs, "valid", 0) or 0)
    if getattr(vs, "last_fetch_at", 0.0):
        return state_valid, target, "/state"
    return current_window_accept_count(), target, "events"


def _validator_liveness_details(
    vs: ValidatorState,
    windows: list | None = None,
) -> dict[str, object]:
    """Return a stable, JSON-safe view of additive liveness telemetry.

    The validator rollout is intentionally one-way compatible: old builds do
    not publish these fields, while new builds may publish only a subset
    during READY. Missing metrics therefore stay ``None``/empty and never
    become synthetic zero-latency samples.
    """
    windows = list(windows or [])
    queue_by_env = getattr(vs, "queue_depth_by_environment", {}) or {}
    workers_by_env = getattr(vs, "admission_workers_by_environment", {}) or {}
    proof_by_env = (
        getattr(vs, "proof_verification_inflight_by_environment", {}) or {}
    )
    admission_by_env = (
        getattr(vs, "admission_latency_ms_by_environment", {}) or {}
    )
    seal_by_env = getattr(vs, "seal_drain_by_environment", {}) or {}
    window_envs = getattr(vs, "window_environments", {}) or {}
    environment_names = sorted(
        {
            str(name)
            for mapping in (
                queue_by_env,
                workers_by_env,
                proof_by_env,
                admission_by_env,
                seal_by_env,
                window_envs,
            )
            if isinstance(mapping, dict)
            for name in mapping
        }
    )
    by_environment: dict[str, dict[str, object]] = {}
    for environment in environment_names:
        by_environment[environment] = {
            "queue_depth": queue_by_env.get(environment),
            "admission_workers": workers_by_env.get(environment),
            "proof_verification_inflight": proof_by_env.get(environment),
            "admission_latency_ms": dict(
                admission_by_env.get(environment, {})
                if isinstance(admission_by_env.get(environment), dict)
                else {}
            ),
            "seal_drain": dict(
                seal_by_env.get(environment, {})
                if isinstance(seal_by_env.get(environment), dict)
                else {}
            ),
        }

    terminal = [
        window
        for window in windows
        if bool(getattr(window, "terminal_data_present", True))
    ]
    completed_numbers = [
        int(getattr(window, "n", 0) or 0)
        for window in terminal
        if getattr(window, "window_status", "completed") == "completed"
        and int(getattr(window, "n", 0) or 0) > 0
    ]
    aborted_numbers = [
        int(getattr(window, "n", 0) or 0)
        for window in terminal
        if getattr(window, "window_status", "completed") == "aborted"
        and int(getattr(window, "n", 0) or 0) > 0
    ]
    archive_queue_depth = getattr(vs, "archive_queue_depth", None)
    if (
        not isinstance(archive_queue_depth, int)
        or isinstance(archive_queue_depth, bool)
        or archive_queue_depth < 0
    ):
        archive_queue_depth = None
    archive = {
        "reported": bool(getattr(vs, "archive_continuity_reported", False)),
        "queue_depth": archive_queue_depth,
        "last_uploaded_window": getattr(vs, "archive_last_uploaded_window", None),
        "last_failed_window": getattr(vs, "archive_last_failed_window", None),
        "uploads_succeeded_total": getattr(
            vs, "archive_uploads_succeeded_total", None
        ),
        "upload_failures_total": getattr(
            vs, "archive_upload_failures_total", None
        ),
        "last_enqueued_window": getattr(vs, "archive_last_enqueued_window", None),
        "archives_enqueued_total": getattr(
            vs, "archive_archives_enqueued_total", None
        ),
        "enqueue_gaps_total": getattr(vs, "archive_enqueue_gaps_total", None),
        "last_enqueue_gap": getattr(vs, "archive_last_enqueue_gap", None),
        "last_completed_window": max(completed_numbers, default=None),
        "last_aborted_window": max(aborted_numbers, default=None),
    }
    seal_timeouts = sorted(
        environment
        for environment, details in by_environment.items()
        if isinstance(details.get("seal_drain"), dict)
        and details["seal_drain"].get("timed_out") is True
    )
    snapshot_nonempty = sorted(
        environment
        for environment, details in by_environment.items()
        if isinstance(details.get("seal_drain"), dict)
        and any(
            isinstance(details["seal_drain"].get(name), int)
            and not isinstance(details["seal_drain"].get(name), bool)
            and details["seal_drain"].get(name, 0) > 0
            for name in (
                "queue_depth_at_snapshot",
                "inflight_workers_at_snapshot",
                "pending_reservations_at_snapshot",
                "inflight_reservations_at_snapshot",
                "dropped_at_snapshot",
                "dropped_at_snapshot_count",
            )
        )
    )
    raw_event_loop = getattr(vs, "event_loop_lag_ms", {}) or {}
    raw_endpoints = getattr(vs, "endpoint_latency_ms", {}) or {}
    return {
        "reported": bool(getattr(vs, "liveness_telemetry_reported", False)),
        "event_loop_lag_ms": (
            dict(raw_event_loop) if isinstance(raw_event_loop, dict) else {}
        ),
        "endpoint_latency_ms": (
            dict(raw_endpoints) if isinstance(raw_endpoints, dict) else {}
        ),
        "by_environment": by_environment,
        "seal_timeout_environments": seal_timeouts,
        "seal_nonempty_snapshot_environments": snapshot_nonempty,
        "archive": archive,
    }


def _valid_pressure_label(valid: int, target: int) -> str:
    suffix = "+" if target > 0 and valid > target else ""
    return f"{valid}{suffix}/{target}"


def _window_share_tone(pct: float, whole: float) -> tuple[str, str]:
    """Non-brand data palette for window slot/reward shares."""
    if whole <= 0:
        return "var(--share-none)", "no data"
    if pct <= 0:
        return "var(--share-zero)", "zero"
    if pct < 25:
        return "var(--share-low)", "low"
    if pct < 50:
        return "var(--share-mid)", "earning"
    if pct < 75:
        return "var(--share-good)", "strong"
    return "var(--share-great)", "dominant"


def _share_legend_html() -> str:
    bands = [
        ("0", "var(--share-zero)"),
        ("1-24%", "var(--share-low)"),
        ("25-49%", "var(--share-mid)"),
        ("50-74%", "var(--share-good)"),
        ("75%+", "var(--share-great)"),
    ]
    return " ".join(
        f"<span class='share-band'><i style='background:{color}'></i>{label}</span>"
        for label, color in bands
    )


def _reliquary_one_context() -> tuple[
    BoxState | None,
    dict[str, object],
    ValidatorState,
    list[object],
]:
    """Return one reconciled view of the configured all-in-one miner."""

    from reliquary_one import reconcile_attempts

    with _lock:
        candidates = [
            item
            for item in _boxes
            if getattr(item, "miner_kind", "legacy") == "reliquary_one"
        ]
        box = next((item for item in candidates if item.proc_alive), None)
        if box is None:
            box = candidates[0] if candidates else None
        validator = _vs
        windows = list(_windows)
        local = dict(getattr(box, "reliquary_one", {}) or {}) if box else {}
    if box is None or not local:
        return box, local, validator, windows
    reconciled = reconcile_attempts(
        local,
        windows=windows,
        hotkey=box.hotkey,
        verdict_rows=verdict_rows_for_hotkey(box.hotkey),
    )
    return box, reconciled, validator, windows


def _reliquary_one_contexts() -> list[tuple[BoxState, dict[str, object]]]:
    """Return every configured live-lane row with authenticated outcomes."""

    from reliquary_one import reconcile_attempts

    with _lock:
        windows = list(_windows)
        rows = [
            (box, dict(getattr(box, "reliquary_one", {}) or {}))
            for box in _boxes
            if getattr(box, "miner_kind", "legacy") == "reliquary_one"
        ]
    contexts: list[tuple[BoxState, dict[str, object]]] = []
    for box, local in rows:
        if local:
            local = reconcile_attempts(
                local,
                windows=windows,
                hotkey=box.hotkey,
                verdict_rows=verdict_rows_for_hotkey(box.hotkey),
            )
        contexts.append((box, local))
    return contexts


def _reliquary_one_public_projection(
    box: BoxState,
    windows: list[object],
) -> dict[str, object]:
    from reliquary_one import public_projection, reconcile_attempts

    local = dict(getattr(box, "reliquary_one", {}) or {})
    if not local:
        return {}
    return public_projection(
        reconcile_attempts(
            local,
            windows=windows,
            hotkey=box.hotkey,
            verdict_rows=verdict_rows_for_hotkey(box.hotkey),
        )
    )


def _one_age(value: object, *, now: float | None = None) -> str:
    current = time.time() if now is None else now
    try:
        timestamp = float(value or 0.0)
    except (TypeError, ValueError, OverflowError):
        return "unknown"
    return fmt_age(max(0.0, current - timestamp)) if timestamp else "unknown"


def _one_number(value: object, *, digits: int = 2, suffix: str = "s") -> str:
    if value is None or isinstance(value, bool):
        return "—"
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return "—"
    if not math.isfinite(number):
        return "—"
    return f"{number:.{digits}f}{suffix}"


def _one_badge(label: str, tone: str = "neutral") -> str:
    safe_tone = tone if tone in {"good", "pending", "bad", "neutral"} else "neutral"
    return (
        f"<span class='one-badge one-badge-{safe_tone}'>"
        f"{html.escape(label)}</span>"
    )


def _one_bool_badge(value: object, true_label: str, false_label: str) -> str:
    if value is True:
        return _one_badge(true_label, "good")
    if value is False:
        return _one_badge(false_label, "bad")
    return _one_badge("pending", "pending")


def _one_empty_panel(title: str, detail: str) -> str:
    return (
        "<div class='panel one-panel'>"
        f"<h2>{html.escape(title)}</h2>"
        f"<div class='one-empty'>{html.escape(detail)}</div></div>"
    )


def _batch_occupancy_html(miner: dict[str, object]) -> str:
    """Render the six aggregate batch-occupancy values without row data."""

    observability = miner.get("observability")
    observability = observability if isinstance(observability, dict) else {}
    occupancy = observability.get("batch_occupancy")
    occupancy = occupancy if isinstance(occupancy, dict) else {}
    required = (
        "apply_steps",
        "rows_at_capacity_steps",
        "rows_at_capacity_fraction",
        "minimum_rows",
        "mean_rows",
        "capacity_rows",
    )
    if not occupancy or any(key not in occupancy for key in required):
        return ""
    try:
        apply_steps = int(occupancy["apply_steps"])
        capacity_steps = int(occupancy["rows_at_capacity_steps"])
        fraction = float(occupancy["rows_at_capacity_fraction"])
        minimum_rows = int(occupancy["minimum_rows"])
        mean_rows = float(occupancy["mean_rows"])
        capacity_rows = int(occupancy["capacity_rows"])
    except (TypeError, ValueError, OverflowError):
        return ""
    if (
        apply_steps < 0
        or capacity_steps < 0
        or capacity_steps > apply_steps
        or minimum_rows < 0
        or capacity_rows < 0
        or minimum_rows > mean_rows
        or mean_rows > capacity_rows
        or not 0.0 <= fraction <= 1.0
        or not math.isfinite(fraction)
        or not math.isfinite(mean_rows)
    ):
        return ""
    title = (
        "Aggregate miner dashboard batch occupancy only: apply steps at row "
        "capacity, plus minimum, mean, and configured capacity rows."
    )
    return (
        f"<span title='{html.escape(title)}'><b>{fraction:.1%} at capacity</b>"
        "<div class='dim small'>"
        f"{capacity_steps}/{apply_steps} apply steps · "
        f"μ{mean_rows:.1f} rows · min {minimum_rows} / cap {capacity_rows}"
        "</div></span>"
    )


def _v4_lane_funnel_html() -> str:
    contexts = _reliquary_one_contexts()
    occupancy_by_lane = [
        _batch_occupancy_html(miner) for _box, miner in contexts
    ]
    show_batch_occupancy = any(occupancy_by_lane)
    rows = []
    for (box, miner), occupancy_html in zip(
        contexts, occupancy_by_lane, strict=True
    ):
        funnel = miner.get("funnel") if isinstance(miner, dict) else None
        funnel = funnel if isinstance(funnel, dict) else {}
        binding = miner.get("binding") if isinstance(miner, dict) else None
        binding = binding if isinstance(binding, dict) else {}
        service = miner.get("service") if isinstance(miner, dict) else None
        service = service if isinstance(service, dict) else {}
        lane = str(binding.get("strategy") or box.active_lane or "unknown")
        environment = str(binding.get("environment") or "unknown")
        status = "active · fresh" if service.get("active") and miner.get("fresh") else (
            "active · stale" if service.get("active") else "inactive"
        )
        tone = "good" if status == "active · fresh" else "bad"
        lane_detail = f"{html.escape(lane)} · {html.escape(environment)}"
        ranked = int(funnel.get("validator_ranked_candidates") or 0)
        rows.append(
            "<tr>"
            f"<td><b>{html.escape(box.label)}</b>"
            f"<div class='dim small'>{lane_detail}</div></td>"
            f"<td>{_one_badge(status, tone)}</td>"
            f"<td class='num'>{int(funnel.get('generated_groups') or 0)}</td>"
            f"<td class='num'>{int(funnel.get('protocol_valid_groups') or 0)}</td>"
            f"<td class='num'>{int(funnel.get('locally_eligible_groups') or 0)}</td>"
            f"<td class='num'>{int(funnel.get('signed_precommits') or 0)}</td>"
            f"<td class='num'>{ranked}</td>"
            f"<td class='num'>{int(funnel.get('selected_slots') or 0)}</td>"
            + (
                f"<td class='mono'>{occupancy_html or '—'}</td>"
                if show_batch_occupancy
                else ""
            )
            + "</tr>"
        )
    return f"""
  <div class='one-table-scroll' tabindex='0'>
    <table class='grid compact' aria-label='V4 production funnel by live lane'>
      <thead><tr>
        <th>live lane</th><th>health</th>
        <th class='num'>generated groups</th>
        <th class='num'>protocol-valid groups</th>
        <th class='num'>locally eligible groups</th>
        <th class='num'>signed precommits</th>
        <th class='num'>validator-ranked candidates</th>
        <th class='num'>selected slots</th>
        {"<th title='Aggregate miner dashboard batch occupancy: fraction of apply steps at configured row capacity, with minimum and mean rows.'>batch occupancy</th>" if show_batch_occupancy else ""}
      </tr></thead>
      <tbody>{''.join(rows)}</tbody>
    </table>
  </div>
  <div class='one-panel-note'>Counters are distinct stages: generated is a
  complete-group lower bound from per-attempt terminal events; protocol-valid
  requires demonstrated local BF16 proof/preflight; rank and selected slot
  require authenticated validator/R2 evidence.{" Batch occupancy is a separate, aggregate-only local apply-step summary; it contains no row IDs or payloads." if show_batch_occupancy else ""}</div>
"""


def render_our_miner_now_html() -> str:
    box, miner, validator, _windows = _reliquary_one_context()
    if box is None:
        return _one_empty_panel(
            "Our miner now",
            "No reliquary_one miner is configured.",
        )
    service = miner.get("service") if isinstance(miner, dict) else None
    service = service if isinstance(service, dict) else {}
    binding = miner.get("binding") if isinstance(miner, dict) else None
    binding = binding if isinstance(binding, dict) else {}
    pipeline = miner.get("current_pipeline") if isinstance(miner, dict) else None
    pipeline = pipeline if isinstance(pipeline, dict) else {}
    gpus = miner.get("gpu") if isinstance(miner, dict) else None
    gpu = gpus[0] if isinstance(gpus, list) and gpus else {}
    attempts = miner.get("attempts") if isinstance(miner, dict) else None
    attempts = attempts if isinstance(attempts, list) else []
    revision = str(binding.get("checkpoint_revision") or "")
    expected_revision = str(
        getattr(validator, "checkpoint_revision", "") or ""
    )
    expected_n = int(getattr(validator, "checkpoint_n", 0) or 0)
    observed_n = int(binding.get("checkpoint_number") or 0)
    checkpoint_equal = bool(
        revision
        and expected_revision
        and revision.lower() == expected_revision.lower()
        and (not expected_n or observed_n == expected_n)
    )
    latest_transport = next(
        (
            row
            for row in attempts
            if isinstance(row, dict)
            and row.get("transport", {}).get("status") != "not_sent"
        ),
        None,
    )
    latest_admission = next(
        (
            row
            for row in attempts
            if isinstance(row, dict)
            and row.get("admission", {}).get("status")
            not in {None, "pending", "not_applicable"}
        ),
        None,
    )
    latest_terminal = next(
        (
            row
            for row in attempts
            if isinstance(row, dict)
            and (
                row.get("auction", {}).get("selected") is not None
                or row.get("auction", {}).get("rewarded") is not None
            )
        ),
        None,
    )
    service_badge = _one_badge(
        "active" if service.get("active") else "down",
        "good" if service.get("active") else "bad",
    )
    fresh_badge = _one_badge(
        "fresh" if miner.get("fresh") else "stale",
        "good" if miner.get("fresh") else "bad",
    )
    checkpoint_badge = _one_badge(
        "validator match" if checkpoint_equal else "match pending",
        "good" if checkpoint_equal else "pending",
    )
    hbm_used = float(gpu.get("memory_used_mib") or 0.0) / 1024.0
    hbm_total = float(gpu.get("memory_total_mib") or 0.0) / 1024.0
    transport_status = (
        str(latest_transport.get("transport", {}).get("status") or "unknown")
        if latest_transport
        else "unknown"
    )
    transport_window = int(latest_transport.get("window_n") or 0) if latest_transport else 0
    transport_badge = _one_badge(
        f"local {transport_status}" + (f" · w{transport_window}" if transport_window else ""),
        "good" if transport_status == "accepted" else "neutral",
    )
    admission = (
        latest_admission.get("admission", {}) if latest_admission else {}
    )
    admission_status = str(admission.get("status") or "pending")
    admission_window = int(latest_admission.get("window_n") or 0) if latest_admission else 0
    admission_badge = _one_badge(
        f"validator {admission_status}" + (f" · w{admission_window}" if admission_window else ""),
        "good" if admission_status == "accepted" else
        "bad" if admission_status == "rejected" else "pending",
    )
    terminal = latest_terminal.get("auction", {}) if latest_terminal else {}
    terminal_window = int(latest_terminal.get("window_n") or 0) if latest_terminal else 0
    pipeline_window = (
        f"w{int(pipeline.get('window_n'))}"
        if isinstance(pipeline.get("window_n"), int)
        else "unscoped event"
    )
    validator_health = str(getattr(validator, "health_status", "") or "unknown")
    validator_health_class = "" if validator_health == "ok" else " class='red'"
    lane = str(binding.get("strategy") or box.active_lane or "unknown")
    environment = html.escape(str(binding.get("environment") or "unknown"))
    accelerator = html.escape(str(gpu.get("name") or "accelerator"))
    kicker = f"{accelerator} · {html.escape(lane)} · {environment}"
    return f"""
<div class='panel one-panel one-now-panel'>
  <h2>Our miner now <span class='one-source-age'>events {_one_age(miner.get('last_heartbeat_at'))} ago</span></h2>
  <div class='one-now-head'>
    <div>
      <div class='one-kicker'>{kicker}</div>
      <div class='one-primary-status'>{service_badge}{fresh_badge}</div>
    </div>
    <div class='one-now-window'>
      <span>validator</span><b>w{int(getattr(validator, 'window', 0) or 0) or '—'}</b>
      <small{validator_health_class}>{html.escape(str(getattr(validator, 'state', '') or 'unknown'))} · health {html.escape(validator_health)}</small>
    </div>
  </div>
  <div class='one-kpi-grid'>
    <div class='one-kpi'><span>service uptime</span><b>{fmt_age(float(service.get('uptime_seconds') or 0.0))}</b><small>{int(service.get('restarts') or 0)} restarts · {'enabled' if service.get('enabled') else 'not enabled'}</small></div>
    <div class='one-kpi'><span>checkpoint</span><b>cp{observed_n or '—'}</b><small class='mono'>{html.escape(revision[:12] or 'unknown')} · {checkpoint_badge}</small></div>
    <div class='one-kpi'><span>pipeline now</span><b>{html.escape(str(pipeline.get('stage') or 'unknown').upper())}</b><small>{html.escape(pipeline_window)} · {_one_number(pipeline.get('stage_elapsed_s'), digits=1)} elapsed</small></div>
    <div class='one-kpi'><span>GPU</span><b>{_one_number(gpu.get('utilization_pct'), digits=0, suffix='%')}</b><small>{hbm_used:.1f}/{hbm_total:.1f} GiB · {_one_number(gpu.get('power_draw_w'), digits=0, suffix='W')}</small></div>
  </div>
  <div class='one-truth-strip' aria-label='Latest outcome truth layers'>
    <div><span>A · local transport</span>{transport_badge}</div>
    <div><span>B · validator admission</span>{admission_badge}<small>{html.escape(str(admission.get('reason') or ''))}</small></div>
    <div><span>C · terminal auction{' · w' + str(terminal_window) if terminal_window else ''}</span>{_one_bool_badge(terminal.get('selected'), 'selected', 'not selected')}{_one_bool_badge(terminal.get('rewarded'), 'rewarded', 'not rewarded')}</div>
  </div>
  {_v4_lane_funnel_html()}
</div>
"""


def _one_pipeline_rows(miner: dict[str, object]) -> list[dict[str, object]]:
    pipeline = miner.get("current_pipeline")
    pipeline = pipeline if isinstance(pipeline, dict) else {}
    attempts = miner.get("attempts")
    attempts = attempts if isinstance(attempts, list) else []
    window_n = pipeline.get("window_n")
    current = next(
        (
            row
            for row in attempts
            if isinstance(row, dict) and row.get("window_n") == window_n
        ),
        None,
    )
    current = current if isinstance(current, dict) else {}
    failure = current.get("failure") if isinstance(current, dict) else None
    transport = current.get("transport") if isinstance(current, dict) else None
    transport = transport if isinstance(transport, dict) else {}
    admission = current.get("admission") if isinstance(current, dict) else None
    admission = admission if isinstance(admission, dict) else {}
    auction = current.get("auction") if isinstance(current, dict) else None
    auction = auction if isinstance(auction, dict) else {}
    running_stage = str(pipeline.get("stage") or "")

    def row(name: str, duration: object, completed: bool) -> dict[str, object]:
        status = "completed" if completed else "pending"
        if running_stage == name.lower().replace("sign/spool", "sign_spool"):
            status = "running"
        if failure and name in {"GENERATE", "PROOF", "SIGN/SPOOL"}:
            status = "failed"
        return {"name": name, "duration": duration, "status": status}

    transport_seen = transport.get("status") in {"accepted", "rejected", "ambiguous"}
    rows = [
        row("OBSERVE", current.get("open_age_at_start_s"), window_n is not None),
        row("CLAIM", None, bool(current)),
        row("GENERATE", current.get("generation_s"), current.get("generation_s") is not None),
        row("PROOF", current.get("proof_s"), current.get("proof_s") is not None),
        row("SIGN/SPOOL", None, transport_seen),
        row("PRECOMMIT", current.get("precommit_s"), transport.get("precommit") in {"accepted", "rejected"}),
        row("REVEAL", current.get("reveal_s"), transport.get("reveal") in {"accepted", "rejected"}),
        row(
            "OUTCOME",
            None,
            admission.get("status") not in {None, "pending"}
            or auction.get("selected") is not None,
        ),
    ]
    if running_stage == "generate" and not current:
        rows[2]["status"] = "running"
    if running_stage == "failed":
        rows[2]["status"] = "failed"
    if rows[-1]["status"] == "pending" and transport_seen:
        rows[-1]["status"] = "running"
    return rows


def render_current_miner_pipeline_html() -> str:
    box, miner, _validator, _windows = _reliquary_one_context()
    if box is None or not miner:
        return _one_empty_panel("Current window pipeline", "Structured events are warming.")
    pipeline = miner.get("current_pipeline")
    pipeline = pipeline if isinstance(pipeline, dict) else {}
    pipeline_window = (
        f"w{int(pipeline.get('window_n'))}"
        if isinstance(pipeline.get("window_n"), int)
        else "unscoped event"
    )
    rows = _one_pipeline_rows(miner)
    cells = []
    for item in rows:
        status = str(item["status"])
        tone = {
            "completed": "good",
            "running": "pending",
            "failed": "bad",
        }.get(status, "neutral")
        cells.append(
            "<li class='one-stage one-stage-" + html.escape(status) + "'>"
            f"<span class='one-stage-dot' aria-hidden='true'></span>"
            f"<b>{html.escape(str(item['name']))}</b>"
            f"<small>{_one_number(item.get('duration'), digits=4)}</small>"
            f"{_one_badge(status, tone)}</li>"
        )
    return f"""
<div class='panel one-panel'>
  <h2>Current window pipeline <span class='one-source-age'>{html.escape(pipeline_window)} · stage {_one_number(pipeline.get('stage_elapsed_s'), digits=1)}</span></h2>
  <ol class='one-stage-list'>{''.join(cells)}</ol>
  <div class='one-panel-note'>Unknown durations remain —. Local transport acceptance is not validator admission.</div>
</div>
"""


def _attempt_outcome_html(attempt: dict[str, object]) -> str:
    admission = attempt.get("admission")
    admission = admission if isinstance(admission, dict) else {}
    auction = attempt.get("auction")
    auction = auction if isinstance(auction, dict) else {}
    status = str(admission.get("status") or "pending")
    reason = str(admission.get("reason") or "")
    admission_html = _one_badge(
        status + (f" · {reason}" if reason else ""),
        "good" if status == "accepted" else
        "bad" if status == "rejected" else
        "neutral" if status == "not_applicable" else "pending",
    )
    selected_html = _one_bool_badge(
        auction.get("selected"), "selected", "not selected"
    )
    rewarded_html = _one_bool_badge(
        auction.get("rewarded"), "rewarded", "not rewarded"
    )
    return (
        "<div class='one-truth-line'><span>validator</span>"
        f"{admission_html}</div>"
        "<div class='one-truth-line'><span>auction</span>"
        f"{selected_html}{rewarded_html}</div>"
    )


def _attempt_filter_state(attempt: dict[str, object]) -> str:
    failure = attempt.get("failure")
    if isinstance(failure, dict) and failure:
        return "failed"
    admission = attempt.get("admission")
    admission = admission if isinstance(admission, dict) else {}
    admission_status = str(admission.get("status") or "pending")
    if admission_status == "accepted":
        return "admitted"
    if admission_status == "rejected":
        return "rejected"
    if admission_status == "pending":
        return "pending"
    return "other"


def render_recent_miner_attempts_html() -> str:
    box, miner, _validator, _windows = _reliquary_one_context()
    if box is None or not miner:
        return _one_empty_panel("Our recent attempts", "No structured attempts yet.")
    attempts = miner.get("attempts")
    attempts = attempts if isinstance(attempts, list) else []
    retained = [attempt for attempt in attempts[:14] if isinstance(attempt, dict)]
    state_counts = {
        state: sum(_attempt_filter_state(attempt) == state for attempt in retained)
        for state in ("pending", "admitted", "rejected", "failed")
    }
    rows = []
    for attempt in retained:
        if not isinstance(attempt, dict):
            continue
        transport = attempt.get("transport")
        transport = transport if isinstance(transport, dict) else {}
        transport_status = str(transport.get("status") or "unknown")
        transport_tone = (
            "good" if transport_status == "accepted" else
            "bad" if transport_status == "rejected" else "neutral"
        )
        failure = attempt.get("failure")
        failure = failure if isinstance(failure, dict) else {}
        attempt_state = _attempt_filter_state(attempt)
        failure_text = ""
        if failure:
            failure_text = (
                f"<div class='one-failure'>{html.escape(str(failure.get('classification') or 'failed'))}"
                f" · {html.escape(str(failure.get('exception_file') or ''))}</div>"
            )
        rows.append(
            f"<tr class='one-attempt-row one-attempt-state-{html.escape(attempt_state)}' "
            f"data-attempt-row data-attempt-state='{html.escape(attempt_state)}' "
            f"data-window='{int(attempt.get('window_n') or 0)}'>"
            f"<td class='one-attempt-identity' data-label='Attempt'><div class='one-attempt-id'><b class='window-id'>w{int(attempt.get('window_n') or 0)}</b><span>#{int(attempt.get('ordinal') or 0)}</span></div>{failure_text}</td>"
            f"<td class='num one-attempt-start' data-label='OPEN+'>{_one_number(attempt.get('open_age_at_start_s'), digits=4)}</td>"
            f"<td class='num one-attempt-generation' data-label='Generate'>{_one_number(attempt.get('generation_s'), digits=4)}</td>"
            f"<td class='num one-attempt-proof' data-label='Proof'>{_one_number(attempt.get('proof_s'), digits=4)}<div class='dim small'>integrity {_one_number(attempt.get('proof_integrity_s'), digits=4)}</div></td>"
            f"<td class='num one-attempt-submit' data-label='Submit'><div><span>PC</span> {_one_number(attempt.get('precommit_s'), digits=4)}</div><div><span>R</span> {_one_number(attempt.get('reveal_s'), digits=4)}</div><small>{html.escape(str(transport.get('precommit') or 'unknown'))} · {html.escape(str(transport.get('reveal') or 'unknown'))}</small></td>"
            f"<td class='num one-attempt-total' data-label='Total'><b>{_one_number(attempt.get('total_s'), digits=4)}</b></td>"
            f"<td class='one-attempt-truth' data-label='Truth'><div class='one-truth-cell'><div class='one-truth-line'><span>transport</span>{_one_badge('local ' + transport_status, transport_tone)}</div>{_attempt_outcome_html(attempt)}</div></td>"
            "</tr>"
        )
    body = "".join(rows) or "<tr><td colspan='7' class='one-empty'>No attempts retained.</td></tr>"
    filter_labels = {
        "pending": "Pending",
        "admitted": "Admitted",
        "rejected": "Rejected",
        "failed": "Failed",
    }
    filters = [
        "<button type='button' class='one-attempt-filter is-active' "
        "data-one-attempt-filter='all' aria-pressed='true'>"
        f"All <span>{len(retained)}</span></button>"
    ]
    for state, label in filter_labels.items():
        count = state_counts[state]
        if not count:
            continue
        filters.append(
            "<button type='button' class='one-attempt-filter' "
            f"data-one-attempt-filter='{state}' aria-pressed='false'>"
            f"{label} <span>{count}</span></button>"
        )
    return f"""
<div class='panel one-panel'>
  <h2>Our recent attempts <span class='one-source-age'>events {_one_age(miner.get('last_heartbeat_at'))} ago</span></h2>
  <div class='one-attempt-toolbar'>
    <div class='one-attempt-filters' role='group' aria-label='Filter recent attempts'>{''.join(filters)}</div>
    <span class='one-attempt-visible' data-one-attempt-visible aria-live='polite'>{len(retained)} shown</span>
  </div>
  <div class='one-table-scroll' tabindex='0'>
    <table class='grid compact one-attempt-table' aria-label='Recent reliquary-one attempts'>
      <thead><tr><th>attempt</th><th class='num'>OPEN+</th><th class='num'>generate</th><th class='num'>proof</th><th class='num'>submit</th><th class='num'>total</th><th>local / validator / auction</th></tr></thead>
      <tbody>{body}</tbody>
    </table>
  </div>
</div>
"""


def _auction_display_value(row: dict[str, object], names: tuple[str, ...]) -> str:
    for name in names:
        value = row.get(name)
        if value is None or isinstance(value, bool):
            continue
        if isinstance(value, float):
            return f"{value:.4f}"
        return html.escape(str(value))
    return "—"


def render_last_sealed_auction_html() -> str:
    box, miner, _validator, windows = _reliquary_one_context()
    if box is None:
        return _one_empty_panel("Last sealed auction", "No active miner configured.")
    attempt_windows = {
        int(row.get("window_n") or 0)
        for row in miner.get("attempts", [])
        if isinstance(row, dict)
    }
    sealed = next(
        (
            window
            for window in windows
            if int(getattr(window, "n", 0) or 0) in attempt_windows
            and bool(getattr(window, "terminal_data_present", False))
        ),
        None,
    )
    if sealed is None:
        return _one_empty_panel(
            "Last sealed auction",
            "Authenticated terminal R2 evidence is pending.",
        )
    environment = "opencodeinstruct"
    public_rows = []
    for collection_name in ("batch", "runners_up", "rejected"):
        collection = getattr(sealed, collection_name, [])
        if not isinstance(collection, list):
            continue
        for raw in collection:
            if not isinstance(raw, dict):
                continue
            if str(raw.get("env_name") or "") != environment:
                continue
            row = dict(raw)
            row["_collection"] = collection_name
            public_rows.append(row)
    public_rows.sort(
        key=lambda row: (
            int(row.get("canonical_rank") or 10_000),
            float(row.get("arrival_ts") or 0.0),
        )
    )
    environment_counts = getattr(sealed, "environment_counts", {})
    environment_counts = (
        environment_counts if isinstance(environment_counts, dict) else {}
    )
    counts = environment_counts.get(environment, {})
    counts = counts if isinstance(counts, dict) else {}
    candidate_count = sum(
        int(counts.get(name) or 0)
        for name in ("selected", "runners_up", "rejected")
    ) or len(public_rows)
    our_count = sum(
        1 for row in public_rows if str(row.get("hotkey") or "") == box.hotkey
    )
    rows_html = []
    selected_seen = 0
    for row in public_rows[:32]:
        selected = row.get("selected_for_batch") is True or row.get("_collection") == "batch"
        if selected:
            selected_seen += 1
        boundary = " slot-boundary" if selected and selected_seen == 8 else ""
        ours = str(row.get("hotkey") or "") == box.hotkey
        candidate = "OUR MINER" if ours else _short_hotkey(str(row.get("hotkey") or ""))
        reason = str(row.get("reject_reason") or row.get("reason") or "")
        outcome = (
            "rewarded" if row.get("rewarded") is True else
            "selected" if selected else
            reason or "not selected"
        )
        outcome_tone = "good" if selected else "bad" if reason else "neutral"
        rows_html.append(
            f"<tr class='{'our-candidate' if ours else ''}{boundary}'>"
            f"<td class='num'>{_auction_display_value(row, ('canonical_rank',))}</td>"
            f"<td><b>{html.escape(candidate)}</b></td>"
            f"<td class='num'>{_auction_display_value(row, ('response_time', 'drand_delta'))}</td>"
            f"<td class='num'>{_auction_display_value(row, ('value', 'score', 'sigma'))}</td>"
            f"<td class='num'>{_auction_display_value(row, ('k', 'k_correct'))}</td>"
            f"<td class='num'>{_auction_display_value(row, ('median_completion_tokens', 'completion_tokens_median'))}</td>"
            f"<td>{_one_badge(outcome, outcome_tone)}</td></tr>"
        )
    randomness = getattr(sealed, "randomness", {})
    randomness = randomness if isinstance(randomness, dict) else {}
    drand = _auction_display_value(
        randomness,
        ("round", "drand_round", "seal_trigger_round"),
    )
    return f"""
<div class='panel one-panel'>
  <h2>Last sealed auction <span class='one-source-age'>R2 · w{int(getattr(sealed, 'n', 0) or 0)}</span></h2>
  <div class='one-auction-meta'>
    <span>Code candidates <b>{candidate_count}</b></span>
    <span>ours <b>{our_count}</b></span>
    <span>slot boundary <b>top 8</b></span>
    <span>drand <b>{drand}</b></span>
  </div>
  <div class='one-table-scroll one-auction-scroll' tabindex='0'>
    <table class='grid compact one-auction-table' aria-label='Last sealed Code auction'>
      <thead><tr><th class='num'>rank</th><th>candidate</th><th class='num'>arrival</th><th class='num'>value/score</th><th class='num'>k</th><th class='num'>median tokens</th><th>outcome</th></tr></thead>
      <tbody>{''.join(rows_html)}</tbody>
    </table>
  </div>
</div>
"""


def render_checkpoint_runtime_html() -> str:
    box, miner, validator, _windows = _reliquary_one_context()
    if box is None or not miner:
        return _one_empty_panel("Checkpoint / runtime", "Binding is warming.")
    binding = miner.get("binding")
    binding = binding if isinstance(binding, dict) else {}
    checkpoints = miner.get("checkpoints")
    checkpoints = checkpoints if isinstance(checkpoints, dict) else {}
    revision = str(binding.get("checkpoint_revision") or "")
    expected_revision = str(getattr(validator, "checkpoint_revision", "") or "")
    exact = bool(revision and expected_revision and revision.lower() == expected_revision.lower())
    current_n = int(binding.get("checkpoint_number") or 0)
    tier_cards = []
    for tier in ("active", "rollback", "incoming"):
        rows = checkpoints.get(tier)
        rows = rows if isinstance(rows, list) else []
        first = rows[0] if rows and isinstance(rows[0], dict) else {}
        tier_revision = str(first.get("revision") or "")
        checkpoint_label = (
            f"cp{current_n}" if tier == "active" and current_n else
            f"cp{current_n - 1}" if tier == "rollback" and current_n > 0 and tier_revision else
            "empty" if not tier_revision else "checkpoint"
        )
        tier_cards.append(
            f"<div class='one-tier'><span>{tier}</span><b>{checkpoint_label}</b>"
            f"<small class='mono'>{html.escape(tier_revision[:12] or '—')}</small></div>"
        )
    rotations = []
    for event in reversed(miner.get("events", [])):
        if not isinstance(event, dict) or event.get("event") != "checkpoint_activated":
            continue
        metrics = event.get("metrics")
        metrics = metrics if isinstance(metrics, dict) else {}
        rotations.append(
            f"<tr><td>{_one_age(event.get('timestamp'))} ago</td>"
            f"<td class='mono'>{html.escape(str(event.get('checkpoint_revision') or '')[:12])}</td>"
            f"<td class='mono dim'>{html.escape(str(metrics.get('previous_checkpoint_revision') or '')[:12] or '—')}</td>"
            f"<td class='num'>{_one_number(metrics.get('download_seconds'), digits=2)}</td></tr>"
        )
        if len(rotations) >= 6:
            break
    return f"""
<div class='panel one-panel'>
  <h2>Checkpoint / runtime <span class='one-source-age'>binding {_one_age(miner.get('source_timestamps', {}).get('binding'))} old</span></h2>
  <div class='one-runtime-head'>
    <div><span>active binding</span><b>cp{current_n or '—'} · {html.escape(revision[:12] or 'unknown')}</b></div>
    <div>{_one_badge('exact validator revision' if exact else 'validator revision pending', 'good' if exact else 'pending')}</div>
    <div class='mono small'>release {html.escape(str(binding.get('release_sha') or '')[:12] or '—')} · generator {html.escape(str(binding.get('generator_fingerprint') or '')[:12] or '—')} · proof runtime {html.escape(str(binding.get('proof_fingerprint') or '')[:12] or '—')}</div>
  </div>
  <div class='one-tier-grid'>{''.join(tier_cards)}</div>
  <div class='one-table-scroll' tabindex='0'>
    <table class='grid compact one-rotation-table' aria-label='Checkpoint rotation history'>
      <thead><tr><th>activated</th><th>revision</th><th>previous</th><th class='num'>download</th></tr></thead>
      <tbody>{''.join(rotations) or "<tr><td colspan='4' class='dim'>No rotation events retained.</td></tr>"}</tbody>
    </table>
  </div>
</div>
"""


def _structured_event_summary(event: dict[str, object]) -> str:
    name = str(event.get("event") or "unknown")
    metrics = event.get("metrics")
    metrics = metrics if isinstance(metrics, dict) else {}
    if name == "checkpoint_activated":
        return (
            f"checkpoint {str(event.get('checkpoint_revision') or '')[:12]} activated"
            f" · download {_one_number(metrics.get('download_seconds'), digits=2)}"
        )
    if name == "window_started":
        return f"OPEN observed at +{_one_number(metrics.get('open_age_seconds'), digits=3)}"
    if name == "live_submission":
        return (
            f"local precommit {'accepted' if metrics.get('precommit_accepted') is True else 'not accepted'}"
            f" · reveal {'accepted' if metrics.get('reveal_accepted') is True else 'not accepted'}"
            f" · total {_one_number(metrics.get('total_seconds'), digits=4)}"
        )
    if name == "window_completed":
        sent = int(metrics.get("first_sent") is True) + int(metrics.get("second_sent") is True)
        return f"window completed · {sent}/2 local exchanges sent"
    if name in {"window_attempt_failed", "daemon_iteration_failed"}:
        return (
            f"{str(event.get('classification') or 'failed')} · "
            f"{str(metrics.get('exception_file') or 'exception file unavailable')}"
        )
    return str(event.get("classification") or name)


def render_structured_miner_log_html() -> str:
    box, miner, _validator, _windows = _reliquary_one_context()
    if box is None or not miner:
        return _one_empty_panel("Structured live log", "Events are warming.")
    events = [row for row in miner.get("events", []) if isinstance(row, dict)]
    windows = sorted(
        {
            int(row["window_n"])
            for row in events
            if isinstance(row.get("window_n"), int)
        },
        reverse=True,
    )
    event_names = sorted({str(row.get("event") or "") for row in events})
    classifications = sorted(
        {str(row.get("classification") or "") for row in events if row.get("classification")}
    )
    options_window = "".join(
        f"<option value='{value}'>w{value}</option>" for value in windows[:24]
    )
    options_event = "".join(
        f"<option value='{html.escape(value)}'>{html.escape(value)}</option>"
        for value in event_names
    )
    options_classification = "".join(
        f"<option value='{html.escape(value)}'>{html.escape(value)}</option>"
        for value in classifications
    )
    rows = []
    for event in reversed(events[-80:]):
        classification = str(event.get("classification") or "")
        name = str(event.get("event") or "")
        window = event.get("window_n")
        tone = "bad" if "failed" in name else "good" if name in {"live_submission", "checkpoint_activated"} else "neutral"
        rows.append(
            f"<tr data-one-log-row data-window='{window if isinstance(window, int) else ''}' data-event='{html.escape(name)}' data-classification='{html.escape(classification)}'>"
            f"<td>{_one_age(event.get('timestamp'))} ago</td>"
            f"<td><b class='window-id'>{'w' + str(window) if isinstance(window, int) else '—'}</b></td>"
            f"<td>{_one_badge(name, tone)}</td>"
            f"<td>{html.escape(classification or '—')}</td>"
            f"<td>{html.escape(_structured_event_summary(event))}</td></tr>"
        )
    return f"""
<div class='panel one-panel'>
  <h2>Structured live log <span class='one-source-age'>last 500 max · {_one_age(miner.get('last_heartbeat_at'))} ago</span></h2>
  <div class='one-log-filters' aria-label='Structured log filters'>
    <label>window<select data-one-log-filter='window'><option value=''>all</option>{options_window}</select></label>
    <label>event<select data-one-log-filter='event'><option value=''>all</option>{options_event}</select></label>
    <label>classification<select data-one-log-filter='classification'><option value=''>all</option>{options_classification}</select></label>
  </div>
  <div class='one-table-scroll one-log-scroll' tabindex='0'>
    <table class='grid compact one-log-table' aria-label='Redacted reliquary-one structured events'>
      <thead><tr><th>age</th><th>window</th><th>event</th><th>classification</th><th>summary</th></tr></thead>
      <tbody>{''.join(rows)}</tbody>
    </table>
  </div>
</div>
"""


def render_scoreboard_html() -> str:
    """Top-of-dashboard score strip: window outcome + EMA in one glance."""
    with _lock:
        windows = list(_windows)
        leaderboard = list(_ema_cache)
        n_windows_used = _ema_window_count
        vs = _vs

    live_valid, proof_target, _live_source = _live_valid_context(vs, windows)
    live_pct = _share_pct(min(live_valid, proof_target), proof_target)
    live_color, live_tone = _window_share_tone(live_pct, proof_target)

    latest = windows[0] if windows else None
    if latest:
        latest_slot_pct = _share_pct(latest.ours, latest.total_batch)
        latest_slot_color, latest_slot_tone = _window_share_tone(
            latest_slot_pct, latest.total_batch
        )
        latest_title = "latest sealed"
        latest_value = f"{latest.ours}/{latest.total_batch}"
        latest_sub = (
            f"w{latest.n} · {_fmt_pct(latest_slot_pct, latest.total_batch)} slots"
        )
    else:
        latest_slot_color = "var(--share-none)"
        latest_title = "latest sealed"
        latest_value = "—"
        latest_sub = "waiting for R2 windows"

    recent = windows[:24]
    recent_ours = sum(w.ours for w in recent)
    recent_total = sum(w.total_batch for w in recent)
    recent_slot_pct = _share_pct(recent_ours, recent_total)
    recent_slot_color, recent_slot_tone = _window_share_tone(recent_slot_pct, recent_total)
    our_total_ema = sum(r["ema"] for r in leaderboard if r["hotkey"] in OUR_SS58)
    ema_color, ema_tone = _window_share_tone(our_total_ema * 100, 1.0)

    cards = [
        (
            "live window",
            f"w{vs.window or '—'}",
            f"{html.escape(vs.state or '?')} · {_valid_pressure_label(live_valid, proof_target)} valid",
            "var(--window-id)",
        ),
        (latest_title, latest_value, latest_sub, latest_slot_color),
        (
            "last 24",
            _fmt_pct(recent_slot_pct, recent_total),
            f"{recent_ours}/{recent_total} slots",
            recent_slot_color,
        ),
        (
            "EMA",
            f"{our_total_ema * 100:.2f}%",
            f"{n_windows_used or len(windows)} windows",
            ema_color,
        ),
    ]
    card_html = "".join(
        f"""
    <div class="score-card">
      <div class="score-label">{label}</div>
      <div class="score-value" style="color:{color}">{value}</div>
      <div class="score-sub">{sub}</div>
    </div>"""
        for label, value, sub, color in cards
    )
    return f"""
<div class="panel score-panel">
  <h2>Window score</h2>
  <div class="score-grid">{card_html}
  </div>
</div>
"""


def render_mission_control_html() -> str:
    """Mission-first view: window race, pool admission, and firing posture."""
    with _lock:
        boxes = list(_boxes)
        vs = _vs
        windows = list(_windows)
        chain_hotkeys = {getattr(h, "hotkey", "") for h in _chain.hotkeys}
        chain_seen = bool(_chain.last_fetch_at)

    cur_events = _mission_current_events()
    watched_rows = _watched_hotkey_rows()
    watched_hotkeys = [str(row["hotkey"]) for row in watched_rows]
    unregistered_targets = [
        hk for hk in watched_hotkeys if chain_seen and hk not in chain_hotkeys
    ]
    our_accepts = [e for e in cur_events if e.kind == "accept" and e.ours]
    all_accepts = [e for e in cur_events if e.kind == "accept"]
    our_rejects = [e for e in cur_events if e.kind == "reject" and e.ours]
    our_late = [e for e in cur_events if e.kind == "late_drop" and e.ours]
    our_accept_by_hk = Counter(e.hotkey12 for e in our_accepts)
    all_accept_by_hk = Counter(e.hotkey12 for e in all_accepts)

    total_ready = sum(b.miner_ready for b in boxes if b.proc_alive)
    total_inflight = sum(b.miner_inflight for b in boxes if b.proc_alive)
    total_submitted = sum(b.miner_submitted_this_win for b in boxes if b.proc_alive)
    total_fresh = sum(b.fresh_built_30m for b in boxes)
    total_cache = sum(b.cache_fwd_30m + b.prefinalized_30m for b in boxes)
    total_bursts = sum(b.burst_30m for b in boxes)
    total_grace = sum(b.late_grace_30m for b in boxes)
    total_batch_filled = sum(b.batch_filled_30m for b in boxes)

    recent = windows[:24]
    recent_ours = sum(w.ours for w in recent)
    recent_total = sum(w.total_batch for w in recent)
    recent_pct = _share_pct(recent_ours, recent_total)
    recent_reward = sum(getattr(w, "reward_ours", 0.0) for w in recent)
    recent_reward_total = sum(getattr(w, "reward_total", 0.0) for w in recent)
    recent_reward_pct = _share_pct(recent_reward, recent_reward_total)
    recent_color, recent_tone = _window_share_tone(recent_pct, recent_total)

    live_valid, proof_target, live_source = _live_valid_context(vs, windows)
    live_color, _live_tone = _window_share_tone(
        _share_pct(len(our_accepts), proof_target), proof_target
    )
    batch_color = (
        "var(--share-zero)" if live_valid >= proof_target and not our_accepts else
        "var(--share-mid)" if live_valid >= proof_target else
        "var(--share-good)"
    )
    all_rounds = [e.drand_round for e in all_accepts if e.drand_round]
    our_rounds = [e.drand_round for e in our_accepts if e.drand_round]
    min_all_round = min(all_rounds) if all_rounds else 0
    min_our_round = min(our_rounds) if our_rounds else 0
    round_delta = (min_our_round - min_all_round) if min_all_round and min_our_round else 0
    if not min_all_round:
        round_value = "—"
        round_sub = "waiting for validator pool accepts"
        round_color = "var(--share-none)"
    elif not min_our_round:
        round_value = str(min_all_round)
        round_sub = "competitor bucket only"
        round_color = "var(--share-zero)"
    elif round_delta <= 0:
        round_value = str(min_our_round)
        round_sub = "ours leads/ties lowest bucket"
        round_color = "var(--share-good)"
    else:
        round_value = f"+{round_delta}"
        round_sub = f"ours {min_our_round} vs best {min_all_round}"
        round_color = "var(--share-zero)" if round_delta >= 2 else "var(--share-low)"

    state_via_ssh = vs.error == "ssh-state"
    if vs.error and not state_via_ssh:
        diagnosis = f"/state flaky ({html.escape(vs.error)}); check direct /health and /verdicts freshness."
        diagnosis_color = "var(--yellow)"
    elif not any(b.proc_alive for b in boxes):
        diagnosis = "No live submitter processes visible."
        diagnosis_color = "var(--red)"
    elif unregistered_targets:
        target = _short_hotkey(unregistered_targets[0])
        diagnosis = f"Target {target} is configured and live, but chain says it is not registered on netuid {NETUID}."
        diagnosis_color = "var(--yellow)"
    elif len(our_accepts) > 0:
        diagnosis = "We are landing this window; watch batch_filled to tune fire timing."
        diagnosis_color = "var(--green)"
    elif live_valid >= proof_target:
        diagnosis = "Window already saturated before our current-window pool admission; this is timing/slot-race, not raw GPU."
        diagnosis_color = "var(--red)"
    elif total_ready + total_inflight == 0 and total_fresh == 0:
        diagnosis = "GPU processes alive but pipeline looks cold; check model load or journal errors."
        diagnosis_color = "var(--red)"
    elif total_ready > 0:
        diagnosis = "Ready sketches exist; if pool admissions stay zero, submission gate/cutoff is the suspect."
        diagnosis_color = "var(--yellow)"
    else:
        diagnosis = "Generating/refilling cache; next open proof decides whether grace fix paid off."
        diagnosis_color = "var(--yellow)"

    hk_rows = []
    for row in watched_rows:
        hk = str(row["hotkey"])
        pref = hk[:12]
        a = our_accept_by_hk.get(pref, 0)
        total = all_accept_by_hk.get(pref, 0)
        color = "var(--green)" if a else "var(--dim)"
        if not chain_seen:
            chain_label = "chain warming"
            chain_color = "var(--dim)"
        elif hk in chain_hotkeys:
            chain_label = "registered"
            chain_color = "var(--green)"
        else:
            chain_label = "not registered"
            chain_color = "var(--yellow)"
        labels = [str(x) for x in row["labels"]]
        display_label = str(row["label"]) if len(labels) <= 1 else f"{row['label']}+{len(labels) - 1}"
        title = f"{hk}: " + ", ".join(labels)
        hk_rows.append(
            f"<span class='mission-chip mission-chip-target' title='{html.escape(title)}'>"
            f"<span class='target-kicker'>target</span>"
            f"<span style='color:{color}'>{html.escape(display_label)}</span>"
            f"<span class='mono'>{html.escape(str(row['short']))}</span>"
            f"<span class='mission-target-status' style='color:{chain_color}'>{chain_label}</span>"
            f"<b style='color:{color}'>{a}</b><span class='dim'>/{total} pool accepts</span></span>"
        )
    hk_html = "".join(hk_rows) or "<span class='dim'>no owned hotkeys configured</span>"

    return f"""
<div class="panel mission-panel">
  <h2>Mission control <span class="dim small">window race · cache · slot capture</span></h2>
  <div class="mission-grid">
    <div class="mission-card">
      <div class="mission-label">current window</div>
      <div class="mission-value" style="color:var(--window-id)">w{vs.window or '—'}</div>
      <div class="mission-sub">{html.escape(vs.state or '?')} · ckpt {vs.checkpoint_n or '—'}{' · ssh' if state_via_ssh else ''}</div>
    </div>
    <div class="mission-card">
      <div class="mission-label">live pool accepts</div>
      <div class="mission-value" style="color:{live_color}">{len(our_accepts)}<span class="dim">/{live_valid}</span></div>
      <div class="mission-sub">ours / validator pool admissions</div>
    </div>
    <div class="mission-card">
      <div class="mission-label">drand bucket</div>
      <div class="mission-value mono" style="color:{round_color};font-size:18px">{html.escape(round_value)}</div>
      <div class="mission-sub">{html.escape(round_sub)}</div>
    </div>
    <div class="mission-card">
      <div class="mission-label">batch pressure</div>
      <div class="mission-value" style="color:{batch_color}">{live_valid}<span class="dim">/{proof_target}</span></div>
      <div class="mission-sub">{html.escape(live_source)} valid gauge</div>
    </div>
    <div class="mission-card">
      <div class="mission-label">last 24 windows</div>
      <div class="mission-value" style="color:{recent_color}" title="{recent_tone} slot share">{recent_ours}<span class="dim">/{recent_total}</span></div>
      <div class="mission-sub">{_fmt_pct(recent_pct, recent_total)} slots · {_fmt_pct(recent_reward_pct, recent_reward_total)} reward</div>
    </div>
    <div class="mission-card">
      <div class="mission-label">pipeline now</div>
      <div class="mission-value cyan">{total_ready}<span class="dim"> ready</span></div>
      <div class="mission-sub">{total_inflight} inflight · {total_submitted} submitted/local</div>
    </div>
    <div class="mission-card">
      <div class="mission-label">30m production</div>
      <div class="mission-value">{total_fresh}</div>
      <div class="mission-sub">{total_cache} cache/prefinal · {total_bursts} burst logs</div>
    </div>
    <div class="mission-card">
      <div class="mission-label">timing rejects</div>
      <div class="mission-value" style="color:{'var(--red)' if total_batch_filled else 'var(--dim)'}">{total_batch_filled}</div>
      <div class="mission-sub">batch_filled learned · grace {total_grace}</div>
    </div>
    <div class="mission-card">
      <div class="mission-label">our reject tail</div>
      <div class="mission-value" style="color:{'var(--red)' if our_rejects or our_late else 'var(--green)'}">{len(our_rejects) + len(our_late)}</div>
      <div class="mission-sub">{len(our_rejects)} worker · {len(our_late)} late-drop</div>
    </div>
  </div>
  <div class="mission-diagnosis" style="border-color:{diagnosis_color};color:{diagnosis_color}">
    {html.escape(diagnosis)}
  </div>
  <div class="mission-hotkeys">{hk_html}</div>
</div>
"""


def render_frontier_html() -> str:
    """B200/frontier feed health plus per-submitter timing knobs."""
    with _lock:
        boxes = list(_boxes)
        vs = _vs

    if not boxes:
        return "<div class='panel'><h2>Frontier feed</h2><span class='dim'>warming…</span></div>"

    current_ckpt = (vs.checkpoint_revision or "")[:12]
    rows = []
    for b in boxes:
        ckpt_count = b.frontier_ckpts.get(current_ckpt, 0) if current_ckpt else 0
        ckpt_total = sum(b.frontier_ckpts.values())
        ckpt_pct = int(ckpt_count / max(ckpt_total, 1) * 100) if ckpt_total else 0
        age = b.frontier_age_s
        newest = b.frontier_newest_age_s
        reference_frontier = bool(
            getattr(b, "frontier_checkpoint_n", 0)
            or getattr(b, "frontier_content_entries", 0)
        )
        fresh_age = 180 if reference_frontier else 20
        warning_age = 600 if reference_frontier else 90
        age_color = (
            "var(--green)" if 0 <= age <= fresh_age else
            "var(--yellow)" if 0 <= age <= warning_age else
            "var(--red)"
        )
        current_color = (
            "var(--green)" if ckpt_pct >= 70 else
            "var(--yellow)" if ckpt_pct >= 25 else
            "var(--red)" if ckpt_total else "var(--dim)"
        )
        relay_color = (
            "var(--green)" if b.state_relay_ok and 0 <= b.state_relay_age_s <= 15 else
            "var(--yellow)" if b.state_relay_ok and 0 <= b.state_relay_age_s <= 120 else
            "var(--red)" if b.state_relay_age_s >= 0 or b.state_relay_error else
            "var(--dim)"
        )
        relay_title = (
            f"upstream={b.state_relay_upstream_ms:.0f}ms "
            f"failures={b.state_relay_failures} {b.state_relay_error}"
        )
        relay_text = f"{b.state_relay_age_s:.1f}s" if b.state_relay_age_s >= 0 else "—"
        if b.watchdog_age_s < 0:
            watchdog_text = "—"
            watchdog_color = "var(--dim)"
            watchdog_title = "no frontier watchdog heartbeat on this box"
        else:
            watchdog_text = "ok" if b.watchdog_ok else "stale"
            if b.watchdog_stale_strikes:
                watchdog_text += f"/{b.watchdog_stale_strikes}"
            watchdog_color = (
                "var(--green)" if b.watchdog_ok and b.watchdog_age_s <= 90 and b.watchdog_stale_strikes == 0 else
                "var(--yellow)" if b.watchdog_ok and b.watchdog_age_s <= 180 else
                "var(--red)"
            )
            watchdog_title = (
                f"heartbeat_age={b.watchdog_age_s:.0f}s restarts={b.watchdog_restart_count} "
                f"action={b.watchdog_last_action} error={b.watchdog_last_error}"
            )
        ckpt_bits = " ".join(
            f"<span class='mission-chip mono'>{html.escape(k)} <b>{v}</b></span>"
            for k, v in list(b.frontier_ckpts.items())[:4]
        ) or "<span class='dim'>—</span>"
        shard = (
            f"{b.prompt_shard_id}/{b.prompt_shard_mod}"
            if b.prompt_shard_id >= 0 and b.prompt_shard_mod else "—"
        )
        offset = f"{b.drand_offset:+d}" if b.unit else "frontier"
        entry_text = str(b.frontier_entries)
        if getattr(b, "frontier_content_entries", 0):
            entry_text += f"+{b.frontier_content_entries}c"
        rows.append(f"""
<tr>
  <td><span class="lbl" style="color:{b.color}">{html.escape(b.label)}</span></td>
  <td class="num" style="color:{age_color}">{age:.0f}s</td>
  <td class="num dim">{newest:.0f}s</td>
  <td class="num" style="color:{relay_color}" title="{html.escape(relay_title)}">{html.escape(relay_text)}</td>
  <td class="mono" style="color:{watchdog_color}" title="{html.escape(watchdog_title)}">{html.escape(watchdog_text)}</td>
  <td class="num" title="prompt-index entries + exact prompt/ground-truth content priors">{entry_text}</td>
  <td class="num" style="color:{current_color}">{ckpt_count}<span class="dim">/{ckpt_total}</span></td>
  <td><div class="mini-meter"><span style="width:{ckpt_pct}%;background:{current_color}"></span></div></td>
  <td class="mono dim">{html.escape(offset)}</td>
  <td class="num dim">{b.picker_epsilon:.2f}</td>
  <td class="num dim">{b.picker_top_k or '—'}</td>
  <td class="num dim">{b.external_min_sigma:.2f}</td>
  <td class="num dim">{b.external_max_len or '—'}</td>
  <td class="mono dim">{html.escape(shard)}</td>
  <td>{ckpt_bits}</td>
</tr>""")

    return f"""
<div class="panel">
  <h2>Prompt frontier</h2>
  <table class="grid compact pipeline-table">
    <thead><tr>
      <th>worker</th><th class="num">file age</th><th class="num">newest</th>
      <th class="num" title="Local /state relay cache age. Green means miners are reading fresh local state instead of hammering the validator.">state</th>
      <th title="B200 frontier watchdog heartbeat. Only frontier-scout boxes normally have this.">watchdog</th>
      <th class="num">entries</th><th class="num">current ckpt</th><th>mix</th>
      <th class="num">drand Δ</th><th class="num">ε</th><th class="num">top-k</th>
      <th class="num">ext σ</th><th class="num">max len</th><th>shard</th><th>ckpts</th>
    </tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</div>
"""


def render_standalone_mining_truth_html() -> str:
    """Render the authoritative standalone miner funnel and identity."""
    with _lock:
        boxes = [
            box
            for box in _boxes
            if getattr(box, "standalone_telemetry_path", "")
        ]
        chain_entries = {
            str(getattr(entry, "hotkey", "") or ""): entry
            for entry in _chain.hotkeys
        }
        validator = _vs
    if not boxes:
        return ""

    def cell(value: object) -> str:
        return "—" if value is None else str(value)

    rows: list[str] = []
    comparison_rows: list[str] = []
    for box in boxes:
        certification = _active_standalone_certification(box)
        certification_gpu = certification.get("gpu", {})
        certification_gpu = (
            certification_gpu
            if isinstance(certification_gpu, dict)
            else {}
        )
        certification_identity = certification.get("identity", {})
        certification_identity = (
            certification_identity
            if isinstance(certification_identity, dict)
            else {}
        )
        telemetry = getattr(box, "standalone_telemetry", {})
        telemetry = telemetry if isinstance(telemetry, dict) else {}
        if certification:
            # A certification process cannot inherit a previous miner funnel,
            # even if a caller constructed an inconsistent cached object.
            telemetry = {}
        funnel = telemetry.get("funnel")
        funnel = funnel if isinstance(funnel, dict) else {}
        supervisor = getattr(box, "standalone_supervisor", {})
        supervisor = supervisor if isinstance(supervisor, dict) else {}
        manifest_profile = telemetry.get("manifest_profile")
        manifest_profile = (
            manifest_profile if isinstance(manifest_profile, dict) else {}
        )
        controller_authority = _registry_controller_authority(box)
        active_profile = supervisor.get("active")
        active_profile = (
            active_profile if isinstance(active_profile, dict) else None
        )
        if controller_authority and box.proc_alive:
            # The digest-bound registry names the controller that actually
            # owns submission authority.  A transition supervisor is useful
            # history, but its previous checkpoint must not override this
            # process-bound manifest after a completed cutover.
            active_profile = manifest_profile
        elif active_profile is None and box.proc_alive:
            active_profile = manifest_profile
        advertised_profile = {
            "protocol_version": getattr(validator, "protocol_version", 0),
            "generation_profile_id": getattr(
                validator, "generation_profile_id", ""
            ),
            "generation_contract_sha256": getattr(
                validator, "generation_contract_sha256", ""
            ),
            "checkpoint_repo_id": getattr(
                validator, "checkpoint_repo_id", ""
            ),
            "checkpoint_revision": getattr(
                validator, "checkpoint_revision", ""
            ),
            "checkpoint_profile_sha256": getattr(
                validator, "checkpoint_profile_sha256", ""
            ),
            "validator_image_revision": getattr(
                validator, "image_revision", ""
            ),
            "window_n": getattr(validator, "window", 0),
        }
        chain_entry = chain_entries.get(box.hotkey)
        uid = (
            str(getattr(chain_entry, "uid", "—"))
            if chain_entry is not None
            else "unregistered"
        )
        fresh = bool(getattr(box, "standalone_fresh", False))
        lifecycle = telemetry.get("window_lifecycle")
        lifecycle = lifecycle if isinstance(lifecycle, dict) else {}
        lifecycle_status = str(lifecycle.get("status") or "").upper()
        if certification:
            tone = "var(--yellow)"
            status = (
                "certifying · "
                f"{certification.get('phase') or 'transition'}"
            )
            status_sub = (
                "non-mining · "
                + (
                    "wallet-free · "
                    if certification.get("service_unit")
                    else ""
                )
                + "submit-disabled · "
                +
                f"runner {int(certification.get('runner_pid') or 0)} · "
                f"worker {int(certification.get('worker_pid') or 0)} · "
                f"GPU {int(certification_gpu.get('utilization_pct') or 0)}% "
                f"{int(certification_gpu.get('memory_used_mb') or 0)//1024}/"
                f"{int(certification_gpu.get('memory_total_mb') or 0)//1024} GB"
            )
        else:
            if not box.proc_alive:
                supervisor_phase = str(
                    supervisor.get("phase")
                    or supervisor.get("state")
                    or ""
                )
                tone = "var(--yellow)"
                status = (
                    f"fenced · {supervisor_phase.lower()}"
                    if supervisor_phase
                    else "fenced · no mining unit"
                )
            elif not fresh:
                tone = "var(--red)"
                status = str(
                    getattr(box, "standalone_error", "") or "stale"
                )
            elif lifecycle_status == "RECOVERING":
                tone = "var(--yellow)"
                status = "recovering"
            elif lifecycle_status == "STALLED":
                tone = "var(--red)"
                status = "stalled"
            elif lifecycle_status in {"READY", "ACTIVE"}:
                tone = "var(--green)"
                status = lifecycle_status.lower()
            else:
                tone = "var(--green)"
                status = "fresh"
            heartbeat_age = lifecycle.get("heartbeat_age_seconds")
            status_sub = (
                f"file {getattr(box, 'standalone_age_s', -1.0):.1f}s"
                f" · progress {getattr(box, 'standalone_progress_age_s', -1.0):.1f}s"
                + (
                    f" · heartbeat {float(heartbeat_age):.1f}s"
                    if isinstance(heartbeat_age, (int, float))
                    and not isinstance(heartbeat_age, bool)
                    else ""
                )
            )
            blockers = supervisor.get("blockers")
            if (
                not controller_authority
                and isinstance(blockers, list)
                and blockers
            ):
                status_sub += " · " + ", ".join(
                    str(value) for value in blockers[:2]
                )
        terminal = funnel.get("terminal_final")
        unresolved = funnel.get("terminal_unresolved")
        attempts = funnel.get("attempts")
        if terminal is not None:
            pending = max(int(attempts or 0) - int(terminal), 0)
            terminal_text = f"{terminal}/{pending}"
        elif unresolved is not None:
            terminal_text = f"—/{unresolved}"
        else:
            terminal_text = "—"
        selected_rate = telemetry.get(
            "selected_slots_per_physical_gpu_hour"
        )
        rewarded_rate = telemetry.get(
            "rewarded_slots_per_physical_gpu_hour"
        )
        live_unit = (
            "no mining unit"
            if certification or not box.proc_alive
            else str(getattr(box, "active_unit", "") or box.unit or "—")
        )
        miner_source = (
            str(certification_identity.get("miner_source_revision") or "—")
            if certification
            else str(getattr(box, "miner_source_revision", "") or "—")
        )
        validator_source = (
            str(
                certification_identity.get(
                    "validator_source_revision"
                ) or "—"
            )
            if certification
            else str(
                getattr(box, "reliquary_source_revision", "") or "—"
            )
        )
        checkpoint_n = (
            int(certification_identity.get("checkpoint_n") or 0)
            if certification
            else int(getattr(box, "runtime_checkpoint_n", 0) or 0)
        )
        advertised_protocol = int(
            advertised_profile.get("protocol_version") or 0
        )
        advertised_profile_id = str(
            advertised_profile.get("generation_profile_id") or "unknown"
        )
        advertised_contract = str(
            advertised_profile.get("generation_contract_sha256") or ""
        )
        active_protocol = (
            int(active_profile.get("protocol_version") or 0)
            if isinstance(active_profile, dict)
            else 0
        )
        active_profile_id = (
            str(active_profile.get("generation_profile_id") or "unknown")
            if isinstance(active_profile, dict)
            else "none"
        )
        historical_protocol = int(
            manifest_profile.get("protocol_version") or 2
        )
        historical_profile_id = str(
            manifest_profile.get("generation_profile_id")
            or "legacy-unscoped"
        )
        profile_cell = (
            f"advertised v{advertised_protocol or '?'} "
            f"{advertised_profile_id}"
            f"<div class='dim mono small'>contract "
            f"{html.escape(advertised_contract[:12] or '—')}</div>"
            f"<div class='small'>active "
            f"{('v' + str(active_protocol) + ' ') if active_protocol else ''}"
            f"{html.escape(active_profile_id)}</div>"
            f"<div class='dim small'>historical v{historical_protocol} "
            f"{html.escape(historical_profile_id)}</div>"
        )
        advertised_checkpoint = (
            f"n{int(getattr(validator, 'checkpoint_n', 0) or 0)} "
            f"{str(advertised_profile.get('checkpoint_revision') or '')[:12]}"
        )
        identity_label = "" if box.proc_alive else "historical "
        checkpoint_revision = (
            str(certification_identity.get("checkpoint_revision") or "—")
            if certification
            else str(
                getattr(box, "runtime_checkpoint_revision", "") or "—"
            )
        )
        physical_gpu_hours_text = (
            "—"
            if certification
            else f"{float(telemetry.get('physical_gpu_hours') or 0.0):.3f}"
        )
        selected_rate_text = (
            "—"
            if certification
            else f"{float(selected_rate or 0.0):.3f}"
        )
        rewarded_rate_text = (
            "—"
            if certification
            else f"{float(rewarded_rate or 0.0):.3f}"
        )
        latest_window = telemetry.get("latest_window")
        latest_window_activity = (
            "requested · completed groups "
            f"{int(funnel.get('generation_complete') or 0)}"
            if isinstance(latest_window, int)
            and not isinstance(latest_window, bool)
            else ""
        )
        rows.append(f"""
<tr>
  <td><span class="lbl" style="color:{box.color}">{html.escape(box.label)}</span>
    <div class="dim small mono">{html.escape(live_unit)}</div>
  </td>
  <td class="mono" style="color:{tone}">{html.escape(status)}
    <div class="dim small">{html.escape(status_sub)}</div>
  </td>
  <td class="num">{cell(latest_window)}
    <div class="dim small">{html.escape(latest_window_activity)}</div>
    <div class="dim small">advertised w{int(advertised_profile.get("window_n") or 0)}</div>
  </td>
  <td class="mono small">{html.escape(box.hotkey or "—")}</td>
  <td class="num">{html.escape(uid)}</td>
  <td class="mono small">{html.escape(str(getattr(box, "operator", "") or "—"))}</td>
  <td class="mono small">{identity_label}{html.escape(miner_source)}
    <div class="dim">{identity_label}validator {html.escape(validator_source)}</div>
    <div class="dim">advertised validator {html.escape(str(advertised_profile.get("validator_image_revision") or "—"))}</div>
  </td>
  <td class="mono small">{identity_label}n{checkpoint_n}
    <div class="dim">{html.escape(checkpoint_revision)}</div>
    <div class="dim">advertised {html.escape(advertised_checkpoint)}</div>
  </td>
  <td class="mono small">{profile_cell}</td>
  <td class="num">{cell(funnel.get("attempts"))}</td>
  <td class="num">{cell(funnel.get("natural_complete_m8"))}</td>
  <td class="num">{cell(funnel.get("local_eligible"))}</td>
  <td class="num">{cell(funnel.get("precommit_accepted"))}</td>
  <td class="num">{cell(funnel.get("reveal_accepted"))}</td>
  <td class="num">{cell(funnel.get("pool_accepted"))}</td>
  <td class="num">{cell(funnel.get("network_proof_passed"))}</td>
  <td class="num">{cell(funnel.get("selected"))}</td>
  <td class="num">{cell(funnel.get("rewarded"))}</td>
  <td class="num">{terminal_text}</td>
  <td class="num">{physical_gpu_hours_text}</td>
  <td class="num">{selected_rate_text}</td>
  <td class="num">{rewarded_rate_text}</td>
</tr>""")
        scopes = telemetry.get("comparison_scopes")
        scopes = scopes if isinstance(scopes, dict) else {}
        for scope_name in ("active_profile", "release", "latest_six", "lifetime"):
            scope = scopes.get(scope_name)
            if not isinstance(scope, dict):
                continue
            scope_funnel = scope.get("funnel")
            scope_funnel = (
                scope_funnel if isinstance(scope_funnel, dict) else {}
            )
            quota = scope.get("quota")
            quota = quota if isinstance(quota, dict) else {}
            timings = scope.get("timings_ms")
            timings = timings if isinstance(timings, dict) else {}
            per_gpu = scope.get("per_gpu")
            per_gpu = per_gpu if isinstance(per_gpu, dict) else {}

            def p95(
                timing_source: dict[str, object],
                *names: str,
            ) -> str:
                values = []
                for name in names:
                    row = timing_source.get(name)
                    if isinstance(row, dict) and isinstance(
                        row.get("p95"), (int, float)
                    ):
                        values.append(float(row["p95"]))
                return (
                    f"{max(values):.0f}"
                    if values
                    else "—"
                )

            window_start = scope.get("window_start")
            window_end = scope.get("window_end")
            window_text = (
                f"w{int(window_start)}–{int(window_end)}"
                if isinstance(window_start, int)
                and isinstance(window_end, int)
                else "—"
            )
            release_id = str(scope.get("release_id") or "")
            if scope_name == "active_profile":
                scope_label = "active profile"
            elif scope_name == "release":
                scope_label = f"historical v{historical_protocol} · release {release_id[:10]}"
            elif scope_name == "latest_six":
                scope_label = f"historical v{historical_protocol} · latest 6 completed"
            else:
                scope_label = f"historical v{historical_protocol} · lifetime"
            for gpu, gpu_row in per_gpu.items():
                if not isinstance(gpu_row, dict):
                    continue
                gpu_timings = gpu_row.get("timings_ms")
                gpu_timings = (
                    gpu_timings
                    if isinstance(gpu_timings, dict) and gpu_timings
                    else timings
                )
                gpu_quota = gpu_row.get("quota")
                gpu_quota = (
                    gpu_quota
                    if isinstance(gpu_quota, dict)
                    else quota
                )
                quota_occupancy = gpu_quota.get("peak_occupancy")
                quota_text = (
                    f"{100.0 * float(quota_occupancy):.0f}%"
                    if isinstance(quota_occupancy, (int, float))
                    else "—"
                )
                quota_peak = cell(gpu_quota.get("peak_effective"))
                quota_capacity = cell(
                    gpu_quota.get("capacity_per_hotkey")
                )
                quota_drops = cell(
                    gpu_quota.get("dropped_solely_for_quota")
                )
                comparison_rows.append(f"""
<tr>
  <td><span class="lbl" style="color:{box.color}">{html.escape(box.label)}</span></td>
  <td class="mono">{html.escape(scope_label)}
    <div class="dim small">{html.escape(window_text)}</div>
  </td>
  <td class="mono small" title="{html.escape(str(gpu))}">{html.escape(str(gpu)[-12:])}</td>
  <td class="num">{cell(gpu_row.get("generated_natural_m8"))}</td>
  <td class="num">{cell(gpu_row.get("raw_k2"))}</td>
  <td class="num">{cell(gpu_row.get("malformed_k2"))}/{cell(gpu_row.get("distribution_dropped_k2"))}</td>
  <td class="num">{cell(gpu_row.get("exact_preflight_passing_k2"))}</td>
  <td class="num">{cell(gpu_row.get("precommit_accepted"))}</td>
  <td class="num">{cell(gpu_row.get("pool_accepted"))}</td>
  <td class="num">{cell(gpu_row.get("selected"))}</td>
  <td class="num">{cell(gpu_row.get("rewarded"))}</td>
  <td class="num">{p95(gpu_timings, "grading", "local_gate")}</td>
  <td class="num">{p95(gpu_timings, "proof", "local_proof")}</td>
  <td class="num">{p95(gpu_timings, "transport", "precommit", "reveal", "provisional")}</td>
  <td class="num">{html.escape(quota_text)}
    <div class="dim small">peak {quota_peak}/{quota_capacity} · drops {quota_drops}</div>
  </td>
  <td class="num">{float(gpu_row.get("physical_gpu_hours") or 0.0):.3f}</td>
  <td class="num">{float(gpu_row.get("selected_slots_per_physical_gpu_hour") or 0.0):.3f}</td>
  <td class="num">{float(gpu_row.get("rewarded_slots_per_physical_gpu_hour") or 0.0):.3f}</td>
</tr>""")
    comparison_panel = (
        f"""
<div class="panel">
  <h2>Comparable GPU yield <span class="dim small">active profile + explicitly historical scopes</span></h2>
  <table class="grid compact pipeline-table">
    <thead><tr>
      <th>miner</th><th>scope / windows</th><th>physical GPU</th>
      <th class="num">natural M=8</th><th class="num">raw k2</th>
      <th class="num">malformed/dist.</th><th class="num">exact k2</th>
      <th class="num">precommit</th><th class="num">pool</th>
      <th class="num">selected</th><th class="num">rewarded</th>
      <th class="num">grade p95 ms</th><th class="num">proof p95 ms</th>
      <th class="num">transport p95 ms</th><th class="num">quota</th>
      <th class="num">GPUh</th><th class="num">selected/GPUh</th>
      <th class="num">rewarded/GPUh</th>
    </tr></thead>
    <tbody>{''.join(comparison_rows)}</tbody>
  </table>
</div>
"""
        if comparison_rows
        else ""
    )
    return f"""
<div class="panel">
  <h2>Standalone mining truth <span class="dim small">atomic miner telemetry · fail-stale</span></h2>
  <table class="grid compact pipeline-table">
    <thead><tr>
      <th>worker / live unit</th><th>snapshot</th><th class="num">window</th>
      <th>hotkey</th><th class="num">UID</th><th>operator</th>
      <th>miner / validator source</th><th>checkpoint</th><th>profile truth</th>
      <th class="num">attempts</th><th class="num">natural M=8</th>
      <th class="num">eligible</th><th class="num">precommit</th>
      <th class="num">reveal</th><th class="num">pool</th>
      <th class="num">network proof</th><th class="num">selected</th>
      <th class="num">rewarded</th><th class="num">terminal/pending</th>
      <th class="num">physical GPUh</th><th class="num">selected/GPUh</th>
      <th class="num">rewarded/GPUh</th>
    </tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</div>
{comparison_panel}"""


def render_pipeline_html() -> str:
    """Per-service fresh-miner pipeline table, including RTX8 shards."""
    with _lock:
        boxes = list(_boxes)
        vs = _vs

    if not boxes:
        return "<div class='panel'><h2>Pipeline monitor</h2><span class='dim'>warming…</span></div>"

    rows = []
    for b in boxes:
        certification = _active_standalone_certification(b)
        certification_gpu = certification.get("gpu", {})
        certification_gpu = (
            certification_gpu
            if isinstance(certification_gpu, dict)
            else {}
        )
        certification_identity = certification.get("identity", {})
        certification_identity = (
            certification_identity
            if isinstance(certification_identity, dict)
            else {}
        )
        code_auction = _box_code_auction_details(b, vs)
        crossover = _box_code_selector_crossover_details(b, vs)
        overlap_abba = _box_code_overlap_abba_details(b, vs)
        stage_title = ""
        if getattr(b, "quarantine_active", False):
            stage = "quarantined"
            stage_color = "var(--red)"
        elif certification:
            stage = "certifying"
            stage_color = "var(--yellow)"
            stage_title = (
                "exact standalone certification · submit-disabled · "
                f"phase={certification.get('phase') or 'transition'} · "
                f"runner pid={int(certification.get('runner_pid') or 0)} · "
                f"worker pid={int(certification.get('worker_pid') or 0)} · "
                f"GPU-bound={bool(certification.get('gpu_process_bound'))} · "
                "no active mining unit and no mining funnel"
            )
        elif (b.unit or b.unit_candidates) and not getattr(
            b, "env_file_ok", False
        ):
            stage = "config-error"
            stage_color = "var(--red)"
        elif not b.proc_alive:
            stage = "down"
            stage_color = "var(--red)"
        elif getattr(b, "standalone_telemetry_path", "") and not getattr(
            b, "standalone_fresh", False
        ):
            stage = "telemetry-stale"
            stage_color = "var(--red)"
            stage_title = str(
                getattr(b, "standalone_error", "") or "atomic snapshot stale"
            )
        elif getattr(b, "standalone_telemetry_path", ""):
            telemetry = getattr(b, "standalone_telemetry", {})
            telemetry = telemetry if isinstance(telemetry, dict) else {}
            lifecycle = telemetry.get("window_lifecycle")
            lifecycle = lifecycle if isinstance(lifecycle, dict) else {}
            lifecycle_status = str(lifecycle.get("status") or "").upper()
            if lifecycle_status == "STALLED":
                stage = "stalled"
                stage_color = "var(--red)"
                stage_title = "Math controller lifecycle is stalled"
            elif lifecycle_status == "RECOVERING":
                stage = "recovering"
                stage_color = "var(--yellow)"
                latest_run = lifecycle.get("latest_run")
                latest_run = (
                    latest_run if isinstance(latest_run, dict) else {}
                )
                stage_title = " · ".join(
                    part
                    for part in (
                        "Math controller lifecycle is recovering",
                        str(latest_run.get("failure_stage") or ""),
                        str(latest_run.get("failure_type") or ""),
                    )
                    if part
                )
            elif lifecycle_status == "READY":
                stage = "ready"
                stage_color = "var(--green)"
                stage_title = "fresh Math telemetry · ready for the next window"
            elif b.miner_state == "active_generation":
                stage = "generating"
                stage_color = "var(--green)"
                stage_title = (
                    "attested current-window generation is active · "
                    "submission funnel remains zero until atomic completion"
                )
            else:
                stage = "mining"
                stage_color = "var(--green)"
                stage_title = (
                    "fresh Math telemetry · active window"
                    if lifecycle_status == "ACTIVE"
                    else "fresh standalone atomic mining telemetry"
                )
        elif getattr(b, "miner_kind", "") == "reliquary_one":
            v5 = _box_v5_host_telemetry(b)
            events = v5.get("fleet_events", {})
            events = events if isinstance(events, dict) else {}
            if events.get("complete_30m") is True:
                stage = "v5-host-events"
                stage_color = "var(--green)"
                stage_title = (
                    "Per-host V5 fleet-events cover the window; accepted=True "
                    "rows are observed admissions only, and rejected outcomes "
                    "remain unavailable"
                )
            else:
                stage = "v5-unattributed"
                stage_color = "var(--yellow)"
                stage_title = (
                    "V5 host event coverage is incomplete; shared-hotkey "
                    "validator totals remain explicitly unattributable"
                )
        elif code_auction["applicable"] and not code_auction["ok"]:
            stage = "auction-blocked"
            stage_color = "var(--red)"
            stage_title = " · ".join(
                _readiness_issue_label(str(issue))
                for issue in code_auction.get("issues", [])
            )
        elif crossover["configured"] and not crossover["ok"]:
            stage = "selector-blocked"
            stage_color = "var(--red)"
            stage_title = " · ".join(
                _readiness_issue_label(
                    f"code_selector_crossover:{issue}"
                )
                for issue in crossover.get("issues", [])
            )
        elif overlap_abba["configured"] and not overlap_abba["ok"]:
            stage = "selector-blocked"
            stage_color = "var(--red)"
            stage_title = " · ".join(
                _readiness_issue_label(f"code_overlap_abba:{issue}")
                for issue in overlap_abba.get("issues", [])
            )
        elif b.batch_filled_30m and b.acpt_30m == 0:
            stage = "late"
            stage_color = "var(--red)"
        elif b.miner_submitted_this_win > 0 or b.burst_30m > 0:
            stage = "firing"
            stage_color = "var(--green)"
        elif getattr(b, "reference_ready", False) and b.gpu_util > 60:
            stage = "generating"
            stage_color = "var(--green)"
        elif getattr(b, "reference_ready", False) or b.miner_ready > 0:
            stage = "ready"
            stage_color = "var(--cyan)"
        elif b.gpu_util > 60 or b.miner_inflight > 0:
            stage = "building"
            stage_color = "var(--yellow)"
        else:
            stage = "idle"
            stage_color = "var(--dim)"

        hk = _short_hotkey(b.hotkey)
        environment = (
            str(certification_identity.get("environment") or "—")
            if certification
            else getattr(b, "miner_environment", "") or "—"
        )
        engine_mode = (
            "certifier" if certification
            else getattr(b, "engine_mode", "") or "—"
        )
        protocol = getattr(b, "protocol_profile", "") or "—"
        parity = (
            "exact evidence"
            if certification
            else "protocol+runtime"
            if getattr(b, "protocol_profile", "") and getattr(b, "runtime_parity_ok", False)
            else "protocol only"
            if getattr(b, "protocol_profile", "")
            else "pending"
        )
        parity_color = (
            "var(--yellow)" if parity == "exact evidence" else
            "var(--green)" if parity == "protocol+runtime" else
            "var(--yellow)" if parity == "protocol only" else
            "var(--dim)"
        )
        source_revs = "/".join(
            rev[:8] for rev in (
                str(
                    certification_identity.get(
                        "miner_source_revision"
                    ) or ""
                )
                if certification
                else getattr(b, "miner_source_revision", ""),
                str(
                    certification_identity.get(
                        "validator_source_revision"
                    ) or ""
                )
                if certification
                else getattr(b, "reliquary_source_revision", ""),
            ) if rev
        ) or "—"
        display_gpu_util = (
            int(certification_gpu.get("utilization_pct") or 0)
            if certification
            else b.gpu_util
        )
        display_gpu_mem_mb = (
            int(certification_gpu.get("memory_used_mb") or 0)
            if certification
            else b.gpu_mem_mb
        )
        display_gpu_total_mb = (
            int(certification_gpu.get("memory_total_mb") or 1)
            if certification
            else b.gpu_total_mb
        )
        final_accept_value = getattr(b, "final_accept_30m", b.acpt_30m)
        final_reject_value = getattr(b, "final_reject_30m", b.rej_30m)
        final_accept = (
            int(final_accept_value)
            if isinstance(final_accept_value, int)
            and not isinstance(final_accept_value, bool)
            else None
        )
        final_reject = (
            int(final_reject_value)
            if isinstance(final_reject_value, int)
            and not isinstance(final_reject_value, bool)
            else None
        )
        final_accept_text = "—" if final_accept is None else str(final_accept)
        final_reject_text = "—" if final_reject is None else str(final_reject)
        live_window = b.miner_window
        if (
            getattr(b, "reference_ready", False)
            and b.miner_state in {"ready", "generating", "proving", "waiting"}
            and vs.window
        ):
            live_window = vs.window
        current_abba = overlap_abba.get("current")
        if isinstance(current_abba, list) and current_abba:
            arm_parts = []
            detail_parts = []
            for lane in current_abba:
                slot = int(lane.get("shard_slot", 0) or 0)
                latest = lane.get("latest_attempt")
                latest = latest if isinstance(latest, dict) else {}
                itt = str(latest.get("itt_arm") or "?")
                execution = str(latest.get("execution_arm") or "?")
                arm = itt[:1].upper()
                if execution != itt:
                    arm += "→" + execution[:1].upper()
                arm_parts.append(f"s{slot}:{arm}")
                fallbacks = lane.get("fallback_counts")
                fallbacks = (
                    fallbacks if isinstance(fallbacks, dict) else {}
                )
                fallback_text = ",".join(
                    f"{reason}={count}"
                    for reason, count in sorted(fallbacks.items())
                ) or "none"
                detail_parts.append(
                    f"s{slot} window attempts="
                    f"{int(lane.get('attempt_count', 0) or 0)} "
                    f"activations="
                    f"{int(lane.get('activation_count', 0) or 0)} "
                    f"selected-overlap="
                    f"{int(lane.get('selected_overlap_count', 0) or 0)} "
                    f"fallbacks={fallback_text} · latest attempt: "
                    f"ITT={itt} execution={execution} "
                    f"policy={latest.get('selector_policy') or '—'} "
                    f"overlap={latest.get('overlap_candidate_count', 0)} "
                    f"selected={bool(latest.get('selected_overlap'))} "
                    f"activated={bool(latest.get('activated'))} "
                    f"fallback={latest.get('fallback_reason') or 'none'}"
                )
            contract = overlap_abba.get("contract")
            contract = contract if isinstance(contract, dict) else {}
            selector_label = "ABBA " + " ".join(arm_parts)
            selector_color = "var(--green)"
            selector_title = (
                f"{contract.get('experiment_id') or 'AB/BA'} · "
                f"contract={contract.get('sha256') or '—'} · "
                f"windows={contract.get('start_window', '—')}-"
                f"{contract.get('end_window', '—')} · "
                + " · ".join(detail_parts)
            )
        elif overlap_abba["configured"] and overlap_abba["ok"]:
            contract = overlap_abba.get("contract")
            contract = contract if isinstance(contract, dict) else {}
            selector_label = (
                "ABBA "
                + str(overlap_abba.get("current_status") or "pending")
            )
            selector_color = "var(--yellow)"
            selector_title = (
                f"{contract.get('experiment_id') or 'AB/BA'} · "
                f"contract={contract.get('sha256') or '—'} · "
                f"windows={contract.get('start_window', '—')}-"
                f"{contract.get('end_window', '—')}"
            )
        elif overlap_abba["configured"]:
            selector_label = "ABBA invalid"
            selector_color = "var(--red)"
            selector_title = " · ".join(
                str(issue) for issue in overlap_abba.get("issues", [])
            )
        else:
            current_crossover = crossover.get("current")
            if isinstance(current_crossover, dict):
                selector_label = str(
                    current_crossover.get("execution_arm") or "unknown"
                )
                selector_color = (
                    "var(--green)"
                    if selector_label == "treatment"
                    else "var(--cyan)"
                )
                selector_title = (
                    f"CURRENT exact validator window "
                    f"{int(current_crossover.get('validator_window', 0) or 0)} · "
                    f"ITT={current_crossover.get('itt_arm') or '—'} · "
                    f"execution={selector_label} · "
                    f"validation={current_crossover.get('validation_status') or '—'}"
                )
            elif crossover["configured"] and crossover["ok"]:
                selector_label = str(
                    crossover.get("current_status") or "pending"
                )
                selector_color = "var(--yellow)"
                selector_title = (
                    "Crossover artifacts are valid; no exact fresh OPEN "
                    "validator window has a matching persisted decision. "
                    "Historical arms are intentionally not painted as current."
                )
            elif crossover["configured"]:
                selector_label = "invalid"
                selector_color = "var(--red)"
                selector_title = " · ".join(
                    str(issue) for issue in crossover.get("issues", [])
                )
            else:
                selector_label = "off"
                selector_color = "var(--dim)"
                selector_title = "No process-bound selector experiment"
        rows.append(f"""
<tr>
  <td><span class="lbl" style="color:{b.color}">{html.escape(b.label)}</span></td>
  <td class="mono dim">{html.escape(hk)}</td>
  <td class="mono" style="color:{stage_color}" title="{html.escape(stage_title)}">{stage}</td>
  <td class="mono" title="service configuration: {html.escape(getattr(b, 'env_file_error', '') or 'readable')}">{html.escape(environment)}</td>
  <td class="mono dim">{html.escape(engine_mode)}</td>
  <td class="mono" style="color:{parity_color}" title="profile={html.escape(protocol)}">{html.escape(parity)}</td>
  <td class="mono" style="color:{selector_color}" title="{html.escape(selector_title)}">{html.escape(selector_label)}</td>
  <td class="num">{display_gpu_util}%</td>
  <td class="num dim">{display_gpu_mem_mb//1024}/{display_gpu_total_mb//1024} GB</td>
  <td class="num">{b.miner_ready}</td>
  <td class="num dim">{b.miner_inflight}</td>
  <td class="num">{b.miner_submitted_this_win}</td>
  <td class="num" title="V5 rows show observed host-local admissions as accepted/—; fleet-events does not provide a complete rejected-outcome source. Otherwise shared-hotkey /verdicts totals are not assigned to a host. Canonical selection/reward remain R2-only.">{final_accept_text}/{final_reject_text}</td>
  <td class="num dim">{b.fresh_built_30m}</td>
  <td class="num dim">{b.cache_fwd_30m}/{b.prefinalized_30m}</td>
  <td class="num" style="color:{'var(--yellow)' if b.late_grace_30m else 'var(--dim)'}">{b.late_grace_30m}</td>
  <td class="num" style="color:{'var(--red)' if b.batch_filled_30m else 'var(--dim)'}">{b.batch_filled_30m}</td>
  <td class="num dim">{b.last_fresh_total_s:.1f}s</td>
  <td class="mono dim">{html.escape((b.miner_state or '?')[:8])} w{live_window or '—'}</td>
  <td class="mono dim" title="miner-pro/reliquary source revisions">{html.escape(source_revs)}</td>
</tr>""")

    standalone = render_standalone_mining_truth_html()
    return standalone + f"""
<div class="panel">
  <h2>GPU/reference pipeline <span class="dim small">parity · generation · pool/proof verdicts</span></h2>
  <table class="grid compact pipeline-table">
    <thead><tr>
      <th>worker</th><th>hotkey</th><th>stage</th><th>env</th><th>engine</th><th>parity</th><th title="Current only when a fresh exact OPEN validator window matches persisted process-bound selector assignments">selector</th><th class="num">util</th>
      <th class="num">mem</th><th class="num">ready</th><th class="num">inflight</th>
      <th class="num">sent</th><th class="num">pool a/r</th><th class="num">fresh/30m</th><th class="num">cache/pre</th>
      <th class="num">grace</th><th class="num">batch_fill</th><th class="num">last build</th><th>state</th><th>source revs</th>
    </tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</div>
"""


def render_window_forensics_html() -> str:
    """Recent windows in a shape aimed at explaining sudden drops."""
    with _lock:
        windows = list(_windows)[:36]

    if not windows:
        return "<div class='panel'><h2>Window forensics</h2><span class='dim'>waiting for R2 windows…</span></div>"

    completed = [
        w for w in windows if getattr(w, "window_status", "completed") == "completed"
    ]
    aborted_count = len(windows) - len(completed)
    total_ours = sum(w.ours for w in completed)
    total_slots = sum(w.total_batch for w in completed)
    total_slot_pct = _share_pct(total_ours, total_slots)
    total_slot_color, total_slot_tone = _window_share_tone(total_slot_pct, total_slots)
    total_reward_ours = sum(getattr(w, "reward_ours", 0.0) for w in completed)
    total_reward = sum(getattr(w, "reward_total", 0.0) for w in completed)
    total_reward_pct = _share_pct(total_reward_ours, total_reward)
    total_reward_color, total_reward_tone = _window_share_tone(total_reward_pct, total_reward)

    rows = []
    for w in windows:
        if getattr(w, "window_status", "completed") == "aborted":
            failure = "/".join(
                value
                for value in (
                    str(getattr(w, "failure_stage", "") or ""),
                    str(getattr(w, "failure_type", "") or ""),
                )
                if value
            ) or "unspecified"
            rows.append(f"""
<tr>
  <td class="mono">w{w.n}</td>
  <td colspan="5"><span style="color:var(--red);font-weight:700">aborted</span>
    <span class="dim small">{html.escape(failure)} · terminal archive, no reward/training evidence</span></td>
</tr>""")
            continue
        pct = _share_pct(w.ours, w.total_batch)
        pct_label = _fmt_pct(pct, w.total_batch)
        color, slot_tone = _window_share_tone(pct, w.total_batch)
        reward = getattr(w, "reward_ours", 0.0)
        reward_total = getattr(w, "reward_total", 0.0)
        reward_pct = _share_pct(reward, reward_total)
        reward_label = _fmt_pct(reward_pct, reward_total)
        reward_color, reward_tone = _window_share_tone(reward_pct, reward_total)
        bar_width = max(0.0, min(100.0, pct))
        rej = " ".join(
            f"<span style='color:var(--red)'>{html.escape(k)}={v}</span>"
            for k, v in sorted(w.rejects.items(), key=lambda kv: -kv[1])[:3]
        ) or "<span class='dim'>—</span>"
        rows.append(f"""
<tr>
  <td class="mono">w{w.n}</td>
  <td class="num" style="color:{color};font-weight:700" title="{slot_tone} slot share">{w.ours}/{w.total_batch} <span class="dim small">{pct_label}</span></td>
  <td class="num" style="color:{reward_color};font-weight:700" title="{reward_tone} reward share">{reward_label}</td>
  <td><div class="mini-meter share-meter" title="slot share {pct_label} · {slot_tone}"><span style="width:{bar_width:.0f}%;background:{color}"></span></div></td>
  <td class="num dim">{w.rt_first:.1f}s</td>
  <td>{rej}</td>
</tr>""")

    streak_zero = 0
    for w in windows:
        if getattr(w, "window_status", "completed") == "aborted":
            continue
        if w.ours == 0:
            streak_zero += 1
        else:
            break
    note = (
        f"<span style='color:var(--share-zero)'>current R2 zero-selected-slot streak: {streak_zero} window(s)</span>"
        if streak_zero else
        "<span style='color:var(--share-good)'>latest sealed window has our slots</span>"
    )
    summary_parts = [
        f"<span class='share-stat'>slot share "
        f"<b style='color:{total_slot_color}'>{total_ours}/{total_slots} = {_fmt_pct(total_slot_pct, total_slots)}</b> "
        f"<em>{total_slot_tone}</em></span>",
        f"<span class='share-stat'>reward share "
        f"<b style='color:{total_reward_color}'>{_fmt_pct(total_reward_pct, total_reward)}</b> "
        f"<em>{total_reward_tone}</em></span>",
        f"<span class='share-legend'>{_share_legend_html()}</span>",
    ]
    if aborted_count:
        summary_parts.append(
            f"<span class='share-stat'><b style='color:var(--red)'>{aborted_count}</b> "
            "aborted terminal(s), excluded</span>"
        )
    summary = " ".join(summary_parts)
    return f"""
<div class="panel">
  <h2>Window forensics <span class="dim small">R2 sealed-window truth</span></h2>
  <div class="window-share-summary">{summary}</div>
  <div class="kpi-row" style="margin-bottom:8px">{note}</div>
  <table class="grid compact">
    <thead><tr><th>win</th><th class="num">slots</th><th class="num">reward %</th><th>slot share</th><th class="num">first rt</th><th>our rejects</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</div>
"""


def render_fleet_summary_html() -> str:
    """Compact operator overview: fleet health + mission state in one table."""
    with _lock:
        boxes = list(_boxes)
        vs = _vs
        windows = list(_windows)
        chain_hotkeys = {getattr(h, "hotkey", "") for h in _chain.hotkeys}
        chain_seen = bool(_chain.last_fetch_at)
    if not boxes:
        return "<div class='panel'><h2>Ops overview</h2><span class='dim'>warming…</span></div>"

    targets = _configured_target_rows(chain_hotkeys, chain_seen)
    target_hotkeys = {str(t["hotkey"]) for t in targets}
    target_boxes = [b for b in boxes if b.hotkey in target_hotkeys]
    target_acpt_30m = _sum_available_counts(
        b.acpt_30m for b in target_boxes
    )
    target_reject_30m = _sum_available_counts(
        (
            None
            if b.rej_30m is None
            else b.rej_30m + b.late_drops_30m
        )
        for b in target_boxes
    )
    primary_target = targets[0] if targets else None
    if primary_target:
        target_label = str(primary_target["label"])
        target_hotkey = str(primary_target["hotkey"])
        target_aliases = [str(x) for x in primary_target.get("aliases", [])]
        target_units = [str(x) for x in primary_target.get("units", [])]
        target_sub = target_aliases[0] if target_aliases else "configured target"
        if target_units:
            target_sub = f"{target_sub} · {target_units[0]}"
        target_prefix = _short_hotkey(target_hotkey)
        registered = primary_target.get("registered")
        if registered is True:
            target_status = "registered"
            target_status_color = "var(--green)"
        elif registered is False:
            target_status = "not registered"
            target_status_color = "var(--yellow)"
        else:
            target_status = "chain warming"
            target_status_color = "var(--dim)"
    else:
        target_label = "none"
        target_hotkey = ""
        target_sub = "no owned hotkey configured"
        target_prefix = "—"
        target_status = "missing"
        target_status_color = "var(--red)"

    total_acpt_30m = _sum_available_counts(b.acpt_30m for b in boxes)
    total_acpt_60m = _sum_available_counts(b.acpt_60m for b in boxes)
    total_oom_60m = sum(b.oom_60m for b in boxes)
    total_wm = sum(b.window_mismatch_30m for b in boxes)
    total_alive = sum(1 for b in boxes if b.proc_alive)
    total_ready = sum(b.miner_ready for b in boxes if b.proc_alive)
    total_inflight = sum(b.miner_inflight for b in boxes if b.proc_alive)
    total_submitted = sum(b.miner_submitted_this_win for b in boxes if b.proc_alive)
    total_fresh = sum(b.fresh_built_30m for b in boxes)
    total_cache = sum(b.cache_fwd_30m + b.prefinalized_30m for b in boxes)
    total_batch_filled = sum(b.batch_filled_30m for b in boxes)

    readiness_by_box = [
        (box, _box_readiness_issues(box, validator_state=vs))
        for box in boxes
    ]
    unready_boxes = [
        (box, issues) for box, issues in readiness_by_box if issues
    ]
    ready_boxes = len(boxes) - len(unready_boxes)
    readiness_issues = [
        issue for _box, issues in unready_boxes for issue in issues
    ]
    critical_readiness = any(
        issue == "down"
        or issue == "quarantined"
        or issue.startswith("probe:")
        or issue.startswith("unit_resolution:")
        or issue.startswith("env_file:")
        or issue.startswith("code_auction:")
        for issue in readiness_issues
    )

    valid_ages = [b.last_oom_age_s for b in boxes if b.last_oom_age_s >= 0 and b.last_oom_age_s < 999_000]
    streak_s = min(valid_ages) if valid_ages else 999_000
    streak_color = (
        "var(--red)" if streak_s < 300 else
        "var(--yellow)" if streak_s < 3600 else
        "var(--green)"
    )

    if ready_boxes == len(boxes) and total_oom_60m == 0:
        health_color = "var(--green)"
        health_lbl = "HEALTHY"
    elif critical_readiness:
        health_color = "var(--red)"
        health_lbl = "ALERT"
    else:
        health_color = "var(--yellow)"
        health_lbl = "DEGRADED"
    health_sub = f"ready {ready_boxes}/{len(boxes)} · alive {total_alive}/{len(boxes)}"

    live_valid, proof_target, _live_source = _live_valid_context(vs, windows)
    valid_color, _valid_tone = _window_share_tone(
        _share_pct(min(live_valid, proof_target), proof_target), proof_target
    )
    cur_events = _mission_current_events()
    our_accepts = [e for e in cur_events if e.kind == "accept" and e.ours]
    all_accepts = [e for e in cur_events if e.kind == "accept"]
    our_rejects = [e for e in cur_events if e.kind == "reject" and e.ours]
    our_late = [e for e in cur_events if e.kind == "late_drop" and e.ours]

    state_via_ssh = vs.error == "ssh-state"
    if vs.error and not state_via_ssh:
        validator_main = f"<span style='color:var(--red)' title='{html.escape(vs.error)}'>unreachable</span>"
        validator_sub = f"last w{vs.window or '—'}"
    else:
        validator_main = (
            f"<span style='color:var(--window-id)'>w{vs.window or '—'}</span> "
            f"<span style='color:var(--cyan)'>{html.escape(vs.state or '?')}</span>"
        )
        validator_sub = "ssh fallback" if state_via_ssh else "live /state"

    quarantined_count = sum(
        1 for box, _issues in readiness_by_box
        if getattr(box, "quarantine_active", False)
    )
    if total_alive < len(boxes):
        attention = f"{len(boxes) - total_alive} box down"
        attention_color = "var(--red)"
    elif quarantined_count:
        attention = f"{quarantined_count} quarantined"
        attention_color = "var(--red)"
    elif unready_boxes:
        attention = (
            f"{len(unready_boxes)} unready · "
            f"{_readiness_issue_label(unready_boxes[0][1][0])}"
        )
        attention_color = (
            "var(--red)" if critical_readiness else "var(--yellow)"
        )
    elif total_oom_60m:
        attention = f"{total_oom_60m} OOM"
        attention_color = "var(--red)"
    elif total_wm:
        attention = f"{total_wm} window mismatch"
        attention_color = "var(--yellow)"
    elif total_batch_filled:
        attention = f"{total_batch_filled} batch_filled"
        attention_color = "var(--yellow)"
    else:
        attention = "clear"
        attention_color = "var(--green)"

    if not any(b.proc_alive for b in boxes):
        mission_note = "no submitters"
        mission_color = "var(--red)"
    elif our_accepts:
        mission_note = "landing"
        mission_color = "var(--green)"
    elif live_valid >= proof_target:
        mission_note = "batch full"
        mission_color = "var(--yellow)"
    elif total_ready or total_inflight:
        mission_note = "ready"
        mission_color = "var(--cyan)"
    else:
        mission_note = "building"
        mission_color = "var(--yellow)"

    def cell(main: str, sub: str = "", *, color: str = "var(--bone)") -> str:
        sub_html = f"<div class='overview-sub'>{sub}</div>" if sub else ""
        return (
            f"<div class='overview-main' style='color:{color}'>{main}</div>"
            f"{sub_html}"
        )

    def count_text(value: int | None) -> str:
        return "—" if value is None else str(value)

    verdict_watch_warning = str(getattr(vs, "verdicts_warning", "") or "")
    verdict_watch_html = (
        "<div class='yellow small' role='status' style='margin-top:7px'>"
        + html.escape(verdict_watch_warning)
        + "; fleet hotkeys are prioritized"
        + "</div>"
        if verdict_watch_warning
        else ""
    )

    return f"""
<div class="panel overview-panel">
  <h2>Ops overview <span class="dim small">fleet + mission</span></h2>
  <table class="grid compact overview-table">
    <thead><tr><th>area</th><th>status</th><th>now</th><th>30m</th><th>attention</th></tr></thead>
    <tbody>
      <tr>
        <td class="lbl">target</td>
        <td>{cell(html.escape(target_label), html.escape(target_sub), color="var(--amber)" if primary_target else "var(--red)")}</td>
        <td title="{html.escape(target_hotkey)}">{cell(html.escape(target_prefix), "active hotkey", color="var(--bone)")}</td>
        <td>{cell(f"{count_text(target_acpt_30m)}/{count_text(target_reject_30m)}", "pool/reject+late", color="var(--green)" if target_acpt_30m else "var(--dim)")}</td>
        <td>{cell(target_status, "", color=target_status_color)}</td>
      </tr>
      <tr>
        <td class="lbl">fleet</td>
        <td>{cell(health_lbl, health_sub, color=health_color)}</td>
        <td>{cell(fmt_age(streak_s), "OOM-free", color=streak_color)}</td>
        <td>{cell(count_text(total_acpt_30m), f"pool/30m · {count_text(total_acpt_60m)}/60m", color="var(--green)" if total_acpt_30m else "var(--dim)")}</td>
        <td>{cell(html.escape(attention), "", color=attention_color)}</td>
      </tr>
      <tr>
        <td class="lbl">validator</td>
        <td>{validator_main}<div class="overview-sub">{validator_sub}</div></td>
        <td>{cell(_valid_pressure_label(live_valid, proof_target), "valid", color=valid_color)}</td>
        <td>{cell(f"{len(our_accepts)}/{len(all_accepts)}", "ours/all pool accepts", color="var(--share-good)" if our_accepts else "var(--dim)")}</td>
        <td>{cell(mission_note, "", color=mission_color)}</td>
      </tr>
      <tr>
        <td class="lbl">pipeline</td>
        <td>{cell(str(total_ready), "ready", color="var(--cyan)" if total_ready else "var(--dim)")}</td>
        <td>{cell(f"{total_inflight}/{total_submitted}", "inflight/submitted")}</td>
        <td>{cell(f"{total_fresh}/{total_cache}", "fresh/cache")}</td>
        <td>{cell(f"{len(our_rejects)}/{len(our_late)}", "reject/late", color="var(--red)" if (our_rejects or our_late) else "var(--dim)")}</td>
      </tr>
    </tbody>
  </table>
  {verdict_watch_html}
</div>
"""


def _fmt_latency_ms(value: object) -> str:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return "—"
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        return "—"
    if parsed < 1:
        return f"{parsed:.3f}"
    if parsed < 100:
        return f"{parsed:.1f}"
    return f"{parsed:,.0f}"


def _fmt_optional_counter(value: object) -> str:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        return "—"
    return f"{value:,}"


def render_validator_liveness_html(
    vs: ValidatorState | None = None,
    windows: list | None = None,
) -> str:
    """Render auction ingress, seal and archive continuity diagnostics."""
    if vs is None or windows is None:
        with _lock:
            vs = _vs if vs is None else vs
            windows = list(_windows) if windows is None else list(windows)
    details = _validator_liveness_details(vs, windows)
    archive = details["archive"]
    if not details["reported"] and not archive["reported"]:
        return (
            "<div class='dim small' style='margin-top:10px'>"
            "auction-v2 liveness telemetry not reported by this validator build"
            "</div>"
        )

    latency_rows = []
    event_loop = details["event_loop_lag_ms"]
    if isinstance(event_loop, dict) and event_loop:
        latency_rows.append(("event loop", event_loop))
    endpoints = details["endpoint_latency_ms"]
    if isinstance(endpoints, dict):
        preferred = ("/health", "/state", "/submit/precommit", "/submit")
        names = [name for name in preferred if name in endpoints]
        names.extend(sorted(name for name in endpoints if name not in names))
        latency_rows.extend(
            (name, endpoints[name])
            for name in names
            if isinstance(endpoints.get(name), dict)
        )
    latency_html = "".join(
        "<tr>"
        f"<td class='mono'>{html.escape(str(name))}</td>"
        f"<td class='num mono'>{_fmt_latency_ms(values.get('p50'))}</td>"
        f"<td class='num mono'>{_fmt_latency_ms(values.get('p95'))}</td>"
        f"<td class='num mono'>{_fmt_latency_ms(values.get('p99'))}</td>"
        f"<td class='num mono'>{_fmt_latency_ms(values.get('max'))}</td>"
        "</tr>"
        for name, values in latency_rows
    )
    if not latency_html:
        latency_html = (
            "<tr><td colspan='5' class='dim'>latency samples warming…</td></tr>"
        )

    environment_rows = []
    by_environment = details["by_environment"]
    if isinstance(by_environment, dict):
        for environment, env_details in sorted(by_environment.items()):
            if not isinstance(env_details, dict):
                continue
            admission = env_details.get("admission_latency_ms")
            admission = admission if isinstance(admission, dict) else {}

            def pair(metric_name: str) -> str:
                metric = admission.get(metric_name)
                if not isinstance(metric, dict):
                    return "—"
                return (
                    f"{_fmt_latency_ms(metric.get('p95'))}/"
                    f"{_fmt_latency_ms(metric.get('p99'))}"
                )

            commit = admission.get("commit_lock_wait_ms")
            commit = commit if isinstance(commit, dict) else {}
            drain = env_details.get("seal_drain")
            drain = drain if isinstance(drain, dict) else {}
            if drain:
                elapsed = drain.get("elapsed_seconds")
                elapsed_text = (
                    f"{float(elapsed):.1f}s"
                    if isinstance(elapsed, (int, float))
                    and not isinstance(elapsed, bool)
                    else "—"
                )
                timed_out = drain.get("timed_out")
                timeout_text = (
                    "timeout" if timed_out is True else
                    "ok" if timed_out is False else "?"
                )
                timeout_color = (
                    "var(--red)" if timed_out is True else
                    "var(--green)" if timed_out is False else "var(--dim)"
                )
                snapshot = "/".join(
                    _fmt_optional_counter(drain.get(name))
                    for name in (
                        "queue_depth_at_snapshot",
                        "inflight_workers_at_snapshot",
                        "pending_reservations_at_snapshot",
                        "inflight_reservations_at_snapshot",
                    )
                )
                drain_html = (
                    f"<span style='color:{timeout_color}'>{timeout_text}</span> "
                    f"<span class='mono'>{elapsed_text}</span>"
                    f"<div class='dim small mono' title='queue/workers/pending/inflight at snapshot'>{snapshot}</div>"
                )
            else:
                drain_html = "<span class='dim'>—</span>"
            environment_rows.append(
                "<tr>"
                f"<td class='mono'>{html.escape(str(environment))}</td>"
                f"<td class='num mono'>{_fmt_optional_counter(env_details.get('queue_depth'))}</td>"
                f"<td class='num mono'>{_fmt_optional_counter(env_details.get('admission_workers'))}</td>"
                f"<td class='num mono'>{_fmt_optional_counter(env_details.get('proof_verification_inflight'))}</td>"
                f"<td class='num mono'>{pair('queue_wait_ms')}</td>"
                f"<td class='num mono'>{pair('admission_prepare_ms')}</td>"
                f"<td class='num mono'>{_fmt_latency_ms(commit.get('p99'))}</td>"
                f"<td class='num mono'>{pair('total_ms')}</td>"
                f"<td>{drain_html}</td>"
                "</tr>"
            )
    environment_html = "".join(environment_rows) or (
        "<tr><td colspan='9' class='dim'>per-environment admission samples warming…</td></tr>"
    )

    last_gap = archive.get("last_enqueue_gap")
    gap_text = "—" if last_gap is None else json.dumps(
        last_gap, sort_keys=True, separators=(",", ":")
    )[:220]
    gap_total = archive.get("enqueue_gaps_total")
    gap_color = (
        "var(--red)"
        if isinstance(gap_total, int) and gap_total > 0
        else "var(--green)"
    )
    archive_html = f"""
    <div class='kpi-row' style='margin-top:8px'>
      <span class='kpi-mini'>archive queue <b>{_fmt_optional_counter(archive.get('queue_depth'))}</b></span>
      <span class='kpi-mini'>last enqueued <b>{_fmt_optional_counter(archive.get('last_enqueued_window'))}</b></span>
      <span class='kpi-mini'>last uploaded <b>{_fmt_optional_counter(archive.get('last_uploaded_window'))}</b></span>
      <span class='kpi-mini'>enqueued total <b>{_fmt_optional_counter(archive.get('archives_enqueued_total'))}</b></span>
      <span class='kpi-mini'>enqueue gaps <b style='color:{gap_color}'>{_fmt_optional_counter(gap_total)}</b></span>
      <span class='kpi-mini'>last completed <b>{_fmt_optional_counter(archive.get('last_completed_window'))}</b></span>
      <span class='kpi-mini'>last aborted <b>{_fmt_optional_counter(archive.get('last_aborted_window'))}</b></span>
    </div>
    <div class='mono dim small' style='margin-top:5px'>last enqueue gap: {html.escape(gap_text)}</div>
    """

    return f"""
  <div style='margin-top:12px'>
    <div class='small dim'>auction-v2 ingress liveness · latency values in ms</div>
    <table class='grid compact' style='margin-top:5px'>
      <thead><tr><th>surface</th><th class='num'>p50</th><th class='num'>p95</th><th class='num'>p99</th><th class='num'>max</th></tr></thead>
      <tbody>{latency_html}</tbody>
    </table>
    <table class='grid compact' style='margin-top:8px'>
      <thead><tr><th>environment</th><th class='num'>queue</th><th class='num'>workers</th><th class='num'>proof in-flight</th><th class='num'>queue p95/p99</th><th class='num'>prepare p95/p99</th><th class='num'>commit p99</th><th class='num'>total p95/p99</th><th>seal drain</th></tr></thead>
      <tbody>{environment_html}</tbody>
    </table>
    {archive_html}
  </div>
"""


def render_validator_html() -> str:
    with _lock:
        vs = _vs
        windows = list(_windows)
        poll_at = _last_poll_at
    live_valid, proof_target, live_source = _live_valid_context(vs, windows)
    source = "ssh fallback" if vs.error == "ssh-state" else "/state"
    state_age = int(time.time() - vs.last_fetch_at) if getattr(vs, "last_fetch_at", 0.0) else -1
    health_age = int(time.time() - vs.health_last_fetch_at) if getattr(vs, "health_last_fetch_at", 0.0) else -1
    verdict_age = int(time.time() - vs.verdicts_last_fetch_at) if getattr(vs, "verdicts_last_fetch_at", 0.0) else -1
    image_revision = getattr(vs, "image_revision", "") or ""
    runtime = getattr(vs, "runtime_fingerprint", {}) or {}
    app_started = float(getattr(vs, "app_started_at", 0.0) or 0.0)
    app_uptime = fmt_age(int(time.time() - app_started)) if app_started else "—"
    badge_health = _validator_badge_health_details(vs, poll_at=poll_at)
    badge_color = {
        "ok": "var(--green)",
        "warning": "var(--yellow)",
        "error": "var(--red)",
    }.get(str(badge_health["tone"]), "var(--yellow)")

    errors = []
    if vs.error and vs.error != "ssh-state":
        errors.append(f"state: {vs.error}")
    if getattr(vs, "health_error", ""):
        errors.append(f"health: {vs.health_error}")
    if getattr(vs, "verdicts_error", ""):
        errors.append(f"verdicts: {vs.verdicts_error}")
    error_html = (
        "<div class='red small' style='margin-top:7px'>" +
        html.escape(" · ".join(errors)) + "</div>"
        if errors else ""
    )

    env_rows = []
    targets = getattr(vs, "environment_targets", {}) or {}
    for env_name, env_data in sorted((getattr(vs, "window_environments", {}) or {}).items()):
        env_data = env_data if isinstance(env_data, dict) else {}
        valid = int(env_data.get("valid_submissions_count", env_data.get("valid_count", 0)) or 0)
        distinct = int(env_data.get("distinct_valid_prompt_count", 0) or 0)
        proof = int(env_data.get("proof_admission_count", 0) or 0)
        pending = int(env_data.get("pending_proof_reservations", 0) or 0)
        inflight = int(env_data.get("inflight_proof_reservations", 0) or 0)
        try:
            target = int(targets.get(env_name, 0) or 0)
        except (TypeError, ValueError):
            target = 0
        source_status = (getattr(vs, "prompt_sources", {}) or {}).get(env_name, {})
        source_state = source_status.get("status", "—") if isinstance(source_status, dict) else "—"
        env_rows.append(
            f"<tr><td class='mono'>{html.escape(str(env_name))}</td>"
            f"<td class='num'><b>{valid}/{target or '—'}</b></td>"
            f"<td class='num dim'>{distinct}</td><td class='num dim'>{proof}</td>"
            f"<td class='num dim'>{pending}/{inflight}</td>"
            f"<td class='mono dim'>{html.escape(str(source_state))}</td></tr>"
        )
    env_html = (
        "<table class='grid compact' style='margin-top:9px'>"
        "<thead><tr><th>environment</th><th class='num'>valid/target</th>"
        "<th class='num'>distinct</th><th class='num'>proof</th>"
        "<th class='num'>pending/inflight</th><th>prompt source</th></tr></thead>"
        f"<tbody>{''.join(env_rows)}</tbody></table>"
        if env_rows else "<div class='dim small' style='margin-top:8px'>per-environment health warming…</div>"
    )

    verdict_rows = []
    for hk, summary in sorted((getattr(vs, "verdicts_by_hotkey", {}) or {}).items()):
        if not isinstance(summary, dict):
            continue
        label = OUR_SS58.get(hk, hk[:12])
        acc30 = int(summary.get("accepted_30m", summary.get("accept_30m", 0)) or 0)
        rej30 = int(summary.get("rejected_30m", summary.get("reject_30m", 0)) or 0)
        acc60 = int(summary.get("accepted_60m", summary.get("accept_60m", 0)) or 0)
        rej60 = int(summary.get("rejected_60m", summary.get("reject_60m", 0)) or 0)
        finalized30 = int(summary.get("finalized_30m", 0) or 0)
        selected30 = int(summary.get("selected_30m", 0) or 0)
        rewarded30 = int(summary.get("rewarded_30m", 0) or 0)
        paid_not_trained30 = int(
            summary.get("rewarded_not_selected_30m", 0) or 0
        )
        reward_amount30 = summary.get("reward_amount_30m")
        effective_slots30 = summary.get("effective_full_slots_30m")
        emission30 = (
            "—"
            if reward_amount30 is None or effective_slots30 is None
            else f"{float(reward_amount30):.5f}/{float(effective_slots30):.2f}"
        )
        by_env_parts = []
        for env, env_counts in sorted((summary.get("by_environment") or {}).items()):
            if not isinstance(env_counts, dict):
                continue
            by_env_parts.append(
                f"{env}:{int(env_counts.get('accepted_30m', 0) or 0)}/"
                f"{int(env_counts.get('rejected_30m', 0) or 0)}"
            )
        by_env_text = " · ".join(by_env_parts) or "—"
        last = summary.get("last_verdict", summary.get("last", {})) or {}
        last_reason = (
            last.get("reject_reason") or last.get("reason") or "accepted"
            if isinstance(last, dict) and last else "—"
        )
        last_env = last.get("env_name", "—") if isinstance(last, dict) else "—"
        verdict_rows.append(
            f"<tr><td>{html.escape(str(label))}<div class='mono dim small'>{html.escape(hk[:12])}…</div></td>"
            f"<td class='num'><b style='color:var(--green)'>{acc30}</b>/{rej30}</td>"
            f"<td class='num dim'>{acc60}/{rej60}</td>"
            f"<td class='num dim'>{finalized30}/{selected30}/{rewarded30}</td>"
            f"<td class='num dim'>{html.escape(emission30)}</td>"
            f"<td class='num dim'>{paid_not_trained30}</td>"
            f"<td class='mono dim'>{html.escape(by_env_text)}</td>"
            f"<td class='mono dim'>{html.escape(str(last_env))}</td>"
            f"<td class='mono dim'>{html.escape(str(last_reason))}</td></tr>"
        )
    verdict_html = (
        "<div class='small dim' style='margin-top:10px'>authoritative /verdicts lifecycle · pool admission and seal-final outcomes · R2 is the durable archive</div>"
        "<table class='grid compact'><thead><tr><th>hotkey</th><th class='num'>30m pool a/r</th>"
        "<th class='num'>60m pool a/r</th><th class='num' title='finalized/selected/rewarded'>30m seal f/s/r</th>"
        "<th class='num' title='reward amount/effective full slots'>30m emission/slots</th>"
        "<th class='num'>paid not trained</th><th>30m by env pool a/r</th>"
        "<th>last env</th><th>last reason</th></tr></thead>"
        f"<tbody>{''.join(verdict_rows)}</tbody></table>"
        if verdict_rows else "<div class='dim small' style='margin-top:9px'>/verdicts pool/proof feed warming…</div>"
    )

    runtime_bits = " · ".join(
        bit for bit in (
            str(runtime.get("gpu_name", "") or ""),
            f"CUDA {runtime.get('cuda_version')}" if runtime.get("cuda_version") else "",
            f"torch {runtime.get('torch_version')}" if runtime.get("torch_version") else "",
            f"transformers {runtime.get('transformers_version')}" if runtime.get("transformers_version") else "",
            f"profile {str(runtime.get('profile_hash'))[:12]}" if runtime.get("profile_hash") else "",
        ) if bit
    ) or "runtime fingerprint unavailable"
    forced_seed = "on" if getattr(vs, "forced_seed_enforced", False) else "OFF"
    forced_cdf = "on" if getattr(vs, "forced_seed_cdf_enforced", False) else "off"
    shadow_envs = ",".join(getattr(vs, "difficulty_auction_shadow_environments", []) or []) or "—"
    latest_quarantine = (
        getattr(windows[0], "training_quarantine", {}) if windows else {}
    ) or {}
    training_quarantined = bool(
        latest_quarantine.get("quarantined")
        if isinstance(latest_quarantine, dict) else False
    )
    quarantine_reasons = ",".join(
        str(x) for x in (
            latest_quarantine.get("reasons", [])
            if isinstance(latest_quarantine, dict) else []
        )
    ) or "none"
    liveness_html = render_validator_liveness_html(vs, windows)
    return f"""
<div class='panel'>
  <h2>Validator <span class='small' style='color:{badge_color}'>{html.escape(str(badge_health['label']))}</span><span class='dim small'> · health {fmt_age(health_age)} ago</span></h2>
  <div>window <b style='color:var(--window-id)'>{vs.window or '—'}</b> ·
    state <b class='cyan'>{html.escape(vs.state or '?')}</b> ·
    valid <b class='magenta'>{_valid_pressure_label(live_valid, proof_target)}</b>
    <span class='dim'>· {html.escape(live_source)} · state {fmt_age(state_age)} ago via {html.escape(source)}</span>
  </div>
  <div class='kpi-row' style='margin-top:8px'>
    <span class='kpi-mini'>image <b class='mono'>{html.escape(image_revision[:12] or '—')}</b></span>
    <span class='kpi-mini'>app uptime <b>{app_uptime}</b></span>
    <span class='kpi-mini'>queue <b>{getattr(vs, 'queue_depth', 0)}</b></span>
    <span class='kpi-mini'>proof admission <b>{getattr(vs, 'proof_admission_count', 0)}/{getattr(vs, 'proof_admission_limit', 0) or '—'}</b></span>
    <span class='kpi-mini'>proof verify <b>{getattr(vs, 'proof_verification_inflight', 0)}</b></span>
    <span class='kpi-mini'>proof reservations <b>{getattr(vs, 'pending_proof_reservations', 0)}/{getattr(vs, 'inflight_proof_reservations', 0)}</b></span>
    <span class='kpi-mini'>archive queue <b>{getattr(vs, 'archive_queue_depth', 0)}</b></span>
  </div>
  <div class='mono dim small' style='margin-top:8px'>{html.escape(runtime_bits)}</div>
  <div class='mono dim small'>forced seed <b>{forced_seed}</b> · CDF <b>{forced_cdf}</b> · difficulty shadow <b>{str(bool(getattr(vs, 'difficulty_auction_shadow_enabled', False))).lower()}</b> [{html.escape(shadow_envs)}] · latest training quarantine <b style='color:{'var(--red)' if training_quarantined else 'var(--green)'}'>{str(training_quarantined).lower()}</b> [{html.escape(quarantine_reasons)}] · verdicts {fmt_age(verdict_age)} ago</div>
  {error_html}
  {env_html}
  {liveness_html}
  {verdict_html}
</div>
"""


def render_windows_html() -> str:
    with _lock:
        all_windows = list(_windows)
    if not all_windows:
        return "<div class='panel'><h2>Recent windows</h2><span class='dim'>R2 unavailable (set R2_ENDPOINT/R2_BUCKET)</span></div>"
    windows = all_windows[:24]
    score_context = all_windows[:72]
    rows = []
    completed = [
        w
        for w in score_context
        if getattr(w, "window_status", "completed") == "completed"
    ]
    aborted_count = len(score_context) - len(completed)
    total_ours = sum(w.ours for w in completed)
    total_slots = sum(w.total_batch for w in completed)
    pct = _share_pct(total_ours, total_slots)
    total_color, total_tone = _window_share_tone(pct, total_slots)
    total_reward_ours = sum(getattr(w, "reward_ours", 0.0) for w in completed)
    total_reward = sum(getattr(w, "reward_total", 0.0) for w in completed)
    reward_pct = _share_pct(total_reward_ours, total_reward)
    reward_color, reward_tone = _window_share_tone(reward_pct, total_reward)
    for w in windows:
        if getattr(w, "window_status", "completed") == "aborted":
            failure = "/".join(
                value
                for value in (
                    str(getattr(w, "failure_stage", "") or ""),
                    str(getattr(w, "failure_type", "") or ""),
                )
                if value
            ) or "unspecified"
            rows.append(
                f"<tr><td class='mono'>w{w.n}</td>"
                "<td><span style='color:var(--red);font-weight:700'>aborted</span> "
                f"<span class='dim small'>{html.escape(failure)}</span></td>"
                "<td class='num dim'>n/a</td><td class='num dim'>n/a</td>"
                "<td class='dim'>terminal; excluded</td></tr>"
            )
            continue
        pct_w = _share_pct(w.ours, w.total_batch)
        pct_label = _fmt_pct(pct_w, w.total_batch)
        share_color, share_tone = _window_share_tone(pct_w, w.total_batch)
        reward_w = getattr(w, "reward_ours", 0.0)
        reward_total_w = getattr(w, "reward_total", 0.0)
        reward_pct_w = _share_pct(reward_w, reward_total_w)
        reward_label = _fmt_pct(reward_pct_w, reward_total_w)
        reward_color_w, reward_tone_w = _window_share_tone(reward_pct_w, reward_total_w)
        # Bar visualisation
        bar_w = max(0.0, min(100.0, pct_w))
        bar = (
            f"<div class='bar-wrap share-bar-wrap' title='slot share {pct_label} · {share_tone}'>"
            f"<div class='bar' style='width:{bar_w:.0f}%;background:{share_color}'></div>"
            f"<span class='bar-text share-bar-text'>{w.ours}/{w.total_batch} · {pct_label}</span>"
            f"</div>"
        )
        rej_str = " ".join(
            f"<span class='red small'>{html.escape(k)}={v}</span>"
            for k, v in w.rejects.items()
        ) or "<span class='dim'>—</span>"
        rows.append(
            f"<tr><td class='mono'>w{w.n}</td>"
            f"<td>{bar}</td>"
            f"<td class='num mono' style='color:{reward_color_w}' title='{reward_tone_w} reward share'>{reward_label}</td>"
            f"<td class='num dim'>{w.rt_first:.1f}s</td>"
            f"<td>{rej_str}</td></tr>"
        )
    summary_parts = [
        f"<span class='share-stat'>slot share "
        f"<b style='color:{total_color}'>{total_ours}/{total_slots} = {_fmt_pct(pct, total_slots)}</b> "
        f"<em>{total_tone}</em></span>",
        f"<span class='share-stat'>reward share "
        f"<b style='color:{reward_color}'>{_fmt_pct(reward_pct, total_reward)}</b> "
        f"<em>{reward_tone}</em></span>",
        f"<span class='share-legend'>{_share_legend_html()}</span>",
    ]
    if aborted_count:
        summary_parts.append(
            f"<span class='share-stat'><b style='color:var(--red)'>{aborted_count}</b> "
            "aborted terminal(s), excluded</span>"
        )
    summary = " ".join(summary_parts)
    return f"""
<div class="panel">
  <h2>Window history <span class="dim small">{len(windows)} shown · {len(score_context)}-window score context</span></h2>
  <div class="window-share-summary">{summary}</div>
  <table class="grid compact">
    <thead><tr><th>win</th><th>slot share</th><th class="num">reward %</th><th>rt₀</th><th>rejects</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
  <div class="total">{len(score_context)}-WINDOW TOTAL: <b style="color:{total_color}">{total_ours}/{total_slots} = {_fmt_pct(pct, total_slots)}</b></div>
</div>
"""


def render_events_html(limit: int = 30) -> str:
    """Merge most recent event lines from all boxes, sort by timestamp."""
    with _lock:
        boxes = list(_boxes)
    merged = []
    for b in boxes:
        for line in b.recent_lines:
            merged.append((b.label, b.color, line))
    def keyfn(rec):
        line = rec[2]
        bits = line.split(maxsplit=3)
        if len(bits) >= 3:
            return bits[0] + " " + bits[1] + " " + bits[2]
        return line
    merged.sort(key=keyfn)
    tail = merged[-limit:]
    rows = []
    if not tail:
        rows.append("<div class='dim'>(no events yet — first poll still warming)</div>")
    for label, color, line in tail:
        msg = line
        # Strip "May 09 22:29:58 host reliquary-miner-pro[27481]: " prefix.
        if ": " in msg and msg.count(":") >= 3:
            msg = msg.split(": ", 1)[1]
        msg = _clean_operator_msg(msg)
        if not msg:
            continue
        sev = (
            "evt-ok" if "ACCEPTED" in msg else
            "evt-bad" if ("ERROR" in msg or "rejected" in msg or "FAIL" in msg)
            else "evt-info"
        )
        rows.append(
            f"<div class='evt {sev}'>"
            f"<span class='evt-lbl' style='color:{color}'>[{html.escape(label)}]</span>"
            f"<span class='evt-msg'>{html.escape(msg[:300])}</span>"
            f"</div>"
        )
    if not rows:
        rows.append("<div class='dim'>no high-signal events in the latest tail</div>")
    return f"<div class='panel'><h2>Live event tail (latest {limit})</h2><div class='events'>{''.join(rows)}</div></div>"


def render_validator_events_html(limit: int = 60) -> str:
    """Validator lifecycle diagnostics from the live container log stream.

    Miner journal events (panel above) only confirm the validator's HTTP layer
    enqueued the submission. This panel shows candidate pool accepts/rejects plus
    final selection/reward/seal/fail stages pulled via SSH+docker logs. The
    exact per-hotkey `/verdicts` snapshot remains authoritative for pool/proof
    admission counters; R2 remains authoritative for selection and reward.
    Lag from queue depth shows as a delta between
    miner-side ACCEPTED-PREGEN and validator-side accepted prompt= for the
    same prompt.
    """
    events = validator_events_snapshot(limit)
    # Newest first so the freshest decision is at the top.
    events = list(reversed(events))

    # Counters over the buffer for a quick at-a-glance KPI strip.
    n_ours = sum(1 for e in events if e.kind == "accept" and e.ours)
    n_acc = sum(1 for e in events if e.kind == "accept")
    n_rej = sum(1 for e in events if e.kind == "reject")
    n_late = sum(1 for e in events if e.kind == "late_drop")
    n_selected = sum(1 for e in events if e.kind == "selected")
    n_reward = sum(1 for e in events if e.kind == "reward")
    n_ours_late = sum(1 for e in events if e.kind == "late_drop" and e.ours)
    n_seal = sum(1 for e in events if e.kind == "seal")
    n_fail = sum(1 for e in events if e.kind == "fail")

    rows = []
    if not events:
        if not VALIDATOR_SSH:
            rows.append(
                "<div class='dim'>Direct validator log tail is disabled in "
                "HTTP-only mode. Exact per-hotkey verdicts and sealed-window "
                "rewards remain available.</div>"
            )
        else:
            rows.append(
                "<div class='dim'>(no validator events yet — tail subprocess "
                "warming, or no recent pool accepts/rejects)</div>"
            )
    # Render the tail with late-drops EXCLUDED — at ~3000/h fleet-wide
    # they'd drown the panel and bury accepts/rejects. The KPI strip
    # above surfaces the count; per-hotkey detail lives in the rundown.
    for e in events:
        if e.kind == "late_drop":
            continue
        if e.kind == "accept":
            cls = "evt-ok-ours" if e.ours else "evt-ok"
        elif e.kind == "reject":
            cls = "evt-bad-ours" if e.ours else "evt-bad"
        elif e.kind == "fail":
            cls = "evt-bad"
        elif e.kind in ("selected", "reward"):
            cls = "evt-info"
        else:  # seal
            cls = "evt-info"
        marker = "▶ " if e.ours else "  "
        msg = _clean_operator_msg(e.msg)
        if not msg:
            continue
        rows.append(
            f"<div class='evt {cls}'>"
            f"<span class='evt-ts mono dim'>{html.escape(e.ts)}</span>"
            f"<span class='evt-kind mono'>{marker}{e.kind:<6}</span>"
            f"<span class='evt-msg'>{html.escape(msg[:280])}</span>"
            f"</div>"
        )
    if not rows:
        rows.append("<div class='dim'>no high-signal validator events in this slice</div>")
    # `ours-late` highlights drops involving fleet hotkeys — the most
    # actionable signal in here (something on your side is too slow).
    ours_late_html = (
        f"<span class='kpi-mini'>ours-late <b style='color:var(--red)'>{n_ours_late}</b></span>"
        if n_ours_late else ""
    )
    kpi = (
        f"<span class='kpi-mini'>ours-pool <b style='color:var(--green)'>{n_ours}</b></span>"
        f"<span class='kpi-mini'>pool accepts <b>{n_acc}</b></span>"
        f"<span class='kpi-mini'>rejects <b style='color:var(--red)'>{n_rej}</b></span>"
        f"<span class='kpi-mini' title='HTTP-accepted submissions dropped before validation because the batcher window already advanced'>"
        f"dropped-late <b style='color:var(--yellow)'>{n_late}</b></span>"
        f"{ours_late_html}"
        f"<span class='kpi-mini'>selected <b style='color:var(--cyan)'>{n_selected}</b></span>"
        f"<span class='kpi-mini'>rewards <b style='color:var(--green)'>{n_reward}</b></span>"
        f"<span class='kpi-mini'>seals <b style='color:var(--cyan)'>{n_seal}</b></span>"
        f"<span class='kpi-mini'>fails <b style='color:var(--red)'>{n_fail}</b></span>"
    )
    return (
        f"<div class='panel'>"
        f"<h2>Validator-side events <span class='dim small'>"
        f"(direct docker logs · last {len(events)}/{limit})</span></h2>"
        f"<div class='kpi-row' style='margin-bottom:8px'>{kpi}</div>"
        f"<div class='events'>{''.join(rows)}</div></div>"
    )


# --- Validator health rundown ---------------------------------------------

# Timeframe presets the rundown panel offers. Keep the labels short — they
# render as inline pills inside the panel header.
RUNDOWN_TIMEFRAMES = {
    "10m": 10 * 60,
    "30m": 30 * 60,
    "60m": 60 * 60,
    "all": 0,  # 0 means "every event still in the buffer"
}


def _fmt_uptime(seconds: int) -> str:
    if seconds < 0:
        return "—"
    if seconds < 60:
        return f"{seconds}s"
    m, _ = divmod(seconds, 60)
    if m < 60:
        return f"{m} min"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"


def _render_late_drop_block(ours: "Counter", others: "Counter") -> str:
    """Render the per-hotkey late-drop breakdown for the rundown panel.

    Two sub-lists: our fleet first (with a red tint so they're easy to
    spot), then the top 5 from the rest of the subnet (informational).
    A hotkey appearing here means it submitted but lost the FIFO race
    — its rollouts never reached worker validation in this timeframe.
    """
    if not ours and not others:
        return "<div class='dim small'>no late drops in window</div>"

    parts: list[str] = []
    if ours:
        items = "".join(
            f"<li><span class='rd-val mono' style='color:var(--red)'>{n}</span>"
            f"<span class='rd-label mono'>{html.escape(hk[:12])}…</span></li>"
            for hk, n in ours.most_common()
        )
        parts.append(
            "<div class='rd-dim small' style='margin-top:4px'>our fleet (always 0 = healthy)</div>"
            f"<ul class='rd-list rd-reasons'>{items}</ul>"
        )
    # Cap the "others" section so the panel doesn't grow unbounded —
    # the top 5 offenders are enough to spot subnet-wide patterns.
    if others:
        top = others.most_common(5)
        items = "".join(
            f"<li><span class='rd-val mono'>{n}</span>"
            f"<span class='rd-label mono dim'>{html.escape(hk[:12])}…</span></li>"
            for hk, n in top
        )
        more = ""
        if len(others) > 5:
            more = (
                f"<li><span class='rd-val mono dim'>+{len(others) - 5}</span>"
                f"<span class='rd-label mono dim'>more hotkeys</span></li>"
            )
        parts.append(
            "<div class='rd-dim small' style='margin-top:6px'>other subnet hotkeys (top 5)</div>"
            f"<ul class='rd-list rd-reasons'>{items}{more}</ul>"
        )
    return "".join(parts)


def render_validator_rundown_html(timeframe: str = "30m") -> str:
    """Aggregate validator-side events over a sliding timeframe into a
    health rundown panel. Mirrors the operator-style writeup:
      - deployment fingerprint (image SHA + uptime)
      - error counters (Window iteration failed, worker_fail, window_timeout,
        randomness retries — each one a separate "PR fix" we track)
      - throughput (accepts, rejects, accept rate, windows sealed)
      - reject reason breakdown (parsed from `reason=<x>` on each reject)

    The timeframe is one of RUNDOWN_TIMEFRAMES. "all" returns every
    event still in the tail buffer (~30-90 min depending on cadence).
    """
    if timeframe not in RUNDOWN_TIMEFRAMES:
        timeframe = "30m"
    window_s = RUNDOWN_TIMEFRAMES[timeframe]
    tf_label = "all events in buffer" if window_s <= 0 else f"last {timeframe}"
    events = validator_events_in_window(window_s)

    # --- Deployment fingerprint ---
    with _lock:
        ds = _deployment
        vs = _vs
        windows = list(_windows)
        poll_at = _last_poll_at
    badge_health = _validator_badge_health_details(vs, poll_at=poll_at)
    deploy_html: str
    health_image = getattr(vs, "image_revision", "") or ""
    deploy_image = ds.image_sha or health_image[:12]
    health_started = float(getattr(vs, "app_started_at", 0.0) or 0.0)
    deploy_uptime = ds.uptime_s if ds.image_sha else (
        max(0, int(time.time() - health_started)) if health_started else -1
    )
    if ds.started_at:
        started_short = ds.started_at[:19].replace("T", " ")
    elif health_started:
        started_short = datetime.fromtimestamp(
            health_started, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
    else:
        started_short = "—"
    deploy_status = ds.status or (
        "running" if getattr(vs, "health_status", "") == "ok" else
        getattr(vs, "health_status", "")
    )
    deploy_source = "SSH inspect" if ds.image_sha else "/health"
    if ds.error and not deploy_image:
        deploy_html = (
            f"<div class='rd-deploy'>"
            f"<span class='rd-dim'>validator image</span> "
            f"<span class='red mono'>probe error: {html.escape(ds.error)}</span>"
            f"</div>"
        )
    elif not deploy_image:
        deploy_html = (
            "<div class='rd-deploy'>"
            "<span class='rd-dim'>validator image</span> "
            "<span class='dim'>warming (/health + SSH)</span>"
            "</div>"
        )
    else:
        # Soft cue when the container restarted in the last ~3 min — the
        # rundown counters reset implicitly and the operator should know
        # they're looking at a fresh deployment, not a steady-state run.
        fresh_chip = ""
        if deploy_uptime >= 0 and deploy_uptime < 180:
            fresh_chip = (
                "<span class='rd-pill' style='background:rgba(210,153,34,0.18);"
                "color:var(--yellow);margin-left:6px'>fresh deploy</span>"
            )
        status_color = "var(--green)" if deploy_status in ("running", "ok") else "var(--red)"
        deploy_html = (
            f"<div class='rd-deploy'>"
            f"<div class='rd-deploy-line'>"
            f"<span class='rd-dim'>image</span> "
            f"<span class='mono cyan'>{html.escape(deploy_image)}</span> "
            f"<span class='rd-dim'>· status</span> "
            f"<span class='mono' style='color:{status_color}'>{html.escape(deploy_status or '?')}</span>"
            f"{fresh_chip}"
            f"</div>"
            f"<div class='rd-deploy-line dim mono small'>"
            f"up {_fmt_uptime(deploy_uptime)} since {html.escape(started_short)}Z · {html.escape(deploy_source)}"
            f"</div>"
            f"</div>"
        )

    # --- Error counters ---
    err_counts = {
        "fail": sum(1 for e in events if e.kind == "fail"),
        "worker_fail": sum(1 for e in events if e.kind == "worker_fail"),
        "window_timeout": sum(1 for e in events if e.kind == "window_timeout"),
        "randomness_retry": sum(1 for e in events if e.kind == "randomness_retry"),
        "randomness_ok": sum(1 for e in events if e.kind == "randomness_ok"),
        # `empty_randomness` is a reject reason, not a top-level event kind.
        "empty_randomness": sum(
            1 for e in events
            if e.kind == "reject" and e.reject_reason == "empty_randomness"
        ),
    }
    err_lines = [
        ("Window iteration failed", err_counts["fail"]),
        ("Empty randomness rejects", err_counts["empty_randomness"]),
        ("Submission worker failed", err_counts["worker_fail"]),
        ("Windows timed out", err_counts["window_timeout"]),
        ("Randomness retries (PR #8)", err_counts["randomness_retry"]),
    ]
    def _err_item(label: str, n: int) -> str:
        color = "var(--red)" if n > 0 else "var(--green)"
        return (
            f"<li><span class='rd-label'>{html.escape(label)}</span>"
            f"<span class='rd-val' style='color:{color}'>{n}</span></li>"
        )
    err_html = "".join(_err_item(label, n) for label, n in err_lines)
    total_errors = err_counts["fail"] + err_counts["worker_fail"] + err_counts["window_timeout"]
    if not badge_health["ok"]:
        badge_error = badge_health["tone"] == "error"
        badge_background = (
            "rgba(248,81,73,0.18)" if badge_error
            else "rgba(210,153,34,0.18)"
        )
        badge_color = "var(--red)" if badge_error else "var(--yellow)"
        health_pill = (
            f"<span class='rd-pill' style='background:{badge_background};"
            f"color:{badge_color}'>{html.escape(str(badge_health['label']))}</span>"
        )
    elif total_errors == 0:
        health_pill = (
            "<span class='rd-pill' style='background:rgba(63,185,80,0.15);"
            "color:var(--green)'>healthy</span>"
        )
    else:
        health_pill = (
            f"<span class='rd-pill' style='background:rgba(248,81,73,0.18);"
            f"color:var(--red)'>{total_errors} "
            f"error{('s' if total_errors != 1 else '')}</span>"
        )

    # --- Throughput ---
    accepts = [e for e in events if e.kind == "accept"]
    rejects = [e for e in events if e.kind == "reject"]
    late_drops = [e for e in events if e.kind == "late_drop"]
    seals = [e for e in events if e.kind == "seal"]
    n_acc = len(accepts)
    n_rej = len(rejects)
    n_late = len(late_drops)
    # `/verdicts` is authoritative for pool/proof admission. Keep the
    # journal-derived counters above for validator-wide diagnostics, but show
    # our hotkeys' pool outcomes separately and leave selection/reward to R2.
    direct_period = "60m" if timeframe in ("60m", "all") else "30m"
    direct_acc = 0
    direct_rej = 0
    for verdict_summary in (getattr(vs, "verdicts_by_hotkey", {}) or {}).values():
        if not isinstance(verdict_summary, dict):
            continue
        direct_acc += int(verdict_summary.get(f"accepted_{direct_period}", 0) or 0)
        direct_rej += int(verdict_summary.get(f"rejected_{direct_period}", 0) or 0)
    # "Submissions seen" = everything the validator HTTP layer accepted
    # (whether or not it ultimately reached the worker). Accept rate is
    # over this denominator so late drops dilute it appropriately —
    # otherwise the dashboard would report 99% accept rate while a third
    # of the traffic gets silently dropped before validation.
    total_submissions = n_acc + n_rej + n_late
    accept_rate_pct = int(n_acc / max(total_submissions, 1) * 100)
    late_rate_pct = int(n_late / max(total_submissions, 1) * 100)
    accept_rate_color = (
        "var(--green)" if accept_rate_pct >= 70 else
        "var(--yellow)" if accept_rate_pct >= 50 else
        "var(--red)"
    )
    late_rate_color = (
        "var(--red)" if late_rate_pct >= 50 else
        "var(--yellow)" if late_rate_pct >= 20 else
        "var(--green)"
    )

    # Fleet hotkeys that lost the FIFO race in this window. Top offenders
    # surface explicitly so the operator can answer "is hotkey X just
    # slow?" without SSH'ing to grep logs.
    our_late_by_hotkey = Counter(
        e.hotkey12 for e in late_drops if e.ours
    )
    other_late_by_hotkey = Counter(
        e.hotkey12 for e in late_drops if not e.ours
    )

    # --- Reject reasons breakdown ---
    reason_counts = Counter(
        (e.reject_reason or "unknown") for e in rejects
    )
    if reason_counts:
        reason_items = "".join(
            f"<li><span class='rd-val mono'>{n}</span>"
            f"<span class='rd-label mono'>{html.escape(reason)}</span></li>"
            for reason, n in reason_counts.most_common()
        )
        reasons_html = f"<ul class='rd-list rd-reasons'>{reason_items}</ul>"
    else:
        reasons_html = "<div class='dim small'>no rejects in window</div>"

    # --- Timeframe pills (the operator switches the slice with these) ---
    pills = "".join(
        (f"<a href='#' class='rd-pill rd-pill-active' data-tf='{tf}'>{tf}</a>"
         if tf == timeframe else
         f"<a href='#' class='rd-pill rd-tf' data-tf='{tf}'>{tf}</a>")
        for tf in RUNDOWN_TIMEFRAMES
    )

    # --- Current window context line (Y/B in flight) ---
    live_v, proof_target, live_source = _live_valid_context(vs, windows)
    in_flight_color, _in_flight_tone = _window_share_tone(
        _share_pct(min(live_v, proof_target), proof_target), proof_target
    )
    in_flight = (
        f"<div class='rd-dim small'>window <b class='mono' style='color:var(--window-id)'>{vs.window or '—'}</b> "
        f"currently <b class='mono'>{html.escape(vs.state or '?')}</b> "
        f"at <b class='mono' style='color:{in_flight_color}'>"
        f"{_valid_pressure_label(live_v, proof_target)}</b> "
        f"<span class='rd-dim'>({html.escape(live_source)})</span></div>"
    )

    return f"""
<div class="panel area-rundown-inner" id="rundown-panel">
  <h2>
    Validator rundown {health_pill}
    <span class='dim small' style='margin-left:auto'>{tf_label}</span>
  </h2>
  <div class="rd-tf-row" id="rundown-tf-row">{pills}</div>

  {deploy_html}

  <div class="rd-section">
    <div class="rd-section-title">Errors <span class='dim small'>({tf_label})</span></div>
    <ul class="rd-list">{err_html}</ul>
  </div>

  <div class="rd-section">
    <div class="rd-section-title">Throughput <span class='dim small'>({tf_label})</span></div>
    <ul class="rd-list">
      <li><span class='rd-label'>Pool-accepted prompts</span>
          <span class='rd-val' style='color:var(--green)'>{n_acc}</span></li>
      <li><span class='rd-label'>Rejected (worker)</span>
          <span class='rd-val' style='color:var(--red)'>{n_rej}</span></li>
      <li title="HTTP-accepted submissions dropped before worker validation because the batcher window already advanced. High here = miners are submitting but losing the FIFO race."><span class='rd-label'>Dropped late (batcher)</span>
          <span class='rd-val' style='color:var(--yellow)'>{n_late}</span></li>
      <li><span class='rd-label'>Total submissions seen</span>
          <span class='rd-val mono'>{total_submissions}</span></li>
      <li><span class='rd-label'>Pool accept rate</span>
          <span class='rd-val' style='color:{accept_rate_color}'>{accept_rate_pct}%</span></li>
      <li><span class='rd-label'>Late-drop rate</span>
          <span class='rd-val' style='color:{late_rate_color}'>{late_rate_pct}%</span></li>
      <li><span class='rd-label'>Windows sealed (target={proof_target})</span>
          <span class='rd-val' style='color:var(--cyan)'>{len(seals)}</span></li>
      <li title="Authoritative /verdicts pool/proof outcomes for configured fleet hotkeys; selection and reward come from R2."><span class='rd-label'>Our pool/proof outcomes ({direct_period})</span>
          <span class='rd-val'><b style='color:var(--green)'>{direct_acc}</b>/<b style='color:var(--red)'>{direct_rej}</b> a/r</span></li>
    </ul>
    {in_flight}
  </div>

  <div class="rd-section">
    <div class="rd-section-title">Reject reasons <span class='dim small'>({tf_label})</span></div>
    {reasons_html}
  </div>

  <div class="rd-section">
    <div class="rd-section-title">Late drops by hotkey <span class='dim small'>({tf_label})</span></div>
    {_render_late_drop_block(our_late_by_hotkey, other_late_by_hotkey)}
  </div>
</div>
"""


# --- New panels (Phase 6) --------------------------------------------------

def render_chain_html() -> str:
    """τ stake + emission per UID + cumulative — direct earnings indicator."""
    cs = _chain
    netuid = globals().get("NETUID", 81)
    source = html.escape(getattr(cs, "source_alias", "") or "")
    probe_s = getattr(cs, "probe_s", 0.0)
    probe_html = (
        f"<span class='kpi-mini'>probe <b>{probe_s:.1f}s</b></span>"
        if probe_s else ""
    )
    source_html = (
        f"<span class='kpi-mini'>via <b class='mono'>{source}</b></span>"
        if source else ""
    )
    if cs.error and not cs.hotkeys:
        return f"""
<div class="panel">
  <h2>Chain · netuid {netuid}</h2>
  <div class="kpi-row" style="margin-bottom:10px">{source_html}{probe_html}</div>
  <div class="red">probe failed: {html.escape(cs.error)}</div>
  <div class="dim small" style="margin-top:6px">trying local bittensor first, then configured fleet SSH targets; cached chain data is unavailable yet</div>
</div>
"""
    if not cs.hotkeys:
        watched = _watched_hotkey_rows()
        watched_rows = "".join(
            f"""
<tr>
  <td><span class="lbl">{html.escape(str(row['label']))}</span></td>
  <td class="mono" title="{html.escape(str(row['hotkey']))}">{html.escape(str(row['prefix']))}…</td>
  <td class="num mono dim">-</td>
  <td class="yellow">active target, not registered on netuid {netuid}</td>
</tr>"""
            for row in watched
        )
        watched_table = (
            f"""
  <div class="small dim" style="margin-top:10px">Dashboard active target hotkeys:</div>
  <table class="grid compact" style="margin-top:6px">
    <thead><tr><th>label</th><th>hotkey</th><th>UID</th><th>chain status</th></tr></thead>
    <tbody>{watched_rows}</tbody>
  </table>
"""
            if watched_rows
            else "<div class='dim small' style='margin-top:10px'>no watched hotkeys configured</div>"
        )
        if cs.last_fetch_at:
            age = int(time.time() - cs.last_fetch_at)
            return f"""
<div class="panel">
  <h2>Chain · netuid {netuid} <span class='dim small'>(refreshed {age}s ago, n={cs.netuid_size} UIDs)</span></h2>
  <div class="kpi-row" style="margin-bottom:10px">{source_html}{probe_html}</div>
  <span class="yellow">chain reachable, but none of our configured hotkeys were found on this netuid</span>
  {watched_table}
</div>
"""
        return f"""
<div class="panel">
  <h2>Chain · netuid {netuid}</h2>
  <div class="kpi-row" style="margin-bottom:10px">{source_html}{probe_html}</div>
  <span class='dim'>warming … (5-min cadence)</span>
  {watched_table}
</div>
"""
    age = int(time.time() - cs.last_fetch_at) if cs.last_fetch_at else -1
    rows = []
    sorted_hks = sorted(cs.hotkeys, key=lambda h: -h.stake)
    max_stake = max((h.stake for h in cs.hotkeys), default=1) or 1
    watched_count = len(OUR_SS58)
    registered_count = len(cs.hotkeys)
    staked_count = sum(1 for h in cs.hotkeys if h.stake > 0)
    emitting_count = sum(1 for h in cs.hotkeys if h.emission > 0)
    for h in sorted_hks:
        stake_pct = int(h.stake / max_stake * 100)
        # h.emission is the UID's fraction of subnet emissions (0–1) → show as %
        share_pct = h.emission * 100
        hk_short = _short_hotkey(getattr(h, "hotkey", ""))
        hk_line = (
            f"<div class='mono dim small' title='{html.escape(h.hotkey)}'>{html.escape(hk_short)}</div>"
            if getattr(h, "hotkey", "") else ""
        )
        rows.append(f"""
<tr>
  <td><span class="lbl">{html.escape(h.label)}</span>{hk_line}</td>
  <td class="num mono">{h.uid}</td>
  <td class="num mono" style="color:var(--green)">{h.stake:.2f}τ</td>
  <td><div class="bar-wrap" style="width:140px"><div class="bar" style="width:{stake_pct}%;background:var(--green)"></div></div></td>
  <td class="num mono dim">{share_pct:.2f}%</td>
</tr>""")
    stale_html = ""
    if cs.error:
        stale_html = (
            f"<div class='yellow small' style='margin:0 0 10px'>"
            f"latest probe failed, showing cached chain data: {html.escape(cs.error)}"
            f"</div>"
        )
    return f"""
<div class="panel">
  <h2>Chain · netuid {netuid} <span class='dim small'>(refreshed {age}s ago, n={cs.netuid_size} UIDs)</span></h2>
  {stale_html}
  <div class="kpi-row" style="margin-bottom:10px">
    <span class="kpi-mini">total stake <b style="color:var(--green)">{cs.total_stake:.2f}τ</b></span>
    <span class="kpi-mini">our subnet share <b style="color:var(--green)">{cs.subnet_share*100:.2f}%</b></span>
    <span class="kpi-mini" title="Configured + starred hotkeys watched by the dashboard">watched <b>{watched_count}</b></span>
    <span class="kpi-mini" title="Watched hotkeys registered on this netuid">registered <b>{registered_count}</b></span>
    <span class="kpi-mini" title="Registered watched hotkeys with non-zero stake">staked <b>{staked_count}</b></span>
    <span class="kpi-mini" title="Registered watched hotkeys with non-zero current emission">emitting <b>{emitting_count}</b></span>
    {source_html}
    {probe_html}
  </div>
  <table class="grid compact">
    <thead><tr><th>hotkey</th><th>UID</th><th>stake/token</th><th>rel</th><th>emission share</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</div>
"""


def render_slot_rank_html() -> str:
    """Per-OUR-hotkey histogram of slot ranks across recent windows."""
    with _lock:
        windows = list(_windows)
    ranks = aggregate_slot_ranks(windows)
    rows = []
    # Filter to hotkeys that actually got at least 1 slot
    active = {k: v for k, v in ranks.items() if sum(v) > 0}
    if not active:
        return ("<div class='panel'><h2>Canonical rank distribution</h2>"
                "<span class='dim'>no slots yet in cached windows</span></div>")
    max_count = max(max(v) for v in active.values()) or 1
    for hk, dist in sorted(active.items(), key=lambda kv: -sum(kv[1])):
        cells = []
        # Canonical rank 1–8 — each environment resets independently.
        for rank_index, count in enumerate(dist):
            opacity = (count / max_count) if max_count else 0
            color = (
                "var(--share-great)" if rank_index <= 1 else
                "var(--share-mid)" if rank_index <= 4 else
                "var(--share-zero)"
            )
            cells.append(
                f"<td class='num' style='background:{color};opacity:{0.2 + 0.8 * opacity};text-align:center;color:#000;font-weight:700'>{count or '·'}</td>"
            )
        total = sum(dist)
        rows.append(
            f"<tr><td class='lbl'>{html.escape(hk)}</td>"
            f"{''.join(cells)}<td class='num dim'>Σ {total}</td></tr>"
        )
    head_cells = "".join(f"<th class='num'>rank {i}</th>" for i in range(1, 9))
    return f"""
<div class="panel">
  <h2>Canonical rank distribution <span class='dim small'>(last {len(windows)} windows · per environment · rank 1 = best)</span></h2>
  <table class="grid compact">
    <thead><tr><th>hotkey</th>{head_cells}<th>total</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</div>
"""


def render_competitors_html() -> str:
    """Other miners landing slots — competitive intelligence."""
    with _lock:
        windows = list(_windows)
    comps = aggregate_competitors(windows, top_n=10)
    if not comps:
        return ("<div class='panel'><h2>Top competing miners</h2>"
                "<span class='dim'>no competitors detected</span></div>")
    rows = []
    for c in comps:
        rows.append(f"""
<tr>
  <td class='mono'>{html.escape(c['hotkey'][:12])}…</td>
  <td class='num mono'>{c['slot_count']}</td>
  <td class='num dim'>{c['mean_rt']:.0f}s</td>
  <td class='num dim'>w{c['last_seen']}</td>
</tr>""")
    return f"""
<div class="panel">
  <h2>Top competing miners <span class='dim small'>(slot share over last {len(windows)} windows)</span></h2>
  <table class="grid compact">
    <thead><tr><th>hotkey</th><th>slots</th><th>mean rt</th><th>last seen</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</div>
"""


def render_quality_html() -> str:
    """σ + k_correct distributions — model/prompt-selection signal."""
    with _lock:
        windows = list(_windows)
    sigma_h = aggregate_sigma_histogram(windows)
    k_h = aggregate_k_histogram(windows)
    if not sigma_h and not k_h:
        return ("<div class='panel'><h2>Quality distribution</h2>"
                "<span class='dim'>no data yet</span></div>")
    sigma_max = max(sigma_h.values(), default=1) or 1
    k_max = max(k_h.values(), default=1) or 1
    sigma_rows = "".join(
        f"<tr><td class='mono'>σ={k}</td>"
        f"<td><div class='bar-wrap' style='width:120px'><div class='bar' style='width:{int(v/sigma_max*100)}%;background:var(--cyan)'></div></div></td>"
        f"<td class='num mono'>{v}</td></tr>"
        for k, v in sorted(sigma_h.items(), key=lambda kv: -kv[1])
    )
    k_rows = "".join(
        f"<tr><td class='mono'>k={k}</td>"
        f"<td><div class='bar-wrap' style='width:120px'><div class='bar' style='width:{int(v/k_max*100)}%;background:var(--magenta)'></div></div></td>"
        f"<td class='num mono'>{v}</td></tr>"
        for k, v in sorted(k_h.items(), key=lambda kv: -kv[1])
    )
    return f"""
<div class="panel">
  <h2>Quality distribution <span class='dim small'>(our R2 selected slots)</span></h2>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px">
    <table class="grid compact"><thead><tr><th>σ</th><th></th><th>n</th></tr></thead><tbody>{sigma_rows}</tbody></table>
    <table class="grid compact"><thead><tr><th>k_correct</th><th></th><th>n</th></tr></thead><tbody>{k_rows}</tbody></table>
  </div>
</div>
"""


def render_baseline_html() -> str:
    """6h rolling baseline + drift indicator + 24h sparkline."""
    bs = _baseline
    if not bs.samples:
        return ("<div class='panel'><h2>Throughput baseline</h2>"
                "<span class='dim'>no samples yet (1-min cadence)</span></div>")
    # Sparkline of last 60 samples (~1h)
    blocks = "▁▂▃▄▅▆▇█"
    arr = [v for _, v in bs.samples[-60:]]
    lo, hi = min(arr), max(arr)
    span = max(hi - lo, 1)
    spark = "".join(blocks[int(round((v - lo) / span * (len(blocks) - 1)))] for v in arr)

    drift_color = (
        "var(--red)" if bs.alert else
        "var(--yellow)" if abs(bs.drift_pct) >= 15 else
        "var(--green)"
    )
    drift_arrow = "↘" if bs.drift_pct < -5 else ("↗" if bs.drift_pct > 5 else "→")
    alert_html = (
        "<span class='red small'><b>ALERT</b> current ACPT/30m below 70% of 6h baseline</span>"
        if bs.alert else "<span class='dim small'>tracking ok</span>"
    )
    return f"""
<div class="panel">
  <h2>Throughput baseline <span class='dim small'>(samples: {len(bs.samples)} · 1-min)</span></h2>
  <div class="kpi-row" style="margin-bottom:8px">
    <span class="kpi-mini">current <b>{bs.current}</b> /30m</span>
    <span class="kpi-mini">6h baseline <b>{bs.baseline_6h:.0f}</b> /30m</span>
    <span class="kpi-mini" style="color:{drift_color}">drift <b>{bs.drift_pct:+.1f}%</b> {drift_arrow}</span>
  </div>
  <div class="spark" style="font-size:14px;color:var(--green)">{spark}</div>
  <div style="margin-top:6px">{alert_html}</div>
</div>
"""


def render_rtt_html() -> str:
    """Network latency mac→validator + each box→validator."""
    rt = _rtt
    if not rt.rtt_ms:
        return ("<div class='panel'><h2>Network RTT to validator</h2>"
                "<span class='dim'>warming … (60s cadence)</span></div>")
    rows = []
    for source, ms in sorted(rt.rtt_ms.items(), key=lambda kv: -kv[1]):
        if ms < 0:
            txt = "<span class='red'>error</span>"
        else:
            color = (
                "var(--red)" if ms > 500 else
                "var(--yellow)" if ms > 200 else
                "var(--green)"
            )
            txt = f"<span style='color:{color}'>{ms} ms</span>"
        rows.append(f"<tr><td class='lbl'>{html.escape(source)}</td><td class='num'>{txt}</td></tr>")
    age = int(time.time() - rt.last_fetch_at) if rt.last_fetch_at else -1
    return f"""
<div class="panel">
  <h2>Network RTT <span class='dim small'>(refreshed {age}s ago)</span></h2>
  <table class="grid compact">
    <thead><tr><th>source</th><th>→ validator</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</div>
"""


def render_ema_leaderboard_html(top_n: int = 12, mode: str = "top") -> str:
    """Replays the validator's exact EMA on our cached R2 windows and renders
    the leaderboard. This is THE leaderboard that maps to on-chain weights —
    not just slot share. Updates whenever new R2 windows arrive (~30s).

    Modes:
        `top` — show only the first `top_n` rows (default). The bottom of
                the leaderboard carries near-zero EMA values that don't
                move on-chain weight allocation in any meaningful way, so
                truncating keeps the panel compact. Default top_n = 12.
        `active` — show every hotkey that has had at least one slot in
                the cached lookback window. Used when the operator
                wants to confirm a specific hotkey has actually submitted
                (rather than guessing from rank). Panel becomes
                internally scrollable when this is many rows tall.

    The `mode` is wired up via the toggle pills in the panel header.
    htmx persists the selection through the area's `hx-get` URL so
    auto-refresh keeps the chosen view.
    """
    with _lock:
        leaderboard = list(_ema_cache)

    if not leaderboard:
        return ("<div class='panel'><h2>EMA leaderboard</h2>"
                "<span class='dim'>warming … need at least 1 window of R2 data</span></div>")

    # SS58 → label for our hotkeys
    label_by_ss58 = OUR_SS58

    if not leaderboard:
        return ("<div class='panel'><h2>EMA leaderboard</h2>"
                "<span class='dim'>no scored miners yet</span></div>")

    # Filter for the chosen mode. `active` keeps every row with at least
    # one observed slot in the lookback — these are the miners that have
    # actually submitted at some point, regardless of EMA magnitude.
    # `top` truncates to the first `top_n` ranked rows.
    leaderboard_by_hotkey = {str(r.get("hotkey", "")): r for r in leaderboard}

    if mode == "active":
        visible = [r for r in leaderboard if r.get("slot_count_total", 0) >= 1]
        mode_chip = (
            f"<span class='dim small'>targets + all active · {len(visible)} hotkeys</span>"
        )
    else:
        mode = "top"  # normalize unexpected values
        visible = leaderboard[:top_n]
        mode_chip = f"<span class='dim small'>targets + top {top_n} network</span>"

    # Toggle pills — match the rundown panel's pill styling so the visual
    # vocabulary stays consistent. Click handler wired up in the global
    # JS block (`#ema-mode-row`).
    def pill(label: str, value: str) -> str:
        cls = "rd-pill rd-pill-active" if value == mode else "rd-pill ema-mode"
        return f"<a href='#' class='{cls}' data-mode='{value}'>{label}</a>"

    mode_pills = (
        f"<div class='rd-tf-row' id='ema-mode-row'>"
        f"{pill('top 12', 'top')}"
        f"{pill('all active', 'active')}"
        f"</div>"
    )
    target_chips = "".join(
        f"<span class='target-context-chip' title='{html.escape(hk)}'>"
        f"target <b>{html.escape(label)}</b> "
        f"<span class='mono'>{html.escape(_short_hotkey(hk))}</span></span>"
        for hk, label in OUR_SS58.items()
    )

    def render_ema_row(
        hk: str,
        rank: object,
        share_pct_chain: float,
        recent: int,
        is_ours: bool,
        forced_note: str = "",
    ) -> str:
        is_ours = hk in OUR_SS58
        label_txt = label_by_ss58.get(hk, hk[:10] + "…")
        # Color rows: us = our hotkey color via FLEET map; others = dim
        row_color = ""
        for f_alias, f_hk, f_label, f_color, _ in FLEET:
            if f_hk == hk:
                row_color = f_color
                break
        if not row_color and is_ours:
            row_color = "#6cf"  # other our hotkeys not on the running fleet
        recent_color = (
            "var(--share-good)" if recent >= 3 else
            "var(--share-mid)" if recent >= 1 else
            "var(--share-zero)"
        )
        # Star button — click toggles is_ours for `hk`. Solid ★ when
        # starred or in a fleet box, hollow ☆ otherwise. Data attribute
        # carries the full hotkey so the click handler can POST without
        # re-parsing the row.
        star_glyph = "★" if is_ours else "☆"
        star_color = "var(--yellow)" if is_ours else "var(--dim)"
        star_btn = (
            f"<button class='star-btn' data-hk='{html.escape(hk)}' "
            f"title='{'Unstar (remove from your set)' if is_ours else 'Star this hotkey'}'"
            f" aria-label='{'Remove hotkey from targets' if is_ours else 'Add hotkey to targets'}'"
            f" style='color:{star_color}'>{star_glyph}</button>"
        )
        scope = (
            "<span class='scope-chip scope-target'>target</span>"
            if is_ours else
            "<span class='scope-chip scope-network'>network</span>"
        )
        note = (
            f"<div class='dim small'>{html.escape(forced_note)}</div>"
            if forced_note else ""
        )
        return f"""
<tr style="{'background:rgba(255,184,108,0.07);' if is_ours else ''}">
  <td class="num mono">{html.escape(str(rank))}</td>
  <td>{star_btn}</td>
  <td>{scope}</td>
  <td><span class="lbl" style="color:{row_color or 'var(--text)'}">{html.escape(label_txt)}</span>{note}</td>
  <td class="num mono">{share_pct_chain:.2f}%</td>
  <td class="num mono" style="color:{recent_color}">{recent}</td>
</tr>"""

    rows = []
    visible_hotkeys = {str(r.get("hotkey", "")) for r in visible}
    for hk, label in OUR_SS58.items():
        if hk in visible_hotkeys:
            continue
        r = leaderboard_by_hotkey.get(hk)
        if r:
            rows.append(render_ema_row(
                hk=hk,
                rank=int(r["rank"]),
                share_pct_chain=float(r["ema"]) * 100,
                recent=int(r["slot_count_recent12"]),
                is_ours=True,
                forced_note="target outside current EMA view",
            ))
        else:
            rows.append(render_ema_row(
                hk=hk,
                rank="—",
                share_pct_chain=0.0,
                recent=0,
                is_ours=True,
                forced_note="target has no EMA in cached windows yet",
            ))

    for r in visible:
        hk = str(r["hotkey"])
        rows.append(render_ema_row(
            hk=hk,
            rank=int(r["rank"]),
            share_pct_chain=float(r["ema"]) * 100,
            recent=int(r["slot_count_recent12"]),
            is_ours=hk in OUR_SS58,
        ))

    return f"""
<div class="panel">
  <h2>
    EMA leaderboard
    <span style='margin-left:auto'>{mode_chip}</span>
  </h2>
  {mode_pills}
  <div class="target-context-row">{target_chips}</div>
  <table class="grid compact">
    <thead><tr>
      <th class='num'>#</th><th aria-label='Target status'><span class='visually-hidden'>Target status</span></th><th>scope</th><th>hotkey</th>
      <th class='num'>weight</th>
      <th class='num' title='Selected slots in the last 12 sealed windows'>
        selected slots · last 12
      </th>
    </tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</div>
"""


def render_box_detail_html(label: str) -> str:
    """Drill-down panel content shown when a box row is clicked."""
    with _lock:
        b = next((x for x in _boxes if x.label == label), None)
    if not b:
        return (
            "<button class='close' type='button' data-drawer-close "
            "aria-label='Close miner details'>×</button>"
            f"<h3>unknown box: {html.escape(label)}</h3>"
        )
    pregen_30m, _pregen_60m, pregen_source = _box_rolling_pregen(b)
    pregen_30m_text = "—" if pregen_30m is None else str(pregen_30m)
    pregen_title = (
        "Exact rolling generation from this V5 host's fleet-events sidecar; "
        "not shared-hotkey validator admission"
        if pregen_source == "v5_fleet_events_generation"
        else "V5 fleet-events does not cover a complete rolling generation "
        "window yet; no zero is inferred"
        if pregen_source == "v5_fleet_events_generation_unavailable"
        else
        "Exact rolling generation is unavailable for this standalone miner; "
        "use its profile-scoped natural-M8 generation funnel"
        if pregen_30m is None
        else "Miner pregen rate (queue activity, not validator-confirmed)"
    )
    pool_30m_text = "—" if b.acpt_30m is None else str(b.acpt_30m)
    pool_60m_text = "—" if b.acpt_60m is None else str(b.acpt_60m)
    pct = b.gpu_mem_mb * 100 // max(b.gpu_total_mb, 1)
    events = list(b.recent_lines)
    event_rows = []
    for line in events:
        msg = _clean_operator_msg(line)
        if not msg:
            continue
        event_rows.append(
            f"<div class='evt evt-info' style='font-size:11px;border-bottom:1px solid var(--border);padding:2px 0'>"
            f"<span class='evt-msg'>{html.escape(msg[:280])}</span></div>"
        )
    ev_html = "".join(event_rows) or "<span class='dim'>no high-signal recent events</span>"
    return f"""
<button class="close" type="button" data-drawer-close aria-label="Close miner details">×</button>
<h3 style="color:{b.color}">{html.escape(b.label)} · {html.escape(b.hotkey)}</h3>
<div style="margin-bottom:14px">
  <div class="kpi-row">
    <span class="kpi-mini">alive <b>{'●' if b.proc_alive else '○'}</b></span>
    <span class="kpi-mini">uptime <b>{fmt_age(b.proc_uptime_s)}</b></span>
    <span class="kpi-mini">restarts <b>{b.restart_count}</b></span>
  </div>
  <div class="kpi-row" style="margin-top:8px">
    <span class="kpi-mini">GPU <b>{b.gpu_mem_mb//1024}/{b.gpu_total_mb//1024} GB ({pct}%)</b></span>
    <span class="kpi-mini">util <b>{b.gpu_util}%</b></span>
    <span class="kpi-mini">CPU <b>{b.cpu_pct}%</b></span>
    <span class="kpi-mini">RSS <b>{b.rss_mb} MB</b></span>
    <span class="kpi-mini">disk <b>{b.disk_used_pct}%</b></span>
  </div>
  <div class="kpi-row" style="margin-top:8px">
    <span class="kpi-mini" title="Observed host-local V5 admissions when the fleet-events tail covers the window; otherwise a shared-hotkey validator aggregate is not assigned to this box">pool/30m <b style="color:var(--green)">{pool_30m_text}</b></span>
    <span class="kpi-mini" title="Observed host-local V5 admissions only; rejected outcomes remain unavailable, and R2 carries canonical selection/reward">pool/60m <b>{pool_60m_text}</b></span>
    <span class="kpi-mini" data-pregen-source="{html.escape(pregen_source)}" title="{html.escape(pregen_title)}">pregen/30m <b class='dim'>{html.escape(pregen_30m_text)}</b></span>
    <span class="kpi-mini" title="Late drops — submissions lost to FIFO race before validation">late/30m <b style="color:{('var(--red)' if b.late_drops_30m > 0 else 'var(--dim)')}">{b.late_drops_30m}</b></span>
    <span class="kpi-mini">mean rt <b>{b.mean_accept_t_s:.0f}s</b></span>
    <span class="kpi-mini">skip/30m <b>{b.skip_30m}</b></span>
  </div>
  <div class="kpi-row" style="margin-top:8px">
    <span class="kpi-mini">state <b>{html.escape(b.miner_state)}</b></span>
    <span class="kpi-mini">win <b>{b.miner_window or '—'}</b></span>
    <span class="kpi-mini">ready <b>{b.miner_ready}</b></span>
    <span class="kpi-mini">inflight <b>{b.miner_inflight}</b></span>
    <span class="kpi-mini">submitted/win <b>{b.miner_submitted_this_win}</b></span>
    <span class="kpi-mini">fresh/30m <b>{b.fresh_built_30m}</b></span>
    <span class="kpi-mini">cache/pre <b>{b.cache_fwd_30m}/{b.prefinalized_30m}</b></span>
    <span class="kpi-mini">grace <b>{b.late_grace_30m}</b></span>
    <span class="kpi-mini">batch_filled <b>{b.batch_filled_30m}</b></span>
  </div>
  <div class="kpi-row" style="margin-top:8px">
    <span class="kpi-mini">window_mismatch <b>{b.window_mismatch_30m}</b></span>
    <span class="kpi-mini">grail_fail <b>{b.grail_fail_30m}</b></span>
    <span class="kpi-mini">bad_term <b>{b.bad_term_30m}</b></span>
    <span class="kpi-mini">pregen_fail <b>{b.pregen_fail_30m}</b></span>
  </div>
  <div class="kpi-row" style="margin-top:8px">
    <span class="kpi-mini">last OOM <b style="color:{color_oom_age(b.last_oom_age_s)}">{fmt_age(b.last_oom_age_s)}</b></span>
    <span class="kpi-mini">oom/60m <b>{b.oom_60m}</b></span>
  </div>
</div>
<h3>Recent events</h3>
<div class="events" style="max-height:50vh;overflow-y:auto">{ev_html}</div>
"""


def _r2_status_snapshot() -> dict:
    """Return the fleet module's R2 freshness state as a JSON-safe dict.

    Kept behind a tiny adapter because fleet.py owns the fetcher and may add
    diagnostic fields without requiring the web process to be restarted in
    lockstep during an upgrade.
    """
    try:
        import fleet as _fleet_mod
        status_fn = getattr(_fleet_mod, "r2_status", None)
        if not callable(status_fn):
            return {"last_success_at": 0.0, "last_error": "r2 status unavailable"}
        status = status_fn()
        if isinstance(status, dict):
            return dict(status)
        if hasattr(status, "__dict__"):
            return dict(vars(status))
        return {"value": status}
    except Exception as exc:  # health/export must remain callable on failure
        return {"last_success_at": 0.0, "last_error": type(exc).__name__}


def compute_healthz(
    refresh_s: float,
    now: float | None = None,
    *,
    poll_stale_s: float | None = None,
) -> dict:
    """Compute an honest readiness result from the cached polling state.

    This endpoint is intended for supervision, not just liveness. A running
    HTTP process with stale validator data, a dead/quarantined miner, or a
    stalled R2 archive feed is degraded and therefore returns ``ok=false``.
    """
    now = time.time() if now is None else float(now)
    effective_refresh = float(refresh_s)
    if not math.isfinite(effective_refresh) or effective_refresh <= 0:
        raise ValueError("refresh_s must be a finite positive value")
    # ``refresh_s`` is the sleep *after* a complete generation, not its wall
    # clock cadence. Named-lane resolution, the atomic box probe, and the
    # validator surfaces all have bounded network timeouts and can make a
    # healthy generation take tens of seconds. Let operators budget for that
    # work explicitly while retaining the historical three-refresh floor.
    configured_poll_limit = 0.0
    if poll_stale_s is not None:
        configured_poll_limit = float(poll_stale_s)
        if not math.isfinite(configured_poll_limit) or configured_poll_limit <= 0:
            raise ValueError("poll_stale_s must be a finite positive value")
    poll_limit = max(
        15.0,
        effective_refresh * 3.0,
        configured_poll_limit,
    )
    # Verdicts are gathered by the same generation before publication, so a
    # smaller limit would reintroduce readiness flapping partway through an
    # otherwise healthy long probe cycle.
    verdict_limit = max(180.0, effective_refresh * 6.0, poll_limit)
    r2_limit = max(120.0, effective_refresh * 12.0)
    with _lock:
        boxes = list(_boxes)
        labs = list(_labs)
        components = list(_components)
        chain_entries = {
            str(getattr(entry, "hotkey", "") or ""): entry
            for entry in _chain.hotkeys
        }
        vs = _vs
        windows = list(_windows)
        poll_at = _last_poll_at
        poll_count = _poll_count

    def age(ts: float) -> float | None:
        return max(0.0, now - ts) if ts else None

    poll_age = age(float(poll_at or 0.0))
    state_age = age(float(getattr(vs, "last_fetch_at", 0.0) or 0.0))
    health_age = age(float(getattr(vs, "health_last_fetch_at", 0.0) or 0.0))
    verdict_age = age(float(getattr(vs, "verdicts_last_fetch_at", 0.0) or 0.0))

    box_details = []
    for box in boxes:
        box_age = age(float(getattr(box, "last_poll_s", 0.0) or 0.0))
        issues = _box_readiness_issues(
            box,
            validator_state=vs,
            now=now,
            poll_limit=poll_limit,
        )
        box_details.append({
            "label": box.label,
            "ok": not issues,
            "age_s": box_age,
            "alive": bool(box.proc_alive),
            "configured_units": [
                str(spec[0]) for spec in getattr(box, "unit_candidates", ())
            ] or ([str(box.unit)] if box.unit else []),
            "host_allowed_units": list(
                getattr(box, "host_unit_allowlist", ()) or ()
            ),
            "coordinated_units": list(
                getattr(box, "coordinated_units", ()) or ()
            ),
            "active_unit": getattr(box, "active_unit", ""),
            "active_units": list(getattr(box, "active_units", []) or []),
            "active_lane": getattr(box, "active_lane", ""),
            "active_lanes": list(getattr(box, "active_lanes", []) or []),
            "coordinated_unit_statuses": (
                _coordinated_unit_status_payload(box)
            ),
            "active_environment": getattr(box, "active_environment", ""),
            "active_pid": int(getattr(box, "active_pid", 0) or 0),
            "restarts": int(getattr(box, "restart_count", 0) or 0),
            "restart_count_semantics": _box_restart_count_semantics(box),
            "unit_resolution_error": getattr(box, "unit_resolution_error", ""),
            "unexpected_active_units": list(
                getattr(box, "unexpected_active_units", []) or []
            ),
            "active_unit_registry": dict(
                getattr(box, "active_unit_registry", {}) or {}
            ),
            "lane_status": _box_lane_status(box, validator_state=vs),
            "quarantine_active": bool(getattr(box, "quarantine_active", False)),
            "standalone": {
                "configured": bool(
                    getattr(box, "standalone_telemetry_path", "")
                ),
                "fresh": bool(getattr(box, "standalone_fresh", False)),
                "age_s": getattr(box, "standalone_age_s", -1.0),
                "error": getattr(box, "standalone_error", ""),
            },
            "certification": dict(
                getattr(box, "standalone_certification", {}) or {}
            ),
            "issues": issues,
            "model": _box_model_details(box, vs),
            "code_auction": _box_code_auction_details(
                box,
                vs,
                now=now,
                health_stale_s=poll_limit,
            ),
            "code_selector_crossover": (
                _box_code_selector_crossover_details(
                    box,
                    vs,
                    now=now,
                    stale_after_s=poll_limit,
                )
            ),
            "code_overlap_abba": _box_code_overlap_abba_details(
                box,
                vs,
                now=now,
                stale_after_s=poll_limit,
            ),
        })
    # Lab status is additive observability only. A failed or idle offline lab
    # must never make the live miner fleet healthy or unhealthy.
    lab_details = [
        _lab_payload(
            lab,
            now=now,
            validator_window=int(getattr(vs, "window", 0) or 0),
        )
        for lab in labs
    ]
    component_details = [
        _component_payload(
            component,
            boxes=boxes,
            chain_entries=chain_entries,
            now=now,
        )
        for component in components
    ]

    r2 = _r2_status_snapshot()
    r2_last_success = float(
        r2.get("last_success_at", r2.get("last_fetch_at", 0.0)) or 0.0
    )
    r2_error = str(r2.get("last_error", r2.get("error", "")) or "")
    r2_age = age(r2_last_success)
    latest_window = int(getattr(windows[0], "n", 0) or 0) if windows else int(
        r2.get("latest_window", r2.get("latest_window_n", 0)) or 0
    )
    terminal_windows = sum(
        1 for window in windows if getattr(window, "terminal_data_present", False)
    )
    completed_windows = sum(
        1
        for window in windows
        if getattr(window, "terminal_data_present", False)
        and getattr(window, "window_status", "completed") == "completed"
    )
    aborted_windows = sum(
        1
        for window in windows
        if getattr(window, "terminal_data_present", False)
        and getattr(window, "window_status", "completed") == "aborted"
    )
    exact_reward_windows = sum(
        1 for window in windows if getattr(window, "reward_data_present", False)
    )
    lifecycle_explicit = any(
        bool(getattr(window, "lifecycle_explicit", False)) for window in windows
    )
    coverage = r2.get("coverage") if isinstance(r2.get("coverage"), dict) else {}
    coverage_complete = bool(
        r2.get("coverage_complete", coverage.get("complete", False))
    )
    validator_window = int(getattr(vs, "window", 0) or 0)
    window_gap = validator_window - latest_window if validator_window and latest_window else None
    latest_fresh = bool(
        latest_window and (
            window_gap is None or -1 <= window_gap <= 4
        )
    )

    health_is_fresh = bool(
        health_age is not None and health_age <= poll_limit
        and not bool(getattr(vs, "health_error", ""))
        and getattr(vs, "health_status", "") == "ok"
    )
    direct_state_is_fresh = bool(
        state_age is not None and state_age <= poll_limit
        and getattr(vs, "error", "") in ("", "ssh-state")
    )
    health_raw = getattr(vs, "health_raw", {})
    if not isinstance(health_raw, dict):
        health_raw = {}
    health_state = str(health_raw.get("current_validator_state") or "").lower()
    try:
        health_window = int(health_raw.get("current_window_n") or 0)
    except (TypeError, ValueError):
        health_window = 0
    # `/state` legitimately has no active batcher during the non-submit READY
    # interval (and can transiently disappear at the end of publishing).  A
    # fresh `/health` snapshot is authoritative for phase, window and model
    # identity then.  Never use this fallback for OPEN: miners need the full
    # `/state` randomness/cooldown payload before an open lane is ready.
    health_non_open_state_is_fresh = bool(
        health_is_fresh
        and health_state in {"training", "publishing", "ready"}
        and health_window > 0
        and health_window == validator_window
    )
    validator_state_is_fresh = (
        direct_state_is_fresh or health_non_open_state_is_fresh
    )
    validator_state_source = (
        "/state"
        if direct_state_is_fresh
        else ("/health (non-open)" if health_non_open_state_is_fresh else "")
    )
    validator_liveness = _validator_liveness_details(vs, windows)
    seal_drain_ok = not bool(
        validator_liveness.get("seal_timeout_environments")
    )
    archive_liveness = validator_liveness.get("archive")
    archive_liveness = (
        archive_liveness if isinstance(archive_liveness, dict) else {}
    )
    archive_gap_count = archive_liveness.get("enqueue_gaps_total")
    archive_continuity_ok = not (
        archive_liveness.get("reported") is True
        and isinstance(archive_gap_count, int)
        and not isinstance(archive_gap_count, bool)
        and archive_gap_count > 0
    )

    checks = {
        "poll_fresh": poll_age is not None and poll_age <= poll_limit,
        "fleet_configured": bool(boxes),
        "fleet_ready": bool(boxes) and all(item["ok"] for item in box_details),
        # Unattached components are prospective capacity. Once an immutable
        # controller binding makes one part of the live topology, its health
        # becomes a readiness requirement.
        "attached_components_ready": all(
            item["ok"]
            for item in component_details
            if item["controller_binding"]["attached"]
        ),
        "validator_state_fresh": validator_state_is_fresh,
        "validator_health_fresh": health_is_fresh,
        "validator_verdicts_fresh": (
            verdict_age is not None and verdict_age <= verdict_limit
            and not bool(getattr(vs, "verdicts_error", ""))
        ),
        "r2_fresh": r2_age is not None and r2_age <= r2_limit and not r2_error,
        "r2_cache_ready": bool(windows)
        and terminal_windows == len(windows)
        and exact_reward_windows == completed_windows,
        "r2_history_complete": coverage_complete,
        "r2_latest_window_fresh": latest_fresh,
        # Additive telemetry is fail-open for old validator schemas. Once a
        # build explicitly reports a destructive seal timeout or archive
        # enqueue gap, readiness must stop claiming the full pipeline is OK.
        "validator_seal_drain_ok": seal_drain_ok,
        "validator_archive_continuity_ok": archive_continuity_ok,
    }
    ok = all(checks.values())
    return {
        "ok": ok,
        "status": "ok" if ok else "degraded",
        "checks": checks,
        "polls": poll_count,
        "request_budget": _request_budget(effective_refresh),
        "thresholds_s": {
            "poll": poll_limit,
            "verdicts": verdict_limit,
            "r2": r2_limit,
        },
        "ages_s": {
            "poll": poll_age,
            "validator_state": state_age,
            "validator_health": health_age,
            "validator_verdicts": verdict_age,
            "r2": r2_age,
        },
        "fleet": box_details,
        "components": component_details,
        "labs": lab_details,
        "validator": {
            "window": validator_window,
            "state": str(getattr(vs, "state", "") or ""),
            "state_source": validator_state_source,
            "state_error": vs.error,
            "health_status": getattr(vs, "health_status", ""),
            "health_error": getattr(vs, "health_error", ""),
            "verdicts_error": getattr(vs, "verdicts_error", ""),
            "verdicts_warning": getattr(vs, "verdicts_warning", ""),
            "model_identity": _validator_model_identity(vs),
            "liveness": validator_liveness,
        },
        "r2": {
            **r2,
            "latest_window": latest_window,
            "exact_reward_windows": exact_reward_windows,
            "requested_windows": len(windows),
            "validator_window_gap": window_gap,
            **(
                {
                    "terminal_windows": terminal_windows,
                    "completed_windows": completed_windows,
                    "aborted_windows": aborted_windows,
                }
                if lifecycle_explicit
                else {}
            ),
        },
    }


def render_export_json() -> dict:
    """JSON export of the full state for offline analysis or alerting."""
    from settings import SETTINGS as runtime_settings

    with _lock:
        validator_state = _vs
        chain_by_hotkey = {
            getattr(h, "hotkey", ""): h for h in _chain.hotkeys
        }
        chain_hotkeys = set(chain_by_hotkey)
        chain_seen = bool(_chain.last_fetch_at)
        watched_hotkeys = _watched_hotkey_rows()
        targets_data = _configured_target_rows(chain_hotkeys, chain_seen)
        boxes_data = [
            {
                "label": b.label,
                "miner_kind": getattr(b, "miner_kind", "legacy"),
                "miner": (
                    _reliquary_one_public_projection(b, list(_windows))
                    if getattr(b, "miner_kind", "legacy") == "reliquary_one"
                    else {}
                ),
                "alias": _display_ssh_alias(b.alias),
                "unit": b.unit,
                "configured_units": [
                    str(spec[0]) for spec in getattr(b, "unit_candidates", ())
                ] or ([str(b.unit)] if b.unit else []),
                "host_allowed_units": list(
                    getattr(b, "host_unit_allowlist", ()) or ()
                ),
                "coordinated_units": list(
                    getattr(b, "coordinated_units", ()) or ()
                ),
                "active_unit": getattr(b, "active_unit", ""),
                "active_units": list(getattr(b, "active_units", []) or []),
                "active_lane": getattr(b, "active_lane", ""),
                "active_lanes": list(getattr(b, "active_lanes", []) or []),
                "coordinated_unit_statuses": (
                    _coordinated_unit_status_payload(b)
                ),
                "active_environment": getattr(b, "active_environment", ""),
                "active_pid": int(getattr(b, "active_pid", 0) or 0),
                "miner_unit_enablement": getattr(
                    b, "miner_unit_enablement", ""
                ),
                "active_started_at": int(
                    getattr(b, "active_started_at", 0) or 0
                ),
                "unit_resolution_error": getattr(b, "unit_resolution_error", ""),
                "unexpected_active_units": list(
                    getattr(b, "unexpected_active_units", []) or []
                ),
                "active_unit_registry": dict(
                    getattr(b, "active_unit_registry", {}) or {}
                ),
                "lane_status": _box_lane_status(
                    b,
                    validator_state=validator_state,
                ),
                "env_file_ok": bool(getattr(b, "env_file_ok", False)),
                "env_file_error": getattr(b, "env_file_error", ""),
                "operator": getattr(b, "operator", ""),
                "uid": (
                    int(getattr(chain_by_hotkey[b.hotkey], "uid", -1))
                    if b.hotkey in chain_by_hotkey
                    else None
                ),
                "hotkey": b.hotkey, "alive": b.proc_alive,
                "hotkey_short": _short_hotkey(b.hotkey),
                "hotkey_prefix": b.hotkey[:12] if b.hotkey else "",
                "uptime_s": b.proc_uptime_s,
                "last_poll_s": b.last_poll_s,
                "gpu_mem_mb": b.gpu_mem_mb, "gpu_total_mb": b.gpu_total_mb,
                "gpu_util": b.gpu_util,
                "cpu_pct": b.cpu_pct, "rss_mb": b.rss_mb,
                "disk_used_pct": b.disk_used_pct,
                "restart_count": b.restart_count,
                "restart_count_semantics": _box_restart_count_semantics(b),
                "acpt_30m": b.acpt_30m, "acpt_60m": b.acpt_60m,
                "pregen_30m": _box_rolling_pregen(b)[0],
                "pregen_60m": _box_rolling_pregen(b)[1],
                "pregen_source": _box_rolling_pregen(b)[2],
                "late_drops_30m": b.late_drops_30m, "late_drops_60m": b.late_drops_60m,
                "rej_30m": b.rej_30m,
                "window_mismatch_30m": b.window_mismatch_30m,
                "grail_fail_30m": b.grail_fail_30m, "bad_term_30m": b.bad_term_30m,
                "skip_30m": b.skip_30m, "pregen_fail_30m": b.pregen_fail_30m,
                "oom_60m": b.oom_60m, "last_oom_age_s": b.last_oom_age_s,
                "mean_accept_t_s": b.mean_accept_t_s,
                "miner_state": b.miner_state,
                "miner_window": b.miner_window or (
                    int(_vs.window)
                    if getattr(b, "reference_ready", False) else 0
                ),
                "miner_valid": b.miner_valid, "miner_ready": b.miner_ready,
                "miner_inflight": b.miner_inflight,
                "miner_submitted_this_win": b.miner_submitted_this_win,
                "fresh_built_30m": b.fresh_built_30m,
                "cache_fwd_30m": b.cache_fwd_30m,
                "prefinalized_30m": b.prefinalized_30m,
                "burst_30m": b.burst_30m,
                "late_grace_30m": b.late_grace_30m,
                "batch_filled_30m": b.batch_filled_30m,
                "checkpoint_restarts_60m": b.checkpoint_restarts_60m,
                "miner_environment": getattr(b, "miner_environment", ""),
                "engine_mode": getattr(b, "engine_mode", ""),
                "protocol_profile": getattr(b, "protocol_profile", ""),
                "runtime_parity_ok": bool(getattr(b, "runtime_parity_ok", False)),
                "reference_ready": bool(getattr(b, "reference_ready", False)),
                "standalone": {
                    "configured": bool(
                        getattr(b, "standalone_telemetry_path", "")
                    ),
                    "telemetry_path": getattr(
                        b, "standalone_telemetry_path", ""
                    ),
                    "runtime_manifest_path": getattr(
                        b, "standalone_runtime_manifest_path", ""
                    ),
                    "supervisor_status_path": getattr(
                        b, "standalone_supervisor_status_path", ""
                    ),
                    "generated_at": getattr(
                        b, "standalone_generated_at", 0.0
                    ),
                    "age_s": getattr(b, "standalone_age_s", -1.0),
                    "progress_age_s": getattr(
                        b, "standalone_progress_age_s", -1.0
                    ),
                    "fresh": bool(getattr(b, "standalone_fresh", False)),
                    "error": getattr(b, "standalone_error", ""),
                    "telemetry": (
                        {}
                        if _active_standalone_certification(b)
                        else getattr(b, "standalone_telemetry", {})
                    ),
                    "runtime_components": list(
                        getattr(b, "runtime_components", []) or []
                    ),
                    "supervisor": dict(
                        getattr(b, "standalone_supervisor", {}) or {}
                    ),
                },
                "certification": dict(
                    getattr(b, "standalone_certification", {}) or {}
                ),
                "miner_source_revision": getattr(b, "miner_source_revision", ""),
                "reliquary_source_revision": getattr(b, "reliquary_source_revision", ""),
                "observed_validator_image_revision": getattr(
                    b, "observed_validator_image_revision", ""
                ),
                "source_manifest_provisioned_ok": bool(getattr(
                    b, "source_manifest_provisioned_ok", False
                )),
                "provisioned_model_kind": getattr(
                    b, "provisioned_model_kind", ""
                ),
                "provisioned_checkpoint_n": getattr(
                    b, "provisioned_checkpoint_n", -1
                ),
                "provisioned_model_repo": getattr(
                    b, "provisioned_model_repo", ""
                ),
                "provisioned_model_revision": getattr(
                    b, "provisioned_model_revision", ""
                ),
                "runtime_checkpoint_n": int(
                    getattr(b, "runtime_checkpoint_n", 0) or 0
                ),
                "runtime_checkpoint_repo": getattr(
                    b, "runtime_checkpoint_repo", ""
                ),
                "runtime_checkpoint_revision": getattr(
                    b, "runtime_checkpoint_revision", ""
                ),
                "base_model_repo": getattr(b, "base_model_repo", ""),
                "base_model_revision": getattr(b, "base_model_revision", ""),
                "model": _box_model_details(b, _vs),
                "code_auction": _box_code_auction_details(b, _vs),
                "code_selector_crossover": (
                    _box_code_selector_crossover_details(b, _vs)
                ),
                "code_overlap_abba": _box_code_overlap_abba_details(
                    b, _vs
                ),
                "readiness_issues": _box_readiness_issues(
                    b, validator_state=_vs
                ),
                "quarantine_active": bool(getattr(b, "quarantine_active", False)),
                "quarantine_path": getattr(b, "quarantine_path", ""),
                "quarantine_reason": getattr(b, "quarantine_reason", ""),
                "quarantine_at": getattr(b, "quarantine_at", 0.0),
                "acceptance_source": getattr(b, "acceptance_source", "events"),
                "final_accept_30m": getattr(b, "final_accept_30m", 0),
                "final_accept_60m": getattr(b, "final_accept_60m", 0),
                "final_reject_30m": getattr(b, "final_reject_30m", 0),
                "final_reject_60m": getattr(b, "final_reject_60m", 0),
                "last_final_reason": getattr(b, "last_final_reason", ""),
                "last_final_window": getattr(b, "last_final_window", 0),
                "last_final_ts": getattr(b, "last_final_ts", 0.0),
                "frontier_entries": b.frontier_entries,
                "frontier_content_entries": getattr(
                    b, "frontier_content_entries", 0
                ),
                "frontier_age_s": b.frontier_age_s,
                "frontier_newest_age_s": b.frontier_newest_age_s,
                "frontier_checkpoint_n": getattr(
                    b, "frontier_checkpoint_n", 0
                ),
                "frontier_checkpoint_revision": getattr(
                    b, "frontier_checkpoint_revision", ""
                ),
                "frontier_ckpts": b.frontier_ckpts,
                "local_checkpoint_n": getattr(b, "local_checkpoint_n", 0),
                "local_checkpoint_revision": getattr(
                    b, "local_checkpoint_revision", ""
                ),
                "state_relay_ok": b.state_relay_ok,
                "state_relay_age_s": b.state_relay_age_s,
                "state_relay_upstream_ms": b.state_relay_upstream_ms,
                "state_relay_failures": b.state_relay_failures,
                "watchdog_ok": b.watchdog_ok,
                "watchdog_age_s": b.watchdog_age_s,
                "watchdog_stale_strikes": b.watchdog_stale_strikes,
                "watchdog_restart_count": b.watchdog_restart_count,
                "error": b.error,
            }
            for b in _boxes
        ]
        labs_data = [
            _lab_payload(
                lab,
                validator_window=int(getattr(_vs, "window", 0) or 0),
            )
            for lab in _labs
        ]
        components_data = [
            _component_payload(
                component,
                boxes=list(_boxes),
                chain_entries=chain_by_hotkey,
            )
            for component in _components
        ]
        windows_data = [
            {
                "n": w.n,
                "ours": w.ours,
                "total": w.total_batch,
                "rt_first": w.rt_first,
                "rejects": w.rejects,
                "reward_ours": getattr(w, "reward_ours", 0.0),
                "reward_total": getattr(w, "reward_total", 0.0),
                "reward_data_present": bool(getattr(w, "reward_data_present", False)),
                **(
                    {
                        "window_status": getattr(w, "window_status", "completed"),
                        "terminal_data_present": bool(
                            getattr(w, "terminal_data_present", False)
                        ),
                        "failure_stage": getattr(w, "failure_stage", ""),
                        "failure_type": getattr(w, "failure_type", ""),
                        "lifecycle_explicit": True,
                        "archive_schema_version": int(
                            getattr(w, "archive_schema_version", 0) or 0
                        ),
                        "auction_seal_drain_by_environment": getattr(
                            w, "auction_seal_drain_by_environment", {}
                        ),
                    }
                    if getattr(w, "lifecycle_explicit", False)
                    else {}
                ),
                "rewards_by_hotkey": getattr(w, "rewards_by_hotkey", {}),
                "environments": getattr(w, "environments", []),
                "environment_counts": getattr(w, "environment_counts", {}),
                "ours_by_environment": getattr(w, "ours_by_environment", {}),
                "rt_first_by_environment": getattr(w, "rt_first_by_environment", {}),
                "reward_ours_by_environment": getattr(w, "reward_ours_by_environment", {}),
                "reward_ours_by_environment_exact": bool(getattr(
                    w, "reward_ours_by_environment_exact", False
                )),
                "batch": getattr(w, "batch", []),
                "runners_up": getattr(w, "runners_up", []),
                "rejected": getattr(w, "rejected", []),
                "force_seal_reason": getattr(w, "force_seal_reason", ""),
                "validator_hotkey": getattr(w, "validator_hotkey", ""),
                "randomness": getattr(w, "randomness", ""),
                "rewarded_but_not_selected_by_hotkey": getattr(
                    w, "rewarded_but_not_selected_by_hotkey", {}
                ),
                "late_drops": getattr(w, "late_drops", {}),
                "reject_summary": getattr(w, "reject_summary", {}),
                "grader_failures": getattr(w, "grader_failures", {}),
                "training_quarantine": getattr(w, "training_quarantine", {}),
                "difficulty_auction_shadow": getattr(w, "difficulty_auction_shadow", {}),
                "server_reject_summary": getattr(w, "server_reject_summary", {}),
                "logical_group_dedup": getattr(w, "logical_group_dedup", {}),
                "training_accumulator": getattr(w, "training_accumulator", {}),
            }
            for w in _windows
        ]
        chain_data = {
            "total_stake": _chain.total_stake,
            "subnet_share": _chain.subnet_share,
            "last_fetch_at": _chain.last_fetch_at,
            "source_alias": _chain.source_alias,
            "error": _chain.error,
            "watched_count": len(watched_hotkeys),
            "registered_watched_count": len(_chain.hotkeys),
            "hotkeys": [
                {"label": h.label, "hotkey": getattr(h, "hotkey", ""),
                 "uid": h.uid, "stake": h.stake, "emission": h.emission}
                for h in _chain.hotkeys
            ],
        }
    return {
        "ts": int(time.time()),
        "request_budget": _request_budget(runtime_settings.refresh_seconds),
        "validator": {
            "window": _vs.window,
            "state": _vs.state,
            "valid": _vs.valid,
            "checkpoint_revision": _vs.checkpoint_revision,
            "checkpoint_repo_id": _vs.checkpoint_repo_id,
            "checkpoint_n": _vs.checkpoint_n,
            "model_identity": _validator_model_identity(_vs),
            "env_name": _vs.env_name,
            "state_raw": getattr(_vs, "state_raw", {}),
            "last_fetch_at": getattr(_vs, "last_fetch_at", 0.0),
            "health_last_fetch_at": getattr(_vs, "health_last_fetch_at", 0.0),
            "health_error": getattr(_vs, "health_error", ""),
            "health_status": getattr(_vs, "health_status", ""),
            "health_raw": getattr(_vs, "health_raw", {}),
            "image_revision": getattr(_vs, "image_revision", ""),
            "protocol_version": getattr(_vs, "protocol_version", 0),
            "generation_profile_id": getattr(
                _vs, "generation_profile_id", ""
            ),
            "generation_contract_sha256": getattr(
                _vs, "generation_contract_sha256", ""
            ),
            "checkpoint_profile_sha256": getattr(
                _vs, "checkpoint_profile_sha256", ""
            ),
            "app_started_at": getattr(_vs, "app_started_at", 0.0),
            "batch_size": getattr(_vs, "batch_size", 0),
            "queue_depth": getattr(_vs, "queue_depth", 0),
            "proof_admission_count": getattr(_vs, "proof_admission_count", 0),
            "proof_admission_limit": getattr(_vs, "proof_admission_limit", 0),
            "proof_verification_inflight": getattr(_vs, "proof_verification_inflight", 0),
            "pending_proof_reservations": getattr(_vs, "pending_proof_reservations", 0),
            "inflight_proof_reservations": getattr(_vs, "inflight_proof_reservations", 0),
            "current_quicknet_drand_round": getattr(_vs, "current_quicknet_drand_round", 0),
            "current_window_open_drand_round": getattr(_vs, "current_window_open_drand_round", 0),
            "forced_seed_enforced": bool(getattr(_vs, "forced_seed_enforced", False)),
            "forced_seed_cdf_enforced": bool(getattr(_vs, "forced_seed_cdf_enforced", False)),
            "runtime_fingerprint": getattr(_vs, "runtime_fingerprint", {}),
            "window_environments": getattr(_vs, "window_environments", {}),
            "environment_targets": getattr(_vs, "environment_targets", {}),
            "prompt_sources": getattr(_vs, "prompt_sources", {}),
            "difficulty_auction_shadow_enabled": bool(getattr(_vs, "difficulty_auction_shadow_enabled", False)),
            "difficulty_auction_shadow_environments": getattr(_vs, "difficulty_auction_shadow_environments", []),
            "recent_reject_counts": getattr(_vs, "recent_reject_counts", {}),
            "archive_queue_depth": getattr(_vs, "archive_queue_depth", 0),
            "liveness": _validator_liveness_details(_vs, _windows),
            "verdicts_by_hotkey": getattr(_vs, "verdicts_by_hotkey", {}),
            "verdicts_last_fetch_at": getattr(_vs, "verdicts_last_fetch_at", 0.0),
            "verdicts_error": getattr(_vs, "verdicts_error", ""),
            "verdicts_warning": getattr(_vs, "verdicts_warning", ""),
            "error": _vs.error,
        },
        "targets": targets_data,
        "watched_hotkeys": watched_hotkeys,
        "fleet": boxes_data,
        "components": components_data,
        "labs": labs_data,
        "windows": windows_data,
        "r2": _r2_status_snapshot(),
        "chain": chain_data,
        "rtt_ms": dict(_rtt.rtt_ms),
        "baseline": {
            "current": _baseline.current,
            "baseline_6h": _baseline.baseline_6h,
            "drift_pct": _baseline.drift_pct,
            "alert": _baseline.alert,
        },
    }


# ---------------------------------------------------------------------------
# Dedicated /logs page — full-buffer, filterable, no truncation
# ---------------------------------------------------------------------------
# The main dashboard's two event panels (events + valevents) are 280-char
# truncated tails with no filtering — fine at a glance but useless for
# forensic work. The /logs page exposes the full deque (up to 6000
# events ≈ 2 h of history) with:
#   - per-kind toggle pills (accept / reject / late_drop / seal / fail / randomness)
#   - reject-reason substring filter
#   - hotkey substring filter
#   - "ours only" toggle
#   - tail toggle (live auto-refresh vs paused for copy/paste)
#   - no truncation, no cooking — raw `msg` is preserved verbatim
#
# This page is the operator's primary entry point when something has
# gone wrong and they need to drill into the validator log stream.

LOG_KINDS = ["accept", "reject", "late_drop", "selected", "reward", "seal", "fail",
             "worker_fail", "window_timeout",
             "randomness_retry", "randomness_ok"]


def render_logs_table_html(
    kinds: set[str] | None,
    reason: str | None,
    hotkey: str | None,
    ours_only: bool,
    limit: int,
    *,
    events: list | None = None,
    buffer_total: int | None = None,
) -> str:
    """Render the /logs results table as an HTMX fragment.

    Newest-first ordering for fast scanning. Each row keeps the full
    raw `msg` text so an operator can copy-paste a line into a bug
    report or grep for a substring.
    """
    if events is None:
        events = validator_events_query(
            kinds=kinds, reason_substr=reason or None,
            hotkey_substr=hotkey or None, ours_only=ours_only,
            limit=limit,
        )
    # Buffer-wide totals (independent of filter) for the header strip.
    buf_total = (
        len(validator_events_snapshot(10_000))
        if buffer_total is None
        else int(buffer_total)
    )
    rows: list[str] = []
    if not events:
        rows.append(
            "<tr><td colspan='5' class='dim small' style='padding:18px;text-align:center'>"
            "no events match the current filter "
            "(or the tail buffer is empty)"
            "</td></tr>"
        )
    for e in reversed(events):  # newest first
        ours_chip = ""
        if e.ours:
            ours_chip = (
                "<span class='chip-ours' title='Hotkey is in our fleet'>★</span>"
            )
        if e.kind == "accept":
            kind_color = "var(--amber)"
        elif e.kind == "reject":
            kind_color = "var(--reject)"
        elif e.kind == "late_drop":
            kind_color = "var(--reject)"
        elif e.kind == "seal":
            kind_color = "var(--ledger)"
        elif e.kind == "selected":
            kind_color = "var(--ledger)"
        elif e.kind == "reward":
            kind_color = "var(--amber)"
        elif e.kind == "fail":
            kind_color = "var(--reject)"
        elif e.kind in ("worker_fail", "window_timeout"):
            kind_color = "var(--reject)"
        elif e.kind == "randomness_retry":
            kind_color = "var(--amber)"
        else:
            kind_color = "var(--stone)"
        reason_html = ""
        if e.kind == "reject" and e.reject_reason:
            meta = reject_meta(e.reject_reason)
            r_color = reject_color(meta["sev"])
            badges = ""
            if meta.get("v23"):
                badges = (
                    "<span class='chip-v23' title='introduced or changed in v2.3'>v2.3</span>"
                )
            if meta.get("deprecated"):
                badges = (
                    "<span class='chip-dep' "
                    "title='deprecated in v2.3+; kept for historical archives'>dep</span>"
                )
            reason_html = (
                f"<span class='log-reason' style='color:{r_color}' "
                f"title='{html.escape(str(meta['explain']))}'>"
                f"{html.escape(str(meta['label']))}{badges}</span>"
            )
        elif e.kind == "late_drop":
            reason_html = (
                "<span class='log-reason' style='color:var(--reject)' "
                "title='HTTP-accepted submission dropped because the batcher window already advanced. "
                "Increases on FIFO race losses; not the miner's fault if rare, but consistent late_drops mean this box is too slow.'>"
                "late_drop</span>"
            )
        hk_html = (
            f"<code class='hk' title='{html.escape(e.hotkey12)}…'>"
            f"{html.escape(e.hotkey12)}{'…' if e.hotkey12 else ''}"
            f"</code>"
            if e.hotkey12 else "<span class='dim'>—</span>"
        )
        msg = _clean_operator_msg(e.msg)
        if not msg:
            continue
        msg_full = html.escape(msg)
        rows.append(
            f"<tr class='log-row' data-ours='{1 if e.ours else 0}'>"
            f"<td class='log-ts mono dim'>{html.escape(e.ts)}</td>"
            f"<td class='log-kind mono' style='color:{kind_color}'>"
            f"{ours_chip}{html.escape(e.kind)}</td>"
            f"<td class='log-reason-cell'>{reason_html}</td>"
            f"<td class='log-hk'>{hk_html}</td>"
            f"<td class='log-msg mono'>{msg_full}</td>"
            f"</tr>"
        )
    if not rows:
        rows.append(
            "<tr><td colspan='5' class='dim small' style='padding:18px;text-align:center'>"
            "only low-signal score/debug lines matched this filter"
            "</td></tr>"
        )
    rendered = len(events)
    meta_line = (
        f"<span class='dim'>showing {rendered} / {buf_total} buffered "
        f"({_count_kinds(events)})</span>"
    )
    return (
        f"<div class='log-meta'>{meta_line}</div>"
        f"<table class='log-table'>"
        f"<thead><tr>"
        f"<th class='log-th-ts'>ts</th>"
        f"<th class='log-th-kind'>kind</th>"
        f"<th class='log-th-reason'>reason</th>"
        f"<th class='log-th-hk'>hotkey</th>"
        f"<th class='log-th-msg'>raw</th>"
        f"</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        f"</table>"
    )


def _count_kinds(events: list) -> str:
    """Compact "kind=N · kind=N …" counter string for the meta strip."""
    counter: Counter = Counter(e.kind for e in events)
    if not counter:
        return "no events"
    parts: list[str] = []
    for kind, n in counter.most_common():
        color = {
            "accept": "var(--amber)",
            "reject": "var(--reject)",
            "late_drop": "var(--reject)",
            "seal": "var(--ledger)",
            "selected": "var(--ledger)",
            "reward": "var(--amber)",
            "fail": "var(--reject)",
        }.get(kind, "var(--stone)")
        parts.append(
            f"<span class='mono small' style='color:{color}'>"
            f"{kind}={n}</span>"
        )
    return " · ".join(parts)


LOGS_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="dark">
<meta name="theme-color" content="#0a0a0b">
<title>Reliquary Fleet · Logs</title>
<link rel="icon" href="/static/brand/relic.svg?v={asset_version}" type="image/svg+xml">
<link rel="preload" href="/static/fonts/geist-sans.woff2" as="font" type="font/woff2" crossorigin>
<link rel="preload" href="/static/fonts/jetbrains-mono.woff2" as="font" type="font/woff2" crossorigin>
<script src="/static/htmx.min.js" defer></script>
<link rel="stylesheet" href="/static/logs.css?v={asset_version}">
</head>
<body>
<header class="logs-header">
  <div class="logs-brand">
    <img src="/static/brand/mark-outline.svg?v={asset_version}" alt="" width="24" height="24">
    <h1>Reliquary Fleet · Logs <span>full local buffer</span></h1>
  </div>
  <nav aria-label="Log tools">
    <a href="/" aria-label="Back to dashboard" title="Back to dashboard">← Dashboard</a>
    <a href="/api/export.json" target="_blank" aria-label="Download JSON export" title="Download JSON export">↓ JSON</a>
  </nav>
</header>

<form class="filter-bar" id="filter-bar">
  <fieldset class="kind-fieldset">
    <legend>kinds</legend>
    <span id="kind-pills">
      <button type="button" class="kind-pill" data-kind="accept" data-on="1" aria-pressed="true">accept</button>
      <button type="button" class="kind-pill" data-kind="reject" data-on="1" aria-pressed="true">reject</button>
      <button type="button" class="kind-pill" data-kind="late_drop" data-on="1" aria-pressed="true">late_drop</button>
      <button type="button" class="kind-pill" data-kind="selected" data-on="1" aria-pressed="true">selected</button>
      <button type="button" class="kind-pill" data-kind="reward" data-on="1" aria-pressed="true">reward</button>
      <button type="button" class="kind-pill" data-kind="seal" data-on="0" aria-pressed="false">seal</button>
      <button type="button" class="kind-pill" data-kind="fail" data-on="1" aria-pressed="true">fail</button>
      <button type="button" class="kind-pill" data-kind="worker_fail" data-on="1" aria-pressed="true">worker_fail</button>
      <button type="button" class="kind-pill" data-kind="window_timeout" data-on="1" aria-pressed="true">window_timeout</button>
      <button type="button" class="kind-pill" data-kind="randomness_retry" data-on="0" aria-pressed="false">randomness_retry</button>
      <button type="button" class="kind-pill" data-kind="randomness_ok" data-on="0" aria-pressed="false">randomness_ok</button>
    </span>
    <input type="hidden" name="kinds" id="kinds-input" value="accept,reject,late_drop,selected,reward,fail,worker_fail,window_timeout">
  </fieldset>
  <label>reason <input type="text" name="reason" placeholder="grail_fail / wrong_…"></label>
  <label>hotkey <input type="text" name="hotkey" placeholder="5D76… / 5Grw…"></label>
  <label class="toggle"><input type="checkbox" name="ours" value="1"> ours only</label>
  <label>limit
    <select name="limit" class="filter-select">
      <option value="200">200</option>
      <option value="500" selected>500</option>
      <option value="1500">1500</option>
      <option value="6000">all (6000)</option>
    </select>
  </label>
  <button type="button" class="pause-btn" id="pause-btn" data-paused="0" aria-pressed="false">● Live</button>
</form>

<div id="log-out" aria-busy="true">
  <div style="padding:24px;text-align:center;color:var(--stone)" role="status">Loading...</div>
</div>

<script src="/static/logs.js?v={asset_version}" defer></script>
</body>
</html>
"""


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="dark">
<meta name="theme-color" content="#0a0a0b">
<title>Reliquary Fleet</title>
<link rel="icon" href="/static/brand/relic.svg?v={asset_version}" type="image/svg+xml">
<link rel="preload" href="/static/fonts/geist-sans.woff2" as="font" type="font/woff2" crossorigin>
<link rel="preload" href="/static/fonts/jetbrains-mono.woff2" as="font" type="font/woff2" crossorigin>
<script src="/static/htmx.min.js" defer></script>
<link rel="stylesheet" href="/static/dashboard.css?v={asset_version}">
</head>
<body class="snapshot-pending" data-demo="{demo_mode}" data-refresh-seconds="{browser_refresh_s}">
<a class="skip-link" href="#main-content">Skip to dashboard</a>
<header class="app-header">
  <div class="brand-lockup">
    <img src="/static/brand/mark-outline.svg?v={asset_version}" alt="" width="26" height="26">
    <h1>Reliquary <span>Fleet</span></h1>
    <span class="live-label">live</span>
    {demo_badge}
  </div>
  <div class="subtitle header-meta">
    <span class="poll-status" title="Local dashboard refresh cadence"><span class="pulse"></span>{browser_refresh_s}s</span>
    <nav aria-label="Dashboard tools">
      <a class="tool-button" href="/logs" title="Open event logs" aria-label="Open event logs">
        <span class="tool-icon" aria-hidden="true">↗</span><span class="tool-label">Logs</span>
      </a>
      <a class="tool-button" href="/api/export.json" target="_blank" title="Download JSON export" aria-label="Download JSON export">
        <span class="tool-icon" aria-hidden="true">↓</span><span class="tool-label">JSON</span>
      </a>
      <button class="tool-button" type="button" id="sound-toggle" aria-pressed="false" title="Enable alert sound">
        <span class="tool-icon" data-sound-icon aria-hidden="true">♪</span><span class="tool-label" data-sound-label>Sound off</span>
      </button>
    </nav>
  </div>
</header>

<!-- Drill-down drawer (populated on row click) -->
<button id="drawer-backdrop" type="button" aria-label="Close miner details" hidden></button>
<aside id="drawer" role="dialog" aria-modal="true" aria-hidden="true" aria-label="Miner details" tabindex="-1" hx-target="this" hx-swap="innerHTML" inert></aside>

<main class="dashboard-shell" id="main-content">
  <div class="operator-command" aria-label="Reliquary One operator view">
    <div class="operator-columns">
      <div class="operator-column operator-column-primary">
        <section class="area-one-now dashboard-area"
         aria-busy="true"
         aria-label="Our miner now"
         data-panel="miner_now"
         hx-get="/api/miner-now"
         hx-trigger="fleet:refresh"
         hx-swap="innerHTML">
          <div class="panel skeleton-panel" aria-hidden="true"><span class="skeleton skeleton-heading"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row"></span></div>
        </section>
        <section class="area-one-attempts dashboard-area"
             aria-busy="true"
             aria-label="Our recent attempts"
             data-panel="miner_attempts"
             hx-get="/api/miner-attempts"
             hx-trigger="fleet:refresh"
             hx-swap="innerHTML">
          <div class="panel skeleton-panel" aria-hidden="true"><span class="skeleton skeleton-heading"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row"></span></div>
        </section>
      </div>
      <div class="operator-column operator-column-secondary">
        <section class="area-one-pipeline dashboard-area"
         aria-busy="true"
         aria-label="Current window pipeline"
         data-panel="miner_pipeline"
         hx-get="/api/miner-pipeline"
         hx-trigger="fleet:refresh"
         hx-swap="innerHTML">
          <div class="panel skeleton-panel" aria-hidden="true"><span class="skeleton skeleton-heading"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row short"></span></div>
        </section>
        <section class="area-one-auction dashboard-area"
         aria-busy="true"
         aria-label="Last sealed auction"
         data-panel="miner_auction"
         hx-get="/api/miner-auction"
         hx-trigger="fleet:refresh"
         hx-swap="innerHTML">
          <div class="panel skeleton-panel" aria-hidden="true"><span class="skeleton skeleton-heading"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row short"></span></div>
        </section>
        <section class="area-one-runtime dashboard-area"
         aria-busy="true"
         aria-label="Checkpoint and runtime"
         data-panel="miner_runtime"
         hx-get="/api/miner-runtime"
         hx-trigger="fleet:refresh"
         hx-swap="innerHTML">
          <div class="panel skeleton-panel" aria-hidden="true"><span class="skeleton skeleton-heading"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row short"></span></div>
        </section>
      </div>
    </div>
    <section class="area-one-log dashboard-area"
         aria-busy="true"
         aria-label="Structured live log"
         data-panel="miner_log"
         hx-get="/api/miner-log"
         hx-trigger="fleet:refresh"
         hx-swap="innerHTML">
      <div class="panel skeleton-panel" aria-hidden="true"><span class="skeleton skeleton-heading"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row"></span></div>
    </section>
  </div>

  <details class="advanced-dashboard">
    <summary><span>Fleet, validator and network diagnostics</span><small>Advanced and historical panels</small></summary>
    <div class="advanced-dashboard-body">
  <div class="dashboard-core">
    <div class="dashboard-stack dashboard-stack-primary">
      <section class="area-score dashboard-area"
           aria-busy="true"
           aria-label="Window score"
           data-panel="score"
           hx-get="/api/scoreboard"
           hx-trigger="fleet:refresh"
           hx-swap="innerHTML">
        <div class="panel skeleton-panel" aria-hidden="true"><span class="skeleton skeleton-heading"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row short"></span></div>
      </section>
      <section class="area-windows dashboard-area"
           aria-busy="true"
           aria-label="Window history"
           data-panel="windows"
           hx-get="/api/windows"
           hx-trigger="fleet:refresh"
           hx-swap="innerHTML">
        <div class="panel skeleton-panel" aria-hidden="true"><span class="skeleton skeleton-heading"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row short"></span></div>
      </section>
      <section class="area-ema dashboard-area"
           aria-busy="true"
           aria-label="EMA leaderboard"
           data-panel="ema"
           hx-get="/api/ema?mode=top"
           hx-trigger="fleet:refresh"
           hx-swap="innerHTML"
           id="ema-area"
           data-mode="top">
        <div class="panel skeleton-panel" aria-hidden="true"><span class="skeleton skeleton-heading"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row short"></span></div>
      </section>
      <section class="area-fleet dashboard-area"
           aria-busy="true"
           aria-label="Fleet health"
           data-panel="fleet"
           hx-get="/api/fleet"
           hx-trigger="fleet:refresh"
           hx-swap="innerHTML">
        <div class="panel skeleton-panel" aria-hidden="true"><span class="skeleton skeleton-heading"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row short"></span></div>
      </section>
    </div>

    <div class="dashboard-stack dashboard-stack-secondary">
      <section class="area-summary dashboard-area"
       aria-busy="true"
       aria-label="Operations overview"
       data-panel="summary"
       hx-get="/api/summary"
       hx-trigger="fleet:refresh"
       hx-swap="innerHTML">
        <div class="panel skeleton-panel" aria-hidden="true"><span class="skeleton skeleton-heading"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row short"></span></div>
      </section>
      <section class="area-forensics dashboard-area"
       aria-busy="true"
       aria-label="Window forensics"
       data-panel="forensics"
       hx-get="/api/window_forensics"
       hx-trigger="fleet:refresh"
       hx-swap="innerHTML">
        <div class="panel skeleton-panel" aria-hidden="true"><span class="skeleton skeleton-heading"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row short"></span></div>
      </section>
      <section class="area-pipeline dashboard-area"
       aria-busy="true"
       aria-label="GPU and reference pipeline"
       data-panel="pipeline"
       hx-get="/api/pipeline"
       hx-trigger="fleet:refresh"
       hx-swap="innerHTML">
        <div class="panel skeleton-panel" aria-hidden="true"><span class="skeleton skeleton-heading"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row short"></span></div>
      </section>
      <section class="area-frontier dashboard-area"
       aria-busy="true"
       aria-label="Frontier selection"
       data-panel="frontier"
       hx-get="/api/frontier"
       hx-trigger="fleet:refresh"
       hx-swap="innerHTML">
        <div class="panel skeleton-panel" aria-hidden="true"><h2>Prompt frontier</h2><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row short"></span></div>
      </section>
    </div>
  </div>

  <section class="area-labs dashboard-area"
       aria-busy="true"
       aria-label="Submit-disabled accelerator labs"
       data-panel="labs"
       hx-get="/api/labs"
       hx-trigger="fleet:refresh"
       hx-swap="innerHTML">
    <div class="panel skeleton-panel" aria-hidden="true"><span class="skeleton skeleton-heading"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row short"></span></div>
  </section>

  <div class="dashboard-tail">
    <section class="area-rundown dashboard-area"
       aria-busy="true"
       aria-label="Validator rundown"
       data-panel="rundown"
       hx-get="/api/validator_rundown?tf=30m"
       hx-trigger="fleet:refresh"
       hx-swap="innerHTML"
       id="rundown-area"
       data-tf="30m">
      <div class="panel skeleton-panel" aria-hidden="true"><span class="skeleton skeleton-heading"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row short"></span></div>
    </section>
    <section class="area-valevents dashboard-area"
       aria-busy="true"
       aria-label="Validator events"
       data-panel="validator_events"
       hx-get="/api/validator_events"
       hx-trigger="fleet:refresh"
       hx-swap="innerHTML">
      <div class="panel skeleton-panel" aria-hidden="true"><span class="skeleton skeleton-heading"></span><span class="skeleton skeleton-row"></span><span class="skeleton skeleton-row short"></span></div>
    </section>
  </div>

  <div class="area-diagnostics">
    <div class="diag-column">
      <div class="area-chain"
           aria-label="Chain and metagraph"
           data-panel="chain"
           hx-get="/api/chain"
           hx-trigger="fleet:refresh"
           hx-swap="innerHTML">
      </div>
      <div class="area-slotrank"
           aria-label="Canonical rank distribution"
           data-panel="slotrank"
           hx-get="/api/slotrank"
           hx-trigger="fleet:refresh"
           hx-swap="innerHTML">
      </div>
    </div>
    <div class="diag-column">
      <div class="area-baseline"
           aria-label="Throughput baseline"
           data-panel="baseline"
           hx-get="/api/baseline"
           hx-trigger="fleet:refresh"
           hx-swap="innerHTML">
      </div>
      <div class="area-competitors"
           aria-label="Top competing miners"
           data-panel="competitors"
           hx-get="/api/competitors"
           hx-trigger="fleet:refresh"
           hx-swap="innerHTML">
      </div>
    </div>
    <div class="diag-column">
      <div class="area-rtt"
           aria-label="Network latency"
           data-panel="rtt"
           hx-get="/api/rtt"
           hx-trigger="fleet:refresh"
           hx-swap="innerHTML">
      </div>
      <div class="area-quality"
           aria-label="Quality distribution"
           data-panel="quality"
           hx-get="/api/quality"
           hx-trigger="fleet:refresh"
           hx-swap="innerHTML">
      </div>
    </div>
    <div class="diag-column">
      <div class="area-events"
           aria-label="Live miner event tail"
           data-panel="events"
           hx-get="/api/events"
           hx-trigger="fleet:refresh"
           hx-swap="innerHTML">
      </div>
    </div>
  </div>
    </div>
  </details>
</main>

<script src="/static/dashboard.js?v={asset_version}" defer></script>
</body>
</html>
"""


_dashboard_snapshot_lock = threading.Lock()
_dashboard_snapshot_cache: dict[
    tuple[str, str, bool], tuple[float, dict[str, object]]
] = {}
_DASHBOARD_SNAPSHOT_CACHE_SECONDS = 0.75


def _invalidate_dashboard_snapshot_cache() -> None:
    with _dashboard_snapshot_lock:
        _dashboard_snapshot_cache.clear()


def render_dashboard_snapshot(
    *,
    ema_mode: str = "top",
    rundown_tf: str = "30m",
    include_advanced: bool = True,
) -> dict[str, object]:
    """Render one failure-isolated payload for the complete local dashboard."""
    normalized_ema = "active" if ema_mode == "active" else "top"
    normalized_tf = rundown_tf if rundown_tf in RUNDOWN_TIMEFRAMES else "30m"
    cache_key = (normalized_ema, normalized_tf, bool(include_advanced))
    now = time.time()
    with _dashboard_snapshot_lock:
        cached = _dashboard_snapshot_cache.get(cache_key)
        if cached and now - cached[0] <= _DASHBOARD_SNAPSHOT_CACHE_SECONDS:
            return dict(cached[1])

        renderers = {
            "miner_now": render_our_miner_now_html,
            "miner_pipeline": render_current_miner_pipeline_html,
            "miner_attempts": render_recent_miner_attempts_html,
            "miner_auction": render_last_sealed_auction_html,
            "miner_runtime": render_checkpoint_runtime_html,
            "miner_log": render_structured_miner_log_html,
        }
        if include_advanced:
            renderers.update(
                {
                    "score": render_scoreboard_html,
                    "windows": render_windows_html,
                    "ema": lambda: render_ema_leaderboard_html(
                        top_n=12,
                        mode=normalized_ema,
                    ),
                    "fleet": render_fleet_html,
                    "summary": render_fleet_summary_html,
                    "forensics": render_window_forensics_html,
                    "pipeline": render_pipeline_html,
                    "frontier": render_frontier_html,
                    "labs": render_labs_html,
                    "rundown": lambda: render_validator_rundown_html(
                        timeframe=normalized_tf
                    ),
                    "validator_events": render_validator_events_html,
                    "chain": render_chain_html,
                    "slotrank": render_slot_rank_html,
                    "baseline": render_baseline_html,
                    "competitors": render_competitors_html,
                    "rtt": render_rtt_html,
                    "quality": render_quality_html,
                    "events": render_events_html,
                }
            )
        panels: dict[str, str] = {}
        errors: dict[str, str] = {}
        for name, renderer in renderers.items():
            try:
                panels[name] = renderer()
            except Exception as exc:
                errors[name] = type(exc).__name__
        payload: dict[str, object] = {
            "schema_version": 1,
            "generated_at": now,
            "panels": panels,
            "errors": errors,
        }
        _dashboard_snapshot_cache[cache_key] = (now, payload)
        return dict(payload)


# --- FastAPI ---------------------------------------------------------------
def make_app(
    refresh_s: float,
    *,
    poll_stale_s: float | None = None,
    demo_mode: bool = False,
) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url=None)
    app.add_middleware(GZipMiddleware, minimum_size=500)

    @app.middleware("http")
    async def security_headers(request, call_next):
        response = await call_next(request)
        response.headers.setdefault("Cache-Control", "no-store")
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; "
            "style-src 'self' 'unsafe-inline'; img-src 'self' data:; "
            "connect-src 'self'; object-src 'none'; base-uri 'none'; "
            "frame-ancestors 'none'",
        )
        response.headers.setdefault(
            "Permissions-Policy",
            "camera=(), microphone=(), geolocation=()",
        )
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        return response

    @app.get("/static/{asset_path:path}", include_in_schema=False)
    def static_asset(asset_path: str):
        if asset_path not in _STATIC_ASSETS:
            raise HTTPException(status_code=404, detail="asset not found")
        return Response(
            _STATIC_ASSETS[asset_path],
            media_type=_STATIC_MEDIA_TYPES[asset_path],
            headers={"Cache-Control": "public, max-age=31536000, immutable"},
        )

    @app.get("/", response_class=HTMLResponse)
    def index():
        browser_refresh_s = max(5.0, float(refresh_s))
        browser_refresh_label = (
            str(int(browser_refresh_s))
            if browser_refresh_s.is_integer()
            else f"{browser_refresh_s:.1f}"
        )
        page = (
            PAGE.replace("{browser_refresh_s}", browser_refresh_label)
            .replace("{asset_version}", _STATIC_ASSET_VERSION)
            .replace("{demo_mode}", "true" if demo_mode else "false")
            .replace(
                "{demo_badge}",
                '<span class="demo-badge">demo data</span>' if demo_mode else "",
            )
        )
        return page

    @app.get("/logs", response_class=HTMLResponse)
    def logs_page():
        """Dedicated forensic-grade logs page. Full deque (6000 events),
        filterable, no truncation."""
        return LOGS_PAGE.replace("{asset_version}", _STATIC_ASSET_VERSION)

    @app.get("/api/logs", response_class=HTMLResponse)
    def api_logs(
        request: Request,
        kinds: str = "accept,reject,late_drop,selected,reward,fail,worker_fail,window_timeout",
        reason: str = "",
        hotkey: str = "",
        ours: str = "",
        limit: int = 500,
    ):
        kind_set: set[str] | None
        if not kinds.strip():
            kind_set = None
        else:
            wanted = {k.strip() for k in kinds.split(",") if k.strip()}
            # Drop unknown kinds rather than 400 — keeps the front-end
            # forgiving when we add new kinds upstream.
            kind_set = wanted & set(LOG_KINDS) if wanted else None
            if kind_set is not None and not kind_set:
                kind_set = None
        # Limit is clamped to the deque size — anything higher is wasted
        # work; anything lower is a deliberate operator choice.
        try:
            limit_i = max(1, min(int(limit), 6000))
        except (TypeError, ValueError):
            limit_i = 500
        events = validator_events_query(
            kinds=kind_set,
            reason_substr=reason.strip() or None,
            hotkey_substr=hotkey.strip() or None,
            ours_only=bool(ours),
            limit=limit_i,
        )
        buffer_total = len(validator_events_snapshot(10_000))
        digest = hashlib.blake2s(digest_size=16)
        digest.update(
            (
                f"{sorted(kind_set) if kind_set else '*'}\0{reason}\0{hotkey}\0"
                f"{bool(ours)}\0{limit_i}\0{buffer_total}\0"
            ).encode("utf-8")
        )
        for event in events:
            digest.update(
                (
                    f"{event.ts_epoch}\0{event.kind}\0{event.reject_reason}\0"
                    f"{event.hotkey12}\0{event.ours}\0{event.msg}\0"
                ).encode("utf-8", errors="replace")
            )
        etag = f'"{digest.hexdigest()}"'
        if request.headers.get("if-none-match") == etag:
            return Response(status_code=304, headers={"ETag": etag})
        content = render_logs_table_html(
            kinds=kind_set,
            reason=reason.strip() or None,
            hotkey=hotkey.strip() or None,
            ours_only=bool(ours),
            limit=limit_i,
            events=events,
            buffer_total=buffer_total,
        )
        return HTMLResponse(content, headers={"ETag": etag})

    @app.get("/api/dashboard-snapshot")
    def api_dashboard_snapshot(
        ema_mode: str = "top",
        rundown_tf: str = "30m",
        advanced: bool = True,
    ):
        return render_dashboard_snapshot(
            ema_mode=ema_mode,
            rundown_tf=rundown_tf,
            include_advanced=advanced,
        )


    @app.get("/api/summary", response_class=HTMLResponse)
    def api_summary():
        return render_fleet_summary_html()

    @app.get("/api/miner-now", response_class=HTMLResponse)
    def api_miner_now():
        return render_our_miner_now_html()

    @app.get("/api/miner-pipeline", response_class=HTMLResponse)
    def api_miner_pipeline():
        return render_current_miner_pipeline_html()

    @app.get("/api/miner-attempts", response_class=HTMLResponse)
    def api_miner_attempts():
        return render_recent_miner_attempts_html()

    @app.get("/api/miner-auction", response_class=HTMLResponse)
    def api_miner_auction():
        return render_last_sealed_auction_html()

    @app.get("/api/miner-runtime", response_class=HTMLResponse)
    def api_miner_runtime():
        return render_checkpoint_runtime_html()

    @app.get("/api/miner-log", response_class=HTMLResponse)
    def api_miner_log():
        return render_structured_miner_log_html()

    @app.get("/api/scoreboard", response_class=HTMLResponse)
    def api_scoreboard():
        return render_scoreboard_html()

    @app.get("/api/fleet", response_class=HTMLResponse)
    def api_fleet():
        return render_fleet_html()

    @app.get("/api/labs", response_class=HTMLResponse)
    def api_labs():
        return render_labs_html()

    @app.get("/api/mission", response_class=HTMLResponse)
    def api_mission():
        return render_mission_control_html()

    @app.get("/api/pipeline", response_class=HTMLResponse)
    def api_pipeline():
        return render_pipeline_html()

    @app.get("/api/frontier", response_class=HTMLResponse)
    def api_frontier():
        return render_frontier_html()

    @app.get("/api/validator", response_class=HTMLResponse)
    def api_validator():
        return render_validator_html()

    @app.get("/api/windows", response_class=HTMLResponse)
    def api_windows():
        return render_windows_html()

    @app.get("/api/window_forensics", response_class=HTMLResponse)
    def api_window_forensics():
        return render_window_forensics_html()

    @app.get("/api/events", response_class=HTMLResponse)
    def api_events():
        return render_events_html()

    @app.get("/api/validator_events", response_class=HTMLResponse)
    def api_validator_events():
        return render_validator_events_html()

    @app.get("/api/validator_rundown", response_class=HTMLResponse)
    def api_validator_rundown(tf: str = "30m"):
        # Defensive fallback — render_validator_rundown_html re-checks
        # but a bad query string shouldn't 500 the panel.
        return render_validator_rundown_html(timeframe=tf)

    @app.get("/api/chain", response_class=HTMLResponse)
    def api_chain():
        return render_chain_html()

    @app.get("/api/slotrank", response_class=HTMLResponse)
    def api_slotrank():
        return render_slot_rank_html()

    @app.get("/api/competitors", response_class=HTMLResponse)
    def api_competitors():
        return render_competitors_html()

    @app.get("/api/quality", response_class=HTMLResponse)
    def api_quality():
        return render_quality_html()

    @app.get("/api/baseline", response_class=HTMLResponse)
    def api_baseline():
        return render_baseline_html()

    @app.get("/api/rtt", response_class=HTMLResponse)
    def api_rtt():
        return render_rtt_html()

    @app.get("/api/ema", response_class=HTMLResponse)
    def api_ema(mode: str = "top", n: int = 12):
        # Defensive normalization — anything other than 'active' falls
        # back to top-N. `n` is bounded so a malformed query string can't
        # render an absurd table.
        if mode != "active":
            mode = "top"
        n = max(1, min(int(n) if n else 12, 500))
        return render_ema_leaderboard_html(top_n=n, mode=mode)

    @app.get("/api/box/{label}", response_class=HTMLResponse)
    def api_box_detail(label: str):
        return render_box_detail_html(label)

    @app.post("/api/star/{hotkey}")
    def api_star_toggle(
        hotkey: str,
        x_reliquary_fleet: str | None = Header(default=None),
    ):
        """Toggle the star on `hotkey`. Returns the new state + the full
        starred set so the client can update its UI without a re-render.

        The dashboard's EMA leaderboard fires this on click — see the
        `Star toggle` block in the bottom-of-page JS. After the toggle
        we also recompute fleet.OUR_SS58 so any subsequent panel render
        picks up the change immediately (live counters re-tint without
        waiting for a full restart).
        """
        if x_reliquary_fleet != "1":
            raise HTTPException(
                status_code=403,
                detail="missing same-origin request header",
            )
        if not re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{40,64}", hotkey):
            raise HTTPException(status_code=400, detail="invalid SS58 hotkey")

        import starred as _starred
        import fleet as _fleet_mod
        now_starred, full_set = _starred.toggle(hotkey)
        # Recompute OUR_SS58 = fleet boxes + starred. Removing a hotkey
        # that was never in a fleet box needs to drop it from OUR_SS58
        # too, so we rebuild from authoritative state each toggle.
        from settings import SETTINGS as _S
        our: dict[str, str] = {}
        for b in _S.fleet:
            if b.hotkey and b.hotkey not in our:
                our[b.hotkey] = b.label
        for hk in _S.starred_hotkeys_seed:
            our.setdefault(hk, _S.starred_hotkey_labels.get(hk, hk[:10]))
        for hk in full_set:
            our.setdefault(hk, hk[:10])
        _fleet_mod.OUR_SS58 = our
        sync_fleet_globals()
        _invalidate_dashboard_snapshot_cache()
        return {
            "hotkey": hotkey,
            "starred": now_starred,
            "count": len(full_set),
        }

    @app.get("/api/starred")
    def api_starred():
        """Snapshot of the current starred set + the fleet-derived 'ours'
        set, for clients that want to render persistent UI state (e.g.
        a config page showing which hotkeys are currently flagged).
        """
        import starred as _starred
        from settings import SETTINGS as _S
        fleet_hks = [b.hotkey for b in _S.fleet if b.hotkey]
        return {
            "starred": sorted(_starred.load()),
            "fleet_hotkeys": fleet_hks,
            "seed_hotkeys": list(_S.starred_hotkeys_seed),
        }

    @app.get("/api/targets")
    @app.get("/api/config/targets")
    def api_targets():
        """Authoritative configured mining targets plus chain visibility."""
        with _lock:
            validator_state = _vs
            chain_hotkeys = {getattr(h, "hotkey", "") for h in _chain.hotkeys}
            chain_seen = bool(_chain.last_fetch_at)
            targets = _configured_target_rows(chain_hotkeys, chain_seen)
            fleet_targets = [
                {
                    "label": b.label,
                    "alias": _display_ssh_alias(b.alias),
                    "unit": b.unit,
                    "configured_units": [
                        str(spec[0]) for spec in getattr(b, "unit_candidates", ())
                    ] or ([str(b.unit)] if b.unit else []),
                    "coordinated_units": list(
                        getattr(b, "coordinated_units", ()) or ()
                    ),
                    "active_unit": getattr(b, "active_unit", ""),
                    "active_units": list(
                        getattr(b, "active_units", []) or []
                    ),
                    "active_lane": getattr(b, "active_lane", ""),
                    "active_lanes": list(
                        getattr(b, "active_lanes", []) or []
                    ),
                    "coordinated_unit_statuses": (
                        _coordinated_unit_status_payload(b)
                    ),
                    "active_environment": getattr(b, "active_environment", ""),
                    "active_pid": int(getattr(b, "active_pid", 0) or 0),
                    "restart_count": int(getattr(b, "restart_count", 0) or 0),
                    "restart_count_semantics": (
                        _box_restart_count_semantics(b)
                    ),
                    "unit_resolution_error": getattr(b, "unit_resolution_error", ""),
                    "unexpected_active_units": list(
                        getattr(b, "unexpected_active_units", []) or []
                    ),
                    "active_unit_registry": dict(
                        getattr(b, "active_unit_registry", {}) or {}
                    ),
                    "lane_status": _box_lane_status(
                        b,
                        validator_state=validator_state,
                    ),
                    "env_file_ok": bool(getattr(b, "env_file_ok", False)),
                    "env_file_error": getattr(b, "env_file_error", ""),
                    "hotkey": b.hotkey,
                    "hotkey_prefix": b.hotkey[:12] if b.hotkey else "",
                    "alive": b.proc_alive,
                    "acpt_30m": b.acpt_30m,
                    "rej_30m": b.rej_30m,
                    "late_drops_30m": b.late_drops_30m,
                    "acceptance_source": getattr(b, "acceptance_source", "events"),
                    "final_accept_30m": getattr(b, "final_accept_30m", 0),
                    "final_reject_30m": getattr(b, "final_reject_30m", 0),
                    "reference_ready": bool(getattr(b, "reference_ready", False)),
                    "runtime_parity_ok": bool(getattr(b, "runtime_parity_ok", False)),
                    "quarantine_active": bool(getattr(b, "quarantine_active", False)),
                    "quarantine_reason": getattr(b, "quarantine_reason", ""),
                }
                for b in _boxes
            ]
            chain = {
                "seen": chain_seen,
                "registered_hotkeys": len(_chain.hotkeys),
                "last_fetch_at": _chain.last_fetch_at,
                "source_alias": _chain.source_alias,
                "error": _chain.error,
            }
        return {
            "netuid": NETUID,
            "targets": targets,
            "fleet": fleet_targets,
            "chain": chain,
        }

    @app.get("/api/export.json")
    def api_export():
        return render_export_json()

    @app.get("/healthz")
    def health():
        result = compute_healthz(refresh_s, poll_stale_s=poll_stale_s)
        return JSONResponse(status_code=200 if result["ok"] else 503, content=result)

    return app


def _positive_refresh_arg(value: str) -> float:
    """Argparse adapter that rejects values which can disable staleness."""
    import settings as _settings

    try:
        return _settings.positive_finite_seconds(value, "--refresh")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def main(argv: list[str] | None = None) -> int:
    global _windows, _ema_cache, _ema_window_count

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--config", default="config.yaml",
        help="Path to config.yaml (default: ./config.yaml)",
    )
    ap.add_argument(
        "--state-dir",
        default=os.environ.get("RELIQUARY_FLEET_STATE_DIR", "state"),
        help="Private cache/state directory (default: ./state)",
    )
    # CLI flags override the matching config.yaml keys when explicitly
    # passed. Argparse uses sentinel defaults so we can tell unset apart
    # from "user typed the same value as config".
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--host", default=None)
    ap.add_argument("--refresh", type=_positive_refresh_arg, default=None,
                    help="seconds between background polls")
    # 216 = validator's ROLLING_WINDOWS * 3 = full archive lookback used by
    # _replay_ema. With this we can replicate on-chain weights locally.
    ap.add_argument("--history", type=int, default=None)
    ap.add_argument(
        "--allow-remote",
        action="store_true",
        help="acknowledge a non-loopback bind; authentication is external",
    )
    ap.add_argument("--open", action="store_true", help="open a local browser")
    args = ap.parse_args(argv)

    # Load operator config first so the rest of fleet.py reads from the
    # YAML-provided values instead of the empty defaults baked into
    # fleet.py at import time.
    import settings as _settings
    try:
        _settings.load(args.config)
    except (FileNotFoundError, OSError, ValueError) as e:
        print(f"[fleet_web] {e}", file=sys.stderr)
        return 2
    _settings.apply_to_fleet_module()

    # CLI flags override config.yaml when explicitly set.
    if args.port is None:
        args.port = _settings.SETTINGS.dashboard_port
    if args.host is None:
        args.host = _settings.SETTINGS.dashboard_host
    if args.refresh is None:
        args.refresh = _settings.SETTINGS.refresh_seconds
    if args.history is None:
        args.history = _settings.SETTINGS.history_windows

    if not 1 <= args.port <= 65535:
        print("[fleet_web] port must be in 1..65535", file=sys.stderr)
        return 2
    if args.history < 1:
        print("[fleet_web] history must be at least 1", file=sys.stderr)
        return 2
    if not _is_loopback_bind(args.host) and not args.allow_remote:
        print(
            "[fleet_web] refusing a non-loopback bind without --allow-remote; "
            "the dashboard has no built-in login",
            file=sys.stderr,
        )
        return 2

    state_dir = str(Path(args.state_dir).expanduser().resolve())
    try:
        os.makedirs(state_dir, mode=0o700, exist_ok=True)
    except OSError as exc:
        print(f"[fleet_web] cannot create state directory: {exc}", file=sys.stderr)
        return 2

    available, bind_error = _port_available(args.host, args.port)
    if not available:
        print(
            f"[fleet_web] cannot listen on {args.host}:{args.port}: {bind_error}",
            file=sys.stderr,
        )
        print(
            "[fleet_web] stop the existing dashboard instead of running a duplicate",
            file=sys.stderr,
        )
        return 3

    # Configure persistence before the first cache/star read. Installed runs
    # use an OS-native private state directory; source checkouts retain ./state.
    import fleet as _fleet
    import starred as _starred

    _fleet.configure_state_dir(state_dir)
    _starred.configure_state_dir(state_dir)
    _fleet.merge_starred_into_our_ss58(_starred.load())
    sync_fleet_globals()

    locked, owner = _acquire_instance_lock(state_dir, args.port)
    if not locked:
        detail = f" (PID {owner})" if owner.isdigit() else ""
        print(
            f"[fleet_web] another dashboard already owns port {args.port}{detail}",
            file=sys.stderr,
        )
        return 3

    init_state()
    baseline_load(_baseline)
    n = window_cache_load()
    if n:
        print(f"[fleet_web] loaded {n} cached windows from disk", file=sys.stderr)
        cached_windows = sorted(
            _fleet._window_cache.values(), key=lambda w: -w.n
        )[:args.history]
        with _lock:
            _windows = cached_windows
        try:
            _ema_cache = compute_ema_leaderboard(cached_windows)
            _ema_window_count = len(cached_windows)
        except Exception:
            pass
    app = make_app(
        args.refresh,
        poll_stale_s=_settings.SETTINGS.health_poll_stale_seconds,
    )
    config = uvicorn.Config(
        app,
        host=args.host,
        port=args.port,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    collectors = _collector_specs(
        _settings.SETTINGS,
        args.refresh,
        args.history,
    )

    def ready() -> None:
        print(
            "[fleet_web] local socket ready; background pollers started · "
            f"open http://{args.host}:{args.port}",
            file=sys.stderr,
        )
        if args.open:
            browser_host = (
                "127.0.0.1" if args.host in {"0.0.0.0", "::"} else args.host
            )
            webbrowser.open(f"http://{browser_host}:{args.port}")

    threading.Thread(
        name="reliquary-fleet-collector-supervisor",
        target=_start_collectors_after_ready,
        args=(server, collectors),
        kwargs={"on_ready": ready},
        daemon=True,
    ).start()
    try:
        server.run()
    finally:
        server.should_exit = True
        _release_instance_lock()
    return 0


if __name__ == "__main__":
    sys.exit(main())
