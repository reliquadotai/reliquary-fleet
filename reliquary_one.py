# ruff: noqa: E501
"""Bounded, redacted telemetry adapter for Reliquary miner systemd units.

The remote probe reads only the structured operator surfaces documented by the
miner.  It projects an allowlisted schema before bytes cross SSH; the local
normalizer validates that projection again before the dashboard stores it.
"""

from __future__ import annotations

import base64
import copy
import math
import re
import shlex
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
MAX_EVENTS = 500
MAX_SUBMISSIONS = 128
_HEX40 = re.compile(r"[0-9a-f]{40}")
_HEX64 = re.compile(r"[0-9a-f]{64}")
_SAFE_TOKEN = re.compile(r"[A-Za-z0-9_.:/+-]{1,160}")
_SAFE_FILE = re.compile(r"[A-Za-z0-9_.-]{1,160}")
_SYSTEMD_UNIT = re.compile(r"[A-Za-z0-9_.@-]{1,160}\.service")

EVENT_TYPES = {
    "admission_pipeline_completed",
    "checkpoint_activated",
    "daemon_iteration_failed",
    "daemon_started",
    "daemon_stopped",
    "doctor_completed",
    "group_ineligible",
    "group_termination_ineligible",
    "live_submission",
    "local_out_of_zone",
    "local_protocol_ineligible",
    "terminal_outcome",
    "window_attempt_failed",
    "window_completed",
    "window_started",
    "workers_hot",
}

_FLOAT_METRICS = {
    "download_seconds",
    "generation_seconds",
    "open_age_at_start",
    "open_age_seconds",
    "peak_hbm_mib",
    "precommit_seconds",
    "proof_integrity_seconds",
    "proof_seconds",
    "reveal_seconds",
    "total_hbm_mib",
    "total_seconds",
}
_BOOL_METRICS = {
    "data_only",
    "downloaded",
    "first_sent",
    "passed",
    "precommit_accepted",
    "precommit_transport_complete",
    "reveal_accepted",
    "reveal_transport_complete",
    "second_attempted",
    "second_sent",
    "transport_complete",
}
_INT_METRICS = {
    "canonical_rank",
    "generated_group_limit",
    "groups_attempted",
    "submission_attempt_limit",
    "submission_attempts",
    "transport_complete_groups",
    "wave_size",
}
_SAFE_METRICS = {
    "outcome_layer",
    "outcome_source",
    "precommit_reason",
    "reason",
    "reveal_reason",
    "stop_reason",
}
_TOKEN_METRICS = {"binding_sha256", "previous_checkpoint_revision"}

_REMOTE_PROBE = r"""
import json
import math
import os
import re
import subprocess
import sys
import time
from pathlib import Path

root = Path(sys.argv[1])
unit = sys.argv[2]
MAX_EVENTS = 500
MAX_EVENT_BYTES = 2 * 1024 * 1024
MAX_JSON_BYTES = 1024 * 1024
EVENT_TYPES = {
    "admission_pipeline_completed", "checkpoint_activated",
    "daemon_iteration_failed", "daemon_started", "daemon_stopped",
    "doctor_completed", "group_ineligible", "group_termination_ineligible",
    "live_submission", "local_out_of_zone", "local_protocol_ineligible",
    "terminal_outcome", "window_attempt_failed", "window_completed",
    "window_started", "workers_hot",
}
FLOAT_METRICS = {
    "download_seconds", "generation_seconds", "open_age_at_start",
    "open_age_seconds", "peak_hbm_mib", "precommit_seconds",
    "proof_integrity_seconds", "proof_seconds", "reveal_seconds",
    "total_hbm_mib", "total_seconds",
}
BOOL_METRICS = {
    "data_only", "downloaded", "first_sent", "passed",
    "precommit_accepted", "precommit_transport_complete", "reveal_accepted",
    "reveal_transport_complete", "second_attempted", "second_sent",
    "transport_complete",
}
INT_METRICS = {
    "canonical_rank", "generated_group_limit", "groups_attempted",
    "submission_attempt_limit", "submission_attempts",
    "transport_complete_groups", "wave_size",
}
SAFE_METRICS = {
    "outcome_layer", "outcome_source", "precommit_reason", "reason",
    "reveal_reason", "stop_reason",
}
TOKEN_METRICS = {"binding_sha256", "previous_checkpoint_revision"}
HEX40 = re.compile(r"[0-9a-f]{40}")
HEX64 = re.compile(r"[0-9a-f]{64}")
SAFE_TOKEN = re.compile(r"[A-Za-z0-9_.:/+-]{1,160}")
SAFE_FILE = re.compile(r"[A-Za-z0-9_.-]{1,160}")
errors = []

def finite_number(value, *, maximum=86400.0):
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or parsed < 0.0 or parsed > maximum:
        return None
    return parsed

def read_json(path, limit, name):
    try:
        size = path.stat().st_size
        if size > limit:
            errors.append(name + "_oversized")
            return None, float(path.stat().st_mtime)
        value = json.loads(path.read_text(encoding="utf-8"))
        return value, float(path.stat().st_mtime)
    except FileNotFoundError:
        errors.append(name + "_missing")
    except (OSError, UnicodeError, json.JSONDecodeError):
        errors.append(name + "_invalid")
    return None, 0.0

def safe_event(raw):
    if not isinstance(raw, dict) or raw.get("schema_version") != 1:
        return None
    event = raw.get("event")
    if event not in EVENT_TYPES:
        return None
    timestamp = finite_number(raw.get("timestamp"), maximum=4_000_000_000.0)
    if timestamp is None or timestamp <= 0:
        return None
    window = raw.get("window_n")
    if window is not None and (
        isinstance(window, bool) or not isinstance(window, int) or window < 0
    ):
        return None
    row = {"event": event, "timestamp": timestamp, "window_n": window}
    attempt_id = str(raw.get("attempt_id") or "").lower()
    if HEX64.fullmatch(attempt_id):
        row["attempt_id"] = attempt_id
    revision = str(raw.get("checkpoint_revision") or "").lower()
    if HEX40.fullmatch(revision):
        row["checkpoint_revision"] = revision
    classification = str(raw.get("classification") or "")
    if SAFE_TOKEN.fullmatch(classification):
        row["classification"] = classification
    metrics = raw.get("metrics")
    clean = {}
    if isinstance(metrics, dict):
        for key in FLOAT_METRICS:
            if key in metrics:
                value = finite_number(metrics.get(key))
                if value is not None:
                    clean[key] = value
        for key in BOOL_METRICS:
            if isinstance(metrics.get(key), bool):
                clean[key] = metrics[key]
        for key in INT_METRICS:
            value = metrics.get(key)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                clean[key] = value
        for key in SAFE_METRICS:
            value = str(metrics.get(key) or "")
            if SAFE_TOKEN.fullmatch(value):
                clean[key] = value
        outcome_value = metrics.get("outcome_value")
        if isinstance(outcome_value, bool) or (
            isinstance(outcome_value, int)
            and not isinstance(outcome_value, bool)
            and outcome_value >= 0
        ):
            clean["outcome_value"] = outcome_value
        for key in TOKEN_METRICS:
            value = str(metrics.get(key) or "").lower()
            if (key == "previous_checkpoint_revision" and HEX40.fullmatch(value)) or (
                key == "binding_sha256" and HEX64.fullmatch(value)
            ):
                clean[key] = value
        exception_file = Path(str(metrics.get("exception_file") or "")).name
        if SAFE_FILE.fullmatch(exception_file):
            clean["exception_file"] = exception_file
    row["metrics"] = clean
    return row

events_path = root / "events.jsonl"
events = []
events_mtime = 0.0
malformed_events = 0
oversized_events = 0
try:
    events_mtime = float(events_path.stat().st_mtime)
    result = subprocess.run(
        ["tail", "-n", str(MAX_EVENTS), "--", str(events_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        timeout=5,
        check=False,
    )
    payload = result.stdout[-MAX_EVENT_BYTES:]
    for line in payload.splitlines()[-MAX_EVENTS:]:
        if len(line) > 65536:
            oversized_events += 1
            continue
        try:
            raw = json.loads(line)
        except (UnicodeError, json.JSONDecodeError):
            malformed_events += 1
            continue
        row = safe_event(raw)
        if row is None:
            malformed_events += 1
        else:
            events.append(row)
except (OSError, subprocess.SubprocessError):
    errors.append("events_unavailable")

binding_raw, binding_mtime = read_json(
    root / "active-binding.json", 128 * 1024, "binding"
)
binding = {}
if isinstance(binding_raw, dict) and binding_raw.get("schema_version") == 1:
    integer = binding_raw.get("checkpoint_number")
    if isinstance(integer, int) and not isinstance(integer, bool) and integer >= 0:
        binding["checkpoint_number"] = integer
    activated = finite_number(binding_raw.get("activated_at"), maximum=4_000_000_000.0)
    if activated is not None:
        binding["activated_at"] = activated
    for key in ("checkpoint_revision", "release_sha"):
        value = str(binding_raw.get(key) or "").lower()
        if HEX40.fullmatch(value):
            binding[key] = value
    for key in (
        "environment", "strategy", "profile_id", "generator_fingerprint",
        "proof_fingerprint",
    ):
        value = str(binding_raw.get(key) or "")
        if SAFE_TOKEN.fullmatch(value):
            binding[key] = value
else:
    errors.append("binding_schema")

submissions_raw, submissions_mtime = read_json(
    root / "submissions.json", MAX_JSON_BYTES, "submissions"
)
submissions = []
if (
    isinstance(submissions_raw, dict)
    and submissions_raw.get("schema_version") in (1, 2, 3, 4)
):
    rows = submissions_raw.get("submissions")
    if isinstance(rows, list):
        for raw in rows[-128:]:
            if not isinstance(raw, dict):
                continue
            window = raw.get("window_n")
            root_hash = str(raw.get("merkle_root") or "").lower()
            if (
                isinstance(window, int)
                and not isinstance(window, bool)
                and window >= 0
                and HEX64.fullmatch(root_hash)
            ):
                row = {"window_n": window, "merkle_root": root_hash}
                accepted = raw.get("accepted_precommit_reveal")
                if isinstance(accepted, bool):
                    row["accepted_precommit_reveal"] = accepted
                submissions.append(row)
else:
    errors.append("submissions_schema")

service = {}
try:
    properties = (
        "Id,LoadState,ActiveState,SubState,UnitFileState,NRestarts,MainPID,"
        "ActiveEnterTimestampMonotonic"
    )
    result = subprocess.run(
        ["systemctl", "show", unit, "--no-pager", "--property=" + properties],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        timeout=5,
        check=False,
    )
    raw_service = {}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            raw_service[key] = value
    service = {
        "unit": unit,
        "load_state": raw_service.get("LoadState", ""),
        "active_state": raw_service.get("ActiveState", ""),
        "sub_state": raw_service.get("SubState", ""),
        "unit_file_state": raw_service.get("UnitFileState", ""),
    }
    for source, target in (("NRestarts", "restarts"), ("MainPID", "pid")):
        try:
            service[target] = max(0, int(raw_service.get(source) or 0))
        except (TypeError, ValueError):
            service[target] = 0
    try:
        boot_uptime = float(Path("/proc/uptime").read_text().split()[0])
        entered_us = int(raw_service.get("ActiveEnterTimestampMonotonic") or 0)
        service["uptime_seconds"] = max(0.0, boot_uptime - entered_us / 1_000_000.0)
    except (OSError, ValueError, IndexError):
        service["uptime_seconds"] = 0.0
except (OSError, subprocess.SubprocessError):
    errors.append("service_unavailable")

gpus = []
try:
    result = subprocess.run(
        [
            "nvidia-smi",
            "--query-gpu=index,name,uuid,utilization.gpu,memory.used,memory.total,power.draw,power.limit",
            "--format=csv,noheader,nounits",
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        timeout=5,
        check=False,
    )
    for line in result.stdout.splitlines()[:8]:
        parts = [part.strip() for part in line.split(",")]
        if len(parts) != 8:
            continue
        try:
            index = int(parts[0])
            utilization = float(parts[3])
            memory_used = float(parts[4])
            memory_total = float(parts[5])
            power_draw = float(parts[6])
            power_limit = float(parts[7])
        except ValueError:
            continue
        uuid = parts[2]
        name = parts[1]
        if not re.fullmatch(r"GPU-[0-9A-Fa-f-]{36}", uuid):
            continue
        if not SAFE_TOKEN.fullmatch(name.replace(" ", "_")):
            continue
        gpus.append({
            "index": index,
            "name": name,
            "uuid": uuid,
            "utilization_pct": utilization,
            "memory_used_mib": memory_used,
            "memory_total_mib": memory_total,
            "power_draw_w": power_draw,
            "power_limit_w": power_limit,
        })
except (OSError, subprocess.SubprocessError):
    errors.append("gpu_unavailable")

checkpoints = {"active": [], "rollback": [], "incoming": []}
checkpoint_path = str(binding_raw.get("checkpoint_path") or "") if isinstance(binding_raw, dict) else ""
try:
    active_path = Path(checkpoint_path)
    checkpoint_root = active_path.parents[1]
    if active_path.parent.name != "active":
        raise ValueError("unexpected checkpoint tier")
    for tier in checkpoints:
        tier_path = checkpoint_root / tier
        rows = []
        if tier_path.is_dir():
            for child in tier_path.iterdir():
                if child.is_dir() and HEX40.fullmatch(child.name):
                    rows.append({"revision": child.name, "mtime": float(child.stat().st_mtime)})
        checkpoints[tier] = sorted(rows, key=lambda row: row["mtime"], reverse=True)[:2]
except (OSError, ValueError, IndexError):
    errors.append("checkpoint_tiers_unavailable")

print(json.dumps({
    "schema_version": 1,
    "collected_at": time.time(),
    "source_timestamps": {
        "events": events_mtime,
        "submissions": submissions_mtime,
        "binding": binding_mtime,
    },
    "service": service,
    "gpu": gpus,
    "binding": binding,
    "checkpoints": checkpoints,
    "events": events,
    "submissions": submissions,
    "event_diagnostics": {
        "malformed": malformed_events,
        "oversized": oversized_events,
        "retained": len(events),
    },
    "errors": sorted(set(errors)),
}, separators=(",", ":"), sort_keys=True))
"""


def build_remote_probe_command(state_root: str, unit: str) -> str:
    """Return one root-readonly SSH command for a complete collector cycle."""

    root = str(state_root or "").strip()
    service = str(unit or "").strip()
    if not root.startswith("/") or "\x00" in root:
        raise ValueError("reliquary_one state_root must be absolute")
    if not _SYSTEMD_UNIT.fullmatch(service):
        raise ValueError("reliquary_one unit is invalid")
    encoded = base64.b64encode(_REMOTE_PROBE.encode("utf-8")).decode("ascii")
    bootstrap = f"import base64;exec(base64.b64decode('{encoded}'))"
    return " ".join(
        (
            "sudo",
            "-n",
            "python3",
            "-c",
            shlex.quote(bootstrap),
            shlex.quote(root),
            shlex.quote(service),
        )
    )


def _finite_number(
    value: object,
    *,
    maximum: float = 86_400.0,
) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or not 0.0 <= parsed <= maximum:
        return None
    return parsed


def _safe_event(raw: object) -> dict[str, Any] | None:
    if not isinstance(raw, dict) or raw.get("event") not in EVENT_TYPES:
        return None
    timestamp = _finite_number(raw.get("timestamp"), maximum=4_000_000_000.0)
    window_n = raw.get("window_n")
    if timestamp is None or timestamp <= 0:
        return None
    if window_n is not None and (
        isinstance(window_n, bool) or not isinstance(window_n, int) or window_n < 0
    ):
        return None
    row: dict[str, Any] = {
        "event": str(raw["event"]),
        "timestamp": timestamp,
        "window_n": window_n,
    }
    attempt_id = str(raw.get("attempt_id") or "").lower()
    if _HEX64.fullmatch(attempt_id):
        row["attempt_id"] = attempt_id
    revision = str(raw.get("checkpoint_revision") or "").lower()
    if _HEX40.fullmatch(revision):
        row["checkpoint_revision"] = revision
    classification = str(raw.get("classification") or "")
    if _SAFE_TOKEN.fullmatch(classification):
        row["classification"] = classification
    metrics = raw.get("metrics")
    clean_metrics: dict[str, Any] = {}
    if isinstance(metrics, dict):
        for key in _FLOAT_METRICS:
            if key not in metrics:
                continue
            value = _finite_number(metrics.get(key))
            if value is not None:
                clean_metrics[key] = value
        for key in _BOOL_METRICS:
            if isinstance(metrics.get(key), bool):
                clean_metrics[key] = metrics[key]
        for key in _INT_METRICS:
            value = metrics.get(key)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                clean_metrics[key] = value
        for key in _SAFE_METRICS:
            value = str(metrics.get(key) or "")
            if _SAFE_TOKEN.fullmatch(value):
                clean_metrics[key] = value
        outcome_value = metrics.get("outcome_value")
        if isinstance(outcome_value, bool) or (
            isinstance(outcome_value, int)
            and not isinstance(outcome_value, bool)
            and outcome_value >= 0
        ):
            clean_metrics["outcome_value"] = outcome_value
        for key in _TOKEN_METRICS:
            value = str(metrics.get(key) or "").lower()
            if (key == "binding_sha256" and _HEX64.fullmatch(value)) or (
                key == "previous_checkpoint_revision" and _HEX40.fullmatch(value)
            ):
                clean_metrics[key] = value
        filename = Path(str(metrics.get("exception_file") or "")).name
        if _SAFE_FILE.fullmatch(filename):
            clean_metrics["exception_file"] = filename
    row["metrics"] = clean_metrics
    return row


def _normalize_service(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    result: dict[str, Any] = {}
    for key in ("unit", "load_state", "active_state", "sub_state", "unit_file_state"):
        value = str(raw.get(key) or "")
        if _SAFE_TOKEN.fullmatch(value):
            result[key] = value
    for key in ("restarts", "pid"):
        value = raw.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            result[key] = value
    uptime = _finite_number(raw.get("uptime_seconds"), maximum=10 * 365 * 86400.0)
    if uptime is not None:
        result["uptime_seconds"] = uptime
    result["active"] = bool(
        result.get("load_state") == "loaded"
        and result.get("active_state") == "active"
        and result.get("sub_state") == "running"
    )
    result["enabled"] = result.get("unit_file_state") in {
        "enabled",
        "enabled-runtime",
        "static",
    }
    return result


def _normalize_binding(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    result: dict[str, Any] = {}
    checkpoint = raw.get("checkpoint_number")
    if (
        isinstance(checkpoint, int)
        and not isinstance(checkpoint, bool)
        and checkpoint >= 0
    ):
        result["checkpoint_number"] = checkpoint
    activated = _finite_number(raw.get("activated_at"), maximum=4_000_000_000.0)
    if activated is not None:
        result["activated_at"] = activated
    for key in ("checkpoint_revision", "release_sha"):
        value = str(raw.get(key) or "").lower()
        if _HEX40.fullmatch(value):
            result[key] = value
    for key in (
        "environment",
        "strategy",
        "profile_id",
        "generator_fingerprint",
        "proof_fingerprint",
    ):
        value = str(raw.get(key) or "")
        if _SAFE_TOKEN.fullmatch(value):
            result[key] = value
    return result


def _normalize_gpu(raw: object) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in raw[:8]:
        if not isinstance(item, dict):
            continue
        uuid = str(item.get("uuid") or "")
        name = str(item.get("name") or "")
        index = item.get("index")
        if (
            not re.fullmatch(r"GPU-[0-9A-Fa-f-]{36}", uuid)
            or not name
            or len(name) > 120
            or not isinstance(index, int)
            or isinstance(index, bool)
            or index < 0
        ):
            continue
        row: dict[str, Any] = {"index": index, "name": name, "uuid": uuid}
        valid = True
        for key, maximum in (
            ("utilization_pct", 100.0),
            ("memory_used_mib", 1_000_000.0),
            ("memory_total_mib", 1_000_000.0),
            ("power_draw_w", 10_000.0),
            ("power_limit_w", 10_000.0),
        ):
            value = _finite_number(item.get(key), maximum=maximum)
            if value is None:
                valid = False
                break
            row[key] = value
        if valid and row["memory_used_mib"] <= row["memory_total_mib"]:
            rows.append(row)
    return rows


def _normalize_checkpoints(raw: object) -> dict[str, list[dict[str, Any]]]:
    result = {"active": [], "rollback": [], "incoming": []}
    if not isinstance(raw, dict):
        return result
    for tier in result:
        rows = raw.get(tier)
        if not isinstance(rows, list):
            continue
        for item in rows[:2]:
            if not isinstance(item, dict):
                continue
            revision = str(item.get("revision") or "").lower()
            mtime = _finite_number(item.get("mtime"), maximum=4_000_000_000.0)
            if _HEX40.fullmatch(revision) and mtime is not None:
                result[tier].append({"revision": revision, "mtime": mtime})
    return result


def _derive_attempts(
    events: list[dict[str, Any]],
    submissions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    starts: dict[int, dict[str, Any]] = {}
    submission_rows: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in submissions:
        submission_rows[int(row["window_n"])].append(row)
    ordinals: dict[int, int] = defaultdict(int)
    attempts: list[dict[str, Any]] = []
    for event in sorted(events, key=lambda row: float(row["timestamp"])):
        window = event.get("window_n")
        if not isinstance(window, int):
            continue
        metrics = event.get("metrics") if isinstance(event.get("metrics"), dict) else {}
        if event["event"] == "window_started":
            starts[window] = event
            continue
        if event["event"] == "live_submission":
            ordinals[window] += 1
            ordinal = ordinals[window]
            local_rows = submission_rows.get(window, [])
            submission = local_rows[ordinal - 1] if ordinal <= len(local_rows) else {}
            precommit = metrics.get("precommit_accepted")
            if not isinstance(precommit, bool):
                precommit = metrics.get("precommit_transport_complete")
            reveal = metrics.get("reveal_accepted")
            if not isinstance(reveal, bool):
                reveal = metrics.get("reveal_transport_complete")
            if precommit is True and reveal is True:
                transport = "accepted"
            elif precommit is False or reveal is False:
                transport = "rejected"
            else:
                transport = "ambiguous"
            attempts.append(
                {
                    "window_n": window,
                    "ordinal": ordinal,
                    "timestamp": event["timestamp"],
                    "attempt_ref": str(event.get("attempt_id") or "")[:12],
                    "merkle_root": str(submission.get("merkle_root") or ""),
                    "open_age_at_start_s": metrics.get("open_age_at_start"),
                    "generation_s": metrics.get("generation_seconds"),
                    "proof_s": metrics.get("proof_seconds"),
                    "proof_integrity_s": metrics.get("proof_integrity_seconds"),
                    "precommit_s": metrics.get("precommit_seconds"),
                    "reveal_s": metrics.get("reveal_seconds"),
                    "total_s": metrics.get("total_seconds"),
                    "transport": {
                        "status": transport,
                        "precommit_reason": metrics.get("precommit_reason"),
                        "precommit": (
                            "accepted"
                            if precommit is True
                            else "rejected"
                            if precommit is False
                            else "unknown"
                        ),
                        "reveal_reason": metrics.get("reveal_reason"),
                        "reveal": (
                            "accepted"
                            if reveal is True
                            else "rejected"
                            if reveal is False
                            else "unknown"
                        ),
                    },
                    "admission": {"status": "pending", "reason": None, "source": None},
                    "auction": {
                        "selected": None,
                        "rewarded": None,
                        "canonical_rank": None,
                        "source": None,
                    },
                    "failure": None,
                }
            )
            continue
        if event["event"] in {"window_attempt_failed", "daemon_iteration_failed"}:
            ordinals[window] += 1
            attempts.append(
                {
                    "window_n": window,
                    "ordinal": ordinals[window],
                    "timestamp": event["timestamp"],
                    "attempt_ref": str(event.get("attempt_id") or "")[:12],
                    "merkle_root": "",
                    "open_age_at_start_s": (
                        starts.get(window, {})
                        .get("metrics", {})
                        .get("open_age_seconds")
                    ),
                    "generation_s": None,
                    "proof_s": None,
                    "proof_integrity_s": None,
                    "precommit_s": None,
                    "reveal_s": None,
                    "total_s": None,
                    "transport": {
                        "status": "not_sent",
                        "precommit": "not_sent",
                        "reveal": "not_sent",
                    },
                    "admission": {
                        "status": "not_applicable",
                        "reason": None,
                        "source": None,
                    },
                    "auction": {
                        "selected": None,
                        "rewarded": None,
                        "canonical_rank": None,
                        "source": None,
                    },
                    "failure": {
                        "classification": str(event.get("classification") or "unknown"),
                        "exception_file": str(metrics.get("exception_file") or ""),
                    },
                }
            )
    attempts.sort(
        key=lambda row: (int(row["window_n"]), int(row["ordinal"])), reverse=True
    )
    return attempts[:64]


def _derive_v4_funnel(
    events: Iterable[dict[str, Any]],
    attempts: Iterable[dict[str, Any]],
) -> dict[str, int]:
    """Build six non-overloaded production counters from demonstrated stages.

    V4 emits no standalone ``generation_complete`` record.  A group is counted
    as generated only when a per-attempt terminal event proves generation
    returned.  Protocol validity and local eligibility are intentionally
    conservative: only a signed submission proves the complete local BF16
    proof/preflight path; ``group_ineligible`` additionally proves protocol
    validity without eligibility.  Validator rank and selection are populated
    only after authenticated verdict/R2 reconciliation updates ``attempts``.
    """

    generated: set[str] = set()
    protocol_valid: set[str] = set()
    locally_eligible: set[str] = set()
    signed_precommits: set[str] = set()
    generation_terminal_events = {
        "group_ineligible",
        "group_termination_ineligible",
        "live_submission",
        "local_out_of_zone",
        "local_protocol_ineligible",
    }
    for event in events:
        attempt_id = str(event.get("attempt_id") or "")
        if not _HEX64.fullmatch(attempt_id):
            continue
        kind = str(event.get("event") or "")
        if kind in generation_terminal_events:
            generated.add(attempt_id)
        if kind in {"group_ineligible", "live_submission"}:
            protocol_valid.add(attempt_id)
        if kind == "live_submission":
            locally_eligible.add(attempt_id)
            signed_precommits.add(attempt_id)

    validator_ranked = 0
    selected_slots = 0
    for attempt in attempts:
        auction = attempt.get("auction")
        if not isinstance(auction, dict):
            continue
        rank = auction.get("canonical_rank")
        if isinstance(rank, int) and not isinstance(rank, bool) and rank > 0:
            validator_ranked += 1
        if auction.get("selected") is True:
            selected_slots += 1
    return {
        "generated_groups": len(generated),
        "protocol_valid_groups": len(protocol_valid),
        "locally_eligible_groups": len(locally_eligible),
        "signed_precommits": len(signed_precommits),
        "validator_ranked_candidates": validator_ranked,
        "selected_slots": selected_slots,
    }


def normalize_probe(
    payload: object,
    *,
    now: float,
    stale_seconds: float,
) -> dict[str, Any]:
    """Validate a remote projection and derive normalized operator state."""

    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("reliquary_one_probe_schema")
    collected_at = _finite_number(payload.get("collected_at"), maximum=4_000_000_000.0)
    if collected_at is None or collected_at <= 0 or collected_at > now + 60.0:
        raise ValueError("reliquary_one_probe_timestamp")
    source_raw = payload.get("source_timestamps")
    source_timestamps: dict[str, float] = {}
    if isinstance(source_raw, dict):
        for key in ("events", "submissions", "binding"):
            value = _finite_number(source_raw.get(key), maximum=4_000_000_000.0)
            if value is not None:
                source_timestamps[key] = value

    events = []
    raw_events = payload.get("events")
    if isinstance(raw_events, list):
        for raw in raw_events[-MAX_EVENTS:]:
            event = _safe_event(raw)
            if event is not None:
                events.append(event)
    events.sort(key=lambda row: float(row["timestamp"]))

    submissions: list[dict[str, Any]] = []
    raw_submissions = payload.get("submissions")
    if isinstance(raw_submissions, list):
        for raw in raw_submissions[-MAX_SUBMISSIONS:]:
            if not isinstance(raw, dict):
                continue
            window = raw.get("window_n")
            merkle_root = str(raw.get("merkle_root") or "").lower()
            if (
                isinstance(window, int)
                and not isinstance(window, bool)
                and window >= 0
                and _HEX64.fullmatch(merkle_root)
            ):
                submissions.append({"window_n": window, "merkle_root": merkle_root})

    service = _normalize_service(payload.get("service"))
    binding = _normalize_binding(payload.get("binding"))
    gpu = _normalize_gpu(payload.get("gpu"))
    checkpoints = _normalize_checkpoints(payload.get("checkpoints"))
    errors = sorted(
        {
            str(value)
            for value in (payload.get("errors") or [])
            if isinstance(value, str) and _SAFE_TOKEN.fullmatch(value)
        }
    )
    source_ages = {
        key: max(0.0, now - timestamp)
        for key, timestamp in source_timestamps.items()
        if timestamp > 0
    }
    event_age = source_ages.get("events")
    fresh = bool(
        service.get("active")
        and binding.get("checkpoint_number") is not None
        and binding.get("checkpoint_revision")
        and events
        and event_age is not None
        and event_age <= stale_seconds
        and not any(
            error
            in {
                "binding_invalid",
                "binding_missing",
                "binding_oversized",
                "binding_schema",
                "events_unavailable",
                "submissions_invalid",
                "submissions_missing",
                "submissions_oversized",
                "submissions_schema",
            }
            for error in errors
        )
    )
    latest_event = events[-1] if events else {}
    event_name = str(latest_event.get("event") or "")
    current_stage = {
        "window_started": "generate",
        "live_submission": "outcome",
        "window_attempt_failed": "failed",
        "window_completed": "observe",
        "daemon_iteration_failed": "failed",
    }.get(event_name, "observe")
    event_timestamp = float(latest_event.get("timestamp") or 0.0)
    pipeline = {
        "window_n": latest_event.get("window_n"),
        "stage": current_stage,
        "stage_started_at": event_timestamp or None,
        "stage_elapsed_s": max(0.0, now - event_timestamp) if event_timestamp else None,
        "classification": latest_event.get("classification") or None,
    }
    attempts = _derive_attempts(events, submissions)
    return {
        "schema_version": SCHEMA_VERSION,
        "collected_at": collected_at,
        "source_timestamps": source_timestamps,
        "source_ages_s": source_ages,
        "fresh": fresh,
        "stale_after_s": stale_seconds,
        "errors": errors,
        "service": service,
        "gpu": gpu,
        "binding": binding,
        "checkpoints": checkpoints,
        "events": events,
        "attempts": attempts,
        "funnel": _derive_v4_funnel(events, attempts),
        "current_pipeline": pipeline,
        "last_heartbeat_at": event_timestamp or None,
        "event_diagnostics": {
            key: int(value)
            for key, value in (payload.get("event_diagnostics") or {}).items()
            if key in {"malformed", "oversized", "retained"}
            and isinstance(value, int)
            and not isinstance(value, bool)
            and value >= 0
        },
    }


def _public_rows(window: object, hotkey: str, environment: str) -> list[dict[str, Any]]:
    if not bool(getattr(window, "terminal_data_present", False)):
        return []
    rows: list[dict[str, Any]] = []
    for collection_name in ("batch", "runners_up", "rejected"):
        collection = getattr(window, collection_name, [])
        if not isinstance(collection, list):
            continue
        for raw in collection:
            if not isinstance(raw, dict):
                continue
            if str(raw.get("hotkey") or "") != hotkey:
                continue
            env_name = str(raw.get("env_name") or "")
            if environment and env_name and env_name != environment:
                continue
            row = dict(raw)
            row["_collection"] = collection_name
            rows.append(row)
    rows.sort(
        key=lambda row: (
            _finite_number(row.get("arrival_ts"), maximum=4_000_000_000.0) or 0.0,
            str(row.get("merkle_root") or ""),
        )
    )
    return rows


def _apply_authoritative_row(
    attempt: dict[str, Any],
    row: dict[str, Any],
    *,
    source: str,
) -> None:
    accepted = row.get("accepted_into_pool")
    if accepted is None and row.get("_collection") == "batch":
        accepted = True
    reason = str(row.get("reject_reason") or row.get("reason") or "") or None
    if isinstance(accepted, bool):
        attempt["admission"] = {
            "status": "accepted" if accepted else "rejected",
            "reason": None if accepted else reason or "rejected",
            "source": source,
        }
    selected = row.get("selected_for_batch")
    if selected is None and row.get("_collection") == "batch":
        selected = True
    elif selected is None and row.get("_collection") in {"rejected", "runners_up"}:
        selected = False
    rewarded = row.get("rewarded")
    if rewarded is None and row.get("_collection") == "rejected":
        rewarded = False
    rank = row.get("canonical_rank")
    attempt["auction"] = {
        "selected": selected if isinstance(selected, bool) else None,
        "rewarded": rewarded if isinstance(rewarded, bool) else None,
        "canonical_rank": (
            rank
            if isinstance(rank, int) and not isinstance(rank, bool) and rank > 0
            else None
        ),
        "source": source
        if isinstance(selected, bool) or isinstance(rewarded, bool)
        else None,
    }


def reconcile_attempts(
    miner: dict[str, Any],
    *,
    windows: Iterable[object],
    hotkey: str,
    verdict_rows: Iterable[dict[str, Any]] = (),
) -> dict[str, Any]:
    """Overlay only authenticated validator/R2 outcomes onto local attempts."""

    result = copy.deepcopy(miner)
    attempts = result.get("attempts")
    if not isinstance(attempts, list):
        return result
    binding = result.get("binding") if isinstance(result.get("binding"), dict) else {}
    environment = str(binding.get("environment") or "")
    window_map = {
        int(window.n): window
        for window in windows
        if isinstance(getattr(window, "n", None), int)
    }
    verdicts = [row for row in verdict_rows if isinstance(row, dict)]
    attempts_by_window: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for attempt in attempts:
        if isinstance(attempt, dict) and isinstance(attempt.get("window_n"), int):
            attempts_by_window[int(attempt["window_n"])].append(attempt)

    for window_n, local_attempts in attempts_by_window.items():
        local_submissions = [
            attempt
            for attempt in sorted(
                local_attempts, key=lambda row: int(row.get("ordinal") or 0)
            )
            if attempt.get("transport", {}).get("status") != "not_sent"
        ]
        window = window_map.get(window_n)
        public_rows = (
            _public_rows(window, hotkey, environment) if window is not None else []
        )
        unmatched = list(public_rows)
        r2_matched_attempts: set[int] = set()
        for attempt in local_submissions:
            root = str(attempt.get("merkle_root") or "")
            exact = next(
                (
                    row
                    for row in unmatched
                    if root
                    and str(row.get("merkle_root") or "").lower() == root.lower()
                ),
                None,
            )
            verdict_exact = [
                row
                for row in verdicts
                if int(row.get("window_n") or -1) == window_n
                and root
                and str(row.get("merkle_root") or "").lower() == root.lower()
            ]
            for row in verdict_exact:
                _apply_authoritative_row(attempt, row, source="verdicts")
            if exact is not None:
                _apply_authoritative_row(attempt, exact, source="r2")
                unmatched.remove(exact)
                r2_matched_attempts.add(id(attempt))
        unresolved = [
            attempt
            for attempt in local_submissions
            if id(attempt) not in r2_matched_attempts
        ]
        if unresolved and len(unresolved) == len(unmatched):
            for attempt, row in zip(unresolved, unmatched, strict=True):
                _apply_authoritative_row(attempt, row, source="r2")
    result["funnel"] = _derive_v4_funnel(
        [row for row in result.get("events", []) if isinstance(row, dict)],
        [row for row in attempts if isinstance(row, dict)],
    )
    return result


def public_projection(miner: dict[str, Any]) -> dict[str, Any]:
    """Remove internal correlation identifiers from the compatible JSON API."""

    result = copy.deepcopy(miner)
    for event in result.get("events", []):
        if isinstance(event, dict):
            event.pop("attempt_id", None)
    for attempt in result.get("attempts", []):
        if not isinstance(attempt, dict):
            continue
        attempt.pop("merkle_root", None)
        attempt.pop("attempt_ref", None)
    for gpu in result.get("gpu", []):
        if isinstance(gpu, dict):
            uuid = str(gpu.pop("uuid", ""))
            gpu["device_id"] = f"GPU-{uuid[-8:]}" if uuid else "GPU"
    # These private host-side surfaces are intentionally outside the public
    # collector contract, even if a caller injects them into a normalized row.
    result.pop("observability", None)
    result.pop("v5_telemetry", None)
    return result
