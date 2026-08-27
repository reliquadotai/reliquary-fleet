import hashlib
import json
import re
import sqlite3
import time
from pathlib import Path

import pytest

import fleet
import fleet_web
import settings

HOTKEY = "5" + "A" * 47
OPERATOR = "5" + "B" * 47
MINER_SOURCE = "1" * 40
VALIDATOR_SOURCE = "2" * 40
OBSERVED_VALIDATOR_IMAGE = "9" * 40
CHECKPOINT = "3" * 40
V3_CONTRACT = "6" * 64
V3_CHECKPOINT = "7" * 40
V3_SOURCE = "8" * 40
GPU_UUID = "11111111-2222-3333-4444-555555555555"
GPU_UUID_2 = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
CERT_GPU_UUID = f"GPU-{GPU_UUID}"
CERT_DIR = "/var/lib/miner/certification"
CERT_RUNNER = f"{CERT_DIR}/run-certify.sh"
CERT_PROFILE = f"{CERT_DIR}/proof-profile.json"
CERT_MANIFEST = "/var/lib/miner/runtime.json"
CERT_CHECKPOINT = "/var/lib/miner/model"
CERT_RUNNER_SHA256 = "4" * 64


def _state(*, stale_seconds: float = 90.0) -> fleet.BoxState:
    return fleet.BoxState(
        alias="operator@example",
        hotkey=HOTKEY,
        label="standalone",
        color="green",
        unit="reliquary-miner-pro-code-mine.service",
        standalone_telemetry_path="/var/lib/miner/dashboard.json",
        standalone_runtime_manifest_path="/var/lib/miner/runtime.json",
        standalone_telemetry_stale_seconds=stale_seconds,
        operator=OPERATOR,
        proc_alive=True,
        active_pid=42,
        active_started_at=900,
        active_unit="reliquary-miner-pro-code-mine.service",
    )


def _probe(
    *,
    generated_at: float = 990.0,
    hotkeys: list[str] | None = None,
) -> list[str]:
    telemetry = {
        "funnel": {
            "attempts": 10,
            "generation_complete": 10,
            "natural_complete_m8": 10,
            "local_eligible": 4,
            "precommit_accepted": 4,
            "reveal_accepted": 4,
            "pool_accepted": 4,
            "network_proof_passed": 2,
            "selected": 2,
            "rewarded": 2,
            "terminal_final": 9,
        },
        "generated_at": generated_at,
        "latest_window": 123,
        "physical_gpu_hours": 0.5,
        "selected_slots_per_physical_gpu_hour": 4.0,
        "rewarded_slots_per_physical_gpu_hour": 4.0,
    }
    if hotkeys is not None:
        telemetry["hotkeys"] = hotkeys
    wrapper = {
        "schema_version": 1,
        "telemetry": telemetry,
        "manifest": {
            "schema_version": 3,
            "miner_source_revision": MINER_SOURCE,
            "validator_source_revision": VALIDATOR_SOURCE,
            "observed_validator_image_revision": OBSERVED_VALIDATOR_IMAGE,
            "checkpoint_n": 53,
            "checkpoint_revision": CHECKPOINT,
            "model_repo": "ReliquaryForge/model",
            "environment": "opencodeinstruct",
        },
    }
    return [json.dumps(wrapper, separators=(",", ":"))]


def _registry_bound_zero_group_probe() -> tuple[fleet.BoxState, list[str]]:
    """Return a live cp70 canary whose requested window made zero groups."""

    state = _state()
    state.active_unit = "reliquary-code-v3-cp70-canary.service"
    state.unit = state.active_unit
    state.active_unit_registry_path = "/var/lib/miner/active-unit-registry.json"
    state.standalone_telemetry_path = "/var/lib/miner/cp70/dashboard.json"
    state.standalone_runtime_manifest_path = "/var/lib/miner/cp70/runtime.json"
    state.unit_controller_config_paths = (
        (state.active_unit, "/etc/reliquary-code/cp70.toml"),
    )
    manifest_sha256 = "d" * 64
    state.active_unit_registry = {
        "schema_version": 1,
        "generated_at": 998.0,
        "registry_sha256": "e" * 64,
        "error": "",
        "active": {
            "unit": state.active_unit,
            "mode": "canary",
            "controller_config_path": "/etc/reliquary-code/cp70.toml",
            "telemetry_path": state.standalone_telemetry_path,
            "runtime_manifest_path": state.standalone_runtime_manifest_path,
            "checkpoint_revision": V3_CHECKPOINT,
            "source_revision": MINER_SOURCE,
            "unit_fragment_sha256": "a" * 64,
            "controller_config_sha256": "b" * 64,
            "runtime_manifest_sha256": manifest_sha256,
        },
        "rollback": [],
    }
    wrapper = {
        "schema_version": 1,
        "telemetry": {
            "generated_at": 999.0,
            "latest_window": 27382,
            "since": 900.0,
            "until": 999.0,
            "funnel": {
                "attempts": 0,
                "generation_complete": 0,
                "natural_complete_m8": 0,
                "local_eligible": 0,
                "precommit_accepted": 0,
                "reveal_accepted": 0,
                "pool_accepted": 0,
                "network_proof_passed": 0,
                "selected": 0,
                "rewarded": 0,
                "terminal_final": 0,
            },
            "physical_gpu_hours": 0.069,
            "selected_slots_per_physical_gpu_hour": 0.0,
            "rewarded_slots_per_physical_gpu_hour": 0.0,
            # This optional transition snapshot predates the digest-bound
            # cp70 controller registry below and must not leak into current
            # dashboard truth.
            "observability": {
                "active_bundle": "stale-cp69-bundle",
                "identity_match": False,
                "identity_mismatches": {
                    "checkpoint_revision": {
                        "active": CHECKPOINT,
                        "advertised": V3_CHECKPOINT,
                    }
                },
                "unsafe_submission_state": True,
                "supervisor": {
                    "phase": "V3_CANARY",
                    "submission_enabled": False,
                    "last_transition_reason": "cp69 canary active",
                },
                "validator_advertised": {
                    "protocol_version": 3,
                    "generation_profile_id": "qwen35-4b-auction-v3",
                    "generation_contract_sha256": V3_CONTRACT,
                    "checkpoint_repo_id": (
                        "ReliquaryForge/qwen3.5-4b-reliquary-v4"
                    ),
                    "checkpoint_revision": CHECKPOINT,
                    "validator_image_revision": OBSERVED_VALIDATOR_IMAGE,
                    "window_n": 27379,
                },
            },
        },
        "manifest_sha256": manifest_sha256,
        "manifest": {
            "schema_version": 3,
            "miner_source_revision": MINER_SOURCE,
            "validator_source_revision": VALIDATOR_SOURCE,
            "observed_validator_image_revision": OBSERVED_VALIDATOR_IMAGE,
            "checkpoint_n": 70,
            "checkpoint_revision": V3_CHECKPOINT,
            "protocol_version": 3,
            "generation_profile_id": "qwen35-4b-auction-v3",
            "generation_contract_sha256": V3_CONTRACT,
            "checkpoint_profile_sha256": "c" * 64,
            "runtime_fingerprint_sha256": "f" * 64,
            "model_repo": "ReliquaryForge/qwen3.5-4b-reliquary-v4",
            "environment": "opencodeinstruct",
            "components": [],
        },
        "controller": {
            "active_unit": state.active_unit,
            "mode": "canary",
            "config_path": "/etc/reliquary-code/cp70.toml",
            "runtime_manifest_path": state.standalone_runtime_manifest_path,
            "ledger_path": "/var/lib/miner/cp70/ledger.sqlite3",
        },
        # The transition supervisor is deliberately stale.  It remains
        # visible for diagnosis but cannot override the digest-bound live
        # controller registry and manifest.
        "supervisor": {
            "schema_version": 2,
            "state": "V3_LOADING",
            "phase": "V3_LOADING",
            "updated_at": 999.0,
            "active_protocol": 3,
            "active_profile": "qwen35-4b-auction-v3",
            "active_checkpoint_repo_id": (
                "ReliquaryForge/qwen3.5-4b-reliquary-v4"
            ),
            "active_checkpoint": CHECKPOINT,
            "submission_enabled": True,
            "advertised": {
                "protocol_version": 3,
                "generation_profile_id": "qwen35-4b-auction-v3",
                "generation_contract_sha256": V3_CONTRACT,
                "checkpoint_repo_id": (
                    "ReliquaryForge/qwen3.5-4b-reliquary-v4"
                ),
                "checkpoint_revision": V3_CHECKPOINT,
                "validator_image_revision": OBSERVED_VALIDATOR_IMAGE,
                "window_n": 27382,
            },
        },
    }
    return state, [json.dumps(wrapper, separators=(",", ":"))]


def _comparison_scope(
    *,
    kind: str,
    window_start: int,
    window_end: int,
    release_id: str | None = None,
) -> dict:
    return {
        "scope_kind": kind,
        "release_id": release_id,
        "window_start": window_start,
        "window_end": window_end,
        "window_count": window_end - window_start + 1,
        "funnel": {
            "attempts": 10,
            "generation_complete": 10,
            "natural_complete_m8": 10,
            "local_eligible": 4,
            "precommit_accepted": 4,
            "pool_accepted": 4,
            "selected": 2,
            "rewarded": 2,
            "raw_k2": 5,
            "malformed_k2": 1,
            "distribution_dropped_k2": 1,
            "exact_preflight_passing_k2": 3,
        },
        "timings_ms": {
            "local_gate": {
                "count": 10,
                "p50": 20.0,
                "p95": 40.0,
                "max": 50.0,
            },
            "local_proof": {
                "count": 4,
                "p50": 100.0,
                "p95": 200.0,
                "max": 250.0,
            },
        },
        "quota": {
            "capacity_per_hotkey": 8,
            "peak_effective": 4,
            "peak_occupancy": 0.5,
            "dropped_solely_for_quota": 0,
        },
        "physical_gpu_hours": 0.5,
        "selected_slots_per_physical_gpu_hour": 4.0,
        "rewarded_slots_per_physical_gpu_hour": 4.0,
        "per_gpu": {
            f"GPU-{GPU_UUID}": {
                "generated_natural_m8": 10,
                "raw_k2": 5,
                "malformed_k2": 1,
                "distribution_dropped_k2": 1,
                "exact_preflight_passing_k2": 3,
                "precommit_accepted": 4,
                "pool_accepted": 4,
                "selected": 2,
                "rewarded": 2,
                "physical_gpu_hours": 0.5,
                "selected_slots_per_physical_gpu_hour": 4.0,
                "rewarded_slots_per_physical_gpu_hour": 4.0,
                "timings_ms": {
                    "local_gate": {
                        "count": 10,
                        "p50": 55.0,
                        "p95": 111.0,
                        "max": 130.0,
                    },
                    "local_proof": {
                        "count": 4,
                        "p50": 160.0,
                        "p95": 222.0,
                        "max": 260.0,
                    },
                },
            }
        },
    }


def _certification_config() -> dict[str, str]:
    return {
        "runner_path": CERT_RUNNER,
        "runner_sha256": CERT_RUNNER_SHA256,
        "artifact_dir": CERT_DIR,
        "runtime_manifest_path": CERT_MANIFEST,
        "proof_profile_path": CERT_PROFILE,
        "checkpoint_path": CERT_CHECKPOINT,
        "gpu_uuid": CERT_GPU_UUID,
    }


def _certification_probe(
    *,
    active: bool = True,
    generated_at: float = 999.0,
) -> str:
    payload = {
        "schema_version": 1,
        "generated_at": generated_at,
        "active": active,
        "exact": True,
        "submit_disabled_attested": True,
        "runner_pid": 101,
        "worker_pid": 202,
        "phase": "reference_proof",
        "started_at": 900.0,
        "elapsed_s": 99.0,
        **_certification_config(),
        "gpu": {
            "uuid": CERT_GPU_UUID,
            "name": "NVIDIA B200",
            "utilization_pct": 100,
            "memory_used_mb": 8_192,
            "memory_total_mb": 183_359,
            "power_w": 196.0,
            "power_limit_w": 1_000.0,
        },
        "gpu_process_bound": True,
        "gpu_worker_memory_mb": 8_180,
        "identity": {
            "miner_source_revision": MINER_SOURCE,
            "validator_source_revision": VALIDATOR_SOURCE,
            "checkpoint_n": 53,
            "checkpoint_revision": CHECKPOINT,
            "model_repo": "ReliquaryForge/model",
            "environment": "openmathinstruct",
        },
        "artifacts": {
            "reference_proof": False,
            "candidate_proof": False,
        },
    }
    if not active:
        payload["runner_pid"] = 0
        payload["worker_pid"] = 0
        payload["error"] = "not_running"
    return json.dumps(payload, separators=(",", ":"))


def _certification_state() -> fleet.BoxState:
    state = _state()
    state.proc_alive = False
    state.active_unit = ""
    state.active_pid = 0
    state.active_started_at = 0
    state.unit_candidates = (
        ("reliquary-miner-pro-math@canary.service", "/etc/canary.env"),
        ("reliquary-miner-pro-math@mine.service", "/etc/mine.env"),
    )
    state.standalone_certification_config = _certification_config()
    state.standalone_telemetry = {}
    return state


def test_standalone_probe_normalizes_fresh_code_funnel() -> None:
    state = _state()

    fleet._apply_standalone_telemetry_probe(state, _probe(), now=1000.0)

    assert state.standalone_fresh is True
    assert state.standalone_error == ""
    assert state.reference_ready is True
    assert state.engine_mode == "standalone"
    assert state.acceptance_source == "standalone_atomic"
    assert state.miner_source_revision == MINER_SOURCE
    assert state.reliquary_source_revision == VALIDATOR_SOURCE
    assert (
        state.observed_validator_image_revision
        == OBSERVED_VALIDATOR_IMAGE
    )
    assert state.runtime_checkpoint_revision == CHECKPOINT
    assert state.standalone_telemetry["funnel"] == {
        "attempts": 10,
        "generation_complete": 10,
        "natural_complete_m8": 10,
        "local_eligible": 4,
        "precommit_accepted": 4,
        "reveal_accepted": 4,
        "pool_accepted": 4,
        "network_proof_passed": 2,
        "selected": 2,
        "rewarded": 2,
        "terminal_final": 9,
        "terminal_unresolved": None,
    }


def test_standalone_v3_fleet_renders_atomic_generation_truth(
    monkeypatch,
) -> None:
    state = _state()
    # A legacy journal counter must never be shown as authoritative for a
    # standalone controller, even if a stale value happens to remain on the
    # in-memory box object.
    state.pregen_30m = 99
    state.pregen_60m = 199
    wrapper = json.loads(_probe()[0])
    wrapper["manifest"].update(
        protocol_version=3,
        generation_profile_id="qwen35-4b-auction-v3",
        generation_contract_sha256=V3_CONTRACT,
        checkpoint_profile_sha256="4" * 64,
    )
    wrapper["telemetry"]["funnel"].update(
        attempts=3,
        generation_complete=3,
        natural_complete_m8=3,
        local_eligible=0,
        precommit_accepted=0,
        reveal_accepted=0,
        pool_accepted=0,
        network_proof_passed=0,
        selected=0,
        rewarded=0,
        terminal_final=3,
    )

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )
    validator = fleet.ValidatorState(
        window=123,
        image_revision=OBSERVED_VALIDATOR_IMAGE,
        checkpoint_n=53,
        checkpoint_revision=CHECKPOINT,
        checkpoint_repo_id="ReliquaryForge/model",
        protocol_version=3,
        generation_profile_id="qwen35-4b-auction-v3",
        generation_contract_sha256=V3_CONTRACT,
    )
    monkeypatch.setattr(fleet_web, "_boxes", [state])
    monkeypatch.setattr(fleet_web, "_vs", validator)
    monkeypatch.setattr(fleet_web, "_chain", fleet.ChainState())

    details = fleet_web._box_code_auction_details(state, validator)
    rendered = fleet_web.render_fleet_html()
    detail = fleet_web.render_box_detail_html(state.label)
    exported = fleet_web.render_export_json()["fleet"][0]

    assert details["protocol_version"] == 3
    assert details["ledger"]["generation_outcomes"] == {
        "source": "standalone_atomic",
        "current": {
            "attempts": 3,
            "generation_complete": 3,
            "natural_complete_m8": 3,
            "local_eligible": 0,
        },
    }
    assert "generation: attempts=3 · complete=3 · natural M8=3" in rendered
    assert "terminal funnel: attempts=3 · terminal=3 · unresolved=0" in rendered
    assert "generation outcomes: legacy ledger" not in rendered
    assert "terminal v2" not in rendered
    assert 'data-pregen-source="standalone_rolling_generation_unavailable"' in rendered
    assert 'pregen/30m <b class=\'dim\'>—</b>' in detail
    assert exported["pregen_30m"] is None
    assert exported["pregen_60m"] is None
    assert exported["pregen_source"] == (
        "standalone_rolling_generation_unavailable"
    )


def test_standalone_readiness_compares_observed_image_not_public_closure() -> None:
    state = _state()
    fleet._apply_standalone_telemetry_probe(state, _probe(), now=1000.0)
    validator = fleet.ValidatorState(
        window=123,
        image_revision=OBSERVED_VALIDATOR_IMAGE,
        checkpoint_n=53,
        checkpoint_revision=CHECKPOINT,
        checkpoint_repo_id="ReliquaryForge/model",
    )

    issues = fleet_web._standalone_readiness_issues(
        state,
        validator_state=validator,
    )

    assert "standalone_validator_source_mismatch" not in issues
    validator.image_revision = "a" * 40
    assert "standalone_validator_source_mismatch" in (
        fleet_web._standalone_readiness_issues(
            state,
            validator_state=validator,
        )
    )


def test_math_standalone_renders_profile_bound_auction_funnel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _state()
    state.unit = "reliquary-miner-pro-math-canary.service"
    state.active_unit = state.unit
    state.active_environment = "openmathinstruct"
    state.miner_environment = "openmathinstruct"
    wrapper = json.loads(_probe()[0])
    wrapper["manifest"]["environment"] = "openmathinstruct"
    wrapper["telemetry"]["funnel"].update(
        attempts=8,
        generation_complete=8,
        natural_complete_m8=8,
        local_eligible=0,
        precommit_accepted=0,
        reveal_accepted=0,
        pool_accepted=0,
        network_proof_passed=0,
        selected=0,
        rewarded=0,
        terminal_final=0,
    )
    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )
    validator = fleet.ValidatorState(
        window=123,
        image_revision=OBSERVED_VALIDATOR_IMAGE,
        checkpoint_n=53,
        checkpoint_revision=CHECKPOINT,
        checkpoint_repo_id="ReliquaryForge/model",
    )
    monkeypatch.setattr(fleet_web, "_boxes", [state])
    monkeypatch.setattr(fleet_web, "_vs", validator)
    monkeypatch.setattr(fleet_web, "_chain", fleet.ChainState())

    rendered = fleet_web.render_fleet_html()

    assert "auction funnel" in rendered
    assert "env=openmathinstruct" in rendered
    assert "generation: attempts=8 · complete=8 · natural M8=8" in rendered
    assert "terminal funnel: attempts=8 · terminal=0 · unresolved=0" in rendered
    assert "non-Code" not in rendered


def test_active_profile_scope_is_accepted_and_primary_for_live_miner() -> None:
    state = _state()
    wrapper = json.loads(_probe()[0])
    active = _comparison_scope(
        kind="active_profile_checkpoint",
        window_start=123,
        window_end=123,
        release_id="a" * 64,
    )
    active["identity_scope"] = "exact_profile_checkpoint_decomposed"
    active["gpu_identity_filter"] = {
        "protocol_version": 3,
        "generation_profile_id": "qwen35-4b-auction-v3",
    }
    wrapper["telemetry"]["comparison_scopes"] = {"active_profile": active}

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )

    assert state.standalone_fresh is True
    assert state.standalone_telemetry["metrics_posture"] == "active_profile"
    assert state.standalone_telemetry["funnel"]["attempts"] == 10
    scope = state.standalone_telemetry["comparison_scopes"]["active_profile"]
    assert scope["identity_scope"] == "exact_profile_checkpoint_decomposed"
    assert scope["gpu_identity_filter"]["protocol_version"] == 3


def test_no_live_unit_is_fenced_with_zero_current_and_v2_history(
    monkeypatch,
) -> None:
    state = _state()
    state.proc_alive = False
    state.active_unit = ""
    state.active_pid = 0
    state.standalone_supervisor_status_path = "/run/profile/status.json"
    wrapper = json.loads(_probe(generated_at=999.0)[0])
    wrapper["telemetry"]["comparison_scopes"] = {
        "release": _comparison_scope(
            kind="active_release",
            window_start=118,
            window_end=123,
            release_id=MINER_SOURCE,
        )
    }
    wrapper["supervisor"] = None
    wrapper["supervisor_error"] = "missing"

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )

    assert state.standalone_fresh is False
    assert state.standalone_error == "no_mining_unit"
    assert state.standalone_telemetry["latest_window"] is None
    assert state.standalone_telemetry["historical_latest_window"] == 123
    assert set(state.standalone_telemetry["funnel"].values()) == {0}
    assert state.standalone_telemetry["historical_funnel"]["attempts"] == 10
    assert state.standalone_telemetry["metrics_posture"] == "historical_fenced"
    assert state.standalone_supervisor == {
        "configured": True,
        "available": False,
        "fresh": False,
        "submission_enabled": False,
        "error": "missing",
    }

    validator = fleet.ValidatorState(
        window=27283,
        checkpoint_n=57,
        checkpoint_repo_id="ReliquaryForge/qwen3.5-4b-reliquary-v4",
        checkpoint_revision=V3_CHECKPOINT,
        image_revision=V3_SOURCE,
        protocol_version=3,
        generation_profile_id="qwen35-4b-auction-v3",
        generation_contract_sha256=V3_CONTRACT,
    )
    monkeypatch.setattr(fleet_web, "_boxes", [state])
    monkeypatch.setattr(fleet_web, "_vs", validator)
    monkeypatch.setattr(fleet_web, "_chain", fleet.ChainState())

    rendered = fleet_web.render_standalone_mining_truth_html()
    fleet_rendered = fleet_web.render_fleet_html()
    assert "fenced · no mining unit" in rendered
    assert "advertised v3 qwen35-4b-auction-v3" in rendered
    assert V3_CONTRACT[:12] in rendered
    assert "active none" in rendered
    assert "historical v2 legacy-unscoped" in rendered
    assert "historical v2 · release" in rendered
    assert "standalone live" not in fleet_rendered
    assert "advertised v3 qwen35-4b-auction-v3" in fleet_rendered
    assert "active none" in fleet_rendered
    assert "current attempts/selected/rewarded 0/0/0" in fleet_rendered
    assert "historical checkpoint" in fleet_rendered
    assert "historical v2" in fleet_rendered


def test_supervisor_profile_mismatch_is_retained_and_fails_closed() -> None:
    state = _state()
    state.standalone_supervisor_status_path = "/run/profile/status.json"
    wrapper = json.loads(_probe()[0])
    wrapper["supervisor"] = {
        "schema_version": 2,
        "state": "V3_CANARY",
        "phase": "V3_CANARY",
        "updated_at": 999.0,
        "active_bundle": "a" * 64,
        "active_protocol": 2,
        "active_profile": "qwen35-2b-auction-v2",
        "active_checkpoint_repo_id": "ReliquaryForge/qwen3.5-2b-reliquary-v3",
        "active_checkpoint": CHECKPOINT,
        "submission_enabled": True,
        "advertised": {
            "protocol_version": 3,
            "generation_profile_id": "qwen35-4b-auction-v3",
            "generation_contract_sha256": V3_CONTRACT,
            "checkpoint_repo_id": "ReliquaryForge/qwen3.5-4b-reliquary-v4",
            "checkpoint_revision": V3_CHECKPOINT,
            "validator_image_revision": V3_SOURCE,
            "window_n": 27283,
        },
    }

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )

    supervisor = state.standalone_supervisor
    assert supervisor["fresh"] is True
    assert supervisor["identity_match"] is False
    assert supervisor["unsafe_submission_state"] is True
    assert set(supervisor["identity_mismatches"]) >= {
        "protocol_version",
        "generation_profile_id",
        "checkpoint_revision",
    }
    assert state.runtime_parity_ok is False


def test_registry_bound_controller_overrides_stale_supervisor_tuple(
    monkeypatch,
) -> None:
    state, lines = _registry_bound_zero_group_probe()
    state.standalone_supervisor_status_path = "/run/profile/status.json"

    fleet._apply_standalone_telemetry_probe(state, lines, now=1000.0)

    assert state.standalone_error == ""
    assert state.standalone_fresh is True
    assert state.runtime_parity_ok is True
    assert state.runtime_checkpoint_n == 70
    assert state.runtime_checkpoint_revision == V3_CHECKPOINT
    assert state.miner_source_revision == MINER_SOURCE
    assert state.miner_window == 27382
    assert state.standalone_telemetry["latest_window"] == 27382
    assert state.standalone_telemetry["funnel"]["generation_complete"] == 0
    assert state.standalone_telemetry["controller_authority"] == {
        "source": "active_unit_registry",
        "mode": "canary",
        "unit": state.active_unit,
        "protocol_version": 3,
        "generation_profile_id": "qwen35-4b-auction-v3",
        "generation_contract_sha256": V3_CONTRACT,
        "checkpoint_n": 70,
        "checkpoint_repo_id": "ReliquaryForge/qwen3.5-4b-reliquary-v4",
        "checkpoint_revision": V3_CHECKPOINT,
        "source_revision": MINER_SOURCE,
        "validator_source_revision": VALIDATOR_SOURCE,
        "observed_validator_image_revision": OBSERVED_VALIDATOR_IMAGE,
        "checkpoint_profile_sha256": "c" * 64,
        "registry_sha256": "e" * 64,
        "runtime_manifest_sha256": "d" * 64,
    }
    observability = state.standalone_telemetry["observability"]
    assert "validator_advertised" not in observability
    assert "active_bundle" not in observability
    assert "identity_match" not in observability
    assert "identity_mismatches" not in observability
    assert "unsafe_submission_state" not in observability
    assert observability["supervisor"] == {
        "status": "FENCED_STALE",
        "reason": "superseded_by_active_unit_registry",
        "authority_source": "active_unit_registry",
        "mismatch_fields": ["checkpoint_revision"],
    }
    assert CHECKPOINT not in json.dumps(observability, sort_keys=True)
    # Retain the stale transition record as diagnostic evidence without
    # allowing it to revoke a newer active registry authority.
    assert state.standalone_supervisor["identity_match"] is False
    assert state.standalone_supervisor["unsafe_submission_state"] is True

    validator = fleet.ValidatorState(
        window=27382,
        checkpoint_n=70,
        checkpoint_repo_id="ReliquaryForge/qwen3.5-4b-reliquary-v4",
        checkpoint_revision=V3_CHECKPOINT,
        image_revision=OBSERVED_VALIDATOR_IMAGE,
    )
    status = fleet_web._box_lane_status(state, validator_state=validator)
    assert status["status"] == "CANARY"
    assert status["funnel_authoritative"] is True
    assert status["funnel"]["attempts"] == 0
    assert status["funnel"]["generation_complete"] == 0

    monkeypatch.setattr(fleet_web, "_boxes", [state])
    monkeypatch.setattr(fleet_web, "_vs", validator)
    monkeypatch.setattr(fleet_web, "_chain", fleet.ChainState())
    rendered = fleet_web.render_standalone_mining_truth_html()
    assert "27382" in rendered
    assert "requested · completed groups 0" in rendered
    assert V3_CHECKPOINT in rendered
    assert CHECKPOINT not in rendered


def test_registry_bound_controller_manifest_digest_mismatch_fails_closed() -> None:
    state, lines = _registry_bound_zero_group_probe()
    wrapper = json.loads(lines[0])
    wrapper["manifest_sha256"] = "0" * 64

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )

    assert state.standalone_error == "active_unit_registry_binding"
    assert state.standalone_fresh is False
    assert state.runtime_parity_ok is False
    assert state.reference_ready is False


def test_registry_bound_active_generation_is_inflight_not_an_attempt() -> None:
    state, lines = _registry_bound_zero_group_probe()
    wrapper = json.loads(lines[0])
    wrapper["telemetry"]["window_lifecycle"] = {
        "status": "ACTIVE",
        "heartbeat_at": 999.0,
        "heartbeat_age_seconds": 0.0,
        "validator_window_n": 27382,
        "validator_state": "open",
        "latest_run": {
            "window_n": 27382,
            "status": "started",
            "started_at": 998.0,
            "updated_at": 998.0,
            "completed_at": None,
            "failure_stage": "",
            "failure_type": "",
            "generated_groups": 0,
            "locally_eligible": 0,
            "precommitted": 0,
            "http_provisional": 0,
        },
        "latest_generated_window": None,
        "latest_attempt_window": None,
    }

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )

    assert state.standalone_fresh is True
    assert state.runtime_parity_ok is True
    assert state.miner_state == "active_generation"
    assert state.miner_window == 27382
    assert state.miner_inflight == 1
    assert state.miner_ready == 0
    assert state.miner_submitted_this_win == 0
    assert state.standalone_telemetry["funnel"]["attempts"] == 0
    assert state.standalone_telemetry["funnel"]["generation_complete"] == 0


@pytest.mark.parametrize(
    ("run_status", "run_window"),
    (("completed", 27382), ("started", 27381)),
)
def test_terminal_or_old_lifecycle_is_not_active_generation(
    run_status: str,
    run_window: int,
) -> None:
    state, lines = _registry_bound_zero_group_probe()
    wrapper = json.loads(lines[0])
    wrapper["telemetry"]["window_lifecycle"] = {
        "status": "READY" if run_status == "completed" else "ACTIVE",
        "heartbeat_at": 999.0,
        "heartbeat_age_seconds": 0.0,
        "validator_window_n": 27382,
        "validator_state": "open",
        "latest_run": {
            "window_n": run_window,
            "status": run_status,
            "started_at": 998.0,
            "updated_at": 999.0,
            "completed_at": 999.0 if run_status == "completed" else None,
            "generated_groups": 0,
            "locally_eligible": 0,
            "precommitted": 0,
            "http_provisional": 0,
        },
        "latest_generated_window": None,
        "latest_attempt_window": None,
    }

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )

    assert state.miner_state == "live"
    assert state.miner_inflight == 0
    assert state.miner_ready == 1
    assert state.standalone_telemetry["funnel"]["attempts"] == 0


def test_validator_advertised_contract_digest_is_canonical() -> None:
    validator = fleet.ValidatorState()
    contract = {
        "profile_id": "qwen35-4b-auction-v3",
        "protocol_version": 3,
        "sampling": {"rollouts": 8, "temperature": 0.6},
    }
    fleet._apply_validator_state(
        validator,
        {
            "state": "open",
            "window_n": 27283,
            "protocol_version": 3,
            "generation_profile_id": "qwen35-4b-auction-v3",
            "generation_contract": contract,
        },
        source="http",
    )
    expected = hashlib.sha256(
        json.dumps(contract, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert validator.protocol_version == 3
    assert validator.generation_profile_id == "qwen35-4b-auction-v3"
    assert validator.generation_contract_sha256 == expected


@pytest.mark.parametrize(
    ("lifecycle_status", "blocked"),
    (
        ("READY", False),
        ("ACTIVE", False),
        ("RECOVERING", True),
        ("STALLED", True),
    ),
)
def test_math_window_lifecycle_is_preserved_and_controls_readiness(
    lifecycle_status: str,
    blocked: bool,
) -> None:
    state = _state()
    wrapper = json.loads(_probe()[0])
    wrapper["telemetry"]["latest_window"] = None
    wrapper["telemetry"]["window_lifecycle"] = {
        "status": lifecycle_status,
        "heartbeat_at": 989.0,
        "heartbeat_age_seconds": 1.0,
        "validator_window_n": 124,
        "validator_state": "open",
        "latest_run": {
            "window_n": 123,
            "status": (
                "failed"
                if lifecycle_status == "RECOVERING"
                else "started"
                if lifecycle_status == "ACTIVE"
                else "completed"
            ),
            "started_at": 980.0,
            "updated_at": 989.0,
            "completed_at": (None if lifecycle_status == "ACTIVE" else 989.0),
            "failure_stage": (
                "generation" if lifecycle_status == "RECOVERING" else None
            ),
            "failure_type": (
                "WorkerEpochChangedError" if lifecycle_status == "RECOVERING" else None
            ),
            "generated_groups": 8,
            "locally_eligible": 2,
            "precommitted": 1,
            "http_provisional": 1,
        },
        "latest_generated_window": 123,
        "latest_attempt_window": 123,
    }

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )

    assert state.standalone_fresh is True
    assert state.standalone_telemetry["latest_window"] is None
    assert state.standalone_telemetry["window_lifecycle"]["status"] == lifecycle_status
    issues = fleet_web._standalone_readiness_issues(
        state,
        validator_state=None,
    )
    lifecycle_issue = f"standalone_window_lifecycle:{lifecycle_status.lower()}"
    assert (lifecycle_issue in issues) is blocked


@pytest.mark.parametrize(
    ("last_natural_window", "blocked"),
    ((123, False), (122, True)),
)
def test_configured_gpu_natural_generation_lag_controls_readiness(
    last_natural_window: int,
    blocked: bool,
) -> None:
    state = _state()
    state.standalone_fresh = True
    state.runtime_parity_ok = True
    state.reliquary_source_revision = VALIDATOR_SOURCE
    state.runtime_checkpoint_n = 53
    state.runtime_checkpoint_revision = CHECKPOINT
    state.runtime_checkpoint_repo = "ReliquaryForge/model"
    state.runtime_components = [
        {
            "role": "generation",
            "component_id": GPU_UUID,
        }
    ]
    state.standalone_telemetry = {
        "per_gpu": {
            f"GPU-{GPU_UUID}": {
                "last_natural_window": last_natural_window,
            },
            # A historical ledger row is not a configured generation
            # component and therefore cannot make the live miner unready.
            f"GPU-{GPU_UUID_2}": {
                "last_natural_window": 1,
            },
        }
    }
    validator = fleet.ValidatorState(
        window=124,
        image_revision=VALIDATOR_SOURCE,
        checkpoint_n=53,
        checkpoint_revision=CHECKPOINT,
        checkpoint_repo_id="ReliquaryForge/model",
    )

    issues = fleet_web._standalone_readiness_issues(
        state,
        validator_state=validator,
    )
    lag_issues = [
        issue for issue in issues if issue.startswith("standalone_gpu_generation_lag:")
    ]
    expected = (
        [f"standalone_gpu_generation_lag:GPU-{GPU_UUID}:last=122:validator=124"]
        if blocked
        else []
    )
    assert lag_issues == expected
    if blocked:
        label = fleet_web._readiness_issue_label(lag_issues[0])
        assert "natural generation stale" in label
        assert "w122 vs w124" in label


def _probe_ledger_summary() -> object:
    source = fleet._STANDALONE_TELEMETRY_PROBE_SOURCE
    prefix = source.split('out = {"schema_version": 1}', 1)[0]
    namespace: dict[str, object] = {}
    exec(prefix, namespace)
    return namespace["ledger_summary"]


def test_remote_probe_reads_math_ledger_without_losing_gpu_identity(
    tmp_path: Path,
) -> None:
    ledger_path = tmp_path / "math.sqlite3"
    now = time.time()
    connection = sqlite3.connect(ledger_path)
    connection.executescript(
        """
        CREATE TABLE attempts(
            group_id TEXT PRIMARY KEY,
            window_n INTEGER NOT NULL,
            prompt_idx INTEGER NOT NULL,
            created_at REAL NOT NULL
        );
        CREATE TABLE generated_groups(
            window_n INTEGER NOT NULL,
            prompt_idx INTEGER NOT NULL,
            physical_gpu_id TEXT NOT NULL,
            natural_complete_m8 INTEGER NOT NULL
        );
        CREATE TABLE gpu_intervals(
            gpu_uuid TEXT NOT NULL,
            started_at REAL NOT NULL,
            completed_at REAL NOT NULL,
            role TEXT NOT NULL
        );
        """
    )
    connection.execute(
        "INSERT INTO attempts VALUES (?,?,?,?)",
        ("group", 123, 7, now - 5.0),
    )
    connection.execute(
        "INSERT INTO generated_groups VALUES (?,?,?,?)",
        (123, 7, f"GPU-{GPU_UUID}", 1),
    )
    connection.execute(
        "INSERT INTO gpu_intervals VALUES (?,?,?,?)",
        (f"GPU-{GPU_UUID}", now - 10.0, now, "generation"),
    )
    connection.commit()
    connection.close()

    summary = _probe_ledger_summary()(
        str(ledger_path),
        now - 20.0,
        now + 1.0,
    )

    gpu = summary["per_gpu"][f"GPU-{GPU_UUID}"]
    assert gpu["attempts"] == 1
    assert gpu["natural_complete_m8"] == 1
    assert gpu["first_window"] == 123
    assert gpu["last_window"] == 123
    assert gpu["last_natural_window"] == 123


def test_remote_probe_preserves_two_gpu_code_attribution(
    tmp_path: Path,
) -> None:
    ledger_path = tmp_path / "code.sqlite3"
    now = time.time()
    connection = sqlite3.connect(ledger_path)
    connection.executescript(
        """
        CREATE TABLE attempts(
            group_id TEXT PRIMARY KEY,
            physical_gpu_id TEXT NOT NULL,
            window_n INTEGER NOT NULL,
            natural_complete INTEGER NOT NULL,
            created_at REAL NOT NULL
        );
        CREATE TABLE gpu_intervals(
            physical_gpu_id TEXT NOT NULL,
            started_at REAL NOT NULL,
            ended_at REAL NOT NULL
        );
        """
    )
    for group_id, gpu, prompt_offset in (
        ("h100", f"GPU-{GPU_UUID}", 0),
        ("rtx", f"GPU-{GPU_UUID_2}", 1),
    ):
        connection.execute(
            "INSERT INTO attempts VALUES (?,?,?,?,?)",
            (group_id, gpu, 123 + prompt_offset, 1, now - 5.0),
        )
        connection.execute(
            "INSERT INTO gpu_intervals VALUES (?,?,?)",
            (gpu, now - 10.0, now),
        )
    connection.execute(
        "INSERT INTO attempts VALUES (?,?,?,?,?)",
        (
            "h100-incomplete",
            f"GPU-{GPU_UUID}",
            125,
            0,
            now - 4.0,
        ),
    )
    connection.commit()
    connection.close()

    summary = _probe_ledger_summary()(
        str(ledger_path),
        now - 20.0,
        now + 1.0,
    )

    assert set(summary["per_gpu"]) == {
        f"GPU-{GPU_UUID}",
        f"GPU-{GPU_UUID_2}",
    }
    assert summary["per_gpu"][f"GPU-{GPU_UUID}"]["attempts"] == 2
    assert summary["per_gpu"][f"GPU-{GPU_UUID}"]["last_window"] == 125
    assert summary["per_gpu"][f"GPU-{GPU_UUID}"]["last_natural_window"] == 123
    assert summary["per_gpu"][f"GPU-{GPU_UUID_2}"]["last_natural_window"] == 124


def test_standalone_probe_fails_stale_without_losing_exact_identity() -> None:
    state = _state(stale_seconds=30.0)

    fleet._apply_standalone_telemetry_probe(
        state, _probe(generated_at=900.0), now=1000.0
    )

    assert state.standalone_fresh is False
    assert state.standalone_error == "stale"
    assert state.reference_ready is False
    assert state.runtime_checkpoint_revision == CHECKPOINT
    issues = fleet_web._standalone_readiness_issues(state, validator_state=None)
    assert "standalone_telemetry:stale" in issues
    assert "standalone_telemetry_stale" in issues


def test_fresh_snapshot_is_not_failed_by_stale_auxiliary_data_products() -> None:
    state = _state(stale_seconds=30.0)
    wrapper = json.loads(_probe(generated_at=999.0)[0])
    wrapper["telemetry"]["staleness_seconds"] = {
        "ledger_update": 4.0,
        "r2_reconciliation": 120.0,
        "memory_ingest": 6_000.0,
    }

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )

    assert state.standalone_fresh is True
    assert state.standalone_error == ""
    assert state.standalone_progress_age_s == 4.0
    assert state.standalone_telemetry["staleness_seconds"] == {
        "ledger_update": 4.0,
        "r2_reconciliation": 120.0,
        "memory_ingest": 6_000.0,
    }


def test_standalone_probe_rejects_wrong_declared_hotkey() -> None:
    state = _state()

    fleet._apply_standalone_telemetry_probe(
        state, _probe(hotkeys=["5" + "C" * 47]), now=1000.0
    )

    assert state.standalone_fresh is False
    assert state.standalone_error == "hotkey_mismatch"
    assert state.standalone_telemetry == {}


def test_standalone_probe_preserves_exact_component_bindings() -> None:
    state = _state()
    wrapper = json.loads(_probe()[0])
    wrapper["manifest"]["components"] = [
        {
            "role": "generation",
            "component_id": "rtx",
            "manifest_file_sha256": "4" * 64,
            "runtime_payload_sha256": "5" * 64,
            "runtime_profile_sha256": "6" * 64,
            "health_gpu_uuid_required": True,
        }
    ]

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )

    assert state.standalone_fresh is True
    assert state.runtime_components == wrapper["manifest"]["components"]


def test_standalone_probe_and_ui_preserve_comparable_gpu_scopes(
    monkeypatch,
) -> None:
    state = _state()
    wrapper = json.loads(_probe()[0])
    wrapper["telemetry"]["comparison_scopes"] = {
        "release": _comparison_scope(
            kind="active_release",
            window_start=118,
            window_end=123,
            release_id=MINER_SOURCE,
        ),
        "latest_six": _comparison_scope(
            kind="latest_six_completed_windows",
            window_start=118,
            window_end=123,
        ),
    }
    # Older miners emit only scope-wide timings. Keep that payload valid and
    # let the UI fall back to the aggregate values for each physical GPU.
    del wrapper["telemetry"]["comparison_scopes"]["latest_six"]["per_gpu"][
        f"GPU-{GPU_UUID}"
    ]["timings_ms"]

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )

    scopes = state.standalone_telemetry["comparison_scopes"]
    assert scopes["release"]["release_id"] == MINER_SOURCE
    assert scopes["latest_six"]["window_count"] == 6
    assert scopes["latest_six"]["per_gpu"][f"GPU-{GPU_UUID}"]["raw_k2"] == 5
    assert (
        scopes["release"]["per_gpu"][f"GPU-{GPU_UUID}"]["timings_ms"]["local_gate"][
            "p95"
        ]
        == 111.0
    )
    assert scopes["latest_six"]["per_gpu"][f"GPU-{GPU_UUID}"]["timings_ms"] == {}

    monkeypatch.setattr(fleet_web, "_boxes", [state])
    monkeypatch.setattr(fleet_web, "_chain", fleet.ChainState())
    rendered = fleet_web.render_standalone_mining_truth_html()
    assert "Comparable GPU yield" in rendered
    assert "latest 6 completed" in rendered
    assert "malformed/dist." in rendered
    assert "rewarded/GPUh" in rendered
    assert ">111<" in rendered
    assert ">222<" in rendered
    assert "peak 4/8 · drops 0" in rendered


def test_comparison_scope_accepts_generation_before_attempt_registration() -> None:
    state = _state()
    wrapper = json.loads(_probe()[0])
    scope = _comparison_scope(
        kind="latest_six_completed_windows",
        window_start=118,
        window_end=123,
    )
    scope["funnel"].update(
        {
            "attempts": 0,
            "generation_complete": 2,
            "natural_complete_m8": 1,
            "local_eligible": 0,
            "precommit_accepted": 0,
            "pool_accepted": 0,
            "selected": 0,
            "rewarded": 0,
            "raw_k2": 0,
            "malformed_k2": 0,
            "distribution_dropped_k2": 0,
            "exact_preflight_passing_k2": 0,
        }
    )
    wrapper["telemetry"]["comparison_scopes"] = {"latest_six": scope}

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )

    assert state.standalone_fresh is True
    funnel = state.standalone_telemetry["comparison_scopes"]["latest_six"]["funnel"]
    assert funnel["attempts"] == 0
    assert funnel["generation_complete"] == 2
    assert funnel["natural_complete_m8"] == 1


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("natural_complete_m8", 11),
        ("local_eligible", 11),
        ("raw_k2", 11),
        ("precommit_accepted", 5),
        ("pool_accepted", 5),
        ("selected", 5),
        ("rewarded", 3),
    ),
)
def test_comparison_scope_rejects_invalid_funnel_order(
    field: str,
    value: int,
) -> None:
    state = _state()
    wrapper = json.loads(_probe()[0])
    scope = _comparison_scope(
        kind="latest_six_completed_windows",
        window_start=118,
        window_end=123,
    )
    scope["funnel"][field] = value
    wrapper["telemetry"]["comparison_scopes"] = {"latest_six": scope}

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )

    assert state.standalone_fresh is False
    assert state.standalone_error == "comparison_funnel_order"
    assert state.standalone_telemetry == {}


def test_comparison_scope_preserves_and_renders_per_gpu_quota(
    monkeypatch,
) -> None:
    state = _state()
    wrapper = json.loads(_probe()[0])
    scope = _comparison_scope(
        kind="latest_six_completed_windows",
        window_start=118,
        window_end=123,
    )
    first_gpu = scope["per_gpu"][f"GPU-{GPU_UUID}"]
    first_gpu["physical_gpu_hours"] = 0.25
    first_gpu["quota"] = {
        "capacity_per_hotkey": 8,
        "peak_effective": 2,
        "peak_occupancy": 0.25,
        "dropped_solely_for_quota": 1,
    }
    second_gpu = json.loads(json.dumps(first_gpu))
    second_gpu["physical_gpu_hours"] = 0.25
    second_gpu["quota"] = {
        "capacity_per_hotkey": 8,
        "peak_effective": 5,
        "peak_occupancy": 0.625,
        "dropped_solely_for_quota": 3,
    }
    scope["per_gpu"][f"GPU-{GPU_UUID_2}"] = second_gpu
    wrapper["telemetry"]["comparison_scopes"] = {
        "latest_six": scope,
    }

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )

    assert state.standalone_fresh is True
    rows = state.standalone_telemetry["comparison_scopes"]["latest_six"]["per_gpu"]
    assert rows[f"GPU-{GPU_UUID}"]["quota"]["peak_effective"] == 2
    assert rows[f"GPU-{GPU_UUID_2}"]["quota"]["dropped_solely_for_quota"] == 3

    monkeypatch.setattr(fleet_web, "_boxes", [state])
    monkeypatch.setattr(fleet_web, "_chain", fleet.ChainState())
    rendered = fleet_web.render_standalone_mining_truth_html()
    assert "peak 2/8 · drops 1" in rendered
    assert "peak 5/8 · drops 3" in rendered


@pytest.mark.parametrize(
    "invalid_quota",
    (
        None,
        {
            "capacity_per_hotkey": 8,
            "peak_effective": 9,
            "peak_occupancy": 1.125,
            "dropped_solely_for_quota": 0,
        },
        {
            "capacity_per_hotkey": 8,
            "peak_effective": 4,
            "peak_occupancy": 1.1,
            "dropped_solely_for_quota": 0,
        },
    ),
)
def test_comparison_scope_rejects_invalid_per_gpu_quota(
    invalid_quota,
) -> None:
    state = _state()
    wrapper = json.loads(_probe()[0])
    scope = _comparison_scope(
        kind="latest_six_completed_windows",
        window_start=118,
        window_end=123,
    )
    scope["per_gpu"][f"GPU-{GPU_UUID}"]["quota"] = invalid_quota
    wrapper["telemetry"]["comparison_scopes"] = {
        "latest_six": scope,
    }

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )

    assert state.standalone_fresh is False
    assert state.standalone_error == "comparison_gpu_quota"
    assert state.standalone_telemetry == {}


def test_standalone_probe_binds_active_config_and_normalizes_gpu_ledger() -> None:
    state = _state()
    state.active_unit = "reliquary-miner-pro-code-canary.service"
    state.unit_controller_config_paths = (
        (
            "reliquary-miner-pro-code-canary.service",
            "/etc/reliquary-code/canary.toml",
        ),
    )
    wrapper = json.loads(_probe()[0])
    wrapper["controller"] = {
        "active_unit": state.active_unit,
        "mode": "canary",
        "config_path": "/etc/reliquary-code/canary.toml",
        "runtime_manifest_path": ("/var/lib/reliquary-code/runtime-manifest.dual.json"),
        "ledger_path": ("/var/lib/reliquary-code/state/code-attempts.sqlite3"),
    }
    wrapper["ledger"] = {
        "schema_version": 1,
        "physical_gpu_hours": 0.25,
        "per_gpu": {
            f"GPU-{GPU_UUID}": {
                "aliases": [GPU_UUID, f"GPU-{GPU_UUID}"],
                "attempts": 10,
                "natural_complete_m8": 10,
                "generated_windows": 3,
                "natural_complete_windows": 3,
                "first_window": 120,
                "last_window": 123,
                "physical_gpu_hours": 0.25,
            },
        },
    }

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )

    assert state.standalone_fresh is True
    assert state.standalone_controller_mode == "canary"
    assert state.standalone_controller_config_path.endswith("canary.toml")
    assert state.standalone_telemetry["physical_gpu_hours"] == 0.25
    assert state.standalone_telemetry["selected_slots_per_physical_gpu_hour"] == 8.0
    assert list(state.standalone_telemetry["per_gpu"]) == [f"GPU-{GPU_UUID}"]
    assert (
        state.standalone_telemetry["per_gpu"][f"GPU-{GPU_UUID}"]["last_natural_window"]
        == 123
    )
    assert fleet._canonical_physical_gpu_id(GPU_UUID) == (f"GPU-{GPU_UUID}")
    assert fleet._canonical_physical_gpu_id(f"GPU-{GPU_UUID}") == (f"GPU-{GPU_UUID}")


def test_active_controller_binding_allows_empty_startup_ledger() -> None:
    """A new exact controller is authoritative before its first GPU interval."""

    state = _state()
    state.active_unit = "reliquary-miner-pro-code-canary.service"
    state.unit_controller_config_paths = (
        (
            state.active_unit,
            "/etc/reliquary-code/canary.toml",
        ),
    )
    wrapper = json.loads(_probe()[0])
    wrapper["telemetry"]["funnel"] = {
        name: 0 for name in wrapper["telemetry"]["funnel"]
    }
    wrapper["telemetry"]["physical_gpu_hours"] = 0.0
    wrapper["telemetry"]["selected_slots_per_physical_gpu_hour"] = None
    wrapper["telemetry"]["rewarded_slots_per_physical_gpu_hour"] = None
    wrapper["telemetry"]["latest_window"] = None
    wrapper["controller"] = {
        "active_unit": state.active_unit,
        "mode": "canary",
        "config_path": "/etc/reliquary-code/canary.toml",
        "runtime_manifest_path": "/var/lib/reliquary-code/runtime-manifest.json",
        "ledger_path": "/var/lib/reliquary-code/state/code-attempts.sqlite3",
    }
    wrapper["ledger"] = {
        "schema_version": 1,
        "physical_gpu_hours": 0.0,
        "per_gpu": {},
    }

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )

    assert state.standalone_fresh is True
    assert state.standalone_error == ""
    assert state.standalone_controller_mode == "canary"
    assert state.standalone_telemetry["per_gpu"] == {}
    assert state.standalone_telemetry["physical_gpu_hours"] == 0.0


def test_null_selected_rate_is_rejected_after_gpu_time_accrues() -> None:
    state = _state()
    wrapper = json.loads(_probe()[0])
    wrapper["telemetry"]["selected_slots_per_physical_gpu_hour"] = None

    fleet._apply_standalone_telemetry_probe(
        state,
        [json.dumps(wrapper, separators=(",", ":"))],
        now=1000.0,
    )

    assert state.standalone_fresh is False
    assert state.standalone_error == (
        "invalid_float:selected_slots_per_physical_gpu_hour"
    )


def test_standalone_config_requires_both_atomic_paths() -> None:
    base = {
        "alias": "operator@example",
        "hotkey": HOTKEY,
        "label": "standalone",
        "operator": OPERATOR,
        "unit": "miner.service",
    }
    try:
        settings._coerce_fleet_box(
            {**base, "telemetry_path": "/var/lib/miner/dashboard.json"}
        )
    except ValueError as exc:
        assert "configured together" in str(exc)
    else:  # pragma: no cover - explicit assertion is clearer than pytest.raises
        raise AssertionError("unpaired standalone paths were accepted")

    box = settings._coerce_fleet_box(
        {
            **base,
            "telemetry_path": "/var/lib/miner/dashboard.json",
            "runtime_manifest_path": "/var/lib/miner/runtime.json",
            "supervisor_status_path": "/run/miner/profile-supervisor.json",
        }
    )
    assert box.operator == OPERATOR
    assert box.telemetry_stale_seconds == 90.0
    assert box.supervisor_status_path == "/run/miner/profile-supervisor.json"

    certified = settings._coerce_fleet_box(
        {
            **base,
            "telemetry_path": "/var/lib/miner/dashboard.json",
            "runtime_manifest_path": CERT_MANIFEST,
            "standalone_certification": _certification_config(),
        }
    )
    assert certified.standalone_certification is not None
    assert certified.standalone_certification.runner_path == CERT_RUNNER
    assert certified.standalone_certification.gpu_uuid == CERT_GPU_UUID


def test_standalone_truth_panel_exposes_exact_identity_and_funnel(
    monkeypatch,
) -> None:
    state = _state()
    fleet._apply_standalone_telemetry_probe(state, _probe(), now=1000.0)
    monkeypatch.setattr(fleet_web, "_boxes", [state])
    monkeypatch.setattr(
        fleet_web,
        "_chain",
        fleet.ChainState(
            hotkeys=[
                fleet.HotkeyChainEntry(
                    label="standalone",
                    hotkey=HOTKEY,
                    uid=175,
                    stake=0.0,
                    emission=0.0,
                )
            ]
        ),
    )

    rendered = fleet_web.render_standalone_mining_truth_html()

    assert "Standalone mining truth" in rendered
    assert HOTKEY in rendered
    assert OPERATOR in rendered
    assert MINER_SOURCE in rendered
    assert VALIDATOR_SOURCE in rendered
    assert CHECKPOINT in rendered
    assert ">175<" in rendered
    assert "selected/GPUh" in rendered


def test_exact_certification_is_non_mining_and_fails_stale() -> None:
    state = _certification_state()

    assert fleet._apply_standalone_certification_probe(
        state,
        _certification_probe(),
        now=1000.0,
    )
    certification = state.standalone_certification
    assert certification["active"] is True
    assert certification["phase"] == "reference_proof"
    assert certification["gpu"]["utilization_pct"] == 100
    assert certification["gpu_process_bound"] is True
    assert state.proc_alive is False
    assert state.active_unit == ""
    assert state.active_pid == 0
    assert state.standalone_telemetry == {}
    assert "certifying_non_mining" in fleet_web._box_readiness_issues(state)

    assert not fleet._apply_standalone_certification_probe(
        state,
        _certification_probe(active=False, generated_at=1001.0),
        now=1001.0,
    )
    assert state.standalone_certification == {
        "active": False,
        "fresh": True,
        "generated_at": 1001.0,
        "error": "not_running",
    }


def test_exact_wallet_free_worker_is_certification_not_mining(
    monkeypatch,
) -> None:
    state = _certification_state()
    service = {
        "service_unit": "reliquary-math-v3-certify.service",
        "service_user": "reliquary-math",
        "service_config_path": f"{CERT_DIR}/worker.toml",
        "service_config_sha256": "5" * 64,
        "progress_path": "/run/reliquary-cert/progress.json",
    }
    state.standalone_certification_config.update(service)
    payload = json.loads(_certification_probe())
    payload.update(
        service,
        phase="generation_ready",
        wallet_free_attested=True,
    )
    payload["identity"].update(
        checkpoint_n=58,
        checkpoint_revision=V3_CHECKPOINT,
        model_repo="ReliquaryForge/qwen3.5-4b-reliquary-v4",
        environment="opencodeinstruct",
    )

    assert fleet._apply_standalone_certification_probe(
        state, json.dumps(payload), now=1000.0
    )
    assert state.standalone_certification["wallet_free_attested"] is True
    assert state.standalone_certification["service_unit"] == service["service_unit"]
    assert state.proc_alive is False
    assert state.standalone_telemetry == {}

    monkeypatch.setattr(fleet_web, "_boxes", [state])
    monkeypatch.setattr(fleet_web, "_vs", fleet.ValidatorState())
    monkeypatch.setattr(fleet_web, "_chain", fleet.ChainState())
    rendered = fleet_web.render_fleet_html()
    assert "CERTIFYING" in rendered
    posture_cell = re.search(r'title="([^"]+)"[^>]*>CERTIFYING', rendered)
    assert posture_cell is not None
    assert "exact cert c58" in posture_cell.group(1)
    assert MINER_SOURCE in posture_cell.group(1)
    assert "active (provisioned_manifest)" not in posture_cell.group(1)
    assert "Code certifying" in rendered
    assert "non-Code" not in rendered
    assert "current attempts/selected/rewarded 0/0/0" in rendered


def test_no_active_miner_keeps_history_before_exact_certification(
    monkeypatch,
) -> None:
    state = _certification_state()
    state.standalone_telemetry = {"funnel": {"attempts": 77}}
    state.proc_alive = True
    state.active_unit = "stale.service"
    state.active_pid = 77

    def no_active(box: fleet.BoxState):
        box.unit_resolution_error = "no_allowed_unit_active"
        return None

    calls: list[str] = []

    def collect_files(box: fleet.BoxState) -> None:
        calls.append("files")
        fleet._apply_standalone_telemetry_probe(
            box,
            _probe(generated_at=999.0),
            now=1000.0,
        )

    def collect_cert(box: fleet.BoxState) -> bool:
        calls.append("cert")
        assert box.standalone_telemetry["metrics_posture"] == "historical_fenced"
        return fleet._apply_standalone_certification_probe(
            box,
            _certification_probe(),
            now=1000.0,
        )

    monkeypatch.setattr(fleet, "_resolve_allowed_active_unit", no_active)
    monkeypatch.setattr(
        fleet,
        "_collect_standalone_certification_after_unit_failure",
        collect_cert,
    )
    monkeypatch.setattr(
        fleet,
        "_collect_standalone_files_after_unit_failure",
        collect_files,
    )

    fleet._collect_box_in_place(state)

    assert state.unit_resolution_error == "no_allowed_unit_active"
    assert state.proc_alive is False
    assert state.active_unit == ""
    assert state.active_pid == 0
    assert calls == ["files", "cert"]
    assert state.standalone_telemetry["funnel"]["attempts"] == 0
    assert state.standalone_telemetry["historical_funnel"]["attempts"] == 10
    assert state.standalone_certification["active"] is True


def test_live_miner_resolution_takes_precedence_over_certification(
    monkeypatch,
) -> None:
    state = _certification_state()
    state.standalone_telemetry_path = ""
    state.standalone_runtime_manifest_path = ""
    unit = "reliquary-miner-pro-math@mine.service"
    env_file = "/etc/mine.env"

    def resolved(box: fleet.BoxState):
        box.active_unit = unit
        box.active_lane = "mine"
        box.active_units = [unit]
        box.active_lanes = ["mine"]
        box.active_pid = 4242
        return unit, env_file

    monkeypatch.setattr(fleet, "_resolve_allowed_active_unit", resolved)
    monkeypatch.setattr(
        fleet,
        "_collect_standalone_certification_after_unit_failure",
        lambda _box: pytest.fail("certifier must not probe over live miner"),
    )
    monkeypatch.setattr(
        fleet,
        "ssh_run",
        lambda *_args, **_kwargs: (
            0,
            "===GPU===\n40000, 183359, 99\n"
            "===PROC===\n1\n"
            "===PID===\n4242\n"
            "===ENVFILE===\nok\n"
            "===MINERENV===\n"
            "RELIQUARY_ENVIRONMENT_NAME=openmathinstruct\n"
            "RELIQUARY_ENGINE_MODE=reference\n"
            "===NRESTARTS===\n0\n",
            "",
        ),
    )

    fleet._collect_box_in_place(state)

    assert state.proc_alive is True
    assert state.active_unit == unit
    assert state.active_pid == 4242
    assert state.standalone_certification == {}


def test_certification_renders_real_gpu_but_no_mining_funnel(
    monkeypatch,
) -> None:
    state = _certification_state()
    state.standalone_telemetry = {"funnel": {"attempts": 77}}
    assert fleet._apply_standalone_certification_probe(
        state,
        _certification_probe(),
        now=1000.0,
    )
    monkeypatch.setattr(fleet_web, "_boxes", [state])
    monkeypatch.setattr(fleet_web, "_vs", fleet.ValidatorState())
    monkeypatch.setattr(fleet_web, "_chain", fleet.ChainState())

    fleet_html = fleet_web.render_fleet_html()
    pipeline_html = fleet_web.render_pipeline_html()
    truth_html = fleet_web.render_standalone_mining_truth_html()

    assert "certifying (non-mining)" in fleet_html
    assert "CERTIFYING" in fleet_html
    assert ">100%</td>" in fleet_html
    assert ">certifying</td>" in pipeline_html
    assert "certifier" in pipeline_html
    assert "certifying · reference_proof" in truth_html
    assert "no mining unit" in truth_html
    assert ">77<" not in truth_html
