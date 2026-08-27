from __future__ import annotations

import json

import fleet
import fleet_web


def _box() -> fleet.BoxState:
    return fleet.BoxState(
        alias="operator@example",
        hotkey="5" + "A" * 47,
        label="code",
        color="magenta",
        unit="reliquary-miner-pro-code-mine.service",
        unit_candidates=(("reliquary-miner-pro-code-mine.service", ""),),
        standalone_telemetry_path="/var/lib/reliquary-code/dashboard.json",
    )


def _service(unit: str, role: str, *, pid: int = 4200) -> dict[str, object]:
    return {
        "unit": unit,
        "active_state": "active",
        "sub_state": "running",
        "pid": pid,
        "restarts": 0,
        "invocation_id": "1" * 32,
        "service_user": "reliquary-code-worker",
        "role": role,
        "submit_disabled": False,
        "exec_start_sha256": "a" * 64,
        "fragment_sha256": "b" * 64,
    }


def test_wallet_free_generator_is_discovered_but_never_called_mining(
    monkeypatch,
) -> None:
    box = _box()
    generator = _service(
        "reliquary-code-v3-cp62-generator.service",
        "wallet_free_generator",
    )

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        assert "reliquary*.service" in command
        return 0, json.dumps({
            "active": None,
            "active_units": [],
            "coordinated_unit_statuses": [],
            "candidates": [],
            "service_inventory": [generator],
            "unexpected": [],
            "error": "no_allowed_unit_active",
        }), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)

    assert fleet._resolve_allowed_active_unit(box) is None
    status = fleet_web._box_lane_status(box)
    assert status["status"] == "READY_UNATTACHED"
    assert status["funnel_authoritative"] is False
    assert status["funnel"] == {}
    assert status["active_generator_units"] == [generator["unit"]]


def test_unconfigured_dynamic_controller_is_explicitly_blocked() -> None:
    box = _box()
    controller = _service(
        "reliquary-code-v3-cp62-canary.service",
        "mining_controller",
    )
    box.service_inventory = [controller]
    box.unexpected_active_units = [str(controller["unit"])]
    box.unit_resolution_error = "unexpected_active_units:" + str(
        controller["unit"]
    )

    status = fleet_web._box_lane_status(box)

    assert status["status"] == "BLOCKED"
    assert status["discovered_controller_units"] == [controller["unit"]]
    assert status["funnel_authoritative"] is False


def test_submit_disabled_dynamic_benchmark_is_certifying_not_blocked() -> None:
    box = _box()
    benchmark = _service(
        "reliquary-code-rtx-cp62-submit-disabled-benchmark.service",
        "certification",
    )
    box.service_inventory = [benchmark]
    box.unit_resolution_error = "no_allowed_unit_active"

    status = fleet_web._box_lane_status(box)

    assert status["status"] == "CERTIFYING"
    assert status["funnel_authoritative"] is False
    assert status["funnel"] == {}


def test_completed_remain_after_exit_certificate_is_not_current_work() -> None:
    box = _box()
    completed = _service(
        "reliquary-code-certify-cutover.service",
        "certification",
        pid=0,
    )
    completed["sub_state"] = "exited"
    generator = _service(
        "reliquary-code-v3-cp66-generator.service",
        "wallet_free_generator",
    )
    box.service_inventory = [completed, generator]
    box.unit_resolution_error = "no_allowed_unit_active"

    status = fleet_web._box_lane_status(box)

    assert status["status"] == "READY_UNATTACHED"
    assert status["funnel_authoritative"] is False


def test_miner_named_support_units_are_not_misclassified_as_controllers() -> None:
    """The ``-miner`` prefix must not accidentally satisfy ``-mine``."""
    assert (
        fleet._classify_dynamic_service_role(
            "reliquary-miner-pro-code-grader.service",
            "/usr/bin/python -m reliquary.grader",
        )
        == "support"
    )
    assert (
        fleet._classify_dynamic_service_role(
            "reliquary-miner-pro-math-r2-tunnel.service",
            "/usr/bin/ssh -N -L 28080:127.0.0.1:8080",
        )
        == "support"
    )
    assert (
        fleet._classify_dynamic_service_role(
            "reliquary-miner-pro-code-mine.service",
            "/opt/reliquary/bin/controller",
        )
        == "mining_controller"
    )


def test_only_process_bound_current_funnel_is_mining() -> None:
    box = _box()
    box.proc_alive = True
    box.runtime_parity_ok = True
    box.standalone_fresh = True
    box.standalone_telemetry = {
        "metrics_posture": "active_profile",
        "funnel": {
            "attempts": 4,
            "generation_complete": 4,
            "natural_complete_m8": 4,
            "local_eligible": 2,
            "precommit_accepted": 2,
            "reveal_accepted": 2,
            "pool_accepted": 2,
            "selected": 1,
            "rewarded": 1,
            "terminal_final": 2,
            "terminal_unresolved": 0,
        },
    }

    status = fleet_web._box_lane_status(box)

    assert status["status"] == "MINING"
    assert status["funnel_authoritative"] is True
    assert status["funnel_scope"] == "active_profile"
    assert status["funnel"]["pool_accepted"] == 2
    assert status["funnel"]["rewarded"] == 1


def test_process_bound_canary_is_not_reported_as_unrestricted_mining() -> None:
    box = _box()
    box.active_unit = "reliquary-code-v3-cp66-canary.service"
    box.proc_alive = True
    box.runtime_parity_ok = True
    box.standalone_fresh = True
    box.standalone_telemetry = {
        "metrics_posture": "active_profile",
        "funnel": {"attempts": 2, "pool_accepted": 0},
        "window_lifecycle": {"status": "ACTIVE"},
    }

    status = fleet_web._box_lane_status(box)

    assert status["status"] == "CANARY"
    assert "quota-one" in status["reason"]
    assert status["funnel_authoritative"] is True


def test_standalone_controller_is_fenced_on_validator_checkpoint_drift() -> None:
    box = _box()
    box.active_unit = "reliquary-code-v3-cp68-canary.service"
    box.proc_alive = True
    box.runtime_parity_ok = True
    box.standalone_fresh = True
    box.standalone_telemetry = {
        "metrics_posture": "active_profile",
        "funnel": {"attempts": 8, "pool_accepted": 1},
    }
    box.source_manifest_provisioned_ok = True
    box.provisioned_model_kind = "validator_checkpoint"
    box.provisioned_checkpoint_n = 68
    box.provisioned_model_repo = "ReliquaryForge/qwen3.5-4b-reliquary-v4"
    box.provisioned_model_revision = "a" * 40
    box.observed_validator_image_revision = "c" * 40
    validator = fleet.ValidatorState(
        checkpoint_n=69,
        checkpoint_repo_id="ReliquaryForge/qwen3.5-4b-reliquary-v4",
        checkpoint_revision="b" * 40,
        image_revision="c" * 40,
    )

    status = fleet_web._box_lane_status(box, validator_state=validator)

    assert status["status"] == "FENCED"
    assert status["reason"] == (
        "checkpoint_mismatch:active=aaaaaaaaaaaa:validator=bbbbbbbbbbbb"
    )
    assert status["funnel_authoritative"] is False
    assert status["funnel"] == {}


def test_attested_fence_registry_is_fenced_without_a_controller_pid() -> None:
    box = _box()
    box.unit_resolution_error = "no_allowed_unit_active"
    box.active_unit_registry = {
        "schema_version": 1,
        "error": "",
        "active": {
            "mode": "fenced",
            "unit": "reliquary-math-v3-cp69-fenced.service",
            "checkpoint_revision": "a" * 40,
        },
    }
    validator = fleet.ValidatorState(
        checkpoint_n=69,
        checkpoint_repo_id="ReliquaryForge/qwen3.5-4b-reliquary-v4",
        checkpoint_revision="b" * 40,
    )

    status = fleet_web._box_lane_status(box, validator_state=validator)

    assert status["status"] == "FENCED"
    assert status["reason"] == (
        "attested submission fence is active "
        "(active checkpoint aaaaaaaaaaaa, validator checkpoint bbbbbbbbbbbb)"
    )
    assert status["funnel_authoritative"] is False
    assert status["funnel"] == {}


def test_attested_certifying_registry_is_not_reported_as_idle() -> None:
    box = _box()
    box.unit_resolution_error = "no_allowed_unit_active"
    box.service_inventory = [
        _service(
            "reliquary-code-v3-cp69-certifying.service",
            "wallet_free_generator",
        )
    ]
    box.active_unit_registry = {
        "schema_version": 1,
        "error": "",
        "active": {
            "mode": "certifying",
            "unit": "reliquary-code-v3-cp69-certifying.service",
            "checkpoint_revision": "a" * 40,
        },
    }

    status = fleet_web._box_lane_status(box)

    assert status["status"] == "CERTIFYING"
    assert status["reason"] == (
        "attested submit-disabled certification is active; "
        "no mining authority"
    )
    assert status["funnel_authoritative"] is False
    assert status["funnel"] == {}


def test_attested_certifying_registry_without_live_service_is_stalled() -> None:
    box = _box()
    box.unit_resolution_error = "no_allowed_unit_active"
    box.active_unit_registry = {
        "schema_version": 1,
        "error": "",
        "active": {
            "mode": "certifying",
            "unit": "reliquary-code-v3-cp69-certifying.service",
            "checkpoint_revision": "a" * 40,
        },
    }

    status = fleet_web._box_lane_status(box)

    assert status["status"] == "STALLED"
    assert status["reason"] == "attested certification has no active service"
    assert status["funnel_authoritative"] is False


def test_attested_certifying_registry_with_stale_checkpoint_is_fenced() -> None:
    box = _box()
    box.service_inventory = [
        _service(
            "reliquary-code-v3-cp68-certifying.service",
            "wallet_free_generator",
        )
    ]
    box.active_unit_registry = {
        "schema_version": 1,
        "error": "",
        "active": {
            "mode": "certifying",
            "unit": "reliquary-code-v3-cp68-certifying.service",
            "checkpoint_revision": "a" * 40,
        },
    }
    validator = fleet.ValidatorState(
        checkpoint_n=69,
        checkpoint_repo_id="ReliquaryForge/qwen3.5-4b-reliquary-v4",
        checkpoint_revision="b" * 40,
    )

    status = fleet_web._box_lane_status(box, validator_state=validator)

    assert status["status"] == "FENCED"
    assert status["reason"] == (
        "certification_checkpoint_mismatch:"
        "active=aaaaaaaaaaaa:validator=bbbbbbbbbbbb"
    )
    assert status["funnel_authoritative"] is False


def test_attested_canary_registry_with_stale_checkpoint_is_fenced_without_pid() -> None:
    box = _box()
    box.unit_resolution_error = "no_allowed_unit_active"
    box.service_inventory = [
        _service(
            "reliquary-code-v3-cp70-generator.service",
            "wallet_free_generator",
        )
    ]
    box.active_unit_registry = {
        "schema_version": 1,
        "error": "",
        "active": {
            "mode": "canary",
            "unit": "reliquary-code-v3-cp70-canary.service",
            "checkpoint_revision": "a" * 40,
        },
    }
    validator = fleet.ValidatorState(
        checkpoint_n=71,
        checkpoint_repo_id="ReliquaryForge/qwen3.5-4b-reliquary-v4",
        checkpoint_revision="b" * 40,
    )

    status = fleet_web._box_lane_status(box, validator_state=validator)

    assert status["status"] == "FENCED"
    assert status["reason"] == (
        "registry_checkpoint_mismatch:"
        "active=aaaaaaaaaaaa:validator=bbbbbbbbbbbb"
    )
    assert status["funnel_authoritative"] is False
    assert status["funnel"] == {}


def test_attested_current_canary_registry_without_controller_is_stalled() -> None:
    box = _box()
    box.unit_resolution_error = "no_allowed_unit_active"
    box.service_inventory = [
        _service(
            "reliquary-code-v3-cp71-generator.service",
            "wallet_free_generator",
        )
    ]
    checkpoint_revision = "b" * 40
    box.active_unit_registry = {
        "schema_version": 1,
        "error": "",
        "active": {
            "mode": "canary",
            "unit": "reliquary-code-v3-cp71-canary.service",
            "checkpoint_revision": checkpoint_revision,
        },
    }
    validator = fleet.ValidatorState(
        checkpoint_n=71,
        checkpoint_repo_id="ReliquaryForge/qwen3.5-4b-reliquary-v4",
        checkpoint_revision=checkpoint_revision,
    )

    status = fleet_web._box_lane_status(box, validator_state=validator)

    assert status["status"] == "STALLED"
    assert status["reason"] == "attested canary controller is not active"
    assert status["funnel_authoritative"] is False
    assert status["funnel"] == {}


def test_invalid_active_registry_never_falls_back_to_stale_lane_state() -> None:
    box = _box()
    box.active_unit_registry_path = "/var/lib/miner/active-unit-registry.json"
    box.active_unit_registry = {
        "path": box.active_unit_registry_path,
        "error": "runtime manifest digest mismatch",
    }
    box.proc_alive = True
    box.runtime_parity_ok = True
    box.standalone_fresh = True
    box.standalone_telemetry = {
        "metrics_posture": "active_profile",
        "funnel": {"attempts": 8, "pool_accepted": 4},
    }

    status = fleet_web._box_lane_status(box)

    assert status["status"] == "BLOCKED"
    assert status["reason"] == "active_unit_registry_invalid"
    assert status["funnel_authoritative"] is False
    assert status["funnel"] == {}


def test_current_failed_window_is_recovering_not_mining() -> None:
    box = _box()
    box.proc_alive = True
    box.runtime_parity_ok = True
    box.standalone_fresh = True
    box.standalone_telemetry = {
        "metrics_posture": "active_profile",
        "funnel": {"attempts": 8, "pool_accepted": 0},
        "window_lifecycle": {
            "status": "RECOVERING",
            "latest_run": {
                "window_n": 27359,
                "status": "failed",
                "failure_stage": "window_pipeline",
                "failure_type": "ControllerError",
            },
        },
    }

    status = fleet_web._box_lane_status(box)

    assert status["status"] == "RECOVERING"
    assert status["funnel_authoritative"] is True
    assert "window_pipeline:ControllerError" in status["reason"]
