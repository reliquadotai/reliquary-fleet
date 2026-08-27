import hashlib
import json
import subprocess
import sys

import fleet
import fleet_web
import settings

HOTKEY = "5" + "A" * 47
PROFILE = "a" * 64
MANIFEST = "b" * 64
VALIDATOR_SOURCE = "c" * 40
MINER_SOURCE = "d" * 40
CHECKPOINT = "e" * 40
GPU_UUID = "11111111-2222-3333-4444-555555555555"


def _component() -> fleet.ComponentState:
    return fleet.ComponentState(
        alias="worker@example",
        label="rtx-code-generation",
        color="magenta",
        role="generation",
        controller_label="h100-code-live",
        generator_unit="generator.service",
        tunnel_unit="tunnel.service",
        runtime_manifest_path="/var/lib/component/runtime.json",
        runtime_profile_path="/var/lib/component/profile.json",
        component_config_path="/etc/component/generator.toml",
        evidence_alias="controller@example",
        evidence_dir="/var/lib/component/evidence",
        evidence_runtime_manifest_path="/var/lib/component/runtime.json",
        evidence_runtime_profile_path="/var/lib/component/profile.json",
        evidence_target_windows=20,
        evidence_stale_seconds=300.0,
        telemetry_stale_seconds=180.0,
    )


def _controller(*, attached: bool) -> fleet.BoxState:
    state = fleet.BoxState(
        alias="controller@example",
        hotkey=HOTKEY,
        label="h100-code-live",
        color="magenta",
        unit="controller.service",
    )
    if attached:
        state.runtime_components = [
            {
                "role": "generation",
                "component_id": GPU_UUID,
                "manifest_file_sha256": MANIFEST,
                "runtime_payload_sha256": "f" * 64,
                "runtime_profile_sha256": PROFILE,
                "health_gpu_uuid_required": True,
            }
        ]
    return state


def _live_payload() -> dict:
    return {
        "schema_version": 1,
        "generator_active_state": "active",
        "generator_sub_state": "running",
        "generator_enablement": "enabled",
        "generator_pid": 101,
        "generator_restarts": 0,
        "tunnel_active_state": "active",
        "tunnel_sub_state": "running",
        "tunnel_enablement": "enabled",
        "tunnel_pid": 102,
        "tunnel_restarts": 0,
        "gpu_uuid": f"GPU-{GPU_UUID}",
        "gpu_name": "NVIDIA RTX PRO 6000 Blackwell",
        "compute_capability": "12.0",
        "gpu_mem_mb": 80_000,
        "gpu_total_mb": 96_000,
        "gpu_util": 92,
        "gpu_power_w": 390.0,
        "gpu_power_limit_w": 600.0,
        "component_role": "generation",
        "manifest_schema_version": 5,
        "manifest_sha256": MANIFEST,
        "manifest_profile_sha256": "1" * 64,
        "miner_source_revision": MINER_SOURCE,
        "validator_source_revision": VALIDATOR_SOURCE,
        "checkpoint_n": 53,
        "checkpoint_revision": CHECKPOINT,
        "model_repo": "ReliquaryForge/model",
        "environment": "opencodeinstruct",
        "runtime_profile_sha256": PROFILE,
        "runtime_regime": "2" * 64,
        "profile_file_sha256": "3" * 64,
        "profile_source_revision": VALIDATOR_SOURCE,
        "profile_checkpoint_revision": CHECKPOINT,
        "profile_gpu_uuid": "1234",
        "identity_ok": True,
        "wallet_absent_attested": True,
        "submit_authority": False,
        "evidence_valid_windows": 14,
        "evidence_complete_groups": 168,
        "evidence_first_window": 27000,
        "evidence_last_window": 27013,
        "evidence_latest_at": 990.0,
        "evidence_error": "",
        "error": "",
    }


def test_component_probe_and_controller_accounting_are_separate(
    monkeypatch,
) -> None:
    component = _component()

    def probe(_alias, command, **_kwargs):
        if "PYCOMPONENTEVIDENCE" in command:
            payload = {
                "schema_version": 1,
                "source_ok": True,
                "error": "",
                "manifest_sha256": MANIFEST,
                "profile_sha256": "3" * 64,
                "valid_windows": 14,
                "complete_groups": 168,
                "first_window": 27000,
                "last_window": 27013,
                "latest_completion_at": 990.0,
            }
        else:
            payload = _live_payload()
        return 0, json.dumps(payload, separators=(",", ":")), ""

    monkeypatch.setattr(
        fleet,
        "ssh_run",
        probe,
    )
    monkeypatch.setattr(fleet.time, "time", lambda: 1000.0)

    observed = fleet.collect_component_snapshot(component)
    controller = _controller(attached=True)
    payload = fleet_web._component_payload(
        observed,
        boxes=[controller],
        chain_entries={
            HOTKEY: fleet.HotkeyChainEntry(
                label=controller.label,
                hotkey=HOTKEY,
                uid=85,
                stake=0.0,
                emission=0.0,
            )
        },
        now=1000.0,
    )

    assert payload["status"] == "live_attached"
    assert payload["controller_binding"]["attached"] is True
    assert payload["authority"] == {
        "wallet_absent_attested": True,
        "submit_authority": False,
        "owns_hotkey": False,
    }
    assert payload["accounting"]["hotkey"] == HOTKEY
    assert payload["accounting"]["uid"] == 85
    assert payload["accounting"]["shared_controller_funnel"] is True
    assert payload["accounting"]["component_attempts"] is None
    assert payload["accounting"]["component_selected_slots"] is None
    assert payload["accounting"]["component_rewarded_slots"] is None
    assert payload["gpu"]["power_w"] == 390.0
    assert payload["evidence"]["source_ok"] is True
    assert payload["evidence"]["fresh"] is True


def test_attached_component_reports_generation_only_for_active_canary() -> None:
    component = _component()
    for key, value in _live_payload().items():
        if hasattr(component, key):
            setattr(component, key, value)
    component.last_poll_s = 995.0
    component.evidence_source_ok = True
    component.evidence_fresh = True
    controller = _controller(attached=True)
    controller.standalone_controller_mode = "canary"
    controller.standalone_telemetry = {
        "per_gpu": {
            f"GPU-{GPU_UUID}": {
                "aliases": [GPU_UUID, f"GPU-{GPU_UUID}"],
                "attempts": 15,
                "natural_complete_m8": 15,
                "generated_windows": 4,
                "natural_complete_windows": 4,
                "first_window": 26361,
                "last_window": 26364,
                "physical_gpu_hours": 0.08,
            },
        },
    }

    payload = fleet_web._component_payload(
        component,
        boxes=[controller],
        chain_entries={},
        now=1000.0,
    )

    assert payload["status"] == "attached_canary"
    assert payload["accounting"]["generated_attempts"] == 15
    assert payload["accounting"]["natural_complete_m8"] == 15
    assert payload["accounting"]["generated_windows"] == 4
    assert payload["accounting"]["component_selected_slots"] is None
    assert payload["accounting"]["component_rewarded_slots"] is None

    controller.standalone_controller_mode = "mine"
    payload = fleet_web._component_payload(
        component,
        boxes=[controller],
        chain_entries={},
        now=1000.0,
    )
    assert payload["status"] == "attached_mine"


def test_controller_manifest_worker_job_projects_activity_without_funnel() -> None:
    component = _component()
    for key, value in _live_payload().items():
        if hasattr(component, key):
            setattr(component, key, value)
    component.last_poll_s = 995.0
    component.evidence_source_ok = True
    component.evidence_fresh = True
    component.resolution_source = "controller_manifest"
    component.worker_state = "active"
    component.worker_progress_fresh = True
    component.active_job_id = "7" * 64
    component.active_window_n = 27440

    controller = _controller(attached=True)
    controller.proc_alive = True
    controller.standalone_fresh = True
    controller.runtime_parity_ok = True
    controller.miner_state = "live"
    controller.miner_window = 27439
    controller.miner_ready = 1
    controller.miner_inflight = 0
    controller.final_accept_60m = 3
    controller.final_reject_60m = 4

    fleet_web._apply_component_active_generation(
        [controller],
        [component],
        fleet.ValidatorState(window=27440),
    )

    assert controller.miner_state == "active_generation"
    assert controller.miner_window == 27440
    assert controller.miner_ready == 0
    assert controller.miner_inflight == 1
    assert controller.final_accept_60m == 3
    assert controller.final_reject_60m == 4


def test_worker_job_does_not_project_across_window_or_manifest() -> None:
    component = _component()
    for key, value in _live_payload().items():
        if hasattr(component, key):
            setattr(component, key, value)
    component.resolution_source = "controller_manifest"
    component.worker_state = "active"
    component.worker_progress_fresh = True
    component.active_job_id = "7" * 64
    component.active_window_n = 27439
    controller = _controller(attached=True)
    controller.proc_alive = True
    controller.standalone_fresh = True
    controller.runtime_parity_ok = True
    controller.miner_state = "live"

    fleet_web._apply_component_active_generation(
        [controller],
        [component],
        fleet.ValidatorState(window=27440),
    )
    assert controller.miner_state == "live"

    component.active_window_n = 27440
    component.manifest_sha256 = "9" * 64
    fleet_web._apply_component_active_generation(
        [controller],
        [component],
        fleet.ValidatorState(window=27440),
    )
    assert controller.miner_state == "live"


def test_dynamic_expectation_comes_only_from_live_exact_controller() -> None:
    component = _component()
    controller = _controller(attached=True)
    controller.runtime_components[0]["manifest_path"] = (
        "/var/lib/controller/runtime-manifest-rtx-generation.json"
    )
    controller.proc_alive = True
    controller.standalone_fresh = True
    controller.runtime_parity_ok = True
    controller.standalone_controller_mode = "canary"
    controller.miner_source_revision = MINER_SOURCE
    controller.reliquary_source_revision = VALIDATOR_SOURCE
    controller.runtime_checkpoint_revision = CHECKPOINT

    expected = fleet_web._controller_component_expectation(
        component, [controller]
    )

    assert expected is not None
    assert expected["manifest_file_sha256"] == MANIFEST
    assert expected["runtime_profile_sha256"] == PROFILE
    assert expected["checkpoint_revision"] == CHECKPOINT
    controller.standalone_fresh = False
    assert (
        fleet_web._controller_component_expectation(component, [controller])
        is None
    )

def test_healthy_unbound_component_is_certifying_not_live() -> None:
    component = _component()
    for key, value in _live_payload().items():
        if hasattr(component, key):
            setattr(component, key, value)
    component.last_poll_s = 995.0
    component.evidence_source_ok = True
    component.evidence_fresh = True

    payload = fleet_web._component_payload(
        component,
        boxes=[_controller(attached=False)],
        chain_entries={},
        now=1000.0,
    )

    assert payload["status"] == "certifying"
    assert payload["ok"] is True
    assert payload["controller_binding"] == {
        "attached": False,
        "reason": "controller_manifest_unbound",
    }


def test_component_config_requires_existing_controller_and_no_hotkey(
    tmp_path,
) -> None:
    row = {
        "alias": "worker@example",
        "label": "rtx-code-generation",
        "role": "generation",
        "controller_label": "h100-code-live",
        "generator_unit": "generator.service",
        "tunnel_unit": "tunnel.service",
        "runtime_manifest_path": "/var/lib/component/runtime.json",
        "runtime_profile_path": "/var/lib/component/profile.json",
        "component_config_path": "/etc/component/generator.toml",
        "evidence_alias": "controller@example",
        "evidence_dir": "/var/lib/component/evidence",
        "evidence_runtime_manifest_path": "/var/lib/component/runtime.json",
        "evidence_runtime_profile_path": "/var/lib/component/profile.json",
    }
    observed = settings._coerce_component(row)
    assert observed.controller_label == "h100-code-live"
    assert observed.evidence_target_windows == 20
    assert observed.evidence_alias == "controller@example"

    try:
        settings._coerce_component({**row, "hotkey": HOTKEY})
    except ValueError as exc:
        assert "must not define a hotkey" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("component hotkey was accepted")

    config = tmp_path / "config.yaml"
    config.write_text(
        "\n".join(
            [
                "validator:",
                "  url: http://validator.example:8080",
                "fleet: []",
                "labs: []",
                "components:",
                "  - alias: worker@example",
                "    label: rtx-code-generation",
                "    role: generation",
                "    controller_label: missing-controller",
                "    generator_unit: generator.service",
                "    tunnel_unit: tunnel.service",
                "    runtime_manifest_path: /var/lib/component/runtime.json",
                "    runtime_profile_path: /var/lib/component/profile.json",
                "    component_config_path: /etc/component/generator.toml",
                "    evidence_alias: controller@example",
                "    evidence_dir: /var/lib/component/evidence",
                "    evidence_runtime_manifest_path: /var/lib/component/runtime.json",
                "    evidence_runtime_profile_path: /var/lib/component/profile.json",
            ]
        )
    )
    try:
        settings.load(config)
    except ValueError as exc:
        assert "controller_label" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("missing component controller was accepted")


def test_incomplete_stale_component_evidence_fails_closed() -> None:
    component = _component()
    for key, value in _live_payload().items():
        if hasattr(component, key):
            setattr(component, key, value)
    component.last_poll_s = 995.0
    component.evidence_source_ok = True
    component.evidence_fresh = False
    component.evidence_error = "stale_incomplete_certification"

    payload = fleet_web._component_payload(
        component,
        boxes=[_controller(attached=False)],
        chain_entries={},
        now=1000.0,
    )

    assert payload["status"] == "component_failed"
    assert payload["ok"] is False
    assert payload["evidence"]["fresh"] is False


def test_complete_frozen_evidence_does_not_expire_but_live_health_does(
    monkeypatch,
) -> None:
    component = _component()

    def probe(_alias, command, **_kwargs):
        if "PYCOMPONENTEVIDENCE" in command:
            payload = {
                "schema_version": 1,
                "source_ok": True,
                "error": "",
                "manifest_sha256": MANIFEST,
                "profile_sha256": "3" * 64,
                "valid_windows": 20,
                "complete_groups": 186,
                "first_window": 27000,
                "last_window": 27019,
                # The frozen evidence is deliberately much older than the
                # in-progress freshness threshold.
                "latest_completion_at": 1.0,
            }
        else:
            payload = _live_payload()
        return 0, json.dumps(payload, separators=(",", ":")), ""

    monkeypatch.setattr(fleet, "ssh_run", probe)
    monkeypatch.setattr(fleet.time, "time", lambda: 1000.0)

    observed = fleet.collect_component_snapshot(component)
    observed.last_poll_s = 1000.0
    healthy = fleet_web._component_payload(
        observed,
        boxes=[_controller(attached=False)],
        chain_entries={},
        now=1000.0,
    )

    assert observed.evidence_source_ok is True
    assert observed.evidence_fresh is True
    assert observed.evidence_error == ""
    assert healthy["evidence"]["complete"] is True
    assert healthy["evidence"]["age_s"] == 999.0
    assert healthy["status"] == "ready_unattached"
    assert healthy["ok"] is True

    # Frozen evidence remains valid, but it cannot mask a failed current
    # worker service or runtime identity.
    observed.generator_active_state = "inactive"
    unhealthy = fleet_web._component_payload(
        observed,
        boxes=[_controller(attached=False)],
        chain_entries={},
        now=1000.0,
    )
    assert unhealthy["evidence"]["fresh"] is True
    assert unhealthy["status"] == "component_failed"
    assert unhealthy["ok"] is False

    observed.generator_active_state = "active"
    observed.identity_ok = False
    wrong_runtime = fleet_web._component_payload(
        observed,
        boxes=[_controller(attached=False)],
        chain_entries={},
        now=1000.0,
    )
    assert wrong_runtime["evidence"]["fresh"] is True
    assert wrong_runtime["status"] == "component_failed"
    assert wrong_runtime["ok"] is False


def test_current_split_gpu_capture_binds_component_through_controller_manifest(
    tmp_path,
) -> None:
    root = tmp_path / "bundle"
    evidence = root / "live-capture"
    evidence.mkdir(parents=True)
    manifest_path = root / "runtime-manifest-rtx-generation.json"
    profile_path = root / "generation-profile-rtx.json"
    controller_path = root / "runtime-manifest-controller.json"
    output_path = evidence / "generation-capture.json"
    audit_path = evidence / "generation-capture.audit.json"

    def write_json(path, payload) -> str:
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        path.write_bytes(raw)
        return hashlib.sha256(raw).hexdigest()

    manifest_sha = write_json(
        manifest_path,
        {
            "schema_version": 5,
            "component_role": "generation",
            "miner_source_revision": MINER_SOURCE,
            "identity": {
                "validator_source_revision": VALIDATOR_SOURCE,
                "checkpoint_revision": CHECKPOINT,
            },
        },
    )
    profile_sha = write_json(
        profile_path,
        {
            "schema_version": 1,
            "role": "generation",
            "source_revision": VALIDATOR_SOURCE,
            "checkpoint_revision": CHECKPOINT,
            "gpu_uuid": GPU_UUID,
        },
    )
    controller_sha = write_json(
        controller_path,
        {
            "schema_version": 5,
            "miner_source_revision": MINER_SOURCE,
            "identity": {
                "validator_source_revision": VALIDATOR_SOURCE,
                "checkpoint_revision": CHECKPOINT,
            },
            "components": [
                {
                    "component_id": GPU_UUID,
                    "role": "generation",
                    "manifest_path": str(manifest_path),
                    "manifest_file_sha256": manifest_sha,
                    "runtime_profile_sha256": PROFILE,
                }
            ],
        },
    )
    output_sha = write_json(
        output_path,
        {
            "schema_version": 2,
            "observed_window_n": 27427,
            "generation_trials": [
                {
                    "group": {
                        "prompt_idx": 123,
                        "rollouts": [{} for _ in range(8)],
                    }
                }
            ],
        },
    )
    write_json(
        audit_path,
        {
            "schema_version": 1,
            "submit_disabled": True,
            "wallet_free": True,
            "details": {"generation_trials": 1},
            "inputs": {
                str(profile_path): profile_sha,
                str(controller_path): controller_sha,
            },
            "output": str(output_path),
            "output_sha256": output_sha,
        },
    )
    config = {
        "role": "generation",
        "evidence_dir": str(evidence),
        "runtime_manifest_path": str(manifest_path),
        "runtime_profile_path": str(profile_path),
        "expected_manifest_sha256": manifest_sha,
        "expected_miner_source_revision": MINER_SOURCE,
        "expected_validator_source_revision": VALIDATOR_SOURCE,
        "expected_checkpoint_revision": CHECKPOINT,
        "expected_gpu_uuid": GPU_UUID,
        "expected_runtime_profile_sha256": PROFILE,
    }

    observed = subprocess.run(
        [
            sys.executable,
            "-c",
            fleet._COMPONENT_EVIDENCE_PROBE_SOURCE,
            json.dumps(config, separators=(",", ":")),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(observed.stdout)

    assert payload["source_ok"] is True
    assert payload["error"] == ""
    assert payload["valid_windows"] == 1
    assert payload["complete_groups"] == 1
    assert payload["first_window"] == 27427
    assert payload["last_window"] == 27427


def test_active_certificate_selects_exact_retry_capture_directory(tmp_path) -> None:
    root = tmp_path / "bundle"
    stale = root / "live-capture"
    selected = root / "live-capture-r2"
    stale.mkdir(parents=True)
    selected.mkdir()
    manifest_path = root / "runtime-manifest-rtx-generation.json"
    profile_path = root / "generation-profile-rtx.json"
    controller_path = root / "runtime-manifest-controller.bound.json"
    certificate_path = root / "code-runtime.checkpoint-refresh.json"

    def write_json(path, payload) -> str:
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        path.write_bytes(raw)
        path.chmod(0o444)
        return hashlib.sha256(raw).hexdigest()

    manifest_sha = write_json(
        manifest_path,
        {
            "schema_version": 5,
            "component_role": "generation",
            "miner_source_revision": MINER_SOURCE,
            "identity": {
                "validator_source_revision": VALIDATOR_SOURCE,
                "checkpoint_revision": CHECKPOINT,
            },
        },
    )
    profile_sha = write_json(
        profile_path,
        {
            "schema_version": 1,
            "role": "generation",
            "source_revision": VALIDATOR_SOURCE,
            "checkpoint_revision": CHECKPOINT,
            "gpu_uuid": GPU_UUID,
        },
    )
    artifact_sha = "9" * 64
    certificate_sha = write_json(
        certificate_path,
        {
            "schema_version": 11,
            "runtime_binding": {
                "validator_source_revision": VALIDATOR_SOURCE,
                "checkpoint_revision": CHECKPOINT,
            },
            "checkpoint_refresh": {
                "evidence_sha256": artifact_sha,
                "groups": 1,
            },
        },
    )
    controller_sha = write_json(
        controller_path,
        {
            "schema_version": 5,
            "miner_source_revision": MINER_SOURCE,
            "identity": {
                "validator_source_revision": VALIDATOR_SOURCE,
                "checkpoint_revision": CHECKPOINT,
            },
            "certificate_path": str(certificate_path),
            "certificate_sha256": certificate_sha,
            "components": [
                {
                    "component_id": GPU_UUID,
                    "role": "generation",
                    "manifest_path": str(manifest_path),
                    "manifest_file_sha256": manifest_sha,
                    "runtime_profile_sha256": PROFILE,
                }
            ],
        },
    )
    capture_path = selected / "generation-capture.json"
    capture_sha = write_json(
        capture_path,
        {
            "schema_version": 2,
            "observed_window_n": 27447,
            "generation_trials": [
                {
                    "group": {
                        "prompt_idx": 123,
                        "rollouts": [{} for _ in range(8)],
                    }
                }
            ],
        },
    )
    write_json(
        selected / "generation-capture.audit.json",
        {
            "schema_version": 1,
            "submit_disabled": True,
            "wallet_free": True,
            "details": {"generation_trials": 1},
            "inputs": {
                str(profile_path): profile_sha,
                str(controller_path): controller_sha,
            },
            "output": str(capture_path),
            "output_sha256": capture_sha,
        },
    )
    evidence_path = selected / "code-runtime.checkpoint-refresh.json"
    evidence_sha = write_json(
        evidence_path,
        {
            "schema_version": 3,
            "runtime_binding": {
                "validator_source_revision": VALIDATOR_SOURCE,
                "checkpoint_revision": CHECKPOINT,
            },
            "checkpoint_refresh": {
                "evidence_sha256": artifact_sha,
                "groups": 1,
            },
        },
    )
    write_json(
        selected / "certification-evidence.audit.json",
        {
            "schema_version": 1,
            "submit_disabled": True,
            "wallet_free": True,
            "details": {"artifact_sha256": artifact_sha},
            "inputs": {},
            "output": str(evidence_path),
            "output_sha256": evidence_sha,
        },
    )
    # A stale retry directory is deliberately present and must not win by
    # conventional name or mtime.
    write_json(stale / "generation-stale.audit.json", {"schema_version": 1})

    config = {
        "role": "generation",
        "evidence_dir": str(root),
        "runtime_manifest_path": str(manifest_path),
        "runtime_profile_path": str(profile_path),
        "expected_manifest_sha256": manifest_sha,
        "expected_miner_source_revision": MINER_SOURCE,
        "expected_validator_source_revision": VALIDATOR_SOURCE,
        "expected_checkpoint_revision": CHECKPOINT,
        "expected_gpu_uuid": GPU_UUID,
        "expected_runtime_profile_sha256": PROFILE,
        "controller_manifest_path": str(controller_path),
    }
    observed = subprocess.run(
        [
            sys.executable,
            "-c",
            fleet._COMPONENT_EVIDENCE_PROBE_SOURCE,
            json.dumps(config, separators=(",", ":")),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(observed.stdout)

    assert payload["source_ok"] is True
    assert payload["certificate_bound"] is True
    assert payload["evidence_dir"] == str(selected)
    assert payload["valid_windows"] == 1
    assert payload["complete_groups"] == 1
