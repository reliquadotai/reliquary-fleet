from __future__ import annotations

import argparse
import copy
import hashlib
import json
import inspect
import math
import sqlite3
import subprocess
import sys
import threading
from io import BytesIO
from types import SimpleNamespace

import fleet
import pytest
import settings


OUR_HOTKEY = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
OTHER_HOTKEY = "5OtherValidatorCandidate1111111111111111111111111111"
RUNNER_HOTKEY = "5RewardedRunnerUp1111111111111111111111111111111"


def _box(**overrides) -> fleet.BoxState:
    values = {
        "alias": "ubuntu@192.0.2.10",
        "hotkey": OUR_HOTKEY,
        "label": "h100-reserve1",
        "color": "cyan",
        "unit": "reliquary-miner-pro",
        "env_file": "/srv/reliquary-miner-pro/state/miner-pro.env",
        "env_file_ok": True,
    }
    values.update(overrides)
    return fleet.BoxState(**values)


def _window(
    n: int,
    rewards: dict[str, float],
    batch: list[dict] | None = None,
    *,
    reward_data_present: bool = True,
) -> fleet.WindowSummary:
    batch = list(batch or [])
    return fleet.WindowSummary(
        n=n,
        ours=sum(1 for row in batch if row.get("hotkey") == OUR_HOTKEY),
        total_batch=len(batch),
        rt_first=0.0,
        rejects={},
        reward_ours=float(rewards.get(OUR_HOTKEY, 0.0)),
        reward_total=sum(rewards.values()),
        rewards_by_hotkey=dict(rewards),
        reward_data_present=reward_data_present,
        batch=batch,
    )


def _ready_code_context() -> tuple[fleet.BoxState, fleet.ValidatorState]:
    observed_at = fleet.time.time()
    public_source = "a" * 40
    miner_source = "b" * 40
    checkpoint_revision = "c" * 40
    runtime_profile = "d" * 64
    repository = "ReliquaryForge/qwen3.5-2b-reliquary-v3"
    unit = "reliquary-miner-pro@code-reserve1.service"
    ledger_path = "/srv/reliquary-miner-pro/state/code-auction.sqlite3"
    grader_socket = "/tmp/reliquary-grader.sock"
    probe = {
        "schema_version": 1,
        "miner_unit": unit,
        "miner_unit_enablement": "enabled",
        "process_pid": 4242,
        "invocation_id": "f" * 32,
        "settings_process_bound": True,
        "environment": "opencodeinstruct",
        "engine_mode": "reference",
        "lane": "code-reserve1",
        "prescreen_enabled": True,
        "auction_policy": "deadline_aware",
        "ledger_path": ledger_path,
        "runtime_attestation": {
            "schema_version": 1,
            "prescreen_enabled": True,
            "auction_policy": "deadline_aware",
            "ledger_path": ledger_path,
            "ledger_available": True,
            "ledger_partition_registered": True,
            "ledger_schema_version": 1,
            "grader_socket": grader_socket,
            "grader_source_revision": public_source,
            "process_pid": 4242,
            "miner_source_revision": miner_source,
            "public_source_revision": public_source,
            "runtime_profile_hash": runtime_profile,
        },
        "grader": {
            "unit": "reliquary-code-grader.service",
            "active_state": "active",
            "enablement": "enabled",
            "socket_path": grader_socket,
            "socket_ok": True,
            "socket_uid": 0,
            "socket_gid": 0,
            "socket_mode": "0660",
            "expected_socket_mode": "0660",
            "bundle_link": "/opt/reliquary-code-grader/current",
            "bundle_ok": True,
            "bundle_target": (
                "/opt/reliquary-code-grader/bundles/"
                f"{public_source}-20260719T000000Z-1"
            ),
            "bundle_source_revision": public_source,
            "metrics_ok": True,
            "canary_eval_ok_total": 1,
            "canary_case_passed_total": 1,
        },
        "ledger": {
            "configured": True,
            "readonly_ok": True,
            "schema_ok": True,
            "application_id": 0x5243414C,
            "user_version": 1,
            "quick_check": "ok",
            "journal_mode": "wal",
            "error": "",
            "partitions": [
                {
                    "model_repository": repository,
                    "checkpoint_revision": checkpoint_revision,
                    "checkpoint_n": 13,
                    "public_source_revision": public_source,
                    "runtime_profile_hash": runtime_profile,
                    "environment": "opencodeinstruct",
                    "registered_at": 1.0,
                    "miner_source_revision": miner_source,
                }
            ],
        },
    }
    box = _box(
        proc_alive=True,
        last_poll_s=observed_at - 1,
        active_unit=unit,
        active_lane="code-reserve1",
        active_environment="opencodeinstruct",
        active_pid=4242,
        miner_environment="opencodeinstruct",
        engine_mode="reference",
        protocol_profile="forced_seed_v2_auction_legacy_wire",
        runtime_parity_ok=True,
        reference_ready=True,
        miner_unit_enablement="enabled",
        runtime_profile_hash=runtime_profile,
        code_auction_probe=probe,
        miner_source_revision=miner_source,
        reliquary_source_revision=public_source,
        source_manifest_provisioned_ok=True,
        provisioned_model_kind="validator_checkpoint",
        provisioned_checkpoint_n=13,
        provisioned_model_repo=repository,
        provisioned_model_revision=checkpoint_revision,
    )
    validator = fleet.ValidatorState(
        state="open",
        window=23806,
        checkpoint_n=13,
        checkpoint_repo_id=repository,
        checkpoint_revision=checkpoint_revision,
        image_revision=public_source,
        runtime_fingerprint={"profile_hash": runtime_profile},
        health_raw={
            "image_revision": public_source,
            "runtime_fingerprint": {"profile_hash": runtime_profile},
        },
        last_fetch_at=observed_at - 1,
        health_last_fetch_at=observed_at - 1,
        health_status="ok",
        verdicts_last_fetch_at=observed_at - 1,
    )
    return box, validator


def _attach_generation_summary(
    box: fleet.BoxState,
    *,
    completed: int = 3,
    local_token_limit: int = 1,
    safe_deadline: int = 100,
) -> dict:
    partition = box.code_auction_probe["ledger"]["partitions"][0]
    summary = {
        key: partition[key]
        for key in (
            "model_repository",
            "checkpoint_revision",
            "checkpoint_n",
            "public_source_revision",
            "runtime_profile_hash",
            "environment",
        )
    }
    summary.update(
        {
            "lane": "code-reserve1",
            "first_window_n": 23816,
            "last_window_n": 23818,
            "last_observed_at": 123.0,
            "natural_eos_complete": completed,
            "local_token_limit": local_token_limit,
            "safe_deadline": safe_deadline,
        }
    )
    box.code_auction_probe["ledger"]["generation_outcomes"] = {
        "table_present": True,
        "schema_ok": True,
        "error": "",
        "summaries": [summary],
    }
    return summary


def _attach_terminal_funnel_summary(
    box: fleet.BoxState,
    *,
    attempts: int = 15,
    http_provisional: int = 11,
    pool_accepted: int = 3,
    selected: int = 1,
    rewarded: int = 1,
) -> dict:
    partition = box.code_auction_probe["ledger"]["partitions"][0]
    summary = {
        key: partition[key]
        for key in (
            "model_repository",
            "checkpoint_revision",
            "checkpoint_n",
            "public_source_revision",
            "runtime_profile_hash",
            "environment",
        )
    }
    summary.update(
        {
            "attempts": attempts,
            "http_provisional": http_provisional,
            "receipt_reserved": http_provisional,
            "reveal_sent": http_provisional,
            "pool_accepted": pool_accepted,
            "selected": selected,
            "rewarded": rewarded,
            "terminal_rejected": attempts - pool_accepted,
            "terminal_unresolved": 0,
        }
    )
    box.code_auction_probe["ledger"]["terminal_funnel"] = {
        "extension_table_present": True,
        "terminal_events_table_present": True,
        "schema_ok": True,
        "api_version": 2,
        "storage_mode": "v1_additive_compat",
        "summaries": [summary],
        "error": "",
    }
    return summary


def test_settings_loads_explicit_health_poll_stale_budget(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "SETTINGS", settings.Settings())
    config = tmp_path / "config.yaml"
    config.write_text(
        "dashboard:\n"
        "  refresh_seconds: 5\n"
        "  health_poll_stale_seconds: 75\n"
    )

    loaded = settings.load(config)

    assert loaded.refresh_seconds == 5
    assert loaded.health_poll_stale_seconds == 75


def test_settings_defaults_missing_health_poll_stale_budget(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "SETTINGS", settings.Settings())
    config = tmp_path / "config.yaml"
    config.write_text("dashboard:\n  refresh_seconds: 5\n")

    loaded = settings.load(config)

    assert loaded.refresh_seconds == 5.0
    assert loaded.health_poll_stale_seconds == 60.0


def test_settings_loads_and_propagates_non_secret_code_readiness_defaults(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(settings, "SETTINGS", settings.Settings())
    config = tmp_path / "config.yaml"
    config.write_text("dashboard:\n  refresh_seconds: 5\n")

    loaded = settings.load(config)

    assert loaded.code_grader_unit == "reliquary-code-grader.service"
    assert loaded.code_grader_socket == "/tmp/reliquary-grader.sock"
    assert loaded.code_grader_bundle_link == "/opt/reliquary-code-grader/current"
    assert loaded.code_grader_metrics_port == 9876
    assert loaded.code_grader_socket_mode == 0o660

    monkeypatch.setattr(fleet, "CODE_GRADER_UNIT", "")
    monkeypatch.setattr(fleet, "CODE_GRADER_SOCKET", "")
    monkeypatch.setattr(fleet, "CODE_GRADER_BUNDLE_LINK", "")
    monkeypatch.setattr(fleet, "CODE_GRADER_METRICS_PORT", 0)
    monkeypatch.setattr(fleet, "CODE_GRADER_SOCKET_MODE", 0)
    settings.apply_to_fleet_module()

    assert fleet.CODE_GRADER_UNIT == "reliquary-code-grader.service"
    assert fleet.CODE_GRADER_SOCKET == "/tmp/reliquary-grader.sock"
    assert fleet.CODE_GRADER_BUNDLE_LINK == "/opt/reliquary-code-grader/current"
    assert fleet.CODE_GRADER_METRICS_PORT == 9876
    assert fleet.CODE_GRADER_SOCKET_MODE == 0o660


@pytest.mark.parametrize(
    "body",
    [
        "code_readiness:\n  grader_unit: 'bad unit'\n",
        "code_readiness:\n  grader_socket: relative.sock\n",
        "code_readiness:\n  grader_bundle_link: relative\n",
        "code_readiness:\n  grader_metrics_port: 0\n",
        "code_readiness:\n  grader_metrics_port: nope\n",
        "code_readiness:\n  grader_socket_mode: '0999'\n",
        "code_readiness:\n  grader_socket_mode: 660\n",
        "code_readiness: []\n",
    ],
)
def test_settings_rejects_unsafe_code_readiness_overrides(
    monkeypatch, tmp_path, body
):
    monkeypatch.setattr(settings, "SETTINGS", settings.Settings())
    config = tmp_path / "config.yaml"
    config.write_text(body)

    with pytest.raises(ValueError, match="code_readiness"):
        settings.load(config)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("refresh_seconds", "0"),
        ("refresh_seconds", "-1"),
        ("refresh_seconds", ".inf"),
        ("refresh_seconds", ".nan"),
        ("health_poll_stale_seconds", "0"),
        ("health_poll_stale_seconds", "-1"),
        ("health_poll_stale_seconds", ".inf"),
        ("health_poll_stale_seconds", ".nan"),
    ],
)
def test_settings_rejects_invalid_health_cadence(
    monkeypatch, tmp_path, field, value
):
    monkeypatch.setattr(settings, "SETTINGS", settings.Settings())
    config = tmp_path / "config.yaml"
    config.write_text(f"dashboard:\n  {field}: {value}\n")

    with pytest.raises(ValueError, match=field):
        settings.load(config)


@pytest.mark.parametrize("value", ["0", "-1", "inf", "nan"])
def test_cli_refresh_rejects_values_that_disable_staleness(value):
    import fleet_web

    with pytest.raises(argparse.ArgumentTypeError, match="finite positive"):
        fleet_web._positive_refresh_arg(value)


@pytest.mark.parametrize(
    ("refresh", "stale"),
    [
        (0, None),
        (-1, None),
        (math.inf, None),
        (math.nan, None),
        (5, 0),
        (5, -1),
        (5, math.inf),
        (5, math.nan),
    ],
)
def test_healthz_rejects_unbounded_or_non_positive_cadence(refresh, stale):
    import fleet_web

    with pytest.raises(ValueError, match="finite positive"):
        fleet_web.compute_healthz(refresh, poll_stale_s=stale)


def test_settings_propagates_explicit_box_env_file(monkeypatch):
    monkeypatch.setenv("MINER_STATE_ROOT", "/srv/reliquary-miner-pro/state")
    configured = settings._coerce_fleet_box(
        {
            "alias": "ubuntu@192.0.2.10",
            "hotkey": OUR_HOTKEY,
            "label": "h100-reserve1",
            "color": "cyan",
            "unit": "reliquary-miner-pro",
            "env_file": "$MINER_STATE_ROOT/miner-pro.env",
        }
    )
    assert configured.env_file == "/srv/reliquary-miner-pro/state/miner-pro.env"

    monkeypatch.setattr(settings.SETTINGS, "fleet", [configured])
    monkeypatch.setattr(fleet, "FLEET", [])
    monkeypatch.setattr(fleet, "FLEET_ENV_FILES", {})
    monkeypatch.setattr(fleet, "FLEET_UNIT_CANDIDATES", {})
    monkeypatch.setattr(fleet, "OUR_SS58", {})
    settings.apply_to_fleet_module()

    assert fleet.FLEET_ENV_FILES == {
        "h100-reserve1": "/srv/reliquary-miner-pro/state/miner-pro.env"
    }
    assert fleet.FLEET[0][4] == "reliquary-miner-pro"


def test_settings_propagates_named_unit_allowlist(monkeypatch):
    configured = settings._coerce_fleet_box(
        {
            "alias": "ubuntu@192.0.2.10",
            "hotkey": OUR_HOTKEY,
            "label": "h100-reserve1",
            "color": "cyan",
            "unit": "reliquary-miner-pro.service",
            "env_file": "/srv/reliquary-miner-pro/state/miner-pro.env",
            "controller_config_path": "/etc/reliquary-code/mine.toml",
            "allowed_units": [
                {
                    "unit": "reliquary-miner-pro@math-reserve1.service",
                    "env_file": "/srv/reliquary-miner-pro/state/miner-pro-math-reserve1.env",
                },
                {
                    "unit": "reliquary-miner-pro@code-reserve1.service",
                    "env_file": "/srv/reliquary-miner-pro/state/miner-pro-code-reserve1.env",
                    "controller_config_path": (
                        "/etc/reliquary-code/canary.toml"
                    ),
                },
            ],
        }
    )
    monkeypatch.setattr(settings.SETTINGS, "fleet", [configured])
    monkeypatch.setattr(fleet, "FLEET", [])
    monkeypatch.setattr(fleet, "FLEET_ENV_FILES", {})
    monkeypatch.setattr(fleet, "FLEET_UNIT_CANDIDATES", {})
    monkeypatch.setattr(fleet, "FLEET_CONTROLLER_CONFIG_PATHS", {})
    monkeypatch.setattr(fleet, "OUR_SS58", {})

    settings.apply_to_fleet_module()

    assert fleet.FLEET_UNIT_CANDIDATES["h100-reserve1"] == [
        (
            "reliquary-miner-pro.service",
            "/srv/reliquary-miner-pro/state/miner-pro.env",
        ),
        (
            "reliquary-miner-pro@math-reserve1.service",
            "/srv/reliquary-miner-pro/state/miner-pro-math-reserve1.env",
        ),
        (
            "reliquary-miner-pro@code-reserve1.service",
            "/srv/reliquary-miner-pro/state/miner-pro-code-reserve1.env",
        ),
    ]
    assert fleet.FLEET_CONTROLLER_CONFIG_PATHS["h100-reserve1"] == {
        "reliquary-miner-pro.service": (
            "/etc/reliquary-code/mine.toml"
        ),
        "reliquary-miner-pro@code-reserve1.service": (
            "/etc/reliquary-code/canary.toml"
        ),
    }


def test_settings_propagates_stable_active_unit_registry(monkeypatch):
    configured = settings._coerce_fleet_box(
        {
            "alias": "ubuntu@192.0.2.10",
            "hotkey": OUR_HOTKEY,
            "label": "h100-reserve1",
            "color": "cyan",
            "unit": "reliquary-code-cp67-canary.service",
            "telemetry_path": "/state/cp67/dashboard.json",
            "runtime_manifest_path": "/evidence/cp67/runtime.json",
            "active_unit_registry_path": "/state/active-unit-registry.json",
        }
    )
    monkeypatch.setattr(settings.SETTINGS, "fleet", [configured])
    monkeypatch.setattr(fleet, "FLEET_STANDALONE_TELEMETRY", {})

    settings.apply_to_fleet_module()

    assert fleet.FLEET_STANDALONE_TELEMETRY["h100-reserve1"][
        "active_unit_registry_path"
    ] == "/state/active-unit-registry.json"


def test_active_unit_registry_replaces_checkpoint_specific_paths(monkeypatch):
    state = _box(
        unit="reliquary-code-cp67-canary.service",
        unit_candidates=(("reliquary-code-cp67-canary.service", ""),),
        standalone_telemetry_path="/state/cp67/dashboard.json",
        standalone_runtime_manifest_path="/evidence/cp67/runtime.json",
        active_unit_registry_path="/state/active-unit-registry.json",
    )
    active = {
        "unit": "reliquary-code-cp68-mine.service",
        "mode": "mine",
        "controller_config_path": "/etc/reliquary-code/cp68.toml",
        "telemetry_path": "/state/cp68/dashboard.json",
        "runtime_manifest_path": "/evidence/cp68/runtime.json",
        "checkpoint_revision": "a" * 40,
        "source_revision": "b" * 40,
        "unit_fragment_sha256": "c" * 64,
        "controller_config_sha256": "d" * 64,
        "runtime_manifest_sha256": "e" * 64,
    }
    rollback = {**active, "unit": "reliquary-code-cp67-canary.service"}
    registry = {
        "schema_version": 1,
        "generated_at": 1234.5,
        "registry_sha256": "f" * 64,
        "active": active,
        "rollback": [rollback],
    }

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        assert alias == state.alias
        assert timeout_s == 12
        assert "root-owned, bounded and non-writable" in command
        assert "systemd unit fragment digest mismatch" in command
        assert "digest_systemctl_cat(entry['unit']" in command
        assert "len(raw['rollback']) > 8" in command
        assert "/state/active-unit-registry.json" in command
        return 0, json.dumps(registry), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)

    assert fleet._refresh_active_unit_registry(state) is True
    assert state.unit == active["unit"]
    assert state.unit_candidates == (
        (active["unit"], ""),
        (rollback["unit"], ""),
    )
    assert state.standalone_telemetry_path == active["telemetry_path"]
    assert state.standalone_runtime_manifest_path == active["runtime_manifest_path"]
    assert fleet._select_active_unit_registry_entry(state, rollback["unit"])
    assert state.standalone_telemetry_path == rollback["telemetry_path"]


def test_active_unit_registry_allows_unloaded_transient_rollback_units(monkeypatch):
    state = _box(active_unit_registry_path="/state/active-unit-registry.json")
    registry = {
        "schema_version": 1,
        "generated_at": 1234.5,
        "registry_sha256": "f" * 64,
        "active": {
            "unit": "reliquary-code-cp71-canary-active1.service",
            "mode": "canary",
            "controller_config_path": "/etc/reliquary-code/cp71-active1.toml",
            "telemetry_path": "/state/cp71/dashboard.json",
            "runtime_manifest_path": "/evidence/cp71/runtime.json",
            "checkpoint_revision": "a" * 40,
            "source_revision": "b" * 40,
            "unit_fragment_sha256": "c" * 64,
            "controller_config_sha256": "d" * 64,
            "runtime_manifest_sha256": "e" * 64,
        },
        "rollback": [],
    }
    observed = {}

    def fake_ssh(_alias: str, command: str, timeout_s: int = 0):
        observed["command"] = command
        return 0, json.dumps(registry), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)

    assert fleet._refresh_active_unit_registry(state) is True
    command = observed["command"]
    assert "def normalize_entry(raw, *, require_live_fragment):" in command
    assert "normalize_entry(raw['active'], require_live_fragment=True)" in command
    assert "normalize_entry(item, require_live_fragment=False)" in command
    assert "elif require_live_fragment:" in command


def test_active_unit_registry_accepts_exact_systemctl_cat_canonicalization(
    monkeypatch,
):
    """The reader supports both producer-approved fragment byte formats."""
    state = _box(active_unit_registry_path="/state/active-unit-registry.json")
    registry = {
        "schema_version": 1,
        "generated_at": 1234.5,
        "registry_sha256": "f" * 64,
        "active": {
            "unit": "reliquary-code-cp75-canary.service",
            "mode": "canary",
            "controller_config_path": "/etc/reliquary-code/cp75.toml",
            "telemetry_path": "/state/cp75/dashboard.json",
            "runtime_manifest_path": "/evidence/cp75/runtime.json",
            "checkpoint_revision": "a" * 40,
            "source_revision": "b" * 40,
            "unit_fragment_sha256": "c" * 64,
            "controller_config_sha256": "d" * 64,
            "runtime_manifest_sha256": "e" * 64,
        },
        "rollback": [],
        "fragment_verification": {
            "reliquary-code-cp75-canary.service": "systemctl_cat",
        },
    }

    def fake_ssh(_alias: str, command: str, timeout_s: int = 0):
        assert "['systemctl', 'cat', '--no-pager', unit]" in command
        assert "fragment_verification = 'systemctl_cat'" in command
        return 0, json.dumps(registry), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)

    assert fleet._refresh_active_unit_registry(state) is True
    assert state.active_unit_registry["fragment_verification"] == {
        "reliquary-code-cp75-canary.service": "systemctl_cat",
    }


def test_active_unit_registry_failure_is_fail_closed(monkeypatch):
    state = _box(active_unit_registry_path="/state/active-unit-registry.json")
    monkeypatch.setattr(
        fleet,
        "ssh_run",
        lambda *_args, **_kwargs: (1, "", "registry digest mismatch"),
    )

    assert fleet._refresh_active_unit_registry(state) is False
    assert state.unit_resolution_error == "active_unit_registry_invalid"
    assert state.active_unit_registry["error"] == "registry digest mismatch"


def test_settings_propagates_only_the_exact_coordinated_unit_set(monkeypatch):
    configured = settings._coerce_fleet_box(
        {
            "alias": "ubuntu@192.0.2.10",
            "hotkey": OUR_HOTKEY,
            "label": "h100-reserve1",
            "color": "cyan",
            "unit": "reliquary-miner-pro@code-reserve1.service",
            "allowed_units": [
                "reliquary-miner-pro@code-reserve2.service",
                "reliquary-miner-pro@math-reserve1.service",
            ],
            "coordinated_units": [
                "reliquary-miner-pro@code-reserve1.service",
                "reliquary-miner-pro@code-reserve2.service",
            ],
        }
    )
    monkeypatch.setattr(settings.SETTINGS, "fleet", [configured])
    monkeypatch.setattr(fleet, "FLEET_COORDINATED_UNITS", {})

    settings.apply_to_fleet_module()

    assert fleet.FLEET_COORDINATED_UNITS == {
        "h100-reserve1": [
            "reliquary-miner-pro@code-reserve1.service",
            "reliquary-miner-pro@code-reserve2.service",
        ]
    }


@pytest.mark.parametrize(
    "coordinated",
    [
        ["reliquary-miner-pro@code-reserve1.service"],
        [
            "reliquary-miner-pro@code-reserve1.service",
            "reliquary-miner-pro@unknown.service",
        ],
    ],
)
def test_settings_rejects_unsafe_coordinated_unit_sets(coordinated):
    with pytest.raises(ValueError, match="coordinated"):
        settings._coerce_fleet_box(
            {
                "alias": "ubuntu@192.0.2.10",
                "hotkey": OUR_HOTKEY,
                "label": "h100-reserve1",
                "unit": "reliquary-miner-pro@code-reserve1.service",
                "allowed_units": [
                    "reliquary-miner-pro@code-reserve2.service"
                ],
                "coordinated_units": coordinated,
            }
        )


def test_env_resolution_and_collect_use_the_exact_reference_path(monkeypatch):
    explicit = _box(env_file="/opt/reliquary/reference.env")
    reference = _box(env_file="")
    templated = _box(
        unit="reliquary-miner-pro@code.service",
        env_file="",
    )
    assert fleet.resolve_box_env_file(explicit) == "/opt/reliquary/reference.env"
    assert fleet.resolve_box_env_file(reference).endswith("/state/miner-pro.env")
    assert fleet.resolve_box_env_file(templated).endswith("/state/miner-pro-code.env")

    seen: dict[str, object] = {}

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        seen.update(alias=alias, command=command, timeout_s=timeout_s)
        return 0, (
            "===PROC===\n0\n"
            "===ENVFILE===\nok\n"
            "===QUARANTINE===\n{\"active\":false}\n"
        ), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(explicit)

    assert seen["alias"] == explicit.alias
    assert seen["timeout_s"] == 30
    assert "/opt/reliquary/reference.env" in str(seen["command"])
    assert "miner-pro-reliquary-miner-pro.env" not in str(seen["command"])
    assert "sudo -n test -r" in str(seen["command"])
    assert "sudo -n grep -E" in str(seen["command"])
    assert "RELIQUARY_FRONTIER_STATE_PATH" in str(seen["command"])
    assert ".reference-auction-v2" in str(seen["command"])
    assert ".reference-auction-v2.public-*" in str(seen["command"])
    assert ".reference-content.json" in str(seen["command"])
    assert "raw.get('stats')" in str(seen["command"])
    assert explicit.env_file_ok is True
    assert explicit.env_file_error == ""


def test_collect_resolves_named_code_lane_and_uses_its_state_paths(monkeypatch):
    legacy_env = "/srv/reliquary-miner-pro/state/miner-pro.env"
    math_env = "/srv/reliquary-miner-pro/state/miner-pro-math-reserve1.env"
    code_env = "/srv/reliquary-miner-pro/state/miner-pro-code-reserve1.env"
    state = _box(
        unit="reliquary-miner-pro.service",
        env_file=legacy_env,
        unit_candidates=(
            ("reliquary-miner-pro.service", legacy_env),
            ("reliquary-miner-pro@math-reserve1.service", math_env),
            ("reliquary-miner-pro@code-reserve1.service", code_env),
        ),
    )
    calls: list[str] = []

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        calls.append(command)
        if "PYUNIT" in command:
            return 0, json.dumps({
                "active": {
                    "unit": "reliquary-miner-pro@code-reserve1.service",
                    "active_state": "active",
                    "sub_state": "running",
                    "pid": 4242,
                    "restarts": 3,
                },
                "candidates": [],
                "unexpected": [],
                "error": "",
            }), ""
        assert "journalctl -u reliquary-miner-pro@code-reserve1.service" in command
        assert code_env in command
        return 0, (
            "===PROC===\n1\n"
            "===PID===\n4242\n"
            "===ENVFILE===\nok\n"
            "===MINERENV===\n"
            "RELIQUARY_ENVIRONMENT_NAME=opencodeinstruct\n"
            "RELIQUARY_ENGINE_MODE=reference\n"
            "===FRONTIER===\n"
            '{"path":"/srv/reliquary-miner-pro/state/frontier-code-reserve1.json",'
            '"entries":0,"content_entries":0,"age_s":-1,"newest_age_s":-1,'
            '"checkpoint_n":0,"checkpoint_revision":"","ckpts":{}}\n'
            "===QUARANTINE===\n"
            '{"path":"/srv/reliquary-miner-pro/state/quarantine-code-reserve1.json",'
            '"active":false}\n'
            "===REFERENCE===\n"
            "starting pro miner env=opencodeinstruct engine_mode=reference\n"
            "validator runtime telemetry enabled\n"
            "pro miner ready\n"
            "===NRESTARTS===\n3\n"
        ), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert len(calls) == 3
    assert "PYUNIT" in calls[0]
    assert "PYUNIT" in calls[2]
    assert state.proc_alive is True
    assert state.active_unit == "reliquary-miner-pro@code-reserve1.service"
    assert state.active_lane == "code-reserve1"
    assert state.active_environment == "opencodeinstruct"
    assert state.active_pid == 4242
    assert state.restart_count == 3
    assert state.env_file_ok is True
    assert state.frontier_path.endswith("frontier-code-reserve1.json")
    assert state.quarantine_path.endswith("quarantine-code-reserve1.json")
    assert state.quarantine_active is False


def test_collect_fails_closed_when_multiple_allowed_lanes_are_active(monkeypatch):
    state = _box(
        proc_alive=True,
        reference_ready=True,
        gpu_mem_mb=45_000,
        gpu_total_mb=81_000,
        gpu_util=99,
        last_event_at="21:02:03",
        last_oom_at="Jul 18 20:59:00",
        last_oom_age_s=183,
        oom_60m=2,
        mean_accept_t_s=4.25,
        restart_count=9,
        miner_environment="opencodeinstruct",
        engine_mode="reference",
        protocol_profile="forced_seed_v2_auction_legacy_wire",
        miner_source_revision="old-miner",
        reliquary_source_revision="old-validator",
        quarantine_active=True,
        quarantine_path="/state/old-code-quarantine.json",
        drand_offset=3,
        picker_epsilon=0.7,
        picker_top_k=17,
        external_min_sigma=0.4,
        external_max_len=8192,
        hash_max_leading_byte=10,
        burst_per_window=8,
        prompt_shard_id=4,
        prompt_shard_mod=9,
        frontier_path="/state/old-code-frontier.json",
        frontier_entries=42,
        unit_candidates=(
            ("reliquary-miner-pro.service", "/state/legacy.env"),
            ("reliquary-miner-pro@math-reserve1.service", "/state/math.env"),
            ("reliquary-miner-pro@code-reserve1.service", "/state/code.env"),
        ),
    )
    calls = 0

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        nonlocal calls
        calls += 1
        return 0, json.dumps({
            "active": None,
            "candidates": [],
            "unexpected": [],
            "error": "multiple_allowed_units_active",
        }), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert calls == 1
    assert state.proc_alive is False
    assert state.reference_ready is False
    assert state.active_unit == ""
    assert state.unit_resolution_error == "multiple_allowed_units_active"
    assert state.env_file_ok is False
    assert state.gpu_mem_mb == 0
    assert state.gpu_total_mb == 1
    assert state.gpu_util == 0
    assert state.last_event_at == "—"
    assert state.last_oom_at == ""
    assert state.last_oom_age_s == -1
    assert state.oom_60m == 0
    assert state.mean_accept_t_s == 0.0
    assert state.restart_count == 0
    assert state.miner_environment == ""
    assert state.engine_mode == ""
    assert state.protocol_profile == ""
    assert state.miner_source_revision == ""
    assert state.reliquary_source_revision == ""
    assert state.quarantine_active is False
    assert state.quarantine_path == ""
    assert state.drand_offset == 0
    assert state.picker_epsilon == 0.0
    assert state.picker_top_k == 0
    assert state.external_min_sigma == 0.0
    assert state.external_max_len == 0
    assert state.hash_max_leading_byte == -1
    assert state.burst_per_window == 0
    assert state.prompt_shard_id == -1
    assert state.prompt_shard_mod == 0
    assert state.frontier_path == ""
    assert state.frontier_entries == 0


def test_resolver_accepts_only_the_explicit_coordinated_active_set(monkeypatch):
    code_one = "reliquary-miner-pro@code-reserve1.service"
    code_two = "reliquary-miner-pro@code-reserve2.service"
    math = "reliquary-miner-pro@math-reserve1.service"
    state = _box(
        unit=code_one,
        unit_candidates=(
            (code_one, "/state/code-one.env"),
            (code_two, "/state/code-two.env"),
            (math, "/state/math.env"),
        ),
        coordinated_units=(code_one, code_two),
    )

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        assert '"coordinated"' in command
        assert "contender_units != coordinated" in command
        assert "active_units != coordinated" in command
        return 0, json.dumps({
            "active": {
                "unit": code_one,
                "active_state": "active",
                "sub_state": "running",
                "pid": 4101,
                "restarts": 2,
            },
            "active_units": [
                {
                    "unit": code_one,
                    "active_state": "active",
                    "sub_state": "running",
                    "pid": 4101,
                    "restarts": 2,
                },
                {
                    "unit": code_two,
                    "active_state": "active",
                    "sub_state": "running",
                    "pid": 4102,
                    "restarts": 5,
                },
            ],
            "coordinated_unit_statuses": [
                {
                    "unit": code_one,
                    "active_state": "active",
                    "sub_state": "running",
                    "pid": 4101,
                    "restarts": 2,
                },
                {
                    "unit": code_two,
                    "active_state": "active",
                    "sub_state": "running",
                    "pid": 4102,
                    "restarts": 5,
                },
            ],
            "candidates": [],
            "unexpected": [],
            "error": "",
        }), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)

    assert fleet._resolve_allowed_active_unit(state) == (
        code_one,
        "/state/code-one.env",
    )
    assert state.active_units == [code_one, code_two]
    assert state.active_lanes == ["code-reserve1", "code-reserve2"]
    assert state.coordinated_unit_statuses == [
        {
            "unit": code_one,
            "active_state": "active",
            "sub_state": "running",
            "pid": 4101,
            "restarts": 2,
        },
        {
            "unit": code_two,
            "active_state": "active",
            "sub_state": "running",
            "pid": 4102,
            "restarts": 5,
        },
    ]
    assert state.restart_count == 7


def test_collect_fails_closed_on_unexpected_active_lane(monkeypatch):
    state = _box(
        unit_candidates=(
            ("reliquary-miner-pro.service", "/state/legacy.env"),
            ("reliquary-miner-pro@math-reserve1.service", "/state/math.env"),
            ("reliquary-miner-pro@code-reserve1.service", "/state/code.env"),
        ),
    )

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        return 0, json.dumps({
            "active": {
                "unit": "reliquary-miner-pro@code-reserve1.service",
                "pid": 4242,
                "restarts": 0,
            },
            "candidates": [],
            "unexpected": ["reliquary-miner-pro@shadow.service"],
            "error": "unexpected_active_units",
        }), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert state.proc_alive is False
    assert state.unit_resolution_error == (
        "unexpected_active_units:reliquary-miner-pro@shadow.service"
    )
    assert state.unexpected_active_units == [
        "reliquary-miner-pro@shadow.service"
    ]


def test_named_unit_resolver_counts_transitional_or_live_pid_as_contenders(
    monkeypatch,
):
    state = _box(
        unit_candidates=(
            ("reliquary-miner-pro.service", "/state/legacy.env"),
            ("reliquary-miner-pro@math-reserve1.service", "/state/math.env"),
            ("reliquary-miner-pro@code-reserve1.service", "/state/code.env"),
        ),
    )

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        assert "'deactivating'" in command
        assert "'reloading'" in command
        assert "row['pid'] > 0" in command
        assert "row['sub_state'] == 'running'" in command
        assert "'--all'" in command
        return 0, json.dumps({
            "active": None,
            "candidates": [],
            "unexpected": [],
            "error": "multiple_allowed_units_active",
        }), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)

    assert fleet._resolve_allowed_active_unit(state) is None
    assert state.unit_resolution_error == "multiple_allowed_units_active"


def test_collect_fails_closed_if_selected_named_unit_stops_between_probes(
    monkeypatch,
):
    state = _box(
        unit="reliquary-miner-pro.service",
        env_file="/state/legacy.env",
        unit_candidates=(
            ("reliquary-miner-pro.service", "/state/legacy.env"),
            ("reliquary-miner-pro@code-reserve1.service", "/state/code.env"),
        ),
    )
    calls = 0

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        nonlocal calls
        calls += 1
        if calls == 1:
            return 0, json.dumps({
                "active": {
                    "unit": "reliquary-miner-pro@code-reserve1.service",
                    "active_state": "active",
                    "sub_state": "running",
                    "pid": 4242,
                    "restarts": 1,
                },
                "candidates": [],
                "unexpected": [],
                "error": "",
            }), ""
        return 0, (
            "===GPU===\n40000, 81000, 99\n"
            "===PROC===\n0\n"
            "===PID===\n0\n"
            "===ENVFILE===\nok\n"
            "===MINERENV===\nRELIQUARY_ENVIRONMENT_NAME=opencodeinstruct\n"
        ), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert calls == 2
    assert state.proc_alive is False
    assert state.active_unit == ""
    assert state.active_lane == ""
    assert state.active_pid == 0
    assert state.gpu_mem_mb == 0
    assert state.unit_resolution_error == "selected_unit_became_inactive"
    assert state.error == "selected_unit_became_inactive"


def test_named_lane_probe_publishes_only_a_complete_snapshot(monkeypatch):
    state = _box(
        unit="reliquary-miner-pro.service",
        env_file="/state/legacy.env",
        unit_candidates=(
            ("reliquary-miner-pro.service", "/state/legacy.env"),
            ("reliquary-miner-pro@code-reserve1.service", "/state/code.env"),
        ),
        proc_alive=True,
        active_unit="reliquary-miner-pro@code-reserve1.service",
        active_lane="code-reserve1",
        active_environment="opencodeinstruct",
        active_pid=4242,
        gpu_mem_mb=40_000,
        gpu_total_mb=81_000,
        gpu_util=97,
        env_file_ok=True,
        miner_environment="opencodeinstruct",
        engine_mode="reference",
    )
    entered = threading.Event()
    release = threading.Event()

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        entered.set()
        assert release.wait(timeout=2)
        return 0, json.dumps({
            "active": None,
            "candidates": [],
            "unexpected": [],
            "error": "no_allowed_unit_active",
        }), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    worker = threading.Thread(target=fleet.collect_box, args=(state,))
    worker.start()
    assert entered.wait(timeout=2)

    # HTTP readers keep seeing the last complete healthy snapshot while the
    # resolver and metrics round trips are in flight.
    assert state.proc_alive is True
    assert state.active_unit == "reliquary-miner-pro@code-reserve1.service"
    assert state.active_lane == "code-reserve1"
    assert state.active_pid == 4242
    assert state.gpu_mem_mb == 40_000
    assert state.env_file_ok is True

    release.set()
    worker.join(timeout=2)
    assert not worker.is_alive()
    assert state.proc_alive is False
    assert state.active_unit == ""
    assert state.active_lane == ""
    assert state.gpu_mem_mb == 0
    assert state.unit_resolution_error == "no_allowed_unit_active"


def test_named_lane_final_guard_rejects_new_contender_during_metrics_probe(
    monkeypatch,
):
    state = _box(
        unit="reliquary-miner-pro.service",
        env_file="/state/legacy.env",
        unit_candidates=(
            ("reliquary-miner-pro.service", "/state/legacy.env"),
            ("reliquary-miner-pro@code-reserve1.service", "/state/code.env"),
            ("reliquary-miner-pro@math-reserve1.service", "/state/math.env"),
        ),
    )
    state.oom_trend.append(7)
    state.mem_trend.append(31)
    calls = 0

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        nonlocal calls
        calls += 1
        if calls == 1:
            return 0, json.dumps({
                "active": {
                    "unit": "reliquary-miner-pro@code-reserve1.service",
                    "active_state": "active",
                    "sub_state": "running",
                    "pid": 4242,
                    "restarts": 0,
                },
                "candidates": [],
                "unexpected": [],
                "error": "",
            }), ""
        if calls == 2:
            return 0, (
                "===GPU===\n40000, 81000, 99\n"
                "===PROC===\n1\n"
                "===PID===\n4242\n"
                "===ENVFILE===\nok\n"
                "===MINERENV===\n"
                "RELIQUARY_ENVIRONMENT_NAME=opencodeinstruct\n"
                "RELIQUARY_ENGINE_MODE=reference\n"
                "===NRESTARTS===\n0\n"
            ), ""
        return 0, json.dumps({
            "active": None,
            "candidates": [],
            "unexpected": [],
            "error": "multiple_allowed_units_active",
        }), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert calls == 3
    assert state.proc_alive is False
    assert state.active_unit == ""
    assert state.active_lane == ""
    assert state.active_pid == 0
    assert state.gpu_mem_mb == 0
    assert state.unit_resolution_error == "multiple_allowed_units_active"
    assert list(state.oom_trend) == [7]
    assert list(state.mem_trend) == [31]


def test_named_lane_rejects_pid_restart_before_metrics_publish(monkeypatch):
    state = _box(
        unit="reliquary-miner-pro.service",
        env_file="/state/legacy.env",
        unit_candidates=(
            ("reliquary-miner-pro.service", "/state/legacy.env"),
            ("reliquary-miner-pro@code-reserve1.service", "/state/code.env"),
        ),
    )
    calls = 0

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        nonlocal calls
        calls += 1
        if calls == 1:
            return 0, json.dumps({
                "active": {
                    "unit": "reliquary-miner-pro@code-reserve1.service",
                    "active_state": "active",
                    "sub_state": "running",
                    "pid": 100,
                    "restarts": 0,
                },
                "candidates": [],
                "unexpected": [],
                "error": "",
            }), ""
        return 0, (
            "===GPU===\n40000, 81000, 99\n"
            "===PROC===\n1\n"
            "===PID===\n200\n"
            "===ENVFILE===\nok\n"
            "===MINERENV===\n"
            "RELIQUARY_ENVIRONMENT_NAME=opencodeinstruct\n"
            "RELIQUARY_ENGINE_MODE=reference\n"
        ), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert calls == 2
    assert state.proc_alive is False
    assert state.active_unit == ""
    assert state.active_lane == ""
    assert state.active_pid == 0
    assert state.active_environment == ""
    assert state.gpu_mem_mb == 0
    assert state.unit_resolution_error == "unit_selection_changed_during_probe"
    assert state.error == "unit_selection_changed_during_probe"


def test_named_lane_requires_metrics_pid_corroboration(monkeypatch):
    state = _box(
        unit="reliquary-miner-pro.service",
        env_file="/state/legacy.env",
        unit_candidates=(
            ("reliquary-miner-pro.service", "/state/legacy.env"),
            ("reliquary-miner-pro@code-reserve1.service", "/state/code.env"),
        ),
    )
    calls = 0

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        nonlocal calls
        calls += 1
        if calls == 1:
            return 0, json.dumps({
                "active": {
                    "unit": "reliquary-miner-pro@code-reserve1.service",
                    "active_state": "active",
                    "sub_state": "running",
                    "pid": 100,
                    "restarts": 0,
                },
                "candidates": [],
                "unexpected": [],
                "error": "",
            }), ""
        return 0, "===PROC===\n1\n===ENVFILE===\nok\n", ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert calls == 2
    assert state.proc_alive is False
    assert state.active_unit == ""
    assert state.active_pid == 0
    assert state.unit_resolution_error == "selected_unit_became_inactive"


def test_named_lane_final_guard_compares_pid_to_initial_resolver(monkeypatch):
    state = _box(
        unit="reliquary-miner-pro.service",
        env_file="/state/legacy.env",
        unit_candidates=(
            ("reliquary-miner-pro.service", "/state/legacy.env"),
            ("reliquary-miner-pro@code-reserve1.service", "/state/code.env"),
        ),
    )
    calls = 0

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        nonlocal calls
        calls += 1
        pid = 100 if calls == 1 else 200
        if calls in (1, 3):
            return 0, json.dumps({
                "active": {
                    "unit": "reliquary-miner-pro@code-reserve1.service",
                    "active_state": "active",
                    "sub_state": "running",
                    "pid": pid,
                    "restarts": int(calls == 3),
                },
                "candidates": [],
                "unexpected": [],
                "error": "",
            }), ""
        return 0, (
            "===GPU===\n40000, 81000, 99\n"
            "===PROC===\n1\n"
            "===PID===\n100\n"
            "===ENVFILE===\nok\n"
            "===MINERENV===\n"
            "RELIQUARY_ENVIRONMENT_NAME=opencodeinstruct\n"
            "RELIQUARY_ENGINE_MODE=reference\n"
        ), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert calls == 3
    assert state.proc_alive is False
    assert state.active_unit == ""
    assert state.active_pid == 0
    assert state.gpu_mem_mb == 0
    assert state.unit_resolution_error == "unit_selection_changed_during_probe"


def test_web_box_publication_keeps_interleaved_reader_on_one_snapshot(
    monkeypatch,
):
    import fleet_web

    old = _box(
        active_unit="reliquary-miner-pro@code-reserve1.service",
        active_lane="code-reserve1",
        active_pid=111,
        proc_alive=True,
    )
    new = _box(
        active_unit="reliquary-miner-pro@math-reserve1.service",
        active_lane="math-reserve1",
        active_pid=222,
        proc_alive=True,
    )
    monkeypatch.setattr(fleet_web, "_boxes", [old])

    # Match a renderer: capture the owned object under the reader lock, then
    # read fields after releasing it. Publish exactly between two field reads.
    with fleet_web._lock:
        captured = list(fleet_web._boxes)[0]
    observed_unit = captured.active_unit
    fleet_web._publish_box_snapshots([new])
    observed_pid = captured.active_pid

    assert (observed_unit, observed_pid) == (
        "reliquary-miner-pro@code-reserve1.service",
        111,
    )
    with fleet_web._lock:
        published = fleet_web._boxes[0]
    assert published is new
    assert (published.active_unit, published.active_pid) == (
        "reliquary-miner-pro@math-reserve1.service",
        222,
    )
    poller_source = inspect.getsource(fleet_web.poller_loop)
    assert "collect_box_snapshot" in poller_source
    assert "_publish_box_snapshots" in poller_source


def test_collect_parses_reference_frontier_and_live_pipeline(monkeypatch):
    state = _box()
    seen: dict[str, str] = {}

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        seen["command"] = command
        return 0, (
            "===PROC===\n1\n"
            "===ENVFILE===\nok\n"
            "===MINERENV===\n"
            "RELIQUARY_BASE_MODEL_REPO=Qwen/Qwen3.5-2B\n"
            "RELIQUARY_BASE_MODEL_REVISION=" + "1" * 40 + "\n"
            "MINER_PRO_SOURCE_REVISION=" + "2" * 40 + "\n"
            "RELIQUARY_SOURCE_REVISION=" + "3" * 40 + "\n"
            "PROVISIONED_OK=1\n"
            "RELIQUARY_PROVISIONED_MODEL_KIND=validator_checkpoint\n"
            "RELIQUARY_PROVISIONED_CHECKPOINT_N=39\n"
            "RELIQUARY_PROVISIONED_MODEL_REPO=ReliquaryForge/qwen3.5-2b-reliquary-v2\n"
            "RELIQUARY_PROVISIONED_MODEL_REVISION="
            "02af5e8015a409e1a00fe381e583e4c2450683b1\n"
            "===FRONTIER===\n"
            '{"path":"/srv/reliquary-miner-pro/state/frontier.json",'
            '"entries":37,"content_entries":1228,"age_s":1.2,'
            '"newest_age_s":1.2,"checkpoint_n":39,'
            '"checkpoint_revision":"02af5e8015a409e1a00fe381e583e4c2450683b1",'
            '"ckpts":{"02af5e8015a4":37}}\n'
            "===QUARANTINE===\n{\"active\":false}\n"
            "===REFERENCE===\n"
            "checkpoint cache miss; falling back to Hub repo=x "
            "revision=02af5e8015a409e1a00fe381e583e4c2450683b1\n"
            "reference Math frontier checkpoint advanced n=39 "
            "hash=2845c238..02af5e80\n"
            "reference Math content frontier selected prompt=19194759 "
            "hash=ca1370795dc0 avg_max_tokens=2034.0\n"
        ), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert "values.get('RELIQUARY_FRONTIER_STATE_PATH')" in seen["command"]
    assert "sudo -n python3" in seen["command"]
    assert "bash -c" not in seen["command"]
    assert "RELIQUARY_PROVISIONED_MODEL_KIND" in seen["command"]
    assert "RELIQUARY_BASE_MODEL_REVISION" in seen["command"]
    assert state.source_manifest_provisioned_ok is True
    assert state.provisioned_model_kind == "validator_checkpoint"
    assert state.provisioned_checkpoint_n == 39
    assert state.provisioned_model_repo.endswith("reliquary-v2")
    assert state.provisioned_model_revision.startswith("02af5e80")
    assert state.base_model_repo == "Qwen/Qwen3.5-2B"
    assert state.base_model_revision == "1" * 40
    assert state.frontier_path.endswith("/state/frontier.json")
    assert state.frontier_entries == 37
    assert state.frontier_content_entries == 1228
    assert state.frontier_checkpoint_n == 39
    assert state.frontier_ckpts == {"02af5e8015a4": 37}
    assert state.local_checkpoint_n == 39
    assert state.local_checkpoint_revision.startswith("02af5e80")
    assert state.miner_state == "generating"
    assert state.miner_inflight == 1


def test_collect_attests_dynamic_checkpoint_from_active_process(monkeypatch):
    revision = "b" * 40
    repo = "ReliquaryForge/qwen3.5-2b-reliquary-v3"
    state = _box()

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        assert "===RUNTIMECHECKPOINT===" in command
        assert "REFERENCE_LOG=$(" in command
        return 0, (
            "===PROC===\n1\n"
            "===PID===\n4242\n"
            "===UPTIME===\n1000\n"
            "===NOW===\n1100\n"
            "===ENVFILE===\nok\n"
            "===MINERENV===\n"
            "PROVISIONED_OK=1\n"
            "RELIQUARY_PROVISIONED_MODEL_KIND=validator_checkpoint\n"
            "RELIQUARY_PROVISIONED_CHECKPOINT_N=4\n"
            f"RELIQUARY_PROVISIONED_MODEL_REPO={repo}\n"
            f"RELIQUARY_PROVISIONED_MODEL_REVISION={'a' * 40}\n"
            "===QUARANTINE===\n{\"active\":false}\n"
            "===RUNTIMECHECKPOINT===\n"
            f"checkpoint cache miss; falling back to Hub repo={repo} "
            f"revision={revision}\n"
            f"Checkpoint /cache/snapshots/{revision} loaded into both models\n"
            "math_generation_group_abort "
            + json.dumps(
                {
                    "checkpoint_n": 5,
                    "checkpoint_revision": revision,
                    "event": "math_generation_group_abort",
                    "reason": "safe_send_deadline",
                },
                separators=(",", ":"),
            )
            + "\n"
        ), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert state.active_pid == 4242
    assert state.active_started_at == 1000
    assert state.proc_uptime_s == 100
    assert state.runtime_checkpoint_loaded is True
    assert state.runtime_checkpoint_n == 5
    assert state.runtime_checkpoint_repo == repo
    assert state.runtime_checkpoint_revision == revision
    assert state.runtime_checkpoint_pid == 4242
    assert state.runtime_checkpoint_started_at == 1000
    assert state.runtime_checkpoint_evidence == "loaded+math_generation_group_abort"
    assert state.local_checkpoint_n == 5
    assert state.local_checkpoint_revision == revision


def test_runtime_checkpoint_parser_requires_matching_load_and_structured_abort():
    revision = "b" * 40
    other_revision = "c" * 40
    repo = "ReliquaryForge/qwen3.5-2b-reliquary-v3"
    resolver = (
        f"checkpoint resolved from local cache repo={repo} revision={revision}"
    )
    loaded = f"Checkpoint /cache/snapshots/{revision} loaded into both models"
    valid_abort = "math_generation_group_abort " + json.dumps(
        {
            "checkpoint_n": 5,
            "checkpoint_revision": revision,
            "event": "math_generation_group_abort",
        },
        separators=(",", ":"),
    )

    assert fleet._parse_runtime_checkpoint_evidence([resolver, loaded]) is None
    assert fleet._parse_runtime_checkpoint_evidence(
        [
            resolver,
            f"Checkpoint /cache/snapshots/{other_revision} loaded into both models",
            valid_abort,
        ]
    ) is None
    assert fleet._parse_runtime_checkpoint_evidence(
        [resolver, loaded, valid_abort.replace('"checkpoint_n":5', '"checkpoint_n":0')]
    ) is None
    assert fleet._parse_runtime_checkpoint_evidence(
        [resolver, loaded, valid_abort]
    ) == {
        "checkpoint_n": 5,
        "repo": repo,
        "revision": revision,
        "evidence": "loaded+math_generation_group_abort",
    }


def test_runtime_checkpoint_parser_accepts_live_generation_checkpoint():
    revision = "d684a42bec0a83f75620e78515f4fb21b251a9e8"
    repo = "ReliquaryForge/qwen3.5-2b-reliquary-v3"
    started = "generation_started " + json.dumps(
        {
            "checkpoint_n": 10,
            "checkpoint_repo_id": repo,
            "checkpoint_revision": revision,
            "event": "generation_started",
        },
        separators=(",", ":"),
    )
    abort = "code_generation_group_abort " + json.dumps(
        {
            "checkpoint_n": 10,
            "checkpoint_revision": revision,
            "event": "code_generation_group_abort",
            "reason": "local_token_limit",
        },
        separators=(",", ":"),
    )

    assert fleet._parse_runtime_checkpoint_evidence([started]) == {
        "checkpoint_n": 10,
        "repo": repo,
        "revision": revision,
        "evidence": "generation+generation_started",
    }
    # The currently deployed miner's abort marker omits the stable repository.
    # Without local repository evidence it remains fail closed.
    assert fleet._parse_runtime_checkpoint_evidence([abort]) is None
    assert fleet._parse_runtime_checkpoint_evidence(
        [abort], repo_hint=repo
    ) == {
        "checkpoint_n": 10,
        "repo": repo,
        "revision": revision,
        "evidence": "generation+code_generation_group_abort",
    }
    assert fleet._parse_runtime_checkpoint_evidence(
        [started.replace('"event":"generation_started"', '"event":"other"')]
    ) is None


@pytest.mark.parametrize(
    "event",
    ["code_generation_group_start", "math_generation_group_start"],
)
def test_runtime_checkpoint_parser_accepts_current_group_start_event(event):
    revision = "947ae4dd9c69ef490e8636ef43a7661b9b548dfd"
    repo = "ReliquaryForge/qwen3.5-2b-reliquary-v3"
    started = event + " " + json.dumps(
        {
            "checkpoint_n": 14,
            "checkpoint_revision": revision,
            "environment": (
                "opencodeinstruct"
                if event.startswith("code_")
                else "openmathinstruct"
            ),
            "event": event,
            "window_n": 23821,
        },
        separators=(",", ":"),
    )

    assert fleet._parse_runtime_checkpoint_evidence(
        [started], repo_hint=repo
    ) == {
        "checkpoint_n": 14,
        "repo": repo,
        "revision": revision,
        "evidence": f"generation+{event}",
    }
    assert fleet._parse_runtime_checkpoint_evidence([started]) is None


def test_runtime_checkpoint_parser_accepts_pid_bound_math_readiness_only():
    revision = "e726baf4c6ce175c1e294de69d27fc5d9dce2df9"
    repo = "ReliquaryForge/qwen3.5-2b-reliquary-v3"
    readiness = {
        "auction_policy": "deadline_aware",
        "checkpoint_n": 26,
        "checkpoint_repo_id": repo,
        "checkpoint_revision": revision,
        "environment": "openmathinstruct",
        "process_pid": 2605800,
        "public_source_revision": "2" * 40,
        "runtime_profile_hash": "3" * 64,
        "schema_version": 1,
    }

    def line(payload):
        return "math_auction_readiness " + json.dumps(
            payload, separators=(",", ":")
        )

    assert fleet._parse_runtime_checkpoint_evidence(
        [line(readiness)],
        expected_process_pid=2605800,
        expected_public_source_revision="2" * 40,
    ) == {
        "checkpoint_n": 26,
        "repo": repo,
        "revision": revision,
        "process_pid": 2605800,
        "evidence": "readiness+math_auction_readiness",
    }
    assert fleet._parse_runtime_checkpoint_evidence([line(readiness)]) is None
    assert fleet._parse_runtime_checkpoint_evidence(
        [line(readiness)],
        expected_process_pid=2605801,
        expected_public_source_revision="2" * 40,
    ) is None

    invalid_fields = {
        "auction_policy": "legacy",
        "checkpoint_n": True,
        "checkpoint_repo_id": "repo with spaces",
        "checkpoint_revision": "e" * 39,
        "environment": "opencodeinstruct",
        "process_pid": True,
        "public_source_revision": "4" * 40,
        "runtime_profile_hash": "3" * 63,
        "schema_version": 2,
    }
    for key, value in invalid_fields.items():
        malformed = dict(readiness)
        malformed[key] = value
        assert fleet._parse_runtime_checkpoint_evidence(
            [line(malformed)],
            expected_process_pid=2605800,
            expected_public_source_revision="2" * 40,
        ) is None


def test_collect_attests_checkpoint_from_current_math_readiness(monkeypatch):
    import fleet_web

    revision = "e726baf4c6ce175c1e294de69d27fc5d9dce2df9"
    repo = "ReliquaryForge/qwen3.5-2b-reliquary-v3"
    readiness = {
        "auction_policy": "deadline_aware",
        "checkpoint_n": 26,
        "checkpoint_repo_id": repo,
        "checkpoint_revision": revision,
        "environment": "openmathinstruct",
        "process_pid": 2605800,
        "public_source_revision": "2" * 40,
        "runtime_profile_hash": "3" * 64,
        "schema_version": 1,
    }
    state = _box()

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        assert "math_auction_readiness[[:space:]]" in command
        return 0, (
            "===PROC===\n1\n"
            "===PID===\n2605800\n"
            "===UPTIME===\n1000\n"
            "===NOW===\n1100\n"
            "===ENVFILE===\nok\n"
            "===MINERENV===\n"
            "RELIQUARY_ENGINE_MODE=reference\n"
            f"RELIQUARY_SOURCE_REVISION={'2' * 40}\n"
            "PROVISIONED_OK=1\n"
            "RELIQUARY_PROVISIONED_MODEL_KIND=validator_checkpoint\n"
            "RELIQUARY_PROVISIONED_CHECKPOINT_N=24\n"
            f"RELIQUARY_PROVISIONED_MODEL_REPO={repo}\n"
            f"RELIQUARY_PROVISIONED_MODEL_REVISION={'a' * 40}\n"
            "===QUARANTINE===\n{\"active\":false}\n"
            "===RUNTIMECHECKPOINT===\n"
            "math_auction_readiness "
            + json.dumps(readiness, separators=(",", ":"))
            + "\n"
        ), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert state.runtime_checkpoint_loaded is True
    assert state.runtime_checkpoint_n == 26
    assert state.runtime_checkpoint_repo == repo
    assert state.runtime_checkpoint_revision == revision
    assert state.runtime_checkpoint_pid == 2605800
    assert state.runtime_checkpoint_started_at == 1000
    assert (
        state.runtime_checkpoint_evidence
        == "readiness+math_auction_readiness"
    )
    validator_state = fleet.ValidatorState(
        checkpoint_n=26,
        checkpoint_repo_id=repo,
        checkpoint_revision=revision,
    )
    details = fleet_web._box_model_details(state, validator_state)
    assert details["runtime"]["valid"] is True
    assert details["runtime"]["exact_validator_identity"] is True
    assert details["active"]["source"] == "runtime_journal"


def test_collect_sparse_restart_checkpoint_supersedes_stale_manifest(monkeypatch):
    import fleet_web

    revision = "d684a42bec0a83f75620e78515f4fb21b251a9e8"
    stale_revision = "a97bcbcba29c432449f342d478a54657cee23ce6"
    repo = "ReliquaryForge/qwen3.5-2b-reliquary-v3"
    state = _box()

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        assert "REFERENCE_LOG=$(" in command
        assert "generation(_group)?_start(ed)?( |$)" in command
        # A long post-generation series of admission blocks must not evict
        # the real checkpoint-bearing start event from the sparse probe.
        assert "generation(_group)?_start(ed)?'" not in command
        return 0, (
            "===PROC===\n1\n"
            "===PID===\n4242\n"
            "===UPTIME===\n1000\n"
            "===NOW===\n1100\n"
            "===ENVFILE===\nok\n"
            "===MINERENV===\n"
            "RELIQUARY_ENGINE_MODE=reference\n"
            "PROVISIONED_OK=1\n"
            "RELIQUARY_PROVISIONED_MODEL_KIND=validator_checkpoint\n"
            "RELIQUARY_PROVISIONED_CHECKPOINT_N=9\n"
            f"RELIQUARY_PROVISIONED_MODEL_REPO={repo}\n"
            f"RELIQUARY_PROVISIONED_MODEL_REVISION={stale_revision}\n"
            "===QUARANTINE===\n{\"active\":false}\n"
            "===RUNTIMECHECKPOINT===\n"
            "code_generation_group_abort "
            + json.dumps(
                {
                    "checkpoint_n": 10,
                    "checkpoint_revision": revision,
                    "event": "code_generation_group_abort",
                    "reason": "local_token_limit",
                },
                separators=(",", ":"),
            )
            + "\n"
        ), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert state.runtime_checkpoint_loaded is True
    assert state.runtime_checkpoint_n == 10
    assert state.runtime_checkpoint_repo == repo
    assert state.runtime_checkpoint_revision == revision
    assert state.runtime_checkpoint_pid == 4242
    assert state.runtime_checkpoint_started_at == 1000
    assert (
        state.runtime_checkpoint_evidence
        == "generation+code_generation_group_abort"
    )
    assert state.local_checkpoint_n == 10
    assert state.local_checkpoint_revision == revision

    validator_state = fleet.ValidatorState(
        checkpoint_n=10,
        checkpoint_repo_id=repo,
        checkpoint_revision=revision,
    )
    assert fleet_web._box_model_readiness_issues(state, validator_state) == []
    details = fleet_web._box_model_details(state, validator_state)
    assert details["provisioned"]["checkpoint_n"] == 9
    assert details["provisioned"]["revision"] == stale_revision
    assert details["runtime"]["valid"] is True
    assert details["active"]["source"] == "runtime_journal"
    assert details["active"]["checkpoint_n"] == 10
    assert details["active"]["revision"] == revision


def test_collect_clears_model_identity_when_manifest_metadata_disappears(monkeypatch):
    state = _box(
        miner_source_revision="2" * 40,
        reliquary_source_revision="3" * 40,
        source_manifest_provisioned_ok=True,
        provisioned_model_kind="validator_checkpoint",
        provisioned_checkpoint_n=39,
        provisioned_model_repo="ReliquaryForge/old",
        provisioned_model_revision="4" * 40,
        base_model_repo="Qwen/Qwen3.5-2B",
        base_model_revision="1" * 40,
    )

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        return 0, (
            "===PROC===\n1\n"
            "===ENVFILE===\nok\n"
            "===MINERENV===\n"
            "===QUARANTINE===\n{\"active\":false}\n"
        ), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert state.miner_source_revision == ""
    assert state.reliquary_source_revision == ""
    assert state.source_manifest_provisioned_ok is False
    assert state.provisioned_model_kind == ""
    assert state.provisioned_checkpoint_n == -1
    assert state.provisioned_model_repo == ""
    assert state.provisioned_model_revision == ""
    assert state.base_model_repo == ""
    assert state.base_model_revision == ""


def test_collect_uses_installed_sampler_profile_when_cli_marker_is_absent(monkeypatch):
    state = _box()

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        assert "===REFERENCEPROFILE===" in command
        assert "installed protocol-identical vectorized forced-seed sampler" in command
        return 0, (
            "===PROC===\n1\n"
            "===ENVFILE===\nok\n"
            "===QUARANTINE===\n{\"active\":false}\n"
            "===REFERENCEPROFILE===\n"
            "installed protocol-identical vectorized forced-seed sampler "
            "with uniform precompute "
            "profile=forced_seed_v2_auction_legacy_wire protocol=2 rollouts=8\n"
            "===REFERENCE===\n"
            "starting pro miner env=OpenMathInstruct engine_mode=reference\n"
            "validator runtime telemetry enabled\n"
            "pro miner ready\n"
        ), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert state.protocol_profile == "forced_seed_v2_auction_legacy_wire"
    assert state.runtime_parity_ok is True
    assert state.reference_ready is True


def test_collect_marks_reference_pipeline_waiting_after_local_drop(monkeypatch):
    state = _box(miner_state="submitted", miner_window=77)

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        assert "pre-screen dropped group before GRAIL" in command
        assert "generated 0/8 .* skipping" in command
        return 0, (
            "===PROC===\n1\n"
            "===ENVFILE===\nok\n"
            "===QUARANTINE===\n{\"active\":false}\n"
            "===REFERENCEPROFILE===\n"
            "validator protocol parity ok profile=forced_seed_v2_auction_legacy_wire\n"
            "validator runtime parity ok\n"
            "pro miner ready\n"
            "===REFERENCE===\n"
            "reference Math content frontier selected prompt=19 hash=abc\n"
            "reference Math frontier learned prompt=19 k=0/8\n"
            "reference Math frontier pre-screen dropped group before GRAIL "
            "k=0/8 sigma=0.0 required=0.43\n"
            "generated 0/8 for prompt 19; skipping\n"
        ), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert state.miner_state == "waiting"
    assert state.miner_window == 0
    assert state.miner_inflight == 0
    assert state.miner_ready == 1
    assert state.runtime_parity_ok is True
    assert state.reference_ready is True
    assert state.miner_submitted_this_win == 0


def test_collect_preserves_window_submission_through_later_local_drop(monkeypatch):
    state = _box()

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        return 0, (
            "===PROC===\n1\n"
            "===ENVFILE===\nok\n"
            "===QUARANTINE===\n{\"active\":false}\n"
            "===REFERENCE===\n"
            "SUBMIT-DIAG prompt=11 window=77 status=200 t=1.0s\n"
            "submitted window=77 prompt=11 accepted=True reason=submitted\n"
            "reference Math content frontier selected prompt=12 hash=abc\n"
            "reference Math frontier learned prompt=12 k=8/8\n"
            "reference Math frontier pre-screen dropped group before GRAIL "
            "k=8/8 sigma=0.0 required=0.43\n"
            "generated 0/8 for prompt 12; skipping\n"
        ), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert state.miner_state == "waiting"
    assert state.miner_window == 77
    assert state.miner_inflight == 0
    assert state.miner_ready == 1
    assert state.miner_submitted_this_win == 1


def test_validator_ssh_options_precede_destination_and_empty_key_is_omitted(monkeypatch):
    monkeypatch.setattr(fleet, "VALIDATOR_SSH", "ubuntu@198.51.100.10")
    monkeypatch.setattr(fleet, "VALIDATOR_PORT", 2222)
    monkeypatch.setattr(fleet, "SSH_KEY", "")

    args = fleet._validator_ssh_args("docker logs reliquary-trainer")
    destination = args.index("ubuntu@198.51.100.10")
    assert "-i" not in args
    assert args.index("-p") < destination
    assert args[destination + 1] == "docker logs reliquary-trainer"
    assert destination == len(args) - 2

    monkeypatch.setattr(fleet, "SSH_KEY", "/tmp/validator-key")
    args = fleet._validator_ssh_args("true", server_alive=True)
    destination = args.index("ubuntu@198.51.100.10")
    assert args[args.index("-i") + 1] == "/tmp/validator-key"
    assert args.index("-i") < destination
    assert args.index("ServerAliveInterval=30") < destination


def test_structured_lifecycle_selected_and_reward_events(monkeypatch):
    monkeypatch.setattr(fleet, "OUR_SS58", {OUR_HOTKEY: "h100-reserve1"})

    selected_payload = {
        "event": "validator_submit_lifecycle",
        "stage": "final_batch_selected",
        "hotkey": OUR_HOTKEY,
        "window_n": 23391,
        "prompt_idx": 18,
        "env_name": "openmathinstruct",
        "selected_for_batch": True,
        "submitted_drand_round": 30485227,
    }
    selected = fleet._classify_validator_line(
        "2026-07-17 01:02:03 | reliquary.validator.server | INFO | "
        + json.dumps(selected_payload)
    )
    assert selected is not None
    assert (selected.kind, selected.ours, selected.window_n) == ("selected", True, 23391)
    assert (selected.env_name, selected.prompt_idx) == ("openmathinstruct", 18)

    reward_payload = {
        **selected_payload,
        "stage": "reward_assigned",
        "env_name": "opencodeinstruct",
        "reward_amount": 0.125,
    }
    reward = fleet._classify_validator_line(
        "2026-07-17 01:02:04 | reliquary.validator.server | INFO | "
        + json.dumps(reward_payload)
    )
    assert reward is not None
    assert reward.kind == "reward"
    assert reward.env_name == "opencodeinstruct"
    assert reward.reward_amount == 0.125


def test_validator_seal_parser_covers_current_and_legacy_messages():
    lines = (
        "2026-07-17 01:02:03 | reliquary.validator.service | INFO | "
        "Window 23491: all 2 batcher(s) sealed",
        "2026-07-17 01:07:03 | reliquary.validator.service | WARNING | "
        "Window 23492 sealed by liveness breaker: sparse_timeout",
        "2026-05-11 07:54:28 | reliquary.validator.service | INFO | "
        "Window 453 sealed (B valid received)",
    )

    events = [fleet._classify_validator_line(line) for line in lines]

    assert [event.kind for event in events if event is not None] == [
        "seal", "seal", "seal"
    ]
    assert [event.window_n for event in events if event is not None] == [
        23491, 23492, 453
    ]
    assert r"batcher\(s\) sealed" in fleet._VALIDATOR_TAIL_GREP_EXPR
    assert "sealed by liveness breaker" in fleet._VALIDATOR_TAIL_GREP_EXPR


def test_health_normalization_and_final_verdict_summary(monkeypatch):
    monkeypatch.setattr(fleet.time, "time", lambda: 10_000.0)
    vs = fleet.ValidatorState(window=40, valid=1)
    fleet._apply_validator_health(
        vs,
        {
            "status": "ok",
            "image_revision": "03524765f656925eadcc7a04183ac38ebaccb514",
            "app_started_at": 9_000.0,
            "current_validator_state": "open",
            "current_window_n": 42,
            "valid_submissions_count": 6,
            "batch_size": 8,
            "queue_depth": 28,
            "proof_admission_count": 7,
            "post_trigger_proof_admission_limit": 8,
            "proof_verification_inflight": 1,
            "pending_proof_reservations": 19,
            "inflight_proof_reservations": 1,
            "forced_seed_enforced": True,
            "forced_seed_cdf_enforced": False,
            "runtime_fingerprint": {"gpu_name": "NVIDIA H100 PCIe", "torch_version": "2.7.0+cu128"},
            "training_accumulator_targets": {"openmathinstruct": "8", "opencodeinstruct": 8},
            "window_environments": {
                "openmathinstruct": {"valid_submissions_count": 6, "distinct_valid_prompt_count": 6},
                "opencodeinstruct": {"valid_submissions_count": 11, "distinct_valid_prompt_count": 11},
            },
            "recent_reject_counts_by_reason": {"batch_filled": 22},
            "archive_queue_depth": 0,
        },
    )
    assert (vs.health_status, vs.window, vs.valid) == ("ok", 42, 6)
    assert vs.health_last_fetch_at == 10_000.0
    assert vs.runtime_fingerprint["gpu_name"] == "NVIDIA H100 PCIe"
    assert vs.environment_targets == {"openmathinstruct": 8, "opencodeinstruct": 8}
    assert vs.window_environments["opencodeinstruct"]["valid_submissions_count"] == 11
    assert vs.forced_seed_enforced is True

    summary = fleet.summarize_verdicts(
        [
            {
                "ts": 9_990.0,
                "accepted": True,
                "selected_for_batch": True,
                "rewarded": True,
                "env_name": "openmathinstruct",
                "window_n": 42,
            },
            {
                "ts": 9_980.0,
                "accepted": False,
                "reject_reason": "batch_filled",
                "env_name": "opencodeinstruct",
                "window_n": 42,
            },
            {
                "ts": 8_000.0,
                "accepted": True,
                "env_name": "openmathinstruct",
                "window_n": 41,
            },
        ],
        now=10_000.0,
    )
    assert summary["accepted_30m"] == 1
    assert summary["accepted_60m"] == 2
    assert summary["rejected_30m"] == summary["rejected_60m"] == 1
    assert summary["selected_30m"] == summary["rewarded_30m"] == 1
    assert summary["reason_counts_30m"] == {"batch_filled": 1}
    assert summary["by_environment"]["openmathinstruct"]["accepted_60m"] == 2


def test_validator_liveness_health_is_normalized_and_old_schema_clears_it(
    monkeypatch,
):
    monkeypatch.setattr(fleet.time, "time", lambda: 10_000.0)
    vs = fleet.ValidatorState()
    fleet._apply_validator_health(
        vs,
        {
            "status": "ok",
            "queue_depth_by_environment": {
                "openmathinstruct": 3,
                "opencodeinstruct": "2",
                "bad": -1,
            },
            "admission_workers_by_environment": {
                "openmathinstruct": 4,
                "opencodeinstruct": 2,
            },
            "proof_verification_inflight_by_environment": {
                "openmathinstruct": 1,
                "opencodeinstruct": 0,
            },
            "event_loop_lag_ms": {
                "p50": 4.266,
                "p95": 2708.269,
                "p99": 13019.382,
                "max": 31655.837,
                "mean": 99,
            },
            "endpoint_latency_ms": {
                "/health": {"p95": 23568.514, "p99": 28408.363},
                "/submit": {"p95": "98875.959", "p99": float("nan")},
                "/bad": {"p95": -1},
            },
            "admission_latency_ms_by_environment": {
                "opencodeinstruct": {
                    "admission_prepare_ms": {"p95": 5232.588, "p99": 6166.78},
                    "commit_lock_wait_ms": {"p95": 0.009, "p99": 0.01},
                    "total_ms": {"p95": 29891.944, "p99": 35462.133},
                },
                "malformed": "not-a-map",
            },
            "window_environments": {
                "openmathinstruct": {
                    "auction_seal_drain": {
                        "elapsed_seconds": 45.27,
                        "timed_out": False,
                        "queue_depth_at_snapshot": 0,
                        "inflight_workers_at_snapshot": 0,
                        "pending_reservations_at_snapshot": 0,
                        "inflight_reservations_at_snapshot": 0,
                    }
                },
                "opencodeinstruct": {
                    "auction_seal_drain": {
                        "elapsed_seconds": "bad",
                        "timed_out": "false",
                        "queue_depth_at_snapshot": True,
                    }
                },
            },
            "archive_queue_depth": 1,
            "archive_last_uploaded_window": 23864,
            "archive_last_enqueued_window": 23865,
            "archive_archives_enqueued_total": 2,
            "archive_enqueue_gaps_total": 1,
            "archive_last_enqueue_gap": {
                "expected_window": 23863,
                "observed_window": 23865,
            },
        },
    )

    assert vs.liveness_telemetry_reported is True
    assert vs.queue_depth_by_environment == {
        "openmathinstruct": 3,
        "opencodeinstruct": 2,
    }
    assert vs.event_loop_lag_ms["p99"] == 13019.382
    assert vs.endpoint_latency_ms["/submit"] == {"p95": 98875.959}
    assert "/bad" not in vs.endpoint_latency_ms
    assert vs.admission_latency_ms_by_environment["opencodeinstruct"][
        "commit_lock_wait_ms"
    ]["p99"] == 0.01
    assert vs.seal_drain_by_environment == {
        "openmathinstruct": {
            "elapsed_seconds": 45.27,
            "timed_out": False,
            "queue_depth_at_snapshot": 0,
            "inflight_workers_at_snapshot": 0,
            "pending_reservations_at_snapshot": 0,
            "inflight_reservations_at_snapshot": 0,
        }
    }
    assert vs.archive_continuity_reported is True
    assert vs.archive_last_enqueued_window == 23865
    assert vs.archive_last_enqueue_gap["expected_window"] == 23863

    # A subsequent old-schema sample must not leave stale future telemetry
    # behind or reinterpret absence as measured zeroes.
    fleet._apply_validator_health(vs, {"status": "ok"})
    assert vs.liveness_telemetry_reported is False
    assert vs.queue_depth_by_environment == {}
    assert vs.event_loop_lag_ms == {}
    assert vs.endpoint_latency_ms == {}
    assert vs.admission_latency_ms_by_environment == {}
    assert vs.seal_drain_by_environment == {}
    assert vs.archive_continuity_reported is False
    assert vs.archive_last_enqueued_window is None
    assert vs.archive_last_enqueue_gap is None


def test_explicit_null_checkpoint_clears_stale_revision_on_base_reset(monkeypatch):
    monkeypatch.setattr(fleet.time, "time", lambda: 10_000.0)
    old_revision = "a" * 40
    vs = fleet.ValidatorState(
        window=23618,
        checkpoint_n=42,
        checkpoint_repo_id="ReliquaryForge/qwen3.5-2b-reliquary-v2",
        checkpoint_revision=old_revision,
    )

    fleet._apply_validator_state(
        vs,
        {
            "state": "open",
            "window_n": 23619,
            "valid_submissions": 0,
            "checkpoint_n": 0,
            "checkpoint_repo_id": None,
            "checkpoint_revision": None,
            "randomness": "ab" * 32,
        },
        source="http",
    )

    assert vs.checkpoint_n == 0
    assert vs.checkpoint_repo_id == ""
    assert vs.checkpoint_revision == ""

    # A complete newer health identity advances checkpoint_n too, so the old
    # explicit-base state payload cannot win merely because /state is lagging.
    new_repo = "ReliquaryForge/qwen3.5-2b-reliquary-v3"
    new_revision = "b" * 40
    fleet._apply_validator_health(
        vs,
        {
            "status": "ok",
            "checkpoint_n": 1,
            "checkpoint_repo_id": new_repo,
            "checkpoint_revision": new_revision,
        },
    )
    assert vs.checkpoint_n == 1
    assert vs.checkpoint_repo_id == new_repo
    assert vs.checkpoint_revision == new_revision

    import fleet_web

    assert fleet_web._validator_model_identity(vs) == {
        "known": True,
        "kind": "validator_checkpoint",
        "checkpoint_n": 1,
        "repo": new_repo,
        "revision": new_revision,
        "source": "health",
    }

    # Sparse legacy health payloads preserve the current identity, while an
    # explicit null continues to describe the base reset authoritatively.
    vs.checkpoint_revision = old_revision
    fleet._apply_validator_health(vs, {"status": "ok"})
    assert vs.checkpoint_revision == old_revision
    fleet._apply_validator_health(
        vs,
        {
            "status": "ok",
            "checkpoint_repo_id": None,
            "checkpoint_revision": None,
        },
    )
    assert vs.checkpoint_repo_id == ""
    assert vs.checkpoint_revision == ""


def test_validator_identity_never_mixes_state_n_with_new_health_revision():
    import fleet_web

    old_repo = "ReliquaryForge/qwen3.5-2b-reliquary-v2"
    old_revision = "a" * 40
    new_repo = "ReliquaryForge/qwen3.5-2b-reliquary-v3"
    new_revision = "b" * 40
    vs = fleet.ValidatorState(
        checkpoint_n=42,
        checkpoint_repo_id=new_repo,
        checkpoint_revision=new_revision,
        state_raw={
            "checkpoint_n": 42,
            "checkpoint_repo_id": old_repo,
            "checkpoint_revision": old_revision,
        },
        # The current health schema has no checkpoint_n, so this is not an
        # atomic n42/new-revision identity and cannot be declared ready.
        health_raw={
            "checkpoint_repo_id": new_repo,
            "checkpoint_revision": new_revision,
        },
    )

    identity = fleet_web._validator_model_identity(vs)
    assert identity["known"] is False
    assert identity["kind"] == "unknown"
    assert identity["source"] == ""


def test_auction_verdict_summary_dedupes_admission_and_keeps_final_outcomes_distinct():
    winner = "a" * 64
    non_winner = "b" * 64
    proof_reject = "c" * 64
    summary = fleet.summarize_verdicts(
        [
            {
                "merkle_root": winner,
                "window_n": 42,
                "ts": 9_970.0,
                "accepted": True,
                "reason": "accepted",
                "accepted_into_pool": True,
            },
            # A duplicate admission record for one root is still one candidate.
            {
                "merkle_root": winner,
                "window_n": 42,
                "ts": 9_975.0,
                "accepted": True,
                "reason": "accepted",
                "accepted_into_pool": True,
            },
            {
                "merkle_root": winner,
                "window_n": 42,
                "ts": 9_980.0,
                "accepted": True,
                "reason": "accepted",
                "accepted_into_pool": True,
                "selected_for_batch": True,
                "rewarded": True,
            },
            {
                "merkle_root": non_winner,
                "window_n": 42,
                "ts": 9_985.0,
                "accepted": True,
                "reason": "accepted",
                "accepted_into_pool": True,
            },
            {
                "merkle_root": non_winner,
                "window_n": 42,
                "ts": 9_990.0,
                "accepted": True,
                "reason": "accepted",
                "accepted_into_pool": True,
                "selected_for_batch": False,
                "rewarded": False,
            },
            {
                "merkle_root": proof_reject,
                "window_n": 42,
                "ts": 9_992.0,
                "accepted": True,
                "reason": "accepted",
                "accepted_into_pool": True,
            },
            {
                "merkle_root": proof_reject,
                "window_n": 42,
                "ts": 9_995.0,
                "accepted": False,
                "reason": "grail_fail",
                "reject_reason": "grail_fail",
                "accepted_into_pool": True,
                "selected_for_batch": False,
                "rewarded": False,
            },
        ],
        now=10_000.0,
    )

    assert summary["accepted_30m"] == summary["accepted_60m"] == 3
    assert summary["rejected_30m"] == summary["rejected_60m"] == 0
    assert summary["finalized_30m"] == summary["finalized_60m"] == 3
    assert summary["final_rejected_30m"] == 1
    assert summary["final_reason_counts_30m"] == {"grail_fail": 1}
    assert summary["reason_counts_30m"] == {}
    assert summary["selected_30m"] == summary["selected_60m"] == 1
    assert summary["rewarded_30m"] == summary["rewarded_60m"] == 1


def test_auction_verdict_summary_preserves_fractional_paid_not_trained_emission():
    summary = fleet.summarize_verdicts(
        [
            {
                "merkle_root": "a" * 64,
                "window_n": 42,
                "ts": 9_980.0,
                "accepted": True,
                "accepted_into_pool": True,
                "selected_for_batch": True,
                "rewarded": True,
                "reward_amount": 0.0625,
                "env_name": "opencodeinstruct",
            },
            {
                "merkle_root": "b" * 64,
                "window_n": 42,
                "ts": 9_990.0,
                "accepted": True,
                "accepted_into_pool": True,
                "selected_for_batch": False,
                "rewarded": True,
                "reward_amount": 0.0625 / 3.0,
                "env_name": "opencodeinstruct",
            },
        ],
        now=10_000.0,
    )

    assert summary["selected_30m"] == 1
    assert summary["rewarded_30m"] == 2
    assert summary["rewarded_not_selected_30m"] == 1
    assert summary["fractional_rewarded_30m"] == 1
    assert summary["reward_amount_30m"] == pytest.approx(0.0625 * 4 / 3)
    assert summary["effective_full_slots_30m"] == pytest.approx(4 / 3)
    assert summary["reward_amount_observations_30m"] == 2
    by_env = summary["by_environment"]["opencodeinstruct"]
    assert by_env["rewarded_not_selected_30m"] == 1
    assert by_env["effective_full_slots_30m"] == pytest.approx(4 / 3)


def test_auction_verdict_identity_preserves_same_root_across_windows():
    root = "Ab" * 32
    summary = fleet.summarize_verdicts(
        [
            {
                "merkle_root": root,
                "window_n": 40,
                "ts": 9_970.0,
                "accepted": True,
                "reason": "accepted",
                "accepted_into_pool": True,
            },
            {
                "merkle_root": root.lower(),
                "window_n": 40,
                "ts": 9_975.0,
                "accepted": True,
                "reason": "accepted",
                "accepted_into_pool": True,
                "selected_for_batch": True,
                "rewarded": True,
            },
            {
                "merkle_root": root.lower(),
                "window_n": 41,
                "ts": 9_980.0,
                "accepted": True,
                "reason": "accepted",
                "accepted_into_pool": True,
            },
            {
                "merkle_root": root,
                "window_n": 41,
                "ts": 9_985.0,
                "accepted": True,
                "reason": "accepted",
                "accepted_into_pool": True,
                "selected_for_batch": False,
                "rewarded": False,
            },
        ],
        now=10_000.0,
    )

    assert summary["accepted_30m"] == summary["accepted_60m"] == 2
    assert summary["finalized_30m"] == summary["finalized_60m"] == 2
    assert summary["selected_30m"] == summary["selected_60m"] == 1
    assert summary["rewarded_30m"] == summary["rewarded_60m"] == 1


def test_fetch_validator_polls_state_health_and_each_hotkey_verdict(monkeypatch):
    monkeypatch.setattr(fleet, "OUR_SS58", {OUR_HOTKEY: "h100-reserve1"})
    monkeypatch.setattr(fleet.time, "time", lambda: 20_000.0)
    calls: list[str] = []

    def fake_http(path: str, *, timeout: float = 0.0):
        calls.append(path)
        if path == "state":
            return {"state": "open", "window_n": 77, "valid_submissions": 3}
        if path == "health":
            return {
                "status": "ok",
                "current_validator_state": "open",
                "current_window_n": 77,
                "valid_submissions_count": 3,
                "window_environments": {},
                "training_accumulator_targets": {},
            }
        if path.startswith(f"verdicts/{OUR_HOTKEY}?"):
            return {
                "verdicts": [
                    {
                        "ts": 19_990.0,
                        "accepted": True,
                        "env_name": "openmathinstruct",
                        "window_n": 77,
                    }
                ]
            }
        raise AssertionError(path)

    monkeypatch.setattr(fleet, "_http_json", fake_http)
    vs = fleet.ValidatorState()
    fleet.fetch_validator(vs)

    assert calls[:2] == ["state", "health"]
    assert any(path.startswith(f"verdicts/{OUR_HOTKEY}?since=") for path in calls)
    assert vs.last_fetch_at == vs.health_last_fetch_at == vs.verdicts_last_fetch_at == 20_000.0
    assert vs.verdicts_by_hotkey[OUR_HOTKEY]["accepted_30m"] == 1
    assert not (vs.error or vs.health_error or vs.verdicts_error)


def test_direct_verdicts_override_best_effort_event_counters(monkeypatch):
    now = 25_000.0
    monkeypatch.setattr(fleet.time, "time", lambda: now)
    monkeypatch.setattr(fleet, "OUR_SS58", {OUR_HOTKEY: "h100-reserve1"})
    with fleet._validator_events_lock:
        fleet._validator_events.clear()
    vs = fleet.ValidatorState(
        verdicts_last_fetch_at=now - 1,
        verdicts_by_hotkey={
            OUR_HOTKEY: {
                "accepted_30m": 3,
                "accepted_60m": 4,
                "rejected_30m": 2,
                "rejected_60m": 5,
                "reason_counts_30m": {"window_mismatch": 1, "grail_fail": 1},
                "last": {"reason": "grail_fail", "window_n": 88, "ts": now - 2},
            }
        },
    )
    box = _box(
        proc_alive=True,
        miner_state="submitted",
        miner_window=88,
        miner_inflight=1,
        miner_ready=0,
        miner_submitted_this_win=1,
    )
    fleet.recompute_validator_acpts([box], vs)

    assert box.acceptance_source == "verdicts"
    assert (box.acpt_30m, box.acpt_60m) == (3, 4)
    assert (box.rej_30m, box.final_reject_60m) == (2, 5)
    assert (box.window_mismatch_30m, box.grail_fail_30m) == (1, 1)
    assert (box.last_final_reason, box.last_final_window) == ("grail_fail", 88)
    assert box.miner_state == "waiting"
    assert box.miner_inflight == 0
    assert box.miner_ready == 1
    assert box.miner_submitted_this_win == 0


def test_current_validator_window_replaces_completed_submit_window(monkeypatch):
    monkeypatch.setattr(fleet, "OUR_SS58", {OUR_HOTKEY: "h100-reserve1"})
    box = _box(
        proc_alive=True,
        miner_state="proving",
        miner_window=85,
        miner_inflight=1,
        miner_ready=0,
        miner_submitted_this_win=0,
    )
    vs = fleet.ValidatorState(state="open", window=86)

    fleet.recompute_validator_acpts([box], vs)

    assert box.miner_state == "proving"
    assert box.miner_window == 86
    assert box.miner_inflight == 1


def test_r2_parser_preserves_multi_environment_rewards_and_telemetry(monkeypatch):
    monkeypatch.setattr(fleet, "OUR_SS58", {OUR_HOTKEY: "h100-reserve1"})
    payload = {
        "environments": ["openmathinstruct", "opencodeinstruct"],
        "batch": [
            {
                "hotkey": OUR_HOTKEY,
                "env_name": "openmathinstruct",
                "prompt_idx": 1,
                "response_time": 3.5,
                "selected_for_batch": True,
                "rewarded": True,
                "reward_amount": 0.2,
                "merkle_root": "ours-root",
            },
            {
                "hotkey": OTHER_HOTKEY,
                "env_name": "opencodeinstruct",
                "prompt_idx": 2,
                "response_time": 4.0,
                "selected_for_batch": True,
                "rewarded": True,
                "reward_amount": 0.7,
            },
        ],
        "runners_up": [
            {
                "hotkey": OUR_HOTKEY,
                "env_name": "opencodeinstruct",
                "prompt_idx": 3,
                "selected_for_batch": False,
                "rewarded": True,
                "reward_amount": 0.1,
                "reject_reason": "boundary_fairness",
            }
        ],
        "rejected": [
            {
                "hotkey": OUR_HOTKEY,
                "env_name": "opencodeinstruct",
                "prompt_idx": 4,
                "reject_stage": "pool",
                "reject_reason": "batch_filled",
            }
        ],
        "rewards_by_hotkey": {OUR_HOTKEY: 0.3, OTHER_HOTKEY: 0.7},
        "rewarded_but_not_selected_by_hotkey": {OUR_HOTKEY: 0.1},
        "training_quarantine": {"active": False},
        "difficulty_auction_shadow": {"enabled": True, "environment": "openmathinstruct"},
        "server_reject_summary": {"by_reason": {"batch_filled": 1}},
        "logical_group_dedup": {"duplicate_rejects": 0},
        "training_accumulator": {"targets": {"openmathinstruct": 8, "opencodeinstruct": 8}},
    }
    window = fleet._parse_window_object(
        "reliquary/dataset/window-23391.json", json.dumps(payload).encode()
    )

    assert window is not None
    assert window.reward_data_present is True
    assert window.environments == ["openmathinstruct", "opencodeinstruct"]
    assert window.environment_counts["openmathinstruct"]["batch"] == 1
    assert window.environment_counts["opencodeinstruct"]["runners_up"] == 1
    assert window.environment_counts["opencodeinstruct"]["rejected"] == 1
    assert window.ours_by_environment == {"openmathinstruct": 1, "opencodeinstruct": 0}
    assert window.rt_first == 3.5
    assert window.rt_first_by_environment == {
        "openmathinstruct": 3.5,
        "opencodeinstruct": 4.0,
    }
    assert window.reward_ours_by_environment == {
        "openmathinstruct": 0.2,
        "opencodeinstruct": 0.1,
    }
    assert window.reward_ours_by_environment_exact is True
    assert window.runners_up[0]["rewarded"] is True
    assert window.rejected[0]["reason"] == "batch_filled"
    assert window.rewards_by_hotkey[OUR_HOTKEY] == 0.3
    assert window.rewarded_but_not_selected_by_hotkey == {OUR_HOTKEY: 0.1}
    assert window.difficulty_auction_shadow["enabled"] is True
    assert window.training_accumulator["targets"]["opencodeinstruct"] == 8


def test_current_archive_schema_uses_per_env_minima_exact_ranks_and_reward_vectors(
    monkeypatch,
):
    """Mirror the deployed R2 row shape (reward_amount is intentionally absent)."""
    monkeypatch.setattr(fleet, "OUR_SS58", {OUR_HOTKEY: "h100-reserve1"})
    payload = {
        "window_start": 23499,
        "environments": ["openmathinstruct", "opencodeinstruct"],
        "batch": [
            {
                "hotkey": OUR_HOTKEY,
                "env_name": "openmathinstruct",
                "response_time": 82.0,
                "canonical_rank": 2,
                "rewarded": True,
                "selected_for_batch": True,
                "reward_vector": "00111111",
            },
            {
                "hotkey": OUR_HOTKEY,
                "env_name": "openmathinstruct",
                "response_time": 78.7,
                "canonical_rank": 1,
                "rewarded": True,
                "selected_for_batch": True,
                "reward_vector": "11001101",
            },
            {
                "hotkey": OUR_HOTKEY,
                "env_name": "opencodeinstruct",
                "response_time": 50.5,
                "canonical_rank": 1,
                "rewarded": True,
                "selected_for_batch": True,
                "reward_vector": [0, 1, 1, 0, 1, 0, 0, 1],
            },
            {
                "hotkey": OUR_HOTKEY,
                "env_name": "opencodeinstruct",
                "response_time": 54.8,
                "canonical_rank": 8,
                "rewarded": True,
                "selected_for_batch": True,
            },
        ],
        "runners_up": [],
        "rejected": [],
        "rewards_by_hotkey": {OUR_HOTKEY: 0.5},
    }
    window = fleet._parse_window_object(
        "reliquary/dataset/window-23499.json.gz",
        fleet.gzip.compress(json.dumps(payload).encode()),
    )

    assert window is not None
    assert window.rt_first == 50.5
    assert window.rt_first_by_environment == {
        "openmathinstruct": 78.7,
        "opencodeinstruct": 50.5,
    }
    assert window.reward_ours_by_environment == {}
    assert window.reward_ours_by_environment_exact is False

    ranks = fleet.aggregate_slot_ranks_by_environment([window])["h100-reserve1"]
    assert ranks["openmathinstruct"][:2] == [1, 1]
    assert ranks["opencodeinstruct"][0] == 1
    assert ranks["opencodeinstruct"][7] == 1
    assert fleet.aggregate_slot_ranks([window])["h100-reserve1"] == [2, 1, 0, 0, 0, 0, 0, 1]

    k_by_env = fleet.aggregate_k_histogram_by_environment([window])
    assert k_by_env == {
        "openmathinstruct": {6: 1, 5: 1},
        "opencodeinstruct": {4: 1, "unknown": 1},
    }
    assert fleet.aggregate_k_histogram([window]) == {
        6: 1, 5: 1, 4: 1, "unknown": 1,
    }


def test_current_archive_attributes_unique_rewarded_environment_without_row_amount(
    monkeypatch,
):
    """Mirror w23691: the aggregate and sole rewarded env prove attribution."""
    monkeypatch.setattr(fleet, "OUR_SS58", {OUR_HOTKEY: "h100-reserve1"})
    payload = {
        "environments": ["openmathinstruct", "opencodeinstruct"],
        "batch": [
            {
                "hotkey": OTHER_HOTKEY,
                "env_name": "openmathinstruct",
                "response_time": 31.2,
                "selected_for_batch": True,
                "rewarded": True,
            },
            {
                "hotkey": OUR_HOTKEY,
                "env_name": "opencodeinstruct",
                "response_time": 179.1,
                "selected_for_batch": True,
                "rewarded": True,
            },
        ],
        "runners_up": [],
        "rejected": [],
        "rewards_by_hotkey": {OUR_HOTKEY: 0.0625, OTHER_HOTKEY: 0.9375},
    }

    window = fleet._parse_window_object(
        "reliquary/dataset/window-23691.json", json.dumps(payload).encode()
    )

    assert window is not None
    assert window.reward_ours == 0.0625
    assert window.ours_by_environment == {
        "openmathinstruct": 0,
        "opencodeinstruct": 1,
    }
    assert window.reward_ours_by_environment == {"opencodeinstruct": 0.0625}
    assert window.reward_ours_by_environment_exact is True


def test_exact_reward_ema_conserves_emission_and_includes_runner_rewards():
    dense_batch = [{"hotkey": OTHER_HOTKEY} for _ in range(16)]
    windows = [
        _window(1, {OTHER_HOTKEY: 0.6, RUNNER_HOTKEY: 0.4}, dense_batch),
        _window(2, {OTHER_HOTKEY: 0.2, RUNNER_HOTKEY: 0.8}, dense_batch),
    ]
    rows = {row["hotkey"]: row for row in fleet.compute_ema_leaderboard(windows)}
    alpha = fleet.EMA_ALPHA

    assert math.isclose(
        rows[RUNNER_HOTKEY]["ema"],
        alpha * 0.8 + (1.0 - alpha) * alpha * 0.4,
    )
    assert rows[RUNNER_HOTKEY]["slot_count_total"] == 0
    assert rows[OTHER_HOTKEY]["slot_share_72"] == 1.0
    assert math.isclose(sum(row["ema"] for row in rows.values()), alpha * (2.0 - alpha))


def test_legacy_cache_rows_without_rewards_are_not_used_for_ema():
    row = {
        "n": 12,
        "ours": 1,
        "total_batch": 1,
        "rt_first": 1.0,
        "rejects": {},
        "batch": [{"hotkey": OUR_HOTKEY}],
        "rewards_by_hotkey": {OUR_HOTKEY: 1.0},
        "reward_data_present": True,
    }
    legacy = fleet._window_from_cache_row(row, legacy=True)
    modern = fleet._window_from_cache_row(row, legacy=False)

    assert legacy.reward_data_present is False
    assert modern.reward_data_present is True
    assert fleet.compute_ema_leaderboard([legacy]) == []
    modern_row = fleet._window_cache_row(modern)
    assert modern_row["rewards_by_hotkey"] == {OUR_HOTKEY: 1.0}
    assert "window_status" not in modern_row
    assert "terminal_data_present" not in modern_row


def test_r2_lifecycle_legacy_completed_aborted_and_unknown_are_typed(monkeypatch):
    monkeypatch.setattr(fleet, "OUR_SS58", {OUR_HOTKEY: "h100-reserve1"})
    legacy_payload = {
        "batch": [{"hotkey": OUR_HOTKEY, "rewarded": True}],
        "runners_up": [],
        "rejected": [],
        "rewards_by_hotkey": {OUR_HOTKEY: 1.0},
    }
    legacy = fleet._parse_window_object(
        "reliquary/dataset/window-90.json", json.dumps(legacy_payload).encode()
    )
    assert legacy is not None
    assert legacy.window_status == "completed"
    assert legacy.terminal_data_present is True
    assert legacy.reward_data_present is True
    assert legacy.lifecycle_explicit is False

    completed_payload = {
        **legacy_payload,
        "window_start": 91,
        "archive_schema_version": 2,
        "window_status": "completed",
        "failure_stage": None,
        "failure_type": None,
        "auction_seal_drain_by_environment": {
            "openmathinstruct": {
                "elapsed_seconds": 45.25,
                "timed_out": False,
                "queue_depth_at_snapshot": 0,
                "inflight_workers_at_snapshot": 0,
                "pending_reservations_at_snapshot": 0,
                "inflight_reservations_at_snapshot": 0,
            }
        },
    }
    completed = fleet._parse_window_object(
        "reliquary/dataset/window-91.json",
        json.dumps(completed_payload).encode(),
    )
    assert completed is not None
    assert completed.window_status == "completed"
    assert completed.reward_data_present is True
    assert completed.lifecycle_explicit is True
    assert completed.archive_schema_version == 2
    assert completed.auction_seal_drain_by_environment == {
        "openmathinstruct": {
            "elapsed_seconds": 45.25,
            "timed_out": False,
            "queue_depth_at_snapshot": 0,
            "inflight_workers_at_snapshot": 0,
            "pending_reservations_at_snapshot": 0,
            "inflight_reservations_at_snapshot": 0,
        }
    }

    tombstone_payload = {
        "window_start": 92,
        "archive_schema_version": 2,
        "window_status": "aborted",
        "failure_stage": "seal",
        "failure_type": "AdmissionDrainTimeout",
        # Contradictory material is never allowed into rewards or EMA.
        "batch": [{"hotkey": OUR_HOTKEY, "rewarded": True}],
        "rewards_by_hotkey": {OUR_HOTKEY: 1.0},
        "training_accumulator": {"trained": True},
    }
    tombstone = fleet._parse_window_object(
        "reliquary/dataset/window-92.json",
        json.dumps(tombstone_payload).encode(),
    )
    assert tombstone is not None
    assert tombstone.window_status == "aborted"
    assert tombstone.terminal_data_present is True
    assert tombstone.reward_data_present is False
    assert tombstone.failure_stage == "seal"
    assert tombstone.failure_type == "AdmissionDrainTimeout"
    assert tombstone.archive_schema_version == 2
    assert tombstone.batch == []
    assert tombstone.rewards_by_hotkey == {}
    assert fleet.compute_ema_leaderboard([tombstone]) == []

    assert fleet._parse_window_object(
        "reliquary/dataset/window-93.json",
        json.dumps({"window_start": 93, "window_status": "partial"}).encode(),
    ) is None
    assert fleet._parse_window_object(
        "reliquary/dataset/window-94.json",
        json.dumps({"window_start": 94, "window_status": "completed"}).encode(),
    ) is None


def test_r2_contiguous_tombstone_closes_continuity_without_reward_or_refetch(
    monkeypatch,
):
    class FakeR2:
        def list_objects_v2(self, **kwargs):
            raise AssertionError("health-indexed steady state must not LIST")

        def get_object(self, **kwargs):
            raise AssertionError("terminal tombstones and completed rows are immutable")

    completed_100 = _window(100, {OTHER_HOTKEY: 1.0})
    tombstone_101 = fleet.WindowSummary(
        n=101,
        ours=0,
        total_batch=0,
        rt_first=0.0,
        rejects={},
        window_status="aborted",
        terminal_data_present=True,
        reward_data_present=False,
        failure_stage="seal",
        failure_type="UnexpectedSealError",
        lifecycle_explicit=True,
    )
    completed_102 = _window(102, {OTHER_HOTKEY: 1.0})
    monkeypatch.setattr(
        fleet,
        "_window_cache",
        {100: completed_100, 101: tombstone_101, 102: completed_102},
    )
    monkeypatch.setattr(fleet, "r2", lambda: FakeR2())
    monkeypatch.setattr(
        fleet,
        "_http_json",
        lambda *args, **kwargs: {
            "archive_last_uploaded_window": 102,
            "archive_queue_depth": 0,
        },
    )
    monkeypatch.setenv("R2_BUCKET", "test-bucket")

    windows = fleet.fetch_recent_windows(3)
    assert [window.n for window in windows] == [102, 101, 100]
    status = fleet.r2_status()
    assert status["coverage_complete"] is True
    assert status["coverage"]["terminal_present"] == 3
    assert status["coverage"]["reward_expected"] == 2
    assert status["coverage"]["exact_rewards"] == 2
    assert status["coverage"]["aborted_windows"] == [101]
    assert status["coverage"]["missing_windows"] == []
    assert status["coverage"]["reward_missing_windows"] == []
    assert len(fleet.compute_ema_leaderboard(windows)) == 1


def test_lifecycle_cache_round_trip_keeps_tombstone_terminal_only(
    monkeypatch,
    tmp_path,
):
    cache_path = tmp_path / "window_cache_v2.json.gz"
    completed = _window(110, {OTHER_HOTKEY: 1.0})
    tombstone = fleet.WindowSummary(
        n=111,
        ours=0,
        total_batch=0,
        rt_first=0.0,
        rejects={},
        window_status="aborted",
        terminal_data_present=True,
        reward_data_present=False,
        failure_stage="archive",
        failure_type="UnexpectedWindowError",
        lifecycle_explicit=True,
    )
    monkeypatch.setattr(fleet, "_WINDOW_CACHE_FILE", str(cache_path))
    monkeypatch.setattr(
        fleet,
        "_LEGACY_WINDOW_CACHE_FILE",
        str(tmp_path / "missing-legacy.json.gz"),
    )
    monkeypatch.setattr(fleet, "_window_cache", {110: completed, 111: tombstone})

    fleet.window_cache_save()
    saved = json.loads(fleet.gzip.decompress(cache_path.read_bytes()))
    assert saved["schema_version"] == fleet._WINDOW_CACHE_LIFECYCLE_SCHEMA
    rows = {row["n"]: row for row in saved["rows"]}
    assert "window_status" not in rows[110]
    assert rows[111]["window_status"] == "aborted"
    assert rows[111]["failure_stage"] == "archive"

    fleet._window_cache.clear()
    assert fleet.window_cache_load() == 2
    assert fleet._window_cache[110].reward_data_present is True
    restored = fleet._window_cache[111]
    assert restored.window_status == "aborted"
    assert restored.terminal_data_present is True
    assert restored.reward_data_present is False
    assert restored.failure_type == "UnexpectedWindowError"


def test_r2_uses_validator_archive_window_for_exact_numeric_keys(monkeypatch):
    class FakeR2:
        def __init__(self):
            self.list_calls = []
            self.get_calls = []

        def list_objects_v2(self, **kwargs):
            self.list_calls.append(kwargs)
            raise AssertionError("health-indexed steady state must not LIST")

        def get_object(self, **kwargs):
            self.get_calls.append(kwargs["Key"])
            payload = {
                "batch": [],
                "rejected": [],
                "rewards_by_hotkey": {OTHER_HOTKEY: 1.0},
            }
            return {"Body": BytesIO(fleet.gzip.compress(json.dumps(payload).encode()))}

    cached = _window(100, {OTHER_HOTKEY: 1.0})
    monkeypatch.setattr(fleet, "_window_cache", {100: cached})
    fake = FakeR2()
    monkeypatch.setattr(fleet, "r2", lambda: fake)
    monkeypatch.setattr(
        fleet,
        "_http_json",
        lambda *args, **kwargs: {"archive_last_uploaded_window": 102},
    )
    monkeypatch.setenv("R2_BUCKET", "test-bucket")
    monkeypatch.setattr(fleet, "window_cache_save", lambda: None)

    windows = fleet.fetch_recent_windows(3)

    assert [window.n for window in windows] == [102, 101, 100]
    assert fake.list_calls == []
    assert set(fake.get_calls) == {
        "reliquary/dataset/window-102.json.gz",
        "reliquary/dataset/window-101.json.gz",
    }
    status = fleet.r2_status()
    assert status["discovery_source"] == "validator_health"
    assert status["coverage"] == {
        "requested": 3,
        "numeric_expected": 3,
        "archived_expected": 3,
        "expected": 3,
        "cached": 3,
        "exact_rewards": 3,
        "absent_windows": [],
        "missing_windows": [],
        "complete": True,
        "source": "validator_health",
    }


def test_r2_health_index_classifies_confirmed_curl_404_as_absent(monkeypatch):
    class SigningR2:
        def list_objects_v2(self, **kwargs):
            raise AssertionError("health-indexed steady state must not LIST")

        def generate_presigned_url(self, operation, **kwargs):
            assert operation == "get_object"
            assert kwargs["Params"]["Key"].endswith("window-101.json.gz")
            return "https://r2.invalid/missing-signed-object"

    def missing_curl(*args, **kwargs):
        return SimpleNamespace(
            returncode=22,
            stdout=b"",
            stderr=b"curl: (22) The requested URL returned error: 404\n",
        )

    monkeypatch.setattr(
        fleet,
        "_window_cache",
        {
            100: _window(100, {OTHER_HOTKEY: 1.0}),
            102: _window(102, {OTHER_HOTKEY: 1.0}),
        },
    )
    monkeypatch.setattr(fleet, "r2", lambda: SigningR2())
    monkeypatch.setattr(fleet.subprocess, "run", missing_curl)
    monkeypatch.setattr(
        fleet,
        "_http_json",
        lambda *args, **kwargs: {
            "archive_last_uploaded_window": 102,
            "archive_queue_depth": 0,
            "archive_queue_oldest_window": None,
        },
    )
    monkeypatch.setenv("R2_BUCKET", "test-bucket")

    windows = fleet.fetch_recent_windows(3)
    status = fleet.r2_status()

    assert [window.n for window in windows] == [102, 100]
    assert status["error"] == ""
    assert status["coverage_complete"] is True
    assert status["coverage"] == {
        "requested": 3,
        "numeric_expected": 3,
        "archived_expected": 2,
        "expected": 3,
        "cached": 2,
        "exact_rewards": 2,
        "absent_windows": [101],
        "missing_windows": [],
        "complete": True,
        "source": "validator_health",
    }


@pytest.mark.parametrize(
    ("failure_mode", "error_fragment"),
    [
        ("timeout", "w101:TimeoutError"),
        ("forbidden", "w101:Forbidden"),
        ("parse", "w101:parse"),
    ],
)
def test_r2_non_absence_failures_remain_strict(
    monkeypatch, failure_mode, error_fragment
):
    class Forbidden(Exception):
        response = {
            "Error": {"Code": "AccessDenied"},
            "ResponseMetadata": {"HTTPStatusCode": 403},
        }

    class FakeR2:
        def get_object(self, **kwargs):
            if failure_mode == "timeout":
                raise TimeoutError("R2 timeout")
            if failure_mode == "forbidden":
                raise Forbidden("R2 denied")
            return {"Body": BytesIO(b"not-a-gzip-archive")}

    monkeypatch.setattr(
        fleet,
        "_window_cache",
        {
            100: _window(100, {OTHER_HOTKEY: 1.0}),
            102: _window(102, {OTHER_HOTKEY: 1.0}),
        },
    )
    monkeypatch.setattr(fleet, "r2", lambda: FakeR2())
    monkeypatch.setattr(
        fleet,
        "_http_json",
        lambda *args, **kwargs: {
            "archive_last_uploaded_window": 102,
            "archive_queue_depth": 0,
        },
    )
    monkeypatch.setenv("R2_BUCKET", "test-bucket")

    windows = fleet.fetch_recent_windows(3)
    status = fleet.r2_status()

    assert [window.n for window in windows] == [102, 100]
    assert error_fragment in status["error"]
    assert status["coverage_complete"] is False
    assert status["coverage"]["absent_windows"] == []
    assert status["coverage"]["missing_windows"] == [101]


def test_r2_queue_pending_404_remains_warming_and_incomplete(monkeypatch):
    class NoSuchKey(Exception):
        response = {
            "Error": {"Code": "NoSuchKey"},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        }

    class FakeR2:
        def get_object(self, **kwargs):
            raise NoSuchKey(kwargs["Key"])

    monkeypatch.setattr(
        fleet,
        "_window_cache",
        {
            100: _window(100, {OTHER_HOTKEY: 1.0}),
            102: _window(102, {OTHER_HOTKEY: 1.0}),
        },
    )
    monkeypatch.setattr(fleet, "r2", lambda: FakeR2())
    monkeypatch.setattr(
        fleet,
        "_http_json",
        lambda *args, **kwargs: {
            "archive_last_uploaded_window": 102,
            "archive_queue_depth": 1,
            "archive_queue_oldest_window": 101,
        },
    )
    monkeypatch.setenv("R2_BUCKET", "test-bucket")

    fleet.fetch_recent_windows(3)
    status = fleet.r2_status()

    assert status["error"] == "reward cache warming:1 remaining"
    assert status["coverage_complete"] is False
    assert status["coverage"]["absent_windows"] == []
    assert status["coverage"]["missing_windows"] == [101]


def test_r2_latest_advertised_archive_404_remains_unresolved(monkeypatch):
    class NoSuchKey(Exception):
        response = {"Error": {"Code": "NoSuchKey"}}

    class FakeR2:
        def get_object(self, **kwargs):
            raise NoSuchKey(kwargs["Key"])

    monkeypatch.setattr(
        fleet,
        "_window_cache",
        {
            100: _window(100, {OTHER_HOTKEY: 1.0}),
            101: _window(101, {OTHER_HOTKEY: 1.0}),
        },
    )
    monkeypatch.setattr(fleet, "r2", lambda: FakeR2())
    monkeypatch.setattr(
        fleet,
        "_http_json",
        lambda *args, **kwargs: {
            "archive_last_uploaded_window": 102,
            "archive_queue_depth": 0,
        },
    )
    monkeypatch.setenv("R2_BUCKET", "test-bucket")

    fleet.fetch_recent_windows(3)
    status = fleet.r2_status()

    assert status["error"] == "reward cache warming:1 remaining"
    assert status["coverage_complete"] is False
    assert status["coverage"]["absent_windows"] == []
    assert status["coverage"]["missing_windows"] == [102]


def test_r2_exhaustive_list_marks_interior_archive_gap_complete(monkeypatch):
    class FakeR2:
        def list_objects_v2(self, **kwargs):
            return {
                "Contents": [
                    {"Key": "reliquary/dataset/window-100.json.gz"},
                    {"Key": "reliquary/dataset/window-102.json.gz"},
                ],
                "IsTruncated": False,
            }

        def get_object(self, **kwargs):
            raise AssertionError("exact cached archives must not be fetched")

    monkeypatch.setattr(
        fleet,
        "_window_cache",
        {
            100: _window(100, {OTHER_HOTKEY: 1.0}),
            102: _window(102, {OTHER_HOTKEY: 1.0}),
        },
    )
    monkeypatch.setattr(fleet, "r2", lambda: FakeR2())
    monkeypatch.setattr(
        fleet,
        "_http_json",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError()),
    )
    monkeypatch.setenv("R2_BUCKET", "test-bucket")

    windows = fleet.fetch_recent_windows(3, allow_list_fallback=True)
    status = fleet.r2_status()

    assert [window.n for window in windows] == [102, 100]
    assert status["error"] == ""
    assert status["coverage_complete"] is True
    assert status["coverage"]["numeric_expected"] == 3
    assert status["coverage"]["archived_expected"] == 2
    assert status["coverage"]["absent_windows"] == [101]
    assert status["coverage"]["missing_windows"] == []


def test_r2_absent_window_is_fetched_when_later_backfilled(monkeypatch):
    class NoSuchKey(Exception):
        response = {"Error": {"Code": "NoSuchKey"}}

    class FakeR2:
        available = False
        get_calls = 0

        def get_object(self, **kwargs):
            self.get_calls += 1
            if not self.available:
                raise NoSuchKey(kwargs["Key"])
            payload = {
                "batch": [],
                "rejected": [],
                "rewards_by_hotkey": {OTHER_HOTKEY: 1.0},
            }
            return {
                "Body": BytesIO(
                    fleet.gzip.compress(json.dumps(payload).encode())
                )
            }

    fake = FakeR2()
    monkeypatch.setattr(
        fleet,
        "_window_cache",
        {
            100: _window(100, {OTHER_HOTKEY: 1.0}),
            102: _window(102, {OTHER_HOTKEY: 1.0}),
        },
    )
    monkeypatch.setattr(fleet, "r2", lambda: fake)
    monkeypatch.setattr(
        fleet,
        "_http_json",
        lambda *args, **kwargs: {
            "archive_last_uploaded_window": 102,
            "archive_queue_depth": 0,
        },
    )
    monkeypatch.setattr(fleet, "window_cache_save", lambda: None)
    monkeypatch.setenv("R2_BUCKET", "test-bucket")

    first = fleet.fetch_recent_windows(3)
    assert [window.n for window in first] == [102, 100]
    assert fleet.r2_status()["coverage"]["absent_windows"] == [101]

    fake.available = True
    second = fleet.fetch_recent_windows(3)
    status = fleet.r2_status()

    assert fake.get_calls == 2
    assert [window.n for window in second] == [102, 101, 100]
    assert status["error"] == ""
    assert status["coverage_complete"] is True
    assert status["coverage"]["archived_expected"] == 3
    assert status["coverage"]["absent_windows"] == []
    assert status["coverage"]["missing_windows"] == []


def test_r2_fallback_exhaustively_lists_through_lexical_wrap(monkeypatch):
    class FakeR2:
        def __init__(self):
            self.list_calls = []

        def list_objects_v2(self, **kwargs):
            self.list_calls.append(kwargs)
            if "ContinuationToken" not in kwargs:
                return {
                    "Contents": [
                        {"Key": "reliquary/dataset/window-100.json.gz"},
                        {"Key": "reliquary/dataset/window-101.json.gz"},
                        {"Key": "reliquary/dataset/window-9.json.gz"},
                    ],
                    "IsTruncated": True,
                    "NextContinuationToken": "after-wrap",
                }
            assert kwargs["ContinuationToken"] == "after-wrap"
            return {
                "Contents": [
                    {"Key": "reliquary/dataset/window-102.json.gz"},
                ],
                "IsTruncated": False,
            }

    cache = {
        n: _window(n, {OTHER_HOTKEY: 1.0})
        for n in (100, 101, 102)
    }
    monkeypatch.setattr(fleet, "_window_cache", cache)
    fake = FakeR2()
    monkeypatch.setattr(fleet, "r2", lambda: fake)
    monkeypatch.setattr(
        fleet, "_http_json", lambda *args, **kwargs: (_ for _ in ()).throw(OSError())
    )
    monkeypatch.setenv("R2_BUCKET", "test-bucket")

    windows = fleet.fetch_recent_windows(3, allow_list_fallback=True)

    assert [window.n for window in windows] == [102, 101, 100]
    assert len(fake.list_calls) == 2
    assert "StartAfter" not in fake.list_calls[0]
    assert fleet.r2_status()["coverage_complete"] is True
    assert fleet.r2_status()["discovery_source"] == "r2_list_exhaustive"


def test_presigned_r2_transport_has_hard_deadline_and_parses_list(monkeypatch):
    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <IsTruncated>true</IsTruncated>
          <NextContinuationToken>next-page</NextContinuationToken>
          <Contents><Key>reliquary%2Fdataset%2Fwindow-123.json.gz</Key></Contents>
        </ListBucketResult>"""
        return SimpleNamespace(returncode=0, stdout=xml, stderr=b"")

    class SigningClient:
        def generate_presigned_url(self, operation, **kwargs):
            assert operation == "list_objects_v2"
            assert kwargs["ExpiresIn"] == 60
            return "https://r2.invalid/signed?secret=hidden"

    monkeypatch.setattr(fleet.subprocess, "run", fake_run)
    response = fleet._r2_list_objects(
        SigningClient(),
        {"Bucket": "bucket", "Prefix": "reliquary/dataset/window-", "MaxKeys": 500},
    )

    assert response["Contents"] == [
        {"Key": "reliquary/dataset/window-123.json.gz"}
    ]
    assert response["IsTruncated"] is True
    assert response["NextContinuationToken"] == "next-page"
    args, kwargs = calls[0]
    assert "https://r2.invalid" not in args
    assert "--max-time" in args
    assert kwargs["timeout"] == 12
    assert b"secret=hidden" in kwargs["input"]


def test_v2_cache_migrates_from_legacy_without_claiming_exact_rewards(
    monkeypatch, tmp_path
):
    legacy_path = tmp_path / "window_cache.json.gz"
    v2_path = tmp_path / "window_cache_v2.json.gz"
    row = {
        "n": 50,
        "ours": 0,
        "total_batch": 1,
        "rt_first": 1.0,
        "rejects": {},
        "reward_ours": 0.0,
        "reward_total": 1.0,
        "reward_ours_by_environment": {
            "openmathinstruct": 0.0,
            "opencodeinstruct": 0.0,
        },
        "batch": [{"hotkey": OTHER_HOTKEY}],
    }
    legacy_path.write_bytes(fleet.gzip.compress(json.dumps([row]).encode()))
    monkeypatch.setattr(fleet, "_WINDOW_CACHE_FILE", str(v2_path))
    monkeypatch.setattr(fleet, "_LEGACY_WINDOW_CACHE_FILE", str(legacy_path))
    monkeypatch.setattr(fleet, "_window_cache", {})

    assert fleet.window_cache_load() == 1
    assert fleet._window_cache[50].reward_data_present is False
    assert fleet._window_cache[50].reward_ours_by_environment == {}
    assert fleet._window_cache[50].reward_ours_by_environment_exact is False
    fleet.window_cache_save()

    saved = json.loads(fleet.gzip.decompress(v2_path.read_bytes()))
    assert saved["schema_version"] == fleet._WINDOW_CACHE_SCHEMA
    assert saved["rows"][0]["reward_data_present"] is False


def test_current_cache_rehydrates_unique_environment_reward_attribution(
    monkeypatch, tmp_path
):
    cache_path = tmp_path / "window_cache_v2.json.gz"
    row = {
        "n": 23691,
        "ours": 1,
        "total_batch": 1,
        "rt_first": 179.1,
        "rejects": {},
        "reward_ours": 0.0625,
        "reward_total": 1.0,
        "rewards_by_hotkey": {OUR_HOTKEY: 0.0625},
        "reward_data_present": True,
        "reward_ours_by_environment": {},
        "reward_ours_by_environment_exact": False,
        "batch": [
            {
                "hotkey": OUR_HOTKEY,
                "env_name": "opencodeinstruct",
                "rewarded": True,
                "selected_for_batch": True,
            }
        ],
    }
    payload = {
        "schema_version": fleet._WINDOW_CACHE_SCHEMA,
        "rows": [row],
    }
    cache_path.write_bytes(fleet.gzip.compress(json.dumps(payload).encode()))
    monkeypatch.setattr(fleet, "OUR_SS58", {OUR_HOTKEY: "h100-reserve1"})
    monkeypatch.setattr(fleet, "_WINDOW_CACHE_FILE", str(cache_path))
    monkeypatch.setattr(
        fleet, "_LEGACY_WINDOW_CACHE_FILE", str(tmp_path / "legacy.json.gz")
    )
    monkeypatch.setattr(fleet, "_window_cache", {})

    assert fleet.window_cache_load() == 1
    window = fleet._window_cache[23691]
    assert window.reward_ours_by_environment == {"opencodeinstruct": 0.0625}
    assert window.reward_ours_by_environment_exact is True


def test_reject_metadata_covers_current_protocol_and_candidate_failures():
    import fleet_web

    current_reasons = {
        "accepted", "submitted", "bad_signature", "bad_envelope_signature",
        "bad_prompt_idx", "prompt_mismatch", "distribution_suspicious",
        "prompt_in_cooldown", "superseded", "prompt_full", "grail_fail",
        "hash_duplicate", "logprob_mismatch", "reward_mismatch",
        "reward_distribution", "out_of_zone", "rate_limited", "batch_filled",
        "wrong_rollout_count", "window_mismatch", "window_not_active",
        "bad_schema", "bad_tokens", "tokens_mismatch", "bad_termination",
        "boxed_answer_tampered", "token_tampered", "malformed_final_answer",
        "reward_shape_suspicious", "wrong_checkpoint", "wrong_randomness",
        "worker_dropped", "stale_round", "future_round",
    }

    assert current_reasons <= set(fleet_web.REJECT_REASON_META)
    assert "pool" in fleet_web.REJECT_REASON_META["accepted"]["explain"]
    assert "selection/reward" in fleet_web.REJECT_REASON_META["accepted"]["explain"]
    assert "rollout commitment" in fleet_web.REJECT_REASON_META["bad_signature"]["explain"]
    assert "before hotkey quota" in fleet_web.REJECT_REASON_META["bad_envelope_signature"]["explain"]
    assert "before decode" in fleet_web.REJECT_REASON_META["tokens_mismatch"]["explain"]
    assert "candidate" in fleet_web.REJECT_REASON_META["boxed_answer_tampered"]["explain"]
    assert fleet_web.REJECT_REASON_META["reward_shape_suspicious"]["deprecated"] is True


def test_ops_overview_uses_shared_reference_readiness_checks(monkeypatch):
    import fleet_web

    revision = "02af5e8015a409e1a00fe381e583e4c2450683b1"
    repo = "ReliquaryForge/qwen3.5-2b-reliquary-v2"
    box = _box(
        proc_alive=True,
        engine_mode="reference",
        protocol_profile="current",
        runtime_parity_ok=True,
        reference_ready=True,
        miner_source_revision="miner-source-revision",
        reliquary_source_revision="reliquary-source-revision",
        source_manifest_provisioned_ok=True,
        provisioned_model_kind="validator_checkpoint",
        provisioned_checkpoint_n=42,
        provisioned_model_repo=repo,
        provisioned_model_revision=revision,
        local_checkpoint_revision=revision,
        frontier_checkpoint_revision=revision[:12],
    )
    vs = fleet.ValidatorState(
        state="open",
        window=100,
        checkpoint_n=42,
        checkpoint_repo_id=repo,
        checkpoint_revision=revision,
    )
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", vs)
    monkeypatch.setattr(fleet_web, "_windows", [])
    monkeypatch.setattr(fleet_web, "_chain", fleet.ChainState())
    monkeypatch.setattr(fleet_web, "_mission_current_events", lambda: [])

    healthy = fleet_web.render_fleet_summary_html()
    assert "HEALTHY" in healthy
    assert "ready 1/1 · alive 1/1" in healthy

    box.env_file_ok = False
    box.env_file_error = "missing"
    bad_env = fleet_web.render_fleet_summary_html()
    assert "ALERT" in bad_env
    assert "env file unreadable" in bad_env

    box.env_file_ok = True
    box.env_file_error = ""
    box.miner_source_revision = ""
    missing_source = fleet_web.render_fleet_summary_html()
    assert "DEGRADED" in missing_source
    assert "source manifest missing" in missing_source

    box.miner_source_revision = "miner-source-revision"
    box.local_checkpoint_revision = "f" * 40
    stale_journal = fleet_web.render_fleet_summary_html()
    assert "HEALTHY" in stale_journal
    assert "local checkpoint mismatch" not in stale_journal

    box.provisioned_model_revision = "f" * 40
    bad_checkpoint = fleet_web.render_fleet_summary_html()
    assert "DEGRADED" in bad_checkpoint
    assert "provisioned model revision mismatch" in bad_checkpoint

    box.local_checkpoint_revision = revision
    box.provisioned_model_revision = revision
    box.frontier_checkpoint_revision = "e" * 40
    bad_frontier = fleet_web.render_fleet_summary_html()
    assert "frontier checkpoint mismatch" in bad_frontier

    box.frontier_checkpoint_revision = revision
    box.quarantine_active = True
    quarantined = fleet_web.render_fleet_summary_html()
    assert "ALERT" in quarantined
    assert "1 quarantined" in quarantined


def test_named_only_unit_requires_readable_selected_environment(monkeypatch):
    import fleet_web

    box = _box(
        unit=None,
        env_file="",
        unit_candidates=(("reliquary-miner-pro@code.service", "/state/code.env"),),
        active_unit="reliquary-miner-pro@code.service",
        active_lane="code",
        active_environment="opencodeinstruct",
        active_pid=4242,
        proc_alive=True,
        env_file_ok=False,
        env_file_error="missing:/state/code.env",
    )
    issues = fleet_web._box_readiness_issues(box)
    assert "env_file:missing:/state/code.env" in issues

    monkeypatch.setattr(fleet_web, "_boxes", [box])
    rendered = fleet_web.render_pipeline_html()
    assert "config-error" in rendered


def test_poller_waits_for_complete_collectors_before_publishing_freshness():
    import fleet_web

    source = inspect.getsource(fleet_web.poller_loop)
    assert ".join(timeout" not in source
    assert "t.join()" in source
    assert "v_thread.join()" in source


def test_seed_v2_auction_ignores_only_stale_disabled_prompt_frontier():
    import fleet_web

    revision = "c" * 40
    repo = "ReliquaryForge/qwen3.5-2b-reliquary-v2"
    box = _box(
        proc_alive=True,
        engine_mode="reference",
        protocol_profile="forced_seed_v2_auction_legacy_wire",
        runtime_parity_ok=True,
        reference_ready=True,
        miner_source_revision="miner-source-revision",
        reliquary_source_revision="reliquary-source-revision",
        source_manifest_provisioned_ok=True,
        provisioned_model_kind="validator_checkpoint",
        provisioned_checkpoint_n=42,
        provisioned_model_repo=repo,
        provisioned_model_revision=revision,
        local_checkpoint_revision=revision,
        frontier_checkpoint_revision="e" * 40,
    )
    vs = fleet.ValidatorState(
        checkpoint_n=42,
        checkpoint_repo_id=repo,
        checkpoint_revision=revision,
    )

    issues = fleet_web._box_readiness_issues(box, validator_state=vs)
    assert "checkpoint_frontier_mismatch" not in issues

    box.protocol_profile = "forced_seed_v1"
    legacy_issues = fleet_web._box_readiness_issues(box, validator_state=vs)
    assert "checkpoint_frontier_mismatch" in legacy_issues

    box.protocol_profile = "forced_seed_v2_auction_legacy_wire"
    box.local_checkpoint_revision = "f" * 40
    wrong_model_issues = fleet_web._box_readiness_issues(box, validator_state=vs)
    assert "model_revision_mismatch" not in wrong_model_issues
    assert "checkpoint_local_mismatch" not in wrong_model_issues


def test_verdict_presentation_names_pool_admission_and_keeps_r2_separate(
    monkeypatch,
):
    import fleet_web

    box = _box(
        proc_alive=True,
        acpt_30m=2,
        acpt_60m=3,
        active_unit="reliquary-miner-pro@code-reserve1.service",
        active_lane="code-reserve1",
        active_environment="opencodeinstruct",
        active_pid=4242,
        restart_count=3,
    )
    box.final_accept_30m = 2
    box.final_reject_30m = 1
    box.acceptance_source = "verdicts"
    vs = fleet.ValidatorState(state="open", window=100, health_status="ok")
    vs.verdicts_by_hotkey = {
        OUR_HOTKEY: {
            "accepted_30m": 2,
            "rejected_30m": 1,
            "accepted_60m": 3,
            "rejected_60m": 1,
            "finalized_30m": 2,
            "selected_30m": 1,
            "rewarded_30m": 1,
            "last": {"accepted": True, "reason": "accepted", "window_n": 99},
        }
    }
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", vs)
    monkeypatch.setattr(fleet_web, "_windows", [])

    fleet_html = fleet_web.render_fleet_html()
    validator_html = fleet_web.render_validator_html()

    assert "pool/30m" in fleet_html
    assert "pool/60m" in fleet_html
    assert "pool a/r" in fleet_html
    assert "code-reserve1" in fleet_html
    assert "opencodeinstruct · pid 4242 · r3" in fleet_html
    assert "Final validator accepts" not in fleet_html
    assert "authoritative /verdicts lifecycle" in validator_html
    assert "pool admission and seal-final outcomes" in validator_html
    assert "30m seal f/s/r" in validator_html
    assert "2/1/1" in validator_html


def test_fleet_panel_displays_the_complete_coordinated_lane_set(monkeypatch):
    import fleet_web

    code_one = "reliquary-miner-pro@code-reserve1.service"
    code_two = "reliquary-miner-pro@code-reserve2.service"
    box = _box(
        proc_alive=True,
        active_unit=code_one,
        active_units=[code_one, code_two],
        active_lane="code-reserve1",
        active_lanes=["code-reserve1", "code-reserve2"],
        active_environment="opencodeinstruct",
        active_pid=4242,
        coordinated_units=(code_one, code_two),
        coordinated_unit_statuses=[
            {
                "unit": code_one,
                "active_state": "active",
                "sub_state": "running",
                "pid": 4242,
                "restarts": 2,
            },
            {
                "unit": code_two,
                "active_state": "active",
                "sub_state": "running",
                "pid": 4243,
                "restarts": 5,
            },
        ],
        restart_count=7,
    )
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", fleet.ValidatorState())

    rendered = fleet_web.render_fleet_html()

    assert "code-reserve1 + code-reserve2" in rendered
    assert "2 coordinated lanes" in rendered
    assert f"active_set={code_one},{code_two}" in rendered
    assert "sum_systemd_nrestarts_across_coordinated_units" in rendered
    assert f"{code_one}[active/running,pid=4242,r=2]" in rendered
    assert f"{code_two}[active/running,pid=4243,r=5]" in rendered
    assert "rΣ7" in rendered


def test_coordinated_restart_projection_is_exact_in_health_and_export(
    monkeypatch,
):
    import fleet_web

    now = 40_000.0
    code_one = "reliquary-miner-pro@code-reserve1.service"
    code_two = "reliquary-miner-pro@code-reserve2.service"
    statuses = [
        {
            "unit": code_one,
            "active_state": "active",
            "sub_state": "running",
            "pid": 4242,
            "restarts": 2,
        },
        {
            "unit": code_two,
            "active_state": "active",
            "sub_state": "running",
            "pid": 4243,
            "restarts": 5,
        },
    ]
    box = _box(
        proc_alive=True,
        last_poll_s=now - 1,
        active_unit=code_one,
        active_units=[code_one, code_two],
        active_lane="code-reserve1",
        active_lanes=["code-reserve1", "code-reserve2"],
        active_environment="opencodeinstruct",
        active_pid=4242,
        coordinated_units=(code_one, code_two),
        coordinated_unit_statuses=statuses,
        restart_count=7,
    )
    validator = fleet.ValidatorState(
        state="open",
        window=100,
        last_fetch_at=now - 1,
        health_last_fetch_at=now - 1,
        health_status="ok",
        verdicts_last_fetch_at=now - 1,
    )
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", validator)
    monkeypatch.setattr(fleet_web, "_windows", [])
    monkeypatch.setattr(fleet_web, "_last_poll_at", now - 1)
    monkeypatch.setattr(fleet_web, "_poll_count", 1)
    monkeypatch.setattr(fleet_web, "_chain", fleet.ChainState())
    monkeypatch.setattr(
        fleet_web,
        "_r2_status_snapshot",
        lambda: {
            "last_success_at": now - 1,
            "error": "",
            "latest_window": 0,
            "coverage_complete": False,
            "coverage": {"complete": False},
        },
    )

    health_box = fleet_web.compute_healthz(5, now=now)["fleet"][0]
    exported_box = fleet_web.render_export_json()["fleet"][0]

    assert health_box["coordinated_unit_statuses"] == statuses
    assert health_box["restarts"] == 7
    assert health_box["restart_count_semantics"] == (
        "sum_systemd_nrestarts_across_coordinated_units"
    )
    assert exported_box["coordinated_unit_statuses"] == statuses
    assert exported_box["restart_count"] == 7
    assert exported_box["restart_count_semantics"] == (
        "sum_systemd_nrestarts_across_coordinated_units"
    )


def test_explicit_clean_base_uses_manifest_and_ignores_old_journal_checkpoint():
    import fleet_web

    base_repo = "Qwen/Qwen3.5-2B"
    base_revision = "1" * 40
    box = _box(
        proc_alive=True,
        engine_mode="reference",
        protocol_profile="forced_seed_v2_auction_legacy_wire",
        runtime_parity_ok=True,
        reference_ready=True,
        miner_source_revision="2" * 40,
        reliquary_source_revision="3" * 40,
        source_manifest_provisioned_ok=True,
        provisioned_model_kind="clean_base",
        provisioned_checkpoint_n=0,
        provisioned_model_repo=base_repo,
        provisioned_model_revision=base_revision,
        base_model_repo=base_repo,
        base_model_revision=base_revision,
        local_checkpoint_n=42,
        local_checkpoint_revision="f" * 40,
    )
    vs = fleet.ValidatorState(
        checkpoint_n=0,
        checkpoint_repo_id="",
        checkpoint_revision="",
        state_raw={
            "state": "open",
            "window_n": 23620,
            "checkpoint_n": 0,
            "checkpoint_repo_id": None,
            "checkpoint_revision": None,
        },
    )

    assert fleet_web._validator_model_identity(vs) == {
        "known": True,
        "kind": "clean_base",
        "checkpoint_n": 0,
        "repo": "",
        "revision": "",
        "source": "state",
    }
    issues = fleet_web._box_readiness_issues(box, validator_state=vs)
    assert issues == []
    assert "checkpoint_local_mismatch" not in issues

    box.base_model_revision = "e" * 40
    issues = fleet_web._box_readiness_issues(box, validator_state=vs)
    assert "base_model_revision_mismatch" in issues

    box.base_model_revision = base_revision
    vs.state_raw = {}
    pending = fleet_web._box_readiness_issues(box, validator_state=vs)
    assert "validator_model_identity_pending" in pending


def test_dynamic_runtime_checkpoint_supersedes_manifest_only_when_attested():
    import fleet_web

    repo = "ReliquaryForge/qwen3.5-2b-reliquary-v3"
    provisioned_revision = "a" * 40
    runtime_revision = "b" * 40
    box = _box(
        proc_alive=True,
        active_pid=4242,
        active_started_at=1_000,
        engine_mode="reference",
        source_manifest_provisioned_ok=True,
        provisioned_model_kind="validator_checkpoint",
        provisioned_checkpoint_n=4,
        provisioned_model_repo=repo,
        provisioned_model_revision=provisioned_revision,
        runtime_checkpoint_loaded=True,
        runtime_checkpoint_n=5,
        runtime_checkpoint_repo=repo,
        runtime_checkpoint_revision=runtime_revision,
        runtime_checkpoint_pid=4242,
        runtime_checkpoint_started_at=1_000,
        runtime_checkpoint_evidence="loaded+math_generation_group_abort",
    )
    vs = fleet.ValidatorState(
        checkpoint_n=5,
        checkpoint_repo_id=repo,
        checkpoint_revision=runtime_revision,
    )

    assert fleet_web._box_model_readiness_issues(box, vs) == []
    details = fleet_web._box_model_details(box, vs)
    assert details["provisioned"]["checkpoint_n"] == 4
    assert details["active"]["source"] == "runtime_journal"
    assert details["active"]["checkpoint_n"] == 5
    assert details["runtime"]["exact_validator_identity"] is True

    # PID/start binding is mandatory. Validator telemetry and even copied
    # runtime-looking values cannot bless evidence from another invocation.
    box.runtime_checkpoint_pid = 9999
    issues = fleet_web._box_model_readiness_issues(box, vs)
    assert "model_checkpoint_n_mismatch" in issues
    assert "model_revision_mismatch" in issues
    assert fleet_web._box_model_details(box, vs)["runtime"]["valid"] is False

    box.runtime_checkpoint_pid = 4242
    box.runtime_checkpoint_started_at = 999
    issues = fleet_web._box_model_readiness_issues(box, vs)
    assert "model_checkpoint_n_mismatch" in issues
    assert fleet_web._box_model_details(box, vs)["runtime"]["valid"] is False


def test_runtime_checkpoint_mismatch_remains_fail_closed():
    import fleet_web

    validator_repo = "ReliquaryForge/qwen3.5-2b-reliquary-v3"
    runtime_revision = "b" * 40
    box = _box(
        proc_alive=True,
        active_pid=4242,
        active_started_at=1_000,
        engine_mode="reference",
        source_manifest_provisioned_ok=True,
        provisioned_model_kind="validator_checkpoint",
        provisioned_checkpoint_n=4,
        provisioned_model_repo=validator_repo,
        provisioned_model_revision="a" * 40,
        runtime_checkpoint_loaded=True,
        runtime_checkpoint_n=5,
        runtime_checkpoint_repo="ReliquaryForge/wrong-repo",
        runtime_checkpoint_revision=runtime_revision,
        runtime_checkpoint_pid=4242,
        runtime_checkpoint_started_at=1_000,
        runtime_checkpoint_evidence="loaded+code_generation_group_abort",
    )
    vs = fleet.ValidatorState(
        checkpoint_n=5,
        checkpoint_repo_id=validator_repo,
        checkpoint_revision=runtime_revision,
    )

    issues = fleet_web._box_model_readiness_issues(box, vs)
    assert "model_repo_mismatch" in issues
    assert fleet_web._box_model_details(box, vs)["runtime"][
        "exact_validator_identity"
    ] is False

    # A matching local/journal revision alone is informational. Without the
    # load+abort process attestation, the old manifest remains active truth.
    box.runtime_checkpoint_loaded = False
    box.runtime_checkpoint_repo = validator_repo
    box.local_checkpoint_n = 5
    box.local_checkpoint_revision = runtime_revision
    issues = fleet_web._box_model_readiness_issues(box, vs)
    assert "model_checkpoint_n_mismatch" in issues
    assert "model_revision_mismatch" in issues


def test_model_manifest_is_displayed_and_exported_in_health_details(monkeypatch):
    import fleet_web

    now = 30_000.0
    repo = "ReliquaryForge/qwen3.5-2b-reliquary-v3"
    revision = "b" * 40
    box = _box(
        last_poll_s=now - 1,
        proc_alive=True,
        engine_mode="reference",
        protocol_profile="forced_seed_v2_auction_legacy_wire",
        runtime_parity_ok=True,
        reference_ready=True,
        miner_source_revision="2" * 40,
        reliquary_source_revision="3" * 40,
        source_manifest_provisioned_ok=True,
        provisioned_model_kind="validator_checkpoint",
        provisioned_checkpoint_n=1,
        provisioned_model_repo=repo,
        provisioned_model_revision=revision,
        # An earlier invocation's marker must stay informational only.
        local_checkpoint_n=42,
        local_checkpoint_revision="f" * 40,
    )
    vs = fleet.ValidatorState(
        state="open",
        window=23620,
        checkpoint_n=1,
        checkpoint_repo_id=repo,
        checkpoint_revision=revision,
        last_fetch_at=now - 1,
        health_last_fetch_at=now - 1,
        health_status="ok",
        verdicts_last_fetch_at=now - 1,
    )
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", vs)
    monkeypatch.setattr(fleet_web, "_windows", [])
    monkeypatch.setattr(fleet_web, "_last_poll_at", now - 1)

    rendered = fleet_web.render_fleet_html()
    assert "checkpoint n1" in rendered
    assert "qwen3.5-2b-reliquary-v3@bbbbbbbbbb" in rendered

    exported = fleet_web.render_export_json()
    exported_box = exported["fleet"][0]
    assert exported["validator"]["checkpoint_repo_id"] == repo
    assert exported["validator"]["model_identity"]["known"] is True
    assert exported_box["provisioned_model_kind"] == "validator_checkpoint"
    assert exported_box["provisioned_checkpoint_n"] == 1
    assert exported_box["provisioned_model_repo"] == repo
    assert exported_box["provisioned_model_revision"] == revision
    assert exported_box["model"]["observed_journal"]["revision"] == "f" * 40
    assert exported_box["readiness_issues"] == []

    monkeypatch.setattr(
        fleet_web,
        "_r2_status_snapshot",
        lambda: {"last_success_at": now - 1, "coverage_complete": False},
    )
    health = fleet_web.compute_healthz(5, now=now)
    model = health["fleet"][0]["model"]
    assert model["ok"] is True
    assert model["issues"] == []
    assert model["provisioned"]["revision"] == revision
    assert model["validator"]["repo"] == repo
    assert health["fleet"][0]["issues"] == []


def test_healthz_accepts_complete_archive_coverage_with_absent_windows(
    monkeypatch,
):
    import fleet_web

    now = 30_000.0
    box = _box(
        last_poll_s=now - 1,
        proc_alive=True,
        active_unit="reliquary-miner-pro.service",
        active_lane="single",
        active_environment="opencodeinstruct",
        active_pid=1234,
    )
    vs = fleet.ValidatorState(
        state="open",
        window=100,
        last_fetch_at=now - 1,
        health_last_fetch_at=now - 1,
        health_status="ok",
        verdicts_last_fetch_at=now - 1,
    )
    window = _window(99, {OTHER_HOTKEY: 1.0})
    coverage = {
        "requested": 3,
        "numeric_expected": 3,
        "archived_expected": 2,
        "expected": 3,
        "cached": 2,
        "exact_rewards": 2,
        "absent_windows": [98],
        "missing_windows": [],
        "complete": True,
        "source": "validator_health",
    }
    r2_state = {
        "last_success_at": now - 1,
        "error": "",
        "latest_window": 99,
        "cache_entries": 2,
        "coverage_complete": True,
        "coverage": coverage,
    }
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", vs)
    monkeypatch.setattr(fleet_web, "_windows", [window])
    monkeypatch.setattr(fleet_web, "_last_poll_at", now - 1)
    monkeypatch.setattr(fleet_web, "_poll_count", 5)
    monkeypatch.setattr(fleet_web, "_r2_status_snapshot", lambda: r2_state)

    health = fleet_web.compute_healthz(5, now=now)
    exported = fleet_web.render_export_json()

    assert health["ok"] is True
    assert all(health["checks"].values())
    assert health["r2"]["coverage"]["absent_windows"] == [98]
    assert health["r2"]["coverage"]["archived_expected"] == 2
    assert "terminal_windows" not in health["r2"]
    assert "aborted_windows" not in health["r2"]
    assert exported["r2"]["coverage"] == coverage


def test_healthz_accepts_terminal_tombstone_without_calling_it_reward_data(
    monkeypatch,
):
    import fleet_web

    now = 30_000.0
    box = _box(
        last_poll_s=now - 1,
        proc_alive=True,
        active_unit="reliquary-miner-pro.service",
        active_lane="single",
        active_environment="opencodeinstruct",
        active_pid=1234,
    )
    vs = fleet.ValidatorState(
        state="open",
        window=100,
        last_fetch_at=now - 1,
        health_last_fetch_at=now - 1,
        health_status="ok",
        verdicts_last_fetch_at=now - 1,
    )
    completed = _window(99, {OTHER_HOTKEY: 1.0})
    tombstone = fleet.WindowSummary(
        n=98,
        ours=0,
        total_batch=0,
        rt_first=0.0,
        rejects={},
        window_status="aborted",
        terminal_data_present=True,
        reward_data_present=False,
        failure_stage="seal",
        failure_type="UnexpectedSealError",
        lifecycle_explicit=True,
    )
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", vs)
    monkeypatch.setattr(fleet_web, "_windows", [completed, tombstone])
    monkeypatch.setattr(fleet_web, "_last_poll_at", now - 1)
    monkeypatch.setattr(fleet_web, "_poll_count", 5)
    monkeypatch.setattr(
        fleet_web,
        "_r2_status_snapshot",
        lambda: {
            "last_success_at": now - 1,
            "error": "",
            "latest_window": 99,
            "coverage_complete": True,
            "coverage": {
                "complete": True,
                "terminal_present": 2,
                "reward_expected": 1,
                "exact_rewards": 1,
                "aborted_windows": [98],
            },
        },
    )

    health = fleet_web.compute_healthz(5, now=now)
    assert health["ok"] is True
    assert health["checks"]["r2_cache_ready"] is True
    assert health["r2"]["terminal_windows"] == 2
    assert health["r2"]["completed_windows"] == 1
    assert health["r2"]["aborted_windows"] == 1
    assert health["r2"]["exact_reward_windows"] == 1


def test_liveness_panel_and_export_surface_ingress_seal_and_archive_v2(
    monkeypatch,
):
    import fleet_web

    vs = fleet.ValidatorState(state="training", window=23865, health_status="ok")
    fleet._apply_validator_health(
        vs,
        {
            "status": "ok",
            "event_loop_lag_ms": {"p50": 4.266, "p95": 2708.269, "p99": 13019.382, "max": 31655.837},
            "endpoint_latency_ms": {
                "/health": {"p50": 1593.151, "p95": 23568.514, "p99": 28408.363, "max": 28453.609},
                "/submit": {"p50": 2375.042, "p95": 98875.959, "p99": 137346.883, "max": 145769.717},
            },
            "queue_depth_by_environment": {
                "openmathinstruct": 0,
                "opencodeinstruct": 2,
            },
            "admission_workers_by_environment": {
                "openmathinstruct": 4,
                "opencodeinstruct": 2,
            },
            "proof_verification_inflight_by_environment": {
                "openmathinstruct": 1,
                "opencodeinstruct": 0,
            },
            "admission_latency_ms_by_environment": {
                "opencodeinstruct": {
                    "queue_wait_ms": {"p95": 1359.771, "p99": 1750.448},
                    "admission_prepare_ms": {"p95": 5232.588, "p99": 6166.78},
                    "commit_lock_wait_ms": {"p95": 0.009, "p99": 0.01},
                    "total_ms": {"p95": 29891.944, "p99": 35462.133},
                }
            },
            "window_environments": {
                "opencodeinstruct": {
                    "auction_seal_drain": {
                        "elapsed_seconds": 45.27,
                        "timed_out": False,
                        "queue_depth_at_snapshot": 0,
                        "inflight_workers_at_snapshot": 0,
                        "pending_reservations_at_snapshot": 0,
                        "inflight_reservations_at_snapshot": 0,
                    }
                }
            },
            "archive_queue_depth": 0,
            "archive_last_uploaded_window": 23864,
            "archive_last_enqueued_window": 23865,
            "archive_archives_enqueued_total": 2,
            "archive_enqueue_gaps_total": 0,
            "archive_last_enqueue_gap": None,
        },
    )
    completed = fleet.WindowSummary(
        n=23864,
        ours=1,
        total_batch=16,
        rt_first=1.0,
        rejects={},
        reward_data_present=True,
        lifecycle_explicit=True,
        archive_schema_version=2,
        auction_seal_drain_by_environment={
            "opencodeinstruct": {
                "elapsed_seconds": 45.27,
                "timed_out": False,
                "queue_depth_at_snapshot": 0,
            }
        },
    )
    tombstone = fleet.WindowSummary(
        n=23863,
        ours=0,
        total_batch=0,
        rt_first=0.0,
        rejects={},
        window_status="aborted",
        terminal_data_present=True,
        reward_data_present=False,
        failure_stage="seal",
        failure_type="UnexpectedSealError",
        lifecycle_explicit=True,
        archive_schema_version=2,
    )
    windows = [completed, tombstone]

    details = fleet_web._validator_liveness_details(vs, windows)
    panel = fleet_web.render_validator_liveness_html(vs, windows)
    assert details["by_environment"]["opencodeinstruct"]["queue_depth"] == 2
    assert details["archive"]["last_completed_window"] == 23864
    assert details["archive"]["last_aborted_window"] == 23863
    assert "auction-v2 ingress liveness" in panel
    assert "prepare p95/p99" in panel
    assert "commit p99" in panel
    assert "45.3s" in panel
    assert "last enqueued <b>23,865</b>" in panel
    assert "last aborted <b>23,863</b>" in panel

    monkeypatch.setattr(fleet_web, "_vs", vs)
    monkeypatch.setattr(fleet_web, "_windows", windows)
    exported = fleet_web.render_export_json()
    assert exported["validator"]["liveness"]["event_loop_lag_ms"]["p99"] == 13019.382
    assert exported["validator"]["liveness"]["archive"]["enqueue_gaps_total"] == 0
    assert exported["windows"][0]["archive_schema_version"] == 2
    assert exported["windows"][0]["auction_seal_drain_by_environment"][
        "opencodeinstruct"
    ]["timed_out"] is False
    assert exported["windows"][1]["window_status"] == "aborted"
    assert exported["windows"][1]["reward_data_present"] is False


def test_healthz_only_fails_explicit_seal_timeout_or_archive_gap(monkeypatch):
    import fleet_web

    now = 30_000.0
    box = _box(
        last_poll_s=now - 1,
        proc_alive=True,
        active_unit="reliquary-miner-pro.service",
        active_lane="single",
        active_environment="openmathinstruct",
        active_pid=1234,
    )
    vs = fleet.ValidatorState(
        state="open",
        window=100,
        last_fetch_at=now - 1,
        health_last_fetch_at=now - 1,
        health_status="ok",
        verdicts_last_fetch_at=now - 1,
        liveness_telemetry_reported=True,
        archive_continuity_reported=True,
        archive_enqueue_gaps_total=0,
        seal_drain_by_environment={
            "openmathinstruct": {"timed_out": False, "elapsed_seconds": 1.0}
        },
    )
    window = _window(99, {OTHER_HOTKEY: 1.0})
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", vs)
    monkeypatch.setattr(fleet_web, "_windows", [window])
    monkeypatch.setattr(fleet_web, "_last_poll_at", now - 1)
    monkeypatch.setattr(fleet_web, "_poll_count", 5)
    monkeypatch.setattr(
        fleet_web,
        "_r2_status_snapshot",
        lambda: {
            "last_success_at": now - 1,
            "error": "",
            "latest_window": 99,
            "coverage_complete": True,
            "coverage": {"complete": True},
        },
    )

    healthy = fleet_web.compute_healthz(5, now=now)
    assert healthy["checks"]["validator_seal_drain_ok"] is True
    assert healthy["checks"]["validator_archive_continuity_ok"] is True

    vs.seal_drain_by_environment["openmathinstruct"]["timed_out"] = True
    timed_out = fleet_web.compute_healthz(5, now=now)
    assert timed_out["ok"] is False
    assert timed_out["checks"]["validator_seal_drain_ok"] is False
    assert timed_out["validator"]["liveness"]["seal_timeout_environments"] == [
        "openmathinstruct"
    ]

    vs.seal_drain_by_environment["openmathinstruct"]["timed_out"] = False
    vs.archive_enqueue_gaps_total = 1
    vs.archive_last_enqueue_gap = {"expected_window": 98, "observed_window": 100}
    gap = fleet_web.compute_healthz(5, now=now)
    assert gap["ok"] is False
    assert gap["checks"]["validator_seal_drain_ok"] is True
    assert gap["checks"]["validator_archive_continuity_ok"] is False


def test_healthz_keeps_unresolved_or_stale_r2_history_degraded(monkeypatch):
    import fleet_web

    now = 30_000.0
    box = _box(
        last_poll_s=now - 1,
        proc_alive=True,
        active_unit="reliquary-miner-pro.service",
        active_lane="single",
        active_environment="opencodeinstruct",
        active_pid=1234,
    )
    vs = fleet.ValidatorState(
        state="open",
        window=100,
        last_fetch_at=now - 1,
        health_last_fetch_at=now - 1,
        health_status="ok",
        verdicts_last_fetch_at=now - 1,
    )
    r2_state = {
        "last_success_at": now - 1,
        "error": "reward cache warming:1 remaining",
        "latest_window": 99,
        "coverage_complete": False,
        "coverage": {
            "requested": 3,
            "numeric_expected": 3,
            "archived_expected": 3,
            "expected": 3,
            "cached": 2,
            "exact_rewards": 2,
            "absent_windows": [],
            "missing_windows": [98],
            "complete": False,
            "source": "validator_health",
        },
    }
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", vs)
    monkeypatch.setattr(
        fleet_web, "_windows", [_window(99, {OTHER_HOTKEY: 1.0})]
    )
    monkeypatch.setattr(fleet_web, "_last_poll_at", now - 1)
    monkeypatch.setattr(fleet_web, "_poll_count", 5)
    monkeypatch.setattr(fleet_web, "_r2_status_snapshot", lambda: r2_state)

    unresolved = fleet_web.compute_healthz(5, now=now)
    assert unresolved["ok"] is False
    assert unresolved["checks"]["r2_fresh"] is False
    assert unresolved["checks"]["r2_history_complete"] is False

    r2_state["error"] = ""
    r2_state["latest_window"] = 94
    r2_state["coverage_complete"] = True
    r2_state["coverage"].update(
        {
            "cached": 3,
            "exact_rewards": 3,
            "missing_windows": [],
            "complete": True,
        }
    )
    monkeypatch.setattr(
        fleet_web, "_windows", [_window(94, {OTHER_HOTKEY: 1.0})]
    )

    stale = fleet_web.compute_healthz(5, now=now)
    assert stale["ok"] is False
    assert stale["checks"]["r2_fresh"] is True
    assert stale["checks"]["r2_history_complete"] is True
    assert stale["checks"]["r2_latest_window_fresh"] is False


def test_healthz_reports_healthy_stale_down_and_quarantined(monkeypatch):
    import fleet_web

    now = 30_000.0
    box = _box(
        last_poll_s=now - 1,
        proc_alive=True,
        active_unit="reliquary-miner-pro.service",
        active_lane="single",
        active_environment="openmathinstruct",
        active_pid=1234,
        restart_count=2,
    )
    vs = fleet.ValidatorState(
        state="open",
        window=100,
        last_fetch_at=now - 1,
        health_last_fetch_at=now - 1,
        health_status="ok",
        verdicts_last_fetch_at=now - 1,
    )
    window = _window(99, {OTHER_HOTKEY: 1.0}, [{"hotkey": OTHER_HOTKEY}])
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", vs)
    monkeypatch.setattr(fleet_web, "_windows", [window])
    monkeypatch.setattr(fleet_web, "_last_poll_at", now - 1)
    monkeypatch.setattr(fleet_web, "_poll_count", 5)
    r2_state = {
        "last_success_at": now - 1,
        "error": "",
        "latest_window": 99,
        "cache_entries": 1,
        "coverage_complete": True,
        "coverage": {
            "requested": 1,
            "expected": 1,
            "cached": 1,
            "exact_rewards": 1,
            "missing_windows": [],
            "complete": True,
            "source": "validator_health",
        },
    }
    monkeypatch.setattr(fleet_web, "_r2_status_snapshot", lambda: r2_state)

    healthy = fleet_web.compute_healthz(5, now=now)
    assert healthy["ok"] is True
    assert all(healthy["checks"].values())
    assert healthy["fleet"][0]["configured_units"] == [
        "reliquary-miner-pro"
    ]
    assert healthy["fleet"][0]["active_unit"] == "reliquary-miner-pro.service"
    assert healthy["fleet"][0]["active_lane"] == "single"
    assert healthy["fleet"][0]["active_environment"] == "openmathinstruct"
    assert healthy["fleet"][0]["active_pid"] == 1234
    assert healthy["fleet"][0]["restarts"] == 2

    window.reward_data_present = False
    inexact = fleet_web.compute_healthz(5, now=now)
    assert inexact["ok"] is False
    assert inexact["checks"]["r2_cache_ready"] is False
    window.reward_data_present = True

    r2_state["coverage_complete"] = False
    r2_state["coverage"]["complete"] = False
    r2_state["coverage"]["missing_windows"] = [98]
    partial = fleet_web.compute_healthz(5, now=now)
    assert partial["ok"] is False
    assert partial["checks"]["r2_history_complete"] is False
    assert partial["r2"]["coverage"]["missing_windows"] == [98]
    r2_state["coverage_complete"] = True
    r2_state["coverage"]["complete"] = True
    r2_state["coverage"]["missing_windows"] = []

    box.proc_alive = False
    down = fleet_web.compute_healthz(5, now=now)
    assert down["ok"] is False
    assert down["checks"]["fleet_ready"] is False
    assert "down" in down["fleet"][0]["issues"]

    box.proc_alive = True
    box.quarantine_active = True
    quarantined = fleet_web.compute_healthz(5, now=now)
    assert quarantined["ok"] is False
    assert "quarantined" in quarantined["fleet"][0]["issues"]

    box.quarantine_active = False
    box.last_poll_s = now - 40
    monkeypatch.setattr(fleet_web, "_last_poll_at", now - 40)
    budgeted = fleet_web.compute_healthz(5, now=now, poll_stale_s=60)
    assert budgeted["checks"]["poll_fresh"] is True
    assert "stale" not in budgeted["fleet"][0]["issues"]
    assert budgeted["thresholds_s"]["poll"] == 60
    assert budgeted["thresholds_s"]["verdicts"] == 180

    strict = fleet_web.compute_healthz(5, now=now)
    assert strict["checks"]["poll_fresh"] is False
    assert "stale" in strict["fleet"][0]["issues"]

    box.last_poll_s = now - 100
    monkeypatch.setattr(fleet_web, "_last_poll_at", now - 100)
    stale = fleet_web.compute_healthz(5, now=now, poll_stale_s=60)
    assert stale["ok"] is False
    assert stale["checks"]["poll_fresh"] is False
    assert "stale" in stale["fleet"][0]["issues"]


def test_healthz_uses_fresh_health_only_for_non_open_state_gap(monkeypatch):
    import fleet_web

    now = 30_000.0
    box = _box(
        last_poll_s=now - 1,
        proc_alive=True,
        active_unit="reliquary-miner-pro.service",
        active_lane="single",
        active_environment="openmathinstruct",
        active_pid=1234,
    )
    vs = fleet.ValidatorState(
        state="ready",
        window=100,
        last_fetch_at=now - 90,
        error="HTTPStatusError",
        health_last_fetch_at=now - 1,
        health_status="ok",
        health_raw={
            "status": "ok",
            "current_validator_state": "ready",
            "current_window_n": 100,
        },
        verdicts_last_fetch_at=now - 1,
    )
    window = _window(99, {OTHER_HOTKEY: 1.0}, [{"hotkey": OTHER_HOTKEY}])
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", vs)
    monkeypatch.setattr(fleet_web, "_windows", [window])
    monkeypatch.setattr(fleet_web, "_last_poll_at", now - 1)
    monkeypatch.setattr(
        fleet_web,
        "_r2_status_snapshot",
        lambda: {
            "last_success_at": now - 1,
            "error": "",
            "latest_window": 99,
            "coverage_complete": True,
        },
    )

    ready = fleet_web.compute_healthz(5, now=now)
    assert ready["ok"] is True
    assert ready["checks"]["validator_state_fresh"] is True
    assert ready["validator"]["state_source"] == "/health (non-open)"

    # OPEN must fail closed without a fresh full `/state` payload: `/health`
    # does not carry the randomness and cooldown fields miners submit against.
    vs.state = "open"
    vs.health_raw["current_validator_state"] = "open"
    opened = fleet_web.compute_healthz(5, now=now)
    assert opened["ok"] is False
    assert opened["checks"]["validator_state_fresh"] is False
    assert opened["validator"]["state_source"] == ""

    # A non-open snapshot for another window is stale/mismatched, not a lease.
    vs.state = "ready"
    vs.health_raw["current_validator_state"] = "ready"
    vs.health_raw["current_window_n"] = 99
    mismatched = fleet_web.compute_healthz(5, now=now)
    assert mismatched["checks"]["validator_state_fresh"] is False


def test_code_auction_probe_is_read_only_process_bound_and_non_secret():
    source = fleet._CODE_AUCTION_READINESS_PROBE_SOURCE

    compile(source, "<code-auction-readiness-probe>", "exec")
    assert "/proc/{pid}/environ" in source
    assert "os.lstat(grader_socket)" in source
    assert "?mode=ro" in source
    assert "PRAGMA query_only=ON" in source
    assert "PRAGMA quick_check" not in source
    assert "PRAGMA integrity_check" not in source
    assert "PRAGMA table_info(partitions)" in source
    assert "PRAGMA table_info(ledger_extensions)" in source
    assert "WHERE extension_name=?" in source
    assert '("terminal_funnel",)' in source
    assert '"v1_additive_compat"' in source
    assert source.count("AND attempt_key NOT LIKE 'r2:%'") == 2
    assert "FROM partitions AS p" in source
    assert "LEFT JOIN attempts" not in source
    assert "COUNT(a.attempt_key)" not in source
    assert "checkpoint_n" in source
    assert 'expected_unit_lane = miner_unit.rsplit("@", 1)' in source
    assert 'if key != "RELIQUARY_LANE_ID"' in source
    assert 'live_values.get("RELIQUARY_LANE_ID", "") == expected_unit_lane' in source
    for write_sql in (
        "INSERT INTO",
        "UPDATE ",
        "DELETE FROM",
        "CREATE TABLE",
        "DROP TABLE",
        "VACUUM",
    ):
        assert write_sql not in source.upper()
    for forbidden in (
        "AWS_SECRET_ACCESS_KEY",
        "wallet",
        "prompt_content_hash",
        "reward_vector_json",
    ):
        assert forbidden not in source


def test_code_auction_probe_binds_injected_lane_to_template_instance(tmp_path):
    env_values = {
        "RELIQUARY_ENVIRONMENT_NAME": "opencodeinstruct",
        "RELIQUARY_ENVIRONMENTS": "opencodeinstruct",
        "RELIQUARY_ENGINE_MODE": "reference",
        "RELIQUARY_LANE_ID": "code-reserve1",
        "RELIQUARY_REFERENCE_CODE_PRESCREEN_ENABLED": "1",
        "RELIQUARY_CODE_AUCTION_POLICY": "deadline_aware",
        "RELIQUARY_CODE_OUTCOME_LEDGER": str(tmp_path / "missing.sqlite3"),
    }
    env_path = tmp_path / "code.env"
    env_path.write_text(
        "\n".join(
            f"{key}={value}"
            for key, value in env_values.items()
            if key != "RELIQUARY_LANE_ID"
        )
        + "\n"
    )
    fake_systemctl = tmp_path / "systemctl"
    fake_systemctl.write_text(
        "#!/bin/sh\n"
        "case \"$1:$3\" in\n"
        "  show:MainPID) printf '%s\\n' \"$TEST_MAIN_PID\" ;;\n"
        "  show:InvocationID) printf '%s\\n' test-invocation ;;\n"
        "  is-enabled:*) printf '%s\\n' enabled ;;\n"
        "  is-active:*) printf '%s\\n' active ;;\n"
        "esac\n"
    )
    fake_systemctl.chmod(0o755)
    source = fleet._CODE_AUCTION_READINESS_PROBE_SOURCE.replace(
        "live_values = process_env(pid) if pid > 0 else {}",
        f"live_values = {env_values!r} if pid > 0 else {{}}",
    )

    def run_probe(unit):
        result = subprocess.run(
            [
                sys.executable,
                "-",
                str(env_path),
                unit,
                "",
                str(tmp_path / "grader.sock"),
                str(tmp_path / "current"),
                "65534",
                str(0o660),
            ],
            input=source,
            text=True,
            capture_output=True,
            timeout=5,
            check=True,
            env={"PATH": str(tmp_path), "TEST_MAIN_PID": "4242"},
        )
        return json.loads(result.stdout)

    matching = run_probe(
        "reliquary-miner-pro@code-reserve1.service"
    )
    mismatched = run_probe(
        "reliquary-miner-pro@code-reserve2.service"
    )

    assert matching["lane"] == "code-reserve1"
    assert matching["settings_process_bound"] is True
    assert mismatched["lane"] == "code-reserve1"
    assert mismatched["settings_process_bound"] is False


def test_code_auction_probe_reads_explicit_empty_partition_registry(tmp_path):
    ledger_path = tmp_path / "code-auction.sqlite3"
    connection = sqlite3.connect(ledger_path)
    connection.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA application_id=1380139340;
        PRAGMA user_version=1;
        CREATE TABLE partitions (
            model_repository TEXT NOT NULL,
            checkpoint_revision TEXT NOT NULL,
            checkpoint_n INTEGER NOT NULL,
            public_source_revision TEXT NOT NULL,
            runtime_profile_hash TEXT NOT NULL,
            environment TEXT NOT NULL,
            registered_at REAL NOT NULL,
            miner_source_revision TEXT NOT NULL
        );
        CREATE TABLE attempts (
            attempt_key TEXT,
            model_repository TEXT,
            checkpoint_revision TEXT,
            checkpoint_n INTEGER,
            public_source_revision TEXT,
            runtime_profile_hash TEXT,
            environment TEXT,
            window_n INTEGER,
            created_at REAL
        );
        CREATE TABLE events (
            model_repository TEXT,
            checkpoint_revision TEXT,
            checkpoint_n INTEGER,
            public_source_revision TEXT,
            runtime_profile_hash TEXT,
            environment TEXT,
            window_n INTEGER,
            observed_at REAL
        );
        """
    )
    connection.execute(
        "INSERT INTO partitions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "R" * 600,
            "c" * 40,
            13,
            "a" * 40,
            "d" * 64,
            "opencodeinstruct",
            123.0,
            "b" * 40,
        ),
    )
    connection.commit()
    connection.close()
    env_path = tmp_path / "code.env"
    env_path.write_text(
        "RELIQUARY_ENVIRONMENT_NAME=opencodeinstruct\n"
        "RELIQUARY_ENGINE_MODE=reference\n"
        "RELIQUARY_REFERENCE_CODE_PRESCREEN_ENABLED=1\n"
        "RELIQUARY_CODE_AUCTION_POLICY=deadline_aware\n"
        f"RELIQUARY_CODE_OUTCOME_LEDGER={ledger_path}\n"
    )
    source = fleet._CODE_AUCTION_READINESS_PROBE_SOURCE.replace(
        '"/srv/reliquary-miner-pro/state"', repr(str(tmp_path))
    )

    result = subprocess.run(
        [
            sys.executable,
            "-",
            str(env_path),
            "",
            "",
            str(tmp_path / "grader.sock"),
            str(tmp_path / "current"),
            "65534",
            str(0o660),
        ],
        input=source,
        text=True,
        capture_output=True,
        timeout=5,
        check=True,
    )
    parsed = json.loads(result.stdout)

    assert parsed["ledger"]["readonly_ok"] is True
    assert parsed["ledger"]["schema_ok"] is True
    assert parsed["ledger"]["quick_check"] == "not_run_fast_poll"
    assert parsed["ledger"]["partitions"] == [
        {
            "model_repository": "R" * 512,
            "checkpoint_revision": "c" * 40,
            "checkpoint_n": 13,
            "public_source_revision": "a" * 40,
            "runtime_profile_hash": "d" * 64,
            "environment": "opencodeinstruct",
            "registered_at": 123.0,
            "miner_source_revision": "b" * 40,
        }
    ]
    assert parsed["ledger"]["generation_outcomes"] == {
        "table_present": False,
        "schema_ok": False,
        "summaries": [],
        "error": "",
    }
    connection = sqlite3.connect(ledger_path)
    assert connection.execute("SELECT COUNT(*) FROM attempts").fetchone()[0] == 0
    connection.close()


def test_code_auction_probe_aggregates_generation_outcomes_without_payloads(
    tmp_path,
):
    ledger_path = tmp_path / "code-auction.sqlite3"
    connection = sqlite3.connect(ledger_path)
    connection.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA application_id=1380139340;
        PRAGMA user_version=1;
        CREATE TABLE partitions (
            model_repository TEXT NOT NULL,
            checkpoint_revision TEXT NOT NULL,
            checkpoint_n INTEGER NOT NULL,
            public_source_revision TEXT NOT NULL,
            runtime_profile_hash TEXT NOT NULL,
            environment TEXT NOT NULL,
            registered_at REAL NOT NULL,
            miner_source_revision TEXT NOT NULL
        );
        CREATE TABLE attempts (
            attempt_key TEXT,
            model_repository TEXT,
            checkpoint_revision TEXT,
            checkpoint_n INTEGER,
            public_source_revision TEXT,
            runtime_profile_hash TEXT,
            environment TEXT,
            window_n INTEGER,
            created_at REAL
        );
        CREATE TABLE events (
            model_repository TEXT,
            checkpoint_revision TEXT,
            checkpoint_n INTEGER,
            public_source_revision TEXT,
            runtime_profile_hash TEXT,
            environment TEXT,
            window_n INTEGER,
            observed_at REAL
        );
        CREATE TABLE generation_outcomes (
            observed_at REAL NOT NULL,
            model_repository TEXT NOT NULL,
            checkpoint_revision TEXT NOT NULL,
            checkpoint_n INTEGER NOT NULL,
            public_source_revision TEXT NOT NULL,
            runtime_profile_hash TEXT NOT NULL,
            environment TEXT NOT NULL,
            window_n INTEGER NOT NULL,
            lane TEXT NOT NULL,
            termination TEXT NOT NULL
        );
        """
    )
    repository = "ReliquaryForge/qwen3.5-2b-reliquary-v3"
    checkpoint_revision = "c" * 40
    public_source = "a" * 40
    runtime_profile = "d" * 64
    partition = (
        repository,
        checkpoint_revision,
        13,
        public_source,
        runtime_profile,
        "opencodeinstruct",
    )
    connection.execute(
        "INSERT INTO partitions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (*partition, 123.0, "b" * 40),
    )
    outcomes = [
        (1.0, *partition, 23816, "code-reserve1", "complete"),
        (2.0, *partition, 23817, "code-reserve1", "complete"),
        (3.0, *partition, 23817, "code-reserve1", "local_token_limit"),
        (4.0, *partition, 23818, "code-reserve1", "safe_deadline"),
        (5.0, *partition, 23818, "code-reserve2", "complete"),
        (
            6.0,
            repository,
            "e" * 40,
            14,
            public_source,
            runtime_profile,
            "opencodeinstruct",
            23819,
            "code-reserve1",
            "local_token_limit",
        ),
    ]
    connection.executemany(
        "INSERT INTO generation_outcomes VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        outcomes,
    )
    connection.commit()
    connection.close()
    env_path = tmp_path / "code.env"
    env_path.write_text(
        "RELIQUARY_ENVIRONMENT_NAME=opencodeinstruct\n"
        "RELIQUARY_ENGINE_MODE=reference\n"
        "RELIQUARY_LANE_ID=code-reserve1\n"
        "RELIQUARY_REFERENCE_CODE_PRESCREEN_ENABLED=1\n"
        "RELIQUARY_CODE_AUCTION_POLICY=deadline_aware\n"
        f"RELIQUARY_CODE_OUTCOME_LEDGER={ledger_path}\n"
    )
    source = fleet._CODE_AUCTION_READINESS_PROBE_SOURCE.replace(
        '"/srv/reliquary-miner-pro/state"', repr(str(tmp_path))
    )

    result = subprocess.run(
        [
            sys.executable,
            "-",
            str(env_path),
            "",
            "",
            str(tmp_path / "grader.sock"),
            str(tmp_path / "current"),
            "65534",
            str(0o660),
        ],
        input=source,
        text=True,
        capture_output=True,
        timeout=5,
        check=True,
    )
    parsed = fleet._parse_code_auction_probe([result.stdout])
    generation = parsed["ledger"]["generation_outcomes"]

    assert parsed["lane"] == "code-reserve1"
    assert generation["table_present"] is True
    assert generation["schema_ok"] is True
    assert generation["error"] == ""
    assert len(generation["summaries"]) == 1
    assert {row["lane"] for row in generation["summaries"]} == {
        "code-reserve1"
    }
    current = next(
        row
        for row in generation["summaries"]
        if row["checkpoint_n"] == 13 and row["lane"] == "code-reserve1"
    )
    assert current["first_window_n"] == 23816
    assert current["last_window_n"] == 23818
    assert current["attempts"] == 4
    assert current["decisive_attempts"] == 3
    assert current["natural_eos_complete"] == 2
    assert current["local_token_limit"] == 1
    assert current["safe_deadline"] == 1
    assert current["natural_eos_before_bound_rate"] == pytest.approx(2 / 3)
    assert "prompt" not in json.dumps(generation).lower()


def test_code_auction_probe_reads_terminal_funnel_15_11_3_1_without_writes(
    tmp_path,
):
    ledger_path = tmp_path / "code-auction.sqlite3"
    connection = sqlite3.connect(ledger_path)
    connection.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA application_id=1380139340;
        PRAGMA user_version=1;
        CREATE TABLE partitions (
            model_repository TEXT NOT NULL,
            checkpoint_revision TEXT NOT NULL,
            checkpoint_n INTEGER NOT NULL,
            public_source_revision TEXT NOT NULL,
            runtime_profile_hash TEXT NOT NULL,
            environment TEXT NOT NULL,
            registered_at REAL NOT NULL,
            miner_source_revision TEXT NOT NULL
        );
        CREATE TABLE attempts (
            attempt_key TEXT NOT NULL,
            model_repository TEXT NOT NULL,
            checkpoint_revision TEXT NOT NULL,
            checkpoint_n INTEGER NOT NULL,
            public_source_revision TEXT NOT NULL,
            runtime_profile_hash TEXT NOT NULL,
            environment TEXT NOT NULL,
            window_n INTEGER NOT NULL,
            created_at REAL NOT NULL
        );
        CREATE TABLE events (
            event_key TEXT NOT NULL,
            observed_at REAL NOT NULL,
            model_repository TEXT NOT NULL,
            checkpoint_revision TEXT NOT NULL,
            checkpoint_n INTEGER NOT NULL,
            public_source_revision TEXT NOT NULL,
            runtime_profile_hash TEXT NOT NULL,
            environment TEXT NOT NULL,
            attempt_key TEXT NOT NULL,
            window_n INTEGER NOT NULL,
            merkle_root TEXT NOT NULL,
            lifecycle TEXT NOT NULL,
            accepted INTEGER,
            accepted_into_pool INTEGER,
            selected_for_batch INTEGER,
            rewarded INTEGER,
            reason_code TEXT NOT NULL DEFAULT '',
            reject_stage TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE generation_outcomes (
            attempt_key TEXT NOT NULL,
            observed_at REAL NOT NULL,
            model_repository TEXT NOT NULL,
            checkpoint_revision TEXT NOT NULL,
            checkpoint_n INTEGER NOT NULL,
            public_source_revision TEXT NOT NULL,
            runtime_profile_hash TEXT NOT NULL,
            environment TEXT NOT NULL,
            window_n INTEGER NOT NULL,
            lane TEXT NOT NULL,
            termination TEXT NOT NULL
        );
        CREATE TABLE terminal_events (
            event_key TEXT PRIMARY KEY,
            observed_at REAL NOT NULL,
            model_repository TEXT NOT NULL,
            checkpoint_revision TEXT NOT NULL,
            checkpoint_n INTEGER NOT NULL,
            public_source_revision TEXT NOT NULL,
            runtime_profile_hash TEXT NOT NULL,
            environment TEXT NOT NULL,
            attempt_key TEXT NOT NULL,
            window_n INTEGER NOT NULL,
            stage TEXT NOT NULL,
            merkle_root TEXT NOT NULL,
            http_provisional INTEGER,
            accepted_into_pool INTEGER,
            selected INTEGER,
            rewarded INTEGER
        );
        CREATE TABLE ledger_extensions (
            extension_name TEXT PRIMARY KEY,
            api_version INTEGER NOT NULL,
            storage_mode TEXT NOT NULL
        );
        """
    )
    repository = "ReliquaryForge/qwen3.5-2b-reliquary-v3"
    checkpoint_revision = "c" * 40
    public_source = "a" * 40
    runtime_profile = "d" * 64
    partition = (
        repository,
        checkpoint_revision,
        13,
        public_source,
        runtime_profile,
        "opencodeinstruct",
    )
    connection.execute(
        "INSERT INTO partitions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (*partition, 123.0, "b" * 40),
    )
    connection.execute(
        "INSERT INTO ledger_extensions VALUES (?, ?, ?)",
        ("terminal_funnel", 2, "v1_additive_compat"),
    )
    connection.executemany(
        "INSERT INTO attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (f"attempt-{index}", *partition, 23849, 100.0 + index)
            for index in range(15)
        ],
    )
    terminal_insert = (
        "INSERT INTO terminal_events VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    connection.executemany(
        terminal_insert,
        [
            (
                f"http-{index}",
                200.0 + index,
                *partition,
                f"attempt-{index}",
                23849,
                "immediate_response",
                f"merkle-{index}",
                1,
                None,
                None,
                None,
            )
            for index in range(11)
        ],
    )
    connection.executemany(
        terminal_insert,
        [
            (
                f"pool-{index}",
                220.0 + index,
                *partition,
                f"attempt-{index}",
                23849,
                "pool_accepted",
                f"merkle-{index}",
                None,
                1,
                None,
                None,
            )
            for index in range(3)
        ],
    )
    connection.execute(
        terminal_insert,
        (
            "selected-0",
            240.0,
            *partition,
            "attempt-0",
            23849,
            "selected",
            "merkle-0",
            None,
            None,
            1,
            None,
        ),
    )
    connection.execute(
        terminal_insert,
        (
            "rewarded-0",
            241.0,
            *partition,
            "attempt-0",
            23849,
            "rewarded",
            "merkle-0",
            None,
            None,
            None,
            1,
        ),
    )
    # A drand ticket expiry is a proven pre-network local drop only while no
    # network-stage fact exists for the same attempt. The second expiry below
    # has a precommit observation and must remain unresolved; an ordinary
    # transport failure is always unresolved.
    legacy_transport_insert = (
        "INSERT INTO events (event_key, observed_at, model_repository, "
        "checkpoint_revision, checkpoint_n, public_source_revision, "
        "runtime_profile_hash, environment, attempt_key, window_n, "
        "lifecycle, merkle_root, accepted, reason_code, reject_stage) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    connection.executemany(
        legacy_transport_insert,
        [
            (
                "legacy-expired-local",
                242.0,
                *partition,
                "attempt-12",
                23849,
                "transport_error",
                "merkle-12",
                0,
                "_DrandSendTicketExpired",
                "",
            ),
            (
                "legacy-expired-after-precommit",
                243.0,
                *partition,
                "attempt-13",
                23849,
                "transport_error",
                "merkle-13",
                0,
                "_DrandSendTicketExpired",
                "",
            ),
            (
                "legacy-timeout",
                244.0,
                *partition,
                "attempt-14",
                23849,
                "transport_error",
                "merkle-14",
                0,
                "ReadTimeout",
                "",
            ),
        ],
    )
    connection.executemany(
        terminal_insert,
        [
            (
                "typed-expired-local",
                245.0,
                *partition,
                "attempt-12",
                23849,
                "terminal_unresolved",
                "merkle-12",
                None,
                None,
                None,
                None,
            ),
            (
                "typed-expired-after-precommit",
                246.0,
                *partition,
                "attempt-13",
                23849,
                "terminal_unresolved",
                "merkle-13",
                None,
                None,
                None,
                None,
            ),
            (
                "typed-precommit",
                247.0,
                *partition,
                "attempt-13",
                23849,
                "precommit_sent",
                "merkle-13",
                None,
                None,
                None,
                None,
            ),
            (
                "typed-timeout",
                248.0,
                *partition,
                "attempt-14",
                23849,
                "terminal_unresolved",
                "merkle-14",
                None,
                None,
                None,
                None,
            ),
        ],
    )
    # The miner deliberately materializes immutable public-population R2
    # outcomes in the same table under a reserved attempt-key namespace.
    # They inform population intelligence, but must never be attributed to
    # this operator's local submission funnel.
    connection.executemany(
        terminal_insert,
        [
            (
                f"r2-population-{stage}",
                250.0 + ordinal,
                *partition,
                "r2:population-selected",
                23849,
                stage,
                "r2-merkle-selected",
                None,
                accepted_into_pool,
                selected,
                rewarded,
            )
            for ordinal, (stage, accepted_into_pool, selected, rewarded) in enumerate(
                (
                    ("pool_accepted", 1, None, None),
                    ("selected", None, 1, None),
                    ("rewarded", None, None, 1),
                )
            )
        ],
    )
    connection.executemany(
        terminal_insert,
        [
            (
                f"r2-population-not-selected-{stage}",
                260.0 + ordinal,
                *partition,
                "r2:population-not-selected",
                23849,
                stage,
                "r2-merkle-not-selected",
                None,
                accepted_into_pool,
                selected,
                rewarded,
            )
            for ordinal, (stage, accepted_into_pool, selected, rewarded) in enumerate(
                (
                    ("pool_accepted", 1, None, None),
                    ("not_selected", None, 0, None),
                    ("not_rewarded", None, None, 0),
                )
            )
        ],
    )
    connection.commit()
    connection.close()
    before = ledger_path.read_bytes()

    env_path = tmp_path / "code.env"
    env_path.write_text(
        "RELIQUARY_ENVIRONMENT_NAME=opencodeinstruct\n"
        "RELIQUARY_ENGINE_MODE=reference\n"
        "RELIQUARY_LANE_ID=code-reserve1\n"
        "RELIQUARY_REFERENCE_CODE_PRESCREEN_ENABLED=1\n"
        "RELIQUARY_CODE_AUCTION_POLICY=deadline_aware\n"
        f"RELIQUARY_CODE_OUTCOME_LEDGER={ledger_path}\n"
    )
    source = fleet._CODE_AUCTION_READINESS_PROBE_SOURCE.replace(
        '"/srv/reliquary-miner-pro/state"', repr(str(tmp_path))
    )
    result = subprocess.run(
        [
            sys.executable,
            "-",
            str(env_path),
            "",
            "",
            str(tmp_path / "grader.sock"),
            str(tmp_path / "current"),
            "65534",
            str(0o660),
        ],
        input=source,
        text=True,
        capture_output=True,
        timeout=5,
        check=True,
    )
    parsed = fleet._parse_code_auction_probe([result.stdout])
    terminal = parsed["ledger"]["terminal_funnel"]

    assert terminal["extension_table_present"] is True
    assert terminal["terminal_events_table_present"] is True
    assert terminal["schema_ok"] is True
    assert terminal["api_version"] == 2
    assert terminal["storage_mode"] == "v1_additive_compat"
    assert terminal["error"] == ""
    assert len(terminal["summaries"]) == 1
    summary = terminal["summaries"][0]
    assert summary["attempts"] == 15
    assert summary["http_provisional"] == 11
    assert summary["pool_accepted"] == 3
    assert summary["selected"] == 1
    assert summary["rewarded"] == 1
    assert summary["terminal_unresolved"] == 2
    assert "accepted" not in summary
    assert ledger_path.read_bytes() == before


def test_code_auction_probe_parser_is_bounded_strict_and_finite():
    box, _validator = _ready_code_context()
    payload = copy.deepcopy(box.code_auction_probe)
    payload.pop("runtime_attestation")
    payload.update({
        "settings_process_bound": "true",
        "prescreen_enabled": "true",
        "wallet_seed": "must-not-survive",
    })
    payload["grader"].update({
        "socket_ok": "true",
        "bundle_ok": "true",
        "metrics_ok": "true",
        "completion": "must-not-survive",
    })
    payload["ledger"].update({
        "configured": "true",
        "readonly_ok": "true",
        "schema_ok": "true",
        "generation_outcomes": {
            "table_present": "true",
            "schema_ok": "true",
            "summaries": [
                {
                    **payload["ledger"]["partitions"][0],
                    "lane": "code-reserve1",
                    "first_window_n": 23816,
                    "last_window_n": 23818,
                    "last_observed_at": 123.0,
                    "natural_eos_complete": 2,
                    "local_token_limit": 1,
                    "safe_deadline": 99,
                    "natural_eos_before_bound_rate": 0.01,
                    "completion": "must-not-survive",
                }
            ]
            * 80,
        },
        "terminal_funnel": {
            "extension_table_present": "true",
            "terminal_events_table_present": "true",
            "schema_ok": "true",
            "api_version": float("inf"),
            "storage_mode": "x" * 100,
            "summaries": [
                {
                    **payload["ledger"]["partitions"][0],
                    "attempts": 15,
                    "http_provisional": 11,
                    "receipt_reserved": 11,
                    "reveal_sent": 11,
                    "pool_accepted": 3,
                    "selected": 1,
                    "rewarded": 1,
                    "terminal_rejected": 4,
                    "terminal_unresolved": 1,
                    "attempt_key": "must-not-survive",
                    "merkle_root": "must-not-survive",
                }
            ]
            * 80,
        },
    })
    payload["ledger"]["partitions"][0]["registered_at"] = float("inf")
    payload["ledger"]["partitions"] *= 40

    parsed = fleet._parse_code_auction_probe([json.dumps(payload)])

    assert parsed["settings_process_bound"] is False
    assert parsed["prescreen_enabled"] is False
    assert parsed["grader"]["socket_ok"] is False
    assert parsed["grader"]["bundle_ok"] is False
    assert parsed["grader"]["metrics_ok"] is False
    assert parsed["ledger"]["configured"] is False
    assert parsed["ledger"]["readonly_ok"] is False
    assert parsed["ledger"]["schema_ok"] is False
    assert len(parsed["ledger"]["partitions"]) == 32
    assert parsed["ledger"]["partitions"][0]["registered_at"] == 0.0
    generation = parsed["ledger"]["generation_outcomes"]
    assert generation["table_present"] is False
    assert generation["schema_ok"] is False
    assert len(generation["summaries"]) == 32
    assert generation["summaries"][0]["decisive_attempts"] == 3
    assert generation["summaries"][0][
        "natural_eos_before_bound_rate"
    ] == pytest.approx(2 / 3)
    assert generation["summaries"][0]["safe_deadline"] == 99
    terminal = parsed["ledger"]["terminal_funnel"]
    assert terminal["extension_table_present"] is False
    assert terminal["terminal_events_table_present"] is False
    assert terminal["schema_ok"] is False
    assert terminal["api_version"] == 0
    assert terminal["storage_mode"] == "x" * 64
    assert len(terminal["summaries"]) == 32
    assert terminal["summaries"][0]["attempts"] == 15
    assert terminal["summaries"][0]["http_provisional"] == 11
    assert "attempt_key" not in terminal["summaries"][0]
    assert "merkle_root" not in terminal["summaries"][0]
    assert "wallet_seed" not in parsed
    assert "completion" not in parsed["grader"]
    assert "completion" not in generation["summaries"][0]
    assert fleet._parse_code_auction_probe(["not-json"]) == {}
    assert fleet._parse_code_auction_probe(["x" * 131_073]) == {}


def test_collect_parses_and_resets_atomic_code_auction_readiness(monkeypatch):
    box, _validator = _ready_code_context()
    unit = box.active_unit
    state = _box(
        unit=unit.removesuffix(".service"),
        env_file="/srv/reliquary-miner-pro/state/code.env",
    )
    probe = copy.deepcopy(box.code_auction_probe)
    attestation = probe.pop("runtime_attestation")
    responses = [
        (
            "===PROC===\n1\n"
            "===PID===\n4242\n"
            "===ENVFILE===\nok\n"
            "===MINERENV===\n"
            "RELIQUARY_ENVIRONMENT_NAME=opencodeinstruct\n"
            "RELIQUARY_ENGINE_MODE=reference\n"
            f"MINER_PRO_SOURCE_REVISION={'b' * 40}\n"
            f"RELIQUARY_SOURCE_REVISION={'a' * 40}\n"
            "PROVISIONED_OK=1\n"
            "RELIQUARY_PROVISIONED_MODEL_KIND=validator_checkpoint\n"
            "RELIQUARY_PROVISIONED_CHECKPOINT_N=13\n"
            "RELIQUARY_PROVISIONED_MODEL_REPO="
            "ReliquaryForge/qwen3.5-2b-reliquary-v3\n"
            f"RELIQUARY_PROVISIONED_MODEL_REVISION={'c' * 40}\n"
            "===CODEAUCTION===\n"
            f"{json.dumps(probe)}\n"
            "===QUARANTINE===\n{\"active\":false}\n"
            "===REFERENCEPROFILE===\n"
            "validator protocol parity ok "
            "profile=forced_seed_v2_auction_legacy_wire\n"
            f"validator runtime telemetry enabled validator_profile={'d' * 64}\n"
            "validator runtime parity ok\n"
            "pro miner ready\n"
            "===CODEREFERENCEPROFILE===\n"
            f"validator runtime telemetry enabled validator_profile={'d' * 64}\n"
            f"code_auction_readiness {json.dumps(attestation)}\n"
        ),
        (
            "===PROC===\n1\n"
            "===PID===\n4242\n"
            "===ENVFILE===\nok\n"
            "===MINERENV===\n"
            "RELIQUARY_ENVIRONMENT_NAME=openmathinstruct\n"
            "RELIQUARY_ENGINE_MODE=reference\n"
            "===QUARANTINE===\n{\"active\":false}\n"
        ),
    ]

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        assert "===CODEAUCTION===" in command
        assert "CODE_REFERENCE_LOG" in command
        assert 'if [ -n "$INVOCATION_ID" ]; then' in command
        assert unit in command
        return 0, responses.pop(0), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert state.active_pid == 4242
    assert state.miner_unit_enablement == "enabled"
    assert state.runtime_profile_hash == "d" * 64
    assert state.code_auction_probe["prescreen_enabled"] is True
    assert state.code_auction_probe["runtime_attestation"] == attestation
    assert state.code_auction_probe["ledger"]["partitions"][0][
        "checkpoint_n"
    ] == 13

    fleet.collect_box(state)

    assert state.active_environment == "openmathinstruct"
    assert state.miner_unit_enablement == ""
    assert state.runtime_profile_hash == ""
    assert state.code_auction_probe == {}


def test_code_auction_ready_surfaces_allow_registered_empty_partition(
    monkeypatch,
):
    import fleet_web

    box, validator = _ready_code_context()
    now = box.last_poll_s + 1
    window = _window(23805, {OTHER_HOTKEY: 1.0})
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", validator)
    monkeypatch.setattr(fleet_web, "_windows", [window])
    monkeypatch.setattr(fleet_web, "_last_poll_at", now - 1)
    monkeypatch.setattr(fleet_web, "_poll_count", 1)
    monkeypatch.setattr(fleet_web, "_chain", fleet.ChainState())
    monkeypatch.setattr(
        fleet_web,
        "_r2_status_snapshot",
        lambda: {
            "last_success_at": now - 1,
            "latest_window": 23805,
            "coverage_complete": True,
            "coverage": {"complete": True},
        },
    )

    details = fleet_web._box_code_auction_details(box, validator)
    fleet_html = fleet_web.render_fleet_html()
    pipeline_html = fleet_web.render_pipeline_html()
    summary_html = fleet_web.render_fleet_summary_html()
    exported = fleet_web.render_export_json()["fleet"][0]
    health = fleet_web.compute_healthz(5, now=now)

    assert details["applicable"] is True
    assert details["ok"] is True
    assert details["issues"] == []
    assert details["ledger"]["current_partition"]["checkpoint_n"] == 13
    assert details["ledger"]["generation_outcomes"]["current"] is None
    assert "auction ready" in fleet_html
    assert "generation outcomes: legacy ledger" in fleet_html
    assert "auction-blocked" not in pipeline_html
    assert "HEALTHY" in summary_html
    assert exported["miner_unit_enablement"] == "enabled"
    assert exported["code_auction"]["ok"] is True
    assert health["ok"] is True
    assert health["fleet"][0]["code_auction"]["ok"] is True
    serialized = json.dumps(exported)
    assert "prompt_content_hash" not in serialized
    assert "completion" not in serialized


def test_code_terminal_funnel_surfaces_exact_current_15_11_3_1(
    monkeypatch,
):
    import fleet_web

    box, validator = _ready_code_context()
    _attach_terminal_funnel_summary(box)
    now = box.last_poll_s + 1
    window = _window(23805, {OTHER_HOTKEY: 1.0})
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", validator)
    monkeypatch.setattr(fleet_web, "_windows", [window])
    monkeypatch.setattr(fleet_web, "_last_poll_at", now - 1)
    monkeypatch.setattr(fleet_web, "_poll_count", 1)
    monkeypatch.setattr(fleet_web, "_chain", fleet.ChainState())
    monkeypatch.setattr(
        fleet_web,
        "_r2_status_snapshot",
        lambda: {
            "last_success_at": now - 1,
            "latest_window": 23805,
            "coverage_complete": True,
            "coverage": {"complete": True},
        },
    )

    details = fleet_web._box_code_auction_details(box, validator)
    terminal = details["ledger"]["terminal_funnel"]
    current = terminal["current"]
    fleet_html = fleet_web.render_fleet_html()
    exported = fleet_web.render_export_json()["fleet"][0]["code_auction"]
    health = fleet_web.compute_healthz(5, now=now)

    assert details["ok"] is True
    assert terminal["api_version"] == 2
    assert terminal["storage_mode"] == "v1_additive_compat"
    assert terminal["legacy_compatible"] is False
    assert current["attempts"] == 15
    assert current["http_provisional"] == 11
    assert current["pool_accepted"] == 3
    assert current["selected"] == 1
    assert current["rewarded"] == 1
    assert "accepted" not in current
    assert "terminal funnel (schema v2): attempts=15" in fleet_html
    assert "HTTP provisional=11" in fleet_html
    assert "pool accepted=3" in fleet_html
    assert "selected=1" in fleet_html
    assert "rewarded=1" in fleet_html
    assert exported["ledger"]["terminal_funnel"]["current"] == current
    assert health["ok"] is True
    assert health["fleet"][0]["code_auction"]["ledger"][
        "terminal_funnel"
    ]["current"] == current


def test_code_terminal_funnel_accepts_paid_untrained_on_boundary_source():
    import fleet_web

    box, validator = _ready_code_context()
    boundary_source = "8835a95e5a7aa6065eef4691760716be004f88cc"
    box.reliquary_source_revision = boundary_source
    box.code_auction_probe["runtime_attestation"][
        "public_source_revision"
    ] = boundary_source
    box.code_auction_probe["runtime_attestation"][
        "grader_source_revision"
    ] = boundary_source
    box.code_auction_probe["grader"][
        "bundle_source_revision"
    ] = boundary_source
    partition = box.code_auction_probe["ledger"]["partitions"][0]
    partition["public_source_revision"] = boundary_source
    validator.image_revision = boundary_source
    validator.health_raw["image_revision"] = boundary_source
    _attach_terminal_funnel_summary(
        box,
        attempts=15,
        pool_accepted=3,
        selected=1,
        rewarded=3,
    )

    details = fleet_web._box_code_auction_details(box, validator)
    terminal = details["ledger"]["terminal_funnel"]

    assert details["ok"] is True
    assert terminal["payout_profile"] == "boundary_fair_split"
    assert terminal["counts_valid"] is True
    assert terminal["current"]["selected"] == 1
    assert terminal["current"]["rewarded"] == 3


def test_code_terminal_funnel_rejects_paid_untrained_on_unreviewed_source():
    import fleet_web

    box, validator = _ready_code_context()
    _attach_terminal_funnel_summary(
        box,
        attempts=15,
        pool_accepted=3,
        selected=1,
        rewarded=3,
    )

    details = fleet_web._box_code_auction_details(box, validator)

    assert details["ok"] is False
    assert (
        details["ledger"]["terminal_funnel"]["payout_profile"]
        == "strict_top8"
    )
    assert "code_auction:terminal_funnel_counts_invalid" in details["issues"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("api_version", 99),
        ("storage_mode", "unknown"),
        ("schema_ok", False),
        ("terminal_events_table_present", False),
    ],
)
def test_code_terminal_funnel_extension_mismatch_degrades_health(
    monkeypatch,
    field,
    value,
):
    import fleet_web

    box, validator = _ready_code_context()
    _attach_terminal_funnel_summary(box)
    box.code_auction_probe["ledger"]["terminal_funnel"][field] = value
    now = box.last_poll_s + 1
    window = _window(23805, {OTHER_HOTKEY: 1.0})
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", validator)
    monkeypatch.setattr(fleet_web, "_windows", [window])
    monkeypatch.setattr(fleet_web, "_last_poll_at", now - 1)
    monkeypatch.setattr(fleet_web, "_poll_count", 1)
    monkeypatch.setattr(fleet_web, "_chain", fleet.ChainState())
    monkeypatch.setattr(
        fleet_web,
        "_r2_status_snapshot",
        lambda: {
            "last_success_at": now - 1,
            "latest_window": 23805,
            "coverage_complete": True,
            "coverage": {"complete": True},
        },
    )

    details = fleet_web._box_code_auction_details(box, validator)
    health = fleet_web.compute_healthz(5, now=now)

    assert details["ok"] is False
    assert "code_auction:terminal_funnel_incompatible" in details["issues"]
    assert health["ok"] is False
    assert health["checks"]["fleet_ready"] is False
    assert "code_auction:terminal_funnel_incompatible" in health["fleet"][0][
        "issues"
    ]


def test_code_auction_surfaces_exact_current_lane_generation_outcomes(
    monkeypatch,
):
    import fleet_web

    box, validator = _ready_code_context()
    _attach_generation_summary(box)
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", validator)
    monkeypatch.setattr(fleet_web, "_last_poll_at", box.last_poll_s)
    monkeypatch.setattr(fleet_web, "_poll_count", 1)

    details = fleet_web._box_code_auction_details(box, validator)
    generation = details["ledger"]["generation_outcomes"]
    current = generation["current"]
    fleet_html = fleet_web.render_fleet_html()
    exported = fleet_web.render_export_json()["fleet"][0]["code_auction"]

    assert details["ok"] is True
    assert generation["lane_matches"] is True
    assert generation["expected_lane"] == "code-reserve1"
    assert generation["process_lane"] == "code-reserve1"
    assert current["attempts"] == 104
    assert current["decisive_attempts"] == 4
    assert current["natural_eos_complete"] == 3
    assert current["local_token_limit"] == 1
    assert current["safe_deadline"] == 100
    assert current["natural_eos_before_bound_rate"] == pytest.approx(0.75)
    assert "completed=3" in fleet_html
    assert "local_token_limit=1" in fleet_html
    assert "safe_deadline=100" in fleet_html
    assert "natural-EOS/decisive=75.0%" in fleet_html
    assert exported["ledger"]["generation_outcomes"]["current"] == current
    serialized = json.dumps(exported)
    assert "prompt_content_hash" not in serialized
    assert "attempt_key" not in serialized


def test_code_auction_deadline_only_outcomes_do_not_become_eos_failures():
    import fleet_web

    box, validator = _ready_code_context()
    _attach_generation_summary(
        box,
        completed=0,
        local_token_limit=0,
        safe_deadline=7,
    )

    details = fleet_web._box_code_auction_details(box, validator)
    current = details["ledger"]["generation_outcomes"]["current"]

    assert details["ok"] is True
    assert current["attempts"] == 7
    assert current["decisive_attempts"] == 0
    assert current["natural_eos_before_bound_rate"] is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("model_repository", "ReliquaryForge/wrong"),
        ("checkpoint_revision", "e" * 40),
        ("checkpoint_n", 12),
        ("public_source_revision", "e" * 40),
        ("runtime_profile_hash", "e" * 64),
        ("environment", "openmathinstruct"),
        ("lane", "code-reserve2"),
    ],
)
def test_code_generation_outcomes_require_exact_partition_and_lane(
    field, value
):
    import fleet_web

    box, validator = _ready_code_context()
    summary = _attach_generation_summary(box)
    summary[field] = value

    details = fleet_web._box_code_auction_details(box, validator)

    assert details["ok"] is True
    assert details["ledger"]["generation_outcomes"]["current"] is None


def test_code_generation_outcomes_require_process_lane_match():
    import fleet_web

    box, validator = _ready_code_context()
    _attach_generation_summary(box)
    box.code_auction_probe["lane"] = "code-reserve2"

    details = fleet_web._box_code_auction_details(box, validator)
    generation = details["ledger"]["generation_outcomes"]

    assert details["ok"] is True
    assert generation["lane_matches"] is False
    assert generation["current"] is None


@pytest.mark.parametrize(
    ("target", "value", "issue"),
    [
        ("miner_unit_enablement", "disabled", "code_auction:miner_unit_disabled"),
        ("settings_process_bound", False, "code_auction:settings_not_process_bound"),
        ("prescreen_enabled", False, "code_auction:prescreen_disabled"),
        ("auction_policy", "legacy", "code_auction:policy_mismatch"),
        (
            "runtime_attestation.ledger_partition_registered",
            False,
            "code_auction:runtime_attestation_missing",
        ),
        ("grader.active_state", "inactive", "code_auction:grader_inactive"),
        ("grader.enablement", "disabled", "code_auction:grader_disabled"),
        ("grader.socket_uid", 1000, "code_auction:grader_socket_invalid"),
        ("grader.socket_mode", "0666", "code_auction:grader_socket_invalid"),
        ("grader.bundle_ok", False, "code_auction:grader_bundle_invalid"),
        ("grader.metrics_ok", False, "code_auction:grader_canary_missing"),
        ("ledger.readonly_ok", False, "code_auction:ledger_unavailable"),
        ("ledger.schema_ok", False, "code_auction:ledger_schema_invalid"),
    ],
)
def test_code_auction_readiness_fails_closed_for_each_runtime_gate(
    target, value, issue
):
    import fleet_web

    box, validator = _ready_code_context()
    node = box.code_auction_probe
    path = target.split(".")
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value

    details = fleet_web._box_code_auction_details(box, validator)

    assert details["ok"] is False
    assert issue in details["issues"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("model_repository", "ReliquaryForge/wrong"),
        ("checkpoint_revision", "e" * 40),
        ("checkpoint_n", 12),
        ("public_source_revision", "e" * 40),
        ("runtime_profile_hash", "e" * 64),
        ("environment", "openmathinstruct"),
    ],
)
def test_code_auction_ledger_requires_exact_six_field_current_partition(
    field, value
):
    import fleet_web

    box, validator = _ready_code_context()
    box.code_auction_probe["ledger"]["partitions"][0][field] = value

    details = fleet_web._box_code_auction_details(box, validator)

    assert details["ok"] is False
    assert "code_auction:ledger_current_partition_missing" in details["issues"]
    assert details["ledger"]["current_partition"] is None


def test_code_auction_uses_current_health_identity_and_ignores_socket_group():
    import fleet_web

    box, validator = _ready_code_context()
    box.code_auction_probe["grader"]["socket_gid"] = 1234
    assert fleet_web._box_code_auction_details(box, validator)["ok"] is True

    validator.health_raw = {}
    details = fleet_web._box_code_auction_details(box, validator)

    assert details["ok"] is False
    assert "code_auction:validator_source_pending" in details["issues"]
    assert "code_auction:runtime_profile_pending" in details["issues"]


def test_code_auction_blockers_are_red_in_pipeline_and_ops(monkeypatch):
    import fleet_web

    box, validator = _ready_code_context()
    box.code_auction_probe["prescreen_enabled"] = False
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", validator)
    monkeypatch.setattr(fleet_web, "_windows", [])
    monkeypatch.setattr(fleet_web, "_chain", fleet.ChainState())
    monkeypatch.setattr(fleet_web, "_mission_current_events", lambda: [])

    assert "auction-blocked" in fleet_web.render_pipeline_html()
    summary = fleet_web.render_fleet_summary_html()
    assert "ALERT" in summary
    assert "exact Code pre-screen disabled" in summary


def test_math_reference_lane_does_not_inherit_code_auction_gates(monkeypatch):
    import fleet_web

    box = _box(
        proc_alive=True,
        active_environment="openmathinstruct",
        miner_environment="openmathinstruct",
        engine_mode="reference",
        code_auction_probe={},
    )

    validator = fleet.ValidatorState()
    details = fleet_web._box_code_auction_details(box, validator)
    issues = fleet_web._box_readiness_issues(
        box, validator_state=validator
    )
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", validator)
    rendered = fleet_web.render_fleet_html()

    assert details == {"applicable": False, "ok": None, "issues": []}
    assert not any(issue.startswith("code_auction:") for issue in issues)
    assert "non-Code" in rendered
    assert "generation outcomes: legacy ledger" not in rendered


def test_settings_builds_host_wide_allowlist_for_dual_code_rows(
    monkeypatch, tmp_path
):
    unit0 = "reliquary-miner-pro@code-reserve1.service"
    unit1 = "reliquary-miner-pro@code-reserve2.service"
    monkeypatch.setattr(settings, "SETTINGS", settings.Settings())
    monkeypatch.setattr(fleet, "FLEET", [])
    monkeypatch.setattr(fleet, "FLEET_ENV_FILES", {})
    monkeypatch.setattr(fleet, "FLEET_UNIT_CANDIDATES", {})
    monkeypatch.setattr(fleet, "FLEET_HOST_UNIT_ALLOWLIST", {})
    config = tmp_path / "config.yaml"
    config.write_text(
        "fleet:\n"
        "  - alias: miner-host\n"
        f"    hotkey: {OUR_HOTKEY}\n"
        "    label: code0\n"
        "    color: cyan\n"
        f"    unit: {unit0}\n"
        "    env_file: /state/code0.env\n"
        "  - alias: miner-host\n"
        f"    hotkey: {OUR_HOTKEY}\n"
        "    label: code1\n"
        "    color: magenta\n"
        f"    unit: {unit1}\n"
        "    env_file: /state/code1.env\n"
    )

    settings.load(config)
    settings.apply_to_fleet_module()

    expected = [unit0, unit1]
    assert fleet.FLEET_HOST_UNIT_ALLOWLIST == {
        "code0": expected,
        "code1": expected,
    }
    assert fleet.FLEET_UNIT_CANDIDATES == {}


def test_settings_builds_exact_single_math_primary_topology(
    monkeypatch, tmp_path
):
    unit = "reliquary-miner-pro@math-primary.service"
    env_file = "/srv/reliquary-miner-pro/state/miner-pro-math-primary.env"
    monkeypatch.setattr(settings, "SETTINGS", settings.Settings())
    monkeypatch.setattr(fleet, "FLEET", [])
    monkeypatch.setattr(fleet, "FLEET_ENV_FILES", {})
    monkeypatch.setattr(fleet, "FLEET_UNIT_CANDIDATES", {})
    monkeypatch.setattr(fleet, "FLEET_HOST_UNIT_ALLOWLIST", {})
    monkeypatch.setattr(fleet, "OUR_SS58", {})
    config = tmp_path / "config.yaml"
    config.write_text(
        "fleet:\n"
        "  - alias: h100-host\n"
        f"    hotkey: {OUR_HOTKEY}\n"
        "    label: h100-math-primary\n"
        "    color: cyan\n"
        f"    unit: {unit}\n"
        f"    env_file: {env_file}\n"
    )

    settings.load(config)
    settings.apply_to_fleet_module()

    assert len(fleet.FLEET) == 1
    assert fleet.FLEET[0][0].endswith(" h100-host")
    assert fleet.FLEET[0][1:] == (
        OUR_HOTKEY,
        "h100-math-primary",
        "cyan",
        unit,
    )
    assert fleet.FLEET_ENV_FILES == {"h100-math-primary": env_file}
    assert fleet.FLEET_UNIT_CANDIDATES == {}
    assert fleet.FLEET_HOST_UNIT_ALLOWLIST == {
        "h100-math-primary": [unit]
    }
    assert fleet.OUR_SS58 == {OUR_HOTKEY: "h100-math-primary"}


@pytest.mark.parametrize(
    ("target_unit", "target_env", "target_pid"),
    [
        (
            "reliquary-miner-pro@code-reserve1.service",
            "/state/code0.env",
            4101,
        ),
        (
            "reliquary-miner-pro@code-reserve2.service",
            "/state/code1.env",
            4102,
        ),
    ],
)
def test_collects_each_dual_code_lane_while_sibling_is_active(
    monkeypatch, target_unit, target_env, target_pid
):
    units = (
        "reliquary-miner-pro@code-reserve1.service",
        "reliquary-miner-pro@code-reserve2.service",
    )
    state = _box(
        unit=target_unit,
        env_file=target_env,
        host_unit_allowlist=units,
    )
    calls: list[str] = []

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        calls.append(command)
        if "PYUNIT" in command:
            assert all(unit in command for unit in units)
            assert "row['unit'] in collectable and is_contender(row)" in command
            return 0, json.dumps({
                "active": {
                    "unit": target_unit,
                    "active_state": "active",
                    "sub_state": "running",
                    "pid": target_pid,
                    "restarts": 0,
                },
                "candidates": [
                    {
                        "unit": unit,
                        "active_state": "active",
                        "sub_state": "running",
                        "pid": 4101 + index,
                    }
                    for index, unit in enumerate(units)
                ],
                "unexpected": [],
                "error": "",
            }), ""
        assert target_env in command
        assert f"journalctl -u {target_unit}" in command
        return 0, (
            "===PROC===\n1\n"
            f"===PID===\n{target_pid}\n"
            "===ENVFILE===\nok\n"
            "===MINERENV===\n"
            "RELIQUARY_ENVIRONMENT_NAME=opencodeinstruct\n"
            "RELIQUARY_ENGINE_MODE=reference\n"
            "===QUARANTINE===\n{\"active\":false}\n"
        ), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert len(calls) == 3
    assert state.unit_resolution_error == ""
    assert state.proc_alive is True
    assert state.active_unit == target_unit
    assert state.active_pid == target_pid
    assert state.active_environment == "opencodeinstruct"


@pytest.mark.parametrize(
    ("drift", "issue"),
    [
        ("environment", "code_auction:environment_mismatch"),
        ("engine", "code_auction:engine_mode_mismatch"),
    ],
)
def test_configured_code_lane_drift_remains_applicable_and_blocked(
    drift, issue
):
    import fleet_web

    box, validator = _ready_code_context()
    if drift == "environment":
        box.active_environment = "openmathinstruct"
        box.miner_environment = "openmathinstruct"
        box.code_auction_probe["environment"] = "openmathinstruct"
    else:
        box.engine_mode = "legacy"
        box.code_auction_probe["engine_mode"] = "legacy"

    details = fleet_web._box_code_auction_details(box, validator)

    assert details["applicable"] is True
    assert details["ok"] is False
    assert issue in details["issues"]


@pytest.mark.parametrize(
    ("failure", "issue"),
    [
        ("error", "code_auction:validator_health_error"),
        ("stale", "code_auction:validator_health_stale"),
    ],
)
def test_code_readiness_never_uses_failed_or_stale_validator_health(
    failure, issue
):
    import fleet_web

    box, validator = _ready_code_context()
    now = fleet.time.time()
    validator.health_last_fetch_at = now
    if failure == "error":
        validator.health_error = "TimeoutError"
    else:
        validator.health_last_fetch_at = now - 61

    details = fleet_web._box_code_auction_details(
        box,
        validator,
        now=now,
        health_stale_s=60,
    )

    assert details["ok"] is False
    assert issue in details["issues"]
    assert details["validator_health"]["fresh"] is False
    assert details["validator_health"]["response_fresh"] is False
    assert details["source"]["validator_revision"] == ""
    assert details["runtime_profile"]["validator"] == ""
    assert details["ledger"]["expected_partition"]["checkpoint_n"] is None


def test_code_readiness_retains_identity_from_fresh_degraded_health():
    import fleet_web

    box, validator = _ready_code_context()
    now = fleet.time.time()
    validator.health_last_fetch_at = now
    validator.health_status = "degraded"

    details = fleet_web._box_code_auction_details(
        box,
        validator,
        now=now,
        health_stale_s=60,
    )

    assert details["ok"] is False
    assert "code_auction:validator_health_status" in details["issues"]
    assert "code_auction:validator_source_pending" not in details["issues"]
    assert "code_auction:runtime_profile_pending" not in details["issues"]
    assert "code_auction:ledger_partition_identity_pending" not in details["issues"]
    assert details["validator_health"]["fresh"] is False
    assert details["validator_health"]["response_fresh"] is True
    assert details["source"]["exact_match"] is True
    assert details["runtime_profile"]["exact_match"] is True
    assert details["ledger"]["expected_partition"]["checkpoint_n"] == 13
    assert details["ledger"]["current_partition"]["checkpoint_n"] == 13


@pytest.mark.parametrize(
    ("surface", "failure", "expected_label"),
    [
        ("poll", "stale", "stale"),
        ("state", "stale", "stale"),
        ("health", "stale", "stale"),
        ("verdicts", "stale", "stale"),
        ("state", "error", "endpoint error"),
        ("health", "error", "endpoint error"),
        ("verdicts", "error", "endpoint error"),
    ],
)
def test_validator_badge_requires_every_poll_surface_fresh(
    surface, failure, expected_label
):
    import fleet_web

    now = 50_000.0
    _box_state, validator = _ready_code_context()
    validator.last_fetch_at = now
    validator.health_last_fetch_at = now
    validator.verdicts_last_fetch_at = now
    validator.health_status = "ok"
    poll_at = now

    timestamp_fields = {
        "state": "last_fetch_at",
        "health": "health_last_fetch_at",
        "verdicts": "verdicts_last_fetch_at",
    }
    error_fields = {
        "state": "error",
        "health": "health_error",
        "verdicts": "verdicts_error",
    }
    if failure == "stale":
        if surface == "poll":
            poll_at = now - 61
        else:
            setattr(validator, timestamp_fields[surface], now - 61)
    else:
        setattr(validator, error_fields[surface], "ReadTimeout")

    details = fleet_web._validator_badge_health_details(
        validator,
        poll_at=poll_at,
        now=now,
        stale_after_s=60,
    )

    assert details["ok"] is False
    assert details["label"] == expected_label


def test_validator_panels_do_not_render_cached_ok_during_endpoint_error(
    monkeypatch,
):
    import fleet_web

    now = 50_000.0
    _box_state, validator = _ready_code_context()
    validator.last_fetch_at = now
    validator.health_last_fetch_at = now - 300
    validator.verdicts_last_fetch_at = now - 300
    validator.health_status = "ok"
    validator.error = "ReadTimeout"
    validator.health_error = "ReadTimeout"
    validator.verdicts_error = "ReadTimeout"
    monkeypatch.setattr(fleet_web.time, "time", lambda: now)
    monkeypatch.setattr(fleet_web, "_vs", validator)
    monkeypatch.setattr(fleet_web, "_windows", [])
    monkeypatch.setattr(fleet_web, "_last_poll_at", now)
    monkeypatch.setattr(fleet_web, "validator_events_in_window", lambda _s: [])

    detail = fleet_web.render_validator_html()
    rundown = fleet_web.render_validator_rundown_html()

    assert ">endpoint error</span>" in detail
    assert ">endpoint error</span>" in rundown
    assert ">ok</span>" not in detail
    assert ">healthy</span>" not in rundown


def test_fresh_degraded_badges_keep_exact_code_identity(monkeypatch):
    import fleet_web

    now = fleet.time.time()
    box, validator = _ready_code_context()
    validator.last_fetch_at = now
    validator.health_last_fetch_at = now
    validator.verdicts_last_fetch_at = now
    validator.health_status = "degraded"
    monkeypatch.setattr(fleet_web.time, "time", lambda: now)
    monkeypatch.setattr(fleet_web, "_vs", validator)
    monkeypatch.setattr(fleet_web, "_windows", [])
    monkeypatch.setattr(fleet_web, "_last_poll_at", now)
    monkeypatch.setattr(fleet_web, "validator_events_in_window", lambda _s: [])

    code = fleet_web._box_code_auction_details(
        box,
        validator,
        now=now,
        health_stale_s=60,
    )
    detail = fleet_web.render_validator_html()
    rundown = fleet_web.render_validator_rundown_html()

    assert code["validator_health"]["response_fresh"] is True
    assert code["source"]["exact_match"] is True
    assert code["runtime_profile"]["exact_match"] is True
    assert code["ledger"]["current_partition"]["checkpoint_n"] == 13
    assert ">degraded</span>" in detail
    assert ">degraded</span>" in rundown
    assert ">healthy</span>" not in rundown


def test_code_attestation_never_falls_back_to_whole_unit_journal(monkeypatch):
    box, _validator = _ready_code_context()
    unit = box.active_unit
    state = _box(unit=unit, env_file="/state/code.env")
    probe = copy.deepcopy(box.code_auction_probe)
    attestation = probe.pop("runtime_attestation")
    probe["invocation_id"] = ""

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        strict = command.split("CODE_REFERENCE_LOG=$(", 1)[1].split(
            "); echo '===REFERENCEPROFILE==='", 1
        )[0]
        assert 'if [ -n "$INVOCATION_ID" ]; then' in strict
        assert "else" not in strict
        return 0, (
            "===PROC===\n1\n"
            "===PID===\n4242\n"
            "===ENVFILE===\nok\n"
            "===MINERENV===\n"
            "RELIQUARY_ENVIRONMENT_NAME=opencodeinstruct\n"
            "RELIQUARY_ENGINE_MODE=reference\n"
            "===CODEAUCTION===\n"
            f"{json.dumps(probe)}\n"
            "===QUARANTINE===\n{\"active\":false}\n"
            "===REFERENCEPROFILE===\n"
            "validator protocol parity ok "
            "profile=forced_seed_v2_auction_legacy_wire\n"
            f"validator runtime telemetry enabled validator_profile={'d' * 64}\n"
            f"code_auction_readiness {json.dumps(attestation)}\n"
            "validator runtime parity ok\n"
            "pro miner ready\n"
        ), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    fleet.collect_box(state)

    assert state.code_auction_probe["invocation_id"] == ""
    assert state.code_auction_probe["runtime_attestation"] == {}
    assert state.runtime_profile_hash == ""


def test_submit_disabled_labs_are_separate_from_live_fleet_identity(monkeypatch):
    configured = settings.Settings(
        fleet=[
            settings.FleetBox(
                alias="ubuntu@192.0.2.10",
                hotkey=OUR_HOTKEY,
                label="h100-code",
                color="cyan",
                unit="reliquary-miner-pro@code-reserve1.service",
            )
        ],
        labs=[
            settings.LabBox(
                alias="root@192.0.2.20",
                label="b200-math-lab",
                color="yellow",
                unit="reliquary-offline-math-evidence-c48.service",
                evidence_db="/srv/reliquary/state/math.sqlite3",
            )
        ],
    )
    monkeypatch.setattr(settings, "SETTINGS", configured)
    monkeypatch.setattr(fleet, "FLEET", [])
    monkeypatch.setattr(fleet, "LABS", [])
    monkeypatch.setattr(fleet, "OUR_SS58", {})

    settings.apply_to_fleet_module()

    assert fleet.FLEET[0][1] == OUR_HOTKEY
    assert fleet.LABS == [
        (
            "root@192.0.2.20",
            "b200-math-lab",
            "yellow",
            "reliquary-offline-math-evidence-c48.service",
            "/srv/reliquary/state/math.sqlite3",
            "/srv/reliquary-miner-pro/state/source-manifest.env",
            "",
        )
    ]
    assert fleet.OUR_SS58 == {OUR_HOTKEY: "h100-code"}


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"hotkey": OUR_HOTKEY}, "must not define a hotkey"),
        ({"evidence_db": "relative.sqlite3"}, "must be an absolute path"),
        ({"unit": "bad/unit"}, "valid systemd unit"),
    ],
)
def test_submit_disabled_lab_config_fails_closed(overrides, message):
    row = {
        "alias": "root@192.0.2.20",
        "label": "offline-lab",
        "color": "yellow",
        "unit": "reliquary-offline-math-evidence-c48.service",
        "evidence_db": "/srv/reliquary/state/math.sqlite3",
    }
    row.update(overrides)

    with pytest.raises(ValueError, match=message):
        settings._coerce_lab_box(row)


def _selector_artifact_document() -> dict:
    document = {
        "artifact_kind": "reliquary_math_selector_catboost_challenger",
        "identity": {
            "checkpoint_n": 48,
            "checkpoint_repository": "ReliquaryForge/model-v3",
            "checkpoint_revision": "3" * 40,
            "public_source_revision": "2" * 40,
        },
        "model_version": "catboost_v2_emitted_slots",
        "policy": {
            "economic_target": "effective_full_slots",
            "objective": (
                "completion_probability_x_expected_effective_full_slots_per_gpu_second"
            ),
            "payout_profile": "boundary_fair_split",
        },
        "schema_version": 1,
        "source": {
            "data_digest": "4" * 64,
            "window_start": 24234,
            "window_end": 24314,
        },
        "status": "shadow_only",
        "training": {
            "completion_terminal_rows": 36,
            "train_local_rows": 55,
            "train_population_rows": 4187,
        },
        "validation": {
            "activation_blocker": "shadow_only_challenger",
            "activation_gate_passed": False,
            "completion_brier": None,
            "emission_base_mse": 0.0925,
            "emission_mse": 0.07125,
            "emitted_slot_lift": 1.625,
            "heldout_value_lift": 0.0284,
            "holdout_rows": 55,
            "holdout_window": 24314,
            "selection_brier": 0.1088,
            "shadow_promotion_gate_passed": True,
            "top_quintile_lift": 0.0,
            "value_multiclass_brier": 0.0727,
        },
    }
    canonical = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    document["checksum"] = hashlib.sha256(canonical).hexdigest()
    return document


def _code_selector_artifact_document() -> dict:
    document = {
        "artifact_kind": "reliquary_code_selector_challenger",
        "blockers": {
            "activation_gate_passed": False,
            "admission_model": "blocked_r2_survivor_population",
            "completion_activation_gate_passed": False,
            "completion_model": "available_from_local_censor_aware_starts",
            "reason": "unconditional validator admission remains unidentifiable",
        },
        "gates": {
            "completion_calibrated": True,
            "completion_top_quintile_lift_1_5x": False,
            "exact_k2_top_quintile_lift_1_5x": False,
            "unconditional_admission_model_available": False,
        },
        "identity": {
            "checkpoint_n": 48,
            "checkpoint_revision": "3" * 40,
            "completion_cap": 4096,
            "environment": "opencodeinstruct",
            "model_repository": "ReliquaryForge/qwen3.5-2b-reliquary-v3",
            "public_source_revision": "2" * 40,
        },
        "model_version": "hierarchical_empirical_bayes_v1",
        "policy": {
            "expires_after_window": 24919,
            "exploration_bps": 2000,
            "objective": (
                "P(complete) * E(emitted_full_slots) / expected_gpu_seconds"
            ),
            "online_activation_allowed": False,
        },
        "schema_version": 1,
        "source": {
            "completion": {
                "natural_completions": 26,
                "rows": 72,
                "safe_deadlines": 46,
                "window_end": 24342,
                "window_start": 24297,
            },
            "dataset_manifest_sha256": "4" * 64,
            "population_rows": 1077,
            "window_end": 24341,
            "window_start": 24283,
        },
        "status": "shadow_only",
        "training": {"backend": "empirical", "task_type": "CPU"},
        "validation": {
            "completion": {
                "completion_brier": 0.4031744354,
                "completion_top_quintile_lift": 1.0,
            },
            "empirical": {
                "aggregate": {
                    "exact_k2_lift": 1.4130434783,
                    "heldout_value_lift": 0.0167581683,
                    "rows": 239,
                    "selection_brier": 0.1718261971,
                },
                "folds": [
                    {"holdout_window": 24340},
                    {"holdout_window": 24341},
                ],
            },
        },
    }
    canonical = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    document["checksum"] = hashlib.sha256(canonical).hexdigest()
    return document


def _selector_search_report_document() -> dict:
    document = {
        "cutoff": {
            "validator_image_revision": "2" * 40,
            "window_end": 24325,
            "window_start": 24249,
        },
        "dataset": {
            "local_attempt_rows": 49,
            "manifest_sha256": "4" * 64,
            "population_rows": 4037,
        },
        "decision": {
            "completion_calibration_gate_passed": False,
            "reason": "activation remains blocked on completion calibration",
            "status": "shadow_only",
        },
        "deterministic_cpu_challenger": {
            "bitwise_reproducible": True,
            "final_cutoff": {
                "activation_gate_passed": False,
                "completion_brier": None,
                "heldout_value_lift": 0.0323722750,
                "holdout_rows": 58,
                "holdout_window": 24325,
                "selection_brier": 0.0858433637,
                "top_quintile_selected_lift": 2.4166666667,
                "value_multiclass_brier": 0.0799518354,
            },
            "rolling_cpu_folds": [],
            "rolling_summary": {
                "folds": 6,
                "folds_positive_value_lift": 6,
                "selection_lift_mean": 1.8131313131,
            },
        },
        "gpu_search": {
            "bitwise_reproducible": True,
            "final_cutoff": {
                "top_quintile_selected_lift": 1.8125,
            },
            "rolling_summary": {
                "folds": 6,
                "folds_positive_value_lift": 6,
                "selection_lift_mean": 1.6063762626,
            },
        },
        "identity": {
            "checkpoint_n": 48,
            "checkpoint_repository": "ReliquaryForge/model-v3",
            "checkpoint_revision": "3" * 40,
            "public_source_revision": "2" * 40,
        },
        "report_kind": "reliquary_math_selector_bounded_search",
        "schema_version": 1,
    }
    canonical = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    document["checksum"] = hashlib.sha256(canonical).hexdigest()
    return document


def test_selector_artifact_only_lab_config_is_valid():
    observed = settings._coerce_lab_box(
        {
            "alias": "root@192.0.2.20",
            "label": "rtx-selector-lab",
            "selector_artifact_manifest": "/srv/artifacts/manifest.json",
        }
    )

    assert observed.unit == ""
    assert observed.evidence_db == ""
    assert observed.selector_artifact_manifest == "/srv/artifacts/manifest.json"


def test_lab_config_requires_database_or_selector_artifact():
    with pytest.raises(ValueError, match="evidence_db or selector_artifact"):
        settings._coerce_lab_box(
            {"alias": "root@192.0.2.20", "label": "empty-lab"}
        )


def test_selector_artifact_probe_verifies_checksum_and_exports_whitelist(tmp_path):
    artifact = tmp_path / "manifest.json"
    artifact.write_text(json.dumps(_selector_artifact_document()))
    artifact.chmod(0o600)
    source_manifest = tmp_path / "source-manifest.env"
    source_manifest.write_text(
        "PROVISIONED_OK=1\nRELIQUARY_SUBMIT_DISABLED_LAB=1\n"
    )
    source_manifest.chmod(0o600)

    observed = subprocess.run(
        [
            sys.executable,
            "-",
            "",
            "",
            str(source_manifest),
            str(artifact),
        ],
        input=fleet._OFFLINE_LAB_PROBE_SOURCE,
        text=True,
        capture_output=True,
        check=False,
        timeout=10,
    )

    assert observed.returncode == 0, observed.stderr
    payload = json.loads(observed.stdout)
    assert payload["artifact_manifest_ok"] is True
    assert payload["artifact_status"] == "shadow_only"
    assert payload["artifact_window_start"] == 24234
    assert payload["artifact_window_end"] == 24314
    assert payload["artifact_train_population_rows"] == 4187
    assert payload["artifact_activation_gate_passed"] is False
    assert payload["artifact_selection_brier"] == pytest.approx(0.1088)
    assert payload["artifact_payout_profile"] == "boundary_fair_split"
    assert payload["artifact_economic_target"] == "effective_full_slots"
    assert payload["artifact_emitted_slot_lift"] == pytest.approx(1.625)
    assert payload["artifact_emission_mse"] == pytest.approx(0.07125)
    assert payload["artifact_emission_base_mse"] == pytest.approx(0.0925)
    assert payload["artifact_shadow_promotion_gate_passed"] is True
    assert payload["artifact_activation_blocker"] == "shadow_only_challenger"


def test_selector_artifact_probe_rejects_tampered_manifest(tmp_path):
    document = _selector_artifact_document()
    document["validation"]["heldout_value_lift"] = 99.0
    artifact = tmp_path / "manifest.json"
    artifact.write_text(json.dumps(document))
    artifact.chmod(0o600)
    source_manifest = tmp_path / "source-manifest.env"
    source_manifest.write_text("")

    observed = subprocess.run(
        [sys.executable, "-", "", "", str(source_manifest), str(artifact)],
        input=fleet._OFFLINE_LAB_PROBE_SOURCE,
        text=True,
        capture_output=True,
        check=False,
        timeout=10,
    )

    payload = json.loads(observed.stdout)
    assert payload["artifact_manifest_ok"] is False
    assert payload["artifact_error"] == "artifact_checksum"


def test_lab_collector_preserves_math_emission_selector_contract(monkeypatch):
    state = fleet.LabState(
        alias="root@192.0.2.20",
        label="rtx-selector-lab",
        color="magenta",
        unit="",
        evidence_db="",
        source_manifest="/srv/source-manifest.env",
        selector_artifact_manifest="/srv/math-selector/manifest.json",
    )
    payload = {
        "artifact_manifest_ok": True,
        "artifact_kind": "reliquary_math_selector_catboost_challenger",
        "artifact_model_version": "catboost_v2_emitted_slots",
        "artifact_payout_profile": "boundary_fair_split",
        "artifact_economic_target": "effective_full_slots",
        "artifact_online_activation_allowed": False,
        "artifact_expires_after_window": 24919,
        "artifact_objective": "expected_slots_per_gpu_second",
        "artifact_exploration_bps": 2000,
        "artifact_emitted_slot_lift": 1.625,
        "artifact_emission_mse": 0.07125,
        "artifact_emission_base_mse": 0.0925,
        "artifact_shadow_promotion_gate_passed": True,
        "artifact_activation_blocker": "shadow_only_challenger",
    }

    monkeypatch.setattr(
        fleet,
        "ssh_run",
        lambda *_args, **_kwargs: (0, json.dumps(payload), ""),
    )

    snapshot = fleet.collect_lab_snapshot(state)

    assert snapshot.artifact_payout_profile == "boundary_fair_split"
    assert snapshot.artifact_economic_target == "effective_full_slots"
    assert snapshot.artifact_online_activation_allowed is False
    assert snapshot.artifact_expires_after_window == 24919
    assert snapshot.artifact_objective == "expected_slots_per_gpu_second"
    assert snapshot.artifact_exploration_bps == 2000
    assert snapshot.artifact_emitted_slot_lift == pytest.approx(1.625)
    assert snapshot.artifact_emission_mse == pytest.approx(0.07125)
    assert snapshot.artifact_emission_base_mse == pytest.approx(0.0925)
    assert snapshot.artifact_shadow_promotion_gate_passed is True
    assert snapshot.artifact_activation_blocker == "shadow_only_challenger"


def test_code_selector_artifact_probe_flattens_censor_aware_evidence(tmp_path):
    artifact = tmp_path / "manifest.json"
    artifact.write_text(json.dumps(_code_selector_artifact_document()))
    artifact.chmod(0o600)
    source_manifest = tmp_path / "source-manifest.env"
    source_manifest.write_text("")

    observed = subprocess.run(
        [sys.executable, "-", "", "", str(source_manifest), str(artifact)],
        input=fleet._OFFLINE_LAB_PROBE_SOURCE,
        text=True,
        capture_output=True,
        check=False,
        timeout=10,
    )

    assert observed.returncode == 0, observed.stderr
    payload = json.loads(observed.stdout)
    assert payload["artifact_manifest_ok"] is True
    assert payload["artifact_kind"] == "reliquary_code_selector_challenger"
    assert payload["artifact_checkpoint_repo_id"] == (
        "ReliquaryForge/qwen3.5-2b-reliquary-v3"
    )
    assert payload["artifact_data_digest"] == "4" * 64
    assert payload["artifact_window_start"] == 24283
    assert payload["artifact_window_end"] == 24341
    assert payload["artifact_train_local_rows"] == 72
    assert payload["artifact_completion_terminal_rows"] == 72
    assert payload["artifact_train_population_rows"] == 1077
    assert payload["artifact_holdout_rows"] == 239
    assert payload["artifact_holdout_window"] == 24341
    assert payload["artifact_activation_gate_passed"] is False
    assert payload["artifact_top_quintile_lift"] == pytest.approx(1.4130434783)
    assert payload["artifact_heldout_value_lift"] == pytest.approx(0.0167581683)
    assert payload["artifact_completion_brier"] == pytest.approx(0.4031744354)
    assert payload["artifact_selection_brier"] == pytest.approx(0.1718261971)
    assert payload["artifact_online_activation_allowed"] is False
    assert payload["artifact_expires_after_window"] == 24919
    assert payload["artifact_objective"] == (
        "P(complete) * E(emitted_full_slots) / expected_gpu_seconds"
    )
    assert payload["artifact_exploration_bps"] == 2000
    assert payload["artifact_blockers"] == [
        "completion_top_quintile_lift_1_5x",
        "exact_k2_top_quintile_lift_1_5x",
        "unconditional_admission_model_available",
    ]
    assert payload["artifact_decision_reason"] == (
        "unconditional validator admission remains unidentifiable"
    )


def test_selector_search_report_probe_exports_gains_and_blockers(tmp_path):
    artifact = tmp_path / "search-report.json"
    artifact.write_text(json.dumps(_selector_search_report_document()))
    artifact.chmod(0o600)
    source_manifest = tmp_path / "source-manifest.env"
    source_manifest.write_text("")

    observed = subprocess.run(
        [sys.executable, "-", "", "", str(source_manifest), str(artifact)],
        input=fleet._OFFLINE_LAB_PROBE_SOURCE,
        text=True,
        capture_output=True,
        check=False,
        timeout=10,
    )

    assert observed.returncode == 0, observed.stderr
    payload = json.loads(observed.stdout)
    assert payload["artifact_manifest_ok"] is True
    assert payload["artifact_status"] == "shadow_only"
    assert payload["artifact_window_start"] == 24249
    assert payload["artifact_window_end"] == 24325
    assert payload["artifact_train_local_rows"] == 49
    assert payload["artifact_train_population_rows"] == 4037
    assert payload["artifact_gpu_rolling_lift_mean"] == pytest.approx(
        1.6063762626
    )
    assert payload["artifact_gpu_final_lift"] == pytest.approx(1.8125)
    assert payload["artifact_gpu_positive_value_folds"] == 6
    assert payload["artifact_gpu_rolling_folds"] == 6
    assert payload["artifact_top_quintile_lift"] == pytest.approx(
        2.4166666667
    )
    assert payload["artifact_heldout_value_lift"] == pytest.approx(
        0.0323722750
    )
    assert payload["artifact_cpu_rolling_lift_mean"] == pytest.approx(
        1.8131313131
    )
    assert payload["artifact_cpu_positive_value_folds"] == 6
    assert payload["artifact_cpu_rolling_folds"] == 6
    assert payload["artifact_cpu_bitwise_reproducible"] is True
    assert payload["artifact_gpu_bitwise_reproducible"] is True
    assert payload["artifact_blockers"] == ["completion_calibration"]


def test_offline_probe_keeps_manifest_release_separate_from_evidence(tmp_path):
    database = tmp_path / "evidence.sqlite3"
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        PRAGMA application_id=1380930373;
        PRAGMA user_version=1;
        CREATE TABLE attempts (
          attempt_id TEXT,
          environment TEXT,
          window_n INTEGER,
          source_revision TEXT,
          checkpoint_repo_id TEXT,
          checkpoint_revision TEXT,
          checkpoint_n INTEGER,
          miner_release TEXT,
          hardware_name TEXT,
          compute_capability TEXT,
          parity_status TEXT,
          attempt_started_at REAL,
          terminal_cause TEXT,
          finished_at REAL,
          deadline_censored INTEGER,
          local_rewards_json TEXT,
          error_type TEXT,
          terminal_digest TEXT
        );
        """
    )
    connection.execute(
        "INSERT INTO attempts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "attempt-1",
            "openmathinstruct",
            24407,
            "2" * 40,
            "ReliquaryForge/model-v3",
            "3" * 40,
            48,
            "5" * 40,
            "NVIDIA RTX PRO 6000 Blackwell",
            "12.0",
            "trusted",
            100.0,
            "natural_eos",
            110.0,
            0,
            "[]",
            None,
            "6" * 64,
        ),
    )
    connection.commit()
    connection.close()
    database.chmod(0o600)
    source_manifest = tmp_path / "source-manifest.env"
    source_manifest.write_text(
        "PROVISIONED_OK=1\n"
        "RELIQUARY_SUBMIT_DISABLED_LAB=1\n"
        f"MINER_PRO_SOURCE_REVISION={'7' * 40}\n"
    )
    source_manifest.chmod(0o600)

    observed = subprocess.run(
        [sys.executable, "-", "", str(database), str(source_manifest), ""],
        input=fleet._OFFLINE_LAB_PROBE_SOURCE,
        text=True,
        capture_output=True,
        check=False,
        timeout=10,
    )

    assert observed.returncode == 0, observed.stderr
    payload = json.loads(observed.stdout)
    assert payload["evidence_db_ok"] is True
    assert payload["miner_release"] == "7" * 40
    assert payload["evidence_miner_release"] == "5" * 40


def test_lab_collector_exports_only_aggregate_offline_evidence(monkeypatch):
    state = fleet.LabState(
        alias="root@192.0.2.20",
        label="b200-math-lab",
        color="yellow",
        unit="reliquary-offline-math-evidence-c48.service",
        evidence_db="/srv/reliquary/state/math.sqlite3",
        source_manifest="/srv/reliquary/state/source-manifest.env",
    )
    observed = {}
    payload = {
        "unit_active_state": "active",
        "unit_sub_state": "running",
        "unit_enablement": "enabled",
        "active_pid": 4321,
        "restart_count": 2,
        "gpu_name": "NVIDIA B200",
        "compute_capability": "10.0",
        "gpu_mem_mb": 90112,
        "gpu_total_mb": 183359,
        "gpu_util": 97,
        "submit_disabled_attested": True,
        "lane_start_disarmed": True,
        "manifest_submit_disabled": True,
        "manifest_provisioned_ok": True,
        "miner_release": "1" * 40,
        "evidence_miner_release": "5" * 40,
        "source_revision": "2" * 40,
        "checkpoint_repo_id": "ReliquaryForge/qwen3.5-2b-reliquary-v3",
        "checkpoint_revision": "3" * 40,
        "checkpoint_n": 48,
        "environment": "openmathinstruct",
        "hardware_name": "NVIDIA B200",
        "parity_status": "untrusted_blackwell",
        "evidence_db_ok": True,
        "evidence_schema_version": 1,
        "attempts": 9,
        "terminal_attempts": 8,
        "complete_attempts": 5,
        "censored_attempts": 3,
        "deadline_censored_attempts": 2,
        "token_censored_attempts": 1,
        "pending_attempts": 1,
        "error_attempts": 0,
        "first_window_n": 24001,
        "last_window_n": 24003,
        "last_attempt_at": 123.0,
    }

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        observed.update(alias=alias, command=command, timeout_s=timeout_s)
        return 0, json.dumps(payload), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    snapshot = fleet.collect_lab_snapshot(state)

    assert observed["alias"] == state.alias
    assert "sudo -n python3 -" in observed["command"]
    assert state.evidence_db in observed["command"]
    assert "?mode=ro" in observed["command"]
    assert snapshot is not state
    assert snapshot.submit_disabled_attested is True
    assert snapshot.miner_release == "1" * 40
    assert snapshot.evidence_miner_release == "5" * 40
    assert snapshot.complete_attempts == 5
    assert snapshot.censored_attempts == 3
    assert snapshot.pending_attempts == 1


def test_lab_panel_and_json_never_project_live_miner_dimensions(monkeypatch):
    import fleet_web

    lab = fleet.LabState(
        alias="root@192.0.2.137",
        label="b200-math-lab",
        color="yellow",
        unit="reliquary-offline-math-evidence-c48.service",
        evidence_db="/srv/reliquary/state/math.sqlite3",
        source_manifest="/srv/reliquary/state/source-manifest.env",
        last_poll_s=100.0,
        gpu_name="NVIDIA B200",
        compute_capability="10.0",
        gpu_mem_mb=90000,
        gpu_total_mb=183359,
        gpu_util=99,
        unit_active_state="active",
        unit_sub_state="running",
        unit_enablement="enabled",
        active_pid=4321,
        submit_disabled_attested=True,
        lane_start_disarmed=True,
        manifest_submit_disabled=True,
        manifest_provisioned_ok=True,
        miner_release="1" * 40,
        evidence_miner_release="5" * 40,
        source_revision="2" * 40,
        checkpoint_revision="3" * 40,
        checkpoint_n=48,
        environment="openmathinstruct",
        parity_status="untrusted_blackwell",
        evidence_db_ok=True,
        evidence_schema_version=1,
        attempts=9,
        complete_attempts=5,
        censored_attempts=3,
        deadline_censored_attempts=2,
        token_censored_attempts=1,
        pending_attempts=1,
    )
    monkeypatch.setattr(fleet_web, "_labs", [lab])
    monkeypatch.setattr(fleet_web, "_last_poll_at", 100.0)
    monkeypatch.setattr(fleet_web.time, "time", lambda: 110.0)

    rendered = fleet_web.render_labs_html()
    payload = fleet_web._lab_payload(lab, now=110.0)
    serialized = json.dumps(payload, sort_keys=True).lower()

    assert "submit-disabled" in rendered.lower()
    assert "no wallet" in rendered.lower()
    assert "9 attempts · 5 complete · 3 censored" in rendered
    assert "live release 1111111111" in rendered
    assert "source 2222222222 · evidence 5555555555" in rendered
    assert payload["identity"]["manifest_release"] == "1" * 40
    assert payload["identity"]["evidence_release"] == "5" * 40
    assert payload["identity"]["release_source"] == "manifest"
    assert payload["evidence"]["release"] == "5" * 40
    assert "hotkey" not in rendered.lower()
    assert "submission" not in rendered.lower()
    assert "reward" not in rendered.lower()
    assert "hotkey" not in serialized
    assert "submission" not in serialized
    assert "reward" not in serialized


def test_inactive_lab_uses_manifest_without_claiming_process_attestation(
    monkeypatch,
):
    import fleet_web

    lab = fleet.LabState(
        alias="root@198.51.100.136",
        label="rtx6000b-code-lab",
        color="magenta",
        unit="reliquary-offline-code-scout.service",
        evidence_db="/srv/reliquary-miner-pro/state/offline-scout/code-evidence-c48.sqlite3",
        source_manifest="/srv/reliquary-miner-pro/state/source-manifest.env",
        last_poll_s=100.0,
        unit_active_state="inactive",
        unit_sub_state="dead",
        active_pid=0,
        manifest_submit_disabled=True,
        manifest_provisioned_ok=True,
        submit_disabled_attested=False,
        lane_start_disarmed=False,
        evidence_db_ok=True,
        attempts=2,
        terminal_attempts=1,
        pending_attempts=1,
    )
    monkeypatch.setattr(fleet_web, "_labs", [lab])
    monkeypatch.setattr(fleet_web, "_last_poll_at", 100.0)
    monkeypatch.setattr(fleet_web.time, "time", lambda: 110.0)

    payload = fleet_web._lab_payload(lab, now=110.0)
    rendered = fleet_web.render_labs_html()

    assert payload["status"] == "offline_inactive"
    assert payload["isolation"]["status"] == "offline_manifest_attested"
    assert payload["isolation"]["process_attestation_applicable"] is False
    assert payload["isolation"]["submit_disabled_attested"] is False
    assert "offline manifest attested" in rendered.lower()
    assert "no live process to attest" in rendered.lower()
    assert "live process isolated" not in rendered.lower()


def test_selector_artifact_panel_replaces_stale_generation_scout_evidence(
    monkeypatch,
):
    import fleet_web

    lab = fleet.LabState(
        alias="root@198.51.100.136",
        label="rtx6000b-selector-lab",
        color="magenta",
        unit="",
        evidence_db="",
        source_manifest="/srv/reliquary-miner-pro/state/source-manifest.env",
        selector_artifact_manifest="/srv/selector/experiments/report.json",
        last_poll_s=100.0,
        gpu_name="NVIDIA RTX PRO 6000 Blackwell",
        compute_capability="12.0",
        gpu_total_mb=97887,
        manifest_submit_disabled=True,
        manifest_provisioned_ok=True,
        artifact_manifest_ok=True,
        artifact_status="shadow_only",
        artifact_kind="reliquary_math_selector_bounded_search",
        artifact_model_version="bounded_search_cpu_gpu",
        artifact_digest="a" * 64,
        artifact_data_digest="b" * 64,
        artifact_source_revision="2" * 40,
        artifact_checkpoint_repo_id="ReliquaryForge/model-v3",
        artifact_checkpoint_revision="3" * 40,
        artifact_checkpoint_n=48,
        artifact_window_start=24249,
        artifact_window_end=24325,
        artifact_train_local_rows=49,
        artifact_train_population_rows=4037,
        artifact_holdout_rows=58,
        artifact_holdout_window=24325,
        artifact_activation_gate_passed=False,
        artifact_top_quintile_lift=2.416667,
        artifact_heldout_value_lift=0.032372,
        artifact_selection_brier=0.085843,
        artifact_gpu_rolling_lift_mean=1.606376,
        artifact_gpu_final_lift=1.8125,
        artifact_gpu_positive_value_folds=6,
        artifact_gpu_rolling_folds=6,
        artifact_cpu_rolling_lift_mean=1.813131,
        artifact_cpu_positive_value_folds=6,
        artifact_cpu_rolling_folds=6,
        artifact_cpu_bitwise_reproducible=True,
        artifact_gpu_bitwise_reproducible=True,
        artifact_blockers=["completion_calibration"],
        artifact_decision_reason="completion calibration remains unavailable",
    )
    monkeypatch.setattr(fleet_web, "_labs", [lab])
    monkeypatch.setattr(fleet_web, "_last_poll_at", 100.0)
    monkeypatch.setattr(fleet_web.time, "time", lambda: 110.0)

    rendered = fleet_web.render_labs_html()
    payload = fleet_web._lab_payload(lab, now=110.0)
    serialized = json.dumps(payload, sort_keys=True).lower()

    assert "selector shadow" in rendered.lower()
    assert "SHADOW-ONLY · NOT ACTIVATION ELIGIBLE" in rendered
    assert "49 local · 4,037 population · 58 holdout" in rendered
    assert "w24249–w24325" in rendered
    assert "GPU forward mean=1.606× · final=1.812×" in rendered
    assert "CPU forward mean=1.813× · final=2.417×" in rendered
    assert "value+=6/6 · repro CPU/GPU=yes/yes" in rendered
    assert "BLOCKED: completion calibration" in rendered
    assert "attempts" not in rendered.lower()
    assert payload["selector_artifact"]["status"] == "shadow_only"
    assert payload["selector_artifact"]["scope"]["train_population_rows"] == 4037
    assert payload["selector_artifact"]["search_report"]["blockers"] == [
        "completion_calibration"
    ]
    assert "hotkey" not in serialized
    assert "submission" not in serialized
    assert "reward" not in serialized


def test_math_emission_selector_artifact_panel_and_api_show_economic_gate(
    monkeypatch,
):
    import fleet_web

    lab = fleet.LabState(
        alias="root@198.51.100.136",
        label="rtx6000b-math-emission-lab",
        color="magenta",
        unit="",
        evidence_db="",
        source_manifest="/srv/reliquary-miner-pro/state/source-manifest.env",
        selector_artifact_manifest="/srv/math-selector/manifest.json",
        last_poll_s=100.0,
        unit_active_state="active",
        unit_sub_state="running",
        active_pid=190974,
        manifest_submit_disabled=True,
        manifest_provisioned_ok=True,
        miner_release="7" * 40,
        evidence_miner_release="5" * 40,
        artifact_manifest_ok=True,
        artifact_status="shadow_only",
        artifact_kind="reliquary_math_selector_catboost_challenger",
        artifact_model_version="catboost_v2_emitted_slots",
        artifact_digest="a" * 64,
        artifact_source_revision="2" * 40,
        artifact_checkpoint_repo_id="ReliquaryForge/model-v3",
        artifact_checkpoint_revision="3" * 40,
        artifact_checkpoint_n=48,
        artifact_window_start=24249,
        artifact_window_end=24404,
        artifact_train_local_rows=49,
        artifact_train_population_rows=8344,
        artifact_holdout_rows=61,
        artifact_holdout_window=24404,
        artifact_activation_gate_passed=False,
        artifact_payout_profile="boundary_fair_split",
        artifact_economic_target="effective_full_slots",
        artifact_emitted_slot_lift=1.625,
        artifact_emission_mse=0.07125,
        artifact_emission_base_mse=0.0925,
        artifact_shadow_promotion_gate_passed=True,
        artifact_activation_blocker="shadow_only_challenger",
    )
    monkeypatch.setattr(fleet_web, "_labs", [lab])
    monkeypatch.setattr(fleet_web, "_last_poll_at", 100.0)
    monkeypatch.setattr(fleet_web.time, "time", lambda: 110.0)

    rendered = fleet_web.render_labs_html()
    payload = fleet_web._lab_payload(lab, now=110.0)["selector_artifact"]

    assert "emitted-slot lift=1.625× · payout=boundary fair split" in rendered
    assert "live 7777777777 · evidence 5555555555" in rendered
    assert (
        "emission MSE=0.07125 · base=0.09250 · "
        "target=effective full slots · shadow promotion=passed"
    ) in rendered
    assert "BLOCKED: shadow only challenger" in rendered
    assert payload["policy"] == {
        "payout_profile": "boundary_fair_split",
        "economic_target": "effective_full_slots",
        "online_activation_allowed": None,
        "expires_after_window": None,
        "objective": "",
        "exploration_bps": None,
        "exploration_rate": None,
    }
    assert payload["validation"]["emitted_slot_lift"] == pytest.approx(1.625)
    assert payload["validation"]["emission_mse"] == pytest.approx(0.07125)
    assert payload["validation"]["emission_base_mse"] == pytest.approx(0.0925)
    assert payload["validation"]["shadow_promotion_gate_passed"] is True
    assert (
        payload["validation"]["activation_blocker"]
        == "shadow_only_challenger"
    )


def test_code_selector_artifact_panel_displays_censor_aware_metrics(monkeypatch):
    import fleet_web

    lab = fleet.LabState(
        alias="root@198.51.100.136",
        label="rtx6000b-selector-lab",
        color="magenta",
        unit="",
        evidence_db="",
        source_manifest="/srv/reliquary-miner-pro/state/source-manifest.env",
        selector_artifact_manifest="/srv/code-selector/manifest.json",
        last_poll_s=100.0,
        manifest_submit_disabled=True,
        manifest_provisioned_ok=True,
        artifact_manifest_ok=True,
        artifact_status="shadow_only",
        artifact_kind="reliquary_code_selector_challenger",
        artifact_model_version="hierarchical_empirical_bayes_v1",
        artifact_digest="a" * 64,
        artifact_source_revision="2" * 40,
        artifact_checkpoint_repo_id="ReliquaryForge/model-v3",
        artifact_checkpoint_revision="3" * 40,
        artifact_checkpoint_n=48,
        artifact_window_start=24283,
        artifact_window_end=24341,
        artifact_train_local_rows=72,
        artifact_train_population_rows=1077,
        artifact_holdout_rows=239,
        artifact_holdout_window=24341,
        artifact_activation_gate_passed=False,
        artifact_top_quintile_lift=1.413043,
        artifact_heldout_value_lift=0.016758,
        artifact_completion_brier=0.403174,
        artifact_selection_brier=0.171826,
        artifact_blockers=["completion_top_quintile_lift_1_5x"],
        artifact_decision_reason="completion lift gate failed",
    )
    monkeypatch.setattr(fleet_web, "_labs", [lab])
    monkeypatch.setattr(fleet_web, "_last_poll_at", 100.0)
    monkeypatch.setattr(fleet_web.time, "time", lambda: 110.0)

    rendered = fleet_web.render_labs_html()

    assert "72 local · 1,077 population · 239 holdout" in rendered
    assert "exact-k2 top lift=1.413× · value lift=+1.68%" in rendered
    assert "completion Brier=0.4032 · selection Brier=0.1718" in rendered
    assert "BLOCKED: completion top quintile lift 1 5x" in rendered


def test_expired_shadow_artifact_is_explicit_without_losing_integrity(
    monkeypatch,
):
    import fleet_web

    lab = fleet.LabState(
        alias="root@198.51.100.136",
        label="rtx6000b-selector-lab",
        color="magenta",
        unit="",
        evidence_db="",
        source_manifest="/srv/source-manifest.env",
        selector_artifact_manifest="/srv/code-selector/manifest.json",
        last_poll_s=100.0,
        manifest_submit_disabled=True,
        manifest_provisioned_ok=True,
        artifact_manifest_ok=True,
        artifact_status="shadow_only",
        artifact_kind="reliquary_code_selector_challenger",
        artifact_model_version="hierarchical_empirical_bayes_v1",
        artifact_digest="a" * 64,
        artifact_checkpoint_n=53,
        artifact_window_start=24821,
        artifact_window_end=24871,
        artifact_online_activation_allowed=False,
        artifact_expires_after_window=24919,
        artifact_objective="expected_slots_per_gpu_second",
        artifact_exploration_bps=2000,
        artifact_decision_reason="shadow-only challenger",
    )
    payload = fleet_web._lab_payload(
        lab,
        now=110.0,
        validator_window=25003,
    )

    assert payload["status"] == "selector_expired"
    assert "expired after w24919" in payload["status_detail"]
    artifact = payload["selector_artifact"]
    assert artifact["ok"] is True
    assert artifact["status"] == "shadow_only"
    assert artifact["artifact_current"] is False
    assert artifact["expired"] is True
    assert artifact["validator_window"] == 25003
    assert artifact["policy"] == {
        "payout_profile": "",
        "economic_target": "",
        "online_activation_allowed": False,
        "expires_after_window": 24919,
        "objective": "expected_slots_per_gpu_second",
        "exploration_bps": 2000,
        "exploration_rate": pytest.approx(0.2),
    }

    monkeypatch.setattr(fleet_web, "_labs", [lab])
    monkeypatch.setattr(fleet_web, "_vs", fleet.ValidatorState(window=25003))
    monkeypatch.setattr(fleet_web, "_last_poll_at", 100.0)
    monkeypatch.setattr(fleet_web.time, "time", lambda: 110.0)
    rendered = fleet_web.render_labs_html()

    assert "SELECTOR EXPIRED" in rendered
    assert "EXPIRED SHADOW · NOT ACTIVATION ELIGIBLE" in rendered
    assert "expires after w24919" in rendered


def test_current_shadow_artifact_reports_temporal_currency(monkeypatch):
    import fleet_web

    lab = fleet.LabState(
        alias="root@198.51.100.136",
        label="rtx6000b-selector-lab",
        color="magenta",
        unit="",
        evidence_db="",
        source_manifest="/srv/source-manifest.env",
        selector_artifact_manifest="/srv/code-selector/manifest.json",
        last_poll_s=100.0,
        manifest_submit_disabled=True,
        manifest_provisioned_ok=True,
        artifact_manifest_ok=True,
        artifact_status="shadow_only",
        artifact_kind="reliquary_code_selector_challenger",
        artifact_model_version="hierarchical_empirical_bayes_v1",
        artifact_digest="a" * 64,
        artifact_checkpoint_n=53,
        artifact_window_start=24821,
        artifact_window_end=24871,
        artifact_online_activation_allowed=False,
        artifact_expires_after_window=24919,
        artifact_objective="expected_slots_per_gpu_second",
        artifact_exploration_bps=2000,
        artifact_decision_reason="shadow-only challenger",
    )
    payload = fleet_web._lab_payload(
        lab,
        now=110.0,
        validator_window=24919,
    )

    assert payload["status"] == "selector_shadow"
    artifact = payload["selector_artifact"]
    assert artifact["ok"] is True
    assert artifact["artifact_current"] is True
    assert artifact["expired"] is False
    assert artifact["validator_window"] == 24919

    monkeypatch.setattr(fleet_web, "_labs", [lab])
    monkeypatch.setattr(fleet_web, "_vs", fleet.ValidatorState(window=24919))
    monkeypatch.setattr(fleet_web, "_last_poll_at", 100.0)
    monkeypatch.setattr(fleet_web.time, "time", lambda: 110.0)
    rendered = fleet_web.render_labs_html()

    assert "SELECTOR SHADOW" in rendered
    assert "SELECTOR EXPIRED" not in rendered
    assert "SHADOW-ONLY · NOT ACTIVATION ELIGIBLE" in rendered


def test_failed_lab_never_changes_live_fleet_health(monkeypatch):
    import fleet_web

    now = 500.0
    broken = fleet.LabState(
        alias="root@192.0.2.20",
        label="broken-offline-lab",
        color="red",
        unit="reliquary-offline-math-evidence-c48.service",
        evidence_db="/srv/reliquary/state/math.sqlite3",
        source_manifest="/srv/reliquary/state/source-manifest.env",
        last_poll_s=now,
        unit_active_state="failed",
        unit_sub_state="failed",
        error="probe_failed",
    )
    monkeypatch.setattr(fleet_web, "_labs", [])
    baseline = fleet_web.compute_healthz(5, now=now)
    monkeypatch.setattr(fleet_web, "_labs", [broken])
    observed = fleet_web.compute_healthz(5, now=now)

    assert observed["ok"] == baseline["ok"]
    assert observed["checks"] == baseline["checks"]
    assert "labs" not in observed["checks"]
    assert observed["labs"][0]["status"] == "offline_failed"


def _valid_crossover_probe(
    box: fleet.BoxState,
    *,
    window_n: int,
) -> dict:
    contract_sha = "1" * 64
    assignment_sha = "2" * 64
    return {
        "schema_version": 1,
        "configured": True,
        "valid": True,
        "settings_process_bound": True,
        "process_unit": "reliquary-miner-pro@code-reserve2.service",
        "process_pid": 5252,
        "lane": "code-reserve2",
        "contract": {
            "sha256": contract_sha,
            "experiment_id": "c53-code-crossover",
            "control_policy_id": "fast-numeric-meta-v1",
            "treatment_policy_id": "conditional-k2-v1",
            "treatment_artifact_sha256": "3" * 64,
            "public_source_revision": box.reliquary_source_revision,
            "checkpoint_repository": box.provisioned_model_repo,
            "checkpoint_revision": box.provisioned_model_revision,
            "checkpoint_n": box.provisioned_checkpoint_n,
            "runtime_profile_sha256": box.runtime_profile_hash,
            "miner_release_revision": box.miner_source_revision,
            "start_window": window_n - 6,
            "end_window": window_n + 5,
            "environment": "opencodeinstruct",
        },
        "score_index": {
            "file_sha256": "3" * 64,
            "row_count": 2_481_806,
            "content_digest": "4" * 64,
            "training_window_start": window_n - 100,
            "training_window_end": window_n - 7,
        },
        "state": {
            "application_id": 0x52435352,
            "user_version": 1,
            "contract_sha256": contract_sha,
            "latest_assignment": {
                "block_start_window": window_n,
                "record_sha256": assignment_sha,
                "sequence": "BA",
                "public_randomness_round": 123,
                "windows": [
                    {"window_n": window_n, "itt_arm": "treatment"},
                    {"window_n": window_n + 1, "itt_arm": "control"},
                ],
            },
            "latest_decision": {
                "window_n": window_n,
                "record_sha256": "5" * 64,
                "assignment_sha256": assignment_sha,
                "itt_arm": "treatment",
                "execution_arm": "treatment",
                "validation_status": "passed",
                "validation_failure_codes": [],
                "fallback_to_control": False,
                "reason": "itt_treatment_validated",
            },
        },
        "error": "",
    }


def test_crossover_probe_source_is_process_bound_and_read_only():
    source = fleet._CODE_SELECTOR_CROSSOVER_PROBE_SOURCE

    compile(source, "<code-selector-crossover-probe>", "exec")
    assert "/proc/{pid}/environ" in source
    assert "PRAGMA query_only=ON" in source
    assert "?mode=ro" in source
    assert "PRAGMA quick_check" in source
    assert "sha256_file(path)" in source
    assert "record_not_canonical" in source
    assert "ORDER BY window_n DESC LIMIT 1" in source
    for key in (
        "RELIQUARY_CODE_SELECTOR_CROSSOVER_CONTRACT_PATH",
        "RELIQUARY_CODE_SELECTOR_CROSSOVER_CONTRACT_SHA256",
        "RELIQUARY_CODE_SELECTOR_CROSSOVER_SCORE_INDEX_PATH",
        "RELIQUARY_CODE_SELECTOR_CROSSOVER_STATE_PATH",
    ):
        assert key in source
    upper = source.upper()
    for write_sql in (
        "INSERT INTO",
        "UPDATE ",
        "DELETE FROM",
        "CREATE TABLE",
        "DROP TABLE",
        "VACUUM",
    ):
        assert write_sql not in upper


def test_crossover_probe_parser_is_bounded_and_whitelisted():
    box, _validator = _ready_code_context()
    payload = _valid_crossover_probe(box, window_n=23806)

    parsed = fleet._parse_code_selector_crossover_probe(
        [json.dumps(payload)]
    )

    assert parsed == payload
    assert fleet._parse_code_selector_crossover_probe(
        ["x" * 65_537]
    ) == {}
    polluted = copy.deepcopy(payload)
    polluted["secret"] = "must-not-pass"
    assert fleet._parse_code_selector_crossover_probe(
        [json.dumps(polluted)]
    ) == {}
    malformed = copy.deepcopy(payload)
    malformed["state"]["latest_decision"]["window_n"] = True
    assert fleet._parse_code_selector_crossover_probe(
        [json.dumps(malformed)]
    ) == {}


def test_crossover_current_arm_requires_exact_fresh_open_window(monkeypatch):
    import fleet_web

    box, validator = _ready_code_context()
    now = box.last_poll_s + 1
    window_n = validator.window
    reserve2 = "reliquary-miner-pro@code-reserve2.service"
    box.active_units = [box.active_unit, reserve2]
    box.active_lanes = [box.active_lane, "code-reserve2"]
    box.code_selector_crossover_probe = _valid_crossover_probe(
        box, window_n=window_n
    )
    validator.state_raw = {"state": "open", "window_n": window_n}

    current = fleet_web._box_code_selector_crossover_details(
        box, validator, now=now, stale_after_s=15
    )
    assert current["ok"] is True
    assert current["current_status"] == "current"
    assert current["current"]["execution_arm"] == "treatment"

    validator.window = window_n - 7
    validator.state_raw["window_n"] = validator.window
    outside = fleet_web._box_code_selector_crossover_details(
        box, validator, now=now, stale_after_s=15
    )
    assert outside["latest_persisted"]["execution_arm"] == "treatment"
    assert outside["current"] is None
    assert outside["current_status"] == "outside_contract"

    # A persisted treatment record remains available only under the explicitly
    # historical key after the validator advances.
    validator.window = window_n + 1
    validator.state_raw["window_n"] = validator.window
    advanced = fleet_web._box_code_selector_crossover_details(
        box, validator, now=now, stale_after_s=15
    )
    assert advanced["latest_persisted"]["execution_arm"] == "treatment"
    assert advanced["current"] is None
    assert advanced["current_status"] == "decision_pending"

    validator.window = window_n
    validator.state_raw["window_n"] = window_n
    validator.last_fetch_at = now - 16
    stale = fleet_web._box_code_selector_crossover_details(
        box, validator, now=now, stale_after_s=15
    )
    assert stale["current"] is None
    assert stale["current_status"] == "validator_stale"

    validator.last_fetch_at = now - 1
    validator.state = "training"
    validator.state_raw["state"] = "training"
    non_open = fleet_web._box_code_selector_crossover_details(
        box, validator, now=now, stale_after_s=15
    )
    assert non_open["current"] is None
    assert non_open["current_status"] == "validator_not_open"

    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_vs", validator)
    monkeypatch.setattr(fleet_web, "_chain", fleet.ChainState())
    monkeypatch.setattr(fleet_web, "_windows", [])
    exported = fleet_web.render_export_json()["fleet"][0][
        "code_selector_crossover"
    ]
    rendered = fleet_web.render_pipeline_html()
    assert exported["current"] is None
    assert exported["latest_persisted"]["execution_arm"] == "treatment"
    assert ">treatment</td>" not in rendered


def _valid_overlap_abba_probe(
    box: fleet.BoxState,
    *,
    window_n: int,
) -> dict:
    contract_sha = "6" * 64
    start_window = window_n - 6
    contract = {
        "checkpoint_n": box.provisioned_checkpoint_n,
        "checkpoint_repository": box.provisioned_model_repo,
        "checkpoint_revision": box.provisioned_model_revision,
        "end_window": start_window + 63,
        "experiment_id": "c53-overlap-first-abba-confirmatory",
        "feature_index_content_digest": "7" * 64,
        "feature_index_file_sha256": "8" * 64,
        "miner_release_revision": box.miner_source_revision,
        "period0_treatment_slot": 0,
        "public_source_revision": box.reliquary_source_revision,
        "runtime_profile_sha256": box.runtime_profile_hash,
        "selector_policy_id": "arithmetic-overlap-first-v2",
        "selector_policy_version": 2,
        "sha256": contract_sha,
        "start_window": start_window,
    }

    def attempt(shard_slot: int, *, latest_is_activation: bool) -> dict:
        offset = window_n - start_window
        treatment_slot = contract["period0_treatment_slot"] ^ (offset % 2)
        itt_arm = (
            "treatment" if shard_slot == treatment_slot else "control"
        )
        execution_arm = itt_arm
        selector_policy = (
            "exploit" if latest_is_activation else "explore"
        )
        selected_overlap = latest_is_activation
        fallback_reason = "" if latest_is_activation else "explore_policy"
        assignment_record = {
            "assignment_algorithm":
                "period0_slot_complementary_two_shard_ab_ba_v1",
            "assignment_unit": "window_shard",
            "contract_sha256": contract_sha,
            "inside_experiment": True,
            "itt_arm": itt_arm,
            "kind": "reliquary_code_overlap_abba_assignment",
            "pair_index": offset // 2,
            "period": offset % 2,
            "reason": (
                "assigned_treatment"
                if itt_arm == "treatment"
                else "assigned_control"
            ),
            "schema_version": 1,
            "shard_slot": shard_slot,
            "treatment_slot": treatment_slot,
            "window_n": window_n,
        }
        return {
            "activated": bool(
                itt_arm == "treatment" and latest_is_activation
            ),
            "assigned_at": 1000.0 + shard_slot,
            "assignment_sha256": hashlib.sha256(
                json.dumps(
                    assignment_record,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
            ).hexdigest(),
            "execution_arm": execution_arm,
            "fallback_reason": fallback_reason,
            "itt_arm": itt_arm,
            "overlap_candidate_count": 1 if latest_is_activation else 0,
            "pair_index": offset // 2,
            "period": offset % 2,
            "selected_overlap": selected_overlap,
            "selector_policy": selector_policy,
            "treatment_slot": treatment_slot,
            "window_n": window_n,
        }

    treatment_latest = attempt(0, latest_is_activation=False)
    control_latest = attempt(1, latest_is_activation=False)
    return {
        "schema_version": 1,
        "configured": True,
        "valid": True,
        "settings_process_bound": True,
        "contract": contract,
        "lanes": [
            {
                "lane": "code-reserve1",
                "latest_window": {
                    "activation_count": 1,
                    "attempt_count": 2,
                    "fallback_counts": {
                        "explore_policy": 1,
                        "none": 1,
                    },
                    "latest_attempt": treatment_latest,
                    "selected_overlap_count": 1,
                    "window_n": window_n,
                },
                "process_pid": 4242,
                "process_unit":
                    "reliquary-miner-pro@code-reserve1.service",
                "shard_count": 2,
                "shard_slot": 0,
            },
            {
                "lane": "code-reserve2",
                "latest_window": {
                    "activation_count": 0,
                    "attempt_count": 1,
                    "fallback_counts": {"explore_policy": 1},
                    "latest_attempt": control_latest,
                    "selected_overlap_count": 0,
                    "window_n": window_n,
                },
                "process_pid": 4243,
                "process_unit":
                    "reliquary-miner-pro@code-reserve2.service",
                "shard_count": 2,
                "shard_slot": 1,
            },
        ],
        "error": "",
    }


def _configure_overlap_abba_pair(box: fleet.BoxState) -> None:
    reserve1 = "reliquary-miner-pro@code-reserve1.service"
    reserve2 = "reliquary-miner-pro@code-reserve2.service"
    box.active_units = [reserve1, reserve2]
    box.active_lanes = ["code-reserve1", "code-reserve2"]
    box.coordinated_units = (reserve1, reserve2)
    box.coordinated_unit_statuses = [
        {
            "unit": reserve1,
            "active_state": "active",
            "sub_state": "running",
            "pid": 4242,
            "restarts": 0,
        },
        {
            "unit": reserve2,
            "active_state": "active",
            "sub_state": "running",
            "pid": 4243,
            "restarts": 0,
        },
    ]


def test_overlap_abba_probe_source_is_process_bound_and_read_only():
    source = fleet._CODE_OVERLAP_ABBA_PROBE_SOURCE

    compile(source, "<code-overlap-abba-probe>", "exec")
    assert "/proc/{pid}/environ" in source
    assert "PRAGMA query_only=ON" in source
    assert "?mode=ro" in source
    assert "ORDER BY window_n DESC,assigned_at DESC,attempt_key DESC" in source
    assert "window_n=(SELECT MAX(window_n)" in source
    assert 'fail(out, "process_identity_unavailable")' in source
    assert 'fail(out, "process_environment_unavailable")' in source
    for key in (
        "RELIQUARY_CODE_OVERLAP_ABBA_CONTRACT_PATH",
        "RELIQUARY_CODE_OVERLAP_ABBA_CONTRACT_SHA256",
        "RELIQUARY_CODE_OUTCOME_LEDGER",
    ):
        assert key in source
    upper = source.upper()
    for write_sql in (
        "INSERT INTO",
        "UPDATE ",
        "DELETE FROM",
        "CREATE TABLE",
        "DROP TABLE",
        "VACUUM",
    ):
        assert write_sql not in upper


def test_overlap_abba_probe_parser_preserves_window_aggregate_and_fails_closed():
    box, validator = _ready_code_context()
    payload = _valid_overlap_abba_probe(box, window_n=validator.window)

    parsed = fleet._parse_code_overlap_abba_probe([json.dumps(payload)])

    assert parsed == payload
    treatment_window = parsed["lanes"][0]["latest_window"]
    assert treatment_window["activation_count"] == 1
    assert treatment_window["attempt_count"] == 2
    assert treatment_window["latest_attempt"]["activated"] is False
    assert fleet._parse_code_overlap_abba_probe(["x" * 65_537]) == {}
    unavailable = {
        "schema_version": 1,
        "configured": True,
        "valid": False,
        "settings_process_bound": False,
        "contract": {},
        "lanes": [],
        "error": "process_identity_unavailable",
    }
    assert fleet._parse_code_overlap_abba_probe(
        [json.dumps(unavailable)]
    ) == unavailable

    mutations = []
    polluted = copy.deepcopy(payload)
    polluted["secret"] = "must-not-pass"
    mutations.append(polluted)
    bool_schema = copy.deepcopy(payload)
    bool_schema["schema_version"] = True
    mutations.append(bool_schema)
    float_shards = copy.deepcopy(payload)
    float_shards["lanes"][0]["shard_count"] = 2.0
    mutations.append(float_shards)
    bool_period = copy.deepcopy(payload)
    bool_period["lanes"][0]["latest_window"]["latest_attempt"][
        "period"
    ] = False
    mutations.append(bool_period)
    control_escalation = copy.deepcopy(payload)
    control_escalation["lanes"][1]["latest_window"]["latest_attempt"][
        "execution_arm"
    ] = "treatment"
    mutations.append(control_escalation)
    lane_mismatch = copy.deepcopy(payload)
    lane_mismatch["lanes"][0]["lane"] = "code-reserve2"
    mutations.append(lane_mismatch)
    bad_total = copy.deepcopy(payload)
    bad_total["lanes"][0]["latest_window"]["attempt_count"] = 3
    mutations.append(bad_total)

    for malformed in mutations:
        assert fleet._parse_code_overlap_abba_probe(
            [json.dumps(malformed)]
        ) == {}


def test_overlap_abba_current_export_health_and_ui_use_window_aggregate(
    monkeypatch,
):
    import fleet_web

    box, validator = _ready_code_context()
    now = box.last_poll_s + 1
    _configure_overlap_abba_pair(box)
    validator.state_raw = {"state": "open", "window_n": validator.window}
    payload = _valid_overlap_abba_probe(box, window_n=validator.window)
    box.code_overlap_abba_probe = fleet._parse_code_overlap_abba_probe(
        [json.dumps(payload)]
    )
    monkeypatch.setattr(
        settings.SETTINGS, "health_poll_stale_seconds", 60.0
    )
    within_configured_budget = (
        fleet_web._box_code_overlap_abba_details(
            box,
            validator,
            now=box.last_poll_s + 30,
        )
    )
    assert within_configured_budget["current_status"] == "current"
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_labs", [])
    monkeypatch.setattr(fleet_web, "_vs", validator)
    monkeypatch.setattr(fleet_web, "_windows", [])
    monkeypatch.setattr(fleet_web, "_last_poll_at", now - 1)
    monkeypatch.setattr(fleet_web, "_poll_count", 1)
    monkeypatch.setattr(fleet_web, "_chain", fleet.ChainState())
    monkeypatch.setattr(
        fleet_web,
        "_r2_status_snapshot",
        lambda: {
            "last_success_at": now - 1,
            "latest_window": validator.window - 1,
            "coverage_complete": True,
            "coverage": {"complete": True},
        },
    )

    details = fleet_web._box_code_overlap_abba_details(
        box, validator, now=now, stale_after_s=15
    )
    exported = fleet_web.render_export_json()["fleet"][0][
        "code_overlap_abba"
    ]
    health = fleet_web.compute_healthz(5, now=now)["fleet"][0][
        "code_overlap_abba"
    ]
    pipeline = fleet_web.render_pipeline_html()

    assert details["ok"] is True
    assert details["current_status"] == "current"
    treatment = details["current"][0]
    assert treatment["activation_count"] == 1
    assert treatment["attempt_count"] == 2
    assert treatment["latest_attempt"]["activated"] is False
    assert exported["current"] == details["current"]
    assert health["current"] == details["current"]
    assert "activations=1" in pipeline
    assert "latest attempt" in pipeline


def test_overlap_abba_current_requires_fresh_open_window_and_exact_pid():
    import fleet_web

    box, validator = _ready_code_context()
    now = box.last_poll_s + 1
    _configure_overlap_abba_pair(box)
    validator.state_raw = {"state": "open", "window_n": validator.window}
    box.code_overlap_abba_probe = _valid_overlap_abba_probe(
        box, window_n=validator.window
    )

    current = fleet_web._box_code_overlap_abba_details(
        box, validator, now=now, stale_after_s=15
    )
    assert current["current_status"] == "current"

    validator.window += 1
    validator.state_raw["window_n"] = validator.window
    pending = fleet_web._box_code_overlap_abba_details(
        box, validator, now=now, stale_after_s=15
    )
    assert pending["current"] is None
    assert pending["current_status"] == "assignment_pending"

    validator.window -= 1
    validator.state_raw["window_n"] = validator.window
    validator.last_fetch_at = now - 16
    stale = fleet_web._box_code_overlap_abba_details(
        box, validator, now=now, stale_after_s=15
    )
    assert stale["current_status"] == "validator_stale"

    validator.last_fetch_at = now - 1
    box.coordinated_unit_statuses[1]["pid"] = 9999
    drifted = fleet_web._box_code_overlap_abba_details(
        box, validator, now=now, stale_after_s=15
    )
    assert drifted["ok"] is False
    assert "process_identity_mismatch:1" in drifted["issues"]

    box.code_overlap_abba_probe = {}
    disabled = fleet_web._box_code_overlap_abba_details(
        box, validator, now=now, stale_after_s=15
    )
    assert disabled["configured"] is False
    assert disabled["ok"] is True
    assert disabled["current_status"] == "disabled"
