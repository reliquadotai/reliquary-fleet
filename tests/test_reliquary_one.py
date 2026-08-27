from __future__ import annotations

import json
from pathlib import Path

import pytest

import fleet
import fleet_web
import reliquary_one
import settings

FIXTURE = Path(__file__).parent / "fixtures" / "reliquary_one_demo.json"
NOW = 1_800_000_000.0
HOTKEY = (
    "5GrwvaEF5zXb26Fz9rcQpDWSn"  # pragma: allowlist secret
    "7F4p6kJ7VSDyHfJ6JQyMZ7v"
)
REVISION = "1" * 40
DEMO_WINDOW = 42_001
FAILURE_WINDOW = 42_002
CURRENT_WINDOW = 42_003
CHECKPOINT_NUMBER = 42


def _payload() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _normalized(payload: dict | None = None) -> dict:
    return reliquary_one.normalize_probe(
        _payload() if payload is None else payload,
        now=NOW,
        stale_seconds=900.0,
    )


def _v4_payload() -> dict:
    payload = _payload()
    payload["service"]["unit"] = "reliquary-v4@code.service"
    payload["binding"].update(
        {
            "environment": "opencodeinstruct",
            "strategy": "code",
            "profile_id": "qwen3-4b-base-dapo-v4",
        }
    )
    attempt_ids = [character * 64 for character in "abcde"]
    payload["events"] = [
        {
            "event": "window_started",
            "timestamp": NOW - 20,
            "window_n": 30001,
            "checkpoint_revision": REVISION,
            "classification": "ok",
            "metrics": {"open_age_seconds": 0.2},
        },
        *[
            {
                "event": event,
                "timestamp": NOW - 19 + index,
                "window_n": 30001,
                "attempt_id": attempt_ids[index],
                "checkpoint_revision": REVISION,
                "classification": "not_applicable",
                "metrics": {"reason": "shadow_reason"},
            }
            for index, event in enumerate(
                (
                    "local_protocol_ineligible",
                    "local_out_of_zone",
                    "group_termination_ineligible",
                    "group_ineligible",
                )
            )
        ],
        {
            "event": "live_submission",
            "timestamp": NOW - 14,
            "window_n": 30001,
            "attempt_id": attempt_ids[-1],
            "checkpoint_revision": REVISION,
            "classification": "rejected",
            "metrics": {
                "generation_seconds": 5.0,
                "proof_seconds": 1.0,
                "precommit_transport_complete": False,
                "precommit_reason": "batch_filled",
                "reveal_transport_complete": False,
                "reveal_reason": "not_required",
                "transport_complete": False,
            },
        },
    ]
    payload["source_timestamps"]["events"] = NOW - 1
    payload["submissions"] = [
        {
            "window_n": 30001,
            "merkle_root": "f" * 64,
        }
    ]
    return payload


def _r2_rejected_window() -> fleet.WindowSummary:
    rejected = [
        {
            "hotkey": HOTKEY,
            "env_name": "opencodeinstruct",
            "accepted_into_pool": False,
            "canonical_rank": None,
            "reject_reason": "out_of_zone",
            "arrival_ts": NOW - 20 + index,
        }
        for index in range(2)
    ]
    return fleet.WindowSummary(
        n=DEMO_WINDOW,
        ours=0,
        total_batch=16,
        rt_first=1.0,
        rejects={"out_of_zone": 2},
        reward_data_present=True,
        terminal_data_present=True,
        lifecycle_explicit=True,
        environments=["openmathinstruct", "opencodeinstruct"],
        environment_counts={
            "opencodeinstruct": {
                "selected": 8,
                "runners_up": 0,
                "rejected": 2,
            }
        },
        rejected=rejected,
        reject_summary={"out_of_zone": 2},
    )


def _box(miner: dict | None = None) -> fleet.BoxState:
    normalized = _normalized() if miner is None else miner
    binding = normalized["binding"]
    service = normalized["service"]
    gpu = normalized["gpu"][0]
    return fleet.BoxState(
        alias="operator@example",
        hotkey=HOTKEY,
        label="code-miner-one",
        color="#7bd389",
        unit="reliquary-one.service",
        miner_kind="reliquary_one",
        reliquary_one_state_root="/var/lib/reliquary-one/code/state",
        standalone_telemetry_stale_seconds=900.0,
        reliquary_one=normalized,
        proc_alive=bool(service["active"]),
        active_unit="reliquary-one.service",
        active_units=["reliquary-one.service"],
        active_lane="code",
        active_lanes=["code"],
        active_environment="opencodeinstruct",
        active_pid=int(service["pid"]),
        restart_count=int(service["restarts"]),
        proc_uptime_s=int(service["uptime_seconds"]),
        last_poll_s=NOW - 1,
        env_file_ok=True,
        reference_ready=True,
        runtime_parity_ok=True,
        engine_mode="reliquary_one",
        gpu_util=int(gpu["utilization_pct"]),
        gpu_mem_mb=int(gpu["memory_used_mib"]),
        gpu_total_mb=int(gpu["memory_total_mib"]),
        runtime_checkpoint_loaded=True,
        runtime_checkpoint_n=int(binding["checkpoint_number"]),
        runtime_checkpoint_revision=str(binding["checkpoint_revision"]),
        local_checkpoint_n=int(binding["checkpoint_number"]),
        local_checkpoint_revision=str(binding["checkpoint_revision"]),
    )


def _validator() -> fleet.ValidatorState:
    return fleet.ValidatorState(
        state="training",
        window=CURRENT_WINDOW,
        checkpoint_n=CHECKPOINT_NUMBER,
        checkpoint_revision=REVISION,
        env_name="opencodeinstruct",
        last_fetch_at=NOW - 2,
        health_last_fetch_at=NOW - 2,
        health_status="ok",
        verdicts_last_fetch_at=NOW - 2,
    )


def test_demo_transport_is_not_validator_or_auction_success() -> None:
    miner = _normalized()
    attempts = [
        row for row in miner["attempts"] if row["window_n"] == DEMO_WINDOW
    ]
    assert len(attempts) == 2
    assert all(row["transport"]["status"] == "accepted" for row in attempts)
    assert all(row["admission"]["status"] == "pending" for row in attempts)
    assert all(row["auction"]["selected"] is None for row in attempts)
    assert all(row["auction"]["rewarded"] is None for row in attempts)

    reconciled = reliquary_one.reconcile_attempts(
        miner,
        windows=[_r2_rejected_window()],
        hotkey=HOTKEY,
    )
    attempts = [
        row
        for row in reconciled["attempts"]
        if row["window_n"] == DEMO_WINDOW
    ]
    assert all(row["transport"]["status"] == "accepted" for row in attempts)
    assert all(
        row["admission"]
        == {
            "status": "rejected",
            "reason": "out_of_zone",
            "source": "r2",
        }
        for row in attempts
    )
    assert all(row["auction"]["selected"] is False for row in attempts)
    assert all(row["auction"]["rewarded"] is False for row in attempts)
    assert all(row["auction"]["canonical_rank"] is None for row in attempts)


def test_demo_timings_and_runtime_failure_are_preserved() -> None:
    attempts = _normalized()["attempts"]
    by_ordinal = {
        row["ordinal"]: row
        for row in attempts
        if row["window_n"] == DEMO_WINDOW
    }
    first, second = by_ordinal[1], by_ordinal[2]
    assert first["open_age_at_start_s"] == pytest.approx(12.5)
    assert first["generation_s"] == pytest.approx(48.25)
    assert first["proof_s"] == pytest.approx(8.5)
    assert first["proof_integrity_s"] == pytest.approx(0.004)
    assert first["precommit_s"] == pytest.approx(0.08)
    assert first["reveal_s"] == pytest.approx(0.21)
    assert first["total_s"] == pytest.approx(58.75)
    assert second["open_age_at_start_s"] == pytest.approx(85.0)
    assert second["total_s"] == pytest.approx(5.75)
    failure = next(
        row for row in attempts if row["window_n"] == FAILURE_WINDOW
    )
    assert failure["transport"]["status"] == "not_sent"
    assert failure["failure"] == {
        "classification": "runtime_other",
        "exception_file": "demo-runtime-failure.json",
    }


def test_selected_rewarded_requires_authoritative_terminal_row() -> None:
    miner = _normalized()
    roots = [
        row["merkle_root"]
        for row in miner["attempts"]
        if row["window_n"] == DEMO_WINDOW
    ]
    window = fleet.WindowSummary(
        n=DEMO_WINDOW,
        ours=2,
        total_batch=8,
        rt_first=1.0,
        rejects={},
        reward_data_present=True,
        terminal_data_present=True,
        batch=[
            {
                "hotkey": HOTKEY,
                "merkle_root": root,
                "env_name": "opencodeinstruct",
                "accepted_into_pool": True,
                "selected_for_batch": True,
                "rewarded": True,
                "canonical_rank": index + 1,
            }
            for index, root in enumerate(roots)
        ],
    )
    reconciled = reliquary_one.reconcile_attempts(
        miner,
        windows=[window],
        hotkey=HOTKEY,
    )
    attempts = [
        row
        for row in reconciled["attempts"]
        if row["window_n"] == DEMO_WINDOW
    ]
    assert [row["auction"]["canonical_rank"] for row in attempts] == [1, 2]
    assert all(row["admission"]["status"] == "accepted" for row in attempts)
    assert all(row["auction"]["selected"] is True for row in attempts)
    assert all(row["auction"]["rewarded"] is True for row in attempts)


def test_v4_funnel_keeps_six_production_stages_distinct() -> None:
    miner = _normalized(_v4_payload())
    assert miner["funnel"] == {
        "generated_groups": 5,
        "protocol_valid_groups": 2,
        "locally_eligible_groups": 1,
        "signed_precommits": 1,
        "validator_ranked_candidates": 0,
        "selected_slots": 0,
    }
    submission = next(
        row for row in miner["attempts"] if row["window_n"] == 30001
    )
    assert submission["transport"]["status"] == "rejected"
    assert submission["transport"]["precommit_reason"] == "batch_filled"

    window = fleet.WindowSummary(
        n=30001,
        ours=1,
        total_batch=8,
        rt_first=1.0,
        rejects={},
        terminal_data_present=True,
        batch=[
            {
                "hotkey": HOTKEY,
                "merkle_root": "f" * 64,
                "env_name": "opencodeinstruct",
                "accepted_into_pool": True,
                "selected_for_batch": True,
                "canonical_rank": 3,
            }
        ],
    )
    reconciled = reliquary_one.reconcile_attempts(
        miner,
        windows=[window],
        hotkey=HOTKEY,
    )
    assert reconciled["funnel"] == {
        "generated_groups": 5,
        "protocol_valid_groups": 2,
        "locally_eligible_groups": 1,
        "signed_precommits": 1,
        "validator_ranked_candidates": 1,
        "selected_slots": 1,
    }


def test_terminal_r2_reconciles_after_direct_admission_verdict() -> None:
    miner = _normalized()
    roots = [
        row["merkle_root"]
        for row in miner["attempts"]
        if row["window_n"] == DEMO_WINDOW
    ]
    verdicts = [
        {
            "window_n": DEMO_WINDOW,
            "merkle_root": root,
            "accepted_into_pool": False,
            "reason": "out_of_zone",
        }
        for root in roots
    ]

    reconciled = reliquary_one.reconcile_attempts(
        miner,
        windows=[_r2_rejected_window()],
        hotkey=HOTKEY,
        verdict_rows=verdicts,
    )

    attempts = [
        row
        for row in reconciled["attempts"]
        if row["window_n"] == DEMO_WINDOW
    ]
    assert all(row["admission"]["source"] == "r2" for row in attempts)
    assert all(row["auction"]["selected"] is False for row in attempts)
    assert all(row["auction"]["rewarded"] is False for row in attempts)


def test_nonterminal_window_rows_leave_outcomes_pending() -> None:
    miner = _normalized()
    roots = [
        row["merkle_root"]
        for row in miner["attempts"]
        if row["window_n"] == DEMO_WINDOW
    ]
    window = fleet.WindowSummary(
        n=DEMO_WINDOW,
        ours=2,
        total_batch=8,
        rt_first=1.0,
        rejects={},
        terminal_data_present=False,
        batch=[
            {
                "hotkey": HOTKEY,
                "merkle_root": root,
                "env_name": "opencodeinstruct",
                "accepted_into_pool": True,
                "selected_for_batch": True,
                "rewarded": True,
            }
            for root in roots
        ],
    )

    reconciled = reliquary_one.reconcile_attempts(
        miner,
        windows=[window],
        hotkey=HOTKEY,
    )

    attempts = [
        row
        for row in reconciled["attempts"]
        if row["window_n"] == DEMO_WINDOW
    ]
    assert all(row["admission"]["status"] == "pending" for row in attempts)
    assert all(row["auction"]["selected"] is None for row in attempts)
    assert all(row["auction"]["rewarded"] is None for row in attempts)


def test_stopped_service_stale_events_and_ambiguous_send_fail_closed() -> None:
    stopped = _payload()
    stopped["service"]["active_state"] = "inactive"
    stopped["service"]["sub_state"] = "dead"
    assert _normalized(stopped)["fresh"] is False

    stale = _payload()
    stale["source_timestamps"]["events"] = NOW - 901
    assert _normalized(stale)["fresh"] is False

    ambiguous = _payload()
    ambiguous["events"][2]["metrics"].pop("reveal_accepted")
    attempts = _normalized(ambiguous)["attempts"]
    target = next(
        row
        for row in attempts
        if row["window_n"] == DEMO_WINDOW and row["ordinal"] == 1
    )
    assert target["transport"]["status"] == "ambiguous"
    assert target["admission"]["status"] == "pending"


def test_malformed_and_oversized_event_diagnostics_are_bounded() -> None:
    payload = _payload()
    payload["events"].extend(
        [
            {"event": "live_submission", "timestamp": "not-a-number"},
            {"event": "raw_prompt", "timestamp": NOW},
        ]
    )
    payload["event_diagnostics"] = {"malformed": 2, "oversized": 1, "retained": 8}
    miner = _normalized(payload)
    assert len(miner["events"]) == 8
    assert miner["event_diagnostics"] == {
        "malformed": 2,
        "oversized": 1,
        "retained": 8,
    }


def test_checkpoint_rotation_is_tiered_without_reading_checkpoint_payloads() -> None:
    miner = _normalized()
    assert miner["binding"]["checkpoint_number"] == CHECKPOINT_NUMBER
    assert miner["checkpoints"]["active"][0]["revision"] == REVISION
    assert miner["checkpoints"]["rollback"][0]["revision"].startswith(
        "2" * 8
    )
    assert miner["checkpoints"]["incoming"] == []
    command = reliquary_one.build_remote_probe_command(
        "/var/lib/reliquary-one/state",
        "reliquary-one.service",
    )
    assert "claimed.json" not in reliquary_one._REMOTE_PROBE
    assert "prepared.json" not in reliquary_one._REMOTE_PROBE
    assert "secret.env" not in reliquary_one._REMOTE_PROBE
    assert "reveal_b64" not in reliquary_one._REMOTE_PROBE
    assert "dashboard.json" not in reliquary_one._REMOTE_PROBE
    assert "fleet-events.jsonl" not in reliquary_one._REMOTE_PROBE
    assert "economic-ledger.jsonl" not in reliquary_one._REMOTE_PROBE
    assert (
        'submissions_raw.get("schema_version") in (1, 2, 3, 4)'
        in reliquary_one._REMOTE_PROBE
    )
    assert "validator_admitted" not in reliquary_one._REMOTE_PROBE
    assert 'raw.get("selected")' not in reliquary_one._REMOTE_PROBE
    assert 'raw.get("rewarded")' not in reliquary_one._REMOTE_PROBE
    assert command.startswith("sudo -n python3 -c ")


def test_private_host_sidecars_are_not_normalized_or_exported() -> None:
    payload = _v4_payload()
    payload["observability"] = {
        "private_snapshot": "must-not-cross-the-probe-boundary"
    }
    payload["v5_telemetry"] = {
        "economic_ledger": {"raw": "must-not-cross-the-probe-boundary"}
    }
    miner = _normalized(payload)
    assert "observability" not in miner
    assert "v5_telemetry" not in miner
    projection = reliquary_one.public_projection(miner)
    assert "must-not-cross-the-probe-boundary" not in json.dumps(
        projection, sort_keys=True
    )


def test_collector_uses_one_ssh_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def probe(_alias: str, command: str, timeout_s: int = 0):
        calls.append(command)
        return 0, json.dumps(_payload()), ""

    monkeypatch.setattr(fleet, "ssh_run", probe)
    monkeypatch.setattr(fleet.time, "time", lambda: NOW)
    state = _box()
    state.reliquary_one = {}
    fleet._collect_reliquary_one_in_place(state)
    assert len(calls) == 1
    assert state.proc_alive is True
    assert state.reliquary_one["fresh"] is True
    assert state.active_environment == "opencodeinstruct"


def test_config_has_explicit_reliquary_one_kind() -> None:
    row = settings._coerce_fleet_box(
        {
            "alias": "operator@example",
            "hotkey": HOTKEY,
            "label": "code-miner-one",
            "color": "#7bd389",
            "unit": "reliquary-one.service",
            "miner_kind": "reliquary_one",
            "state_root": "/var/lib/reliquary-one/state",
            "telemetry_stale_seconds": 900,
        }
    )
    assert row.miner_kind == "reliquary_one"
    assert row.state_root == "/var/lib/reliquary-one/state"
    with pytest.raises(ValueError, match="not legacy lane telemetry"):
        settings._coerce_fleet_box(
            {
                "alias": "operator@example",
                "hotkey": HOTKEY,
                "label": "bad",
                "unit": "reliquary-one.service",
                "miner_kind": "reliquary_one",
                "state_root": "/var/lib/reliquary-one/state",
                "telemetry_path": "/legacy/dashboard.json",
                "runtime_manifest_path": "/legacy/runtime.json",
            }
        )


def test_web_box_uses_reliquary_one_staleness_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    label = "code-miner-one"
    monkeypatch.setattr(
        fleet,
        "FLEET",
        [("operator@example", HOTKEY, label, "green", "reliquary-one.service")],
    )
    monkeypatch.setattr(fleet, "FLEET_STANDALONE_TELEMETRY", {})
    monkeypatch.setattr(
        fleet,
        "FLEET_RELIQUARY_ONE",
        {
            label: {
                "state_root": "/var/lib/reliquary-one/state",
                "stale_seconds": 900.0,
            }
        },
    )
    monkeypatch.setattr(fleet_web, "_boxes", [])

    fleet_web.init_state()

    assert len(fleet_web._boxes) == 1
    assert fleet_web._boxes[0].standalone_telemetry_stale_seconds == 900.0


def test_normalized_public_projection_has_no_sensitive_surfaces() -> None:
    public = reliquary_one.public_projection(_normalized())
    rendered = json.dumps(public, sort_keys=True).lower()
    for forbidden in (
        "wallet",
        "secret.env",
        "signature",
        "request_body",
        "raw_prompt",
        "token_stream",
        "proof_body",
        "randomness",
        "cooldown",
        "exception_text",
        "prepared.json",
        "reveal_b64",
        "merkle_root",
        "attempt_id",
    ):
        assert forbidden not in rendered
    assert "proof_s" in rendered
    assert "exception_file" in rendered


def test_demo_html_gate_and_fleet_health(monkeypatch: pytest.MonkeyPatch) -> None:
    box = _box()
    validator = _validator()
    window = _r2_rejected_window()
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_windows", [window])
    monkeypatch.setattr(fleet_web, "_vs", validator)
    monkeypatch.setattr(fleet_web, "_last_poll_at", NOW - 1)
    clock = type("Clock", (), {"time": staticmethod(lambda: NOW)})
    monkeypatch.setattr(fleet_web, "time", clock)
    monkeypatch.setattr(fleet_web, "verdict_rows_for_hotkey", lambda _hotkey: [])

    attempts_html = fleet_web.render_recent_miner_attempts_html()
    assert attempts_html.count("w42001") == 2
    for value in (
        "12.5000s",
        "48.2500s",
        "8.5000s",
        "0.0040s",
        "0.0800s",
        "0.2100s",
        "58.7500s",
        "85.0000s",
        "4.2500s",
        "0.7500s",
        "0.0010s",
        "0.0500s",
        "0.1800s",
        "5.7500s",
    ):
        assert value in attempts_html
    assert attempts_html.count("local accepted") == 2
    assert attempts_html.count("rejected · out_of_zone") == 2
    assert attempts_html.count("not selected") == 2
    assert attempts_html.count("not rewarded") == 2

    issues = fleet_web._box_readiness_issues(
        box,
        validator_state=validator,
        now=NOW,
        poll_limit=60,
    )
    assert issues == []


def test_dual_lane_funnel_and_ema_labels_are_unambiguous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    code = _box(_normalized(_v4_payload()))
    code.label = "demo-code-v4"
    code.unit = "reliquary-v4@code.service"
    math_miner = _normalized(_v4_payload())
    math_miner["binding"]["environment"] = "openmathinstruct"
    math_miner["binding"]["strategy"] = "math"
    math = _box(math_miner)
    math.label = "demo-math-v4"
    math.unit = "reliquary-v4@math.service"
    math.active_lane = "math"
    math.active_environment = "openmathinstruct"
    monkeypatch.setattr(fleet_web, "_boxes", [code, math])
    monkeypatch.setattr(fleet_web, "_windows", [])
    monkeypatch.setattr(fleet_web, "_vs", _validator())
    monkeypatch.setattr(fleet_web, "verdict_rows_for_hotkey", lambda _hotkey: [])

    rendered = fleet_web.render_our_miner_now_html()
    for label in (
        "generated groups",
        "protocol-valid groups",
        "locally eligible groups",
        "signed precommits",
        "validator-ranked candidates",
        "selected slots",
        "demo-code-v4",
        "demo-math-v4",
    ):
        assert label in rendered

    monkeypatch.setattr(
        fleet_web,
        "_ema_cache",
        [
            {
                "hotkey": HOTKEY,
                "rank": 1,
                "ema": 0.5,
                "slot_count_recent12": 2,
            }
        ],
    )
    monkeypatch.setattr(fleet_web, "OUR_SS58", {HOTKEY: "ours"})
    ema = fleet_web.render_ema_leaderboard_html()
    assert "selected slots · last 12" in ema
    assert "Selected slots in the last 12 sealed windows" in ema


def test_math_v4_unit_has_lane_specific_readiness_and_status() -> None:
    payload = _v4_payload()
    payload["service"]["unit"] = "reliquary-v4@math.service"
    payload["binding"]["environment"] = "openmathinstruct"
    payload["binding"]["strategy"] = "math"
    math = _box(_normalized(payload))
    math.unit = "reliquary-v4@math.service"
    math.active_unit = math.unit
    math.active_units = [math.unit]
    math.active_lane = "math"
    math.active_lanes = ["math"]
    math.active_environment = "openmathinstruct"

    assert fleet_web._box_readiness_issues(
        math,
        validator_state=_validator(),
        now=NOW,
        poll_limit=60.0,
    ) == []
    lane = fleet_web._box_lane_status(math, validator_state=_validator())
    assert lane["status"] == "MINING"
    assert lane["funnel_authoritative"] is True
    assert set(lane["funnel"]) == {
        "generated_groups",
        "protocol_valid_groups",
        "locally_eligible_groups",
        "signed_precommits",
        "validator_ranked_candidates",
        "selected_slots",
    }


@pytest.mark.parametrize(
    ("state_root", "strategy", "environment"),
    (
        ("/var/lib/reliquary-one/code/state", "code", "opencodeinstruct"),
        ("/var/lib/reliquary-one/math/state", "math", "openmathinstruct"),
    ),
)
def test_unified_service_derives_exact_lane_from_state_root(
    state_root: str,
    strategy: str,
    environment: str,
) -> None:
    payload = _v4_payload()
    payload["service"]["unit"] = "reliquary-one.service"
    payload["binding"].update(
        {"environment": environment, "strategy": strategy}
    )
    box = _box(_normalized(payload))
    box.reliquary_one_state_root = state_root
    box.active_lane = strategy
    box.active_lanes = [strategy]
    box.active_environment = environment

    assert fleet_web._reliquary_one_expected_lane(box) == (
        strategy,
        environment,
    )
    assert fleet_web._box_readiness_issues(
        box,
        validator_state=_validator(),
        now=NOW,
        poll_limit=60.0,
    ) == []
    assert fleet_web._box_lane_status(
        box, validator_state=_validator()
    )["status"] == "MINING"


def test_unified_service_with_ambiguous_state_root_fails_closed() -> None:
    box = _box()
    box.reliquary_one_state_root = "/var/lib/reliquary-one/state"

    assert fleet_web._reliquary_one_expected_lane(box) == ("", "")
    issues = fleet_web._box_readiness_issues(
        box,
        validator_state=_validator(),
        now=NOW,
        poll_limit=60.0,
    )
    assert "configured_lane_unknown" in issues
    assert fleet_web._box_lane_status(
        box, validator_state=_validator()
    )["status"] == "BLOCKED"


def test_v5_shared_hotkey_without_sidecars_is_unattributable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    code = _box(_normalized(_v4_payload()))
    code.label = "demo-code-node"
    code.unit = "reliquary-v4@code.service"
    math_payload = _v4_payload()
    math_payload["binding"].update(
        {"environment": "openmathinstruct", "strategy": "math"}
    )
    math = _box(_normalized(math_payload))
    math.label = "demo-math-node"
    math.unit = "reliquary-v4@math.service"
    math.active_unit = math.unit
    math.active_environment = "openmathinstruct"
    math.active_lane = "math"

    validator = _validator()
    validator.verdicts_by_hotkey = {
        HOTKEY: {
            "_fetched_at": NOW - 1,
            "accepted_30m": 99,
            "accepted_60m": 199,
            "rejected_30m": 9,
            "rejected_60m": 19,
        }
    }
    monkeypatch.setattr(fleet.time, "time", lambda: NOW)
    fleet.recompute_validator_acpts([code, math], validator)

    for box in (code, math):
        assert (box.acpt_30m, box.acpt_60m, box.rej_30m) == (None, None, None)
        assert box.acceptance_source == "shared_hotkey_aggregate_unattributable"
        assert box.acceptance_scope == "shared_hotkey_aggregate"
        assert box.shared_hotkey_verdicts["accepted_30m"] == 99


def test_dual_host_unattributable_counts_do_not_break_summary_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    code = _box(_normalized(_v4_payload()))
    code.label = "demo-code-node"

    math_payload = _v4_payload()
    math_payload["binding"].update(
        {"environment": "openmathinstruct", "strategy": "math"}
    )
    math = _box(_normalized(math_payload))
    math.label = "demo-math-node"
    math.reliquary_one_state_root = "/var/lib/reliquary-one/math/state"
    math.active_lane = "math"
    math.active_lanes = ["math"]
    math.active_environment = "openmathinstruct"

    validator = _validator()
    fleet.recompute_validator_acpts([code, math], validator)
    assert code.acpt_30m is None
    assert math.acpt_30m is None
    assert code.rej_30m is None
    assert math.rej_30m is None
    assert fleet_web._sum_available_counts([2, None]) is None

    monkeypatch.setattr(fleet_web, "_boxes", [code, math])
    monkeypatch.setattr(fleet_web, "_vs", validator)
    monkeypatch.setattr(fleet_web, "_windows", [])
    monkeypatch.setattr(fleet_web, "_chain", fleet.ChainState())
    monkeypatch.setattr(fleet_web, "_mission_current_events", lambda: [])
    monkeypatch.setattr(
        fleet_web,
        "_configured_target_rows",
        lambda *_args: [
            {
                "hotkey": HOTKEY,
                "label": "dual-v5",
                "aliases": [],
                "units": ["reliquary-one.service"],
                "registered": True,
            }
        ],
    )

    rendered = fleet_web.render_fleet_summary_html()
    assert "—/—" in rendered
    fleet_web._invalidate_dashboard_snapshot_cache()
    snapshot = fleet_web.render_dashboard_snapshot()
    assert "summary" in snapshot["panels"]
    assert "summary" not in snapshot["errors"]
    fleet_web._invalidate_dashboard_snapshot_cache()


def test_export_adds_redacted_normalized_miner_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    box = _box()
    monkeypatch.setattr(fleet_web, "_boxes", [box])
    monkeypatch.setattr(fleet_web, "_windows", [_r2_rejected_window()])
    monkeypatch.setattr(fleet_web, "_vs", _validator())
    monkeypatch.setattr(fleet_web, "verdict_rows_for_hotkey", lambda _hotkey: [])
    exported = fleet_web.render_export_json()["fleet"][0]
    assert exported["miner_kind"] == "reliquary_one"
    assert exported["miner"]["fresh"] is True
    serialized = json.dumps(exported["miner"], sort_keys=True).lower()
    assert "merkle_root" not in serialized
    assert "attempt_id" not in serialized
    assert "operator@example" not in serialized
