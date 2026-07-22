"""Network-free dashboard fixture used by visual tests and release captures."""

from __future__ import annotations

import argparse
import importlib
import sys
from collections import deque
from pathlib import Path
from types import SimpleNamespace

import uvicorn

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

fleet = importlib.import_module("fleet")
fleet_web = importlib.import_module("fleet_web")

FIXED_NOW = 1_784_688_400.0
CHECKPOINT_REVISION = "a" * 40
SOURCE_REVISION = "b" * 40
MODEL_REPOSITORY = "reliquary/demo-checkpoint"
HOTKEYS = (
    "demo-hotkey-alpha",
    "demo-hotkey-beta",
    "demo-hotkey-gamma",
)


class _FrozenClock:
    @staticmethod
    def time() -> float:
        return FIXED_NOW


def _box(index: int, label: str, hotkey: str, color: str) -> fleet.BoxState:
    pid = 4200 + index
    started_at = int(FIXED_NOW) - (9_400 + index * 620)
    return fleet.BoxState(
        alias=f"demo-node-{index + 1}",
        hotkey=hotkey,
        label=label,
        color=color,
        unit=f"reliquary-miner@demo-{index + 1}.service",
        env_file=f"/demo/miner-{index + 1}.env",
        unit_candidates=(
            (
                f"reliquary-miner@demo-{index + 1}.service",
                f"/demo/miner-{index + 1}.env",
            ),
        ),
        active_unit=f"reliquary-miner@demo-{index + 1}.service",
        active_lane=f"math-{index + 1}",
        active_units=[f"reliquary-miner@demo-{index + 1}.service"],
        active_lanes=[f"math-{index + 1}"],
        active_environment="openmathinstruct",
        active_pid=pid,
        env_file_ok=True,
        last_poll_s=FIXED_NOW - 2,
        gpu_mem_mb=(37 + index * 4) * 1024,
        gpu_total_mb=80 * 1024,
        gpu_util=76 + index * 6,
        proc_alive=True,
        proc_uptime_s=9_400 + index * 620,
        active_started_at=started_at,
        last_event_at=f"14:{32 + index:02d}:0{index}",
        acpt_30m=8 - index,
        rej_30m=index,
        acpt_60m=15 - index,
        pregen_30m=42 + index * 4,
        pregen_60m=83 + index * 7,
        last_oom_age_s=999_000,
        mean_accept_t_s=4.2 + index * 1.1,
        pregen_ok_30m=40 + index * 4,
        skip_30m=2 + index,
        miner_state="generating" if index == 2 else "ready",
        miner_window=7421,
        miner_valid=6,
        miner_inflight=2 + index,
        miner_ready=5 - index,
        miner_submitted_this_win=1 if index < 2 else 0,
        fresh_built_30m=18 + index * 3,
        cache_fwd_30m=12 + index,
        prefinalized_30m=7 + index,
        burst_30m=3 - min(index, 2),
        last_fresh_total_s=14.1 + index * 2.3,
        miner_environment="openmathinstruct",
        engine_mode="reference",
        protocol_profile="current",
        runtime_parity_ok=True,
        reference_ready=True,
        miner_source_revision="c" * 40,
        reliquary_source_revision=SOURCE_REVISION,
        source_manifest_provisioned_ok=True,
        provisioned_model_kind="validator_checkpoint",
        provisioned_checkpoint_n=24,
        provisioned_model_repo=MODEL_REPOSITORY,
        provisioned_model_revision=CHECKPOINT_REVISION,
        runtime_checkpoint_loaded=True,
        runtime_checkpoint_n=24,
        runtime_checkpoint_repo=MODEL_REPOSITORY,
        runtime_checkpoint_revision=CHECKPOINT_REVISION,
        runtime_checkpoint_pid=pid,
        runtime_checkpoint_started_at=started_at,
        runtime_checkpoint_evidence="loaded+generation+demo",
        acceptance_source="verdicts",
        final_accept_30m=8 - index,
        final_accept_60m=15 - index,
        final_reject_30m=index,
        final_reject_60m=index + 1,
        last_final_reason="accepted",
        last_final_window=7421,
        last_final_ts=FIXED_NOW - 8,
        drand_offset=index - 1,
        picker_epsilon=0.08 + index * 0.02,
        picker_top_k=64,
        external_min_sigma=0.43,
        external_max_len=4096,
        prompt_shard_id=index,
        prompt_shard_mod=3,
        frontier_path=f"/demo/frontier-{index + 1}.json",
        frontier_entries=1_420 + index * 180,
        frontier_content_entries=380 + index * 60,
        frontier_age_s=11.0 + index * 4,
        frontier_newest_age_s=4.0 + index,
        frontier_checkpoint_n=24,
        frontier_checkpoint_revision=CHECKPOINT_REVISION,
        frontier_ckpts={CHECKPOINT_REVISION[:12]: 1_300 + index * 120},
        local_checkpoint_n=24,
        local_checkpoint_revision=CHECKPOINT_REVISION,
        state_relay_ok=True,
        state_relay_age_s=1.2 + index * 0.3,
        state_relay_upstream_ms=17.0 + index * 2,
        watchdog_ok=True,
        watchdog_age_s=20 + index * 3,
        rss_mb=14_200 + index * 700,
        cpu_pct=34 + index * 6,
        disk_used_pct=42 + index * 3,
        acpt_trend=deque(
            [5 + index, 7 - index, 6 + index, 8 - index, 8 - index],
            maxlen=20,
        ),
        recent_lines=deque(
            [
                f"2026-07-22 14:3{index}:12 INFO cache ready for demo window 7421",
                f"2026-07-22 14:3{index}:42 INFO candidate admitted by "
                "local demo validator",
            ],
            maxlen=8,
        ),
    )


def _windows() -> list[fleet.WindowSummary]:
    slot_counts = [3, 2, 4, 1, 3, 2, 0, 3, 2, 4, 1, 2, 3, 1, 2, 3, 2, 1]
    competitors = ("demo-competitor-one", "demo-competitor-two")
    windows: list[fleet.WindowSummary] = []
    for offset, ours in enumerate(slot_counts):
        window_n = 7420 - offset
        batch = []
        rewards: dict[str, float] = {}
        for rank in range(1, 9):
            if rank <= ours:
                hotkey = HOTKEYS[(rank + offset) % len(HOTKEYS)]
            else:
                hotkey = competitors[(rank + offset) % len(competitors)]
            batch.append(
                {
                    "hotkey": hotkey,
                    "canonical_rank": rank,
                    "env_name": "openmathinstruct",
                    "response_time": 3.0 + rank * 0.7 + offset * 0.08,
                    "sigma": (0.500, 0.433, 0.484)[rank % 3],
                    "reward_vector": ("11110000", "11000000", "11100000")[rank % 3],
                }
            )
            rewards[hotkey] = rewards.get(hotkey, 0.0) + 0.125
        reward_ours = sum(rewards.get(hotkey, 0.0) for hotkey in HOTKEYS)
        windows.append(
            fleet.WindowSummary(
                n=window_n,
                ours=ours,
                total_batch=8,
                rt_first=3.7 + offset * 0.1,
                rejects={"batch_filled": 2} if offset in {5, 11} else {},
                reward_ours=reward_ours,
                reward_total=1.0,
                rewards_by_hotkey=rewards,
                reward_data_present=True,
                lifecycle_explicit=True,
                archive_schema_version=2,
                environments=["openmathinstruct"],
                environment_counts={"openmathinstruct": {"selected": 8}},
                ours_by_environment={"openmathinstruct": ours},
                rt_first_by_environment={"openmathinstruct": 3.7 + offset * 0.1},
                batch=batch,
                reject_summary={"batch_filled": 2} if offset in {5, 11} else {},
            )
        )
    return windows


def seed_fixture() -> None:
    fleet_web.time = _FrozenClock
    boxes = [
        _box(0, "alpha", HOTKEYS[0], "#7bd389"),
        _box(1, "beta", HOTKEYS[1], "#4fb8e0"),
        _box(2, "gamma", HOTKEYS[2], "#a78bfa"),
    ]
    fleet_rows = [
        (box.alias, box.hotkey, box.label, box.color, box.unit)
        for box in boxes
    ]
    labels = {box.hotkey: box.label for box in boxes}
    fleet.FLEET = fleet_rows
    fleet.OUR_SS58 = labels
    fleet.NETUID = 81
    fleet_web.FLEET = fleet_rows
    fleet_web.OUR_SS58 = labels
    fleet_web.NETUID = 81

    windows = _windows()
    validator = fleet.ValidatorState(
        state="open",
        window=7421,
        valid=6,
        checkpoint_n=24,
        checkpoint_repo_id=MODEL_REPOSITORY,
        checkpoint_revision=CHECKPOINT_REVISION,
        env_name="openmathinstruct",
        anchor_block=19_482_160,
        last_fetch_at=FIXED_NOW - 2,
        health_last_fetch_at=FIXED_NOW - 2,
        health_status="ok",
        health_raw={"image_revision": SOURCE_REVISION},
        image_revision=SOURCE_REVISION,
        app_started_at=FIXED_NOW - 7_200,
        batch_size=8,
        queue_depth=2,
        queue_depth_by_environment={"openmathinstruct": 2},
        admission_workers_by_environment={"openmathinstruct": 4},
        proof_admission_count=6,
        proof_admission_limit=8,
        proof_verification_inflight=1,
        proof_verification_inflight_by_environment={"openmathinstruct": 1},
        pending_proof_reservations=1,
        inflight_proof_reservations=1,
        liveness_telemetry_reported=True,
        event_loop_lag_ms={"p50": 1.8, "p95": 6.4, "p99": 11.2, "max": 18.1},
        endpoint_latency_ms={
            "/health": {"p50": 4.2, "p95": 9.1, "p99": 12.0, "max": 16.4},
            "/submit": {"p50": 41.0, "p95": 82.0, "p99": 103.0, "max": 121.0},
        },
        admission_latency_ms_by_environment={
            "openmathinstruct": {
                "queue_wait_ms": {"p95": 12.0, "p99": 18.0},
                "admission_prepare_ms": {"p95": 41.0, "p99": 55.0},
                "commit_lock_wait_ms": {"p95": 0.2, "p99": 0.3},
                "total_ms": {"p95": 70.0, "p99": 91.0},
            }
        },
        seal_drain_by_environment={
            "openmathinstruct": {
                "elapsed_seconds": 2.8,
                "timed_out": False,
                "queue_depth_at_snapshot": 0,
                "inflight_workers_at_snapshot": 0,
                "pending_reservations_at_snapshot": 0,
                "inflight_reservations_at_snapshot": 0,
            }
        },
        window_environments={
            "openmathinstruct": {
                "valid_submissions_count": 6,
                "target_batch_size": 8,
            }
        },
        environment_targets={"openmathinstruct": 8},
        recent_reject_counts={"batch_filled": 2},
        archive_queue_depth=0,
        archive_continuity_reported=True,
        archive_last_uploaded_window=7420,
        archive_last_enqueued_window=7420,
        archive_uploads_succeeded_total=7421,
        archive_upload_failures_total=0,
        archive_archives_enqueued_total=7421,
        archive_enqueue_gaps_total=0,
        verdicts_by_hotkey={
            hotkey: {
                "accepted_30m": 8 - index,
                "rejected_30m": index,
                "accepted_60m": 15 - index,
                "rejected_60m": index + 1,
            }
            for index, hotkey in enumerate(HOTKEYS)
        },
        verdicts_last_fetch_at=FIXED_NOW - 2,
    )

    lab = fleet.LabState(
        alias="demo-lab",
        label="offline-lab",
        color="#d44d7a",
        unit="reliquary-lab@demo.service",
        evidence_db="/demo/evidence.sqlite3",
        source_manifest="/demo/source.json",
        selector_artifact_manifest="/demo/selector.json",
        last_poll_s=FIXED_NOW - 4,
        gpu_name="Demo accelerator",
        gpu_mem_mb=21 * 1024,
        gpu_total_mb=48 * 1024,
        gpu_util=68,
        unit_active_state="active",
        unit_sub_state="running",
        unit_enablement="enabled",
        active_pid=5100,
        submit_disabled_attested=True,
        lane_start_disarmed=True,
        manifest_submit_disabled=True,
        manifest_provisioned_ok=True,
        miner_release="1.0-demo",
        source_revision="d" * 40,
        checkpoint_repo_id=MODEL_REPOSITORY,
        checkpoint_revision=CHECKPOINT_REVISION,
        checkpoint_n=24,
        environment="openmathinstruct",
        hardware_name="demo-accelerator",
        parity_status="exact",
        evidence_miner_release="1.0-demo",
        evidence_db_ok=True,
        evidence_schema_version=1,
        attempts=128,
        terminal_attempts=121,
        complete_attempts=108,
        censored_attempts=13,
        pending_attempts=7,
        first_window_n=7360,
        last_window_n=7420,
        last_attempt_at=FIXED_NOW - 24,
        artifact_manifest_ok=True,
        artifact_status="ready",
        artifact_kind="selector",
        artifact_model_version="demo-v3",
        artifact_activation_gate_passed=True,
        artifact_top_quintile_lift=0.18,
        artifact_heldout_value_lift=0.11,
        artifact_decision_reason="demo fixture",
    )

    chain_entries = [
        fleet.HotkeyChainEntry(
            label=box.label,
            hotkey=box.hotkey,
            uid=41 + index,
            stake=1_280.0 - index * 170.0,
            emission=(0.021, 0.017, 0.013)[index],
        )
        for index, box in enumerate(boxes)
    ]
    chain = fleet.ChainState(
        hotkeys=chain_entries,
        total_stake=sum(entry.stake for entry in chain_entries),
        subnet_share=sum(entry.emission for entry in chain_entries),
        netuid_size=256,
        last_fetch_at=FIXED_NOW - 18,
        source_alias="local chain client",
        probe_s=1.4,
    )

    fleet_web._boxes = boxes
    fleet_web._labs = [lab]
    fleet_web._vs = validator
    fleet_web._windows = windows
    fleet_web._last_poll_at = FIXED_NOW - 2
    fleet_web._poll_count = 84
    fleet_web._chain = chain
    fleet_web._chain_last = FIXED_NOW - 18
    fleet_web._rtt = fleet.RTTState(
        rtt_ms={"local": 4, "alpha": 18, "beta": 24, "gamma": 31},
        last_fetch_at=FIXED_NOW - 12,
    )
    fleet_web._baseline = fleet.BaselineState(
        samples=[
            (int(FIXED_NOW) - (59 - index) * 60, 20 + index % 5)
            for index in range(60)
        ],
        baseline_6h=22.0,
        current=21,
        drift_pct=-4.5,
        alert=False,
    )
    fleet_web._ema_cache = fleet.compute_ema_leaderboard(windows)
    fleet_web._ema_window_count = len(windows)
    fleet_web._deployment = SimpleNamespace(
        image_ref="reliquary/demo-validator:stable",
        image_sha="b7d9e21a4c10",
        image_sha_full="b7d9e21a4c10" + "0" * 52,
        started_at="2026-07-22T12:30:00Z",
        started_epoch=FIXED_NOW - 7_200,
        status="running",
        last_fetch_at=FIXED_NOW - 8,
        error="",
        uptime_s=7_200,
    )
    fleet_web._r2_status_snapshot = lambda: {
        "last_success_at": FIXED_NOW - 9,
        "last_error": "",
        "latest_window": 7420,
        "coverage_complete": True,
        "coverage": {
            "complete": True,
            "terminal_present": len(windows),
            "reward_expected": len(windows),
            "exact_rewards": len(windows),
        },
    }

    events = [
        fleet.ValidatorEvent(
            ts="14:33:42",
            ts_epoch=FIXED_NOW - 18,
            msg="accepted demo candidate · validation 4.2s",
            ours=True,
            hotkey12=HOTKEYS[0][:12],
            kind="accept",
            window_n=7421,
            env_name="openmathinstruct",
        ),
        fleet.ValidatorEvent(
            ts="14:33:37",
            ts_epoch=FIXED_NOW - 23,
            msg="accepted demo candidate · validation 5.1s",
            ours=True,
            hotkey12=HOTKEYS[1][:12],
            kind="accept",
            window_n=7421,
            env_name="openmathinstruct",
        ),
        fleet.ValidatorEvent(
            ts="14:33:28",
            ts_epoch=FIXED_NOW - 32,
            msg="rejected demo candidate · reason=batch_filled",
            ours=False,
            hotkey12="demo-competit",
            kind="reject",
            reject_reason="batch_filled",
            window_n=7421,
            env_name="openmathinstruct",
        ),
        fleet.ValidatorEvent(
            ts="14:29:00",
            ts_epoch=FIXED_NOW - 300,
            msg="sealed demo window 7420 · archive complete",
            ours=False,
            kind="seal",
            window_n=7420,
            env_name="openmathinstruct",
        ),
    ]
    with fleet._validator_events_lock:
        fleet._validator_events.clear()
        fleet._validator_events.extend(events)


seed_fixture()
app = fleet_web.make_app(5, poll_stale_s=60, demo_mode=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=19091)
    args = parser.parse_args()
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="info")
