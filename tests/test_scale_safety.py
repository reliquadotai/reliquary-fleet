from __future__ import annotations

import asyncio
import gzip
import json
import os
import threading
import time
from collections import deque
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

import fleet
import fleet_web
import settings
from reliquary_fleet import cli


def _request(app, method: str, path: str, **kwargs) -> httpx.Response:
    async def run() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            return await client.request(method, path, **kwargs)

    return asyncio.run(run())


def test_scale_safe_defaults_are_loaded(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "SETTINGS", settings.Settings())
    config = tmp_path / "config.yaml"
    config.write_text(
        "validator:\n  url: http://127.0.0.1:8080\n",
        encoding="utf-8",
    )

    loaded = settings.load(config)

    assert loaded.validator_state_seconds == 15
    assert loaded.validator_health_seconds == 30
    assert loaded.validator_verdict_seconds == 60
    assert loaded.max_verdict_hotkeys == 8
    assert loaded.chain_refresh_seconds == 300
    assert loaded.rtt_refresh_seconds == 120
    assert loaded.startup_jitter_seconds == 30
    assert fleet.R2_ALLOW_LIST_FALLBACK is False
    assert loaded.r2_allow_list_fallback is False
    assert loaded.r2_fetch_batch_size == 8
    assert loaded.r2_fetch_workers == 2
    assert loaded.r2_refresh_seconds == 60


def test_public_archive_configuration_is_credential_free(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(settings, "SETTINGS", settings.Settings())
    monkeypatch.delenv("R2_PUBLIC_BASE_URL", raising=False)
    config = tmp_path / "config.yaml"
    config.write_text(
        "r2:\n"
        "  public_base_url: https://archives.example/base\n"
        "  allow_list_fallback: false\n",
        encoding="utf-8",
    )

    loaded = settings.load(config)

    assert loaded.r2_public_base_url == "https://archives.example/base"


@pytest.mark.parametrize(
    "url",
    [
        "https://user:secret@validator.example:8080",
        "http://validator.example:8080/state",
        "http://validator.example:8080?token=secret",
        "http://validator.example;touch/tmp/x:8080",
    ],
)
def test_validator_configuration_rejects_non_origin_urls(
    tmp_path: Path,
    monkeypatch,
    url: str,
) -> None:
    monkeypatch.setattr(settings, "SETTINGS", settings.Settings())
    config = tmp_path / "config.yaml"
    config.write_text(
        f"validator:\n  url: {url}\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="validator.url"):
        settings.load(config)


def test_starred_config_rejects_non_ss58_values(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(settings, "SETTINGS", settings.Settings())
    config = tmp_path / "config.yaml"
    config.write_text(
        "validator:\n"
        "  url: http://127.0.0.1:8080\n"
        "starred_hotkeys:\n"
        "  - ../health\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="full SS58"):
        settings.load(config)


@pytest.mark.parametrize(
    "url",
    [
        "ftp://archives.example",
        "https://user:secret@archives.example",
        "https://archives.example/base?token=secret",
        "https://archives.example/base#fragment",
    ],
)
def test_public_archive_configuration_rejects_unsafe_urls(
    tmp_path: Path,
    monkeypatch,
    url: str,
) -> None:
    monkeypatch.setattr(settings, "SETTINGS", settings.Settings())
    monkeypatch.delenv("R2_PUBLIC_BASE_URL", raising=False)
    config = tmp_path / "config.yaml"
    config.write_text(
        f"r2:\n  public_base_url: {url}\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="public_base_url"):
        settings.load(config)


@pytest.mark.parametrize(
    ("body", "field"),
    [
        ("upstream:\n  validator_state_seconds: 9\n", "validator_state_seconds"),
        ("upstream:\n  validator_health_seconds: 10\n", "validator_health_seconds"),
        ("upstream:\n  validator_verdict_seconds: 10\n", "validator_verdict_seconds"),
        ("upstream:\n  max_verdict_hotkeys: 0\n", "max_verdict_hotkeys"),
        ("r2:\n  fetch_batch_size: 33\n", "fetch_batch_size"),
        (
            "r2:\n  fetch_batch_size: 2\n  fetch_workers: 3\n",
            "fetch_workers",
        ),
        ("r2:\n  allow_list_fallback: 1\n", "allow_list_fallback"),
    ],
)
def test_scale_limits_reject_unsafe_overrides(
    tmp_path: Path,
    monkeypatch,
    body: str,
    field: str,
) -> None:
    monkeypatch.setattr(settings, "SETTINGS", settings.Settings())
    config = tmp_path / "config.yaml"
    config.write_text(body, encoding="utf-8")

    with pytest.raises(ValueError, match=field):
        settings.load(config)


def test_http_only_doctor_mode_does_not_require_validator_ssh(
    tmp_path: Path,
) -> None:
    config = tmp_path / "config.yaml"
    config.write_text(
        "validator:\n"
        "  url: http://127.0.0.1:8080\n"
        "fleet: []\n",
        encoding="utf-8",
    )
    os.chmod(config, 0o600)

    loaded, failures, warnings = cli._doctor_config(config)

    assert loaded is not None
    assert failures == []
    assert any("validator SSH is not configured" in item for item in warnings)
    assert any("fleet is empty" in item for item in warnings)


def test_optional_validator_ssh_controls_privileged_collectors() -> None:
    runtime = settings.Settings()
    names = {
        item[0] for item in fleet_web._collector_specs(runtime, 5, 216)
    }
    assert "validator-tail" not in names
    assert "deployment" not in names

    runtime.validator_ssh_host = "ops@validator"
    names = {
        item[0] for item in fleet_web._collector_specs(runtime, 5, 216)
    }
    assert {"validator-tail", "deployment"} <= names


def test_request_budget_is_explicit_and_bounded(monkeypatch) -> None:
    runtime = settings.Settings()
    monkeypatch.setattr(settings, "SETTINGS", runtime)
    monkeypatch.setattr(
        fleet_web,
        "OUR_SS58",
        {f"hotkey-{index}": f"m{index}" for index in range(4)},
    )
    monkeypatch.setattr(
        fleet_web,
        "_boxes",
        [SimpleNamespace() for _ in range(4)],
    )

    budget = fleet_web._request_budget(5)

    assert budget["validator_requests_per_minute_upper_bound"] == 12.5
    assert budget["browser_local_requests_per_minute"] == 12.0
    assert budget["r2_cold_batch_objects"] == 8
    assert budget["r2_cold_max_concurrency"] == 2


def test_verdict_fetch_uses_incremental_overlap_after_initial_sync(
    monkeypatch,
) -> None:
    hotkey = "5" + "A" * 47
    monkeypatch.setattr(fleet, "FLEET", [])
    monkeypatch.setattr(fleet, "OUR_SS58", {hotkey: "miner"})
    monkeypatch.setattr(fleet, "VALIDATOR_MAX_VERDICT_HOTKEYS", 8)
    monkeypatch.setattr(fleet, "_verdict_rows_by_hotkey", {})
    monkeypatch.setattr(fleet, "_verdict_cursor_by_hotkey", {})
    current = {"now": 10_000.0}
    monkeypatch.setattr(fleet.time, "time", lambda: current["now"])
    calls: list[str] = []

    def fake_http(path: str, *, timeout: float = 0.0):
        calls.append(path)
        return {
            "verdicts": [
                {
                    "ts": current["now"] - 10,
                    "accepted": True,
                    "window_n": int(current["now"]),
                }
            ]
        }

    monkeypatch.setattr(fleet, "_http_json", fake_http)
    state = fleet.ValidatorState()

    fleet._fetch_validator_verdicts(state)
    current["now"] = 10_060.0
    fleet._fetch_validator_verdicts(state)

    first_since = float(calls[0].split("since=", 1)[1])
    second_since = float(calls[1].split("since=", 1)[1])
    assert first_since == 6_400.0
    assert second_since == 9_880.0
    assert state.verdicts_by_hotkey[hotkey]["accepted_60m"] == 2


def test_verdict_watch_cap_prioritizes_configured_fleet(monkeypatch) -> None:
    owned = ["5" + character * 47 for character in ("A", "B")]
    watched = [*owned, "5" + "C" * 47, "5" + "D" * 47]
    monkeypatch.setattr(
        fleet,
        "FLEET",
        [
            ("host-a", owned[0], "a", "cyan", None),
            ("host-b", owned[1], "b", "green", None),
        ],
    )
    monkeypatch.setattr(fleet, "OUR_SS58", {key: key for key in watched})
    monkeypatch.setattr(fleet, "VALIDATOR_MAX_VERDICT_HOTKEYS", 3)
    monkeypatch.setattr(fleet, "_verdict_rows_by_hotkey", {})
    monkeypatch.setattr(fleet, "_verdict_cursor_by_hotkey", {})
    calls: list[str] = []

    def fake_http(path: str, *, timeout: float = 0.0):
        calls.append(path)
        return {"verdicts": []}

    monkeypatch.setattr(fleet, "_http_json", fake_http)
    state = fleet.ValidatorState()
    fleet._fetch_validator_verdicts(state)

    assert [path.split("/", 1)[1].split("?", 1)[0] for path in calls] == [
        owned[0],
        owned[1],
        watched[2],
    ]
    assert state.verdicts_warning == "watch limit: polling 3 of 4 hotkeys"


def test_doctor_warns_when_configured_hotkeys_exceed_watch_cap(
    tmp_path: Path,
) -> None:
    hotkeys = [
        "5" + character * 47
        for character in ("A", "B", "C")
    ]
    ssh_key = tmp_path / "id_ed25519"
    ssh_key.write_text("test-only", encoding="utf-8")
    ssh_key.chmod(0o600)
    config = tmp_path / "config.yaml"
    config.write_text(
        f"""
validator:
  url: http://127.0.0.1:8000
  ssh:
    key: {ssh_key}
fleet:
  - alias: miner-a
    label: miner-a
    hotkey: {hotkeys[0]}
  - alias: miner-b
    label: miner-b
    hotkey: {hotkeys[1]}
starred_hotkeys:
  - {hotkeys[2]}
upstream:
  max_verdict_hotkeys: 2
""",
        encoding="utf-8",
    )

    loaded, failures, warnings = cli._doctor_config(config)

    assert loaded is not None
    assert not failures
    assert any("2 of 3 configured hotkeys" in warning for warning in warnings)


def test_retry_after_is_honored(monkeypatch) -> None:
    class Response:
        status_code = 429
        headers = {"retry-after": "7"}
        content = b""

        @staticmethod
        def raise_for_status():
            raise RuntimeError("throttled")

    class Client:
        @staticmethod
        def get(*args, **kwargs):
            return Response()

    monkeypatch.setattr(fleet, "_http_client", lambda: Client())
    monkeypatch.setattr(fleet, "_validator_retry_after_until", 0.0)
    monkeypatch.setattr(fleet.time, "time", lambda: 1_000.0)

    with pytest.raises(RuntimeError, match="throttled"):
        fleet._http_json("state")

    assert fleet.validator_retry_after_remaining(now=1_000.0) == 7.0


def test_public_archive_transport_is_credential_free_and_bounded(
    monkeypatch,
) -> None:
    payload = gzip.compress(
        json.dumps(
            {"batch": [], "rejected": [], "rewards_by_hotkey": {}}
        ).encode()
    )
    seen: list[str] = []

    class Response:
        status_code = 200
        content = payload

        @staticmethod
        def raise_for_status():
            return None

    class Client:
        @staticmethod
        def get(url, **kwargs):
            seen.append(url)
            return Response()

    monkeypatch.setattr(fleet, "_http_client", lambda: Client())
    client = fleet._PublicR2Client("https://archives.example")

    result = fleet._r2_get_object(
        client,
        bucket="unused",
        key="reliquary/dataset/window-42.json.gz",
    )

    assert result == payload
    assert seen == [
        "https://archives.example/reliquary/dataset/window-42.json.gz"
    ]


def test_missing_archive_index_serves_cache_without_bucket_list(
    monkeypatch,
) -> None:
    class Client:
        @staticmethod
        def list_objects_v2(**kwargs):
            raise AssertionError("LIST must remain disabled")

    cached = fleet.WindowSummary(
        n=42,
        ours=0,
        total_batch=0,
        rt_first=0,
        rejects={},
        reward_data_present=True,
    )
    monkeypatch.setattr(fleet, "_window_cache", {42: cached})
    monkeypatch.setattr(fleet, "r2", lambda: Client())
    monkeypatch.setenv("R2_BUCKET", "bucket")

    windows = fleet.fetch_recent_windows(
        1,
        archive_health={},
        allow_list_fallback=False,
    )

    assert [window.n for window in windows] == [42]
    assert fleet.r2_status()["discovery_source"] == "cache_without_archive_index"


def test_r2_cold_warmup_respects_batch_and_worker_caps(monkeypatch) -> None:
    payload = gzip.compress(
        json.dumps(
            {"batch": [], "rejected": [], "rewards_by_hotkey": {}}
        ).encode()
    )

    class Client:
        active = 0
        maximum = 0
        calls = 0
        lock = threading.Lock()

        def get_object(self, **kwargs):
            with self.lock:
                self.active += 1
                self.maximum = max(self.maximum, self.active)
                self.calls += 1
            time.sleep(0.01)
            with self.lock:
                self.active -= 1
            return {"Body": BytesIO(payload)}

    client = Client()
    monkeypatch.setattr(fleet, "_window_cache", {})
    monkeypatch.setattr(fleet, "r2", lambda: client)
    monkeypatch.setattr(fleet, "window_cache_save", lambda: None)
    monkeypatch.setattr(fleet, "R2_FETCH_BATCH_SIZE", 3)
    monkeypatch.setattr(fleet, "R2_FETCH_WORKERS", 2)
    monkeypatch.setenv("R2_BUCKET", "bucket")

    fleet.fetch_recent_windows(
        10,
        archive_health={"archive_last_uploaded_window": 10},
    )

    assert client.calls == 3
    assert client.maximum <= 2


def test_dashboard_snapshot_is_single_failure_isolated_payload(monkeypatch) -> None:
    fleet_web._invalidate_dashboard_snapshot_cache()
    app = fleet_web.make_app(5)

    response = _request(app, "GET", "/api/dashboard-snapshot")

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == 1
    assert len(payload["panels"]) == 18
    assert set(payload["panels"]) == {
        "score",
        "windows",
        "ema",
        "fleet",
        "summary",
        "forensics",
        "pipeline",
        "frontier",
        "labs",
        "rundown",
        "validator_events",
        "chain",
        "slotrank",
        "baseline",
        "competitors",
        "rtt",
        "quality",
        "events",
    }


def test_logs_return_304_when_filtered_buffer_is_unchanged(monkeypatch) -> None:
    event = fleet.ValidatorEvent(
        ts="12:00:00",
        ts_epoch=1_000.0,
        msg="accepted test candidate",
        ours=True,
        hotkey12="test-hotkey",
        kind="accept",
    )
    monkeypatch.setattr(
        fleet,
        "_validator_events",
        deque([event], maxlen=6000),
    )
    app = fleet_web.make_app(5)

    first = _request(app, "GET", "/api/logs?kinds=accept")
    second = _request(
        app,
        "GET",
        "/api/logs?kinds=accept",
        headers={"If-None-Match": first.headers["etag"]},
    )

    assert first.status_code == 200
    assert second.status_code == 304
    assert second.content == b""


def test_browser_polling_is_consolidated_and_visibility_aware() -> None:
    script = (
        Path(__file__).parents[1]
        / "reliquary_fleet"
        / "static"
        / "dashboard.js"
    ).read_text(encoding="utf-8")
    logs = (
        Path(__file__).parents[1]
        / "reliquary_fleet"
        / "static"
        / "logs.js"
    ).read_text(encoding="utf-8")

    assert "/api/dashboard-snapshot" in script
    assert "visibilitychange" in script
    assert "document.hidden" in script
    assert "visibilitychange" in logs
    assert "If-None-Match" in logs
