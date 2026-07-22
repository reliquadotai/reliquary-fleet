from __future__ import annotations

import asyncio
import hashlib
import os
import re
import socket
import stat
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import httpx

import fleet
import fleet_web
import starred
from reliquary_fleet import __version__, cli
from reliquary_fleet.paths import resolve_config, resolve_state_dir


def _request(app, method: str, path: str, **kwargs) -> httpx.Response:
    async def run() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            return await client.request(method, path, **kwargs)

    return asyncio.run(run())


def test_release_version_is_1_0_1() -> None:
    assert __version__ == "1.0.1"


def test_packaged_config_matches_repository_template() -> None:
    root_template = Path(__file__).parents[1] / "config.example.yaml"
    packaged_template = (
        Path(cli.resources.files("reliquary_fleet")) / "config.example.yaml"
    )
    assert packaged_template.read_bytes() == root_template.read_bytes()


def test_init_creates_owner_only_config_and_refuses_overwrite(tmp_path: Path) -> None:
    destination = tmp_path / "config.yaml"
    args = cli._parser().parse_args(["init", "--config", str(destination)])

    assert args.handler(args) == 0
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    original = destination.read_bytes()

    assert args.handler(args) == 2
    assert destination.read_bytes() == original


def test_init_force_does_not_follow_a_symlink(tmp_path: Path) -> None:
    victim = tmp_path / "victim"
    victim.write_text("keep me", encoding="utf-8")
    destination = tmp_path / "config.yaml"
    destination.symlink_to(victim)
    args = cli._parser().parse_args(["init", "--config", str(destination), "--force"])

    assert args.handler(args) == 2
    assert victim.read_text(encoding="utf-8") == "keep me"


def test_path_resolution_prefers_explicit_then_environment(
    tmp_path: Path, monkeypatch
) -> None:
    explicit = tmp_path / "explicit.yaml"
    configured = tmp_path / "configured.yaml"
    state = tmp_path / "state"
    monkeypatch.setenv("RELIQUARY_FLEET_CONFIG", str(configured))
    monkeypatch.setenv("RELIQUARY_FLEET_STATE_DIR", str(state))

    assert resolve_config(explicit) == explicit.resolve()
    assert resolve_config() == configured.resolve()
    assert resolve_state_dir() == state.resolve()


def test_starter_config_requires_operator_changes(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    config.write_text(cli._template_text(), encoding="utf-8")
    os.chmod(config, 0o600)

    loaded, failures, warnings = cli._doctor_config(config)

    assert loaded is not None
    assert any("example validator URL" in failure for failure in failures)
    assert any("example validator SSH host" in failure for failure in failures)
    assert any("full SS58" in failure for failure in failures)
    assert warnings == []


def test_state_files_follow_configured_private_directory(tmp_path: Path) -> None:
    fleet.configure_state_dir(str(tmp_path))
    starred.configure_state_dir(tmp_path)
    baseline = fleet.BaselineState()

    fleet.baseline_record(baseline, 3)
    starred.add("5" + "A" * 47)

    assert Path(fleet.BASELINE_FILE).parent == tmp_path
    assert stat.S_IMODE(Path(fleet.BASELINE_FILE).stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / "starred.json").stat().st_mode) == 0o600


def test_web_assets_and_security_headers_are_local() -> None:
    app = fleet_web.make_app(5)

    page = _request(app, "GET", "/")
    assert page.status_code == 200
    assert 'src="/static/htmx.min.js"' in page.text
    assert 'href="/static/dashboard.css?v=1.0.1"' in page.text
    assert 'src="/static/dashboard.js?v=1.0.1"' in page.text
    assert "unpkg.com" not in page.text
    assert page.headers["x-frame-options"] == "DENY"
    policy = page.headers["content-security-policy"]
    assert "default-src 'self'" in policy
    assert "script-src 'self';" in policy
    assert "script-src 'self' 'unsafe-inline'" not in policy

    asset = _request(app, "GET", "/static/htmx.min.js")
    digest = hashlib.sha384(asset.content).digest()
    assert asset.status_code == 200
    assert digest.hex() == (
        "1f94ab71fca01e602e4c366984c1ea0492dcdc586cb0a8c6ef0fc2782a4545e49"
        "fc015834caa64ccf3fc73e70bb0af95"
    )

    expected_assets = {
        "/static/dashboard.css": "text/css",
        "/static/dashboard.js": "text/javascript",
        "/static/logs.css": "text/css",
        "/static/logs.js": "text/javascript",
        "/static/brand/mark-outline.svg": "image/svg+xml",
        "/static/brand/relic.svg": "image/svg+xml",
        "/static/fonts/geist-sans.woff2": "font/woff2",
        "/static/fonts/jetbrains-mono.woff2": "font/woff2",
    }
    for path, media_type in expected_assets.items():
        response = _request(app, "GET", path)
        assert response.status_code == 200
        assert response.headers["content-type"].startswith(media_type)
        assert response.headers["cache-control"].endswith("immutable")
        assert response.content
    assert _request(app, "GET", "/static/not-packaged.js").status_code == 404


def test_demo_badge_is_explicit_and_opt_in() -> None:
    production_page = _request(fleet_web.make_app(5), "GET", "/").text
    demo_page = _request(fleet_web.make_app(5, demo_mode=True), "GET", "/").text
    assert "demo data" not in production_page
    assert 'data-demo="false"' in production_page
    assert "demo data" in demo_page
    assert 'data-demo="true"' in demo_page
    assert 'hx-trigger="load, every' in production_page
    assert 'hx-trigger="load, every' not in demo_page


def test_fixture_source_contains_only_synthetic_identifiers() -> None:
    fixture = Path(__file__).with_name("ui_fixture_server.py").read_text(
        encoding="utf-8"
    )
    ipv4 = set(re.findall(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", fixture))
    assert ipv4 <= {"127.0.0.1"}
    assert re.search(r"\b[1-9A-HJ-NP-Za-km-z]{40,64}\b", fixture) is None
    assert "prompt_idx" not in fixture


def test_collectors_wait_for_uvicorn_readiness() -> None:
    server = SimpleNamespace(started=False, should_exit=False)
    collector_started = threading.Event()
    supervisor = threading.Thread(
        target=fleet_web._start_collectors_after_ready,
        args=(server, [("fixture", collector_started.set, ())]),
        kwargs={"wait_s": 0.001},
    )
    supervisor.start()
    time.sleep(0.02)
    assert collector_started.is_set() is False

    server.started = True
    assert collector_started.wait(1.0)
    supervisor.join(1.0)
    assert supervisor.is_alive() is False


def test_star_mutation_requires_same_origin_header() -> None:
    app = fleet_web.make_app(5)
    hotkey = "5" + "A" * 47

    assert _request(app, "POST", f"/api/star/{hotkey}").status_code == 403
    assert (
        _request(
            app,
            "POST",
            "/api/star/not-a-hotkey",
            headers={"X-Reliquary-Fleet": "1"},
        ).status_code
        == 400
    )


def test_non_loopback_bind_requires_explicit_acknowledgement(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    config.write_text(
        "validator:\n"
        "  url: http://127.0.0.1:8080\n"
        "  ssh:\n"
        "    host: operator@validator\n"
        "    key: ~/.ssh/id_ed25519\n"
        "fleet: []\n"
        "dashboard:\n"
        "  host: 0.0.0.0\n",
        encoding="utf-8",
    )

    assert (
        fleet_web.main(
            ["--config", str(config), "--state-dir", str(tmp_path / "state")]
        )
        == 2
    )


def test_instance_lock_rejects_duplicate(tmp_path: Path) -> None:
    first, _ = fleet_web._acquire_instance_lock(str(tmp_path), 19091)
    second, owner = fleet_web._acquire_instance_lock(str(tmp_path), 19091)
    try:
        assert first is True
        assert second is False
        assert owner == str(os.getpid())
    finally:
        fleet_web._release_instance_lock()


def test_port_preflight_rejects_an_existing_listener() -> None:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    port = listener.getsockname()[1]
    try:
        available, error = fleet_web._port_available("127.0.0.1", port)
        assert available is False
        assert error
    finally:
        listener.close()
