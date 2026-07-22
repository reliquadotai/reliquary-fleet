from __future__ import annotations

import json

import fleet


def test_remote_chain_probe_supports_root_protected_bittensor_venv(monkeypatch):
    payload = {
        "hotkeys": [
            {
                "label": "demo-miner",
                "hotkey": "demo-hotkey",
                "uid": 17,
                "stake": 42.5,
                "emission": 0.031,
            }
        ],
        "total": 42.5,
        "n": 256,
        "emit": 0.031,
    }

    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        assert alias == "operator@192.0.2.40"
        assert timeout_s == 30
        assert 'probe_python "$py"' in command
        assert 'sudo -n test -x "$py"' in command
        assert 'probe_python sudo -n "$py"' in command
        return 0, json.dumps(payload), ""

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    state = fleet.ChainState()

    fleet.fetch_chain_state(state, ssh_alias="operator@192.0.2.40")

    assert state.error == ""
    assert state.source_alias == "operator@192.0.2.40"
    assert state.netuid_size == 256
    assert state.total_stake == 42.5
    assert state.subnet_share == 0.031
    assert state.hotkeys[0].uid == 17


def test_remote_chain_probe_keeps_the_actionable_failure(monkeypatch):
    def fake_ssh(alias: str, command: str, timeout_s: int = 0):
        return 1, "", "\n".join(
            (
                "metagraph failed: ConnectionError: DNS unavailable",
                "candidate /opt/reliquary-venv/bin/python failed rc=1",
                "import bittensor failed: ModuleNotFoundError",
                "candidate python3 failed rc=1",
                "chain probe failed on all python candidates",
            )
        )

    monkeypatch.setattr(fleet, "ssh_run", fake_ssh)
    state = fleet.ChainState()

    fleet.fetch_chain_state(state, ssh_alias="operator@192.0.2.40")

    assert "metagraph failed: ConnectionError: DNS unavailable" in state.error
