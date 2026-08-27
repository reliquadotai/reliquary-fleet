import subprocess

import fleet


def test_ssh_run_retries_mux_channel_failure_directly(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_run(argv, **_kwargs):
        calls.append(list(argv))
        if len(calls) == 1:
            return subprocess.CompletedProcess(
                argv,
                255,
                "",
                "mux_client_request_session: sendmsg(2): Message too long",
            )
        return subprocess.CompletedProcess(argv, 0, "ok\n", "")

    monkeypatch.setattr(fleet.subprocess, "run", fake_run)

    assert fleet.ssh_run("operator@example", "true") == (0, "ok\n", "")
    assert len(calls) == 2
    assert "ControlMaster=auto" in calls[0]
    assert "ControlMaster=no" in calls[1]
    assert "ControlPersist=no" in calls[1]
    assert "ControlPath=none" in calls[1]


def test_ssh_run_does_not_retry_remote_command_failure(monkeypatch) -> None:
    calls = 0

    def fake_run(argv, **_kwargs):
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(argv, 1, "", "remote failed")

    monkeypatch.setattr(fleet.subprocess, "run", fake_run)

    assert fleet.ssh_run("operator@example", "false") == (
        1,
        "",
        "remote failed",
    )
    assert calls == 1
