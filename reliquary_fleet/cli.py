"""Supported command-line entry point for Reliquary Fleet."""

from __future__ import annotations

import argparse
import ipaddress
import os
import re
import shlex
import shutil
import stat
import sys
from contextlib import suppress
from importlib import resources
from pathlib import Path
from urllib.parse import urlsplit

from reliquary_fleet import __version__
from reliquary_fleet.paths import (
    CONFIG_ENV,
    STATE_ENV,
    resolve_config,
    resolve_state_dir,
    user_config_file,
)


def _is_loopback(host: str) -> bool:
    value = host.strip().strip("[]").lower()
    if value == "localhost":
        return True
    try:
        return ipaddress.ip_address(value).is_loopback
    except ValueError:
        return False


def _template_text() -> str:
    return (
        resources.files("reliquary_fleet")
        .joinpath("config.example.yaml")
        .read_text(encoding="utf-8")
    )


def _write_private(path: Path, content: str, *, force: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | (os.O_TRUNC if force else os.O_EXCL)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(content)
    except Exception:
        with suppress(OSError):
            os.close(descriptor)
        raise
    os.chmod(path, 0o600)


def command_init(args: argparse.Namespace) -> int:
    target = resolve_config(args.config) if args.config else user_config_file()
    try:
        _write_private(target, _template_text(), force=args.force)
    except FileExistsError:
        print(f"Config already exists: {target}", file=sys.stderr)
        print("Use --force only when you intend to replace it.", file=sys.stderr)
        return 2
    except OSError as exc:
        print(f"Could not create config safely: {exc}", file=sys.stderr)
        return 2
    print(f"Created private config: {target}")
    print(f"Next: edit it, then run `reliquary-fleet doctor --config {target}`")
    return 0


def _config_permission_warning(path: Path) -> str:
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
    except OSError:
        return ""
    if mode & 0o077:
        return f"config permissions are {mode:04o}; use 0600 if it contains credentials"
    return ""


def _doctor_config(config_path: Path) -> tuple[object | None, list[str], list[str]]:
    failures: list[str] = []
    warnings: list[str] = []
    if not config_path.is_file():
        failures.append(f"config not found: {config_path}")
        return None, failures, warnings

    if warning := _config_permission_warning(config_path):
        warnings.append(warning)

    try:
        import settings

        loaded = settings.load(config_path)
    except (FileNotFoundError, OSError, ValueError) as exc:
        failures.append(f"invalid config: {exc}")
        return None, failures, warnings

    parsed = urlsplit(loaded.validator_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        failures.append("validator.url must be an http(s) URL with a host")
    if parsed.username or parsed.password:
        failures.append("validator.url must not contain embedded credentials")
    if not loaded.validator_ssh_host:
        failures.append("validator.ssh.host is required")
    elif loaded.validator_ssh_host.endswith(".example"):
        failures.append("replace the example validator SSH host")
    if parsed.hostname and parsed.hostname.endswith(".example"):
        failures.append("replace the example validator URL")
    if not loaded.fleet:
        warnings.append("fleet is empty; miner panels will have no rows")
    for box in loaded.fleet:
        if (
            not box.alias
            or box.alias.startswith("-")
            or any(character.isspace() for character in box.alias)
        ):
            failures.append(
                f"fleet alias must be one SSH host token: {box.label or '?'}"
            )
        if not box.label:
            failures.append("every fleet row requires a label")
        if not re.fullmatch(
            r"[1-9A-HJ-NP-Za-km-z]{40,64}",
            box.hotkey,
        ):
            failures.append(
                f"fleet hotkey is not a full SS58 value: {box.label or '?'}"
            )
    for lab in loaded.labs:
        if lab.alias.startswith("-") or any(
            character.isspace() for character in lab.alias
        ):
            failures.append(f"lab alias must be one SSH host token: {lab.label or '?'}")
    if not 1 <= loaded.dashboard_port <= 65535:
        failures.append("dashboard.port must be in 1..65535")
    if not _is_loopback(loaded.dashboard_host):
        warnings.append(
            "dashboard.host is not loopback; use an authenticated reverse proxy "
            "and pass --allow-remote deliberately"
        )

    key_paths = {
        Path(str(key)).expanduser()
        for key in [
            loaded.validator_ssh_key,
            *(box.ssh_key for box in loaded.fleet),
            *(lab.ssh_key for lab in loaded.labs),
        ]
        if key
    }
    for key_path in sorted(key_paths):
        if not key_path.is_file():
            failures.append(f"SSH key not found: {key_path}")
            continue
        key_mode = stat.S_IMODE(key_path.stat().st_mode)
        if key_mode & 0o077:
            warnings.append(f"SSH key permissions are {key_mode:04o}: {key_path}")

    for binary in ("ssh", "curl"):
        if not shutil.which(binary):
            failures.append(f"required executable not found: {binary}")

    return loaded, failures, warnings


def _live_checks(loaded: object) -> tuple[list[str], list[str]]:
    failures: list[str] = []
    successes: list[str] = []

    try:
        import httpx

        response = httpx.get(f"{loaded.validator_url.rstrip('/')}/health", timeout=5)
        response.raise_for_status()
        successes.append("validator /health reachable")
    except Exception as exc:
        failures.append(f"validator /health failed: {type(exc).__name__}")

    try:
        import settings

        settings.apply_to_fleet_module()
        import fleet

        targets = {fleet.VALIDATOR_SSH, *(row[0] for row in fleet.FLEET)}
        for target in sorted(value for value in targets if value):
            code, _, error = fleet.ssh_run(target, "true", timeout_s=8)
            try:
                label = shlex.split(target)[-1]
            except (ValueError, IndexError):
                label = "invalid-target"
            if code == 0:
                successes.append(f"SSH reachable: {label}")
            else:
                failures.append(
                    f"SSH failed: {label} ({error.strip() or f'rc={code}'})"
                )
    except Exception as exc:
        failures.append(f"SSH checks failed to run: {type(exc).__name__}")
    return successes, failures


def command_doctor(args: argparse.Namespace) -> int:
    config_path = resolve_config(args.config)
    state_dir = resolve_state_dir(args.state_dir)
    loaded, failures, warnings = _doctor_config(config_path)

    print(f"Config: {config_path}")
    print(f"State:  {state_dir}")
    if loaded is not None:
        print(
            f"Fleet:  {len(loaded.fleet)} miner row(s), {len(loaded.labs)} lab row(s)"
        )
        print("OK     configuration parsed")
    if args.connect and loaded is not None and not failures:
        successes, live_failures = _live_checks(loaded)
        for message in successes:
            print(f"OK     {message}")
        failures.extend(live_failures)
    for message in warnings:
        print(f"WARN   {message}")
    for message in failures:
        print(f"FAIL   {message}")
    if failures:
        print(f"Doctor found {len(failures)} blocking issue(s).", file=sys.stderr)
        return 1
    print("Doctor passed." if not warnings else "Doctor passed with warnings.")
    return 0


def command_serve(args: argparse.Namespace) -> int:
    config_path = resolve_config(args.config)
    state_dir = resolve_state_dir(args.state_dir)
    if not config_path.is_file():
        print(f"Config not found: {config_path}", file=sys.stderr)
        print(
            "Run `reliquary-fleet init`, then edit the generated file.", file=sys.stderr
        )
        return 2

    os.environ[CONFIG_ENV] = str(config_path)
    os.environ[STATE_ENV] = str(state_dir)

    forwarded = ["--config", str(config_path), "--state-dir", str(state_dir)]
    for flag in ("host", "port", "refresh", "history"):
        value = getattr(args, flag)
        if value is not None:
            forwarded.extend([f"--{flag}", str(value)])
    if args.allow_remote:
        forwarded.append("--allow-remote")
    if args.open:
        forwarded.append("--open")

    from fleet_web import main as web_main

    return web_main(forwarded)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="reliquary-fleet",
        description=(
            "Monitor Reliquary miners, validator truth, chain state, and rewards."
        ),
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )
    subparsers = parser.add_subparsers(dest="command")

    init_parser = subparsers.add_parser("init", help="create a private starter config")
    init_parser.add_argument("--config", help="destination config path")
    init_parser.add_argument(
        "--force", action="store_true", help="replace an existing config"
    )
    init_parser.set_defaults(handler=command_init)

    doctor_parser = subparsers.add_parser(
        "doctor", help="validate config and prerequisites"
    )
    doctor_parser.add_argument("--config", help="config path")
    doctor_parser.add_argument("--state-dir", help="runtime state directory")
    doctor_parser.add_argument(
        "--connect",
        action="store_true",
        help="also test validator HTTP and SSH targets",
    )
    doctor_parser.set_defaults(handler=command_doctor)

    serve_parser = subparsers.add_parser("serve", help="run the local web dashboard")
    serve_parser.add_argument("--config", help="config path")
    serve_parser.add_argument("--state-dir", help="runtime state directory")
    serve_parser.add_argument(
        "--host", help="listen address (default comes from config)"
    )
    serve_parser.add_argument("--port", type=int, help="listen port")
    serve_parser.add_argument("--refresh", type=float, help="poll delay in seconds")
    serve_parser.add_argument("--history", type=int, help="R2 windows to retain")
    serve_parser.add_argument(
        "--open", action="store_true", help="open a browser after start"
    )
    serve_parser.add_argument(
        "--allow-remote",
        action="store_true",
        help="acknowledge a non-loopback bind; put authentication in front",
    )
    serve_parser.set_defaults(handler=command_serve)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        print("\nFirst run: reliquary-fleet init")
        return 0
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
