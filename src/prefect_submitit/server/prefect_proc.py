"""Prefect server process lifecycle management."""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import prefect

from prefect_submitit.server import discovery, postgres
from prefect_submitit.server.config import require_binary

if TYPE_CHECKING:
    from prefect_submitit.server.config import ServerConfig


def _find_server_pids() -> list[int]:
    """Find PIDs of running Prefect server processes owned by the current user.

    Returns:
        List of PIDs.
    """
    try:
        result = subprocess.run(
            ["pgrep", "-u", str(os.getuid()), "-f", "prefect server start"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return [int(p) for p in result.stdout.strip().split("\n") if p.strip()]
    except (FileNotFoundError, ValueError):
        pass
    return []


def _build_env(config: ServerConfig, *, sqlite: bool) -> dict[str, str]:
    """Build environment variables for the Prefect server process.

    Args:
        config: Server configuration.
        sqlite: If True, use SQLite backend (no DB URL set).

    Returns:
        Environment dict for subprocess.
    """
    env = os.environ.copy()
    env["PREFECT_HOME"] = str(config.data_dir)
    env["PREFECT_SERVER_API_HOST"] = "0.0.0.0"
    env["PREFECT_UI_API_URL"] = f"http://localhost:{config.port}/api"
    env["PREFECT_SERVER_DATABASE_SQLALCHEMY_POOL_SIZE"] = "50"
    env["PREFECT_SERVER_DATABASE_SQLALCHEMY_MAX_OVERFLOW"] = "120"

    if not sqlite:
        env["PREFECT_API_DATABASE_CONNECTION_URL"] = (
            f"postgresql+asyncpg://{config.pg_user}"
            f"@localhost:{config.pg_port}/{config.pg_database}"
        )

    return env


def _kill_pid(pid: int, timeout: float = 5.0) -> None:
    """Kill a process with SIGTERM, falling back to SIGKILL.

    Args:
        pid: Process ID to kill.
        timeout: Seconds to wait for graceful shutdown before SIGKILL.
    """
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
            time.sleep(0.5)
        except ProcessLookupError:
            return

    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGKILL)


def _get_prefect_version() -> str:
    """Return the installed Prefect package version."""
    return prefect.__version__


def _check_prefect_version(config: ServerConfig) -> str | None:
    """Check if the Prefect version changed since last successful start.

    Args:
        config: Server configuration.

    Returns:
        Warning message if the version changed, None otherwise.
    """
    version_file = config.data_dir / "prefect_version"
    try:
        stored = version_file.read_text().strip()
    except (FileNotFoundError, OSError):
        return None
    current = _get_prefect_version()
    if stored == current:
        return None
    return (
        f"Prefect version changed from {stored} to {current}. "
        f"Database migrations will run automatically."
    )


def _write_prefect_version(config: ServerConfig) -> None:
    """Record the current Prefect version in the data directory.

    Args:
        config: Server configuration.
    """
    config.data_dir.mkdir(parents=True, exist_ok=True)
    (config.data_dir / "prefect_version").write_text(_get_prefect_version() + "\n")


def _read_log_tail(log_file: Path, n: int = 20) -> str:
    """Read the last *n* lines of a log file.

    Args:
        log_file: Path to the log file.
        n: Number of trailing lines to return.

    Returns:
        The tail content, or a placeholder if the file is missing/empty.
    """
    try:
        lines = log_file.read_text().splitlines()
    except OSError:
        return "(no log output)"
    if not lines:
        return "(no log output)"
    return "\n".join(lines[-n:])


def _wait_for_healthy_or_death(
    proc: subprocess.Popen,
    url: str,
    log_file: Path,
    timeout: float = 30,
    poll: float = 1.0,
) -> None:
    """Wait for the server to become healthy, detecting early exit.

    Args:
        proc: The server Popen object.
        url: Base API URL for health checks.
        log_file: Path to the server log file.
        timeout: Maximum wait time in seconds.
        poll: Time between checks in seconds.

    Raises:
        RuntimeError: If the server exits or fails to become healthy.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if discovery.health_check(url, timeout=min(poll, 3)):
            return

        exit_code = proc.poll()
        if exit_code is not None:
            tail = _read_log_tail(log_file)
            msg = (
                f"Prefect server exited during startup "
                f"(exit code {exit_code}).\n\n"
                f"Last log lines from {log_file}:\n{tail}\n\n"
                f"To fix: check the full log above. If caused by a "
                f"version change, reinitialize:\n"
                f"  prefect-server init-db --reset"
            )
            raise RuntimeError(msg)

        time.sleep(poll)

    msg = (
        f"Prefect server not healthy after {timeout}s "
        f"(PID {proc.pid} still running).\n\n"
        f"The server may still be starting "
        f"(e.g. running database migrations).\n"
        f"Monitor: tail -f {log_file}\n"
        f"Check later: prefect-server status"
    )
    raise RuntimeError(msg)


def start(
    config: ServerConfig,
    *,
    background: bool = False,
    sqlite: bool = False,
) -> int:
    """Start the Prefect server.

    Args:
        config: Server configuration.
        background: If True, daemonize the server process.
        sqlite: If True, use SQLite backend instead of PostgreSQL.

    Returns:
        PID of the server process.

    Raises:
        FileNotFoundError: If prefect binary is not found.
        RuntimeError: If the server fails to start.
    """
    prefect_bin = require_binary("prefect")

    if not sqlite:
        postgres.start(config)

    env = _build_env(config, sqlite=sqlite)
    backend = "sqlite" if sqlite else "postgres"

    cmd = [
        prefect_bin,
        "server",
        "start",
        "--host",
        "0.0.0.0",
        "--port",
        str(config.port),
    ]

    if background:
        config.log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        log_file = config.log_dir / f"prefect-server-{timestamp}.log"

        with log_file.open("w") as lf:
            proc = subprocess.Popen(
                cmd,
                env=env,
                stdout=lf,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )

        _wait_for_healthy_or_death(proc, config.api_url, log_file)
        discovery.write_discovery(config, proc.pid, backend)
        _write_prefect_version(config)
        return proc.pid

    proc = subprocess.Popen(cmd, env=env)

    discovery.write_discovery(config, proc.pid, backend)
    _write_prefect_version(config)

    def _cleanup(signum: int, _frame: object) -> None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        discovery.remove_discovery(config)
        postgres.stop(config)
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, _cleanup)
    signal.signal(signal.SIGINT, _cleanup)

    try:
        proc.wait()
    finally:
        discovery.remove_discovery(config)
        postgres.stop(config)

    return proc.pid


def stop(
    config: ServerConfig,
    *,
    force: bool = False,
) -> None:
    """Stop the Prefect server and PostgreSQL.

    Args:
        config: Server configuration.
        force: If True, use SIGKILL as fallback.
    """
    pids = _find_server_pids()
    for pid in pids:
        _kill_pid(pid, timeout=5.0 if not force else 2.0)

    discovery.remove_discovery(config)
    postgres.stop(config, force=force)


def status(config: ServerConfig) -> dict | None:
    """Get the current server status.

    Args:
        config: Server configuration.

    Returns:
        Status dict with discovery info and health, or None if not running.
    """
    info = discovery.read_discovery(config.discovery_file)
    if info is None:
        return None

    url = info.get("url", "")
    info["healthy"] = discovery.health_check(url) if url else False
    return info
