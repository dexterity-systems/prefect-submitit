"""Prefect server process lifecycle management."""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from prefect_submitit.server import discovery, postgres
from prefect_submitit.server.config import require_binary

if TYPE_CHECKING:
    from prefect_submitit.server.config import ServerConfig


def _find_server_pids() -> list[int]:
    """Find PIDs of running Prefect server processes.

    Returns:
        List of PIDs.
    """
    try:
        result = subprocess.run(
            ["pgrep", "-f", "prefect server start"],
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

        discovery.write_discovery(config, proc.pid, backend)
        discovery.wait_for_healthy(config.api_url)
        return proc.pid
    proc = subprocess.Popen(cmd, env=env)

    discovery.write_discovery(config, proc.pid, backend)

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
