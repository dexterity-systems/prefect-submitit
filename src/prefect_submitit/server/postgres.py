"""PostgreSQL lifecycle management."""

from __future__ import annotations

import contextlib
import os
import shutil as _shutil
import signal
import socket
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING

from prefect_submitit.server.config import require_binary

if TYPE_CHECKING:
    from prefect_submitit.server.config import ServerConfig

_START_SENTINEL = "# --- prefect-submitit custom settings ---"
_END_SENTINEL = "# --- end prefect-submitit custom settings ---"


def _pg_binary_version(pg_ctl_bin: str) -> str | None:
    """Extract the major version from the installed pg_ctl binary.

    Args:
        pg_ctl_bin: Path to the pg_ctl binary.

    Returns:
        Major version string (e.g. ``"16"``), or ``None`` if it cannot
        be determined.
    """
    try:
        result = subprocess.run(
            [pg_ctl_bin, "--version"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    if result.returncode != 0:
        return None
    # Output format: "pg_ctl (PostgreSQL) 16.2"
    parts = result.stdout.strip().split()
    for part in parts:
        if part[0].isdigit():
            return part.split(".")[0]
    return None


def _check_pg_version(config: ServerConfig, pg_ctl_bin: str) -> None:
    """Raise if the data directory version doesn't match the binary.

    Args:
        config: Server configuration.
        pg_ctl_bin: Path to the pg_ctl binary.

    Raises:
        RuntimeError: If versions are incompatible.
    """
    pg_version_file = config.pg_data_dir / "PG_VERSION"
    if not pg_version_file.exists():
        return
    try:
        data_version = pg_version_file.read_text().strip()
    except OSError:
        return
    binary_version = _pg_binary_version(pg_ctl_bin)
    if binary_version is None or data_version == binary_version:
        return
    msg = (
        f"PostgreSQL version mismatch: data directory was initialized "
        f"with version {data_version}, but the installed binary is "
        f"version {binary_version}.\n"
        f"Data directory: {config.pg_data_dir}\n\n"
        f"To fix, reinitialize the database:\n"
        f"  prefect-server init-db --reset\n"
        f"WARNING: this deletes all existing Prefect data."
    )
    raise RuntimeError(msg)


def _run_pg_cmd(
    cmd: list[str],
    *,
    label: str,
) -> subprocess.CompletedProcess[bytes]:
    """Run a PostgreSQL command with clear error reporting.

    Args:
        cmd: Command and arguments.
        label: Human-readable label for error messages (e.g. ``"initdb"``).

    Returns:
        The completed process result.

    Raises:
        RuntimeError: If the command exits non-zero.
    """
    try:
        return subprocess.run(cmd, check=True, capture_output=True)
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or b"").decode(errors="replace").strip()
        msg = f"{label} failed (exit code {exc.returncode})."
        if stderr:
            msg += f"\n{stderr}"
        raise RuntimeError(msg) from exc


def _write_custom_config(pg_data_dir: os.PathLike, pg_port: int) -> None:
    """Write sentinel-bounded custom config block to postgresql.conf.

    Idempotent: removes any existing block before writing.

    Args:
        pg_data_dir: Path to the PostgreSQL data directory.
        pg_port: Port for PostgreSQL to listen on.
    """
    conf = Path(pg_data_dir) / "postgresql.conf"
    lines = conf.read_text().splitlines(keepends=True)

    # Remove existing sentinel block
    filtered: list[str] = []
    inside = False
    for line in lines:
        if _START_SENTINEL in line:
            inside = True
            continue
        if _END_SENTINEL in line:
            inside = False
            continue
        if not inside:
            filtered.append(line)

    # Append new config block
    block = (
        f"{_START_SENTINEL}\n"
        f"listen_addresses = 'localhost'\n"
        f"port = {pg_port}\n"
        f"max_connections = 200\n"
        f"shared_buffers = 256MB\n"
        f"log_destination = 'stderr'\n"
        f"logging_collector = off\n"
        f"{_END_SENTINEL}\n"
    )
    filtered.append(block)

    conf.write_text("".join(filtered))


def is_running(config: ServerConfig) -> int | None:
    """Check if PostgreSQL is running.

    Args:
        config: Server configuration.

    Returns:
        PID if running, None otherwise.
    """
    pid_file = config.pg_data_dir / "postmaster.pid"
    if not pid_file.exists():
        return None
    try:
        pid = int(pid_file.read_text().split("\n")[0])
        os.kill(pid, 0)
        return pid
    except (ValueError, ProcessLookupError, PermissionError, OSError):
        return None


def init_db(config: ServerConfig, *, reset: bool = False) -> None:
    """Initialize the PostgreSQL database cluster.

    Args:
        config: Server configuration.
        reset: If True, delete existing data and reinitialize.

    Raises:
        FileNotFoundError: If initdb binary is not found.
        subprocess.CalledProcessError: If initdb fails.
    """
    initdb_bin = require_binary("initdb")
    pg_ctl_bin = require_binary("pg_ctl")
    createdb_bin = require_binary("createdb")

    if reset and config.pg_data_dir.exists():
        # Stop PG if running before removing data
        pid = is_running(config)
        if pid is not None:
            stop(config, force=True)
        _shutil.rmtree(config.pg_data_dir)

    if config.pg_data_dir.exists() and (config.pg_data_dir / "PG_VERSION").exists():
        # Already initialized — check version compatibility first
        _check_pg_version(config, pg_ctl_bin)
        _write_custom_config(config.pg_data_dir, config.pg_port)
        _ensure_database(config, pg_ctl_bin, createdb_bin)
        return

    config.pg_data_dir.mkdir(parents=True, exist_ok=True)

    _run_pg_cmd(
        [
            initdb_bin,
            "-D",
            str(config.pg_data_dir),
            "-U",
            config.pg_user,
            "--auth=trust",
            "--encoding=UTF8",
        ],
        label="initdb",
    )

    _write_custom_config(config.pg_data_dir, config.pg_port)

    # Temp start to create the database
    _run_pg_cmd(
        [
            pg_ctl_bin,
            "-D",
            str(config.pg_data_dir),
            "-l",
            str(config.pg_data_dir / "postgres.log"),
            "-o",
            f"-p {config.pg_port}",
            "start",
        ],
        label="pg_ctl start",
    )

    try:
        _wait_for_pg_ready(config.pg_port)

        _run_pg_cmd(
            [
                createdb_bin,
                "-h",
                "localhost",
                "-p",
                str(config.pg_port),
                "-U",
                config.pg_user,
                config.pg_database,
            ],
            label="createdb",
        )
    finally:
        subprocess.run(
            [pg_ctl_bin, "-D", str(config.pg_data_dir), "stop"],
            check=False,
            capture_output=True,
        )


def _ensure_database(
    config: ServerConfig,
    pg_ctl_bin: str,
    createdb_bin: str,
) -> None:
    """Ensure the prefect database exists in an initialized cluster.

    Starts PG temporarily if needed, creates the DB if missing.

    Args:
        config: Server configuration.
        pg_ctl_bin: Path to pg_ctl binary.
        createdb_bin: Path to createdb binary.
    """
    was_stopped = is_running(config) is None

    if was_stopped:
        _run_pg_cmd(
            [
                pg_ctl_bin,
                "-D",
                str(config.pg_data_dir),
                "-l",
                str(config.pg_data_dir / "postgres.log"),
                "-o",
                f"-p {config.pg_port}",
                "start",
                "-w",
                "-t",
                "10",
            ],
            label="pg_ctl start",
        )

    # createdb is idempotent-ish: it fails if DB exists, which we ignore
    subprocess.run(
        [
            createdb_bin,
            "-h",
            "localhost",
            "-p",
            str(config.pg_port),
            "-U",
            config.pg_user,
            config.pg_database,
        ],
        capture_output=True,
        check=False,
    )

    if was_stopped:
        subprocess.run(
            [pg_ctl_bin, "-D", str(config.pg_data_dir), "stop", "-w"],
            capture_output=True,
            check=False,
        )


def _find_pid_on_port(port: int) -> int | None:
    """Find the PID of a process listening on the given TCP port.

    Args:
        port: TCP port to check.

    Returns:
        PID of the listening process, or None if port is free or lsof
        is unavailable.

    Raises:
        FileNotFoundError: Re-raised so callers can detect missing lsof.
    """
    try:
        result = subprocess.run(
            ["lsof", "-ti", f"tcp:{port}", "-sTCP:LISTEN"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        raise
    if result.returncode != 0 or not result.stdout.strip():
        return None
    first_line = result.stdout.strip().splitlines()[0]
    try:
        return int(first_line)
    except ValueError:
        return None


def _is_postgres_process(pid: int) -> bool:
    """Check whether a PID belongs to a PostgreSQL process.

    Args:
        pid: Process ID to inspect.

    Returns:
        True if the process command name contains "postgres" or "postmaster".
    """
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "comm="],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return False
    comm = result.stdout.strip().lower()
    return "postgres" in comm or "postmaster" in comm


def _kill_orphan_on_port(port: int) -> bool:
    """Detect and kill an orphan PostgreSQL process holding a port.

    Args:
        port: TCP port to check.

    Returns:
        True if an orphan was found and killed, False if port was free.

    Raises:
        RuntimeError: If the port is occupied by a non-PostgreSQL process,
            or if lsof is unavailable and the port is in use.
    """
    try:
        pid = _find_pid_on_port(port)
    except FileNotFoundError:
        # lsof unavailable — fall back to TCP connect check
        try:
            with socket.create_connection(("localhost", port), timeout=1):
                pass
        except OSError:
            return False
        msg = (
            f"Port {port} is occupied but lsof is not available to identify "
            f"the process. Free the port manually and retry."
        )
        raise RuntimeError(msg) from None

    if pid is None:
        return False

    if not _is_postgres_process(pid):
        msg = f"Port {port} is occupied by PID {pid}, not PostgreSQL"
        raise RuntimeError(msg)

    # Graceful shutdown
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return True

    # Wait up to 5 seconds for process to exit
    for _ in range(50):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.1)

    # Force kill
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGKILL)
    return True


def _wait_for_pg_ready(
    pg_port: int,
    timeout: float = 10,
    poll: float = 0.5,
) -> None:
    """Wait for PostgreSQL to accept TCP connections.

    Args:
        pg_port: Port to check.
        timeout: Maximum wait time in seconds.
        poll: Time between checks in seconds.

    Raises:
        RuntimeError: If PostgreSQL doesn't become ready.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("localhost", pg_port), timeout=1):
                return
        except OSError:
            time.sleep(poll)
    msg = f"PostgreSQL did not become ready on port {pg_port} within {timeout}s"
    raise RuntimeError(msg)


def start(config: ServerConfig) -> int:
    """Start PostgreSQL if not already running.

    Auto-initializes the database if needed.

    Args:
        config: Server configuration.

    Returns:
        PID of the running PostgreSQL process.

    Raises:
        RuntimeError: If PostgreSQL fails to start.
    """
    pid = is_running(config)
    if pid is not None:
        return pid

    # Auto-init if needed
    if not (config.pg_data_dir / "PG_VERSION").exists():
        init_db(config)

    # Sync config
    _write_custom_config(config.pg_data_dir, config.pg_port)

    pg_ctl_bin = require_binary("pg_ctl")

    # Detect incompatible data directory before attempting start
    _check_pg_version(config, pg_ctl_bin)

    # Clean stale pid file
    pid_file = config.pg_data_dir / "postmaster.pid"
    if pid_file.exists() and is_running(config) is None:
        pid_file.unlink()

    # Kill orphan postgres holding our port (e.g. after pid file deletion)
    _kill_orphan_on_port(config.pg_port)

    _run_pg_cmd(
        [
            pg_ctl_bin,
            "-D",
            str(config.pg_data_dir),
            "-l",
            str(config.pg_data_dir / "postgres.log"),
            "-o",
            f"-p {config.pg_port}",
            "start",
        ],
        label="pg_ctl start",
    )

    _wait_for_pg_ready(config.pg_port)

    pid = is_running(config)
    if pid is None:
        msg = "PostgreSQL started but PID could not be determined"
        raise RuntimeError(msg)
    return pid


def stop(config: ServerConfig, *, force: bool = False) -> None:
    """Stop PostgreSQL.

    Args:
        config: Server configuration.
        force: If True, use SIGKILL as fallback after failed graceful stop.
    """
    pid = is_running(config)
    if pid is None:
        # Best-effort orphan cleanup; ignore if port is held by non-postgres
        with contextlib.suppress(RuntimeError):
            _kill_orphan_on_port(config.pg_port)
        return

    pg_ctl_bin = require_binary("pg_ctl")
    result = subprocess.run(
        [pg_ctl_bin, "-D", str(config.pg_data_dir), "stop", "-m", "fast"],
        capture_output=True,
        check=False,
    )

    if result.returncode != 0 and force:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)
