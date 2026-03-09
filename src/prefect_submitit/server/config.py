"""Server configuration and path resolution."""

from __future__ import annotations

import os
import shutil
import socket
import sys
from dataclasses import dataclass
from pathlib import Path

DEFAULT_PG_PORT = 5433
DEFAULT_DATA_DIR = Path.home() / ".prefect-submitit"
PG_USER = "prefect"
PG_DATABASE = "prefect"


def default_port() -> int:
    """Compute a UID-based port to avoid conflicts on shared nodes.

    Returns:
        Port in the range 4200-4999.
    """
    return 4200 + (os.getuid() % 800)


def default_host() -> str:
    """Return a resolvable hostname for cross-node access.

    Falls back to the IP of the short hostname when the FQDN
    (e.g. an IPv6 PTR for 0.0.0.0) cannot be resolved.

    Returns:
        Resolvable hostname or IP address.
    """
    hostname = socket.getfqdn()
    try:
        socket.getaddrinfo(hostname, None)
        return hostname
    except socket.gaierror:
        return socket.gethostbyname(socket.gethostname())


@dataclass(frozen=True)
class ServerConfig:
    """Immutable configuration for a Prefect server instance."""

    port: int
    host: str
    api_url: str
    data_dir: Path
    discovery_file: Path
    pg_data_dir: Path
    pg_port: int
    pg_user: str
    pg_database: str
    log_dir: Path


def make_config(
    port: int | None = None,
    pg_port: int | None = None,
) -> ServerConfig:
    """Build a ServerConfig with computed defaults.

    Args:
        port: Prefect server port. Defaults to UID-based port.
        pg_port: PostgreSQL port. Defaults to 5433.

    Returns:
        Fully populated ServerConfig.
    """
    port = port if port is not None else default_port()
    pg_port = pg_port if pg_port is not None else DEFAULT_PG_PORT
    host = default_host()
    data_dir = DEFAULT_DATA_DIR

    return ServerConfig(
        port=port,
        host=host,
        api_url=f"http://{host}:{port}/api",
        data_dir=data_dir,
        discovery_file=data_dir / "server.json",
        pg_data_dir=data_dir / "postgres",
        pg_port=pg_port,
        pg_user=PG_USER,
        pg_database=PG_DATABASE,
        log_dir=data_dir / "logs",
    )


def require_binary(name: str) -> str:
    """Find a binary on PATH or in the current interpreter's environment.

    Falls back to the directory containing ``sys.executable`` so that
    binaries installed alongside the Python interpreter (e.g. in a pixi
    or conda environment) are found even when PATH is not set.

    Args:
        name: Binary name to locate.

    Returns:
        Absolute path to the binary.

    Raises:
        FileNotFoundError: If the binary is not found.
    """
    path = shutil.which(name)
    if path is None:
        # Check the interpreter's own bin directory (handles Jupyter
        # kernels whose PATH doesn't include the env's bin dir).
        candidate = Path(sys.executable).resolve().parent / name
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
        msg = (
            f"Required binary '{name}' not found on PATH. "
            f"Install it or ensure your environment is activated."
        )
        raise FileNotFoundError(msg)
    return path
