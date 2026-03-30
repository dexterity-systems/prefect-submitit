"""Discovery file management and health checks."""

from __future__ import annotations

import contextlib
import getpass
import json
import os
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import prefect

from prefect_submitit.server.config import DEFAULT_DATA_DIR

if TYPE_CHECKING:
    from prefect_submitit.server.config import ServerConfig


def write_discovery(
    config: ServerConfig,
    pid: int,
    backend: str,
) -> None:
    """Write the discovery file atomically.

    Args:
        config: Server configuration.
        pid: PID of the running Prefect server process.
        backend: Database backend ("postgres" or "sqlite").
    """
    data = {
        "url": config.api_url,
        "host": config.host,
        "port": config.port,
        "pid": pid,
        "user": getpass.getuser(),
        "backend": backend,
        "prefect_version": prefect.__version__,
        "started": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    config.discovery_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = config.discovery_file.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=4) + "\n")
    tmp.replace(config.discovery_file)


def read_discovery(path: Path | None = None) -> dict | None:
    """Read and parse the discovery file.

    Args:
        path: Path to discovery file. Defaults to standard location.

    Returns:
        Parsed discovery dict, or None if missing/malformed.
    """
    if path is None:
        path = DEFAULT_DATA_DIR / "server.json"
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def remove_discovery(config: ServerConfig) -> None:
    """Remove the discovery file if it exists.

    Args:
        config: Server configuration.
    """
    with contextlib.suppress(FileNotFoundError):
        config.discovery_file.unlink()


def health_check(url: str, timeout: float = 5) -> bool:
    """Check if a Prefect server is healthy.

    Args:
        url: Base API URL (e.g. "http://host:port/api").
        timeout: Request timeout in seconds.

    Returns:
        True if the server responds to health check.
    """
    endpoint = f"{url.rstrip('/')}/health"
    try:
        with urllib.request.urlopen(endpoint, timeout=timeout):
            return True
    except (urllib.error.URLError, OSError):
        return False


def wait_for_healthy(
    url: str,
    timeout: float = 30,
    poll: float = 1.0,
) -> None:
    """Poll until the server is healthy or timeout.

    Args:
        url: Base API URL.
        timeout: Maximum wait time in seconds.
        poll: Time between polls in seconds.

    Raises:
        RuntimeError: If the server is not healthy within the timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if health_check(url, timeout=min(poll, 3)):
            return
        time.sleep(poll)
    msg = f"Server at {url} did not become healthy within {timeout}s"
    raise RuntimeError(msg)


def resolve_api_url(explicit: str | None = None) -> str:
    """Resolve the Prefect API URL from multiple sources.

    Resolution order:
        1. Explicit argument
        2. PREFECT_SUBMITIT_SERVER env var
        3. PREFECT_API_URL env var
        4. Discovery file
        5. Raise with instructions

    Args:
        explicit: Explicitly provided URL.

    Returns:
        The resolved API URL.

    Raises:
        RuntimeError: If no URL can be resolved.
    """
    if explicit:
        return explicit

    env_submitit = os.environ.get("PREFECT_SUBMITIT_SERVER")
    if env_submitit:
        return env_submitit

    env_prefect = os.environ.get("PREFECT_API_URL")
    if env_prefect:
        return env_prefect

    discovery = read_discovery()
    if discovery and "url" in discovery:
        return discovery["url"]

    msg = (
        "Could not resolve Prefect API URL. Either:\n"
        "  - Start a server: prefect-server start\n"
        "  - Set PREFECT_API_URL or PREFECT_SUBMITIT_SERVER env var\n"
    )
    raise RuntimeError(msg)
