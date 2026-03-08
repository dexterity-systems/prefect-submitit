"""Fixtures, hooks, and configuration for SLURM integration tests."""

from __future__ import annotations

import dataclasses
import os
import socket
import subprocess
import time
import urllib.request
from pathlib import Path

import pytest

from prefect_submitit import SlurmTaskRunner

from .helpers import find_free_port

# ---------------------------------------------------------------------------
# Pytest hooks
# ---------------------------------------------------------------------------


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--run-slurm"):
        skip = pytest.mark.skip(reason="need --run-slurm to run")
        for item in items:
            if "slurm" in item.keywords:
                item.add_marker(skip)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class SlurmTestConfig:
    partition: str
    mem_gb: int
    time_limit: str
    account: str | None
    qos: str | None
    max_wait: int
    gpu_partition: str | None
    log_dir: Path


@pytest.fixture(scope="session")
def slurm_config():
    """Read SLURM test configuration from environment variables."""
    partition = os.environ.get("SLURM_TEST_PARTITION")
    if not partition:
        pytest.skip("SLURM_TEST_PARTITION not set")

    log_dir_env = os.environ.get("SLURM_TEST_LOG_DIR")
    if log_dir_env:
        log_dir = Path(log_dir_env)
        log_dir.mkdir(parents=True, exist_ok=True)
    else:
        log_dir = Path.cwd() / ".slurm_test_logs"
        log_dir.mkdir(parents=True, exist_ok=True)

    return SlurmTestConfig(
        partition=partition,
        mem_gb=int(os.environ.get("SLURM_TEST_MEM_GB", "1")),
        time_limit=os.environ.get("SLURM_TEST_TIME_LIMIT", "00:05:00"),
        account=os.environ.get("SLURM_TEST_ACCOUNT"),
        qos=os.environ.get("SLURM_TEST_QOS"),
        max_wait=int(os.environ.get("SLURM_TEST_MAX_WAIT", "300")),
        gpu_partition=os.environ.get("SLURM_TEST_GPU_PARTITION"),
        log_dir=log_dir,
    )


# ---------------------------------------------------------------------------
# SLURM availability guard
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def _check_slurm_available(request):
    if not request.config.getoption("--run-slurm"):
        return
    try:
        subprocess.run(
            ["squeue", "--me", "--noheader"],
            capture_output=True,
            timeout=10,
            check=True,
        )
    except (FileNotFoundError, subprocess.SubprocessError) as e:
        pytest.skip(f"SLURM not available: {e}")


# ---------------------------------------------------------------------------
# Prefect server
# ---------------------------------------------------------------------------


def _wait_for_server(url, timeout=30):
    """Poll until the Prefect API is responding."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url + "/health", timeout=5):
                return
        except Exception:
            time.sleep(1)
    msg = f"Prefect server at {url} did not become ready within {timeout}s"
    raise RuntimeError(msg)


def _get_routable_hostname() -> str:
    """Return a hostname or IP that compute nodes can reach."""
    hostname = socket.getfqdn()
    try:
        socket.getaddrinfo(hostname, None)
        return hostname
    except socket.gaierror:
        return socket.gethostbyname(socket.gethostname())


@pytest.fixture(scope="session", autouse=True)
def prefect_server(request, slurm_config):  # noqa: ARG001
    """Start an ephemeral Prefect server for the test session.

    Always starts a fresh server to avoid version mismatches with
    pre-existing servers. Any existing PREFECT_API_URL is overridden.
    """
    if not request.config.getoption("--run-slurm"):
        yield None
        return

    hostname = _get_routable_hostname()
    port = find_free_port()
    api_url = f"http://{hostname}:{port}/api"

    proc = subprocess.Popen(
        ["prefect", "server", "start", "--host", "0.0.0.0", "--port", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        _wait_for_server(api_url, timeout=30)
    except RuntimeError:
        proc.terminate()
        proc.wait(timeout=10)
        pytest.skip("Could not start Prefect server")

    # Override both env var (for subprocesses) and Prefect's in-process
    # settings cache (profile values are cached at import time).
    from prefect.settings import get_current_settings

    old_url = os.environ.get("PREFECT_API_URL")
    os.environ["PREFECT_API_URL"] = api_url
    settings = get_current_settings()
    old_settings_url = settings.api.url
    settings.api.url = api_url
    yield api_url

    os.environ.pop("PREFECT_API_URL", None)
    if old_url:
        os.environ["PREFECT_API_URL"] = old_url
    settings.api.url = old_settings_url
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# Runner fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def slurm_runner(slurm_config):
    """A configured SlurmTaskRunner for testing."""
    log_dir = slurm_config.log_dir / "slurm_logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    extra_kwargs = {}
    if slurm_config.account:
        extra_kwargs["slurm_account"] = slurm_config.account
    if slurm_config.qos:
        extra_kwargs["slurm_qos"] = slurm_config.qos

    return SlurmTaskRunner(
        partition=slurm_config.partition,
        time_limit=slurm_config.time_limit,
        mem_gb=slurm_config.mem_gb,
        gpus_per_node=0,
        poll_interval=2.0,
        max_poll_time=slurm_config.max_wait + 300,
        log_folder=str(log_dir),
        **extra_kwargs,
    )


# ---------------------------------------------------------------------------
# Job cleanup
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def _session_slurm_job_ids():
    """Session-wide set of all SLURM job IDs submitted during tests."""
    return set()


@pytest.fixture
def slurm_jobs(_session_slurm_job_ids):
    """Track submitted SLURM job IDs for teardown cleanup."""
    job_ids: list[str] = []
    yield job_ids
    _session_slurm_job_ids.update(job_ids)
    if job_ids:
        subprocess.run(
            ["scancel", *job_ids],
            capture_output=True,
            timeout=10,
            check=False,
        )


def poll_for_task_runs(client, filter_fn, retries=15, delay=2):
    """Poll Prefect API until matching task runs appear."""
    for _ in range(retries):
        task_runs = client.read_task_runs()
        matching = [tr for tr in task_runs if filter_fn(tr)]
        if matching:
            return matching
        time.sleep(delay)
    return []


@pytest.fixture(scope="session", autouse=True)
def _session_job_cleanup(request, _session_slurm_job_ids):
    """Safety-net: scancel test jobs that weren't cleaned up per-test."""
    yield
    if not request.config.getoption("--run-slurm"):
        return
    if not _session_slurm_job_ids:
        return
    try:
        subprocess.run(
            ["scancel", *_session_slurm_job_ids],
            capture_output=True,
            timeout=10,
            check=False,
        )
    except Exception:
        pass
