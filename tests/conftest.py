"""Shared test fixtures for prefect-submitit."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from prefect_submitit.constants import ExecutionMode


def pytest_addoption(parser):
    parser.addoption(
        "--run-slurm",
        action="store_true",
        default=False,
        help="Run tests that require a real SLURM cluster",
    )


@pytest.fixture
def mock_runner():
    """Create a minimal mock SlurmTaskRunner for testing standalone functions.

    Attributes mirror those accessed by utility / submission functions.
    """
    runner = MagicMock()
    runner.units_per_worker = 3
    runner.max_array_size = None
    runner._cached_max_array_size = None
    runner.execution_mode = ExecutionMode.SLURM
    runner.logger = MagicMock()
    runner.poll_interval = 5.0
    runner.max_poll_time = None
    runner.timeout_min = 60
    runner.fail_on_error = True
    return runner
