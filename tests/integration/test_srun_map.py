"""srun mode map() integration tests.

Run inside an allocation::

    salloc -n4 --mem=4G pixi run -e dev pytest --run-slurm tests/integration/test_srun_map.py
"""

from __future__ import annotations

import os

import pytest
from prefect import flow

from prefect_submitit import ExecutionMode
from tests.integration.tasks import add, identity

pytestmark = pytest.mark.slurm

if not os.environ.get("SLURM_JOB_ID"):
    pytestmark = [
        pytest.mark.slurm,
        pytest.mark.skip(reason="srun tests require SLURM_JOB_ID (run inside salloc)"),
    ]


@pytest.fixture
def srun_runner(make_slurm_runner):
    """A SlurmTaskRunner configured for srun mode."""
    return make_slurm_runner(execution_mode=ExecutionMode.SRUN)


class TestSrunMap:
    """Non-batched map via srun."""

    def test_map_basic(self, srun_runner):
        @flow(task_runner=srun_runner)
        def compute():
            futures = identity.map([1, 2, 3, 4])
            return [f.result() for f in futures]

        assert compute() == [1, 2, 3, 4]

    def test_map_multi_param(self, srun_runner):
        @flow(task_runner=srun_runner)
        def compute():
            futures = add.map([1, 2, 3], [10, 20, 30])
            return [f.result() for f in futures]

        assert compute() == [11, 22, 33]


class TestSrunBatchedMap:
    """Batched map via srun (units_per_worker > 1)."""

    def test_batched_map(self, make_slurm_runner):
        runner = make_slurm_runner(
            execution_mode=ExecutionMode.SRUN, units_per_worker=2
        )

        @flow(task_runner=runner)
        def compute():
            futures = identity.map([10, 20, 30, 40])
            return [f.result() for f in futures]

        assert compute() == [10, 20, 30, 40]
