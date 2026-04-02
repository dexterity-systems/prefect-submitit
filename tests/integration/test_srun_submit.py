"""srun mode single task submission integration tests.

Run inside an allocation::

    salloc -n4 --mem=4G pixi run -e dev pytest --run-slurm tests/integration/test_srun_submit.py
"""

from __future__ import annotations

import os

import pytest
from prefect import flow

from prefect_submitit import ExecutionMode
from tests.integration.tasks import add, async_add, identity

pytestmark = pytest.mark.slurm

# Skip entire module if not inside a SLURM allocation
if not os.environ.get("SLURM_JOB_ID"):
    pytestmark = [
        pytest.mark.slurm,
        pytest.mark.skip(reason="srun tests require SLURM_JOB_ID (run inside salloc)"),
    ]


@pytest.fixture
def srun_runner(make_slurm_runner):
    """A SlurmTaskRunner configured for srun mode."""
    return make_slurm_runner(execution_mode=ExecutionMode.SRUN)


class TestSrunSingleTask:
    """Basic submit and result retrieval via srun."""

    def test_sync_task_roundtrip(self, srun_runner):
        @flow(task_runner=srun_runner)
        def compute():
            future = add.submit(1, 2)
            return future.result()

        assert compute() == 3

    def test_async_task_roundtrip(self, srun_runner):
        @flow(task_runner=srun_runner)
        def compute():
            future = async_add.submit(10, 20)
            return future.result()

        assert compute() == 30

    def test_identity_serialization(self, srun_runner):
        @flow(task_runner=srun_runner)
        def compute():
            future = identity.submit({"key": [1, 2, 3]})
            return future.result()

        assert compute() == {"key": [1, 2, 3]}

    def test_task_dependency_chain(self, srun_runner):
        @flow(task_runner=srun_runner)
        def compute():
            a = add.submit(1, 2)
            b = add.submit(a.result(), 10)
            return b.result()

        assert compute() == 13
