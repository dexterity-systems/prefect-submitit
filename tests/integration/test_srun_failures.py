"""srun mode failure handling integration tests.

Run inside an allocation::

    salloc -n4 --mem=4G pixi run -e dev pytest --run-slurm tests/integration/test_srun_failures.py
"""

from __future__ import annotations

import os

import pytest
from prefect import flow

from prefect_submitit import ExecutionMode
from prefect_submitit.futures.base import SlurmJobFailed
from tests.integration.tasks import conditional_fail, fail_with

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


class TestSrunFailures:
    """Task error propagation in srun mode."""

    def test_exception_propagates(self, srun_runner):
        @flow(task_runner=srun_runner)
        def compute():
            future = fail_with.submit("ValueError", "bad input")
            return future.result()

        with pytest.raises(SlurmJobFailed, match="bad input"):
            compute()

    def test_exception_no_raise(self, srun_runner):
        @flow(task_runner=srun_runner)
        def compute():
            future = fail_with.submit("ValueError", "bad input")
            return future.result(raise_on_failure=False)

        assert compute() is None

    def test_partial_map_failure(self, srun_runner):
        """One task in a map fails, others succeed."""

        @flow(task_runner=srun_runner)
        def compute():
            futures = conditional_fail.map([1, 2, 3], fail_on=2)
            results = []
            for f in futures:
                try:
                    results.append(f.result())
                except SlurmJobFailed:
                    results.append("FAILED")
            return results

        result = compute()
        assert result[0] == 10  # 1 * 10
        assert result[1] == "FAILED"  # x == fail_on
        assert result[2] == 30  # 3 * 10
