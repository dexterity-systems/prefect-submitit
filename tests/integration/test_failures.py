"""SLURM failure mode integration tests."""

from __future__ import annotations

import subprocess

import pytest
from prefect import flow

from prefect_submitit.futures import SlurmJobFailed
from tests.integration.helpers import wait_for_running
from tests.integration.tasks import fail_with, sleep_and_return

pytestmark = pytest.mark.slurm


class TestExceptionPropagation:
    """P0: Exception propagation through SLURM."""

    def test_task_exception_propagation(self, slurm_runner, slurm_jobs):
        @flow(task_runner=slurm_runner)
        def compute():
            future = fail_with.submit(error_type="ValueError", message="test error")
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        with pytest.raises(SlurmJobFailed, match="test error"):
            compute()


class TestSlurmFailureModes:
    """P1-P2: SLURM-level failure detection."""

    def test_invalid_partition_fails_cleanly(self, make_slurm_runner, slurm_jobs):
        runner = make_slurm_runner(
            partition="nonexistent_partition_xyz",
            mem_gb=1,
            max_poll_time=60,
        )

        @flow(task_runner=runner)
        def compute():
            from tests.integration.tasks import add

            future = add.submit(1, 2)
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        with pytest.raises((SlurmJobFailed, RuntimeError, TimeoutError)):
            compute()

    def test_job_no_output_raises_slurm_job_failed(self, slurm_runner, slurm_jobs):
        """SIGKILL prevents result writing -> SlurmJobFailed."""

        @flow(task_runner=slurm_runner)
        def compute():
            future = sleep_and_return.submit(seconds=120.0)
            slurm_jobs.append(future.slurm_job_id)
            wait_for_running(future, timeout=300)
            # SIGKILL: no chance to write results
            subprocess.run(
                ["scancel", "--signal=SIGKILL", future.slurm_job_id],
                check=True,
                capture_output=True,
            )
            return future.result()

        with pytest.raises(SlurmJobFailed):
            compute()
