"""Polling and timing integration tests."""

from __future__ import annotations

import pytest
from prefect import flow

from prefect_submitit import SlurmTaskRunner
from tests.integration.tasks import add, sleep_and_return

pytestmark = pytest.mark.slurm


class TestPolling:
    """P2: Polling behavior."""

    def test_rapid_completion(self, slurm_runner, slurm_jobs):
        """Job finishes before first poll — no errors."""

        @flow(task_runner=slurm_runner)
        def compute():
            future = add.submit(21, 21)
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        assert compute() == 42

    def test_max_poll_time_fires(self, slurm_config, slurm_jobs):
        """max_poll_time triggers TimeoutError.

        Current behavior (Known Issue 1): wait() raises TimeoutError
        rather than returning silently as PrefectFuture protocol specifies.
        """
        extra_kwargs = {}
        if slurm_config.account:
            extra_kwargs["slurm_account"] = slurm_config.account
        if slurm_config.qos:
            extra_kwargs["slurm_qos"] = slurm_config.qos

        runner = SlurmTaskRunner(
            partition=slurm_config.partition,
            time_limit=slurm_config.time_limit,
            mem_gb=slurm_config.mem_gb,
            gpus_per_node=0,
            poll_interval=2.0,
            max_poll_time=10,
            log_folder=str(slurm_config.log_dir / "slurm_logs"),
            **extra_kwargs,
        )

        @flow(task_runner=runner)
        def compute():
            future = sleep_and_return.submit(seconds=120.0)
            slurm_jobs.append(future.slurm_job_id)
            future.wait()

        with pytest.raises(TimeoutError):
            compute()

    def test_wait_timeout_overrides_max_poll(self, slurm_config, slurm_jobs):
        """Explicit timeout param takes precedence over max_poll_time."""
        extra_kwargs = {}
        if slurm_config.account:
            extra_kwargs["slurm_account"] = slurm_config.account
        if slurm_config.qos:
            extra_kwargs["slurm_qos"] = slurm_config.qos

        runner = SlurmTaskRunner(
            partition=slurm_config.partition,
            time_limit=slurm_config.time_limit,
            mem_gb=slurm_config.mem_gb,
            gpus_per_node=0,
            poll_interval=2.0,
            max_poll_time=600,
            log_folder=str(slurm_config.log_dir / "slurm_logs"),
            **extra_kwargs,
        )

        @flow(task_runner=runner)
        def compute():
            future = sleep_and_return.submit(seconds=120.0)
            slurm_jobs.append(future.slurm_job_id)
            future.wait(timeout=10)

        with pytest.raises(TimeoutError):
            compute()

    def test_wait_timeout_zero(self, slurm_config, slurm_jobs):
        """wait(timeout=0) should raise TimeoutError immediately."""
        extra_kwargs = {}
        if slurm_config.account:
            extra_kwargs["slurm_account"] = slurm_config.account
        if slurm_config.qos:
            extra_kwargs["slurm_qos"] = slurm_config.qos

        runner = SlurmTaskRunner(
            partition=slurm_config.partition,
            time_limit=slurm_config.time_limit,
            mem_gb=slurm_config.mem_gb,
            gpus_per_node=0,
            poll_interval=2.0,
            max_poll_time=600,
            log_folder=str(slurm_config.log_dir / "slurm_logs"),
            **extra_kwargs,
        )

        @flow(task_runner=runner)
        def compute():
            future = sleep_and_return.submit(seconds=120.0)
            slurm_jobs.append(future.slurm_job_id)
            # timeout=0 should raise TimeoutError immediately
            future.wait(timeout=0)

        with pytest.raises(TimeoutError):
            compute()
