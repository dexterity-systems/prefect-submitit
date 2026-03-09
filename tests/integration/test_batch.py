"""Batched execution integration tests."""

from __future__ import annotations

import pytest
from prefect import flow
from prefect.client.orchestration import get_client

from tests.integration.tasks import conditional_fail, identity

pytestmark = pytest.mark.slurm


class TestBatchBasic:
    """P1: Basic batch execution."""

    def test_batch_basic(self, make_slurm_runner, slurm_jobs):
        """10 items, units_per_worker=5 -> 2 SLURM jobs."""
        runner = make_slurm_runner(units_per_worker=5)

        @flow(task_runner=runner)
        def compute():
            futures = identity.map(x=list(range(10)))
            for f in futures:
                slurm_jobs.append(f.slurm_job_future.slurm_job_id)
            return [f.result() for f in futures]

        results = compute()
        assert results == list(range(10))

    def test_batch_with_remainder(self, make_slurm_runner, slurm_jobs):
        """7 items, units_per_worker=3 -> batches of [3, 3, 1]."""
        runner = make_slurm_runner(units_per_worker=3)

        @flow(task_runner=runner)
        def compute():
            futures = identity.map(x=list(range(7)))
            for f in futures:
                slurm_jobs.append(f.slurm_job_future.slurm_job_id)
            return [f.result() for f in futures]

        results = compute()
        assert results == list(range(7))


class TestBatchFailures:
    """P1: Per-item failure isolation in batch."""

    def test_batch_per_item_failure(self, make_slurm_runner, slurm_jobs):
        """Item that raises -> error dict; other items succeed."""
        runner = make_slurm_runner(units_per_worker=5)

        @flow(task_runner=runner)
        def compute():
            futures = conditional_fail.map(x=[0, 1, 2, 3, 4], fail_on=2)
            for f in futures:
                slurm_jobs.append(f.slurm_job_future.slurm_job_id)
            return [f.result() for f in futures]

        results = compute()
        assert results[0] == 0  # 0 * 10
        assert results[1] == 10  # 1 * 10
        # Item 2 failed: batch executor wraps in error dict
        assert isinstance(results[2], dict)
        assert results[2]["success"] is False
        assert "intentional failure on 2" in results[2]["error"]
        assert results[3] == 30  # 3 * 10
        assert results[4] == 40  # 4 * 10


class TestBatchParameters:
    """P2: Static parameter merging in batch."""

    def test_batch_with_static_params(self, make_slurm_runner, slurm_jobs):
        runner = make_slurm_runner(units_per_worker=3)

        @flow(task_runner=runner)
        def compute():
            # conditional_fail has a static fail_on param
            futures = conditional_fail.map(x=[1, 2, 3, 4, 5], fail_on=-1)
            for f in futures:
                slurm_jobs.append(f.slurm_job_future.slurm_job_id)
            return [f.result() for f in futures]

        results = compute()
        assert results == [10, 20, 30, 40, 50]


class TestBatchPrefectAPI:
    """P2: Batch task run state in Prefect API."""

    def test_batch_task_run_state(self, make_slurm_runner, slurm_jobs, prefect_server):
        if not prefect_server:
            pytest.skip("No Prefect server available")

        runner = make_slurm_runner(units_per_worker=5)

        @flow(task_runner=runner)
        def compute():
            from prefect.context import FlowRunContext

            flow_run_id = str(FlowRunContext.get().flow_run.id)
            futures = identity.map(x=list(range(5)))
            for f in futures:
                slurm_jobs.append(f.slurm_job_future.slurm_job_id)
            results = [f.result() for f in futures]
            return flow_run_id, results

        flow_run_id, results = compute()
        assert results == list(range(5))

        client = get_client(sync_client=True)
        task_runs = client.read_task_runs()
        matching = [tr for tr in task_runs if str(tr.flow_run_id) == flow_run_id]
        assert len(matching) >= 1
        assert any(tr.state.is_completed() for tr in matching)
