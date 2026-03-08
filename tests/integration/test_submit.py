"""Single task submission integration tests."""

from __future__ import annotations

import pytest
from prefect import flow
from prefect.client.orchestration import get_client

from tests.integration.tasks import (
    add,
    async_add,
    receive_complex,
    return_complex,
)

pytestmark = pytest.mark.slurm


class TestSingleTaskSubmission:
    """P0: Basic submission and result retrieval."""

    def test_sync_task_roundtrip(self, slurm_runner, slurm_jobs):
        @flow(task_runner=slurm_runner)
        def compute():
            future = add.submit(1, 2)
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        assert compute() == 3

    def test_async_task_roundtrip(self, slurm_runner, slurm_jobs):
        @flow(task_runner=slurm_runner)
        def compute():
            future = async_add.submit(10, 20)
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        assert compute() == 30


class TestTaskDependencies:
    """P1: Task dependency chains."""

    def test_task_with_dependency(self, slurm_runner, slurm_jobs):
        @flow(task_runner=slurm_runner)
        def compute():
            future_a = add.submit(1, 2)
            slurm_jobs.append(future_a.slurm_job_id)
            future_b = add.submit(future_a.result(), 10)
            slurm_jobs.append(future_b.slurm_job_id)
            return future_b.result()

        assert compute() == 13


class TestPrefectAPIIntegration:
    """P2: Prefect API visibility."""

    @pytest.mark.xfail(
        reason="Prefect EventsWorker websocket auth fails from compute nodes; "
        "task run events don't reach the ephemeral server",
        strict=False,
    )
    def test_task_run_in_prefect_api(self, slurm_runner, slurm_jobs, prefect_server):
        if not prefect_server:
            pytest.skip("No Prefect server available")

        flow_run_id_holder = {}

        @flow(task_runner=slurm_runner)
        def compute():
            from prefect.context import FlowRunContext

            ctx = FlowRunContext.get()
            flow_run_id_holder["id"] = str(ctx.flow_run.id)
            future = add.submit(5, 5)
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        result = compute()
        assert result == 10

        client = get_client(sync_client=True)
        task_runs = client.read_task_runs()
        matching = [
            tr
            for tr in task_runs
            if str(tr.flow_run_id) == flow_run_id_holder["id"]
        ]
        assert len(matching) >= 1
        assert any(tr.state.is_completed() for tr in matching)

    @pytest.mark.xfail(
        reason="Prefect EventsWorker websocket auth fails from compute nodes; "
        "task run events don't reach the ephemeral server",
        strict=False,
    )
    def test_task_run_name_contains_slurm_job_id(
        self, slurm_runner, slurm_jobs, prefect_server
    ):
        if not prefect_server:
            pytest.skip("No Prefect server available")

        job_id_holder = {}

        @flow(task_runner=slurm_runner)
        def compute():
            future = add.submit(1, 1)
            job_id_holder["id"] = future.slurm_job_id
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        compute()

        client = get_client(sync_client=True)
        task_runs = client.read_task_runs()
        slurm_job_id = job_id_holder["id"]
        matching = [
            tr for tr in task_runs if tr.name and f"slurm-{slurm_job_id}" in tr.name
        ]
        assert len(matching) >= 1


class TestSerialization:
    """P2: Complex parameter and return value serialization."""

    def test_complex_return_value(self, slurm_runner, slurm_jobs):
        @flow(task_runner=slurm_runner)
        def compute():
            future = return_complex.submit()
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        result = compute()
        assert result["string"] == "hello"
        assert result["number"] == 42
        assert result["nested"]["a"] == [1, 2, 3]
        assert result["none"] is None

    def test_complex_input_parameters(self, slurm_runner, slurm_jobs):
        data = {
            "key1": [1, 2, 3],
            "key2": {"nested": True},
            "key3": "value",
        }

        @flow(task_runner=slurm_runner)
        def compute():
            future = receive_complex.submit(data=data)
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        result = compute()
        assert "keys=" in result
        assert "len=3" in result
