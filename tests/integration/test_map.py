"""Job array (map) integration tests."""

from __future__ import annotations

import pytest
from prefect import flow
from prefect.client.orchestration import get_client

from prefect_submitit.futures import SlurmJobFailed
from tests.integration.conftest import poll_for_task_runs
from tests.integration.tasks import add, conditional_fail, identity

pytestmark = pytest.mark.slurm


class TestMapBasic:
    """P0: Basic job array submission."""

    def test_map_small_array(self, slurm_runner, slurm_jobs):
        @flow(task_runner=slurm_runner)
        def compute():
            futures = identity.map(x=[1, 2, 3, 4, 5])
            for f in futures:
                slurm_jobs.append(f.slurm_job_id)
            return [f.result() for f in futures]

        results = compute()
        assert results == [1, 2, 3, 4, 5]


class TestMapParameters:
    """P1: Parameter handling in map."""

    def test_map_with_mixed_params(self, slurm_runner, slurm_jobs):
        @flow(task_runner=slurm_runner)
        def compute():
            futures = add.map(a=[1, 2, 3], b=[10, 20, 30])
            for f in futures:
                slurm_jobs.append(f.slurm_job_id)
            return [f.result() for f in futures]

        results = compute()
        assert results == [11, 22, 33]


class TestMapChunking:
    """P1: Job array chunking when exceeding max_array_size."""

    def test_map_exceeding_max_array_size(self, make_slurm_runner, slurm_jobs):
        runner = make_slurm_runner(max_array_size=3)

        @flow(task_runner=runner)
        def compute():
            futures = identity.map(x=[10, 20, 30, 40, 50, 60, 70])
            for f in futures:
                slurm_jobs.append(f.slurm_job_id)
            return [f.result() for f in futures]

        results = compute()
        assert results == [10, 20, 30, 40, 50, 60, 70]

    def test_map_with_parallelism_throttle(self, make_slurm_runner, slurm_jobs):
        runner = make_slurm_runner(slurm_array_parallelism=3)

        @flow(task_runner=runner)
        def compute():
            futures = identity.map(x=list(range(10)))
            for f in futures:
                slurm_jobs.append(f.slurm_job_id)
            return [f.result() for f in futures]

        results = compute()
        assert results == list(range(10))


class TestMapFailures:
    """P1: Per-task failure isolation in map."""

    def test_map_one_task_fails(self, slurm_runner, slurm_jobs):
        @flow(task_runner=slurm_runner)
        def compute():
            futures = conditional_fail.map(x=[1, 2, 3, 4, 5], fail_on=3)
            for f in futures:
                slurm_jobs.append(f.slurm_job_id)
            results = []
            for f in futures:
                try:
                    results.append(f.result())
                except SlurmJobFailed as e:
                    results.append(str(e))
            return results

        results = compute()
        assert results[0] == 10  # 1 * 10
        assert results[1] == 20  # 2 * 10
        assert "intentional failure on 3" in results[2]
        assert results[3] == 40  # 4 * 10
        assert results[4] == 50  # 5 * 10


class TestMapPrefectAPI:
    """P2: Map task visibility in Prefect API."""

    def test_map_array_task_names_in_api(
        self, slurm_runner, slurm_jobs, prefect_server
    ):
        if not prefect_server:
            pytest.skip("No Prefect server available")

        array_job_ids = set()

        @flow(task_runner=slurm_runner)
        def compute():
            futures = identity.map(x=[1, 2, 3])
            for f in futures:
                slurm_jobs.append(f.slurm_job_id)
                array_job_ids.add(f.array_job_id)
            return [f.result() for f in futures]

        compute()

        client = get_client(sync_client=True)
        for ajid in array_job_ids:
            matching = poll_for_task_runs(
                client,
                lambda tr, _ajid=ajid: tr.name and f"slurm-{_ajid}_" in tr.name,
            )
            assert len(matching) >= 1
