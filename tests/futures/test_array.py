"""Tests for prefect_submitit.futures.array.

Tests SlurmArrayPrefectFuture.
"""

from __future__ import annotations

from subprocess import CalledProcessError
from unittest.mock import MagicMock, patch
from uuid import uuid4

import cloudpickle

from prefect_submitit.futures.array import SlurmArrayPrefectFuture


def _mock_job(job_id: str = "12345_0", state: str = "PENDING") -> MagicMock:
    """Create a mock submitit Job."""
    job = MagicMock()
    job.job_id = job_id
    job.state = state
    job.get_info.return_value = {"State": state}
    job.done.return_value = state == "COMPLETED"
    return job


class TestSlurmArrayPrefectFuture:
    """Tests for SlurmArrayPrefectFuture."""

    def _make_future(
        self,
        job_id: str = "12345_2",
        state: str = "PENDING",
        array_job_id: str = "12345",
        array_task_index: int = 2,
        array_size: int = 10,
        fail_on_error: bool = True,
    ) -> SlurmArrayPrefectFuture:
        return SlurmArrayPrefectFuture(
            job=_mock_job(job_id, state),
            task_run_id=uuid4(),
            poll_interval=1.0,
            max_poll_time=60.0,
            array_job_id=array_job_id,
            array_task_index=array_task_index,
            array_size=array_size,
            fail_on_error=fail_on_error,
        )

    # --- Properties ---

    def test_array_job_id(self):
        future = self._make_future(array_job_id="99999")
        assert future.array_job_id == "99999"

    def test_array_task_index(self):
        future = self._make_future(array_task_index=5)
        assert future.array_task_index == 5

    def test_array_size(self):
        future = self._make_future(array_size=100)
        assert future.array_size == 100

    def test_slurm_job_id_combines_array_and_task(self):
        future = self._make_future(array_job_id="12345", array_task_index=2)
        assert future.slurm_job_id == "12345_2"

    # --- Repr ---

    def test_repr_contains_key_info(self):
        future = self._make_future(
            job_id="12345_5",
            state="RUNNING",
            array_job_id="12345",
            array_task_index=5,
            array_size=100,
        )

        repr_str = repr(future)
        assert "12345_5" in repr_str
        assert "5/100" in repr_str
        assert "RUNNING" in repr_str

    # --- Cancel ---

    @patch("subprocess.run")
    def test_cancel_cancels_entire_array(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        future = self._make_future(array_job_id="12345", array_task_index=3)

        assert future.cancel() is True
        mock_run.assert_called_once_with(
            ["scancel", "12345"], check=True, capture_output=True
        )

    @patch("subprocess.run")
    def test_cancel_returns_false_on_failure(self, mock_run):
        mock_run.side_effect = CalledProcessError(1, "scancel")
        future = self._make_future()

        assert future.cancel() is False

    @patch("subprocess.run")
    def test_cancel_task_cancels_single_task(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        future = self._make_future(array_job_id="12345", array_task_index=7)

        assert future.cancel_task() is True
        mock_run.assert_called_once_with(
            ["scancel", "12345_7"], check=True, capture_output=True
        )

    @patch("subprocess.run")
    def test_cancel_task_returns_false_on_failure(self, mock_run):
        mock_run.side_effect = CalledProcessError(1, "scancel")
        future = self._make_future()

        assert future.cancel_task() is False

    # --- Result ---

    def test_result_uses_fail_on_error_default(self):
        from prefect.states import Completed

        job = _mock_job("12345_0", state="COMPLETED")
        job.done.return_value = True
        state = Completed(data=42)
        job.result.return_value = cloudpickle.dumps(state)

        future = SlurmArrayPrefectFuture(
            job=job,
            task_run_id=uuid4(),
            poll_interval=0.01,
            max_poll_time=60.0,
            array_job_id="12345",
            array_task_index=0,
            array_size=10,
            fail_on_error=True,
        )

        assert future.result() == 42

    def test_result_respects_raise_on_failure_override(self):
        from prefect.states import Completed

        job = _mock_job("12345_0", state="COMPLETED")
        job.done.return_value = True
        state = Completed(data="success")
        job.result.return_value = cloudpickle.dumps(state)

        future = SlurmArrayPrefectFuture(
            job=job,
            task_run_id=uuid4(),
            poll_interval=0.01,
            max_poll_time=60.0,
            array_job_id="12345",
            array_task_index=0,
            array_size=10,
            fail_on_error=True,
        )

        result = future.result(raise_on_failure=False)
        assert result == "success"

    def test_result_none_override_uses_fail_on_error(self):
        """When raise_on_failure=None, should default to _fail_on_error."""
        from prefect.states import Completed

        job = _mock_job("12345_0", state="COMPLETED")
        job.done.return_value = True
        state = Completed(data=99)
        job.result.return_value = cloudpickle.dumps(state)

        future = SlurmArrayPrefectFuture(
            job=job,
            task_run_id=uuid4(),
            poll_interval=0.01,
            max_poll_time=60.0,
            array_job_id="12345",
            array_task_index=0,
            array_size=10,
            fail_on_error=False,
        )

        result = future.result(raise_on_failure=None)
        assert result == 99
