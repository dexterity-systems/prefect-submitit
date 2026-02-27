"""Tests for prefect_submitit.futures.base.

Tests SlurmJobFailed exception and SlurmPrefectFuture base class.
"""

from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

import cloudpickle
import pytest
from submitit.core.utils import FailedJobError, UncompletedJobError

from prefect_submitit.futures.base import SlurmJobFailed, SlurmPrefectFuture


def _mock_job(job_id: str = "12345", state: str = "PENDING") -> MagicMock:
    """Create a mock submitit Job."""
    job = MagicMock()
    job.job_id = job_id
    job.state = state
    job.done.return_value = state == "COMPLETED"
    return job


class TestSlurmJobFailed:
    """Tests for SlurmJobFailed exception."""

    def test_is_exception(self):
        assert issubclass(SlurmJobFailed, Exception)

    def test_message(self):
        exc = SlurmJobFailed("Job 123 failed")
        assert str(exc) == "Job 123 failed"


class TestSlurmPrefectFuture:
    """Tests for SlurmPrefectFuture."""

    # --- Properties ---

    def test_task_run_id(self):
        task_run_id = uuid4()
        future = SlurmPrefectFuture(_mock_job(), task_run_id, 1.0, 60.0)

        assert future.task_run_id == task_run_id

    def test_slurm_job_id(self):
        future = SlurmPrefectFuture(_mock_job("67890"), uuid4(), 1.0, 60.0)

        assert future.slurm_job_id == "67890"

    def test_is_done_initially_false(self):
        future = SlurmPrefectFuture(_mock_job(), uuid4(), 1.0, 60.0)

        assert future.is_done is False

    # --- State mapping ---

    def test_state_pending(self):
        future = SlurmPrefectFuture(_mock_job(state="PENDING"), uuid4(), 1.0, 60.0)

        assert future.state.is_pending()

    def test_state_running(self):
        future = SlurmPrefectFuture(_mock_job(state="RUNNING"), uuid4(), 1.0, 60.0)

        assert future.state.is_running()

    def test_state_completed(self):
        future = SlurmPrefectFuture(_mock_job(state="COMPLETED"), uuid4(), 1.0, 60.0)

        assert future.state.is_completed()

    @pytest.mark.parametrize(
        "slurm_state",
        ["FAILED", "NODE_FAIL", "TIMEOUT", "CANCELLED", "OUT_OF_MEMORY"],
    )
    def test_state_failed(self, slurm_state):
        future = SlurmPrefectFuture(_mock_job(state=slurm_state), uuid4(), 1.0, 60.0)

        state = future.state
        assert state.is_failed()
        assert slurm_state in state.message

    def test_state_unknown_treated_as_pending(self):
        future = SlurmPrefectFuture(_mock_job(state="CONFIGURING"), uuid4(), 1.0, 60.0)

        assert future.state.is_pending()

    def test_terminal_failure_states_is_frozenset(self):
        assert isinstance(SlurmPrefectFuture.TERMINAL_FAILURE_STATES, frozenset)
        assert len(SlurmPrefectFuture.TERMINAL_FAILURE_STATES) == 5

    # --- Wait ---

    def test_wait_completes(self):
        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        future.wait()

        assert future.is_done is True

    def test_wait_timeout(self):
        job = _mock_job(state="RUNNING")
        job.done.return_value = False
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 0.05)

        with pytest.raises(TimeoutError, match="did not complete"):
            future.wait(timeout=0.02)

    def test_wait_failure_state_raises(self):
        job = _mock_job(state="NODE_FAIL")
        job.done.return_value = False
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        with pytest.raises(SlurmJobFailed, match="NODE_FAIL"):
            future.wait()

    def test_wait_failure_state_stderr_unavailable(self):
        job = _mock_job(state="FAILED")
        job.done.return_value = False
        job.stderr.side_effect = Exception("no stderr")
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        with pytest.raises(SlurmJobFailed, match="stderr unavailable"):
            future.wait()

    # --- Result ---

    def test_result_from_pickled_state(self):
        from prefect.states import Completed

        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        state = Completed(data=42)
        job.result.return_value = cloudpickle.dumps(state)
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        assert future.result() == 42

    def test_result_raw_value_if_not_state(self):
        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        job.result.return_value = cloudpickle.dumps(42)
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        assert future.result() == 42

    def test_result_cached(self):
        from prefect.states import Completed

        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        state = Completed(data=42)
        job.result.return_value = cloudpickle.dumps(state)
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        result1 = future.result()
        result2 = future.result()

        assert result1 == result2
        job.result.assert_called_once()

    def test_result_failed_job_raises(self):
        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        job.result.side_effect = FailedJobError("job failed")
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        with pytest.raises(SlurmJobFailed, match="failed"):
            future.result(raise_on_failure=True)

    def test_result_failed_job_returns_none(self):
        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        job.result.side_effect = FailedJobError("job failed")
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        assert future.result(raise_on_failure=False) is None

    def test_result_uncompleted_job_raises(self):
        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        job.result.side_effect = UncompletedJobError("no output")
        job.stderr.return_value = "OOM killed"
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        with pytest.raises(SlurmJobFailed, match="no output"):
            future.result(raise_on_failure=True)

    def test_result_uncompleted_job_returns_none(self):
        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        job.result.side_effect = UncompletedJobError("no output")
        job.stderr.return_value = "OOM killed"
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        assert future.result(raise_on_failure=False) is None

    # --- Callbacks ---

    def test_callbacks_fired_on_completion(self):
        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        callback_called = []
        future.add_done_callback(lambda f: callback_called.append(f))
        future.wait()

        assert len(callback_called) == 1
        assert callback_called[0] is future

    def test_callbacks_fired_immediately_if_done(self):
        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)
        future.wait()

        callback_called = []
        future.add_done_callback(lambda f: callback_called.append(f))

        assert len(callback_called) == 1

    def test_fire_callbacks_handles_exception(self):
        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        good_calls = []
        future.add_done_callback(lambda f: (_ for _ in ()).throw(RuntimeError("boom")))
        future.add_done_callback(lambda f: good_calls.append(f))

        future.wait()
        assert len(good_calls) == 1

    # --- Logs ---

    def test_logs_returns_stdout_stderr(self):
        job = _mock_job()
        job.stdout.return_value = "stdout content"
        job.stderr.return_value = "stderr content"
        future = SlurmPrefectFuture(job, uuid4(), 1.0, 60.0)

        stdout, stderr = future.logs()

        assert stdout == "stdout content"
        assert stderr == "stderr content"

    def test_logs_returns_value_for_none_stdout(self):
        """logs() may return None or '' when stdout/stderr are None."""
        job = _mock_job()
        job.stdout.return_value = None
        job.stderr.return_value = None
        future = SlurmPrefectFuture(job, uuid4(), 1.0, 60.0)

        stdout, stderr = future.logs()

        assert stdout is None or stdout == ""
        assert stderr is None or stderr == ""
