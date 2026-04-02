"""Tests for prefect_submitit.futures.base.

Tests SlurmJobFailed exception and SlurmPrefectFuture base class.
"""

from __future__ import annotations

from subprocess import CalledProcessError
from unittest.mock import MagicMock, patch
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
    job.get_info.return_value = {"State": state}
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

    def test_state_completing(self):
        future = SlurmPrefectFuture(_mock_job(state="COMPLETING"), uuid4(), 1.0, 60.0)

        assert future.state.is_completed()

    def test_state_completed_when_done(self):
        future = SlurmPrefectFuture(_mock_job(state="RUNNING"), uuid4(), 1.0, 60.0)
        future._done = True

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

    # --- Failed Prefect state ---

    def test_result_failed_state_raises_slurm_job_failed(self):
        from prefect.states import Failed

        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        exc = ValueError("bad input")
        state = Failed(data=exc)
        job.result.return_value = cloudpickle.dumps(state)
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        with pytest.raises(SlurmJobFailed, match="bad input"):
            future.result()

    def test_result_failed_state_no_raise(self):
        from prefect.states import Failed

        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        state = Failed(data=ValueError("bad input"))
        job.result.return_value = cloudpickle.dumps(state)
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        assert future.result(raise_on_failure=False) is None

    def test_result_failed_state_chains_original_exception(self):
        from prefect.states import Failed

        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        exc = ValueError("bad input")
        state = Failed(data=exc)
        job.result.return_value = cloudpickle.dumps(state)
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        with pytest.raises(SlurmJobFailed) as exc_info:
            future.result()
        assert isinstance(exc_info.value.__cause__, ValueError)

    def test_result_failed_state_includes_job_id(self):
        from prefect.states import Failed

        job = _mock_job(job_id="99999", state="COMPLETED")
        job.done.return_value = True
        state = Failed(data=ValueError("bad input"))
        job.result.return_value = cloudpickle.dumps(state)
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        with pytest.raises(SlurmJobFailed, match="Job 99999"):
            future.result()

    # --- Callbacks ---

    def test_callbacks_fired_on_completion(self):
        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        callback_called = []
        future.add_done_callback(callback_called.append)
        future.wait()

        assert len(callback_called) == 1
        assert callback_called[0] is future

    def test_callbacks_fired_immediately_if_done(self):
        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)
        future.wait()

        callback_called = []
        future.add_done_callback(callback_called.append)

        assert len(callback_called) == 1

    def test_fire_callbacks_handles_exception(self):
        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        good_calls = []
        future.add_done_callback(lambda f: (_ for _ in ()).throw(RuntimeError("boom")))
        future.add_done_callback(good_calls.append)

        future.wait()
        assert len(good_calls) == 1

    # --- Logs ---

    def test_logs_returns_stdout_stderr(self, tmp_path):
        job = _mock_job()
        stdout_path = tmp_path / "stdout.txt"
        stderr_path = tmp_path / "stderr.txt"
        stdout_path.write_text("stdout content")
        stderr_path.write_text("stderr content")
        job.paths.stdout = stdout_path
        job.paths.stderr = stderr_path
        future = SlurmPrefectFuture(job, uuid4(), 1.0, 60.0)

        stdout, stderr = future.logs()

        assert stdout == "stdout content"
        assert stderr == "stderr content"

    def test_logs_returns_empty_for_missing_files(self, tmp_path):
        """logs() returns empty strings when log files don't exist."""
        job = _mock_job()
        job.paths.stdout = tmp_path / "nonexistent_stdout.txt"
        job.paths.stderr = tmp_path / "nonexistent_stderr.txt"
        future = SlurmPrefectFuture(job, uuid4(), 1.0, 60.0)

        stdout, stderr = future.logs(_retries=1)

        assert stdout == ""
        assert stderr == ""

    # --- Post-loop state checks ---

    @pytest.mark.parametrize(
        "slurm_state",
        ["CANCELLED", "FAILED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY"],
    )
    def test_wait_post_loop_terminal_failure(self, slurm_state):
        """Job.done() returns True but state is a terminal failure."""
        job = _mock_job(state=slurm_state)
        job.done.return_value = True
        job.stderr.return_value = "some error"
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        with pytest.raises(SlurmJobFailed, match=slurm_state):
            future.wait()

    def test_wait_post_loop_completed(self):
        """Job.done() returns True and state is COMPLETED — no exception."""
        job = _mock_job(state="COMPLETED")
        job.done.return_value = True
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        future.wait()

        assert future.is_done is True

    # --- Cancel ---

    @patch("prefect_submitit.futures.base.subprocess.run")
    def test_cancel_success(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        future = SlurmPrefectFuture(_mock_job("99999"), uuid4(), 1.0, 60.0)

        assert future.cancel() is True
        mock_run.assert_called_once_with(
            ["scancel", "99999"], check=True, capture_output=True
        )

    @patch("prefect_submitit.futures.base.subprocess.run")
    def test_cancel_failure(self, mock_run):
        mock_run.side_effect = CalledProcessError(1, "scancel")
        future = SlurmPrefectFuture(_mock_job("99999"), uuid4(), 1.0, 60.0)

        assert future.cancel() is False

    # --- CANCELLED by <uid> variant ---

    def test_cancelled_by_uid_state(self):
        job = _mock_job(state="CANCELLED by 1000")
        future = SlurmPrefectFuture(job, uuid4(), 1.0, 60.0)

        assert future.state.is_failed()

    def test_cancelled_by_uid_wait(self):
        job = _mock_job(state="CANCELLED by 1000")
        job.done.return_value = False
        job.stderr.return_value = "cancelled"
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 60.0)

        with pytest.raises(SlurmJobFailed, match="CANCELLED by 1000"):
            future.wait()

    # --- Plus suffix handling ---

    def test_timeout_plus_suffix(self):
        job = _mock_job(state="TIMEOUT+")
        future = SlurmPrefectFuture(job, uuid4(), 1.0, 60.0)

        assert future.state.is_failed()

    def test_cancelled_plus_suffix(self):
        job = _mock_job(state="CANCELLED+")
        future = SlurmPrefectFuture(job, uuid4(), 1.0, 60.0)

        assert future.state.is_failed()

    # --- Timeout message includes state ---

    def test_timeout_message_includes_state(self):
        job = _mock_job(state="RUNNING")
        job.done.return_value = False
        future = SlurmPrefectFuture(job, uuid4(), 0.01, 0.05)

        with pytest.raises(TimeoutError, match="last observed state:"):
            future.wait(timeout=0.02)
