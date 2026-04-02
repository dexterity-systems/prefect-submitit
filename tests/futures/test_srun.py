"""Tests for prefect_submitit.futures.srun.SrunPrefectFuture."""

from __future__ import annotations

import pickle
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from prefect_submitit.futures.base import SlurmJobFailed
from prefect_submitit.futures.srun import SrunPrefectFuture


def _make_future(
    tmp_path: Path,
    returncode: int | None = None,
    *,
    step_index: int = 0,
    slurm_job_id: str = "123",
    poll_interval: float = 0.01,
    max_poll_time: float = 5.0,
) -> SrunPrefectFuture:
    """Create a future with a mock process."""
    proc = MagicMock()
    proc.pid = 12345
    proc.returncode = returncode
    proc.poll.return_value = returncode
    proc.stderr = None
    return SrunPrefectFuture(
        process=proc,
        job_folder=tmp_path,
        task_run_id=uuid4(),
        step_index=step_index,
        slurm_job_id=slurm_job_id,
        poll_interval=poll_interval,
        max_poll_time=max_poll_time,
    )


class TestProperties:
    """Test basic property accessors."""

    def test_task_run_id(self, tmp_path):
        f = _make_future(tmp_path)
        assert f.task_run_id is not None

    def test_slurm_job_id(self, tmp_path):
        f = _make_future(tmp_path, slurm_job_id="456", step_index=3)
        assert f.slurm_job_id == "456.3"

    def test_array_task_index(self, tmp_path):
        f = _make_future(tmp_path, step_index=7)
        assert f.array_task_index == 7

    def test_is_done_initially_false(self, tmp_path):
        f = _make_future(tmp_path, returncode=None)
        assert f.is_done is False


class TestState:
    """Test state property mapping."""

    def test_running(self, tmp_path):
        f = _make_future(tmp_path, returncode=None)
        assert f.state.is_running()

    def test_completed(self, tmp_path):
        f = _make_future(tmp_path, returncode=0)
        f._done = True
        assert f.state.is_completed()

    def test_failed(self, tmp_path):
        f = _make_future(tmp_path, returncode=1)
        f._done = True
        assert f.state.is_failed()


class TestWait:
    """Test wait() behavior."""

    def test_wait_success(self, tmp_path):
        f = _make_future(tmp_path, returncode=0)
        f.wait()
        assert f.is_done is True

    def test_wait_nonzero_raises(self, tmp_path):
        f = _make_future(tmp_path, returncode=1)
        with pytest.raises(SlurmJobFailed, match="exited with code 1"):
            f.wait()

    def test_wait_signal_137_mentions_sigkill(self, tmp_path):
        f = _make_future(tmp_path, returncode=137)
        with pytest.raises(SlurmJobFailed, match="SIGKILL"):
            f.wait()

    def test_wait_signal_143_mentions_sigterm(self, tmp_path):
        f = _make_future(tmp_path, returncode=143)
        with pytest.raises(SlurmJobFailed, match="SIGTERM"):
            f.wait()

    def test_wait_timeout(self, tmp_path):
        f = _make_future(tmp_path, returncode=None, max_poll_time=0.05)
        with pytest.raises(TimeoutError, match="did not complete"):
            f.wait()

    def test_wait_already_done(self, tmp_path):
        f = _make_future(tmp_path, returncode=0)
        f.wait()
        f.wait()  # second call is a no-op


class TestResult:
    """Test result() behavior."""

    def test_result_ok(self, tmp_path):
        f = _make_future(tmp_path, returncode=0)
        envelope = {"status": "ok", "result": 42}
        with open(tmp_path / "result.pkl", "wb") as fp:
            pickle.dump(envelope, fp)
        assert f.result() == 42

    def test_result_cached(self, tmp_path):
        f = _make_future(tmp_path, returncode=0)
        envelope = {"status": "ok", "result": "cached"}
        with open(tmp_path / "result.pkl", "wb") as fp:
            pickle.dump(envelope, fp)
        assert f.result() == "cached"
        assert f.result() == "cached"  # uses cache

    def test_result_error_raises(self, tmp_path):
        f = _make_future(tmp_path, returncode=0)
        envelope = {
            "status": "error",
            "error": "bad",
            "type": "ValueError",
            "traceback": "",
        }
        with open(tmp_path / "result.pkl", "wb") as fp:
            pickle.dump(envelope, fp)
        with pytest.raises(SlurmJobFailed, match="ValueError: bad"):
            f.result()

    def test_result_error_no_raise(self, tmp_path):
        f = _make_future(tmp_path, returncode=0)
        envelope = {
            "status": "error",
            "error": "bad",
            "type": "ValueError",
            "traceback": "",
        }
        with open(tmp_path / "result.pkl", "wb") as fp:
            pickle.dump(envelope, fp)
        assert f.result(raise_on_failure=False) is None

    def test_result_missing_pkl_raises(self, tmp_path):
        f = _make_future(tmp_path, returncode=0)
        with pytest.raises(SlurmJobFailed, match="no result.pkl"):
            f.result()

    def test_result_failed_state_raises_slurm_job_failed(self, tmp_path):
        from prefect.states import Failed

        f = _make_future(tmp_path, returncode=0)
        state = Failed(data=ValueError("bad input"))
        envelope = {"status": "ok", "result": state}
        with open(tmp_path / "result.pkl", "wb") as fp:
            pickle.dump(envelope, fp)
        with pytest.raises(SlurmJobFailed, match="bad input"):
            f.result()

    def test_result_failed_state_no_raise(self, tmp_path):
        from prefect.states import Failed

        f = _make_future(tmp_path, returncode=0)
        state = Failed(data=ValueError("bad input"))
        envelope = {"status": "ok", "result": state}
        with open(tmp_path / "result.pkl", "wb") as fp:
            pickle.dump(envelope, fp)
        assert f.result(raise_on_failure=False) is None

    def test_result_failed_state_chains_original_exception(self, tmp_path):
        from prefect.states import Failed

        f = _make_future(tmp_path, returncode=0)
        state = Failed(data=ValueError("bad input"))
        envelope = {"status": "ok", "result": state}
        with open(tmp_path / "result.pkl", "wb") as fp:
            pickle.dump(envelope, fp)
        with pytest.raises(SlurmJobFailed) as exc_info:
            f.result()
        assert isinstance(exc_info.value.__cause__, ValueError)

    def test_result_failed_state_includes_job_label(self, tmp_path):
        f = _make_future(tmp_path, returncode=0, slurm_job_id="456", step_index=3)
        from prefect.states import Failed

        state = Failed(data=ValueError("bad input"))
        envelope = {"status": "ok", "result": state}
        with open(tmp_path / "result.pkl", "wb") as fp:
            pickle.dump(envelope, fp)
        with pytest.raises(SlurmJobFailed, match="srun step 456.3"):
            f.result()

    def test_result_failed_state_cached_after_no_raise(self, tmp_path):
        from prefect.states import Failed

        f = _make_future(tmp_path, returncode=0)
        state = Failed(data=ValueError("bad input"))
        envelope = {"status": "ok", "result": state}
        with open(tmp_path / "result.pkl", "wb") as fp:
            pickle.dump(envelope, fp)
        assert f.result(raise_on_failure=False) is None
        assert f.result(raise_on_failure=False) is None  # uses cache


class TestCancel:
    """Test cancel() behavior."""

    def test_cancel_terminates_process(self, tmp_path):
        f = _make_future(tmp_path, returncode=None)
        assert f.cancel() is True
        f._process.terminate.assert_called_once()

    def test_cancel_returns_false_on_error(self, tmp_path):
        f = _make_future(tmp_path, returncode=None)
        f._process.terminate.side_effect = OSError("already dead")
        assert f.cancel() is False


class TestCallbacks:
    """Test done callback mechanism."""

    def test_callback_fires_on_completion(self, tmp_path):
        f = _make_future(tmp_path, returncode=0)
        called = []
        f.add_done_callback(lambda fut: called.append(True))
        f.wait()
        assert called == [True]

    def test_callback_fires_immediately_if_done(self, tmp_path):
        f = _make_future(tmp_path, returncode=0)
        f.wait()
        called = []
        f.add_done_callback(lambda fut: called.append(True))
        assert called == [True]

    def test_callback_exception_isolated(self, tmp_path):
        f = _make_future(tmp_path, returncode=0)
        f.add_done_callback(lambda fut: 1 / 0)
        f.add_done_callback(lambda fut: None)  # should still fire
        f.wait()  # no exception propagated


class TestSignalName:
    """Test exit code to signal name mapping."""

    def test_sigkill(self):
        assert SrunPrefectFuture._signal_name(137) is not None
        assert "KILL" in SrunPrefectFuture._signal_name(137)

    def test_normal_exit(self):
        assert SrunPrefectFuture._signal_name(1) is None

    def test_zero(self):
        assert SrunPrefectFuture._signal_name(0) is None
