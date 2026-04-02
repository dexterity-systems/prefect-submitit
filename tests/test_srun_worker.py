"""Tests for prefect_submitit.srun_worker."""

from __future__ import annotations

import pickle
import subprocess
import sys

import cloudpickle


class TestSrunWorkerRoundTrip:
    """Test the worker via subprocess (real invocation)."""

    def test_success(self, tmp_path):
        """Serialize a callable, run the worker, verify result envelope."""

        def compute():
            return 42

        job_pkl = tmp_path / "job.pkl"
        with open(job_pkl, "wb") as f:
            cloudpickle.dump(compute, f)

        result = subprocess.run(
            [sys.executable, "-m", "prefect_submitit.srun_worker", str(tmp_path)],
            capture_output=True,
            timeout=30,
        )
        assert result.returncode == 0

        with open(tmp_path / "result.pkl", "rb") as f:
            envelope = pickle.load(f)
        assert envelope["status"] == "ok"
        assert envelope["result"] == 42

    def test_task_error_exits_zero(self, tmp_path):
        """Task raises an exception, worker exits 0, error is in envelope."""

        def failing():
            raise ValueError("bad input")

        with open(tmp_path / "job.pkl", "wb") as f:
            cloudpickle.dump(failing, f)

        result = subprocess.run(
            [sys.executable, "-m", "prefect_submitit.srun_worker", str(tmp_path)],
            capture_output=True,
            timeout=30,
        )
        assert result.returncode == 0

        with open(tmp_path / "result.pkl", "rb") as f:
            envelope = pickle.load(f)
        assert envelope["status"] == "error"
        assert envelope["type"] == "ValueError"
        assert "bad input" in envelope["error"]
        assert "traceback" in envelope

    def test_bootstrap_failure_exits_nonzero(self, tmp_path):
        """Missing job.pkl -> worker exits non-zero, no result.pkl."""
        result = subprocess.run(
            [sys.executable, "-m", "prefect_submitit.srun_worker", str(tmp_path)],
            capture_output=True,
            timeout=30,
        )
        assert result.returncode != 0
        assert not (tmp_path / "result.pkl").exists()

    def test_atomic_write(self, tmp_path):
        """Result file exists as result.pkl, not a .tmp file."""

        def noop():
            return None

        with open(tmp_path / "job.pkl", "wb") as f:
            cloudpickle.dump(noop, f)

        subprocess.run(
            [sys.executable, "-m", "prefect_submitit.srun_worker", str(tmp_path)],
            capture_output=True,
            timeout=30,
        )
        assert (tmp_path / "result.pkl").exists()
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert len(tmp_files) == 0

    def test_no_args_exits_2(self):
        """Running without arguments exits with code 2."""
        result = subprocess.run(
            [sys.executable, "-m", "prefect_submitit.srun_worker"],
            capture_output=True,
            timeout=30,
        )
        assert result.returncode == 2
