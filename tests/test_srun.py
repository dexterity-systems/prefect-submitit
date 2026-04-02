"""Tests for prefect_submitit.srun.SrunBackend."""

from __future__ import annotations

import pickle
import sys
from unittest.mock import MagicMock, patch
from uuid import uuid4

from prefect_submitit.srun import SrunBackend


def _mock_runner(**overrides):
    """Create a mock runner with sensible defaults."""
    runner = MagicMock()
    runner.log_folder = overrides.get("log_folder", "/tmp/test_srun_logs")
    runner.gpus_per_node = overrides.get("gpus_per_node", 0)
    runner.mem_gb = overrides.get("mem_gb", 4)
    runner.cpus_per_task = overrides.get("cpus_per_task", 1)
    runner.time_limit = overrides.get("time_limit", "01:00:00")
    runner.poll_interval = overrides.get("poll_interval", 0.5)
    runner.max_poll_time = overrides.get("max_poll_time")
    runner.srun_launch_concurrency = overrides.get("srun_launch_concurrency", 128)
    runner.slurm_kwargs = overrides.get("slurm_kwargs", {})
    return runner


class TestBuildSrunCommand:
    """Test _build_srun_command output."""

    def test_basic_command(self, tmp_path):
        runner = _mock_runner(log_folder=str(tmp_path))
        backend = SrunBackend(runner)
        cmd = backend._build_srun_command("/tmp/step_0")
        assert cmd[:4] == ["srun", "--exact", "--mpi=none", "-n1"]
        assert sys.executable in cmd
        assert "-m" in cmd
        assert "prefect_submitit.srun_worker" in cmd
        assert "/tmp/step_0" in cmd

    def test_gpu_flag(self, tmp_path):
        runner = _mock_runner(log_folder=str(tmp_path), gpus_per_node=2)
        backend = SrunBackend(runner)
        cmd = backend._build_srun_command("/tmp/step_0")
        assert "--gres" in cmd
        assert "gpu:2" in cmd

    def test_slurm_gres_overrides_gpus(self, tmp_path):
        runner = _mock_runner(
            log_folder=str(tmp_path),
            gpus_per_node=2,
            slurm_kwargs={"slurm_gres": "gpu:a100:1"},
        )
        backend = SrunBackend(runner)
        cmd = backend._build_srun_command("/tmp/step_0")
        assert "gpu:a100:1" in cmd
        assert "gpu:2" not in cmd

    def test_mem_flag(self, tmp_path):
        runner = _mock_runner(log_folder=str(tmp_path), mem_gb=16)
        backend = SrunBackend(runner)
        cmd = backend._build_srun_command("/tmp/step_0")
        assert "--mem" in cmd
        assert "16G" in cmd

    def test_cpus_per_task_flag(self, tmp_path):
        runner = _mock_runner(log_folder=str(tmp_path), cpus_per_task=4)
        backend = SrunBackend(runner)
        cmd = backend._build_srun_command("/tmp/step_0")
        assert "--cpus-per-task" in cmd
        assert "4" in cmd

    def test_time_flag(self, tmp_path):
        runner = _mock_runner(log_folder=str(tmp_path), time_limit="02:30:00")
        backend = SrunBackend(runner)
        cmd = backend._build_srun_command("/tmp/step_0")
        assert "--time" in cmd
        assert "02:30:00" in cmd


class TestSubmitOne:
    """Test submit_one serialization and future creation."""

    @patch("prefect_submitit.srun.subprocess.Popen")
    def test_serializes_callable(self, mock_popen, tmp_path):
        runner = _mock_runner(log_folder=str(tmp_path))
        mock_proc = MagicMock()
        mock_proc.pid = 123
        mock_popen.return_value = mock_proc

        backend = SrunBackend(runner)

        def my_fn():
            return 42

        future = backend.submit_one(my_fn, uuid4())

        # Verify job.pkl was written
        job_pkl = tmp_path / "srun" / "step_0" / "job.pkl"
        assert job_pkl.exists()
        with open(job_pkl, "rb") as f:
            loaded = pickle.load(f)
        assert loaded() == 42

    @patch("prefect_submitit.srun.subprocess.Popen")
    def test_increments_step_counter(self, mock_popen, tmp_path):
        runner = _mock_runner(log_folder=str(tmp_path))
        mock_popen.return_value = MagicMock(pid=1)

        backend = SrunBackend(runner)
        f1 = backend.submit_one(lambda: 1, uuid4())
        f2 = backend.submit_one(lambda: 2, uuid4())

        assert f1._step_index == 0
        assert f2._step_index == 1


class TestSubmitMany:
    """Test submit_many dispatches correctly."""

    @patch("prefect_submitit.srun.subprocess.Popen")
    def test_submits_all(self, mock_popen, tmp_path):
        runner = _mock_runner(log_folder=str(tmp_path))
        mock_popen.return_value = MagicMock(pid=1)

        backend = SrunBackend(runner)
        calls = [lambda i=i: i for i in range(3)]
        ids = [uuid4() for _ in range(3)]
        futures = backend.submit_many(calls, ids)

        assert len(futures) == 3
        assert futures[0]._step_index == 0
        assert futures[2]._step_index == 2


class TestClose:
    """Test close() cleanup."""

    def test_close_terminates_active(self, tmp_path):
        runner = _mock_runner(log_folder=str(tmp_path))
        backend = SrunBackend(runner)

        proc1 = MagicMock()
        proc1.poll.return_value = None  # still running
        proc1.wait.return_value = 0
        proc2 = MagicMock()
        proc2.poll.return_value = 0  # already done

        backend._active_processes = [proc1, proc2]
        backend.close()

        proc1.terminate.assert_called_once()
        proc2.terminate.assert_not_called()
        assert len(backend._active_processes) == 0

    def test_close_empty_is_safe(self, tmp_path):
        runner = _mock_runner(log_folder=str(tmp_path))
        backend = SrunBackend(runner)
        backend.close()  # no error
