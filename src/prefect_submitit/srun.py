"""SrunBackend: dispatch Prefect tasks via srun within a SLURM allocation."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from prefect_submitit.futures.srun import SrunPrefectFuture

if TYPE_CHECKING:
    from prefect_submitit.runner import SlurmTaskRunner

logger = logging.getLogger(__name__)

# Minimum delay between successive srun launches to avoid overwhelming slurmctld
_MIN_LAUNCH_INTERVAL = 0.05  # 50ms


class SrunBackend:
    """Dispatch tasks via ``srun`` subprocesses within an existing allocation.

    Args:
        runner: The parent ``SlurmTaskRunner`` (for config access).
    """

    def __init__(self, runner: SlurmTaskRunner) -> None:
        self._runner = runner
        self._log_folder = Path(runner.log_folder) / "srun"
        self._log_folder.mkdir(parents=True, exist_ok=True)

        self._active_processes: list[subprocess.Popen[bytes]] = []
        self._step_counter: int = 0
        self._last_launch: float = 0.0
        self._slurm_job_id = os.environ.get("SLURM_JOB_ID", "unknown")

    # -- Public API -----------------------------------------------------------

    def submit_one(
        self,
        wrapped_call: Callable[..., Any],
        task_run_id: uuid.UUID,
    ) -> SrunPrefectFuture:
        """Serialize a callable and launch it via srun."""
        step_index = self._step_counter
        self._step_counter += 1

        job_folder = self._log_folder / f"step_{step_index}"
        job_folder.mkdir(parents=True, exist_ok=True)

        # Serialize with cloudpickle to handle closures and lambdas
        import cloudpickle

        job_path = job_folder / "job.pkl"
        with open(job_path, "wb") as f:
            cloudpickle.dump(wrapped_call, f)

        # Launch
        self._wait_for_slot()
        self._enforce_launch_interval()

        cmd = self._build_srun_command(str(job_folder))
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self._active_processes.append(proc)
        self._last_launch = time.monotonic()

        logger.debug(
            "Launched srun step %d (pid=%d) for task %s",
            step_index,
            proc.pid,
            task_run_id,
        )

        max_poll = self._runner.max_poll_time
        if max_poll is None:
            from prefect_submitit.constants import DEFAULT_POLL_TIME_MULTIPLIER
            from prefect_submitit.utils import parse_time_to_minutes

            max_poll = (
                parse_time_to_minutes(self._runner.time_limit)
                * 60
                * DEFAULT_POLL_TIME_MULTIPLIER
            )

        return SrunPrefectFuture(
            process=proc,
            job_folder=job_folder,
            task_run_id=task_run_id,
            step_index=step_index,
            slurm_job_id=self._slurm_job_id,
            poll_interval=self._runner.poll_interval,
            max_poll_time=max_poll,
        )

    def submit_many(
        self,
        wrapped_calls: list[Callable[..., Any]],
        task_run_ids: list[uuid.UUID],
    ) -> list[SrunPrefectFuture]:
        """Submit multiple callables, respecting concurrency limits."""
        futures = []
        for call, trid in zip(wrapped_calls, task_run_ids):
            futures.append(self.submit_one(call, trid))
        return futures

    def close(self) -> None:
        """Terminate all active srun subprocesses (SIGTERM -> wait -> SIGKILL)."""
        alive = [p for p in self._active_processes if p.poll() is None]
        if not alive:
            self._active_processes.clear()
            return

        logger.info("Terminating %d active srun processes", len(alive))
        for proc in alive:
            try:
                proc.terminate()
            except OSError:
                pass

        deadline = time.monotonic() + 10.0
        for proc in alive:
            remaining = max(0, deadline - time.monotonic())
            try:
                proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                logger.warning("Force-killing srun pid=%d", proc.pid)
                proc.kill()
                proc.wait(timeout=5)

        self._active_processes.clear()

    # -- Private helpers ------------------------------------------------------

    def _build_srun_command(self, job_folder: str) -> list[str]:
        """Construct the srun command line."""
        cmd = ["srun", "--exact", "--mpi=none", "-n1"]

        runner = self._runner
        if "slurm_gres" in runner.slurm_kwargs:
            cmd.extend(["--gres", runner.slurm_kwargs["slurm_gres"]])
        elif runner.gpus_per_node > 0:
            cmd.extend(["--gres", f"gpu:{runner.gpus_per_node}"])

        if runner.mem_gb > 0:
            cmd.extend(["--mem", f"{runner.mem_gb}G"])

        if runner.cpus_per_task > 0:
            cmd.extend(["--cpus-per-task", str(runner.cpus_per_task)])

        if runner.time_limit:
            cmd.extend(["--time", runner.time_limit])

        cmd.extend(
            [sys.executable, "-u", "-m", "prefect_submitit.srun_worker", job_folder]
        )
        return cmd

    def _wait_for_slot(self) -> None:
        """Block until active process count is below the concurrency cap."""
        cap = self._runner.srun_launch_concurrency
        while True:
            self._reap_finished()
            if len(self._active_processes) < cap:
                return
            time.sleep(self._runner.poll_interval)

    def _enforce_launch_interval(self) -> None:
        """Sleep if less than _MIN_LAUNCH_INTERVAL since last launch."""
        elapsed = time.monotonic() - self._last_launch
        if elapsed < _MIN_LAUNCH_INTERVAL:
            time.sleep(_MIN_LAUNCH_INTERVAL - elapsed)

    def _reap_finished(self) -> None:
        """Remove completed processes from the active list."""
        self._active_processes = [p for p in self._active_processes if p.poll() is None]
