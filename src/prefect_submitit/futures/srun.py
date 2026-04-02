"""Prefect future wrapping an srun subprocess."""

from __future__ import annotations

import logging
import pickle
import signal
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import cloudpickle
from prefect.futures import PrefectFuture
from prefect.states import Completed, Failed, Pending, Running, State

from prefect_submitit.futures.base import SlurmJobFailed, _unwrap_state

logger = logging.getLogger(__name__)

# Signals that produce 128+N exit codes
_SIGNAL_NAMES = {
    signal.SIGKILL: "SIGKILL (OOM or scancel -9)",
    signal.SIGTERM: "SIGTERM (scancel or wall-time)",
    signal.SIGINT: "SIGINT",
    signal.SIGSEGV: "SIGSEGV",
}


class SrunPrefectFuture(PrefectFuture[Any]):
    """Wrap an srun subprocess and its result pickle."""

    def __init__(
        self,
        process: Any,  # subprocess.Popen
        job_folder: Path,
        task_run_id: uuid.UUID,
        step_index: int,
        slurm_job_id: str,
        poll_interval: float,
        max_poll_time: float,
    ):
        self._process = process
        self._job_folder = job_folder
        self._task_run_id = task_run_id
        self._step_index = step_index
        self._slurm_job_id = slurm_job_id
        self._poll_interval = poll_interval
        self._max_poll_time = max_poll_time
        self._callbacks: list[Callable[[PrefectFuture[Any]], None]] = []
        self._result_cache: Any = None
        self._result_retrieved = False
        self._done = False

    # -- Properties -----------------------------------------------------------

    @property
    def task_run_id(self) -> uuid.UUID:
        return self._task_run_id

    @property
    def slurm_job_id(self) -> str:
        return f"{self._slurm_job_id}.{self._step_index}"

    @property
    def array_task_index(self) -> int:
        """Step index, for SlurmBatchedItemFuture compatibility."""
        return self._step_index

    @property
    def is_done(self) -> bool:
        return self._done

    @property
    def state(self) -> State:
        if self._done:
            rc = self._process.returncode
            if rc == 0:
                return Completed()
            return Failed()
        if self._process.poll() is not None:
            return Completed() if self._process.returncode == 0 else Failed()
        return Running() if self._process.pid else Pending()

    # -- Core protocol --------------------------------------------------------

    def wait(self, timeout: float | None = None) -> None:
        if self._done:
            return

        effective_timeout = timeout if timeout is not None else self._max_poll_time
        deadline = time.monotonic() + effective_timeout

        while True:
            rc = self._process.poll()
            if rc is not None:
                break
            if time.monotonic() >= deadline:
                msg = (
                    f"srun step {self.slurm_job_id} did not complete "
                    f"within {effective_timeout}s"
                )
                raise TimeoutError(msg)
            time.sleep(self._poll_interval)

        if rc != 0:
            stderr = self._read_stderr()
            sig_name = self._signal_name(rc)
            detail = f" ({sig_name})" if sig_name else ""
            msg = (
                f"srun step {self.slurm_job_id} exited with code {rc}{detail}"
                f"\nstderr: {stderr}"
            )
            raise SlurmJobFailed(msg)

        self._done = True
        self._fire_callbacks()

    def result(
        self,
        timeout: float | None = None,
        raise_on_failure: bool = True,
    ) -> Any:
        if self._result_retrieved:
            return self._result_cache

        self.wait(timeout)

        result_path = self._job_folder / "result.pkl"
        if not result_path.exists():
            if raise_on_failure:
                msg = f"srun step {self.slurm_job_id}: no result.pkl found"
                raise SlurmJobFailed(msg)
            return None

        with open(result_path, "rb") as f:
            envelope = pickle.load(f)

        if envelope["status"] == "error":
            if raise_on_failure:
                msg = (
                    f"srun step {self.slurm_job_id}: "
                    f"{envelope['type']}: {envelope['error']}"
                )
                raise SlurmJobFailed(msg)
            return None

        raw_result = envelope["result"]
        # Unwrap cloudpickle-serialized Prefect state if present
        if isinstance(raw_result, bytes):
            raw_result = cloudpickle.loads(raw_result)
        self._result_cache = _unwrap_state(
            raw_result, raise_on_failure, f"srun step {self.slurm_job_id}"
        )

        self._result_retrieved = True
        return self._result_cache

    def cancel(self) -> bool:
        """Send SIGTERM to the srun process."""
        try:
            self._process.terminate()
            return True
        except OSError:
            return False

    def add_done_callback(self, fn: Callable[[PrefectFuture[Any]], None]) -> None:
        self._callbacks.append(fn)
        if self._done:
            fn(self)

    # -- Helpers --------------------------------------------------------------

    def logs(self) -> tuple[str, str]:
        """Return captured stdout and stderr."""
        stdout = ""
        stderr = self._read_stderr()
        stdout_path = self._job_folder / "stdout.log"
        if stdout_path.exists():
            stdout = stdout_path.read_text()
        return stdout, stderr

    def _fire_callbacks(self) -> None:
        for fn in self._callbacks:
            try:
                fn(self)
            except Exception:
                callback_name = getattr(fn, "__name__", fn)
                logger.exception(
                    "Callback %s failed for step %s",
                    callback_name,
                    self.slurm_job_id,
                )

    def _read_stderr(self) -> str:
        """Read stderr from the process pipe."""
        if self._process.stderr is not None:
            try:
                return self._process.stderr.read().decode(errors="replace")
            except (ValueError, OSError):
                return ""
        return ""

    @staticmethod
    def _signal_name(exit_code: int) -> str | None:
        """Decode 128+N exit code convention to a signal name."""
        if exit_code > 128:
            sig_num = exit_code - 128
            try:
                sig = signal.Signals(sig_num)
                return _SIGNAL_NAMES.get(sig, sig.name)
            except ValueError:
                return f"signal {sig_num}"
        return None
