"""Base Prefect future wrapper for submitit jobs."""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable
from typing import Any

import cloudpickle
import submitit
from prefect.futures import PrefectFuture
from prefect.states import (  # type: ignore[attr-defined]
    Completed,
    Failed,
    Pending,
    Running,
    State,
)
from submitit.core.utils import FailedJobError, UncompletedJobError

logger = logging.getLogger(__name__)


class SlurmJobFailed(Exception):
    """Raised when a SLURM job fails."""


class SlurmPrefectFuture(PrefectFuture[Any]):
    """Wrap submitit.Job to implement Prefect's PrefectFuture protocol."""

    TERMINAL_FAILURE_STATES = frozenset(
        {"FAILED", "NODE_FAIL", "TIMEOUT", "CANCELLED", "OUT_OF_MEMORY"}
    )

    def __init__(
        self,
        job: submitit.Job[Any],
        task_run_id: uuid.UUID,
        poll_interval: float,
        max_poll_time: float,
    ):
        self._job = job
        self._task_run_id = task_run_id
        self._poll_interval = poll_interval
        self._max_poll_time = max_poll_time
        self._callbacks: list[Callable[[PrefectFuture[Any]], None]] = []
        self._result_cache: Any = None
        self._result_retrieved = False
        self._done = False

    @property
    def task_run_id(self) -> uuid.UUID:
        return self._task_run_id

    @property
    def slurm_job_id(self) -> str:
        return str(self._job.job_id)

    @property
    def is_done(self) -> bool:
        return self._done

    @property
    def state(self) -> State:
        slurm_state = self._job.state
        if slurm_state == "COMPLETED":
            return Completed()
        if slurm_state in self.TERMINAL_FAILURE_STATES:
            return Failed(message=f"SLURM: {slurm_state}")
        if slurm_state == "RUNNING":
            return Running()
        return Pending()

    def wait(self, timeout: float | None = None) -> None:
        effective_timeout = timeout or self._max_poll_time
        start = time.time()

        while not self._job.done():
            elapsed = time.time() - start
            if effective_timeout and elapsed > effective_timeout:
                msg = (
                    f"Job {self.slurm_job_id} did not complete "
                    f"within {effective_timeout:.0f}s"
                )
                raise TimeoutError(msg)

            slurm_state = self._job.state
            if slurm_state in self.TERMINAL_FAILURE_STATES:
                try:
                    stderr = self._job.stderr()
                except Exception:
                    stderr = "(stderr unavailable)"
                msg = f"Job {self.slurm_job_id}: {slurm_state}\nstderr:\n{stderr}"
                raise SlurmJobFailed(msg)

            time.sleep(self._poll_interval)

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

        try:
            pickled_result = self._job.result()
            state = cloudpickle.loads(pickled_result)
            if hasattr(state, "result"):
                self._result_cache = state.result()
            else:
                self._result_cache = state

            self._result_retrieved = True
            return self._result_cache

        except FailedJobError as e:
            if raise_on_failure:
                msg = f"Job {self.slurm_job_id} failed: {e}"
                raise SlurmJobFailed(msg) from e
            return None
        except UncompletedJobError as e:
            stderr = self._job.stderr()
            if raise_on_failure:
                msg = (
                    f"Job {self.slurm_job_id} produced no output: {e}\nstderr: {stderr}"
                )
                raise SlurmJobFailed(msg) from e
            return None

    def add_done_callback(self, fn: Callable[[PrefectFuture[Any]], None]) -> None:
        self._callbacks.append(fn)
        if self._done:
            fn(self)

    def _fire_callbacks(self) -> None:
        for fn in self._callbacks:
            try:
                fn(self)
            except Exception:
                callback_name = fn.__name__ if hasattr(fn, "__name__") else fn
                logger.exception(
                    "Callback %s failed for job %s",
                    callback_name,
                    self.slurm_job_id,
                )

    def logs(self) -> tuple[str, str]:
        return self._job.stdout() or "", self._job.stderr() or ""
