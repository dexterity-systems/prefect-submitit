"""Per-item future wrapper for batched SLURM jobs."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from prefect.futures import PrefectFuture
from prefect.states import State  # type: ignore[attr-defined]

from prefect_submitit.futures.array import (
    SlurmArrayPrefectFuture,
)

if TYPE_CHECKING:
    from prefect_submitit.futures.srun import SrunPrefectFuture

logger = logging.getLogger(__name__)


class SlurmBatchedItemFuture(PrefectFuture[Any]):
    """Future representing one item inside a batched SLURM or srun job."""

    def __init__(
        self,
        slurm_job_future: SlurmArrayPrefectFuture | SrunPrefectFuture,
        item_index_in_job: int,
        global_item_index: int,
        task_run_id: uuid.UUID,
    ):
        self._slurm_job_future = slurm_job_future
        self._item_index_in_job = item_index_in_job
        self._global_item_index = global_item_index
        self._task_run_id = task_run_id
        self._callbacks: list[Callable[[PrefectFuture[Any]], None]] = []
        self._result_cache: Any = None
        self._result_retrieved = False

    @property
    def task_run_id(self) -> uuid.UUID:
        return self._task_run_id

    @property
    def slurm_job_future(self) -> SlurmArrayPrefectFuture | SrunPrefectFuture:
        return self._slurm_job_future

    @property
    def item_index_in_job(self) -> int:
        return self._item_index_in_job

    @property
    def global_item_index(self) -> int:
        return self._global_item_index

    @property
    def slurm_job_index(self) -> int:
        return self._slurm_job_future.array_task_index

    @property
    def slurm_job_id(self) -> str:
        return self._slurm_job_future.slurm_job_id

    @property
    def state(self) -> State:
        return self._slurm_job_future.state

    def wait(self, timeout: float | None = None) -> None:
        self._slurm_job_future.wait(timeout)
        self._fire_callbacks()

    def result(
        self,
        timeout: float | None = None,
        raise_on_failure: bool = True,
    ) -> Any:
        if self._result_retrieved:
            return self._result_cache

        job_result = self._slurm_job_future.result(
            timeout=timeout, raise_on_failure=raise_on_failure
        )
        if job_result is None:
            return None

        if self._item_index_in_job >= len(job_result):
            self._result_cache = {
                "success": False,
                "error": (
                    f"Batch job returned {len(job_result)} results "
                    f"but expected index {self._item_index_in_job} "
                    f"(batch was likely terminated early)"
                ),
                "item_count": 1,
                "execution_run_ids": [],
            }
        else:
            self._result_cache = job_result[self._item_index_in_job]
        self._result_retrieved = True
        return self._result_cache

    def add_done_callback(self, fn: Callable[[PrefectFuture[Any]], None]) -> None:
        self._callbacks.append(fn)
        if self._slurm_job_future.is_done:
            fn(self)

    def _fire_callbacks(self) -> None:
        for fn in self._callbacks:
            try:
                fn(self)
            except Exception:
                callback_name = fn.__name__ if hasattr(fn, "__name__") else fn
                logger.exception(
                    "Callback %s failed for batched item %s in job %s",
                    callback_name,
                    self._global_item_index,
                    self.slurm_job_id,
                )

    def __repr__(self) -> str:
        return (
            f"SlurmBatchedItemFuture("
            f"global_index={self._global_item_index}, "
            f"job_index={self.slurm_job_index}, "
            f"item_in_job={self._item_index_in_job}, "
            f"slurm_job={self.slurm_job_id}, "
            f"state={self.state.type})"
        )
