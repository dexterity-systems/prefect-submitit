"""SLURM TaskRunner for Prefect 3."""

from __future__ import annotations

import os
import signal
import uuid
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, Self

import submitit
from prefect._internal.uuid7 import uuid7
from prefect.client.schemas.objects import RunInput
from prefect.context import FlowRunContext, serialize_context
from prefect.futures import PrefectFuture, PrefectFutureList
from prefect.futures import wait as prefect_wait
from prefect.logging.loggers import get_run_logger
from prefect.settings.context import get_current_settings
from prefect.task_runners import TaskRunner
from prefect.utilities.engine import resolve_inputs_sync

from prefect_submitit.constants import (
    DEFAULT_POLL_TIME_MULTIPLIER,
    ExecutionMode,
)
from prefect_submitit.futures import (
    SlurmArrayPrefectFuture,
    SlurmBatchedItemFuture,
    SlurmPrefectFuture,
)
from prefect_submitit.submission import (
    batch_items,
    build_array_callable,
    build_batch_callable,
    build_task_callable,
    submit_batch_array_chunk,
    submit_batched_job_array,
    submit_job_array,
    submit_single_job_array,
)
from prefect_submitit.utils import (
    get_cluster_max_array_size,
    parse_time_to_minutes,
    partition_parameters,
    validate_iterable_lengths,
)

if TYPE_CHECKING:
    from prefect.tasks import Task


class SlurmTaskRunner(TaskRunner):
    """Prefect 3 TaskRunner that submits tasks via submitit."""

    def __init__(
        self,
        partition: str = "cpu",
        time_limit: str = "01:00:00",
        mem_gb: int = 4,
        gpus_per_node: int = 0,
        cpus_per_task: int = 1,
        slurm_array_parallelism: int = 1000,
        poll_interval: float | None = None,
        max_poll_time: float | None = None,
        log_folder: str = "slurm_logs",
        fail_on_error: bool = True,
        units_per_worker: int = 1,
        execution_mode: ExecutionMode | str | None = None,
        max_array_size: int | None = None,
        srun_launch_concurrency: int = 128,
        **slurm_kwargs: Any,
    ):
        super().__init__()  # type: ignore[no-untyped-call]

        if execution_mode is None:
            env_backend = os.environ.get("SLURM_TASKRUNNER_BACKEND")
            if env_backend:
                execution_mode = ExecutionMode(env_backend)
            else:
                execution_mode = ExecutionMode.SLURM
        elif isinstance(execution_mode, str):
            execution_mode = ExecutionMode(execution_mode)

        if units_per_worker < 1:
            msg = f"units_per_worker must be >= 1, got {units_per_worker}"
            raise ValueError(msg)

        self.execution_mode: ExecutionMode = execution_mode
        self.partition = partition
        self.time_limit = time_limit
        self.mem_gb = mem_gb
        self.gpus_per_node = gpus_per_node
        self.cpus_per_task = cpus_per_task
        self.slurm_array_parallelism = slurm_array_parallelism
        self.max_poll_time = max_poll_time
        self.log_folder = log_folder
        self.fail_on_error = fail_on_error
        self.units_per_worker = units_per_worker
        self.max_array_size = max_array_size
        self.srun_launch_concurrency = srun_launch_concurrency
        self.slurm_kwargs = slurm_kwargs

        if poll_interval is not None:
            self.poll_interval = poll_interval
        elif self.execution_mode == ExecutionMode.LOCAL:
            self.poll_interval = 1.0
        elif self.execution_mode == ExecutionMode.SRUN:
            self.poll_interval = 0.5
        else:
            self.poll_interval = 5.0

        self._executor: submitit.AutoExecutor | submitit.LocalExecutor | None = None
        self._backend: Any | None = None
        self._entered: bool = False
        self._cached_max_array_size: int | None = None

    def _parse_time_to_minutes(self, time_str: str) -> int:
        return parse_time_to_minutes(time_str)

    def __enter__(self) -> Self:
        super().__enter__()
        self._entered = True
        if self.execution_mode == ExecutionMode.LOCAL:
            ignored_params = []
            if self.partition != "cpu":
                ignored_params.append(f"partition={self.partition!r}")
            if self.mem_gb != 4:
                ignored_params.append(f"mem_gb={self.mem_gb}")
            if self.gpus_per_node != 0:
                ignored_params.append(f"gpus_per_node={self.gpus_per_node}")
            if self.slurm_array_parallelism != 1000:
                ignored_params.append(
                    f"slurm_array_parallelism={self.slurm_array_parallelism}"
                )
            if self.slurm_kwargs:
                ignored_params.append(f"slurm_kwargs={list(self.slurm_kwargs.keys())}")
            if ignored_params:
                self.logger.warning(
                    "Local backend ignores SLURM parameters: %s",
                    ", ".join(ignored_params),
                )

            self._executor = submitit.LocalExecutor(folder=self.log_folder)
            self._executor.update_parameters(
                timeout_min=self._parse_time_to_minutes(self.time_limit),
            )
        elif self.execution_mode == ExecutionMode.SLURM:
            self._executor = submitit.AutoExecutor(folder=f"{self.log_folder}/%j")
            params: dict[str, Any] = {
                "slurm_partition": self.partition,
                "timeout_min": self._parse_time_to_minutes(self.time_limit),
                "mem_gb": self.mem_gb,
                "cpus_per_task": self.cpus_per_task,
                "slurm_array_parallelism": self.slurm_array_parallelism,
                "slurm_srun_args": ["--export=ALL"],
            }
            if "slurm_gres" in self.slurm_kwargs:
                if self.gpus_per_node > 0:
                    self.logger.warning(
                        "Both gpus_per_node=%s and slurm_gres=%s specified. "
                        "Using slurm_gres only (they are mutually exclusive in SLURM).",
                        self.gpus_per_node,
                        self.slurm_kwargs["slurm_gres"],
                    )
            elif self.gpus_per_node > 0:
                params["gpus_per_node"] = self.gpus_per_node

            params.update(self.slurm_kwargs)
            self._executor.update_parameters(**params)
        elif self.execution_mode == ExecutionMode.SRUN:
            if not os.environ.get("SLURM_JOB_ID"):
                msg = (
                    "SRUN mode requires an existing SLURM allocation "
                    "(SLURM_JOB_ID not set). Use salloc or sbatch first."
                )
                raise RuntimeError(msg)

            ignored_params = []
            if self.partition != "cpu":
                ignored_params.append(f"partition={self.partition!r}")
            if self.slurm_array_parallelism != 1000:
                ignored_params.append(
                    f"slurm_array_parallelism={self.slurm_array_parallelism}"
                )
            if self.max_array_size is not None:
                ignored_params.append(f"max_array_size={self.max_array_size}")
            if self.slurm_kwargs:
                for key in self.slurm_kwargs:
                    if key != "slurm_gres":
                        ignored_params.append(key)
            if ignored_params:
                self.logger.warning(
                    "SRUN backend ignores scheduler parameters: %s",
                    ", ".join(ignored_params),
                )

            from prefect_submitit.srun import SrunBackend

            self._backend = SrunBackend(self)

            # Register SIGTERM handler so cleanup runs on job cancellation
            self._prev_sigterm = signal.getsignal(signal.SIGTERM)
            signal.signal(signal.SIGTERM, self._sigterm_handler)

        return self

    def _sigterm_handler(self, signum: int, frame: Any) -> None:
        """Clean up srun processes on SIGTERM."""
        if self._backend is not None:
            self._backend.close()
        # Re-raise so the process actually terminates
        if self._prev_sigterm and self._prev_sigterm not in (
            signal.SIG_DFL,
            signal.SIG_IGN,
        ):
            self._prev_sigterm(signum, frame)
        else:
            raise SystemExit(128 + signum)

    def __exit__(self, *args: Any) -> None:
        self._entered = False
        if self._backend is not None:
            self._backend.close()
            self._backend = None
            # Restore previous SIGTERM handler
            if hasattr(self, "_prev_sigterm"):
                signal.signal(signal.SIGTERM, self._prev_sigterm)
        self._executor = None
        super().__exit__(*args)

    def duplicate(self) -> Self:
        return SlurmTaskRunner(  # type: ignore[return-value]
            partition=self.partition,
            time_limit=self.time_limit,
            mem_gb=self.mem_gb,
            gpus_per_node=self.gpus_per_node,
            cpus_per_task=self.cpus_per_task,
            slurm_array_parallelism=self.slurm_array_parallelism,
            poll_interval=self.poll_interval,
            max_poll_time=self.max_poll_time,
            log_folder=self.log_folder,
            fail_on_error=self.fail_on_error,
            units_per_worker=self.units_per_worker,
            execution_mode=self.execution_mode,
            max_array_size=self.max_array_size,
            srun_launch_concurrency=self.srun_launch_concurrency,
            **self.slurm_kwargs,
        )

    def submit(  # type: ignore[override]
        self,
        task: Task[Any, Any],
        parameters: dict[str, Any],
        wait_for: Iterable[PrefectFuture[Any]] | None = None,
        dependencies: dict[str, set[RunInput]] | None = None,
    ) -> SlurmPrefectFuture | PrefectFuture[Any]:
        if not self._entered:
            msg = "SlurmTaskRunner must be used as context manager"
            raise RuntimeError(msg)

        if wait_for:
            prefect_wait(list(wait_for))

        resolved_parameters = resolve_inputs_sync(
            parameters, return_data=True, max_depth=-1
        )
        task_run_id = uuid7()

        flow_run_context = FlowRunContext.get()
        logger = get_run_logger(flow_run_context) if flow_run_context else self.logger
        if self.execution_mode == ExecutionMode.SRUN:
            logger.debug("Submitting task %s via srun...", task.name)
        else:
            logger.debug(
                "Submitting task %s to SLURM (partition=%s)...",
                task.name,
                self.partition,
            )

        context = serialize_context()
        env = (
            get_current_settings().to_environment_variables(exclude_unset=True)
            | os.environ
        )
        wrapped_call = build_task_callable(
            task=task,
            task_run_id=task_run_id,
            parameters=resolved_parameters,
            context=context,
            env=env,
            dependencies=dependencies,
        )

        if self.execution_mode == ExecutionMode.SRUN:
            return self._backend.submit_one(wrapped_call, task_run_id)

        job = self._executor.submit(wrapped_call)

        max_poll = self.max_poll_time
        if max_poll is None:
            max_poll = (
                self._parse_time_to_minutes(self.time_limit)
                * 60
                * DEFAULT_POLL_TIME_MULTIPLIER
            )

        return SlurmPrefectFuture(
            job=job,
            task_run_id=task_run_id,
            poll_interval=self.poll_interval,
            max_poll_time=max_poll,
        )

    def _partition_parameters(
        self, parameters: dict[str, Any]
    ) -> tuple[dict[str, list[Any]], dict[str, Any]]:
        return partition_parameters(parameters)

    def _validate_iterable_lengths(self, iterable_params: dict[str, list[Any]]) -> int:
        return validate_iterable_lengths(iterable_params)

    def _get_cluster_max_array_size(self) -> int:
        return get_cluster_max_array_size(self)

    def _build_array_callable(
        self,
        task: Task[Any, Any],
        index: int,
        iterable_params: dict[str, list[Any]],
        static_params: dict[str, Any],
        task_run_id: uuid.UUID,
        context: dict[str, Any],
        env: dict[str, str],
    ) -> Callable[..., Any]:
        return build_array_callable(
            task,
            index,
            iterable_params,
            static_params,
            task_run_id,
            context,
            env,
        )

    def _batch_items(self, items: list[Any]) -> list[list[Any]]:
        return batch_items(self, items)

    def _build_batch_callable(
        self,
        task: Task[Any, Any],
        task_run_id: uuid.UUID,
        batch: list[Any],
        param_name: str,
        static_params: dict[str, Any],
        context: dict[str, Any],
        env: dict[str, str],
    ) -> Callable[..., Any]:
        return build_batch_callable(
            task, task_run_id, batch, param_name, static_params, context, env
        )

    def _submit_batched_job_array(
        self,
        task: Task[Any, Any],
        items: list[Any],
        param_name: str,
        static_params: dict[str, Any],
    ) -> list[SlurmBatchedItemFuture]:
        return submit_batched_job_array(self, task, items, param_name, static_params)

    def _submit_batch_array_chunk(
        self,
        wrapped_calls: list[Callable[..., Any]],
        task_run_ids: list[uuid.UUID],
        array_size: int,
    ) -> list[SlurmArrayPrefectFuture]:
        return submit_batch_array_chunk(self, wrapped_calls, task_run_ids, array_size)

    def _submit_single_job_array(
        self,
        task: Task[Any, Any],
        iterable_params: dict[str, list[Any]],
        static_params: dict[str, Any],
        map_length: int,
        task_run_ids: list[uuid.UUID],
        context: dict[str, Any],
        env: dict[str, str],
    ) -> list[SlurmArrayPrefectFuture]:
        return submit_single_job_array(
            self,
            task,
            iterable_params,
            static_params,
            map_length,
            task_run_ids,
            context,
            env,
        )

    def _submit_job_array(
        self,
        task: Task[Any, Any],
        iterable_params: dict[str, list[Any]],
        static_params: dict[str, Any],
        map_length: int,
    ) -> list[SlurmArrayPrefectFuture]:
        return submit_job_array(self, task, iterable_params, static_params, map_length)

    def map(  # type: ignore[override]
        self,
        task: Task[Any, Any],
        parameters: dict[str, Any],
        wait_for: Iterable[PrefectFuture[Any]] | None = None,
    ) -> PrefectFutureList[Any]:
        if not self._entered:
            msg = "SlurmTaskRunner must be used as context manager"
            raise RuntimeError(msg)

        if wait_for:
            prefect_wait(list(wait_for))

        iterable_params, static_params = self._partition_parameters(parameters)
        map_length = self._validate_iterable_lengths(iterable_params)

        flow_run_context = FlowRunContext.get()
        logger = get_run_logger(flow_run_context) if flow_run_context else self.logger

        # -- SRUN mode: dispatch via srun backend ----------------------------
        if self.execution_mode == ExecutionMode.SRUN:
            return self._map_srun(
                task, iterable_params, static_params, map_length, logger
            )

        # -- SLURM / LOCAL mode: dispatch via submitit -----------------------
        if self.units_per_worker > 1:
            if len(iterable_params) > 1:
                msg = (
                    "Multi-argument map() is not supported with units_per_worker > 1. "
                    f"Found {len(iterable_params)} iterable parameters: "
                    f"{list(iterable_params.keys())}. "
                    "Use units_per_worker=1 for multi-arg map, or pre-zip arguments "
                    "into single iterable."
                )
                raise ValueError(msg)
            param_name = next(iter(iterable_params.keys()))
            items = iterable_params[param_name]
            num_batches = (
                len(items) + self.units_per_worker - 1
            ) // self.units_per_worker
            logger.info(
                "Submitting %s items as %s batched SLURM jobs "
                "(units_per_worker=%s, partition=%s, parallelism=%s)",
                len(items),
                num_batches,
                self.units_per_worker,
                self.partition,
                self.slurm_array_parallelism,
            )
            batched_futures = self._submit_batched_job_array(
                task, items, param_name, static_params
            )
            return PrefectFutureList(batched_futures)  # type: ignore[abstract]

        logger.info(
            "Submitting %s tasks as SLURM job array (partition=%s, parallelism=%s)",
            map_length,
            self.partition,
            self.slurm_array_parallelism,
        )
        array_futures = self._submit_job_array(
            task, iterable_params, static_params, map_length
        )
        return PrefectFutureList(array_futures)  # type: ignore[abstract]

    def _map_srun(
        self,
        task: Task[Any, Any],
        iterable_params: dict[str, list[Any]],
        static_params: dict[str, Any],
        map_length: int,
        logger: Any,
    ) -> PrefectFutureList[Any]:
        """Dispatch map() tasks via srun backend."""
        context = serialize_context()
        env = (
            get_current_settings().to_environment_variables(exclude_unset=True)
            | os.environ
        )
        task_run_ids = [uuid7() for _ in range(map_length)]

        if self.units_per_worker > 1:
            # Batched srun map
            if len(iterable_params) > 1:
                msg = (
                    "Multi-argument map() is not supported with units_per_worker > 1. "
                    f"Found {len(iterable_params)} iterable parameters: "
                    f"{list(iterable_params.keys())}."
                )
                raise ValueError(msg)
            param_name = next(iter(iterable_params.keys()))
            items = iterable_params[param_name]
            batches = batch_items(self, items)
            batch_run_ids = [uuid7() for _ in batches]
            logger.info(
                "Submitting %s items as %s batched srun steps (units_per_worker=%s)",
                len(items),
                len(batches),
                self.units_per_worker,
            )
            wrapped_calls = [
                build_batch_callable(
                    task=task,
                    task_run_id=batch_run_ids[i],
                    batch=batch,
                    param_name=param_name,
                    static_params=static_params,
                    context=context,
                    env=env,
                )
                for i, batch in enumerate(batches)
            ]
            srun_futures = self._backend.submit_many(wrapped_calls, batch_run_ids)

            # Wrap in SlurmBatchedItemFuture per item
            item_futures = []
            for batch_idx, batch in enumerate(batches):
                for item_in_batch, _item in enumerate(batch):
                    global_idx = batch_idx * self.units_per_worker + item_in_batch
                    trid = (
                        task_run_ids[global_idx]
                        if global_idx < len(task_run_ids)
                        else uuid7()
                    )
                    item_futures.append(
                        SlurmBatchedItemFuture(
                            slurm_job_future=srun_futures[batch_idx],
                            item_index_in_job=item_in_batch,
                            global_item_index=global_idx,
                            task_run_id=trid,
                        )
                    )
            return PrefectFutureList(item_futures)  # type: ignore[abstract]

        # Non-batched srun map
        logger.info("Submitting %s tasks via srun", map_length)
        wrapped_calls = [
            build_array_callable(
                task=task,
                index=i,
                iterable_params=iterable_params,
                static_params=static_params,
                task_run_id=task_run_ids[i],
                context=context,
                env=env,
            )
            for i in range(map_length)
        ]
        futures = self._backend.submit_many(wrapped_calls, task_run_ids)
        return PrefectFutureList(futures)  # type: ignore[abstract]
