"""Submission helpers for `SlurmTaskRunner` map/array workflows."""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable
from typing import Any

import submitit
from prefect._internal.uuid7 import uuid7
from prefect.context import serialize_context
from prefect.settings.context import get_current_settings
from prefect.utilities.callables import cloudpickle_wrapped_call
from prefect.utilities.engine import resolve_inputs_sync

from prefect_submitit.constants import (
    DEFAULT_POLL_TIME_MULTIPLIER,
)
from prefect_submitit.executors import (
    run_batch_in_slurm,
    run_task_in_slurm,
)
from prefect_submitit.futures import (
    SlurmArrayPrefectFuture,
    SlurmBatchedItemFuture,
)
from prefect_submitit.utils import get_cluster_max_array_size


def build_task_callable(
    task: Any,
    task_run_id: uuid.UUID,
    parameters: dict[str, Any],
    context: dict[str, Any],
    env: dict[str, str],
    dependencies: dict[str, Any] | None = None,
) -> Callable[..., Any]:
    """Build a cloudpickle-wrapped callable for a single task submission."""
    submit_kwargs: dict[str, Any] = {
        "task": task,
        "task_run_id": task_run_id,
        "parameters": parameters,
        "wait_for": None,
        "return_type": "state",
        "dependencies": dependencies,
        "context": context,
    }
    result: Callable[..., Any] = cloudpickle_wrapped_call(
        run_task_in_slurm, env=env, **submit_kwargs
    )
    return result


def build_array_callable(
    task: Any,
    index: int,
    iterable_params: dict[str, list[Any]],
    static_params: dict[str, Any],
    task_run_id: uuid.UUID,
    context: dict[str, Any],
    env: dict[str, str],
) -> Callable[..., Any]:
    """Build a cloudpickle-wrapped callable for one array task."""
    params = {key: values[index] for key, values in iterable_params.items()}
    params.update(static_params)
    resolved_params = resolve_inputs_sync(params, return_data=True, max_depth=-1)
    submit_kwargs = {
        "task": task,
        "task_run_id": task_run_id,
        "parameters": resolved_params,
        "wait_for": None,
        "return_type": "state",
        "dependencies": None,
        "context": context,
    }
    result: Callable[..., Any] = cloudpickle_wrapped_call(
        run_task_in_slurm, env=env, **submit_kwargs
    )
    return result


def batch_items(runner: Any, items: list[Any]) -> list[list[Any]]:
    """Group items into batches of units_per_worker."""
    return [
        items[i : i + runner.units_per_worker]
        for i in range(0, len(items), runner.units_per_worker)
    ]


def build_batch_callable(
    task: Any,
    task_run_id: uuid.UUID,
    batch: list[Any],
    param_name: str,
    static_params: dict[str, Any],
    context: dict[str, Any],
    env: dict[str, str],
) -> Callable[..., Any]:
    """Build a callable that processes one batch in a single task run."""
    params = {
        "_batch_items": batch,
        "_batch_param_name": param_name,
        **static_params,
    }
    resolved_params = resolve_inputs_sync(params, return_data=True, max_depth=-1)
    submit_kwargs = {
        "task": task,
        "task_run_id": task_run_id,
        "parameters": resolved_params,
        "wait_for": None,
        "return_type": "state",
        "dependencies": None,
        "context": context,
    }
    result: Callable[..., Any] = cloudpickle_wrapped_call(
        run_batch_in_slurm, env=env, **submit_kwargs
    )
    return result


def submit_batch_array_chunk(
    runner: Any,
    wrapped_calls: list[Callable[..., Any]],
    task_run_ids: list[uuid.UUID],
    array_size: int,
) -> list[SlurmArrayPrefectFuture]:
    """Submit a chunk of wrapped callables as one SLURM array."""
    jobs: list[submitit.Job[Any]] = []
    with runner._executor.batch():
        for wrapped_call in wrapped_calls:
            jobs.append(runner._executor.submit(wrapped_call))

    array_job_id = jobs[0].job_id.split("_")[0] if jobs else ""
    max_poll = runner.max_poll_time or (
        runner._parse_time_to_minutes(runner.time_limit)
        * 60
        * DEFAULT_POLL_TIME_MULTIPLIER
    )
    return [
        SlurmArrayPrefectFuture(
            job=jobs[i],
            task_run_id=task_run_ids[i],
            poll_interval=runner.poll_interval,
            max_poll_time=max_poll,
            array_job_id=array_job_id,
            array_task_index=i,
            array_size=array_size,
            fail_on_error=runner.fail_on_error,
        )
        for i in range(array_size)
    ]


def submit_single_job_array(
    runner: Any,
    task: Any,
    iterable_params: dict[str, list[Any]],
    static_params: dict[str, Any],
    map_length: int,
    task_run_ids: list[uuid.UUID],
    context: dict[str, Any],
    env: dict[str, str],
) -> list[SlurmArrayPrefectFuture]:
    """Submit tasks as a single SLURM job array."""
    wrapped_calls = [
        build_array_callable(
            task, i, iterable_params, static_params, task_run_ids[i], context, env
        )
        for i in range(map_length)
    ]
    return submit_batch_array_chunk(runner, wrapped_calls, task_run_ids, map_length)


def submit_job_array(
    runner: Any,
    task: Any,
    iterable_params: dict[str, list[Any]],
    static_params: dict[str, Any],
    map_length: int,
) -> list[SlurmArrayPrefectFuture]:
    """Submit tasks as job array(s), chunking if necessary."""
    context = serialize_context()
    env = (
        get_current_settings().to_environment_variables(exclude_unset=True) | os.environ
    )
    task_run_ids = [uuid7() for _ in range(map_length)]

    max_array_size = get_cluster_max_array_size(runner)
    if map_length <= max_array_size:
        return submit_single_job_array(
            runner,
            task,
            iterable_params,
            static_params,
            map_length,
            task_run_ids,
            context,
            env,
        )

    num_chunks = (map_length + max_array_size - 1) // max_array_size
    runner.logger.info(
        "Array size %s exceeds cluster limit %s, splitting into %s chunks",
        map_length,
        max_array_size,
        num_chunks,
    )

    all_futures: list[SlurmArrayPrefectFuture] = []
    for chunk_idx in range(num_chunks):
        chunk_start = chunk_idx * max_array_size
        chunk_end = min(chunk_start + max_array_size, map_length)
        chunk_length = chunk_end - chunk_start

        chunk_iterable_params = {
            k: v[chunk_start:chunk_end] for k, v in iterable_params.items()
        }
        chunk_task_run_ids = task_run_ids[chunk_start:chunk_end]
        chunk_futures = submit_single_job_array(
            runner,
            task,
            chunk_iterable_params,
            static_params,
            chunk_length,
            chunk_task_run_ids,
            context,
            env,
        )
        all_futures.extend(chunk_futures)

    return all_futures


def submit_batched_job_array(
    runner: Any,
    task: Any,
    items: list[Any],
    param_name: str,
    static_params: dict[str, Any],
) -> list[SlurmBatchedItemFuture]:
    """Submit batched items as one or more SLURM arrays."""
    batches = batch_items(runner, items)
    num_batches = len(batches)
    context = serialize_context()
    env = (
        get_current_settings().to_environment_variables(exclude_unset=True) | os.environ
    )
    batch_task_run_ids = [uuid7() for _ in range(num_batches)]

    wrapped_calls = [
        build_batch_callable(
            task=task,
            task_run_id=batch_task_run_ids[i],
            batch=batch,
            param_name=param_name,
            static_params=static_params,
            context=context,
            env=env,
        )
        for i, batch in enumerate(batches)
    ]

    max_array_size = get_cluster_max_array_size(runner)
    job_futures: list[SlurmArrayPrefectFuture] = []
    if num_batches <= max_array_size:
        job_futures = submit_batch_array_chunk(
            runner, wrapped_calls, batch_task_run_ids, num_batches
        )
    else:
        num_chunks = (num_batches + max_array_size - 1) // max_array_size
        runner.logger.info(
            "Batch count %s exceeds cluster limit %s, splitting into %s array submissions",
            num_batches,
            max_array_size,
            num_chunks,
        )
        for chunk_idx in range(num_chunks):
            chunk_start = chunk_idx * max_array_size
            chunk_end = min(chunk_start + max_array_size, num_batches)
            chunk_length = chunk_end - chunk_start
            chunk_futures = submit_batch_array_chunk(
                runner,
                wrapped_calls[chunk_start:chunk_end],
                batch_task_run_ids[chunk_start:chunk_end],
                chunk_length,
            )
            job_futures.extend(chunk_futures)

    item_futures: list[SlurmBatchedItemFuture] = []
    global_item_index = 0
    for job_idx, batch in enumerate(batches):
        job_future = job_futures[job_idx]
        for item_idx_in_job in range(len(batch)):
            item_futures.append(
                SlurmBatchedItemFuture(
                    slurm_job_future=job_future,
                    item_index_in_job=item_idx_in_job,
                    global_item_index=global_item_index,
                    task_run_id=uuid7(),
                )
            )
            global_item_index += 1
    return item_futures
