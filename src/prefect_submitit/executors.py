"""SLURM-side execution functions.

These functions are serialized via cloudpickle and executed on SLURM compute nodes.
They are NOT meant to be called directly - they are entry points for SLURM jobs.
"""

from __future__ import annotations

import logging
import os
from contextlib import suppress
from typing import Any


def build_slurm_task_name() -> str | None:
    """Build a Prefect task run name from SLURM environment variables.

    Priority: SLURM_ARRAY_JOB_ID+TASK_ID > SLURM_JOB_ID+STEP_ID > SLURM_JOB_ID.

    Returns:
        A name like ``slurm-123_4`` (array), ``slurm-123.0`` (step),
        ``slurm-123`` (plain job), or ``None`` if no SLURM env is set.
    """
    array_job = os.environ.get("SLURM_ARRAY_JOB_ID")
    array_task = os.environ.get("SLURM_ARRAY_TASK_ID")
    job_id = os.environ.get("SLURM_JOB_ID")
    step_id = os.environ.get("SLURM_STEP_ID")

    if array_job and array_task:
        return f"slurm-{array_job}_{array_task}"
    if job_id and step_id:
        return f"slurm-{job_id}.{step_id}"
    if job_id:
        return f"slurm-{job_id}"
    return None


def run_task_in_slurm(
    *args: Any,
    env: dict[str, str] | None = None,
    **kwargs: Any,
) -> Any:
    """Run a Prefect task inside a SLURM job.

    This function is submitted to SLURM and executes the task using Prefect's
    task runner, ensuring proper task run tracking and state management.

    The task run name is set to include the SLURM job ID for easy correlation
    between Prefect UI and SLURM queue (squeue/sacct).
    """
    from prefect.context import hydrated_context
    from prefect.task_engine import run_task_async, run_task_sync

    os.environ.update(env or {})
    context = kwargs.pop("context", None)
    task_run_name = build_slurm_task_name()

    with hydrated_context(context):
        task = kwargs.get("task")

        # Create task with custom name if we have SLURM job info
        if task_run_name and task:
            # Override the task's task_run_name for this execution
            task = task.with_options(task_run_name=task_run_name)
            kwargs["task"] = task

        if task and task.isasync:
            import asyncio

            return asyncio.run(run_task_async(*args, **kwargs))
        return run_task_sync(*args, **kwargs)


def run_batch_in_slurm(
    env: dict[str, str] | None = None,
    **kwargs: Any,
) -> Any:
    """Execute a batch of items inside a SLURM job as a single Prefect task run.

    This is the SLURM-side entry point for batched execution. It creates a task
    run for visibility in the Prefect UI, then executes the batch processing
    within that context.

    The parameters contain:
    - _batch_items: List of items to process
    - _batch_param_name: Original parameter name for the items
    - Other static parameters

    Args:
        env: Environment variables to set.
        **kwargs: Keyword args including task, task_run_id, parameters, context.

    Returns:
        List of results from processing each item in the batch.
    """
    from prefect.context import FlowRunContext, hydrated_context
    from prefect.logging.loggers import get_run_logger
    from prefect.states import Completed, Failed, Running

    os.environ.update(env or {})

    context = kwargs.pop("context", None)
    task = kwargs.get("task")
    task_run_id = kwargs.get("task_run_id")
    parameters = kwargs.get("parameters", {})

    # Extract batch-specific parameters
    batch_items = parameters.pop("_batch_items")
    param_name = parameters.pop("_batch_param_name")
    static_params = parameters  # Remaining are static params

    if task is None or task_run_id is None:
        msg = "task and task_run_id are required in batch kwargs"
        raise ValueError(msg)
    original_fn = task.fn
    logger = logging.getLogger("prefect.slurm.batch")

    with hydrated_context(context):
        # Get flow run context for logging
        flow_run_context = FlowRunContext.get()

        task_run_name = build_slurm_task_name()

        # Create a minimal task run for tracking
        # We use the task's create_local_run to register with Prefect
        try:
            from prefect.client.orchestration import get_client

            client = get_client(sync_client=True)

            # Create the task run in Prefect's backend
            task_run = client.create_task_run(
                task=task,
                flow_run_id=(
                    flow_run_context.flow_run.id
                    if flow_run_context and flow_run_context.flow_run
                    else None
                ),
                dynamic_key=str(task_run_id),
                id=task_run_id,
                name=task_run_name,
                state=Running(),
            )

            if flow_run_context:
                run_logger = get_run_logger(flow_run_context)
                run_logger.debug(
                    "Batch task run %s started with %s items",
                    task_run_name or task_run_id,
                    len(batch_items),
                )

        except Exception as e:
            logger.warning("Could not create task run in Prefect: %s", e)
            task_run = None

        # Process the batch
        results = []
        try:
            for i, item in enumerate(batch_items):
                try:
                    logger.debug(
                        "Processing item %s/%s: %s",
                        i + 1,
                        len(batch_items),
                        _item_repr(item),
                    )
                    # Call original function with the item and static params
                    call_params = {param_name: item}
                    call_params.update(static_params)
                    result = original_fn(**call_params)
                    results.append(result)
                    logger.debug(
                        "Item %s/%s completed successfully",
                        i + 1,
                        len(batch_items),
                    )
                except Exception as e:
                    logger.error(
                        "Item %s/%s failed: %s: %s\nItem details: %s",
                        i + 1,
                        len(batch_items),
                        type(e).__name__,
                        e,
                        _item_repr(item),
                    )
                    results.append(
                        {
                            "success": False,
                            "error": f"{type(e).__name__}: {e}",
                            "item_count": 1,
                            "execution_run_ids": [],
                        }
                    )
                    continue

            # Mark task run as completed
            if task_run:
                try:
                    client.set_task_run_state(
                        task_run_id=task_run_id,
                        state=Completed(),
                    )
                except Exception as e:
                    logger.warning("Could not update task run state: %s", e)

            return results

        except Exception as e:
            # Safety net: mark task run as failed, return partial results
            if task_run:
                with suppress(Exception):
                    client.set_task_run_state(
                        task_run_id=task_run_id,
                        state=Failed(message=str(e)),
                    )
            # Return whatever results we have plus a failure for the batch
            logger.error(
                "Batch processing failed unexpectedly: %s: %s",
                type(e).__name__,
                e,
            )
            results.append(
                {
                    "success": False,
                    "error": f"{type(e).__name__}: {e}",
                    "item_count": 1,
                    "execution_run_ids": [],
                }
            )
            return results


def _item_repr(item: Any, max_len: int = 100) -> str:
    """Safe string representation of an item for logging.

    Args:
        item: The item to represent.
        max_len: Maximum length before truncation.

    Returns:
        String representation, truncated if needed.
    """
    try:
        r = repr(item)
        return r if len(r) <= max_len else r[:max_len] + "..."
    except Exception:
        return f"<{type(item).__name__}>"
