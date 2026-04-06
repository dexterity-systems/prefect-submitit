from __future__ import annotations
from typing import Any
from prefect import task as prefect_task


def task(
    *args: Any,
    slurm_kwargs: dict[str, Any] | None = None,
    **prefect_kwargs: Any,
) -> Any:
    """Task decorator that attaches per-task SLURM resource overrides.

    SLURM parameters specified here override the SlurmTaskRunner defaults for
    this task only. All other Prefect task options are forwarded to @task.

    Args:
        slurm_kwargs: SLURM parameters to override.
            These are submitit executor parameters, not SlurmTaskRunner parameters
            (e.g. use timeout_min=1, not time_limit="00:01:00").
        **prefect_kwargs: Forwarded to Prefect's @task decorator.

    Example:
        @task(slurm_kwargs={"slurm_nodes": 2, "slurm_ntasks_per_node": 4})
        def my_task(x: int) -> int:
            ...
    """

    def decorator(fn: Any) -> Any:
        fn._slurm_kwargs = slurm_kwargs or {}
        return prefect_task(fn, **prefect_kwargs)

    if args:
        # Called as @task without parentheses
        return decorator(args[0])
    return decorator
