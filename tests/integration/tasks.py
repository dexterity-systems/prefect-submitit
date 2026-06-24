"""Module-level @task definitions for integration tests.

Defined at module level (not inside test functions) so cloudpickle serializes
them by reference, avoiding closure-capture issues on compute nodes.
"""

from __future__ import annotations

from prefect import task


@task
def add(a: int, b: int) -> int:
    return a + b


@task
async def async_add(a: int, b: int) -> int:
    return a + b


@task
def identity(x):
    """Return input unchanged. For testing serialization."""
    return x


@task
def sleep_and_return(seconds: float) -> float:
    import time

    time.sleep(seconds)
    return seconds


@task
def get_env_var(name: str) -> str | None:
    import os

    return os.environ.get(name)


@task
def get_flow_run_id() -> str:
    from prefect.context import FlowRunContext

    ctx = FlowRunContext.get()
    return str(ctx.flow_run.id) if ctx and ctx.flow_run else "no-context"


@task
def fail_with(error_type: str, message: str):
    import builtins

    exc_class = getattr(builtins, error_type, ValueError)
    raise exc_class(message)


@task
def conditional_fail(x: int, fail_on: int = -1):
    """Return x * 10, unless x == fail_on, then raise."""
    if x == fail_on:
        msg = f"intentional failure on {x}"
        raise ValueError(msg)
    return x * 10


@task
def print_marker(marker: str) -> str:
    print(marker)
    return marker


@task
def return_complex() -> dict:
    """Return a nested structure with various types."""
    return {
        "string": "hello",
        "number": 42,
        "float": 3.14,
        "nested": {"a": [1, 2, 3], "b": {"c": True}},
        "list": [1, "two", 3.0, None],
        "none": None,
    }


@task
def receive_complex(data: dict) -> str:
    """Accept complex input and return a summary."""
    return f"keys={sorted(data.keys())},len={len(data)}"


@task
def return_unpicklable():
    """Return something that can't be pickled (a generator)."""

    def gen():
        yield 1

    return gen()


@task
def get_slurm_cpus_per_task() -> str | None:
    """Return SLURM_CPUS_PER_TASK from the job environment."""
    import os

    return os.environ.get("SLURM_CPUS_PER_TASK")
