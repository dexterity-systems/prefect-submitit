"""Submit a Prefect task to SLURM and verify the result.

Usage (inside the Docker SLURM container):

    pip install -e .                          # first time only
    prefect-server start --bg --port 4200     # first time per session
    python examples/slurm_submit_and_run.py
"""

from __future__ import annotations

import os

from prefect import flow, task
from prefect_submitit import SlurmTaskRunner

PARTITION = os.environ.get("SLURM_TEST_PARTITION", "cpu")


@task
def add(a: int, b: int) -> int:
    """Add two numbers on a SLURM worker."""
    return a + b


@flow(
    task_runner=SlurmTaskRunner(
        execution_mode="slurm",
        partition=PARTITION,
        slurm_name="smoke_test",
    )
)
def smoke_test():
    future = add.submit(2, 3)
    result = future.result()

    print(f"Job ID:  {future.slurm_job_id}")
    print(f"Result:  {result}")

    stdout, stderr = future.logs()
    if stdout:
        print(f"stdout:  {stdout!r}")
    if stderr:
        print(f"stderr:  {stderr!r}")

    assert result == 5, f"Expected 5, got {result}"
    print("OK")


if __name__ == "__main__":
    smoke_test()
