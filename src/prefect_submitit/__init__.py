"""SLURM TaskRunner implementation package."""

from __future__ import annotations

from prefect_submitit.constants import ExecutionMode
from prefect_submitit.futures import (
    SlurmArrayPrefectFuture,
    SlurmBatchedItemFuture,
    SlurmJobFailed,
    SlurmPrefectFuture,
    SrunPrefectFuture,
)
from prefect_submitit.runner import SlurmTaskRunner

__all__ = [
    "ExecutionMode",
    "SlurmArrayPrefectFuture",
    "SlurmBatchedItemFuture",
    "SlurmJobFailed",
    "SlurmPrefectFuture",
    "SlurmTaskRunner",
    "SrunPrefectFuture",
]
