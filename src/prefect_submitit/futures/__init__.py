"""Public future exports for the SLURM task runner."""

from __future__ import annotations

from prefect_submitit.futures.array import (
    SlurmArrayPrefectFuture,
)
from prefect_submitit.futures.base import (
    SlurmJobFailed,
    SlurmPrefectFuture,
)
from prefect_submitit.futures.batched import (
    SlurmBatchedItemFuture,
)
from prefect_submitit.futures.srun import (
    SrunPrefectFuture,
)

__all__ = [
    "SlurmArrayPrefectFuture",
    "SlurmBatchedItemFuture",
    "SlurmJobFailed",
    "SlurmPrefectFuture",
    "SrunPrefectFuture",
]
