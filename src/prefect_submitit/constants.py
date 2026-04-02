"""Constants used by the SLURM task runner implementation."""

from __future__ import annotations

from enum import StrEnum

DEFAULT_MAX_ARRAY_SIZE = 1000
DEFAULT_POLL_TIME_MULTIPLIER = 2


class ExecutionMode(StrEnum):
    """How the SLURM task runner dispatches work."""

    SLURM = "slurm"
    LOCAL = "local"
    SRUN = "srun"
