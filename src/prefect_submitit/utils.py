"""Utility helpers for `SlurmTaskRunner`."""

from __future__ import annotations

import re
import subprocess
from typing import Any

from prefect.utilities.annotations import allow_failure, quote, unmapped

from prefect_submitit.constants import (
    DEFAULT_MAX_ARRAY_SIZE,
    ExecutionMode,
)


def parse_time_to_minutes(time_str: str) -> int:
    """Parse HH:MM:SS or MM:SS or MM to minutes."""
    parts = time_str.split(":")
    if len(parts) == 3:
        return int(parts[0]) * 60 + int(parts[1]) + int(parts[2]) // 60
    if len(parts) == 2:
        return int(parts[0]) + int(parts[1]) // 60
    return int(parts[0])


def partition_parameters(
    parameters: dict[str, Any],
) -> tuple[dict[str, list[Any]], dict[str, Any]]:
    """Separate iterable parameters from static ones, handling Prefect wrappers."""
    iterable_params: dict[str, list[Any]] = {}
    static_params: dict[str, Any] = {}

    for key, value in parameters.items():
        unwrapped = value
        is_unmapped = False

        if isinstance(value, unmapped):
            unwrapped = value.value
            is_unmapped = True
        elif isinstance(value, quote | allow_failure):
            unwrapped = value.value

        is_iterable = hasattr(unwrapped, "__iter__") and not isinstance(
            unwrapped, str | bytes | dict
        )

        if is_iterable and not is_unmapped:
            iterable_params[key] = list(unwrapped)
        else:
            static_params[key] = value

    return iterable_params, static_params


def validate_iterable_lengths(iterable_params: dict[str, list[Any]]) -> int:
    """Ensure all iterables have same length and are non-empty."""
    if not iterable_params:
        msg = "No iterable parameters found for map()"
        raise ValueError(msg)

    lengths = {k: len(v) for k, v in iterable_params.items()}
    if any(length == 0 for length in lengths.values()):
        msg = (
            f"Empty iterable passed to map(). Lengths: {lengths}. "
            "If intentional, guard with: `if items: task.map(items)`"
        )
        raise ValueError(msg)

    unique_lengths = set(lengths.values())
    if len(unique_lengths) > 1:
        msg = f"Iterable parameters have mismatched lengths: {lengths}"
        raise ValueError(msg)

    return unique_lengths.pop()


def get_cluster_max_array_size(runner: Any) -> int:
    """Get the maximum job array size with fallback and caching."""
    if runner.max_array_size is not None:
        return int(runner.max_array_size)
    if runner._cached_max_array_size is not None:
        return int(runner._cached_max_array_size)
    if runner.execution_mode == ExecutionMode.LOCAL:
        runner._cached_max_array_size = DEFAULT_MAX_ARRAY_SIZE
        return int(runner._cached_max_array_size)

    try:
        result = subprocess.run(
            ["scontrol", "show", "config"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if result.returncode == 0:
            match = re.search(r"MaxArraySize\s*=\s*(\d+)", result.stdout)
            if match:
                runner._cached_max_array_size = int(match.group(1))
                runner.logger.debug(
                    "Detected cluster MaxArraySize: %s",
                    runner._cached_max_array_size,
                )
                return int(runner._cached_max_array_size)
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        runner.logger.warning("Could not detect MaxArraySize: %s", e)

    runner._cached_max_array_size = DEFAULT_MAX_ARRAY_SIZE
    runner.logger.warning(
        "Using fallback MaxArraySize: %s", runner._cached_max_array_size
    )
    return int(runner._cached_max_array_size)
