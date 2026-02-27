"""Tests for prefect_submitit.constants."""

from __future__ import annotations

import pytest

from prefect_submitit.constants import (
    DEFAULT_MAX_ARRAY_SIZE,
    DEFAULT_POLL_TIME_MULTIPLIER,
    ExecutionMode,
)


class TestExecutionMode:
    """Tests for the ExecutionMode enum."""

    def test_slurm_value(self):
        assert ExecutionMode.SLURM == "slurm"

    def test_local_value(self):
        assert ExecutionMode.LOCAL == "local"

    def test_is_str_enum(self):
        assert isinstance(ExecutionMode.SLURM, str)
        assert isinstance(ExecutionMode.LOCAL, str)

    def test_from_string(self):
        assert ExecutionMode("slurm") == ExecutionMode.SLURM
        assert ExecutionMode("local") == ExecutionMode.LOCAL

    def test_invalid_value_raises(self):
        with pytest.raises(ValueError, match="is not a valid ExecutionMode"):
            ExecutionMode("invalid")


class TestDefaultConstants:
    """Tests for module-level constants."""

    def test_default_max_array_size(self):
        assert DEFAULT_MAX_ARRAY_SIZE == 1000

    def test_default_poll_time_multiplier(self):
        assert DEFAULT_POLL_TIME_MULTIPLIER == 2
