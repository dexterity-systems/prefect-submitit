"""Tests for prefect_submitit.utils.

Tests utility functions directly (not through SlurmTaskRunner wrappers).
"""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from prefect_submitit.constants import DEFAULT_MAX_ARRAY_SIZE, ExecutionMode
from prefect_submitit.utils import (
    get_cluster_max_array_size,
    parse_time_to_minutes,
    partition_parameters,
    validate_iterable_lengths,
)


class TestParseTimeToMinutes:
    """Tests for parse_time_to_minutes."""

    def test_hhmmss_format(self):
        assert parse_time_to_minutes("01:30:00") == 90
        assert parse_time_to_minutes("02:00:00") == 120
        assert parse_time_to_minutes("00:10:30") == 10

    def test_mmss_format(self):
        assert parse_time_to_minutes("30:00") == 30
        assert parse_time_to_minutes("10:30") == 10

    def test_plain_minutes(self):
        assert parse_time_to_minutes("60") == 60
        assert parse_time_to_minutes("0") == 0

    def test_seconds_truncated(self):
        assert parse_time_to_minutes("00:00:59") == 0
        assert parse_time_to_minutes("00:01:59") == 1


class TestPartitionParameters:
    """Tests for partition_parameters."""

    def test_simple_iterables(self):
        params = {"x": [1, 2, 3], "y": "static"}
        iterable, static = partition_parameters(params)

        assert iterable == {"x": [1, 2, 3]}
        assert static == {"y": "static"}

    def test_multiple_iterables(self):
        params = {"a": [1, 2], "b": [3, 4], "c": "static"}
        iterable, static = partition_parameters(params)

        assert iterable == {"a": [1, 2], "b": [3, 4]}
        assert static == {"c": "static"}

    def test_unmapped_annotation_keeps_as_static(self):
        from prefect.utilities.annotations import unmapped

        params = {"x": [1, 2, 3], "config": unmapped([4, 5, 6])}
        iterable, static = partition_parameters(params)

        assert iterable == {"x": [1, 2, 3]}
        assert "config" in static
        assert isinstance(static["config"], unmapped)

    def test_quote_annotation_unwrapped(self):
        from prefect.utilities.annotations import quote

        params = {"x": quote([1, 2, 3])}
        iterable, static = partition_parameters(params)

        assert iterable == {"x": [1, 2, 3]}

    def test_allow_failure_annotation_unwrapped(self):
        from prefect.utilities.annotations import allow_failure

        params = {"x": allow_failure([1, 2, 3])}
        iterable, static = partition_parameters(params)

        assert iterable == {"x": [1, 2, 3]}

    def test_dict_treated_as_static(self):
        params = {"x": [1, 2], "config": {"key": "value"}}
        iterable, static = partition_parameters(params)

        assert iterable == {"x": [1, 2]}
        assert static == {"config": {"key": "value"}}

    def test_string_treated_as_static(self):
        params = {"x": [1, 2], "name": "hello"}
        iterable, static = partition_parameters(params)

        assert iterable == {"x": [1, 2]}
        assert static == {"name": "hello"}

    def test_bytes_treated_as_static(self):
        params = {"x": [1, 2], "data": b"bytes"}
        iterable, static = partition_parameters(params)

        assert iterable == {"x": [1, 2]}
        assert static == {"data": b"bytes"}

    def test_all_static(self):
        params = {"a": "str", "b": 42, "c": {"d": 1}}
        iterable, static = partition_parameters(params)

        assert iterable == {}
        assert static == params

    def test_tuple_treated_as_iterable(self):
        params = {"x": (1, 2, 3)}
        iterable, static = partition_parameters(params)

        assert iterable == {"x": [1, 2, 3]}


class TestValidateIterableLengths:
    """Tests for validate_iterable_lengths."""

    def test_valid_same_lengths(self):
        iterable_params = {"a": [1, 2, 3], "b": [4, 5, 6]}
        assert validate_iterable_lengths(iterable_params) == 3

    def test_single_iterable(self):
        assert validate_iterable_lengths({"a": [1, 2]}) == 2

    def test_empty_iterables_raises(self):
        with pytest.raises(ValueError, match="Empty iterable"):
            validate_iterable_lengths({"a": []})

    def test_no_iterables_raises(self):
        with pytest.raises(ValueError, match="No iterable parameters"):
            validate_iterable_lengths({})

    def test_mismatched_lengths_raises(self):
        with pytest.raises(ValueError, match="mismatched lengths"):
            validate_iterable_lengths({"a": [1, 2, 3], "b": [4, 5]})

    def test_partial_empty_raises(self):
        with pytest.raises(ValueError, match="Empty iterable"):
            validate_iterable_lengths({"a": [1, 2], "b": []})


class TestGetClusterMaxArraySize:
    """Tests for get_cluster_max_array_size."""

    @patch("prefect_submitit.utils.subprocess.run")
    def test_detects_cluster_limit(self, mock_run, mock_runner):
        mock_run.return_value = MagicMock(
            returncode=0, stdout="MaxArraySize = 2000\nOtherConfig = 100"
        )
        result = get_cluster_max_array_size(mock_runner)

        assert result == 2000
        assert mock_runner._cached_max_array_size == 2000

    @patch("prefect_submitit.utils.subprocess.run")
    def test_caches_result(self, mock_run, mock_runner):
        mock_run.return_value = MagicMock(returncode=0, stdout="MaxArraySize = 1500")

        get_cluster_max_array_size(mock_runner)
        mock_runner._cached_max_array_size = 1500
        get_cluster_max_array_size(mock_runner)

        mock_run.assert_called_once()

    @patch("prefect_submitit.utils.subprocess.run")
    def test_fallback_on_scontrol_failure(self, mock_run, mock_runner):
        mock_run.return_value = MagicMock(returncode=1, stdout="")

        result = get_cluster_max_array_size(mock_runner)

        assert result == DEFAULT_MAX_ARRAY_SIZE

    @patch("prefect_submitit.utils.subprocess.run")
    def test_fallback_on_file_not_found(self, mock_run, mock_runner):
        mock_run.side_effect = FileNotFoundError("scontrol not found")

        result = get_cluster_max_array_size(mock_runner)

        assert result == DEFAULT_MAX_ARRAY_SIZE

    @patch("prefect_submitit.utils.subprocess.run")
    def test_fallback_on_timeout(self, mock_run, mock_runner):
        mock_run.side_effect = subprocess.TimeoutExpired("scontrol", 10)

        result = get_cluster_max_array_size(mock_runner)

        assert result == DEFAULT_MAX_ARRAY_SIZE

    def test_explicit_max_array_size_takes_precedence(self, mock_runner):
        mock_runner.max_array_size = 50

        result = get_cluster_max_array_size(mock_runner)

        assert result == 50

    def test_cached_value_used(self, mock_runner):
        mock_runner._cached_max_array_size = 777

        result = get_cluster_max_array_size(mock_runner)

        assert result == 777

    def test_local_backend_uses_default(self, mock_runner):
        mock_runner.execution_mode = ExecutionMode.LOCAL

        result = get_cluster_max_array_size(mock_runner)

        assert result == DEFAULT_MAX_ARRAY_SIZE
        assert mock_runner._cached_max_array_size == DEFAULT_MAX_ARRAY_SIZE

    @patch("prefect_submitit.utils.subprocess.run")
    def test_no_match_in_scontrol_output(self, mock_run, mock_runner):
        mock_run.return_value = MagicMock(returncode=0, stdout="SomeOtherConfig = 999")

        result = get_cluster_max_array_size(mock_runner)

        assert result == DEFAULT_MAX_ARRAY_SIZE
