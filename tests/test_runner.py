"""Tests for prefect_submitit.runner.

Tests SlurmTaskRunner class: initialization, context management,
submit, map, backend selection, and local integration.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
from prefect.futures import PrefectFutureList

from prefect_submitit import (
    ExecutionMode,
    SlurmArrayPrefectFuture,
    SlurmBatchedItemFuture,
    SlurmPrefectFuture,
    SlurmTaskRunner,
)

# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestSlurmTaskRunnerInit:
    """Tests for SlurmTaskRunner.__init__."""

    def test_defaults(self):
        runner = SlurmTaskRunner()

        assert runner.partition == "cpu"
        assert runner.time_limit == "01:00:00"
        assert runner.mem_gb == 4
        assert runner.gpus_per_node == 0
        assert runner.slurm_array_parallelism == 1000
        assert runner.poll_interval == 5.0
        assert runner.log_folder == "slurm_logs"
        assert runner.execution_mode == ExecutionMode.SLURM
        assert runner.max_array_size is None
        assert runner.fail_on_error is True
        assert runner.units_per_worker == 1

    def test_custom_parameters(self):
        runner = SlurmTaskRunner(
            partition="gpu",
            time_limit="02:30:00",
            mem_gb=16,
            gpus_per_node=2,
            slurm_array_parallelism=50,
            poll_interval=10.0,
            log_folder="custom_logs",
            constraint="a100",
        )

        assert runner.partition == "gpu"
        assert runner.time_limit == "02:30:00"
        assert runner.mem_gb == 16
        assert runner.gpus_per_node == 2
        assert runner.slurm_array_parallelism == 50
        assert runner.poll_interval == 10.0
        assert runner.log_folder == "custom_logs"
        assert runner.slurm_kwargs == {"constraint": "a100"}

    def test_invalid_units_per_worker_zero(self):
        with pytest.raises(ValueError, match="units_per_worker must be >= 1"):
            SlurmTaskRunner(units_per_worker=0)

    def test_invalid_units_per_worker_negative(self):
        with pytest.raises(ValueError, match="units_per_worker must be >= 1"):
            SlurmTaskRunner(units_per_worker=-1)

    def test_custom_units_per_worker(self):
        runner = SlurmTaskRunner(units_per_worker=10)
        assert runner.units_per_worker == 10

    def test_custom_fail_on_error(self):
        runner = SlurmTaskRunner(fail_on_error=False)
        assert runner.fail_on_error is False

    def test_custom_max_array_size(self):
        runner = SlurmTaskRunner(max_array_size=42)
        assert runner.max_array_size == 42


# ---------------------------------------------------------------------------
# Duplicate
# ---------------------------------------------------------------------------


class TestSlurmTaskRunnerDuplicate:
    """Tests for SlurmTaskRunner.duplicate."""

    def test_creates_identical_copy(self):
        original = SlurmTaskRunner(
            partition="gpu",
            time_limit="04:00:00",
            mem_gb=32,
            gpus_per_node=4,
            constraint="v100",
        )
        duplicate = original.duplicate()

        assert duplicate.partition == original.partition
        assert duplicate.time_limit == original.time_limit
        assert duplicate.mem_gb == original.mem_gb
        assert duplicate.gpus_per_node == original.gpus_per_node
        assert duplicate.slurm_kwargs == original.slurm_kwargs
        assert duplicate is not original

    def test_preserves_fail_on_error(self):
        runner = SlurmTaskRunner(fail_on_error=False)
        assert runner.duplicate().fail_on_error is False

    def test_preserves_units_per_worker(self):
        runner = SlurmTaskRunner(units_per_worker=5)
        assert runner.duplicate().units_per_worker == 5

    def test_preserves_backend(self):
        runner = SlurmTaskRunner(execution_mode=ExecutionMode.LOCAL)
        assert runner.duplicate().execution_mode == ExecutionMode.LOCAL

    def test_preserves_max_array_size(self):
        runner = SlurmTaskRunner(max_array_size=42)
        assert runner.duplicate().max_array_size == 42


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


class TestSlurmTaskRunnerContextManager:
    """Tests for __enter__ / __exit__."""

    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_enter_initializes_executor(self, mock_executor_class):
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor

        runner = SlurmTaskRunner(partition="gpu", time_limit="02:00:00", mem_gb=16)

        with runner as r:
            assert r is runner
            assert runner._executor is mock_executor
            mock_executor_class.assert_called_once_with(folder="slurm_logs/%j")
            mock_executor.update_parameters.assert_called_once()

    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_exit_cleans_up_executor(self, mock_executor_class):
        mock_executor_class.return_value = MagicMock()
        runner = SlurmTaskRunner()

        with runner:
            assert runner._executor is not None
        assert runner._executor is None

    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_slurm_gres_excludes_gpus_per_node(self, mock_executor_class):
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor

        runner = SlurmTaskRunner(
            partition="gpu",
            gpus_per_node=1,
            slurm_gres="gpu:a4000:1",
        )

        with runner:
            pass

        call_kwargs = mock_executor.update_parameters.call_args[1]
        assert "gpus_per_node" not in call_kwargs
        assert call_kwargs["slurm_gres"] == "gpu:a4000:1"

    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_gpus_per_node_passed_without_slurm_gres(self, mock_executor_class):
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor

        runner = SlurmTaskRunner(partition="gpu", gpus_per_node=2)

        with runner:
            pass

        call_kwargs = mock_executor.update_parameters.call_args[1]
        assert call_kwargs["gpus_per_node"] == 2

    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_gpus_per_node_zero_omitted(self, mock_executor_class):
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor

        runner = SlurmTaskRunner(partition="cpu")

        with runner:
            pass

        call_kwargs = mock_executor.update_parameters.call_args[1]
        assert "gpus_per_node" not in call_kwargs


# ---------------------------------------------------------------------------
# Submit
# ---------------------------------------------------------------------------


class TestSlurmTaskRunnerSubmit:
    """Tests for SlurmTaskRunner.submit."""

    def test_without_context_raises(self):
        runner = SlurmTaskRunner()
        mock_task = MagicMock()
        mock_task.fn = lambda x: x

        with pytest.raises(RuntimeError, match="context manager"):
            runner.submit(mock_task, {"x": 1})

    @patch("prefect.context.serialize_context")
    @patch("prefect.settings.context.get_current_settings")
    @patch("prefect.utilities.engine.resolve_inputs_sync")
    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_returns_future(
        self,
        mock_executor_class,
        mock_resolve_inputs,
        mock_settings,
        mock_serialize_context,
    ):
        mock_job = MagicMock()
        mock_job.job_id = "12345"
        mock_executor = MagicMock()
        mock_executor.submit.return_value = mock_job
        mock_executor_class.return_value = mock_executor

        mock_resolve_inputs.return_value = {"x": 5}
        mock_serialize_context.return_value = {}
        mock_settings.return_value.to_environment_variables.return_value = {}

        mock_task = MagicMock()
        mock_task.fn = lambda x: x * 2
        mock_task.name = "test_task"
        mock_task.isasync = False

        runner = SlurmTaskRunner()

        with runner:
            future = runner.submit(mock_task, {"x": 5})

        assert isinstance(future, SlurmPrefectFuture)
        assert future.slurm_job_id == "12345"
        assert isinstance(future.task_run_id, UUID)


# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------


class TestSlurmTaskRunnerMap:
    """Tests for SlurmTaskRunner.map (non-batched)."""

    def test_without_context_raises(self):
        runner = SlurmTaskRunner()
        mock_task = MagicMock()

        with pytest.raises(RuntimeError, match="context manager"):
            runner.map(mock_task, {"x": [1, 2, 3]})

    @patch("prefect.context.serialize_context")
    @patch("prefect.settings.context.get_current_settings")
    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_empty_iterable_raises(
        self, mock_executor_class, mock_settings, mock_serialize
    ):
        mock_executor_class.return_value = MagicMock()
        mock_serialize.return_value = {}
        mock_settings.return_value.to_environment_variables.return_value = {}

        runner = SlurmTaskRunner()
        mock_task = MagicMock()
        mock_task.name = "test_task"

        with runner, pytest.raises(ValueError, match="Empty iterable"):
            runner.map(mock_task, {"x": []})

    @patch("prefect.context.serialize_context")
    @patch("prefect.settings.context.get_current_settings")
    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_mismatched_lengths_raises(
        self, mock_executor_class, mock_settings, mock_serialize
    ):
        mock_executor_class.return_value = MagicMock()
        mock_serialize.return_value = {}
        mock_settings.return_value.to_environment_variables.return_value = {}

        runner = SlurmTaskRunner()
        mock_task = MagicMock()
        mock_task.name = "test_task"

        with runner, pytest.raises(ValueError, match="mismatched lengths"):
            runner.map(mock_task, {"x": [1, 2, 3], "y": [1, 2]})

    @patch("prefect.utilities.engine.resolve_inputs_sync")
    @patch("prefect.context.serialize_context")
    @patch("prefect.settings.context.get_current_settings")
    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_returns_future_list(
        self, mock_executor_class, mock_settings, mock_serialize, mock_resolve
    ):
        mock_job = MagicMock()
        mock_job.job_id = "12345_0"
        mock_executor = MagicMock()
        mock_executor.submit.return_value = mock_job
        mock_executor_class.return_value = mock_executor

        mock_serialize.return_value = {}
        mock_settings.return_value.to_environment_variables.return_value = {}
        mock_resolve.side_effect = lambda x, **kwargs: x

        runner = SlurmTaskRunner(max_array_size=1000)
        mock_task = MagicMock()
        mock_task.name = "test_task"
        mock_task.fn = lambda x: x * 2
        mock_task.isasync = False

        with runner:
            futures = runner.map(mock_task, {"x": [1, 2, 3]})

        assert len(futures) == 3
        assert isinstance(futures, PrefectFutureList)
        assert all(isinstance(f, SlurmArrayPrefectFuture) for f in futures)


class TestJobArrayChunking:
    """Tests for job array chunking when array exceeds cluster limit."""

    @patch("prefect.utilities.engine.resolve_inputs_sync")
    @patch("prefect.context.serialize_context")
    @patch("prefect.settings.context.get_current_settings")
    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_chunks_large_arrays(
        self, mock_executor_class, mock_settings, mock_serialize, mock_resolve
    ):
        mock_job = MagicMock()
        mock_job.job_id = "12345_0"
        mock_executor = MagicMock()
        mock_executor.submit.return_value = mock_job
        mock_executor_class.return_value = mock_executor

        mock_serialize.return_value = {}
        mock_settings.return_value.to_environment_variables.return_value = {}
        mock_resolve.side_effect = lambda x, **kwargs: x

        runner = SlurmTaskRunner(max_array_size=3)
        mock_task = MagicMock()
        mock_task.name = "test_task"
        mock_task.fn = lambda x: x
        mock_task.isasync = False

        with runner:
            futures = runner.map(mock_task, {"x": list(range(10))})

        assert len(futures) == 10
        assert mock_executor.batch.call_count == 4  # chunks: 3, 3, 3, 1


# ---------------------------------------------------------------------------
# Map with batching (units_per_worker > 1)
# ---------------------------------------------------------------------------


class TestMapWithBatching:
    """Tests for map() with units_per_worker > 1."""

    @patch("prefect.context.serialize_context")
    @patch("prefect.settings.context.get_current_settings")
    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_multi_arg_raises(self, mock_executor_class, mock_settings, mock_serialize):
        mock_executor_class.return_value = MagicMock()
        mock_serialize.return_value = {}
        mock_settings.return_value.to_environment_variables.return_value = {}

        runner = SlurmTaskRunner(units_per_worker=5)
        mock_task = MagicMock()
        mock_task.name = "test_task"

        with runner:
            with pytest.raises(ValueError, match="Multi-argument map.*not supported"):
                runner.map(mock_task, {"x": [1, 2, 3], "y": [4, 5, 6]})

    @patch("prefect.context.serialize_context")
    @patch("prefect.settings.context.get_current_settings")
    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_returns_item_futures(
        self, mock_executor_class, mock_settings, mock_serialize
    ):
        mock_job = MagicMock()
        mock_job.job_id = "12345_0"
        mock_executor = MagicMock()
        mock_executor.submit.return_value = mock_job
        mock_executor_class.return_value = mock_executor

        mock_serialize.return_value = {}
        mock_settings.return_value.to_environment_variables.return_value = {}

        runner = SlurmTaskRunner(units_per_worker=5, max_array_size=1000)
        mock_task = MagicMock()
        mock_task.name = "test_task"
        mock_task.fn = lambda x: x * 2
        mock_task.isasync = False

        with runner:
            futures = runner.map(mock_task, {"x": list(range(10))})

        assert len(futures) == 10
        assert all(isinstance(f, SlurmBatchedItemFuture) for f in futures)

    @patch("prefect.context.serialize_context")
    @patch("prefect.settings.context.get_current_settings")
    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_handles_remainder(
        self, mock_executor_class, mock_settings, mock_serialize
    ):
        mock_job = MagicMock()
        mock_job.job_id = "12345_0"
        mock_executor = MagicMock()
        mock_executor.submit.return_value = mock_job
        mock_executor_class.return_value = mock_executor

        mock_serialize.return_value = {}
        mock_settings.return_value.to_environment_variables.return_value = {}

        runner = SlurmTaskRunner(units_per_worker=3, max_array_size=1000)
        mock_task = MagicMock()
        mock_task.name = "test_task"
        mock_task.fn = lambda x: x
        mock_task.isasync = False

        with runner:
            futures = runner.map(mock_task, {"x": list(range(7))})

        assert len(futures) == 7
        assert all(isinstance(f, SlurmBatchedItemFuture) for f in futures)

    @patch("prefect.context.serialize_context")
    @patch("prefect.settings.context.get_current_settings")
    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_global_indices(self, mock_executor_class, mock_settings, mock_serialize):
        mock_job = MagicMock()
        mock_job.job_id = "12345_0"
        mock_executor = MagicMock()
        mock_executor.submit.return_value = mock_job
        mock_executor_class.return_value = mock_executor

        mock_serialize.return_value = {}
        mock_settings.return_value.to_environment_variables.return_value = {}

        runner = SlurmTaskRunner(units_per_worker=3, max_array_size=1000)
        mock_task = MagicMock()
        mock_task.name = "test_task"
        mock_task.fn = lambda x: x
        mock_task.isasync = False

        with runner:
            futures = runner.map(mock_task, {"x": list(range(7))})

        for i, future in enumerate(futures):
            assert future.global_item_index == i

    @patch("prefect.context.serialize_context")
    @patch("prefect.settings.context.get_current_settings")
    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_item_index_in_job(
        self, mock_executor_class, mock_settings, mock_serialize
    ):
        mock_job = MagicMock()
        mock_job.job_id = "12345_0"
        mock_executor = MagicMock()
        mock_executor.submit.return_value = mock_job
        mock_executor_class.return_value = mock_executor

        mock_serialize.return_value = {}
        mock_settings.return_value.to_environment_variables.return_value = {}

        runner = SlurmTaskRunner(units_per_worker=3, max_array_size=1000)
        mock_task = MagicMock()
        mock_task.name = "test_task"
        mock_task.fn = lambda x: x
        mock_task.isasync = False

        with runner:
            futures = runner.map(mock_task, {"x": list(range(7))})

        # Job 0: items 0,1,2  Job 1: items 3,4,5  Job 2: item 6
        expected = [0, 1, 2, 0, 1, 2, 0]
        for i, future in enumerate(futures):
            assert future.item_index_in_job == expected[i]


# ---------------------------------------------------------------------------
# Backend / ExecutionMode
# ---------------------------------------------------------------------------


class TestBackend:
    """Tests for execution_mode configuration."""

    def test_defaults_to_slurm(self):
        assert SlurmTaskRunner().execution_mode == ExecutionMode.SLURM

    def test_explicit_slurm(self):
        runner = SlurmTaskRunner(execution_mode=ExecutionMode.SLURM)
        assert runner.execution_mode == ExecutionMode.SLURM

    def test_explicit_local(self):
        runner = SlurmTaskRunner(execution_mode=ExecutionMode.LOCAL)
        assert runner.execution_mode == ExecutionMode.LOCAL

    def test_string_backward_compat(self):
        runner = SlurmTaskRunner(execution_mode="local")
        assert runner.execution_mode == ExecutionMode.LOCAL

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="is not a valid ExecutionMode"):
            SlurmTaskRunner(execution_mode="invalid")

    def test_from_env_var(self, monkeypatch):
        monkeypatch.setenv("SLURM_TASKRUNNER_BACKEND", "local")
        runner = SlurmTaskRunner()
        assert runner.execution_mode == ExecutionMode.LOCAL

    def test_explicit_overrides_env_var(self, monkeypatch):
        monkeypatch.setenv("SLURM_TASKRUNNER_BACKEND", "local")
        runner = SlurmTaskRunner(execution_mode=ExecutionMode.SLURM)
        assert runner.execution_mode == ExecutionMode.SLURM

    def test_poll_interval_local_default(self):
        runner = SlurmTaskRunner(execution_mode="local")
        assert runner.poll_interval == 1.0

    def test_poll_interval_slurm_default(self):
        runner = SlurmTaskRunner(execution_mode="slurm")
        assert runner.poll_interval == 5.0

    def test_poll_interval_explicit_override(self):
        runner = SlurmTaskRunner(execution_mode="local", poll_interval=5.0)
        assert runner.poll_interval == 5.0

    @patch("prefect_submitit.runner.submitit.LocalExecutor")
    def test_local_uses_local_executor(self, mock_executor_class):
        mock_executor_class.return_value = MagicMock()
        runner = SlurmTaskRunner(execution_mode="local")

        with runner:
            mock_executor_class.assert_called_once_with(folder="slurm_logs")

    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_slurm_uses_auto_executor(self, mock_executor_class):
        mock_executor_class.return_value = MagicMock()
        runner = SlurmTaskRunner(execution_mode="slurm")

        with runner:
            mock_executor_class.assert_called_once_with(folder="slurm_logs/%j")

    @patch("prefect_submitit.runner.submitit.LocalExecutor")
    def test_local_warns_on_ignored_params(self, mock_executor_class, caplog):
        mock_executor_class.return_value = MagicMock()

        runner = SlurmTaskRunner(
            execution_mode="local",
            partition="gpu",
            mem_gb=16,
            gpus_per_node=2,
            slurm_array_parallelism=50,
            constraint="v100",
        )

        with (
            caplog.at_level(logging.WARNING, logger="prefect.task_runner.slurm"),
            runner,
        ):
            pass

        assert "Local backend ignores SLURM parameters" in caplog.text
        assert "partition='gpu'" in caplog.text
        assert "mem_gb=16" in caplog.text
        assert "gpus_per_node=2" in caplog.text
        assert "slurm_array_parallelism=50" in caplog.text

    @patch("prefect_submitit.runner.submitit.LocalExecutor")
    def test_local_no_warning_with_defaults(self, mock_executor_class, caplog):
        mock_executor_class.return_value = MagicMock()
        runner = SlurmTaskRunner(execution_mode="local")

        with (
            caplog.at_level(logging.WARNING, logger="prefect.task_runner.slurm"),
            runner,
        ):
            pass

        assert "Local backend ignores" not in caplog.text


# ---------------------------------------------------------------------------
# Local backend integration
# ---------------------------------------------------------------------------


class TestLocalBackendIntegration:
    """Integration tests for local backend mode."""

    @patch("prefect.utilities.engine.resolve_inputs_sync")
    @patch("prefect.context.serialize_context")
    @patch("prefect.settings.context.get_current_settings")
    @patch("prefect_submitit.runner.submitit.LocalExecutor")
    def test_map_without_chunking(
        self, mock_executor_class, mock_settings, mock_serialize, mock_resolve
    ):
        mock_job = MagicMock()
        mock_job.job_id = "local_12345_0"
        mock_executor = MagicMock()
        mock_executor.submit.return_value = mock_job
        mock_executor_class.return_value = mock_executor

        mock_serialize.return_value = {}
        mock_settings.return_value.to_environment_variables.return_value = {}
        mock_resolve.side_effect = lambda x, **kwargs: x

        runner = SlurmTaskRunner(execution_mode="local", max_array_size=1000)
        mock_task = MagicMock()
        mock_task.name = "test_task"
        mock_task.fn = lambda x: x * 2
        mock_task.isasync = False

        with runner:
            futures = runner.map(mock_task, {"x": range(10)})

        assert len(futures) == 10
        assert mock_executor.batch.call_count == 1

    @patch("prefect.utilities.engine.resolve_inputs_sync")
    @patch("prefect.context.serialize_context")
    @patch("prefect.settings.context.get_current_settings")
    @patch("prefect_submitit.runner.submitit.LocalExecutor")
    def test_map_with_chunking(
        self, mock_executor_class, mock_settings, mock_serialize, mock_resolve
    ):
        mock_job = MagicMock()
        mock_job.job_id = "local_12345_0"
        mock_executor = MagicMock()
        mock_executor.submit.return_value = mock_job
        mock_executor_class.return_value = mock_executor

        mock_serialize.return_value = {}
        mock_settings.return_value.to_environment_variables.return_value = {}
        mock_resolve.side_effect = lambda x, **kwargs: x

        runner = SlurmTaskRunner(execution_mode="local", max_array_size=5)
        mock_task = MagicMock()
        mock_task.name = "test_task"
        mock_task.fn = lambda x: x * 2
        mock_task.isasync = False

        with runner:
            futures = runner.map(mock_task, {"x": range(10)})

        assert len(futures) == 10
        assert mock_executor.batch.call_count == 2

    @patch("prefect.utilities.engine.resolve_inputs_sync")
    @patch("prefect.context.serialize_context")
    @patch("prefect.settings.context.get_current_settings")
    @patch("prefect_submitit.runner.submitit.LocalExecutor")
    def test_poll_interval_used(
        self, mock_executor_class, mock_settings, mock_serialize, mock_resolve
    ):
        mock_job = MagicMock()
        mock_job.job_id = "local_12345"
        mock_executor = MagicMock()
        mock_executor.submit.return_value = mock_job
        mock_executor_class.return_value = mock_executor

        mock_serialize.return_value = {}
        mock_settings.return_value.to_environment_variables.return_value = {}
        mock_resolve.side_effect = lambda x, **kwargs: x

        runner = SlurmTaskRunner(execution_mode="local")
        mock_task = MagicMock()
        mock_task.name = "test_task"
        mock_task.fn = lambda x: x
        mock_task.isasync = False

        with runner:
            futures = runner.map(mock_task, {"x": [1, 2, 3]})

        for future in futures:
            assert future._poll_interval == 1.0
