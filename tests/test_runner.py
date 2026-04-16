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

    def test_cpus_per_task_default(self):
        runner = SlurmTaskRunner()
        assert runner.cpus_per_task == 1

    def test_cpus_per_task_custom(self):
        runner = SlurmTaskRunner(cpus_per_task=4)
        assert runner.cpus_per_task == 4

    def test_srun_launch_concurrency_default(self):
        runner = SlurmTaskRunner()
        assert runner.srun_launch_concurrency == 128

    def test_srun_launch_concurrency_custom(self):
        runner = SlurmTaskRunner(srun_launch_concurrency=64)
        assert runner.srun_launch_concurrency == 64


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

    def test_preserves_cpus_per_task(self):
        runner = SlurmTaskRunner(cpus_per_task=8)
        assert runner.duplicate().cpus_per_task == 8

    def test_preserves_srun_launch_concurrency(self):
        runner = SlurmTaskRunner(srun_launch_concurrency=64)
        assert runner.duplicate().srun_launch_concurrency == 64


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

    @patch("prefect.context.serialize_context")
    @patch("prefect.settings.context.get_current_settings")
    @patch("prefect.utilities.engine.resolve_inputs_sync")
    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_per_task_slurm_kwargs_applied_and_restored(
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
        mock_settings.return_value.to_environment_variables.return_value = {}
        mock_serialize_context.return_value = {}

        def task_fn(x: int) -> int:
            return x * 2

        task_fn._slurm_kwargs = {"cpus_per_task": 4}

        mock_task = MagicMock()
        mock_task.fn = task_fn
        mock_task.name = "test_task"
        mock_task.isasync = False

        runner = SlurmTaskRunner(cpus_per_task=1)

        with runner:
            runner.submit(mock_task, {"x": 5})

        update_calls = mock_executor.update_parameters.call_args_list
        # First call is SlurmTaskRunner.__enter__ (base params)
        # Second call overrides for task
        # Third call restores base params
        assert len(update_calls) == 3
        original_kwargs = update_calls[0][1]
        override_kwargs = update_calls[1][1]
        restore_kwargs = update_calls[2][1]
        assert original_kwargs["cpus_per_task"] == 1
        assert override_kwargs["cpus_per_task"] == 4
        assert restore_kwargs["cpus_per_task"] == 1


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

    @patch("prefect.utilities.engine.resolve_inputs_sync")
    @patch("prefect.context.serialize_context")
    @patch("prefect.settings.context.get_current_settings")
    @patch("prefect_submitit.runner.submitit.AutoExecutor")
    def test_per_task_slurm_kwargs_applied_and_restored(
        self, mock_executor_class, mock_settings, mock_serialize, mock_resolve
    ):
        mock_job = MagicMock()
        mock_job.job_id = "12345_0"
        mock_executor = MagicMock()
        mock_executor.submit.return_value = mock_job
        mock_executor_class.return_value = mock_executor

        mock_settings.return_value.to_environment_variables.return_value = {}
        mock_serialize.return_value = {}
        mock_resolve.side_effect = lambda x, **kwargs: x

        def task_fn(x: int) -> int:
            return x * 2

        task_fn._slurm_kwargs = {"cpus_per_task": 4}

        mock_task = MagicMock()
        mock_task.name = "test_task"
        mock_task.fn = task_fn
        mock_task.isasync = False

        runner = SlurmTaskRunner(cpus_per_task=1, max_array_size=1000)

        with runner:
            futures = runner.map(mock_task, {"x": [1, 2, 3]})

        assert len(futures) == 3
        update_calls = mock_executor.update_parameters.call_args_list
        # First call is SlurmTaskRunner.__enter__ (base params)
        # Second call overrides for task
        # Third call restores base params
        assert len(update_calls) == 3
        original_kwargs = update_calls[0][1]
        override_kwargs = update_calls[1][1]
        restore_kwargs = update_calls[2][1]
        assert original_kwargs["cpus_per_task"] == 1
        assert override_kwargs["cpus_per_task"] == 4
        assert restore_kwargs["cpus_per_task"] == 1


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

    def test_poll_interval_srun_default(self):
        runner = SlurmTaskRunner(execution_mode="srun")
        assert runner.poll_interval == 0.5

    def test_poll_interval_explicit_override(self):
        runner = SlurmTaskRunner(execution_mode="local", poll_interval=5.0)
        assert runner.poll_interval == 5.0

    def test_explicit_srun(self):
        runner = SlurmTaskRunner(execution_mode=ExecutionMode.SRUN)
        assert runner.execution_mode == ExecutionMode.SRUN

    def test_srun_from_env_var(self, monkeypatch):
        monkeypatch.setenv("SLURM_TASKRUNNER_BACKEND", "srun")
        runner = SlurmTaskRunner()
        assert runner.execution_mode == ExecutionMode.SRUN

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


# ---------------------------------------------------------------------------
# SRUN backend
# ---------------------------------------------------------------------------


class TestSrunBackend:
    """Tests for SRUN execution mode __enter__/__exit__/submit/map."""

    def test_enter_raises_without_slurm_job_id(self, monkeypatch):
        monkeypatch.delenv("SLURM_JOB_ID", raising=False)
        runner = SlurmTaskRunner(execution_mode="srun")
        with pytest.raises(RuntimeError, match="SLURM_JOB_ID"):
            runner.__enter__()

    @patch("prefect_submitit.srun.SrunBackend")
    def test_enter_creates_backend(self, mock_backend_class, monkeypatch):
        monkeypatch.setenv("SLURM_JOB_ID", "999")
        runner = SlurmTaskRunner(execution_mode="srun")
        with runner:
            assert runner._backend is not None
            assert runner._entered is True

    @patch("prefect_submitit.srun.SrunBackend")
    def test_exit_calls_close(self, mock_backend_class, monkeypatch):
        monkeypatch.setenv("SLURM_JOB_ID", "999")
        mock_backend = MagicMock()
        mock_backend_class.return_value = mock_backend
        runner = SlurmTaskRunner(execution_mode="srun")
        runner.__enter__()
        runner.__exit__(None, None, None)
        mock_backend.close.assert_called_once()
        assert runner._backend is None
        assert runner._entered is False

    @patch("prefect_submitit.srun.SrunBackend")
    def test_enter_warns_on_ignored_params(
        self, mock_backend_class, monkeypatch, caplog
    ):
        monkeypatch.setenv("SLURM_JOB_ID", "999")
        runner = SlurmTaskRunner(
            execution_mode="srun",
            partition="gpu",
            slurm_array_parallelism=50,
            max_array_size=42,
        )
        with (
            caplog.at_level(logging.WARNING, logger="prefect.task_runner.slurm"),
            runner,
        ):
            pass
        assert "SRUN backend ignores" in caplog.text
        assert "partition='gpu'" in caplog.text

    @patch("prefect.utilities.engine.resolve_inputs_sync")
    @patch("prefect.context.serialize_context")
    @patch("prefect.settings.context.get_current_settings")
    @patch("prefect_submitit.srun.SrunBackend")
    def test_submit_dispatches_to_backend(
        self,
        mock_backend_class,
        mock_settings,
        mock_serialize,
        mock_resolve,
        monkeypatch,
    ):
        monkeypatch.setenv("SLURM_JOB_ID", "999")
        mock_backend = MagicMock()
        mock_future = MagicMock()
        mock_backend.submit_one.return_value = mock_future
        mock_backend_class.return_value = mock_backend

        mock_serialize.return_value = {}
        mock_settings.return_value.to_environment_variables.return_value = {}
        mock_resolve.side_effect = lambda x, **kwargs: x

        runner = SlurmTaskRunner(execution_mode="srun")
        mock_task = MagicMock()
        mock_task.name = "test_task"

        with runner:
            result = runner.submit(mock_task, {"x": 1})

        mock_backend.submit_one.assert_called_once()
        assert result is mock_future

    @patch("prefect.utilities.engine.resolve_inputs_sync")
    @patch("prefect.context.serialize_context")
    @patch("prefect.settings.context.get_current_settings")
    @patch("prefect_submitit.srun.SrunBackend")
    def test_map_dispatches_to_backend(
        self,
        mock_backend_class,
        mock_settings,
        mock_serialize,
        mock_resolve,
        monkeypatch,
    ):
        monkeypatch.setenv("SLURM_JOB_ID", "999")
        mock_backend = MagicMock()
        mock_backend.submit_many.return_value = [MagicMock(), MagicMock(), MagicMock()]
        mock_backend_class.return_value = mock_backend

        mock_serialize.return_value = {}
        mock_settings.return_value.to_environment_variables.return_value = {}
        mock_resolve.side_effect = lambda x, **kwargs: x

        runner = SlurmTaskRunner(execution_mode="srun")
        mock_task = MagicMock()
        mock_task.name = "test_task"
        mock_task.fn = lambda x: x
        mock_task.isasync = False

        with runner:
            futures = runner.map(mock_task, {"x": [1, 2, 3]})

        mock_backend.submit_many.assert_called_once()
        assert len(futures) == 3
