"""Tests for prefect_submitit.executors.

Tests SLURM-side execution functions. These are serialized and run on compute
nodes, so unit tests mock Prefect internals.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from prefect_submitit.executors import (
    _item_repr,
    build_slurm_task_name,
    run_batch_in_slurm,
    run_task_in_slurm,
)


class TestItemRepr:
    """Tests for _item_repr helper function."""

    def test_simple_types(self):
        assert _item_repr(42) == "42"
        assert _item_repr("hello") == "'hello'"
        assert _item_repr([1, 2]) == "[1, 2]"

    def test_truncation(self):
        long_string = "x" * 200
        result = _item_repr(long_string, max_len=100)

        assert len(result) <= 103  # 100 chars + "..."
        assert result.endswith("...")

    def test_no_truncation_when_short(self):
        result = _item_repr("short", max_len=100)
        assert result == "'short'"
        assert not result.endswith("...")

    def test_custom_max_len(self):
        result = _item_repr("a" * 50, max_len=20)
        assert len(result) <= 23

    def test_exception_in_repr(self):
        class BadRepr:
            def __repr__(self):
                raise RuntimeError("No repr")

        result = _item_repr(BadRepr())
        assert result == "<BadRepr>"

    def test_none(self):
        assert _item_repr(None) == "None"

    def test_dict(self):
        assert _item_repr({"a": 1}) == "{'a': 1}"


class TestBuildSlurmTaskName:
    """Tests for build_slurm_task_name helper."""

    def test_array_job(self, monkeypatch):
        monkeypatch.setenv("SLURM_ARRAY_JOB_ID", "100")
        monkeypatch.setenv("SLURM_ARRAY_TASK_ID", "5")
        monkeypatch.setenv("SLURM_JOB_ID", "105")
        assert build_slurm_task_name() == "slurm-100_5"

    def test_step_id(self, monkeypatch):
        monkeypatch.setenv("SLURM_JOB_ID", "200")
        monkeypatch.setenv("SLURM_STEP_ID", "3")
        monkeypatch.delenv("SLURM_ARRAY_JOB_ID", raising=False)
        monkeypatch.delenv("SLURM_ARRAY_TASK_ID", raising=False)
        assert build_slurm_task_name() == "slurm-200.3"

    def test_plain_job(self, monkeypatch):
        monkeypatch.setenv("SLURM_JOB_ID", "300")
        monkeypatch.delenv("SLURM_ARRAY_JOB_ID", raising=False)
        monkeypatch.delenv("SLURM_ARRAY_TASK_ID", raising=False)
        monkeypatch.delenv("SLURM_STEP_ID", raising=False)
        assert build_slurm_task_name() == "slurm-300"

    def test_no_slurm_env(self, monkeypatch):
        monkeypatch.delenv("SLURM_JOB_ID", raising=False)
        monkeypatch.delenv("SLURM_ARRAY_JOB_ID", raising=False)
        monkeypatch.delenv("SLURM_ARRAY_TASK_ID", raising=False)
        monkeypatch.delenv("SLURM_STEP_ID", raising=False)
        assert build_slurm_task_name() is None

    def test_array_takes_priority_over_step(self, monkeypatch):
        monkeypatch.setenv("SLURM_ARRAY_JOB_ID", "100")
        monkeypatch.setenv("SLURM_ARRAY_TASK_ID", "5")
        monkeypatch.setenv("SLURM_JOB_ID", "105")
        monkeypatch.setenv("SLURM_STEP_ID", "0")
        assert build_slurm_task_name() == "slurm-100_5"

    def test_step_takes_priority_over_plain_job(self, monkeypatch):
        monkeypatch.setenv("SLURM_JOB_ID", "200")
        monkeypatch.setenv("SLURM_STEP_ID", "7")
        monkeypatch.delenv("SLURM_ARRAY_JOB_ID", raising=False)
        monkeypatch.delenv("SLURM_ARRAY_TASK_ID", raising=False)
        assert build_slurm_task_name() == "slurm-200.7"


class TestRunTaskInSlurm:
    """Tests for run_task_in_slurm."""

    @patch("prefect.task_engine.run_task_sync")
    @patch("prefect.context.hydrated_context")
    def test_sync_task_dispatched(self, mock_hydrated_ctx, mock_run_sync, monkeypatch):
        monkeypatch.delenv("SLURM_ARRAY_JOB_ID", raising=False)
        monkeypatch.delenv("SLURM_ARRAY_TASK_ID", raising=False)
        monkeypatch.delenv("SLURM_JOB_ID", raising=False)

        mock_task = MagicMock()
        mock_task.isasync = False
        mock_run_sync.return_value = "result_value"

        result = run_task_in_slurm(
            task=mock_task,
            task_run_id=uuid4(),
            parameters={"x": 1},
            context={},
        )

        mock_run_sync.assert_called_once()
        assert result == "result_value"

    @patch("prefect.task_engine.run_task_sync")
    @patch("prefect.context.hydrated_context")
    def test_task_run_name_from_array_env(
        self, mock_hydrated_ctx, mock_run_sync, monkeypatch
    ):
        monkeypatch.setenv("SLURM_ARRAY_JOB_ID", "99999")
        monkeypatch.setenv("SLURM_ARRAY_TASK_ID", "42")

        mock_task = MagicMock()
        mock_task.isasync = False
        mock_task.with_options.return_value = mock_task
        mock_run_sync.return_value = "result"

        run_task_in_slurm(task=mock_task, context={})

        mock_task.with_options.assert_called_once_with(task_run_name="slurm-99999_42")

    @patch("prefect.task_engine.run_task_sync")
    @patch("prefect.context.hydrated_context")
    def test_task_run_name_from_job_id(
        self, mock_hydrated_ctx, mock_run_sync, monkeypatch
    ):
        monkeypatch.delenv("SLURM_ARRAY_JOB_ID", raising=False)
        monkeypatch.delenv("SLURM_ARRAY_TASK_ID", raising=False)
        monkeypatch.setenv("SLURM_JOB_ID", "55555")

        mock_task = MagicMock()
        mock_task.isasync = False
        mock_task.with_options.return_value = mock_task
        mock_run_sync.return_value = "result"

        run_task_in_slurm(task=mock_task, context={})

        mock_task.with_options.assert_called_once_with(task_run_name="slurm-55555")

    @patch("prefect.task_engine.run_task_sync")
    @patch("prefect.context.hydrated_context")
    def test_no_naming_without_slurm_env(
        self, mock_hydrated_ctx, mock_run_sync, monkeypatch
    ):
        monkeypatch.delenv("SLURM_ARRAY_JOB_ID", raising=False)
        monkeypatch.delenv("SLURM_ARRAY_TASK_ID", raising=False)
        monkeypatch.delenv("SLURM_JOB_ID", raising=False)

        mock_task = MagicMock()
        mock_task.isasync = False
        mock_run_sync.return_value = "result"

        run_task_in_slurm(task=mock_task, context={})

        mock_task.with_options.assert_not_called()

    @patch("prefect.task_engine.run_task_sync")
    @patch("prefect.context.hydrated_context")
    def test_env_vars_updated(self, mock_hydrated_ctx, mock_run_sync, monkeypatch):
        monkeypatch.delenv("SLURM_ARRAY_JOB_ID", raising=False)
        monkeypatch.delenv("SLURM_ARRAY_TASK_ID", raising=False)
        monkeypatch.delenv("SLURM_JOB_ID", raising=False)

        test_var = "_PREFECT_SUBMITIT_TEST_VAR"
        monkeypatch.delenv(test_var, raising=False)

        mock_task = MagicMock()
        mock_task.isasync = False
        mock_run_sync.return_value = None

        run_task_in_slurm(
            task=mock_task,
            env={test_var: "hello"},
            context={},
        )

        assert os.environ.get(test_var) == "hello"

    @patch("prefect.task_engine.run_task_sync")
    @patch("prefect.context.hydrated_context")
    def test_context_popped_from_kwargs(
        self, mock_hydrated_ctx, mock_run_sync, monkeypatch
    ):
        """Context should be passed to hydrated_context, not to run_task_sync."""
        monkeypatch.delenv("SLURM_ARRAY_JOB_ID", raising=False)
        monkeypatch.delenv("SLURM_ARRAY_TASK_ID", raising=False)
        monkeypatch.delenv("SLURM_JOB_ID", raising=False)

        mock_task = MagicMock()
        mock_task.isasync = False
        mock_run_sync.return_value = None
        test_context = {"flow_run": "test"}

        run_task_in_slurm(task=mock_task, context=test_context)

        mock_hydrated_ctx.assert_called_once_with(test_context)
        call_kwargs = mock_run_sync.call_args[1]
        assert "context" not in call_kwargs


class TestRunBatchInSlurm:
    """Tests for run_batch_in_slurm."""

    @patch("prefect.client.orchestration.get_client")
    @patch("prefect.context.FlowRunContext")
    @patch("prefect.context.hydrated_context")
    def test_processes_items_successfully(
        self, mock_hydrated_ctx, mock_flow_ctx, mock_get_client
    ):
        mock_flow_ctx.get.return_value = None
        mock_get_client.side_effect = Exception("no client")

        mock_task = MagicMock()
        mock_task.fn = lambda x: x * 10

        result = run_batch_in_slurm(
            task=mock_task,
            task_run_id=uuid4(),
            parameters={
                "_batch_items": [1, 2, 3],
                "_batch_param_name": "x",
            },
            context={},
        )

        assert result == [10, 20, 30]

    @patch("prefect.client.orchestration.get_client")
    @patch("prefect.context.FlowRunContext")
    @patch("prefect.context.hydrated_context")
    def test_processes_with_static_params(
        self, mock_hydrated_ctx, mock_flow_ctx, mock_get_client
    ):
        mock_flow_ctx.get.return_value = None
        mock_get_client.side_effect = Exception("no client")

        mock_task = MagicMock()
        mock_task.fn = lambda x, y=0: x + y

        result = run_batch_in_slurm(
            task=mock_task,
            task_run_id=uuid4(),
            parameters={
                "_batch_items": [1, 2, 3],
                "_batch_param_name": "x",
                "y": 100,
            },
            context={},
        )

        assert result == [101, 102, 103]

    @patch("prefect.client.orchestration.get_client")
    @patch("prefect.context.FlowRunContext")
    @patch("prefect.context.hydrated_context")
    def test_handles_per_item_errors(
        self, mock_hydrated_ctx, mock_flow_ctx, mock_get_client
    ):
        mock_flow_ctx.get.return_value = None
        mock_get_client.side_effect = Exception("no client")

        def failing_fn(x):
            if x == 2:
                raise ValueError("bad value")
            return x * 10

        mock_task = MagicMock()
        mock_task.fn = failing_fn

        result = run_batch_in_slurm(
            task=mock_task,
            task_run_id=uuid4(),
            parameters={
                "_batch_items": [1, 2, 3],
                "_batch_param_name": "x",
            },
            context={},
        )

        assert len(result) == 3
        assert result[0] == 10
        assert result[1]["success"] is False
        assert "ValueError" in result[1]["error"]
        assert result[2] == 30

    def test_missing_task_raises(self):
        with pytest.raises((ValueError, AttributeError)):
            run_batch_in_slurm(
                task_run_id=uuid4(),
                parameters={
                    "_batch_items": [1],
                    "_batch_param_name": "x",
                },
                context={},
            )

    @patch("prefect.client.orchestration.get_client")
    @patch("prefect.context.FlowRunContext")
    @patch("prefect.context.hydrated_context")
    def test_env_vars_updated(
        self, mock_hydrated_ctx, mock_flow_ctx, mock_get_client, monkeypatch
    ):
        mock_flow_ctx.get.return_value = None
        mock_get_client.side_effect = Exception("no client")

        test_var = "_PREFECT_SUBMITIT_BATCH_TEST"
        monkeypatch.delenv(test_var, raising=False)

        mock_task = MagicMock()
        mock_task.fn = lambda x: x

        run_batch_in_slurm(
            task=mock_task,
            task_run_id=uuid4(),
            parameters={
                "_batch_items": [1],
                "_batch_param_name": "x",
            },
            env={test_var: "batch_test"},
            context={},
        )

        assert os.environ.get(test_var) == "batch_test"

    @patch("prefect.client.orchestration.get_client")
    @patch("prefect.context.FlowRunContext")
    @patch("prefect.context.hydrated_context")
    def test_empty_batch(self, mock_hydrated_ctx, mock_flow_ctx, mock_get_client):
        mock_flow_ctx.get.return_value = None
        mock_get_client.side_effect = Exception("no client")

        mock_task = MagicMock()
        mock_task.fn = lambda x: x

        result = run_batch_in_slurm(
            task=mock_task,
            task_run_id=uuid4(),
            parameters={
                "_batch_items": [],
                "_batch_param_name": "x",
            },
            context={},
        )

        assert result == []
