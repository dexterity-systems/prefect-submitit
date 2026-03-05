"""Tests for prefect_submitit.submission.

Tests submission helper functions directly.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import uuid4

from prefect_submitit.futures import SlurmArrayPrefectFuture
from prefect_submitit.submission import (
    batch_items,
    build_array_callable,
    build_batch_callable,
    submit_batch_array_chunk,
)


class TestBatchItems:
    """Tests for batch_items."""

    def test_exact_division(self, mock_runner):
        mock_runner.units_per_worker = 3
        items = [1, 2, 3, 4, 5, 6]

        batches = batch_items(mock_runner, items)

        assert len(batches) == 2
        assert batches[0] == [1, 2, 3]
        assert batches[1] == [4, 5, 6]

    def test_remainder(self, mock_runner):
        mock_runner.units_per_worker = 3
        items = [1, 2, 3, 4, 5, 6, 7]

        batches = batch_items(mock_runner, items)

        assert len(batches) == 3
        assert batches[0] == [1, 2, 3]
        assert batches[1] == [4, 5, 6]
        assert batches[2] == [7]

    def test_single_item_per_job(self, mock_runner):
        mock_runner.units_per_worker = 1
        items = [1, 2, 3]

        batches = batch_items(mock_runner, items)

        assert len(batches) == 3
        assert all(len(b) == 1 for b in batches)

    def test_large_batch_size(self, mock_runner):
        mock_runner.units_per_worker = 10
        items = [1, 2, 3]

        batches = batch_items(mock_runner, items)

        assert len(batches) == 1
        assert batches[0] == [1, 2, 3]

    def test_empty_list(self, mock_runner):
        mock_runner.units_per_worker = 3
        assert batch_items(mock_runner, []) == []


class TestBuildArrayCallable:
    """Tests for build_array_callable."""

    @patch("prefect_submitit.submission.cloudpickle_wrapped_call")
    @patch("prefect.utilities.engine.resolve_inputs_sync")
    def test_returns_callable(self, mock_resolve, mock_cloudpickle):
        mock_resolve.side_effect = lambda x, **kwargs: x
        expected = MagicMock()
        mock_cloudpickle.return_value = expected

        result = build_array_callable(
            task=MagicMock(),
            index=0,
            iterable_params={"x": [1, 2, 3]},
            static_params={"y": 10},
            task_run_id=uuid4(),
            context={},
            env={},
        )

        assert result is expected
        mock_resolve.assert_called_once()
        mock_cloudpickle.assert_called_once()

    @patch("prefect_submitit.submission.cloudpickle_wrapped_call")
    @patch("prefect.utilities.engine.resolve_inputs_sync")
    def test_selects_correct_index(self, mock_resolve, mock_cloudpickle):
        mock_resolve.side_effect = lambda x, **kwargs: x
        mock_cloudpickle.return_value = MagicMock()

        build_array_callable(
            task=MagicMock(),
            index=1,
            iterable_params={"x": [10, 20, 30], "y": [40, 50, 60]},
            static_params={"z": 99},
            task_run_id=uuid4(),
            context={},
            env={},
        )

        resolved_params = mock_resolve.call_args[0][0]
        assert resolved_params["x"] == 20
        assert resolved_params["y"] == 50
        assert resolved_params["z"] == 99

    @patch("prefect_submitit.submission.cloudpickle_wrapped_call")
    @patch("prefect.utilities.engine.resolve_inputs_sync")
    def test_passes_return_type_state(self, mock_resolve, mock_cloudpickle):
        mock_resolve.side_effect = lambda x, **kwargs: x
        mock_cloudpickle.return_value = MagicMock()

        build_array_callable(
            task=MagicMock(),
            index=0,
            iterable_params={"x": [1]},
            static_params={},
            task_run_id=uuid4(),
            context={},
            env={"KEY": "val"},
        )

        call_kwargs = mock_cloudpickle.call_args[1]
        assert call_kwargs["return_type"] == "state"
        assert call_kwargs["env"] == {"KEY": "val"}


class TestBuildBatchCallable:
    """Tests for build_batch_callable."""

    @patch("prefect_submitit.submission.cloudpickle_wrapped_call")
    @patch("prefect.utilities.engine.resolve_inputs_sync")
    def test_returns_callable(self, mock_resolve, mock_cloudpickle):
        mock_resolve.side_effect = lambda x, **kwargs: x
        expected = MagicMock()
        mock_cloudpickle.return_value = expected

        result = build_batch_callable(
            task=MagicMock(),
            task_run_id=uuid4(),
            batch=[1, 2, 3],
            param_name="x",
            static_params={"y": 10},
            context={},
            env={},
        )

        assert result is expected

    @patch("prefect_submitit.submission.cloudpickle_wrapped_call")
    @patch("prefect.utilities.engine.resolve_inputs_sync")
    def test_includes_batch_metadata(self, mock_resolve, mock_cloudpickle):
        mock_resolve.side_effect = lambda x, **kwargs: x
        mock_cloudpickle.return_value = MagicMock()

        build_batch_callable(
            task=MagicMock(),
            task_run_id=uuid4(),
            batch=[10, 20],
            param_name="item",
            static_params={"config": "abc"},
            context={},
            env={},
        )

        resolved_params = mock_resolve.call_args[0][0]
        assert resolved_params["_batch_items"] == [10, 20]
        assert resolved_params["_batch_param_name"] == "item"
        assert resolved_params["config"] == "abc"


class TestSubmitBatchArrayChunk:
    """Tests for submit_batch_array_chunk."""

    def test_creates_futures(self, mock_runner):
        mock_job = MagicMock()
        mock_job.job_id = "12345_0"

        mock_executor = MagicMock()
        mock_executor.submit.return_value = mock_job
        mock_runner._executor = mock_executor

        wrapped_calls = [MagicMock(), MagicMock(), MagicMock()]
        task_run_ids = [uuid4(), uuid4(), uuid4()]

        futures = submit_batch_array_chunk(mock_runner, wrapped_calls, task_run_ids, 3)

        assert len(futures) == 3
        assert all(isinstance(f, SlurmArrayPrefectFuture) for f in futures)

    def test_uses_batch_context_manager(self, mock_runner):
        mock_job = MagicMock()
        mock_job.job_id = "12345_0"

        mock_executor = MagicMock()
        mock_executor.submit.return_value = mock_job
        mock_runner._executor = mock_executor

        submit_batch_array_chunk(mock_runner, [MagicMock()], [uuid4()], 1)

        mock_executor.batch.assert_called_once()

    def test_array_job_id_extracted(self, mock_runner):
        mock_job = MagicMock()
        mock_job.job_id = "99999_0"

        mock_executor = MagicMock()
        mock_executor.submit.return_value = mock_job
        mock_runner._executor = mock_executor

        futures = submit_batch_array_chunk(mock_runner, [MagicMock()], [uuid4()], 1)

        assert futures[0].array_job_id == "99999"

    def test_futures_have_correct_indices(self, mock_runner):
        mock_job = MagicMock()
        mock_job.job_id = "12345_0"

        mock_executor = MagicMock()
        mock_executor.submit.return_value = mock_job
        mock_runner._executor = mock_executor

        futures = submit_batch_array_chunk(
            mock_runner, [MagicMock(), MagicMock()], [uuid4(), uuid4()], 2
        )

        assert futures[0].array_task_index == 0
        assert futures[1].array_task_index == 1
        assert futures[0].array_size == 2
        assert futures[1].array_size == 2
