"""Tests for prefect_submitit.futures.batched.

Tests SlurmBatchedItemFuture.
"""

from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

from prefect_submitit.futures.array import SlurmArrayPrefectFuture
from prefect_submitit.futures.batched import SlurmBatchedItemFuture


def _mock_slurm_job_future(
    job_id: str = "12345_0", array_task_index: int = 0
) -> MagicMock:
    """Create a mock SlurmArrayPrefectFuture."""
    mock_future = MagicMock(spec=SlurmArrayPrefectFuture)
    mock_future.slurm_job_id = job_id
    mock_future.array_task_index = array_task_index
    mock_future.state.is_pending.return_value = True
    mock_future.state.is_running.return_value = False
    mock_future.state.is_completed.return_value = False
    mock_future.is_done = False
    return mock_future


class TestSlurmBatchedItemFuture:
    """Tests for SlurmBatchedItemFuture."""

    # --- Properties ---

    def test_properties(self):
        mock_job_future = _mock_slurm_job_future("12345_2", 2)
        task_run_id = uuid4()

        future = SlurmBatchedItemFuture(
            slurm_job_future=mock_job_future,
            item_index_in_job=1,
            global_item_index=7,
            task_run_id=task_run_id,
        )

        assert future.slurm_job_future is mock_job_future
        assert future.item_index_in_job == 1
        assert future.global_item_index == 7
        assert future.task_run_id == task_run_id
        assert future.slurm_job_index == 2

    def test_slurm_job_id_delegates(self):
        mock_job_future = _mock_slurm_job_future("99999_3")

        future = SlurmBatchedItemFuture(
            slurm_job_future=mock_job_future,
            item_index_in_job=0,
            global_item_index=0,
            task_run_id=uuid4(),
        )

        assert future.slurm_job_id == "99999_3"

    # --- State ---

    def test_state_delegates_to_slurm_job_future(self):
        mock_job_future = _mock_slurm_job_future()
        mock_state = MagicMock()
        mock_job_future.state = mock_state

        future = SlurmBatchedItemFuture(
            slurm_job_future=mock_job_future,
            item_index_in_job=0,
            global_item_index=0,
            task_run_id=uuid4(),
        )

        assert future.state is mock_state

    # --- Result ---

    def test_result_extracts_item_from_batch(self):
        mock_job_future = _mock_slurm_job_future()
        mock_job_future.result.return_value = ["a", "b", "c"]

        future = SlurmBatchedItemFuture(
            slurm_job_future=mock_job_future,
            item_index_in_job=1,
            global_item_index=4,
            task_run_id=uuid4(),
        )

        assert future.result() == "b"

    def test_result_cached(self):
        mock_job_future = _mock_slurm_job_future()
        mock_job_future.result.return_value = ["x", "y", "z"]

        future = SlurmBatchedItemFuture(
            slurm_job_future=mock_job_future,
            item_index_in_job=2,
            global_item_index=5,
            task_run_id=uuid4(),
        )

        result1 = future.result()
        result2 = future.result()

        assert result1 == result2 == "z"
        mock_job_future.result.assert_called_once()

    def test_result_returns_none_if_job_result_none(self):
        mock_job_future = _mock_slurm_job_future()
        mock_job_future.result.return_value = None

        future = SlurmBatchedItemFuture(
            slurm_job_future=mock_job_future,
            item_index_in_job=0,
            global_item_index=0,
            task_run_id=uuid4(),
        )

        assert future.result() is None

    def test_result_early_termination(self):
        """When batch job was terminated early, index exceeds results."""
        mock_job_future = _mock_slurm_job_future()
        mock_job_future.result.return_value = ["only_one"]

        future = SlurmBatchedItemFuture(
            slurm_job_future=mock_job_future,
            item_index_in_job=3,
            global_item_index=10,
            task_run_id=uuid4(),
        )

        result = future.result()

        assert result["success"] is False
        assert "terminated early" in result["error"]

    # --- Wait ---

    def test_wait_delegates_to_slurm_job_future(self):
        mock_job_future = _mock_slurm_job_future()

        future = SlurmBatchedItemFuture(
            slurm_job_future=mock_job_future,
            item_index_in_job=0,
            global_item_index=0,
            task_run_id=uuid4(),
        )

        future.wait(timeout=10.0)
        mock_job_future.wait.assert_called_once_with(10.0)

    def test_wait_without_timeout(self):
        mock_job_future = _mock_slurm_job_future()

        future = SlurmBatchedItemFuture(
            slurm_job_future=mock_job_future,
            item_index_in_job=0,
            global_item_index=0,
            task_run_id=uuid4(),
        )

        future.wait()
        mock_job_future.wait.assert_called_once_with(None)

    # --- Callbacks ---

    def test_add_done_callback_registers(self):
        mock_job_future = _mock_slurm_job_future()
        mock_job_future.is_done = False

        future = SlurmBatchedItemFuture(
            slurm_job_future=mock_job_future,
            item_index_in_job=0,
            global_item_index=0,
            task_run_id=uuid4(),
        )

        callback = MagicMock()
        future.add_done_callback(callback)
        callback.assert_not_called()

    def test_add_done_callback_fires_immediately_if_done(self):
        mock_job_future = _mock_slurm_job_future()
        mock_job_future.is_done = True

        future = SlurmBatchedItemFuture(
            slurm_job_future=mock_job_future,
            item_index_in_job=0,
            global_item_index=0,
            task_run_id=uuid4(),
        )

        callback = MagicMock()
        future.add_done_callback(callback)
        callback.assert_called_once_with(future)

    def test_fire_callbacks_handles_exception(self):
        """Callback exceptions should not propagate or stop other callbacks."""
        mock_job_future = _mock_slurm_job_future()

        future = SlurmBatchedItemFuture(
            slurm_job_future=mock_job_future,
            item_index_in_job=0,
            global_item_index=0,
            task_run_id=uuid4(),
        )

        good_calls = []
        future._callbacks = [
            lambda f: (_ for _ in ()).throw(RuntimeError("boom")),
            good_calls.append,
        ]
        future._fire_callbacks()

        assert len(good_calls) == 1

    # --- Repr ---

    def test_repr_contains_key_info(self):
        mock_job_future = _mock_slurm_job_future("12345_3", 3)
        mock_job_future.state.type = "RUNNING"

        future = SlurmBatchedItemFuture(
            slurm_job_future=mock_job_future,
            item_index_in_job=2,
            global_item_index=11,
            task_run_id=uuid4(),
        )

        repr_str = repr(future)
        assert "SlurmBatchedItemFuture" in repr_str
        assert "global_index=11" in repr_str
        assert "job_index=3" in repr_str
        assert "item_in_job=2" in repr_str
        assert "12345_3" in repr_str
