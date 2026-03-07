# Design: SLURM Cancellation Detection Improvements

**Date:** 2026-03-07
**Status:** Draft
**Analysis:** [slurm-cancellation-detection.md](../analysis/slurm-cancellation-detection.md)

## Objective

Improve the reliability and speed of detecting cancelled SLURM jobs. This covers a targeted set of fixes drawn from the cancellation analysis — specifically the items that are safe to implement without design risk or behavior changes.

## Scope

### In scope (this PR)

| # | Change | Analysis ref |
|---|--------|-------------|
| A | Fix race condition in `wait()` — validate state after poll loop exits | Issue 1 |
| B | Add `cancel()` method to base `SlurmPrefectFuture` | Issue 2 |
| C | Handle `CANCELLED by <uid>` state variant | Issue 3 |
| D | Include current SLURM state in timeout error messages | Issue 7 |
| E | Reduce default poll interval from 30s to 5s | Issue 8 (partial) |

### Out of scope (future work)

| Change | Why deferred |
|--------|-------------|
| Prefect backend state update on cancellation (Issue 4) | Introduces client coupling into futures layer; needs design for where this responsibility lives |
| SIGTERM handler on compute nodes (Issue 5) | Signal handlers are global state; risk of interfering with Prefect/submitit handlers |
| Cancel-on-exit in task runner (Issue 6) | Behavior change; must be opt-in; needs separate design |
| Batched sibling propagation (Issue 9) | Optimization, not correctness |

---

## Detailed Design

### A. Fix race condition in `wait()`

**File:** `src/prefect_submitit/futures/base.py` — `SlurmPrefectFuture.wait()`

**Problem:** When `job.done()` returns `True` for a cancelled/failed job, the `while` loop exits and the failure state check (which is inside the loop) never runs. The future is marked as done and callbacks fire as if the job succeeded.

**Change:** Add a state check between the loop exit and the `self._done = True` assignment.

```python
def wait(self, timeout: float | None = None) -> None:
    effective_timeout = timeout or self._max_poll_time
    start = time.time()

    while not self._job.done():
        elapsed = time.time() - start
        if effective_timeout and elapsed > effective_timeout:
            current_state = self._job.state                          # [D]
            msg = (
                f"Job {self.slurm_job_id} did not complete "
                f"within {effective_timeout:.0f}s "
                f"(last observed state: {current_state})"            # [D]
            )
            raise TimeoutError(msg)

        slurm_state = self._job.state
        if self._is_terminal_failure(slurm_state):                   # [C]
            self._raise_job_failed(slurm_state)

        time.sleep(self._poll_interval)

    # --- NEW: validate final state after loop exits ---             # [A]
    slurm_state = self._job.state
    if self._is_terminal_failure(slurm_state):
        self._raise_job_failed(slurm_state)

    self._done = True
    self._fire_callbacks()
```

**Notes:**
- The `self._job.state` call after the loop is an additional SLURM query, but it only happens once per future (at completion) so the cost is negligible.
- The existing in-loop check is kept for early detection during long-running jobs.

### B. Add `cancel()` to base `SlurmPrefectFuture`

**File:** `src/prefect_submitit/futures/base.py`

**Problem:** Only `SlurmArrayPrefectFuture` can cancel jobs. Single-task submissions via `runner.submit()` return a `SlurmPrefectFuture` with no cancellation API.

**Change:** Add a `cancel()` method to the base class.

```python
def cancel(self) -> bool:
    """Cancel the SLURM job.

    Returns:
        True if scancel succeeded, False otherwise.
    """
    try:
        subprocess.run(
            ["scancel", self.slurm_job_id],
            check=True,
            capture_output=True,
        )
        return True
    except subprocess.CalledProcessError:
        return False
```

**Note:** `base.py` will need `import subprocess` added.

**Impact on `SlurmArrayPrefectFuture`:** The existing `cancel()` in the subclass cancels by `self._array_job_id` (the whole array). The base class version cancels by `self.slurm_job_id`. For array futures, `slurm_job_id` is `{array_id}_{task_index}`, so the base class `cancel()` would cancel a single task — which is actually what `cancel_task()` does. The subclass override that cancels the entire array is preserved, so behavior is unchanged for array futures.

### C. Handle `CANCELLED by <uid>` state variant

**File:** `src/prefect_submitit/futures/base.py`

**Problem:** SLURM can report `CANCELLED by 1000` instead of plain `CANCELLED`. Exact set membership won't match.

**Change:** Extract a helper method that checks for both exact matches and the `CANCELLED` prefix.

```python
def _is_terminal_failure(self, slurm_state: str) -> bool:
    """Check if a SLURM state represents a terminal failure."""
    normalized = slurm_state.rstrip("+")
    return (
        normalized in self.TERMINAL_FAILURE_STATES
        or normalized.startswith("CANCELLED")
    )
```

Also extract a helper for raising, to avoid duplicating the stderr-fetch logic:

```python
def _raise_job_failed(self, slurm_state: str) -> None:
    """Raise SlurmJobFailed with stderr context."""
    try:
        stderr = self._job.stderr()
    except Exception:
        stderr = "(stderr unavailable)"
    msg = f"Job {self.slurm_job_id}: {slurm_state}\nstderr:\n{stderr}"
    raise SlurmJobFailed(msg)
```

These two helpers centralize the failure-detection logic so it's consistent across the in-loop check, the post-loop check, and the `state` property.

**`state` property update:** The `state` property should also use the helper for consistency:

```python
@property
def state(self) -> State:
    slurm_state = self._job.state
    normalized = slurm_state.rstrip("+")
    if normalized == "COMPLETED":
        return Completed()
    if self._is_terminal_failure(slurm_state):
        return Failed(message=f"SLURM: {slurm_state}")
    if normalized == "RUNNING":
        return Running()
    return Pending()
```

### D. Include SLURM state in timeout errors

**File:** `src/prefect_submitit/futures/base.py`

**Problem:** Timeout errors don't mention the job's current state, which can mask the real issue (e.g., job was cancelled just before timeout).

**Change:** Query `self._job.state` and include it in the `TimeoutError` message. See the updated `wait()` in section A above.

### E. Reduce default poll interval to 5 seconds

**File:** `src/prefect_submitit/runner.py`

**Problem:** The 30-second default delays cancellation detection. Users may not realize their jobs were cancelled for up to 30 seconds.

**Change:**

```python
# Before
self.poll_interval = (
    1.0
    if poll_interval is None and self.execution_mode == ExecutionMode.LOCAL
    else 30.0
    if poll_interval is None
    else poll_interval
)

# After
self.poll_interval = (
    1.0
    if poll_interval is None and self.execution_mode == ExecutionMode.LOCAL
    else 5.0
    if poll_interval is None
    else poll_interval
)
```

**Rationale for 5 seconds:**

- Each poll invokes a SLURM CLI command (`sacct`/`squeue`) via submitit. At 5s per future, a 200-task array where futures are polled concurrently would generate ~40 calls/second, which is within reasonable scheduler load.
- In practice, futures are usually polled sequentially (iterating a `PrefectFutureList`), so actual load is much lower — only one SLURM query is in-flight at a time.
- The `poll_interval` parameter is already user-configurable, so users with very large workloads can increase it.
- 5 seconds is 6x faster than the current 30s default for detecting cancellations, timeouts, and other failures.

---

## Files Changed

| File | Changes |
|------|---------|
| `src/prefect_submitit/futures/base.py` | Add `cancel()`, `_is_terminal_failure()`, `_raise_job_failed()`; update `wait()` with post-loop check and improved timeout message; update `state` property to use helper |
| `src/prefect_submitit/runner.py` | Change default `poll_interval` from `30.0` to `5.0` |
| `tests/futures/test_base.py` | Tests for post-loop failure detection, cancel(), `CANCELLED by` handling, timeout message content |
| `tests/futures/test_array.py` | Verify array cancel() still works with base class cancel() present |
| `tests/test_runner.py` | Update any tests that assert `poll_interval == 30.0` |
| `tests/conftest.py` | Update hardcoded `poll_interval = 30.0` to `5.0` |

---

## Test Plan

### New tests needed

1. **Race condition fix (A):**
   - Mock `job.done()` to return `True` on first call, with `job.state` as `"CANCELLED"` — verify `SlurmJobFailed` is raised.
   - Same test with `"FAILED"`, `"NODE_FAIL"`, `"TIMEOUT"`, `"OUT_OF_MEMORY"`.
   - Mock `job.done()` to return `True` with `job.state` as `"COMPLETED"` — verify no exception, `is_done` is `True`.

2. **Base cancel (B):**
   - Mock `subprocess.run` success — verify `cancel()` returns `True` and calls `scancel` with correct job ID.
   - Mock `subprocess.run` raising `CalledProcessError` — verify returns `False`.

3. **CANCELLED by variant and `+` suffix (C):**
   - Future with `job.state = "CANCELLED by 1000"` — verify `state` property returns `Failed`.
   - Same state in `wait()` — verify `SlurmJobFailed` is raised.
   - Future with `job.state = "TIMEOUT+"` — verify `state` property returns `Failed`.
   - Future with `job.state = "CANCELLED+"` — verify detected as terminal failure.

4. **Timeout message (D):**
   - Trigger timeout — verify error message contains `last observed state:`.

5. **Poll interval default (E):**
   - Construct `SlurmTaskRunner()` with no `poll_interval` — verify default is `5.0`.
   - Construct with `execution_mode=LOCAL` — verify default is still `1.0`.

### Existing tests to update

- Any test asserting `poll_interval == 30.0` needs to change to `5.0` (in `tests/test_runner.py` and `tests/conftest.py`).
- The `test_wait_failure_state_raises` test covers the in-loop check; the new post-loop test complements it.

---

## Rollout

These are all internal behavior improvements with no public API changes (except the additive `cancel()` method). The poll interval change is the only observable behavior difference, and it makes things faster, not slower.

Ship as a single PR. No feature flags needed.
