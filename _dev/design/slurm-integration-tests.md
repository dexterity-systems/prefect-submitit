# Design: Portable SLURM Integration Tests

**Date:** 2026-03-07
**Status:** Draft
**Companion:** `_dev/analysis/slurm-integration-testing.md`

---

## Goal

Test `prefect-submitit` against real SLURM clusters without hardcoding any
cluster-specific assumptions. The same test suite should run on any SLURM
cluster with Python 3.12+ and the package installed.

---

## 1. Configuration

### Environment variables

All cluster-specific parameters come from environment variables. No defaults for
values that genuinely vary between clusters.

```
# ─── Required ────────────────────────────────────────────────────────────────
SLURM_TEST_PARTITION        SLURM partition for test jobs (e.g. "cpu", "batch")

# ─── Optional (sensible defaults) ────────────────────────────────────────────
SLURM_TEST_MEM_GB           Memory per job in GB                  [default: 1]
SLURM_TEST_TIME_LIMIT       Wall time for normal test jobs        [default: "00:05:00"]
SLURM_TEST_ACCOUNT          SLURM account (--account)             [default: unset]
SLURM_TEST_QOS              SLURM QOS (--qos)                     [default: unset]
SLURM_TEST_MAX_WAIT         Max seconds to wait for scheduling    [default: 300]
SLURM_TEST_GPU_PARTITION    Partition with GPUs                    [default: unset → skip GPU tests]
SLURM_TEST_LOG_DIR          Directory for submitit logs           [default: tempdir per session]
```

These are read once in a session-scoped fixture and bundled into a frozen
dataclass:

```python
@dataclasses.dataclass(frozen=True)
class SlurmTestConfig:
    partition: str
    mem_gb: int
    time_limit: str
    account: str | None
    qos: str | None
    max_wait: int
    gpu_partition: str | None
    log_dir: Path
```

### Pytest markers and collection

```ini
# pyproject.toml additions
markers = [
    "slurm: tests that submit real SLURM jobs (deselected unless --run-slurm)",
    "slurm_gpu: tests that require a GPU partition",
]
```

Tests are **deselected by default**. Enabled via:
```bash
pytest --run-slurm                          # run SLURM tests
pytest --run-slurm -m "slurm and not slurm_gpu"   # skip GPU tests
```

A `conftest.py` plugin handles this:
```python
def pytest_addoption(parser):
    parser.addoption("--run-slurm", action="store_true", default=False,
                     help="Run tests that require a real SLURM cluster")

def pytest_collection_modifyitems(config, items):
    if not config.getoption("--run-slurm"):
        skip = pytest.mark.skip(reason="need --run-slurm to run")
        for item in items:
            if "slurm" in item.keywords:
                item.add_marker(skip)
```

### Guard: is SLURM available?

Before any SLURM test runs, a session fixture verifies:
```python
@pytest.fixture(scope="session", autouse=True)
def _check_slurm_available(request):
    if not request.config.getoption("--run-slurm"):
        return
    try:
        subprocess.run(["squeue", "--me", "--noheader"], capture_output=True,
                       timeout=10, check=True)
    except (FileNotFoundError, subprocess.SubprocessError) as e:
        pytest.skip(f"SLURM not available: {e}")
```

---

## 2. Prefect server strategy

### The problem

`run_task_in_slurm` calls Prefect's `run_task_sync`, which registers task runs
with the Prefect API. `run_batch_in_slurm` explicitly calls
`get_client().create_task_run()`. Both require a Prefect server reachable from
SLURM compute nodes.

### The solution: Prefect server as session fixture

Start a Prefect server on the login node during the test session. Compute nodes
connect back to it. This mirrors real-world usage (flows run on login nodes).

```python
@pytest.fixture(scope="session")
def prefect_server(slurm_config):
    """Start an ephemeral Prefect server for the test session."""
    # Determine a routable IP (not localhost — compute nodes need to reach it)
    hostname = socket.getfqdn()

    port = find_free_port()  # random available port
    api_url = f"http://{hostname}:{port}/api"

    proc = subprocess.Popen(
        ["prefect", "server", "start", "--host", "0.0.0.0", "--port", str(port)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    _wait_for_server(api_url, timeout=30)

    old_url = os.environ.get("PREFECT_API_URL")
    os.environ["PREFECT_API_URL"] = api_url
    yield api_url

    os.environ.pop("PREFECT_API_URL", None)
    if old_url:
        os.environ["PREFECT_API_URL"] = old_url
    proc.terminate()
    proc.wait(timeout=10)
```

**Fallback:** If `PREFECT_API_URL` is already set (user has an existing server),
skip starting one and use the existing URL.

**Skip condition:** If no Prefect server can be started or reached, Tier 2 tests
are skipped, but Tier 1 (SLURM-mechanics-only) tests still run.

---

## 3. Fixture design

### Job cleanup (critical for portability)

Every test that submits SLURM jobs must ensure cleanup. A leaked job wastes
cluster resources and can cause subsequent test failures.

```python
@pytest.fixture
def slurm_jobs():
    """Track submitted job IDs for cleanup."""
    job_ids = []
    yield job_ids
    # Teardown: cancel any remaining jobs
    if job_ids:
        subprocess.run(
            ["scancel"] + [str(jid) for jid in job_ids],
            capture_output=True, timeout=10,
        )
```

Tests register jobs: `slurm_jobs.append(future.slurm_job_id)`.

Session-scoped safety net: scan `SLURM_TEST_LOG_DIR` for submitit job folders
and `scancel` anything still running.

### Runner fixture

```python
@pytest.fixture
def slurm_runner(slurm_config, tmp_path):
    """A configured SlurmTaskRunner for testing."""
    extra_kwargs = {}
    if slurm_config.account:
        extra_kwargs["slurm_account"] = slurm_config.account
    if slurm_config.qos:
        extra_kwargs["slurm_qos"] = slurm_config.qos

    runner = SlurmTaskRunner(
        partition=slurm_config.partition,
        time_limit=slurm_config.time_limit,
        mem_gb=slurm_config.mem_gb,
        gpus_per_node=0,
        poll_interval=2.0,      # fast polling for tests
        log_folder=str(tmp_path / "slurm_logs"),
        **extra_kwargs,
    )
    return runner
```

### Flow context fixture

Many tests need a Prefect flow run context (since `SlurmTaskRunner.submit()`
reads `FlowRunContext.get()`). Rather than running a real flow, we create a
minimal flow context:

```python
@pytest.fixture
def flow_context(prefect_server):
    """Run test body inside a Prefect flow context."""
    @prefect.flow
    def _test_flow():
        yield  # test body runs here

    # Use Prefect's test utilities or context manager
    ...
```

Alternatively, the simplest portable approach: **define actual `@flow`-decorated
functions** that use the runner, and call them from tests. This avoids manual
context setup and matches real usage:

```python
def test_single_task_submit(slurm_runner, prefect_server, slurm_jobs):
    @task
    def add(a, b):
        return a + b

    @flow(task_runner=slurm_runner)
    def my_flow():
        future = add.submit(1, 2)
        slurm_jobs.append(future.slurm_job_id)
        return future.result()

    assert my_flow() == 3
```

This is the **recommended pattern** — it's simple, mirrors real usage, and
exercises the full Prefect flow→task→runner pipeline.

---

## 4. Timeout strategy

SLURM scheduling latency is the biggest portability threat. A test that works in
2s on an idle cluster may take 5 minutes on a busy one.

### Approach: adaptive waits, hard ceilings

```python
SLURM_TEST_MAX_WAIT = int(os.environ.get("SLURM_TEST_MAX_WAIT", "300"))

# On SlurmTaskRunner for tests:
runner = SlurmTaskRunner(
    time_limit="00:05:00",            # 5min wall time (generous for simple tasks)
    max_poll_time=SLURM_TEST_MAX_WAIT + 300,  # scheduling wait + execution time
    poll_interval=2.0,                # check every 2s (not the default 5s)
)
```

Each test also gets a `pytest.mark.timeout()`:
```python
@pytest.mark.timeout(SLURM_TEST_MAX_WAIT + 120)  # generous ceiling
```

The two layers:
1. **`max_poll_time`** on the future — raises `TimeoutError` from `wait()`
2. **`pytest.mark.timeout`** — kills the test process if everything hangs

### Short tasks for fast tests

Test task functions should complete in <5 seconds of actual computation:
```python
@task
def add(a, b):
    return a + b

@task
def sleepy(seconds):
    import time
    time.sleep(seconds)
    return seconds
```

Most test time is scheduling wait, not execution.

---

## 5. Test catalog (P0 + P1 + P2)

### File organization

```
tests/
├── integration/
│   ├── conftest.py             # Fixtures, config, markers, cleanup
│   ├── test_submit.py          # Single-task submission (P0/P1)
│   ├── test_map.py             # Job array submission (P0/P1)
│   ├── test_batch.py           # Batched execution (P1)
│   ├── test_cancel.py          # Cancellation (P0/P1/P2)
│   ├── test_failures.py        # SLURM failure modes (P0/P1/P2)
│   ├── test_polling.py         # Polling and timing (P2)
│   └── test_environment.py     # Env/context propagation (P2)
```

### 5.1 `test_submit.py` — Single task submission

#### P0: `test_sync_task_roundtrip`
Submit a sync `@task` function, retrieve result.
- **Asserts:** `future.result() == expected_value`
- **Validates:** Full submission→execution→result pipeline
- **Portability:** Uses `add(1, 2)` — no external dependencies

#### P0: `test_async_task_roundtrip`
Submit an async `@task` function, retrieve result.
- **Asserts:** `future.result() == expected_value`
- **Validates:** The `asyncio.run(run_task_async(...))` branch
- **Portability:** Same as sync — `async def add(a, b): return a + b`

#### P1: `test_task_with_dependency`
Submit task A, then task B with `wait_for=[future_a]`.
- **Asserts:** Task B sees task A's result; both complete
- **Validates:** `prefect_wait()` + `resolve_inputs_sync()` across SLURM jobs
- **Portability:** Two sequential jobs; scheduling order doesn't matter since
  B explicitly waits

#### P2: `test_task_run_in_prefect_api`
Submit a task, retrieve result, then query Prefect API for the task run.
- **Asserts:** Task run exists with correct `flow_run_id`, state is `Completed`
- **Validates:** Context propagation + Prefect API calls from compute node
- **Portability:** Requires Prefect server reachable from compute nodes

#### P2: `test_task_run_name_contains_slurm_job_id`
Submit a task, check the task run name in Prefect API.
- **Asserts:** Task run name matches `slurm-{SLURM_JOB_ID}` pattern
- **Validates:** `SLURM_JOB_ID` env var reading + `task.with_options(task_run_name=...)`
- **Portability:** Pattern match, not exact value

#### P2: `test_complex_return_value`
Submit a task returning a nested dataclass/dict with various types.
- **Asserts:** Returned object equals original (deep equality)
- **Validates:** cloudpickle round-trip over shared filesystem
- **Portability:** Pure Python types; no filesystem assumptions

#### P2: `test_complex_input_parameters`
Submit a task with nested dicts, lists, custom objects as parameters.
- **Asserts:** Task receives correct parameters, result depends on them
- **Validates:** Parameter serialization via cloudpickle
- **Portability:** Pure Python types

### 5.2 `test_map.py` — Job array submission

#### P0: `test_map_small_array`
`task.map(x=[1, 2, 3, 4, 5])` — 5 items, single job array.
- **Asserts:** All 5 results returned, correct values, correct order
- **Validates:** `sbatch --array=0-4`, per-task result retrieval
- **Portability:** Well under any cluster's `MaxArraySize`

#### P1: `test_map_with_mixed_params`
`task.map(x=[1,2,3], y="static")` — iterable + static params.
- **Asserts:** Each task received its `x` value + shared `y`
- **Validates:** `partition_parameters()` in real flow context
- **Portability:** No cluster-specific behavior

#### P1: `test_map_exceeding_max_array_size`
Set `max_array_size=3` on the runner, map 7 items.
- **Asserts:** All 7 results returned correctly
- **Validates:** Chunk splitting into multiple `sbatch --array` calls
- **Note:** We set `max_array_size` explicitly to force chunking regardless of
  cluster limit. This avoids needing to know the real `MaxArraySize`.
- **Portability:** Fully portable — doesn't rely on cluster's actual limit

#### P1: `test_map_with_parallelism_throttle`
Map 10 items with `slurm_array_parallelism=3`.
- **Asserts:** All 10 complete (no deadlock), results correct
- **Validates:** `%3` suffix in `sbatch --array`; SLURM respects it
- **Portability:** Only requires partition to exist

#### P1: `test_map_one_task_fails`
Map 5 items where item 3 raises `ValueError`.
- **Asserts:** Items 1,2,4,5 return correct values; item 3's future raises
  `SlurmJobFailed` (because the SLURM job itself exits non-zero when
  `run_task_sync` hits an unhandled exception)
- **Actually:** Prefect's `run_task_sync` catches the exception and returns a
  `Failed` state. The state is pickled. On the submission host,
  `state.result()` re-raises the original `ValueError`.
- **Validates:** Per-array-task failure isolation and error propagation
- **Portability:** Fully portable

#### P2: `test_map_array_task_names_in_api`
Map 3 items, check Prefect API for task run names.
- **Asserts:** Each has name matching `slurm-{array_job_id}_{task_index}`
- **Validates:** `SLURM_ARRAY_JOB_ID` / `SLURM_ARRAY_TASK_ID` env vars
- **Portability:** Pattern match

### 5.3 `test_batch.py` — Batched execution

#### P1: `test_batch_basic`
10 items with `units_per_worker=5` → 2 SLURM jobs.
- **Asserts:** All 10 `SlurmBatchedItemFuture.result()` values correct
- **Validates:** `run_batch_in_slurm` end-to-end, result indexing
- **Portability:** Fully portable

#### P1: `test_batch_with_remainder`
7 items with `units_per_worker=3` → batches of [3, 3, 1].
- **Asserts:** All 7 results correct, last batch has 1 item
- **Validates:** Uneven batch sizes, index arithmetic
- **Portability:** Fully portable

#### P1: `test_batch_per_item_failure`
5 items where item 2 raises. Other items succeed.
- **Asserts:** Items 0,1,3,4 return correct values; item 2 returns
  `{"success": False, "error": "..."}` dict
- **Validates:** Per-item error isolation in `run_batch_in_slurm`
- **Portability:** Fully portable

#### P2: `test_batch_with_static_params`
Batch items with additional static parameters.
- **Asserts:** Each item receives both the iterable param and static params
- **Validates:** Static parameter merging in `build_batch_callable`
- **Portability:** Fully portable

#### P2: `test_batch_task_run_state`
Submit a batch, query Prefect API for the batch task run.
- **Asserts:** Task run exists, state is `Completed`
- **Validates:** `create_task_run(state=Running())` → `set_task_run_state(Completed())`
- **Portability:** Requires Prefect API reachable from compute nodes

### 5.4 `test_cancel.py` — Cancellation

#### P0: `test_cancel_running_task`
Submit a long-running task (`time.sleep(120)`), wait until it's `RUNNING`,
then `future.cancel()`.
- **Asserts:** `cancel()` returns `True`; subsequent `future.wait()` raises
  `SlurmJobFailed` with `CANCELLED` in message
- **Validates:** `scancel` → SLURM signal delivery → state detection
- **Wait strategy:** Poll `future.state` until it's `Running()` before
  cancelling. Timeout if scheduling takes too long.
- **Portability:** Only assumes SLURM delivers SIGTERM on `scancel` (universal)

#### P1: `test_cancel_entire_array`
Map 5 long-running tasks, cancel via `future.cancel()` (cancels whole array).
- **Asserts:** All 5 futures raise `SlurmJobFailed`
- **Validates:** `scancel {array_job_id}` cancels all tasks
- **Portability:** Universal `scancel` behavior

#### P1: `test_cancel_single_array_task`
Map 5 long-running tasks, cancel task 2 via `futures[2].cancel_task()`.
- **Asserts:** Task 2 raises `SlurmJobFailed`; tasks 0,1,3,4 complete
  successfully
- **Validates:** `scancel {array_job_id}_{task_index}` precision cancellation
- **Portability:** Universal `scancel` behavior

#### P2: `test_cancel_cancelled_by_uid_detection`
Cancel a running task, then inspect the state string.
- **Asserts:** `future.state` is `Failed` with message containing `CANCELLED`
- **Note:** SLURM may report `CANCELLED by <uid>`. We check `startswith`.
- **Validates:** The `CANCELLED by N` state string variant
- **Portability:** Works on all SLURM versions that report cancellation user

#### P2: `test_cancel_batch_mid_execution`
Submit a batch of 5 items where each sleeps 10s. Cancel after ~2 items
should have completed.
- **Asserts:** Some `SlurmBatchedItemFuture.result()` calls return values;
  later ones return `{"success": False, "error": "...terminated early"}`.
  OR all fail if cancellation is fast enough.
- **Validates:** `SlurmBatchedItemFuture` early-termination handling
- **Portability note:** Timing-dependent; test accepts multiple valid outcomes
  (all cancelled, partial results, or all complete if cancel was too slow)

#### P2: `test_cancel_pending_job`
Submit to a partition/QOS that queues jobs. Cancel before it starts running.
- **Asserts:** `cancel()` returns `True`; future detects `CANCELLED`
- **Portability:** May need a way to ensure the job stays pending (e.g.,
  request more resources than available, or use `--begin=now+1hour`).
  **Fallback:** Use `slurm_kwargs={"slurm_begin": "now+1hour"}` to force
  deferred start. Cancel before the begin time.
- **Portability risk:** `--begin` support varies. If not supported, skip.

#### P2: `test_cancel_race_with_completion`
Submit a task that completes in ~1 second. Immediately call `cancel()`.
- **Asserts:** No crash. Either `cancel()` succeeds and future raises
  `SlurmJobFailed`, or `cancel()` returns `False` and future returns result.
- **Validates:** Race condition handling — both outcomes are acceptable
- **Portability:** Fully portable; test asserts on *either* outcome

### 5.5 `test_failures.py` — SLURM failure modes

#### P0: `test_task_exception_propagation`
Submit a task that raises `ValueError("test error")`.
- **Asserts:** `future.result()` raises `ValueError` with `"test error"` in
  message (the original exception propagates through State serialization)
- **Validates:** Exception → Failed State → pickle → deserialize → re-raise
- **Portability:** Fully portable

#### P1: `test_timeout_detection`
Submit a task that sleeps longer than `time_limit`.
- **Setup:** `time_limit="00:01:00"`, task sleeps 120s
- **Asserts:** `future.wait()` raises `SlurmJobFailed` with `TIMEOUT` in
  message
- **Validates:** SLURM timeout enforcement, `TIMEOUT`/`TIMEOUT+` state
  detection
- **Portability:** Requires cluster to enforce time limits (most do). If
  the cluster doesn't kill on timeout (rare), test is skipped.
- **Wait:** Takes ~60s of wall time (the time limit)

#### P2: `test_unpicklable_return_raises_cleanly`
Submit a task that returns something cloudpickle can't deserialize on the
submission host (e.g., a `module` object — actually cloudpickle handles most
things. Better: return a result, then corrupt the pickle file before
`result()` is called).
- **Alternative approach:** Submit a task whose result is valid, but mock
  `cloudpickle.loads` to raise. This is really a unit test concern.
- **Revised:** Test that a task returning a `lambda` works (cloudpickle
  handles it), confirming the serialization path. If we want to test
  failure, submit a task that creates a non-picklable object and try to
  return it — this would fail inside `run_task_sync` and produce a Failed
  state with PicklingError.
- **Asserts:** `future.result()` raises with pickling-related error info
- **Portability:** Fully portable

#### P2: `test_invalid_partition_fails_cleanly`
Create a runner with `partition="nonexistent_partition_xyz"`, submit a task.
- **Asserts:** Submission raises an error (from submitit or SLURM) — not a
  hang
- **Validates:** Clean error surfacing for misconfiguration
- **Portability:** Assumes no cluster has this exact partition name
- **Note:** The error may come at `sbatch` time (immediate) or at job
  scheduling time (delayed). Test accepts either.

#### P2: `test_job_no_output_raises_slurm_job_failed`
Submit a task, then externally kill it with `scancel --signal=SIGKILL` (no
chance to write results).
- **Asserts:** `future.result()` raises `SlurmJobFailed` (from
  `UncompletedJobError`)
- **Validates:** The `UncompletedJobError` → `SlurmJobFailed` path
- **Portability:** `scancel --signal=SIGKILL` is universal SLURM

### 5.6 `test_polling.py` — Polling and timing

#### P2: `test_rapid_completion`
Submit a trivial task (`return 42`).
- **Asserts:** Result is `42`; no errors even if job finishes before first
  poll
- **Validates:** `job.done()` returning `True` on first check is handled
- **Portability:** Fully portable

#### P2: `test_max_poll_time_fires`
Submit a long-sleeping task with `max_poll_time=10`.
- **Asserts:** `future.wait()` raises `TimeoutError` within ~15s
- **Validates:** `max_poll_time` enforcement, error message contains state
- **Cleanup:** `scancel` the still-running job in teardown
- **Portability:** Fully portable (timeout is on submission host, not SLURM)

#### P2: `test_wait_timeout_overrides_max_poll`
Submit a long task with `max_poll_time=600`, call `future.wait(timeout=10)`.
- **Asserts:** `TimeoutError` raised in ~10s (not 600s)
- **Validates:** Explicit `timeout` parameter takes precedence
- **Portability:** Fully portable

### 5.7 `test_environment.py` — Environment and context propagation

#### P2: `test_custom_env_var_propagation`
Set `MY_TEST_VAR="hello_from_test"` before submission. Task reads and
returns `os.environ.get("MY_TEST_VAR")`.
- **Asserts:** Result is `"hello_from_test"`
- **Validates:** `os.environ` capture → cloudpickle → `os.environ.update()`
- **Portability:** Fully portable

#### P2: `test_flow_run_context_on_compute_node`
Submit a task that reads and returns its `FlowRunContext.get().flow_run.id`.
- **Asserts:** Matches the flow run ID on the submission host
- **Validates:** `serialize_context()` → `hydrated_context()` round-trip
- **Portability:** Requires Prefect API reachable from compute nodes

#### P2: `test_logs_contain_output`
Submit a task that `print("MARKER_STRING")`, then check `future.logs()`.
- **Asserts:** stdout contains `"MARKER_STRING"`
- **Validates:** submitit log capture, `job.stdout()` path
- **Portability:** Depends on submitit's log folder being on shared
  filesystem — which is a package requirement anyway

---

## 6. Test task definitions

Keep test tasks minimal and deterministic. Define them at module level (not
inside test functions) to avoid cloudpickle issues with closures.

```python
# tests/integration/tasks.py

from prefect import task

@task
def add(a: int, b: int) -> int:
    return a + b

@task
async def async_add(a: int, b: int) -> int:
    return a + b

@task
def identity(x):
    """Return input unchanged. For testing serialization."""
    return x

@task
def sleep_and_return(seconds: float) -> float:
    import time
    time.sleep(seconds)
    return seconds

@task
def get_env_var(name: str) -> str | None:
    import os
    return os.environ.get(name)

@task
def get_flow_run_id() -> str:
    from prefect.context import FlowRunContext
    ctx = FlowRunContext.get()
    return str(ctx.flow_run.id) if ctx and ctx.flow_run else "no-context"

@task
def fail_with(error_type: str, message: str):
    raise getattr(__builtins__, error_type, ValueError)(message)

@task
def conditional_fail(x: int, fail_on: int = -1):
    """Return x, unless x == fail_on, then raise."""
    if x == fail_on:
        msg = f"intentional failure on {x}"
        raise ValueError(msg)
    return x * 10

@task
def print_marker(marker: str) -> str:
    print(marker)  # noqa: T201
    return marker
```

**Why module-level?** cloudpickle serializes functions by reference when
possible. Functions defined inside test bodies may capture test-local state
that's not available on compute nodes, causing `AttributeError` on unpickling.

---

## 7. Test helper utilities

```python
# tests/integration/helpers.py

import time
from prefect.states import Running

def wait_for_running(future, timeout=None, poll=2.0):
    """Block until a future's SLURM state is RUNNING."""
    max_wait = timeout or SLURM_TEST_MAX_WAIT
    start = time.time()
    while time.time() - start < max_wait:
        state = future.state
        if isinstance(state, Running):
            return
        time.sleep(poll)
    msg = f"Job {future.slurm_job_id} did not reach RUNNING within {max_wait}s"
    raise TimeoutError(msg)
```

---

## 8. Portability principles (summary)

1. **No hardcoded partition names.** Always from `SLURM_TEST_PARTITION`.

2. **No hardcoded resource amounts.** Use minimal resources (1GB, 0 GPU)
   unless the test specifically needs more.

3. **No timing assumptions.** Never assert "job completed in <N seconds."
   Use generous timeouts from `SLURM_TEST_MAX_WAIT`. Assert on *outcomes*,
   not *timing*.

4. **Accept multiple valid outcomes for races.** A cancel-vs-complete race
   test should assert `(cancelled OR completed)`, not one or the other.

5. **Skip, don't fail, on missing capabilities.** GPU partition not
   configured? Skip. Prefect API unreachable? Skip Tier 2 tests.

6. **Module-level task definitions.** Avoid cloudpickle closure issues.

7. **Clean up aggressively.** Track every submitted job ID. `scancel` in
   teardown. Session-scoped safety net.

8. **Minimal task functions.** <5s of actual compute. Test time should be
   dominated by scheduling, not execution.

9. **No network calls from tasks** (except Prefect API which the framework
   handles). Tasks do pure computation or read env vars.

10. **No filesystem assumptions beyond submitit's log folder.** Don't assume
    specific paths exist on compute nodes.

---

## 9. Example: complete test with all fixtures

```python
# tests/integration/test_submit.py

import pytest
from prefect import flow

from prefect_submitit import SlurmTaskRunner
from tests.integration.tasks import add, async_add

pytestmark = pytest.mark.slurm


class TestSingleTaskSubmission:
    """P0: Basic submission and result retrieval."""

    def test_sync_task_roundtrip(self, slurm_runner, slurm_jobs):
        @flow(task_runner=slurm_runner)
        def compute():
            future = add.submit(1, 2)
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        assert compute() == 3

    def test_async_task_roundtrip(self, slurm_runner, slurm_jobs):
        @flow(task_runner=slurm_runner)
        def compute():
            future = async_add.submit(10, 20)
            slurm_jobs.append(future.slurm_job_id)
            return future.result()

        assert compute() == 30
```

---

## 10. CI/CD considerations

These tests won't run in GitHub Actions (no SLURM). Options:

1. **Manual trigger** — `workflow_dispatch` with cluster SSH details
2. **Self-hosted runner** on a SLURM login node
3. **Separate CI job** triggered after merge, running on HPC infrastructure
4. **Local development** — developers run `pytest --run-slurm` on their
   cluster manually

Recommended: Option 2 or 3, with results posted back to the PR as a status
check. The standard CI pipeline continues to run unit tests only.

---

## 11. Open questions

1. **Should we test `get_cluster_max_array_size()` against real `scontrol`
   output?** It's currently mocked in unit tests. A simple integration test
   could call it and assert the result is a positive integer.

2. **Should OOM testing be included?** Deliberately triggering `OUT_OF_MEMORY`
   requires allocating more than `mem_gb` allows, which depends on SLURM's
   memory cgroup enforcement. Many clusters don't enforce memory limits. This
   test may be permanently skipped on most clusters. **Recommendation:** defer
   to a future "advanced failure modes" test suite.

3. **How to handle flaky scheduling?** Some tests (especially cancellation)
   depend on the job being in `RUNNING` state at a specific moment. On very
   busy clusters, the job may not start within `SLURM_TEST_MAX_WAIT`.
   **Recommendation:** These tests should `pytest.skip()` on scheduling
   timeout rather than `pytest.fail()`. A scheduling timeout is a cluster
   issue, not a code issue.

4. **Should we test with different Python versions on compute nodes?**
   Cloudpickle version skew is a real risk. **Recommendation:** not in this
   test suite — it's an environment setup concern. Document it as a known
   requirement.
