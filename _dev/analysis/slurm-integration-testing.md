# Analysis: SLURM Integration Testing for prefect-submitit

**Date:** 2026-03-07
**Status:** Analysis complete

## 1. Current State

### Test inventory
- **175 unit tests** across 11 files (~2,319 lines of test code)
- **Zero real SLURM interaction** — every test mocks submitit, subprocess, and Prefect internals
- **One "integration" suite** (`TestLocalBackendIntegration`) tests local backend mode, still with mocked executors
- **Test-to-source ratio:** 2.1:1 (healthy for unit tests, but no system coverage)

### What the mocks hide

The mocking strategy replaces the following real interactions with stubs:

| Layer | Real interaction | Mock substitute |
|-------|-----------------|-----------------|
| submitit → SLURM | `sbatch`, `squeue`, `sacct` | `MagicMock().submit()` returning fake `Job` |
| submitit → filesystem | Pickle files in `slurm_logs/%j/` | `job.result()` returning preset values |
| SLURM → compute node | Process launch, env setup, signal delivery | Never tested |
| Compute node → Prefect API | `create_task_run`, `set_task_run_state` | `@patch("prefect.client.orchestration.get_client")` |
| Submission host → SLURM control | `scontrol show config`, `scancel` | `@patch("subprocess.run")` |
| Context round-trip | `serialize_context()` → cloudpickle → `hydrated_context()` | Both patched; never tested end-to-end |

### Key untested code paths

1. **Async task execution** (`executors.py:58-61`) — the `asyncio.run(run_task_async(...))` branch is entirely untested
2. **Real cloudpickle serialization** — mock Jobs bypass pickle entirely; serialization failures (lambdas, closures with unpicklable state) are invisible
3. **Environment propagation fidelity** — `os.environ.update(env)` on compute nodes is assumed to work but depends on SLURM's `--export` behavior, shell profile sourcing, and env variable length limits
4. **`scontrol show config` output parsing** — `get_cluster_max_array_size()` parses `MaxArraySize = N` from stdout; never tested against real SLURM output (which varies by version)
5. **`sacct`/`squeue` state strings** — the state mapping in `SlurmPrefectFuture.state` handles `CANCELLED by <uid>`, `TIMEOUT+`, etc. These are SLURM version-specific formats
6. **Shared filesystem timing** — submitit writes result pickles to NFS/Lustre; `job.done()` may return `True` before the file is flushed/visible on the login node
7. **Signal handling during cancellation** — SLURM sends `SIGTERM` then `SIGKILL`; neither executor function handles signals

## 2. Execution architecture (testing implications)

### The two-host problem

```
Login/Head Node (test runner)          SLURM Compute Node
==================================     ==================================
pytest process                         SLURM job process
  │                                      │
  ├─ SlurmTaskRunner.__enter__()         │
  ├─ executor.submit(cloudpickled_fn)    │
  │   └─ sbatch → scheduler ──────────► job starts
  │                                      ├─ cloudpickle.loads(fn)
  │                                      ├─ os.environ.update(env)
  │                                      ├─ hydrated_context(ctx)
  │                                      ├─ run_task_sync(...)
  │                                      │   └─ task.fn(**params) ◄── actual work
  │                                      ├─ cloudpickle.dumps(State)
  │                                      └─ writes result pickle ──► shared filesystem
  │                                                                        │
  ├─ future.wait() polls job.done() ◄──── reads result file ◄─────────────┘
  ├─ future.result() deserializes State
  └─ pytest asserts on result
```

**Critical insight:** The compute node code (`executors.py`) runs in a **completely separate process** on a **different machine**. It inherits the test environment only through:
1. Cloudpickled function + kwargs (serialized by submitit)
2. Captured `env` dict (restored via `os.environ.update`)
3. Serialized Prefect context (restored via `hydrated_context`)

This means integration tests can't directly control or observe the compute node — they can only:
- Submit work and check results
- Check SLURM job state via `sacct`/`squeue`
- Read stdout/stderr logs after completion
- Verify Prefect API state (if API is reachable from compute nodes)

### Prefect API reachability

The package requires Prefect API access from compute nodes for:
- `run_task_in_slurm` → `run_task_sync()` → creates/updates task runs
- `run_batch_in_slurm` → `get_client()` → `create_task_run()`, `set_task_run_state()`

**Without Prefect API from compute nodes**, jobs will either:
- Fail with connection errors (if Prefect client raises on unreachable API)
- Silently skip task run registration (if Prefect handles it gracefully)

This is a fundamental constraint that shapes the test infrastructure design.

## 3. Portability concerns

### What varies between SLURM clusters

| Concern | Variation | Impact on tests |
|---------|-----------|----------------|
| **Partition names** | `cpu`, `batch`, `normal`, `default`, `short` | Every test that submits a job |
| **Available memory** | Some partitions have 1GB min, others 64GB min | `mem_gb` parameter must be valid |
| **MaxArraySize** | 1000, 4096, unlimited | Chunking tests need to know the real limit |
| **Time limits** | Some partitions require ≥5min, others allow 1min | Short jobs for tests may be rejected |
| **Account/QOS** | Some clusters require `--account=`, `--qos=` | Submissions fail without them |
| **Scheduling latency** | 1-5s (idle cluster) to 30min+ (busy cluster) | Test timeouts must accommodate |
| **Compute node network** | Some have full internet, some are air-gapped | Prefect API reachability |
| **Shared filesystem** | NFS, Lustre, GPFS; varies in flush latency | Result file visibility after `job.done()` |
| **SLURM version** | 20.x, 21.x, 22.x, 23.x — state format changes | State string parsing |
| **Python availability** | Module system, conda, venv, spack | The test Python env must be available on compute nodes |
| **GPU partitions** | May not exist, or require special accounts | GPU tests must be optional |
| **`scancel` permissions** | Some clusters restrict cancellation of others' jobs | Cancellation tests |
| **Default env propagation** | `--export=ALL` vs `--export=NONE` cluster defaults | Env restoration behavior |

### What's consistent across clusters

- `sbatch`, `squeue`, `sacct`, `scancel`, `scontrol` CLI interface
- SLURM state names (`COMPLETED`, `FAILED`, `CANCELLED`, etc.)
- Job array syntax (`--array=0-N`)
- Environment variables (`SLURM_JOB_ID`, `SLURM_ARRAY_JOB_ID`, `SLURM_ARRAY_TASK_ID`)
- Signal delivery order (SIGTERM → grace period → SIGKILL)

## 4. Risk areas for integration tests

### High risk (most likely to cause flaky tests)

1. **Scheduling latency** — A test expecting a job to start in 5s may wait 10min on a busy cluster. Every poll-based assertion needs generous, configurable timeouts.

2. **Filesystem flush latency** — `job.done()` queries SLURM's accounting DB. The result pickle file may not be visible on NFS for seconds after the job completes. This is the most common source of intermittent failures in HPC testing.

3. **Python environment on compute nodes** — If the compute node doesn't have the same Python environment (with `prefect-submitit`, `prefect`, `submitit`, `cloudpickle` installed), jobs fail with `ModuleNotFoundError`. This is a setup issue, not a test issue, but it's the #1 reason integration tests fail on new clusters.

### Medium risk

4. **Prefect API connectivity** — If compute nodes can't reach the Prefect server, `run_task_sync` may fail, hang, or produce surprising error states depending on Prefect's error handling.

5. **Cancellation timing** — `scancel` is asynchronous. A job may complete between `scancel` being issued and SLURM processing it. Tests must handle both "cancelled" and "completed" as valid outcomes in race scenarios.

6. **SLURM state reporting delay** — After a job transitions, `sacct` may take 1-2 polling cycles to reflect the new state. The post-loop state check in `wait()` mitigates this but doesn't eliminate it.

### Low risk

7. **Cloudpickle compatibility** — Rarely changes between minor Python versions, but cloudpickle version skew between login and compute nodes can cause deserialization failures.

8. **Environment variable size limits** — The full `os.environ` is captured and cloudpickled. On systems with very large environments (>128KB), this can cause serialization issues.

## 5. Recommendations

### Test tier structure

**Tier 1 — SLURM mechanics (no Prefect API needed on compute nodes)**
- Submit simple callables via submitit directly (not through `SlurmTaskRunner`)
- Verify SLURM job lifecycle: submit → run → complete
- Test cancellation via `scancel`
- Test state detection via `sacct`
- *Purpose:* Validates that SLURM + submitit + cloudpickle work on this cluster

**Tier 2 — SlurmTaskRunner end-to-end (requires Prefect API from compute nodes)**
- Submit Prefect tasks via `SlurmTaskRunner`
- Verify results, array submission, batched execution
- Verify Prefect task run state in API
- Test cancellation propagation
- *Purpose:* Validates the full package works as intended

**Tier 3 — Failure modes and edge cases**
- Timeout detection, OOM detection
- Partial cancellation of arrays
- Batched execution with per-item failures
- Race conditions (cancel vs. complete)
- *Purpose:* Validates resilience and error handling

### Configuration approach

All cluster-specific values via environment variables with sensible defaults:
```
SLURM_TEST_PARTITION    — required, no default (forces explicit configuration)
SLURM_TEST_MEM_GB       — default: 1
SLURM_TEST_TIME_LIMIT   — default: "00:05:00"
SLURM_TEST_MAX_WAIT     — default: 300 (seconds to wait for scheduling)
SLURM_TEST_ACCOUNT      — default: None (not passed if unset)
SLURM_TEST_QOS          — default: None
SLURM_TEST_GPU_PARTITION — default: None (GPU tests skipped if unset)
PREFECT_API_URL         — required for Tier 2+ (must be reachable from compute nodes)
```

### Cleanup strategy

Every test must track submitted SLURM job IDs and `scancel` them in teardown, even if the test fails or times out. A session-scoped fixture should run a final `scancel` sweep as a safety net.
