# Integration Test Fixes — Lessons Learned

## 1. Node-local `/tmp` filesystem

Submitit's `log_folder` was set to pytest's `tmp_path` (under `/tmp`), which is a
local ZFS pool (`pool/tmp`) — not shared via NFS across SLURM nodes. Compute nodes
couldn't read the pickled job payload or write logs back. Every job failed with
`FAILED` and `stderr: None`.

**Symptom:** All SLURM jobs fail with exit code 1:0, no stderr/stdout logs written.

**Fix:** Default `log_folder` to `.slurm_test_logs/` in the project directory (on NFS).
Use `slurm_config.log_dir` in all test runners instead of `tmp_path`.

**Lesson:** On HPC clusters, always verify that paths passed to SLURM are on a shared
filesystem. `/tmp`, `/var/tmp`, and pytest's `tmp_path` are typically node-local.

## 2. Prefect server version mismatch

A pre-existing Prefect server at `:4296` was running `3.6.13` while the pixi client
was `3.6.21`. The websocket event auth protocol changed between versions, so compute
nodes couldn't push task run events back to the server (`EventsWorker` failed).

**Symptom:** Task runs don't appear in the Prefect API after successful SLURM execution.
Compute node stderr shows: `Unable to authenticate to the event stream`.

**Fix:** Always start a fresh ephemeral Prefect server in the test fixture; ignore any
pre-existing `PREFECT_API_URL` from the shell environment.

**Lesson:** Don't reuse external services in tests. Version drift between client and
server causes subtle failures that look like code bugs.

## 3. `prefect.states.Running` is a function, not a type

`isinstance(state, Running)` raises `TypeError` because Prefect's state constructors
(`Running`, `Completed`, `Failed`) are factory functions returning `State` instances,
not classes.

**Symptom:** `TypeError: isinstance() arg 2 must be a type, a tuple of types, or a union`

**Fix:** Use `state.is_running()`, `state.is_completed()`, etc.

**Lesson:** Prefect states are created via factory functions. Always use the `.is_*()`
methods for state checks.

## 4. `timeout=0` treated as falsy in `wait()`

`effective_timeout = timeout or self._max_poll_time` treats `0` as falsy, so
`wait(timeout=0)` waited for `max_poll_time` instead of raising immediately.

**Symptom:** `test_wait_timeout_zero` xfail — wait hangs instead of raising `TimeoutError`.

**Fix:** `timeout if timeout is not None else self._max_poll_time`

**Lesson:** Never use `or` for default values when `0` or `""` are valid inputs.
Use `x if x is not None else default`.

## 5. Cluster-enforced minimum time limit

The cluster rejects `time_limit="00:01:00"` with `sbatch: error: The job time limit
is too short; run longer jobs!`. The timeout detection test needed a job to exceed its
time limit, but the minimum was too high to be practical in CI.

**Symptom:** `FailedJobError: sbatch: error: The job time limit is too short`

**Fix:** Deleted `test_timeout_detection`. Use `slurm_config.time_limit` everywhere
instead of hardcoded values.

**Lesson:** Don't hardcode SLURM parameters in tests. Different clusters have different
policies. Always read from `slurm_config`.

## 6. Unclosed file handles from `subprocess.Popen`

The Prefect server fixture used `stdout=subprocess.PIPE` but never closed the pipes.
With `filterwarnings = ["error"]`, the `ResourceWarning` became a test error during
teardown.

**Symptom:** `ResourceWarning: unclosed file <_io.BufferedReader name=12>` in teardown.

**Fix:** Use `subprocess.DEVNULL` instead of `PIPE` (we don't need the server output).
Added `"ignore::ResourceWarning"` to pytest filterwarnings as a safety net.

## Deleted tests

| Test | Reason |
|---|---|
| `test_timeout_detection` | Cluster minimum time limit makes it impractical (~30 min wait) |
| `test_unpicklable_return_raises_cleanly` | Submitit/Prefect serialize the result before our code sees it |
| `test_cancel_pending_job` | `slurm_begin` flag not supported on this cluster |
