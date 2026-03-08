# Integration Test Failures — Round 2

5 failures from the latest run. Three root causes.

---

## 1. `test_wait_timeout_zero` — `timeout=0` still treated as falsy

**File:** `src/prefect_submitit/futures/base.py:98`

**Root cause:** The fix documented in `integration-test-failures.md` (lesson #4) was never applied. Line 98 still reads:

```python
if effective_timeout and elapsed > effective_timeout:
```

When `timeout=0`, `effective_timeout = 0`, which is falsy in Python. The timeout check is skipped entirely, so the loop polls until the job finishes (600s `max_poll_time`) instead of raising `TimeoutError` immediately.

**Affected scope:** Only `base.py` needs the fix. `SlurmArrayPrefectFuture` inherits `wait()` from the base class. `SlurmBatchedItemFuture.wait()` delegates to the base class. No unit test exists for `timeout=0` — only the integration test catches this.

**Semantics of `timeout=0`:** Enter the loop, call `done()` once. If not done, `elapsed > 0` is immediately true → raises `TimeoutError`. This means "check if done right now, don't wait" — correct behavior.

**Edge cases:**
- Negative timeouts: `elapsed > negative_value` is immediately true → raises. Reasonable.
- `timeout=None` with `max_poll_time=None`: Not reachable given constructor signature.

### Fix

```python
# base.py:98
# Before:
if effective_timeout and elapsed > effective_timeout:
# After:
if effective_timeout is not None and elapsed > effective_timeout:
```

**Confidence:** Certain — pure logic bug, one-line fix.

---

## 2. `test_logs_contain_output` — NFS attribute cache returns stale file content

**File:** `tests/integration/test_environment.py:58-69`, `src/prefect_submitit/futures/base.py:182`

**Root cause:** The test calls `future.logs()` immediately after `future.result()`. The compute node wrote the marker to the stdout log file (verified on disk), but submitit's `Job._get_logs_string()` does a plain `open() + read()`. When the head node already has the file cached from an earlier read during polling, NFS attribute caching (`actimeo`, typically 3-60s) means the kernel serves stale content without re-reading from the server.

**Precedent — artisan-dev NFS patterns:** Two patterns found in `/home/ach94/git/artisan-dev/src/artisan/storage/io/`:

1. **Writer-side flush** (`parquet_writer.py`): `_sync_staging_to_nfs()` calls `os.fsync()` on each file fd and the directory fd after writing.
2. **Reader-side cache invalidation** (`staging_verification.py`): `_invalidate_nfs_dir_cache()` calls `os.listdir()` on ancestor directories to flush dcache entries. `verify_file_exists_nfs()` does `open() + read(1)` to exploit close-to-open consistency. `await_staging_files()` polls with exponential backoff.

### Fix — NFS cache invalidation in `logs()`

Apply the artisan reader-side pattern in `SlurmPrefectFuture.logs()`. Since we can't control the writer side (submitit's worker), we invalidate the cache before reading:

```python
def logs(self) -> tuple[str, str]:
    """Read stdout/stderr logs with NFS cache invalidation."""
    return (
        self._read_log_nfs_safe(self._job.paths.stdout),
        self._read_log_nfs_safe(self._job.paths.stderr),
    )

@staticmethod
def _read_log_nfs_safe(path: Path) -> str:
    """Read a log file after invalidating NFS attribute cache."""
    if not path.exists():
        try:
            os.listdir(path.parent)  # invalidate dir cache
        except OSError:
            pass
        if not path.exists():
            return ""

    # Force attribute cache refresh: open, fstat, close, then re-open to read
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fstat(fd)  # refresh cached attrs
        finally:
            os.close(fd)
    except OSError:
        return ""

    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return ""
```

Requires adding `from pathlib import Path` to base.py imports (`os` is already imported).

**Why library-level, not test-level:** Any user calling `logs()` on NFS would hit the same issue. The fix belongs in the library.

**Confidence:** High — confirmed stale read, proven pattern from artisan-dev.

---

## 3. Prefect API visibility tests (3 failures) — EventsWorker websocket auth failure

**Tests:**
- `test_task_run_in_prefect_api` (`test_submit.py:59`) — `assert 0 >= 1`
- `test_task_run_name_contains_slurm_job_id` (`test_submit.py:88`) — `assert 0 >= 1`
- `test_map_array_task_names_in_api` (`test_map.py:135`) — `assert 0 >= 1`

### Root cause — confirmed from SLURM job stderr logs

Every SLURM job stderr log (checked: 6382611, 6382610, 6382608, 6382587, 6382576, 6382505) contains:

```
Unable to connect to 'ws://acoupa.dhcp.ipd:4296/api/events/in'.
Please check your network settings to ensure websocket connections to the API
are allowed. Otherwise event data (including task run data) may be lost.
Reason: Unable to authenticate to the event stream.
...
Service 'EventsWorker' failed with 4 pending items.
```

**This is NOT a new issue — it's the same problem documented in lesson #2**, but against a *different* stale server. The test fixture starts a fresh server on a new port, but a pre-existing Prefect server at `:4296` (v3.6.13) was still running. The `PREFECT_API_URL` in the environment pointed to the stale server, not the fixture's server.

### How Prefect 3 task run reporting works

In Prefect 3.x, task runs are **not** created via REST API calls. Instead:
1. `SyncTaskRunEngine._create_task_run_locally()` creates a `TaskRun` object **in memory only**
2. State changes are reported via `emit_task_run_state_change_event()` → `EventsWorker` → **websocket** to `ws://<server>/api/events/in`
3. The server-side `task-run-recorder` service processes these events and persists task runs

**When the websocket connection fails → events are dropped → task runs are never recorded → `read_task_runs()` returns nothing.**

### Why the websocket fails

The error `"Unable to authenticate to the event stream"` means the websocket connection was established but the auth handshake failed. The `PrefectEventsClient` sends `{"type": "auth", "token": null}`. A version-mismatched server (3.6.13 vs 3.6.21 client) may reject this handshake due to protocol changes. This matches lesson #2 exactly.

### Env propagation is NOT the issue

Confirmed: `runner.py:217-219` captures environment correctly via `get_current_settings().to_environment_variables() | os.environ`, and `srun --export=ALL` propagates it. The logs show the compute nodes *are* connecting to the right URL — they just can't authenticate.

### Fix — multi-pronged

**Fix A (Required): Kill stale Prefect servers in test fixture**

```python
# conftest.py — in prefect_server fixture, before starting the new server:
subprocess.run(
    ["pkill", "-f", "prefect server start"],
    capture_output=True, check=False,
)
time.sleep(2)
```

This prevents the stale v3.6.13 server from interfering.

**Fix B (Defense): Add retry polling before API queries in tests**

Even with a clean server, Prefect event processing is async. The flow returns results via pickle files (fast), but events may still be in-flight. Add a retry:

```python
client = get_client(sync_client=True)
matching = []
for _ in range(15):
    task_runs = client.read_task_runs()
    matching = [tr for tr in task_runs if str(tr.flow_run_id) == flow_run_id_holder["id"]]
    if matching:
        break
    time.sleep(2)
assert len(matching) >= 1
```

**Confidence:** Certain — error is in every SLURM job log. Same root cause as lesson #2.

---

## Summary

| # | Test | Root Cause | Fix | Complexity |
|---|------|-----------|-----|------------|
| 1 | `test_wait_timeout_zero` | `0` is falsy in `if effective_timeout and ...` | Change to `is not None` | One line |
| 2 | `test_logs_contain_output` | NFS attribute cache returns stale file content | Add `_read_log_nfs_safe()` (artisan pattern) | ~20 lines |
| 3 | 3x Prefect API tests | Stale Prefect server → websocket auth fails → events dropped | Kill stale servers + add retry polling | ~15 lines |

## Next Steps

1. Apply fix #1 — trivial, no risk
2. Apply fix #2 — add NFS cache invalidation to `logs()`, following artisan-dev pattern
3. Apply fix #3A — kill stale Prefect servers in fixture
4. Apply fix #3B — add retry polling in Prefect API tests
5. Re-run integration tests to verify
