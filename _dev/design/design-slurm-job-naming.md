# Design Doc: Use Pytest Test Name as SLURM Job Name in Integration Tests

## Goal

When integration tests submit SLURM jobs, set the SLURM job name (`#SBATCH --job-name`) to the pytest test name (e.g., `test_sync_task_roundtrip`). This makes it straightforward to correlate `squeue` output, `sacct` history, and SLURM log files back to the specific test that submitted each job.

## Current Behavior

The `slurm_runner` fixture (line 180 of `tests/integration/conftest.py`) creates a `SlurmTaskRunner` without passing `slurm_job_name`. As a result, submitit uses its own default job naming (typically `submitit`), making all test jobs indistinguishable in `squeue` and SLURM accounting.

The fixture signature is `def slurm_runner(slurm_config)` -- it does not take the pytest `request` fixture, so it has no access to the test node ID.

## Proposed Approach

**Single change in one file:** `tests/integration/conftest.py`, the `slurm_runner` fixture.

1. Add `request` to the fixture's parameter list.
2. Extract the test name from `request.node.name` (e.g., `test_sync_task_roundtrip` or `test_map_small_array`).
3. Sanitize the name: SLURM job names should be short and avoid special characters. Truncate to a reasonable length (e.g., 50 chars) and replace any characters outside `[a-zA-Z0-9_-]` with `_`.
4. Pass `slurm_job_name=sanitized_name` to `SlurmTaskRunner(...)`.

The resulting fixture would look like:

```python
@pytest.fixture
def slurm_runner(slurm_config, request):
    """A configured SlurmTaskRunner for testing."""
    import re

    log_dir = slurm_config.log_dir / "slurm_logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # Derive SLURM job name from the pytest test name
    raw_name = request.node.name
    sanitized = re.sub(r"[^a-zA-Z0-9_-]", "_", raw_name)[:50]

    extra_kwargs = {}
    if slurm_config.account:
        extra_kwargs["slurm_account"] = slurm_config.account
    if slurm_config.qos:
        extra_kwargs["slurm_qos"] = slurm_config.qos

    return SlurmTaskRunner(
        partition=slurm_config.partition,
        time_limit=slurm_config.time_limit,
        mem_gb=slurm_config.mem_gb,
        gpus_per_node=0,
        poll_interval=2.0,
        max_poll_time=slurm_config.max_wait + 300,
        log_folder=str(log_dir),
        slurm_job_name=sanitized,
        **extra_kwargs,
    )
```

**How it flows through:**
- `slurm_job_name=sanitized` is captured in `self.slurm_kwargs` (line 97 of `runner.py`).
- In `__enter__`, `params.update(self.slurm_kwargs)` (line 158) merges it into the params dict.
- `self._executor.update_parameters(**params)` passes it to submitit's `AutoExecutor`, which sets `#SBATCH --job-name`.

No changes to production code are needed. This is purely a test infrastructure improvement.

## Edge Cases

- **Parameterized tests:** `request.node.name` includes the parameter suffix, e.g., `test_foo[param1]`. The brackets get sanitized to underscores (`test_foo_param1_`), which is fine and actually useful for differentiation.
- **Tests that create their own `SlurmTaskRunner`:** Some tests (e.g., `test_map_exceeding_max_array_size` in `test_map.py` line 49) construct a `SlurmTaskRunner` directly instead of using the fixture. These will not get the job name automatically. This is acceptable; the fixture covers the majority of tests.
- **Multiple jobs per test:** A single test may submit multiple SLURM jobs (via `.submit()` or `.map()`). All jobs from the same test will share the same SLURM job name, which is the desired behavior -- it groups them by test.
- **Name length:** SLURM allows job names up to 1024 characters; truncating at 50 is conservative and keeps `squeue` output readable.

## Alternatives Considered

1. **Set job name at the `SlurmTaskRunner.__enter__` level** -- Would require threading test context into production code. The fixture approach keeps the change test-only.
2. **Use `request.node.nodeid` instead of `request.node.name`** -- Too long for useful `squeue` output.
3. **Add a prefix like `pytest-`** -- Could help filter test jobs but adds length without much benefit.
4. **Use `log_folder` per-test instead** -- Helps with log correlation but is a larger change and orthogonal.
