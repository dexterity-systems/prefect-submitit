## Description

Bug fixes and test hardening for v0.1.5. Addresses GRES errors on strict SLURM
clusters, incorrect state reporting for completing/completed jobs, and
PREFECT_HOME isolation so the server subprocess and integration tests don't
corrupt the user's default Prefect state.

## Changes

- Skip `--gpus_per_node` when value is 0 to avoid GRES errors on strict SLURM clusters
- Treat SLURM `COMPLETING` state as completed to prevent false state reports
- Return `Completed` state immediately when job result is already available
- Set `PREFECT_HOME` in server subprocess to use `config.data_dir`
- Isolate `PREFECT_HOME` in integration test server fixture
- Fix 9 pre-existing ruff lint violations
- Pin `importlib-metadata < 8.8`

## Checklist

- [x] Code follows the project's style guidelines (`pixi run -e dev fmt`)
- [x] Tests pass locally (`pixi run -e dev test`)
- [x] New/changed behavior is covered by tests
- [x] CHANGELOG.md updated (if user-facing change)
- [x] Self-reviewed the diff
