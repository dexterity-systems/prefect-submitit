# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.5] - 2026-03-30

### Added

- PostgreSQL version mismatch detection: compares data directory version against
  the installed binary and raises a clear error with remediation steps before
  attempting to start.
- Prefect version change tracking: records the Prefect version on successful
  server start and warns on the next startup if the version changed.
- Background startup failure diagnostics: detects when the Prefect server
  process dies during startup and surfaces the last 20 lines of the log file
  in the error message.
- Timeout vs. crash distinction: when the server doesn't become healthy,
  the error message now differentiates between "process died" (with log tail)
  and "process still running" (with monitoring instructions).
- `prefect_version` field in the discovery file (`server.json`) for
  `prefect-server status` display.

### Fixed

- Discovery file is no longer written before the server is confirmed healthy
  in background mode, preventing stale discovery files from persisting after
  failed startups.
- `subprocess.CalledProcessError` from PostgreSQL commands (`initdb`,
  `pg_ctl`, `createdb`) now surfaces stderr in readable error messages
  instead of raw Python tracebacks.
- `pg_ctl stop` in the `init_db` cleanup path changed from `check=True`
  to `check=False` to avoid masking the original error.
- Skip `--gpus_per_node` when value is 0 to avoid GRES errors on strict SLURM clusters
- Treat SLURM `COMPLETING` state as completed to prevent false state reports
- Return `Completed` state immediately when job result is already available
- Set `PREFECT_HOME` in server subprocess to use `config.data_dir`, preventing
  corruption of the user's default `~/.prefect/` directory
- Isolate `PREFECT_HOME` in integration test server fixture to avoid polluting
  the developer's Prefect state

### Changed

- Fix 9 pre-existing ruff lint violations so `pixi run -e dev fmt` passes cleanly
- Pin `importlib-metadata < 8.8` to avoid compatibility issues

## [0.1.4] - 2026-03-10

### Added

- `prefect-server` CLI for managing a local PostgreSQL-backed Prefect server on
  shared HPC nodes (`start`, `stop`, `status`, `init-db`).
- Python API for server management (`prefect_submitit.server.start/stop/status`).
- UID-based port allocation (4200–4999) to avoid conflicts on multi-user nodes.
- Discovery file (`~/.prefect-submitit/server.json`) for cross-node worker
  resolution.
- Improved SLURM cancellation detection reliability.
- Reduced default SLURM poll interval from 30s to 5s.
- Five example notebooks covering single tasks, job arrays, batched execution,
  error handling, and local development mode.
- Portable SLURM integration test suite.
- `make_slurm_runner` factory fixture for integration tests with SLURM job naming.
- Unit test suite for executors, array futures, and server module.
- CONTRIBUTING.md, CODE_OF_CONDUCT.md, and SECURITY.md community health files.
- CLAUDE.md contributor guide.

### Fixed

- NFS-safe log reads with cache invalidation and retry in
  `SlurmPrefectFuture.logs()`.
- `wait(timeout=0)` no longer falls through to the default max poll time.
- Integration tests use shared filesystem for SLURM logs instead of `tmp_path`.
- `require_binary()` falls back to interpreter's bin directory for Jupyter
  kernel environments where PATH is incomplete.
- `prefect-server start` is now idempotent — skips startup when the server is
  already running and healthy.
- `default_host()` falls back to the short-hostname IP when the FQDN is
  unresolvable.
- Server PID retrieval now filters by current user for multi-user safety.
- Detect and kill orphan PostgreSQL processes holding the configured port.
- Always stop PostgreSQL when stopping Prefect server.
- README: corrected discovery file and PostgreSQL data directory paths; added
  direct CLI reference, server discovery docs, and missing configuration params
  (`poll_interval`, `max_poll_time`, `max_array_size`).
- README: added Examples and Integration Tests sections with prominent
  placement after Configuration.

### Changed

- Enhanced database initialization with error handling and proper shutdown.
- Improved type safety and fixed mypy/linting issues.
- Hardened CI pipeline.
- Updated pyproject.toml dependencies and linting configurations.
- Linked PyPI package to conda-forge.
- Drain Prefect background workers before test server teardown.
- Session-end `scancel` scoped to test-submitted jobs only.

## [0.1.3] - 2026-02-24

### Changed

- Updated PyPI project URLs.
- Version bump to validate GitHub Actions release workflow.

## [0.1.2] - 2026-02-24

### Added

- Initial public release.
- `SlurmTaskRunner` for submitting Prefect tasks to SLURM via submitit.
- Single task submission as individual SLURM jobs.
- Job array support for `task.map()` with automatic chunking.
- Batched execution via `units_per_worker`.
- Local execution mode for testing without SLURM.
- Prefect UI integration with SLURM job ID cross-referencing.
- CI/CD with lint, test, build, and release-to-PyPI workflows.
- Pre-commit hooks (ruff, mypy, codespell, repo-review).
- Dependabot for GitHub Actions.

[Unreleased]:
  https://github.com/dexterity-systems/prefect-submitit/compare/v0.1.5...HEAD
[0.1.5]:
  https://github.com/dexterity-systems/prefect-submitit/compare/v0.1.4...v0.1.5
[0.1.4]:
  https://github.com/dexterity-systems/prefect-submitit/compare/v0.1.3...v0.1.4
[0.1.3]:
  https://github.com/dexterity-systems/prefect-submitit/compare/v0.1.2...v0.1.3
[0.1.2]:
  https://github.com/dexterity-systems/prefect-submitit/releases/tag/v0.1.2
