# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Unit test suite for executors and array futures.

### Changed

- Improved type safety and fixed mypy/linting issues.
- Hardened CI pipeline.
- Updated pyproject.toml dependencies and linting configurations.
- Linked PyPI package to conda-forge.

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
  https://github.com/dexterity-systems/prefect-submitit/compare/v0.1.3...HEAD
[0.1.3]:
  https://github.com/dexterity-systems/prefect-submitit/compare/v0.1.2...v0.1.3
[0.1.2]:
  https://github.com/dexterity-systems/prefect-submitit/releases/tag/v0.1.2
