## Description

Add a containerized single-node SLURM cluster for local development, so
contributors can run `sbatch`, `squeue`, `sacct`, and the full integration
test suite on a laptop without cluster access. Also fixes a reliability bug
in SLURM job state detection.

## Changes

- **Docker SLURM environment** (`docker/`): Dockerfile, entrypoint, compose,
  and slurmdbd config based on `nathanhess/slurm:full-v1.2.0` with MariaDB +
  slurmdbd for full accounting support.
- **Pixi tasks**: `slurm-build`, `slurm-up`, `slurm-down`, `slurm-shell`,
  and `test-slurm-docker` for one-command Docker workflow.
- **Example script**: `examples/slurm_submit_and_run.py` for the Docker
  environment.
- **State detection fix**: Force-refresh submitit's sacct watcher on each poll
  iteration to prevent stale cached states from causing flaky failures in
  long-running sessions.
- **CLAUDE.md**: Added Docker commands and SLURM test marks.
- **CHANGELOG.md**: v0.1.6 entry.

## Checklist

- [x] Code follows the project's style guidelines (`pixi run -e dev fmt`)
- [x] Tests pass locally (`pixi run -e dev test`)
- [x] New/changed behavior is covered by tests
- [x] CHANGELOG.md updated (if user-facing change)
- [x] Self-reviewed the diff
