# v0.1.6

Local development without a SLURM cluster. This release adds a Docker-based
single-node SLURM environment with full accounting support, plus a fix for
unreliable job state detection in long-running sessions.

## Highlights

- **Docker SLURM dev environment** — Run `pixi run slurm-build && pixi run slurm-up` to get a working SLURM cluster on your laptop. Includes MariaDB + slurmdbd so `sacct` returns real job history.
- **Reliable state detection** — Fixed a bug where submitit's internal polling backoff (up to 10 minutes) caused stale job states, leading to flaky test failures and missed state transitions.

## Full Changelog

See [CHANGELOG.md](CHANGELOG.md) for the complete list of changes.
