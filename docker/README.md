# Docker SLURM Environment

A containerized single-node SLURM cluster for local development. Lets you
run `sbatch`, `srun`, `squeue`, and `scancel` on a laptop without cluster
access.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) running

## Quick Start

```bash
# Build the image (first time only, ~2 min):
pixi run slurm-build

# Start the container:
pixi run slurm-up

# Shell in:
pixi run slurm-shell
```

Inside the container (first time per session):

```bash
pixi install
pixi run -- prefect-server start --bg --port 4200
```

Use `pixi run` to access pixi-managed binaries. The explicit `--port 4200`
matches the docker-compose port mapping (the default port is UID-based and
won't be 4200 inside the container).

Run the example:

```bash
pixi run python examples/slurm_submit_and_run.py
```

When done:

```bash
exit
pixi run slurm-down
```

## What Resets When

| Action | What's lost |
|--------|-------------|
| `slurm-down` | Everything — pixi environment, Prefect server, PostgreSQL data. Repeat the first-time setup on next `slurm-up`. |
| `slurm-build` | Only needed after changing the `Dockerfile`. |
| Host source edits | Visible immediately inside the container (volume mount + editable install). No rebuild needed. |
| `pyproject.toml` changes | Re-run `pixi install` inside the container. |

## Environment Details

| Property | Value |
|----------|-------|
| Base image | `nathanhess/slurm:full-v1.2.0` (amd64) |
| SLURM accounting | MariaDB + slurmdbd (`sacct` works) |
| SLURM partition | `debug` (single node, no time limit) |
| Python | 3.12 (via pixi/conda-forge) |
| Prefect server | PostgreSQL-backed, port 4200 |
| Architecture | amd64 only; runs under QEMU on Apple Silicon (~3-5x slower) |

## Useful Commands Inside the Container

```bash
sinfo                       # Show cluster status
squeue                      # Show running/pending jobs
sacct -j <jobid>            # Show job history and resource usage
srun hostname               # Run a trivial job
scancel <job_id>            # Cancel a job
pixi run -e dev test-sbatch-docker  # Run sbatch integration tests
pixi run -e dev test-srun-docker   # Run srun integration tests (via salloc)
prefect-server status       # Check Prefect server health
```
