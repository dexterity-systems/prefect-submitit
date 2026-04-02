# prefect-submitit

A [Prefect 3](https://docs.prefect.io/) TaskRunner that submits tasks to
[SLURM](https://slurm.schedmd.com/) clusters via
[submitit](https://github.com/facebookincubator/submitit).

## Features

- **sbatch submission** -- submit individual tasks as SLURM jobs and
  `task.map()` calls as job arrays, with automatic chunking when arrays exceed
  cluster limits
- **srun submission** -- run tasks as `srun` steps within an existing
  allocation (`salloc`), avoiding per-task scheduling overhead
- **Batched execution** -- group multiple items per SLURM job with
  `units_per_worker` to reduce scheduling overhead (works with both sbatch
  and srun modes)
- **Local mode** -- swap to local execution for development without changing
  your flow code
- **Prefect UI integration** -- task run names include SLURM job IDs for easy
  cross-referencing with `squeue`/`sacct`

## Requirements

- Python >= 3.12
- Prefect >= 3.6, < 4.0
- SLURM cluster (for sbatch/srun modes) or Docker (for local development)

## Installation

```bash
pip install prefect-submitit
```

With [pixi](https://pixi.sh):

```bash
pixi add prefect-submitit
```

With conda:

```bash
conda install -c conda-forge prefect-submitit
```

## Quick start

```python
from prefect import flow, task
from prefect_submitit import SlurmTaskRunner


@task
def add(x: int, y: int) -> int:
    return x + y


@flow(task_runner=SlurmTaskRunner(partition="cpu", time_limit="00:10:00"))
def my_flow():
    # Single task
    future = add.submit(1, 2)
    print(future.result())  # 3

    # Map over inputs (submitted as a SLURM job array)
    futures = add.map([1, 2, 3], [4, 5, 6])
    print([f.result() for f in futures])  # [5, 7, 9]


if __name__ == "__main__":
    my_flow()
```

## Execution modes

The runner supports three execution modes, selected via the `execution_mode`
parameter or the `SLURM_TASKRUNNER_BACKEND` environment variable (`slurm`,
`srun`, or `local`):

| Mode | Dispatch | Requires | Best for |
|------|----------|----------|----------|
| `slurm` | sbatch | SLURM access | Batch workloads |
| `srun` | srun | Active allocation (`SLURM_JOB_ID`) | Interactive / low-latency |
| `local` | None | Nothing | Development and testing |

### slurm (default)

Each `.submit()` becomes a SLURM job via `sbatch`. Each `.map()` becomes a job
array with automatic chunking when the array exceeds cluster limits.

```python
SlurmTaskRunner(execution_mode="slurm", partition="gpu", gpus_per_node=1)
```

### srun

Runs tasks as `srun` steps inside an existing SLURM allocation. Requires a
prior `salloc` or `sbatch` session with `SLURM_JOB_ID` set. Avoids per-task
scheduling overhead, making it suited for many small tasks sharing a single
allocation.

```bash
salloc -N2 --mem=32G --time=02:00:00 -- python my_flow.py
```

```python
SlurmTaskRunner(execution_mode="srun", mem_gb=4, cpus_per_task=2)
```

### local

Runs tasks locally via submitit's `LocalExecutor`. SLURM parameters are
ignored. For development and testing without a cluster.

```python
SlurmTaskRunner(execution_mode="local")
```

Or via environment variable:

```bash
export SLURM_TASKRUNNER_BACKEND=local
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `partition` | `"cpu"` | SLURM partition |
| `time_limit` | `"01:00:00"` | Wall time (HH:MM:SS) |
| `mem_gb` | `4` | Memory per job in GB |
| `gpus_per_node` | `0` | GPUs per job |
| `cpus_per_task` | `1` | CPUs per task |
| `units_per_worker` | `1` | Items per SLURM job (>1 enables batched execution) |
| `slurm_array_parallelism` | `1000` | Max concurrent array tasks |
| `execution_mode` | `None` | `"slurm"`, `"srun"`, or `"local"`. Falls back to `SLURM_TASKRUNNER_BACKEND` env var, then `"slurm"` |
| `poll_interval` | mode-dependent | Seconds between status checks (slurm=5.0, srun=0.5, local=1.0) |
| `max_poll_time` | `None` | Max seconds to poll before timing out. Default: time_limit × 2 |
| `log_folder` | `"slurm_logs"` | Directory for submitit logs |
| `fail_on_error` | `True` | Raise on SLURM job failure |
| `max_array_size` | `None` | Override auto-detected cluster MaxArraySize |
| `srun_launch_concurrency` | `128` | Max concurrent srun steps (srun mode only) |

Additional keyword arguments are passed through to submitit (e.g.
`slurm_gres="gpu:a100:1"`).

## Examples

The `examples/` directory contains Jupyter notebooks demonstrating each feature
on a real SLURM cluster:

| Notebook | Covers |
|----------|--------|
| `01_single_task_submission` | Submitting individual tasks as SLURM jobs |
| `02_job_arrays_with_map` | `task.map()` with automatic job array chunking |
| `03_batched_execution` | Grouping items per job with `units_per_worker` |
| `04_error_handling_and_cancellation` | Failure propagation and job cancellation |
| `05_local_mode_and_development` | Local execution mode for dev/testing |
| `slurm_submit_and_run.py` | Minimal script for the Docker SLURM environment |

To run the notebooks: install dependencies, register the Jupyter kernel, and
start the Prefect server (see [Development](#development) below), then open any
notebook and select the **Prefect-Submitit** kernel.

## Prefect server

The repo includes a `prefect-server` CLI to run a local Prefect server backed
by PostgreSQL (handles SLURM concurrency better than SQLite). The server uses
UID-based port allocation to avoid conflicts on shared nodes.

```bash
pixi run prefect-start   # Start in background (PostgreSQL + Prefect)
pixi run prefect-stop    # Stop the server
```

The CLI automatically:
- Initializes PostgreSQL on first run (stored in `~/.prefect-submitit/postgres/`)
- Picks UID-based ports: Prefect on even ports (4200--5798), PostgreSQL on odd
  ports (5433--7031)
- Uses the node's FQDN so SLURM workers can reach it (falls back to IP if
  FQDN is unresolvable)
- Writes a discovery file to `~/.prefect-submitit/server.json`
- Tunes connection pool sizes for high-concurrency SLURM workloads

**Direct CLI**

```bash
prefect-server start [--bg] [--sqlite] [--restart] [--port N] [--pg-port N]
prefect-server stop [-f]
prefect-server status
prefect-server init-db [--reset]
```

- `--sqlite` uses SQLite instead of PostgreSQL
- `--restart` stops any existing server before starting
- `start` is idempotent -- skips if the server is already healthy

**Server discovery**

Workers resolve the Prefect API URL in this order: `PREFECT_SUBMITIT_SERVER`
env var → `PREFECT_API_URL` env var → discovery file (auto-written by
`prefect-server start`).

## Development

Requires [pixi](https://pixi.sh):

```bash
pixi install
pixi run -e dev fmt       # Format and lint
pixi run -e dev test      # Run unit tests
```

### Docker SLURM environment

A containerized single-node SLURM cluster is included for development and
integration testing without access to a real cluster:

```bash
pixi run slurm-build     # Build the Docker image
pixi run slurm-up        # Start the SLURM container
pixi run slurm-shell     # Shell into the running container
pixi run slurm-down      # Stop and remove the container
```

See [`docker/README.md`](docker/README.md) for details.

### Integration tests

Integration tests submit real SLURM jobs and are gated behind `--run-slurm`.
Tests are split by submission mode:

```bash
# sbatch tests (standard SLURM submission)
pixi run -e dev test-sbatch          # On a real cluster
pixi run -e dev test-sbatch-docker   # In the Docker environment

# srun tests (within an allocation)
pixi run -e dev test-srun            # On a real cluster (wraps in salloc)
pixi run -e dev test-srun-docker     # In the Docker environment (wraps in salloc)
```

Tests cover single submission, job arrays, batched execution, cancellation,
failure handling, polling, and environment propagation.

### IDE setup (VS Code)

**Python interpreter:** Set the pixi environment as your VS Code Python
interpreter:

```bash
pixi run which python
# Example: /home/user/prefect-submitit/.pixi/envs/default/bin/python
```

In VS Code: `Ctrl+Shift+P` → "Python: Select Interpreter" → paste the path.

**Jupyter kernel:** Register the pixi environment as a Jupyter kernel so
notebooks use the correct packages:

```bash
pixi run install-kernel
```

In VS Code: open a `.ipynb` file → click "Select Kernel" → choose
**Prefect-Submitit**.

## License

BSD 3-Clause. See [LICENSE](LICENSE) for details.
