# prefect-submitit

A [Prefect 3](https://docs.prefect.io/) TaskRunner that submits tasks to
[SLURM](https://slurm.schedmd.com/) clusters via
[submitit](https://github.com/facebookincubator/submitit).

## Features

- **Single task submission** -- submit individual Prefect tasks as SLURM jobs
- **Job arrays** -- submit `task.map()` calls as SLURM job arrays with automatic
  chunking when the array exceeds cluster limits
- **Batched execution** -- group multiple items per SLURM job with
  `units_per_worker` to reduce scheduling overhead
- **Local mode** -- swap to local execution for testing without changing your
  flow code (`execution_mode="local"` or `SLURM_TASKRUNNER_BACKEND=local`)
- **Prefect UI integration** -- task run names include SLURM job IDs for easy
  cross-referencing with `squeue`/`sacct`

## Installation

```bash
pip install prefect-submitit
```

Or with conda (after the first conda-forge release):

```bash
conda install -c conda-forge prefect-submitit
```

## Quick Start

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

## Configuration

```python
SlurmTaskRunner(
    partition="gpu",  # SLURM partition
    time_limit="04:00:00",  # Wall time (HH:MM:SS)
    mem_gb=16,  # Memory per job
    gpus_per_node=1,  # GPUs per job
    units_per_worker=10,  # Items per SLURM job (batched mode)
    execution_mode="slurm",  # "slurm" or "local"
    slurm_array_parallelism=100,  # Max concurrent array tasks
    log_folder="slurm_logs",  # Where submitit writes logs
    fail_on_error=True,  # Raise on SLURM job failure
)
```

Any additional keyword arguments are passed through to submitit (e.g.
`slurm_gres="gpu:a100:1"`).

## Local Testing

Set the environment variable to skip SLURM entirely:

```bash
export SLURM_TASKRUNNER_BACKEND=local
```

Or pass it directly:

```python
SlurmTaskRunner(execution_mode="local")
```

## Development

Requires [Pixi](https://pixi.sh):

```bash
pixi install
pixi run -e dev fmt
pixi run -e dev test
```

### Prefect Server

The repo includes scripts to run a local Prefect server backed by PostgreSQL
(handles SLURM concurrency better than SQLite). The server uses a UID-based
port to avoid conflicts on shared nodes.

```bash
pixi run prefect-start   # Start in background (PostgreSQL + Prefect)
pixi run prefect-stop    # Stop the server
```

The start script automatically:
- Initializes PostgreSQL on first run (stored in `.prefect-postgres/`)
- Picks a UID-based port (range 4200-4999) to avoid conflicts
- Uses the node's FQDN so SLURM workers on compute nodes can reach it
- Writes a discovery file to `~/.prefect-submitit/prefect/server.json`
- Tunes connection pool sizes for high-concurrency SLURM workloads

Use `--sqlite` if you don't need PostgreSQL:

```bash
./scripts/prefect/start_prefect_server.sh --sqlite
```

### IDE Setup (VSCode)

**Python Interpreter:** Set the Pixi environment as your VSCode Python
interpreter:

```bash
pixi run which python
# Example output: /home/user/prefect-submitit/.pixi/envs/default/bin/python
```

In VSCode: `Ctrl+Shift+P` → "Python: Select Interpreter" → paste the path.

**Jupyter Kernel:** Register the Pixi environment as a Jupyter kernel so
notebooks use the correct packages:

```bash
pixi run install-kernel
```

In VSCode: open a `.ipynb` file → click "Select Kernel" → choose
**Prefect-Submitit**.

> **Tip:** If the Jupyter kernel takes 30+ seconds to start in VS Code, the
> `Python Environments` extension (`ms-python.vscode-python-envs`) is likely
> the cause. Uninstall it — the core Python extension works fine without it.
> Tracked upstream:
> [microsoft/vscode-python#25804](https://github.com/microsoft/vscode-python/issues/25804)

### Running Demos

The `examples/` directory contains demo notebooks. After setting up:

```bash
pixi install               # Install all dependencies
pixi run install-kernel    # Register Jupyter kernel
pixi run prefect-start     # Start Prefect server
```

Then open any notebook in `examples/` and select the **Prefect-Submitit** kernel.

## License

BSD 3-Clause. See [LICENSE](LICENSE) for details.
