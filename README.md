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
pixi run -e dev test
pixi run -e dev fmt
```

## License

BSD 3-Clause. See [LICENSE](LICENSE) for details.
