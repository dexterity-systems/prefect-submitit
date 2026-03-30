# v0.1.5

Bug fixes for SLURM job submission and state reporting, plus isolation
improvements for the managed Prefect server.

## Highlights

- **GRES fix** — `--gpus_per_node` is no longer passed when set to 0, avoiding
  errors on strict SLURM clusters that reject empty GRES requests.
- **State reporting** — Jobs in SLURM `COMPLETING` state are now correctly
  reported as completed, and jobs whose results are already available return
  `Completed` immediately instead of re-querying SLURM.
- **PREFECT_HOME isolation** — The managed Prefect server subprocess now uses
  `config.data_dir` as its `PREFECT_HOME`, preventing it from reading or
  writing to the user's default `~/.prefect/` directory.

## Full Changelog

See [CHANGELOG.md](CHANGELOG.md) for the complete list of changes.
