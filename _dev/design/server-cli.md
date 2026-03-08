# Design: Prefect Server CLI Entry Point

**Date:** 2026-03-08
**Status:** Draft

## Problem

The Prefect server lifecycle scripts (`start_prefect_server.sh`,
`stop_prefect_server.sh`, `init_postgres.sh`) live in `scripts/prefect/`. This
directory is not included in the Python wheel, so downstream consumers (like
artisan) cannot use them after `pip install prefect-submitit`. They must either:

- Copy the scripts into their own repo (duplication)
- Reference them by filesystem path (fragile, assumes repo checkout)

The scripts also hardcode `$PROJECT_ROOT/.pixi/envs/default/bin/` paths for
PostgreSQL and Prefect binaries, coupling them to a specific environment layout.

## Goal

Make Prefect server management available as a Python CLI entry point that ships
with the `prefect-submitit` package. After installation, users run:

```bash
prefect-server start           # foreground, postgres backend
prefect-server start --bg      # background
prefect-server start --sqlite  # sqlite backend
prefect-server start --restart # stop existing, start fresh
prefect-server stop            # stop server
prefect-server stop --all      # also stop postgres
prefect-server init-db         # initialize postgres
prefect-server init-db --reset # reset postgres data
prefect-server status          # show running server info
```

Downstream projects just declare `prefect-submitit` as a dependency and get
server management for free.

## Scope

### In scope

- Python CLI entry point replacing all three shell scripts
- Server discovery module (read/write discovery file, health check)
- PostgreSQL lifecycle management (init, start, stop)
- Prefect server lifecycle management (start, stop, restart)
- `[project.scripts]` entry point in pyproject.toml

### Out of scope

- Changing how `SlurmTaskRunner` connects to Prefect (it already uses
  `PREFECT_API_URL`)
- Docker/containerized deployment
- Multi-server management

---

## Design

### Package structure

```
src/prefect_submitit/
├── server/
│   ├── __init__.py
│   ├── cli.py              # Entry point: argument parsing, dispatch
│   ├── config.py            # Port calculation, paths, constants
│   ├── discovery.py         # Discovery file read/write, health check
│   ├── postgres.py          # PostgreSQL init, start, stop
│   └── prefect_proc.py      # Prefect server start, stop, status
├── __init__.py
├── constants.py
├── executors.py
├── ...
```

### Entry point registration

```toml
# pyproject.toml
[project.scripts]
prefect-server = "prefect_submitit.server.cli:main"
```

### Module responsibilities

**`config.py`** — Configuration and path resolution.

```python
@dataclass(frozen=True)
class ServerConfig:
    port: int                  # UID-based: 4200 + (uid % 800)
    host: str                  # FQDN for cross-node access
    api_url: str               # http://{host}:{port}/api
    discovery_dir: Path        # ~/.prefect-submitit/
    discovery_file: Path       # ~/.prefect-submitit/server.json
    pg_data_dir: Path          # ~/.prefect-submitit/postgres/
    pg_port: int               # 5433
    pg_user: str               # "prefect"
    pg_database: str           # "prefect"
    log_dir: Path              # ~/.prefect-submitit/logs/
```

Key change from shell scripts: paths are under `~/.prefect-submitit/` instead
of `$PROJECT_ROOT/`. This decouples data from any specific repo checkout.

Binary resolution: find `prefect`, `pg_ctl`, `initdb`, etc. on `$PATH` via
`shutil.which()`. No more hardcoded pixi paths. If a binary is missing, raise
a clear error with install instructions.

**`discovery.py`** — Discovery file management and health checks.

This replaces artisan's `src/artisan/orchestration/prefect_server.py`. The
resolution order stays the same:

```
1. Explicit argument
2. PREFECT_SUBMITIT_SERVER env var (was ARTISAN_PREFECT_SERVER)
3. PREFECT_API_URL env var
4. Discovery file (~/.prefect-submitit/server.json)
5. Raise with instructions
```

Artisan's module becomes a thin re-export or is removed entirely in favor of
importing from `prefect_submitit.server.discovery`.

**`postgres.py`** — PostgreSQL lifecycle.

Functions: `init_db()`, `start()`, `stop()`, `reset()`, `is_running()`,
`ensure_db_exists()`. All use `subprocess.run()` with binaries found via
`shutil.which()`.

The PostgreSQL config block (`max_connections`, `shared_buffers`, pool tuning)
is written programmatically, same sentinel-bounded approach as the shell script.

**`prefect_proc.py`** — Prefect server lifecycle.

Functions: `start()`, `stop()`, `status()`, `is_running()`. Uses
`subprocess.Popen` for background mode, sets the same environment variables
the shell scripts set (`PREFECT_SERVER_API_HOST`, `PREFECT_UI_API_URL`,
connection pool tuning, database URL).

**`cli.py`** — Argument parsing and dispatch.

Uses `argparse` (no external dependency). Subcommands: `start`, `stop`,
`init-db`, `status`.

```python
def main() -> None:
    parser = argparse.ArgumentParser(prog="prefect-server")
    sub = parser.add_subparsers(dest="command", required=True)

    start = sub.add_parser("start")
    start.add_argument("--bg", action="store_true")
    start.add_argument("--sqlite", action="store_true")
    start.add_argument("--restart", action="store_true")
    start.add_argument("--port", type=int)
    start.add_argument("--pg-port", type=int)

    stop = sub.add_parser("stop")
    stop.add_argument("--all", action="store_true")
    stop.add_argument("-f", "--force", action="store_true")

    init_db = sub.add_parser("init-db")
    init_db.add_argument("--reset", action="store_true")

    sub.add_parser("status")

    args = parser.parse_args()
    # dispatch to appropriate function
```

### Discovery file format

Same JSON format, updated path:

```json
{
    "url": "http://node01.cluster:4242/api",
    "host": "node01.cluster",
    "port": 4242,
    "pid": 12345,
    "user": "username",
    "backend": "postgres",
    "started": "2026-03-08T10:30:00Z"
}
```

Written on server start, removed on server stop. Two changes from the shell
scripts:

- **Path change**: the discovery file moves from
  `~/.prefect-submitit/prefect/server.json` to `~/.prefect-submitit/server.json`
  (the `prefect/` subdirectory is removed since `~/.prefect-submitit/` is already
  scoped to this project).
- **`project_root` field dropped**: no longer needed since data lives in
  `~/.prefect-submitit/` instead of `$PROJECT_ROOT/`. Any downstream code that
  reads this field (e.g., artisan's `prefect_server.py`) must be updated.

### Data directory layout

```
~/.prefect-submitit/
├── server.json              # Discovery file
├── postgres/                # PostgreSQL data directory
│   ├── PG_VERSION
│   ├── postgresql.conf
│   ├── postgres.log
│   └── ...
└── logs/                    # Prefect server logs
    └── prefect-server-20260308-103000.log
```

### Migration of existing data

The shell scripts store PostgreSQL data at `$PROJECT_ROOT/.prefect-postgres`.
The Python CLI moves this to `~/.prefect-submitit/postgres/`. On first run,
`init-db` and `start` (with postgres backend) check for an existing data
directory at the new location. If absent, they initialize fresh.

Users with existing data can migrate manually:

```bash
mv $PROJECT_ROOT/.prefect-postgres ~/.prefect-submitit/postgres
```

The CLI does **not** auto-migrate or auto-detect old data — this keeps the code
simple and the old location is project-specific (users may have multiple
checkouts). The shell scripts continue to work with the old location until they
are removed, so there is no breakage during the transition.

### Migration from shell scripts

The shell scripts remain in the repo temporarily for backwards compatibility
but are no longer the primary interface. The pixi tasks switch to the Python
entry point:

```toml
# Before
[tool.pixi.tasks]
prefect-start = "./scripts/prefect/start_prefect_server.sh --bg"
prefect-stop = "./scripts/prefect/stop_prefect_server.sh -f"

# After
[tool.pixi.tasks]
prefect-start = "prefect-server start --bg"
prefect-stop = "prefect-server stop --force"
```

Shell scripts can be deleted once the Python CLI is validated.

### Impact on artisan

Artisan currently has:

- `src/artisan/orchestration/prefect_server.py` — discovery + health check
- `scripts/prefect/` — shell scripts (already migrated to prefect-submitit)

After this change:

- `prefect_server.py` imports from `prefect_submitit.server.discovery`
  (or is replaced entirely)
- `scripts/prefect/` is deleted from artisan
- artisan declares `prefect-submitit` as a dependency (it already does for
  `SlurmTaskRunner`)

---

## Dependencies

No new dependencies. The CLI uses only stdlib:

- `argparse` for CLI parsing
- `subprocess` for process management
- `shutil.which` for binary resolution
- `socket.getfqdn` for hostname
- `json` for discovery file
- `os` / `pathlib` / `signal` for process lifecycle

---

## Test plan

- **Unit tests** for `config.py`: port calculation, path resolution, binary
  lookup with mocked `shutil.which`
- **Unit tests** for `discovery.py`: resolution order, health check
  success/failure, discovery file read/write
- **Unit tests** for `postgres.py`: mock subprocess calls, verify correct
  `pg_ctl`/`initdb` invocations
- **Unit tests** for `prefect_proc.py`: mock subprocess calls, verify env
  vars set correctly, discovery file written/removed
- **Unit tests** for `cli.py`: argument parsing, subcommand dispatch
- **Integration test**: `prefect-server start --bg --sqlite` → health check
  → `prefect-server stop` (requires Prefect installed, no SLURM needed)

---

## Rollout

- Implement `server/` subpackage with CLI
- Update pixi tasks to use `prefect-server` command
- Validate on SLURM cluster
- Remove shell scripts from `scripts/prefect/`
- Update artisan to use `prefect_submitit.server.discovery`
- Remove `scripts/prefect/` from artisan

---

## Resolved questions

- **Command name**: `prefect-server`. Prefect's own CLI is `prefect` (with
  `prefect server start` as a subcommand), so no collision. If Prefect ever
  ships a `prefect-server` binary, we'd know well in advance since we depend
  on their package. Rename at that point if needed.

- **Data directory location**: `~/.prefect-submitit/`. One flat location is
  easier to find, explain, and debug over SSH on HPC clusters where XDG
  conventions are less established. The data (postgres, logs, discovery file)
  is tightly coupled — splitting across XDG dirs adds complexity with no
  benefit.

- **PostgreSQL as optional**: Same fail-fast approach as the shell scripts.
  `shutil.which("pg_ctl")` is checked when postgres is actually needed
  (`start` without `--sqlite`, or `init-db`). Clear error with install
  instructions if missing. No conditional imports or extras groups. If you
  use `--sqlite` or point `PREFECT_API_URL` at an existing server, none of
  the postgres code runs.
