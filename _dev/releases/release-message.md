# v0.1.6

This release makes server startup failures much easier to diagnose. When
PostgreSQL or Prefect versions change (common after environment updates),
`prefect-server start` now tells you exactly what went wrong and how to fix it.

## Highlights

- **PostgreSQL version mismatch detection** — If the PG binary version doesn't
  match the data directory, you get a clear error pointing to
  `prefect-server init-db --reset` instead of a cryptic traceback.
- **Prefect version change warnings** — The server now tracks the Prefect
  version across restarts and warns you upfront when a version change is
  detected, so you know migrations will run.
- **Better startup failure diagnostics** — Background mode now detects when the
  server process dies during startup and shows the last 20 lines of the log
  file in the error message. Timeout errors now distinguish between "process
  crashed" and "process still starting."
- **No more stale discovery files** — The discovery file (`server.json`) is now
  written only after the server is confirmed healthy, preventing stale entries
  from misleading subsequent status checks.

## Full Changelog

See [CHANGELOG.md](CHANGELOG.md) for the complete list of changes.
