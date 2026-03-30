## Summary

Detect PostgreSQL and Prefect version changes at server startup and provide
actionable error messages instead of raw tracebacks or generic timeouts. Also
fixes stale discovery files left behind after failed background startups.

## Changes

- Add PG data directory vs binary version check before `pg_ctl start`, with
  clear "version mismatch" error and `init-db --reset` remediation
- Track Prefect version across restarts via `~/.prefect-submitit/prefect_version`;
  warn on startup if the version changed since last successful start
- Replace generic 30s health timeout with process-death-aware monitoring that
  tails the log file on failure and distinguishes "process died" from "process
  still starting"
- Wrap all `subprocess.run(check=True)` calls in `postgres.py` to surface
  stderr content in error messages
- Move discovery file write to after health check succeeds in background mode
  (no more stale `server.json` after failed startups)
- Add `CalledProcessError` catch in CLI `main()` as a safety net
- Include `prefect_version` in the discovery file for `prefect-server status`

## Motivation

When PostgreSQL or Prefect versions change (e.g. via `pixi update`), the server
fails to start with unhelpful errors: raw Python tracebacks from
`CalledProcessError`, or a generic "did not become healthy within 30s" timeout
with no indication of what went wrong. This has caused repeated debugging
frustration on shared HPC nodes where environment updates are common.

## Testing

- [x] Existing tests pass (`pixi run -e dev test`)
- [x] New/updated tests cover changed behavior (31 new test cases)
- [x] Linting passes (`pixi run -e dev fmt && git diff --exit-code`)

## Checklist

- [x] I have read the [Contributing Guide](../CONTRIBUTING.md)
- [x] Documentation is updated (if applicable)
