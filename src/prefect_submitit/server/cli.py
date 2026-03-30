"""CLI entry point for Prefect server management."""

from __future__ import annotations

import argparse
import subprocess
import sys

from prefect_submitit.server import discovery, postgres, prefect_proc
from prefect_submitit.server.config import make_config


def _cmd_start(args: argparse.Namespace) -> None:
    """Handle the 'start' subcommand."""
    config = make_config(port=args.port, pg_port=args.pg_port)

    if not args.restart and discovery.health_check(config.api_url):
        print(f"Server already running and healthy at {config.api_url}")
        return

    if args.restart:
        print("Stopping existing server...")
        prefect_proc.stop(config, force=True)

    if not args.sqlite and not (config.pg_data_dir / "PG_VERSION").exists():
        print("Initializing PostgreSQL database...")
        postgres.init_db(config)

    version_warning = prefect_proc._check_prefect_version(config)
    if version_warning:
        print(f"Note: {version_warning}")

    backend = "SQLite" if args.sqlite else "PostgreSQL"
    mode = "background" if args.bg else "foreground"
    print(f"Starting Prefect server ({backend}, {mode})...")
    print(f"  API URL:  {config.api_url}")
    print(f"  Port:     {config.port}")
    print(f"  Host:     {config.host}")
    if not args.sqlite:
        print(f"  PG port:  {config.pg_port}")
    print()

    pid = prefect_proc.start(config, background=args.bg, sqlite=args.sqlite)

    if args.bg:
        print(f"Server started (PID: {pid})")
        print(f"  Logs: {config.log_dir}")
        print("  Stop: prefect-server stop")


def _cmd_stop(args: argparse.Namespace) -> None:
    """Handle the 'stop' subcommand."""
    config = make_config()
    print("Stopping Prefect server...")
    prefect_proc.stop(config, force=args.force)
    print("Server stopped.")


def _cmd_init_db(args: argparse.Namespace) -> None:
    """Handle the 'init-db' subcommand."""
    config = make_config()
    if args.reset:
        print("Resetting PostgreSQL database...")
    else:
        print("Initializing PostgreSQL database...")
    postgres.init_db(config, reset=args.reset)
    print(f"PostgreSQL data directory: {config.pg_data_dir}")
    print("Done.")


def _cmd_status(_args: argparse.Namespace) -> None:
    """Handle the 'status' subcommand."""
    config = make_config()
    info = prefect_proc.status(config)
    if info is None:
        print("No server running.")
        return

    healthy = info.pop("healthy", False)
    status_str = "healthy" if healthy else "unhealthy"
    print(f"Prefect server: {status_str}")
    for key, value in info.items():
        print(f"  {key}: {value}")


def main(argv: list[str] | None = None) -> None:
    """Parse arguments and dispatch to subcommands.

    Args:
        argv: Command-line arguments. Defaults to sys.argv[1:].
    """
    parser = argparse.ArgumentParser(
        prog="prefect-server",
        description="Manage a local Prefect server with PostgreSQL or SQLite.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # start
    start_p = sub.add_parser("start", help="Start the Prefect server")
    start_p.add_argument("--bg", action="store_true", help="Run in background")
    start_p.add_argument(
        "--sqlite", action="store_true", help="Use SQLite instead of PostgreSQL"
    )
    start_p.add_argument(
        "--restart", action="store_true", help="Stop existing server first"
    )
    start_p.add_argument("--port", type=int, default=None, help="Server port")
    start_p.add_argument("--pg-port", type=int, default=None, help="PostgreSQL port")

    # stop
    stop_p = sub.add_parser("stop", help="Stop the Prefect server and PostgreSQL")
    stop_p.add_argument(
        "-f", "--force", action="store_true", help="Force stop with SIGKILL fallback"
    )

    # init-db
    init_db_p = sub.add_parser("init-db", help="Initialize PostgreSQL database")
    init_db_p.add_argument(
        "--reset", action="store_true", help="Reset database (deletes all data)"
    )

    # status
    sub.add_parser("status", help="Show server status")

    args = parser.parse_args(argv)

    dispatch = {
        "start": _cmd_start,
        "stop": _cmd_stop,
        "init-db": _cmd_init_db,
        "status": _cmd_status,
    }

    try:
        dispatch[args.command](args)
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or b"").decode(errors="replace").strip()
        msg = f"Command failed: {' '.join(exc.cmd)} (exit code {exc.returncode})"
        if stderr:
            msg += f"\n{stderr}"
        print(f"Error: {msg}", file=sys.stderr)
        sys.exit(1)
    except (FileNotFoundError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
