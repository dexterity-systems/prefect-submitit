"""Tests for CLI argument parsing and dispatch."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from prefect_submitit.server.cli import main


class TestArgParsing:
    def test_start_defaults(self):
        with patch("prefect_submitit.server.cli._cmd_start") as mock:
            main(["start"])
            args = mock.call_args[0][0]
            assert args.command == "start"
            assert args.bg is False
            assert args.sqlite is False
            assert args.restart is False
            assert args.port is None
            assert args.pg_port is None

    def test_start_all_flags(self):
        with patch("prefect_submitit.server.cli._cmd_start") as mock:
            main(
                [
                    "start",
                    "--bg",
                    "--sqlite",
                    "--restart",
                    "--port",
                    "5000",
                    "--pg-port",
                    "5555",
                ]
            )
            args = mock.call_args[0][0]
            assert args.bg is True
            assert args.sqlite is True
            assert args.restart is True
            assert args.port == 5000
            assert args.pg_port == 5555

    def test_stop_defaults(self):
        with patch("prefect_submitit.server.cli._cmd_stop") as mock:
            main(["stop"])
            args = mock.call_args[0][0]
            assert args.command == "stop"
            assert args.force is False

    def test_stop_force(self):
        with patch("prefect_submitit.server.cli._cmd_stop") as mock:
            main(["stop", "-f"])
            args = mock.call_args[0][0]
            assert args.force is True

    def test_init_db_defaults(self):
        with patch("prefect_submitit.server.cli._cmd_init_db") as mock:
            main(["init-db"])
            args = mock.call_args[0][0]
            assert args.command == "init-db"
            assert args.reset is False

    def test_init_db_reset(self):
        with patch("prefect_submitit.server.cli._cmd_init_db") as mock:
            main(["init-db", "--reset"])
            args = mock.call_args[0][0]
            assert args.reset is True

    def test_status(self):
        with patch("prefect_submitit.server.cli._cmd_status") as mock:
            main(["status"])
            args = mock.call_args[0][0]
            assert args.command == "status"

    def test_no_command_exits(self):
        with pytest.raises(SystemExit):
            main([])

    def test_unknown_command_exits(self):
        with pytest.raises(SystemExit):
            main(["unknown-cmd"])


class TestErrorHandling:
    def test_file_not_found_exits(self):
        with (
            patch(
                "prefect_submitit.server.cli._cmd_start",
                side_effect=FileNotFoundError("binary not found"),
            ),
            pytest.raises(SystemExit, match="1"),
        ):
            main(["start"])

    def test_runtime_error_exits(self):
        with (
            patch(
                "prefect_submitit.server.cli._cmd_start",
                side_effect=RuntimeError("server failed"),
            ),
            pytest.raises(SystemExit, match="1"),
        ):
            main(["start"])
