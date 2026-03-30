"""Tests for PostgreSQL lifecycle management."""

from __future__ import annotations

import os
import socket
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from prefect_submitit.server.config import ServerConfig
from prefect_submitit.server.postgres import (
    _END_SENTINEL,
    _START_SENTINEL,
    _check_pg_version,
    _find_pid_on_port,
    _is_postgres_process,
    _kill_orphan_on_port,
    _pg_binary_version,
    _run_pg_cmd,
    _write_custom_config,
    is_running,
)

MODULE = "prefect_submitit.server.postgres"


@pytest.fixture
def tmp_config(tmp_path):
    pg_data = tmp_path / "postgres"
    pg_data.mkdir()
    return ServerConfig(
        port=4242,
        host="test.local",
        api_url="http://test.local:4242/api",
        data_dir=tmp_path,
        discovery_file=tmp_path / "server.json",
        pg_data_dir=pg_data,
        pg_port=5433,
        pg_user="prefect",
        pg_database="prefect",
        log_dir=tmp_path / "logs",
    )


class TestWriteCustomConfig:
    def test_fresh_config(self, tmp_config):
        conf = tmp_config.pg_data_dir / "postgresql.conf"
        conf.write_text("# default config\n")

        _write_custom_config(tmp_config.pg_data_dir, 5433)

        content = conf.read_text()
        assert _START_SENTINEL in content
        assert _END_SENTINEL in content
        assert "port = 5433" in content
        assert "max_connections = 200" in content
        assert "shared_buffers = 256MB" in content
        assert "# default config" in content

    def test_idempotent(self, tmp_config):
        conf = tmp_config.pg_data_dir / "postgresql.conf"
        conf.write_text("# default config\n")

        _write_custom_config(tmp_config.pg_data_dir, 5433)
        _write_custom_config(tmp_config.pg_data_dir, 5433)

        content = conf.read_text()
        assert content.count(_START_SENTINEL) == 1
        assert content.count(_END_SENTINEL) == 1

    def test_update_port(self, tmp_config):
        conf = tmp_config.pg_data_dir / "postgresql.conf"
        conf.write_text("# default config\n")

        _write_custom_config(tmp_config.pg_data_dir, 5433)
        _write_custom_config(tmp_config.pg_data_dir, 5500)

        content = conf.read_text()
        assert "port = 5500" in content
        assert "port = 5433" not in content


class TestIsRunning:
    def test_no_pid_file(self, tmp_config):
        assert is_running(tmp_config) is None

    def test_stale_pid(self, tmp_config):
        pid_file = tmp_config.pg_data_dir / "postmaster.pid"
        # Use a PID that almost certainly doesn't exist
        pid_file.write_text("999999999\n")
        assert is_running(tmp_config) is None

    def test_current_process(self, tmp_config):
        """Our own PID should be detectable."""
        pid_file = tmp_config.pg_data_dir / "postmaster.pid"
        pid_file.write_text(f"{os.getpid()}\n")
        assert is_running(tmp_config) == os.getpid()


def _unused_port() -> int:
    """Find an unused TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


class TestFindPidOnPort:
    def test_free_port(self):
        port = _unused_port()
        assert _find_pid_on_port(port) is None

    def test_with_listener(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("localhost", 0))
        srv.listen(1)
        port = srv.getsockname()[1]
        try:
            pid = _find_pid_on_port(port)
            assert pid == os.getpid()
        finally:
            srv.close()


class TestIsPostgresProcess:
    def test_false_for_python(self):
        assert _is_postgres_process(os.getpid()) is False


class TestKillOrphanOnPort:
    def test_free_port(self):
        port = _unused_port()
        assert _kill_orphan_on_port(port) is False

    def test_non_postgres_raises(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("localhost", 0))
        srv.listen(1)
        port = srv.getsockname()[1]
        try:
            with pytest.raises(RuntimeError, match="not PostgreSQL"):
                _kill_orphan_on_port(port)
        finally:
            srv.close()


class TestPgBinaryVersion:
    def test_parses_standard_output(self):
        result = MagicMock(returncode=0, stdout="pg_ctl (PostgreSQL) 16.2\n")
        with patch(f"{MODULE}.subprocess.run", return_value=result):
            assert _pg_binary_version("/usr/bin/pg_ctl") == "16"

    def test_parses_with_extra_info(self):
        result = MagicMock(
            returncode=0, stdout="pg_ctl (PostgreSQL) 14.11 (Ubuntu 14.11-1)\n"
        )
        with patch(f"{MODULE}.subprocess.run", return_value=result):
            assert _pg_binary_version("/usr/bin/pg_ctl") == "14"

    def test_returns_none_on_failure(self):
        result = MagicMock(returncode=1, stdout="")
        with patch(f"{MODULE}.subprocess.run", return_value=result):
            assert _pg_binary_version("/usr/bin/pg_ctl") is None

    def test_returns_none_on_unparseable(self):
        result = MagicMock(returncode=0, stdout="unexpected output\n")
        with patch(f"{MODULE}.subprocess.run", return_value=result):
            assert _pg_binary_version("/usr/bin/pg_ctl") is None


class TestCheckPgVersion:
    def test_matching_versions_passes(self, tmp_config):
        (tmp_config.pg_data_dir / "PG_VERSION").write_text("16\n")
        with patch(f"{MODULE}._pg_binary_version", return_value="16"):
            _check_pg_version(tmp_config, "/usr/bin/pg_ctl")

    def test_mismatch_raises_with_remediation(self, tmp_config):
        (tmp_config.pg_data_dir / "PG_VERSION").write_text("15\n")
        with patch(f"{MODULE}._pg_binary_version", return_value="16"):
            with pytest.raises(RuntimeError, match="version mismatch") as exc_info:
                _check_pg_version(tmp_config, "/usr/bin/pg_ctl")
            assert "version 15" in str(exc_info.value)
            assert "version 16" in str(exc_info.value)
            assert "init-db --reset" in str(exc_info.value)

    def test_skips_when_binary_version_unknown(self, tmp_config):
        (tmp_config.pg_data_dir / "PG_VERSION").write_text("16\n")
        with patch(f"{MODULE}._pg_binary_version", return_value=None):
            _check_pg_version(tmp_config, "/usr/bin/pg_ctl")

    def test_skips_when_pg_version_missing(self, tmp_config):
        # PG_VERSION does not exist — should not raise
        _check_pg_version(tmp_config, "/usr/bin/pg_ctl")


class TestRunPgCmd:
    def test_success_returns_result(self):
        result = MagicMock(returncode=0)
        with patch(f"{MODULE}.subprocess.run", return_value=result):
            assert _run_pg_cmd(["pg_ctl", "start"], label="pg_ctl start") is result

    def test_failure_raises_runtime_error_with_stderr(self):
        exc = subprocess.CalledProcessError(
            1, ["pg_ctl", "start"], stderr=b"FATAL: incompatible version"
        )
        with patch(f"{MODULE}.subprocess.run", side_effect=exc):
            with pytest.raises(RuntimeError, match="pg_ctl start failed") as exc_info:
                _run_pg_cmd(["pg_ctl", "start"], label="pg_ctl start")
            assert "incompatible version" in str(exc_info.value)
