"""Tests for PostgreSQL lifecycle management."""

from __future__ import annotations

import os
import socket

import pytest

from prefect_submitit.server.config import ServerConfig
from prefect_submitit.server.postgres import (
    _END_SENTINEL,
    _START_SENTINEL,
    _find_pid_on_port,
    _is_postgres_process,
    _kill_orphan_on_port,
    _write_custom_config,
    is_running,
)


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
