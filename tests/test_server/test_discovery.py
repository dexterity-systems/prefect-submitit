"""Tests for discovery file management and health checks."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from prefect_submitit.server.discovery import (
    health_check,
    read_discovery,
    remove_discovery,
    resolve_api_url,
    wait_for_healthy,
    write_discovery,
)


@pytest.fixture
def tmp_config(tmp_path):
    """Create a config with paths under tmp_path."""
    from prefect_submitit.server.config import ServerConfig

    return ServerConfig(
        port=4242,
        host="test.local",
        api_url="http://test.local:4242/api",
        data_dir=tmp_path,
        discovery_file=tmp_path / "server.json",
        pg_data_dir=tmp_path / "postgres",
        pg_port=5433,
        pg_user="prefect",
        pg_database="prefect",
        log_dir=tmp_path / "logs",
    )


class TestWriteReadRoundtrip:
    def test_roundtrip(self, tmp_config):
        write_discovery(tmp_config, pid=12345, backend="postgres")
        data = read_discovery(tmp_config.discovery_file)

        assert data is not None
        assert data["url"] == "http://test.local:4242/api"
        assert data["host"] == "test.local"
        assert data["port"] == 4242
        assert data["pid"] == 12345
        assert data["backend"] == "postgres"
        assert "prefect_version" in data
        assert "started" in data
        assert "user" in data

    def test_creates_parent_dirs(self, tmp_path):
        from prefect_submitit.server.config import ServerConfig

        nested = tmp_path / "a" / "b" / "server.json"
        config = ServerConfig(
            port=4242,
            host="test.local",
            api_url="http://test.local:4242/api",
            data_dir=tmp_path / "a",
            discovery_file=nested,
            pg_data_dir=tmp_path / "postgres",
            pg_port=5433,
            pg_user="prefect",
            pg_database="prefect",
            log_dir=tmp_path / "logs",
        )
        write_discovery(config, pid=1, backend="sqlite")
        assert nested.exists()


class TestReadDiscovery:
    def test_missing_file(self, tmp_path):
        assert read_discovery(tmp_path / "nonexistent.json") is None

    def test_malformed_json(self, tmp_path):
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("not json {{{")
        assert read_discovery(bad_file) is None


class TestRemoveDiscovery:
    def test_remove_existing(self, tmp_config):
        write_discovery(tmp_config, pid=1, backend="sqlite")
        assert tmp_config.discovery_file.exists()
        remove_discovery(tmp_config)
        assert not tmp_config.discovery_file.exists()

    def test_remove_missing(self, tmp_config):
        # Should not raise
        remove_discovery(tmp_config)


class TestHealthCheck:
    def test_healthy(self):
        with patch("prefect_submitit.server.discovery.urllib.request.urlopen"):
            assert health_check("http://localhost:4200/api") is True

    def test_unhealthy(self):
        import urllib.error

        with patch(
            "prefect_submitit.server.discovery.urllib.request.urlopen",
            side_effect=urllib.error.URLError("refused"),
        ):
            assert health_check("http://localhost:4200/api") is False


class TestWaitForHealthy:
    def test_already_healthy(self):
        with patch("prefect_submitit.server.discovery.health_check", return_value=True):
            wait_for_healthy("http://localhost:4200/api", timeout=1)

    def test_timeout(self):
        with (
            patch("prefect_submitit.server.discovery.health_check", return_value=False),
            pytest.raises(RuntimeError, match="did not become healthy"),
        ):
            wait_for_healthy("http://localhost:4200/api", timeout=0.1, poll=0.05)


class TestResolveApiUrl:
    def test_explicit(self):
        assert resolve_api_url("http://explicit/api") == "http://explicit/api"

    def test_env_submitit(self, monkeypatch):
        monkeypatch.setenv("PREFECT_SUBMITIT_SERVER", "http://env-submitit/api")
        monkeypatch.delenv("PREFECT_API_URL", raising=False)
        assert resolve_api_url() == "http://env-submitit/api"

    def test_env_prefect(self, monkeypatch):
        monkeypatch.delenv("PREFECT_SUBMITIT_SERVER", raising=False)
        monkeypatch.setenv("PREFECT_API_URL", "http://env-prefect/api")
        assert resolve_api_url() == "http://env-prefect/api"

    def test_discovery_file(self, monkeypatch, tmp_path):
        monkeypatch.delenv("PREFECT_SUBMITIT_SERVER", raising=False)
        monkeypatch.delenv("PREFECT_API_URL", raising=False)

        discovery_file = tmp_path / "server.json"
        discovery_file.write_text(json.dumps({"url": "http://discovered/api"}))

        with patch("prefect_submitit.server.discovery.DEFAULT_DATA_DIR", tmp_path):
            assert resolve_api_url() == "http://discovered/api"

    def test_raises_when_nothing(self, monkeypatch, tmp_path):
        monkeypatch.delenv("PREFECT_SUBMITIT_SERVER", raising=False)
        monkeypatch.delenv("PREFECT_API_URL", raising=False)

        with (
            patch("prefect_submitit.server.discovery.DEFAULT_DATA_DIR", tmp_path),
            pytest.raises(RuntimeError, match="Could not resolve"),
        ):
            resolve_api_url()
