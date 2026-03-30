"""Tests for Prefect server process management."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from prefect_submitit.server.config import ServerConfig
from prefect_submitit.server.prefect_proc import (
    _build_env,
    _check_prefect_version,
    _find_server_pids,
    _kill_pid,
    _read_log_tail,
    _wait_for_healthy_or_death,
    _write_prefect_version,
    start,
    status,
    stop,
)

MODULE = "prefect_submitit.server.prefect_proc"


@pytest.fixture
def tmp_config(tmp_path):
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


class TestBuildEnv:
    def test_postgres_env(self, tmp_config):
        env = _build_env(tmp_config, sqlite=False)
        assert env["PREFECT_HOME"] == str(tmp_config.data_dir)
        assert env["PREFECT_SERVER_API_HOST"] == "0.0.0.0"
        assert env["PREFECT_UI_API_URL"] == "http://localhost:4242/api"
        assert env["PREFECT_SERVER_DATABASE_SQLALCHEMY_POOL_SIZE"] == "50"
        assert env["PREFECT_SERVER_DATABASE_SQLALCHEMY_MAX_OVERFLOW"] == "120"
        assert (
            "postgresql+asyncpg://prefect@localhost:5433/prefect"
            in env["PREFECT_API_DATABASE_CONNECTION_URL"]
        )

    def test_sqlite_env(self, tmp_config):
        env = _build_env(tmp_config, sqlite=True)
        assert env["PREFECT_SERVER_API_HOST"] == "0.0.0.0"
        assert "PREFECT_API_DATABASE_CONNECTION_URL" not in env


class TestKillPid:
    def test_nonexistent_pid(self):
        _kill_pid(999999999, timeout=0.1)

    def test_graceful_kill(self):
        with (
            patch(f"{MODULE}.os.kill") as mock_kill,
        ):
            mock_kill.side_effect = [None, ProcessLookupError]
            _kill_pid(12345, timeout=1.0)

            import signal

            mock_kill.assert_any_call(12345, signal.SIGTERM)


class TestFindServerPids:
    def test_returns_pids_from_pgrep(self):
        result = MagicMock(returncode=0, stdout="100\n200\n")
        with patch(f"{MODULE}.subprocess.run", return_value=result):
            assert _find_server_pids() == [100, 200]

    def test_returns_empty_when_no_match(self):
        result = MagicMock(returncode=1, stdout="")
        with patch(f"{MODULE}.subprocess.run", return_value=result):
            assert _find_server_pids() == []

    def test_returns_empty_when_pgrep_not_found(self):
        with patch(f"{MODULE}.subprocess.run", side_effect=FileNotFoundError):
            assert _find_server_pids() == []

    def test_scopes_to_current_user(self):
        result = MagicMock(returncode=1, stdout="")
        with (
            patch(f"{MODULE}.subprocess.run", return_value=result) as mock_run,
            patch(f"{MODULE}.os.getuid", return_value=1234),
        ):
            _find_server_pids()
            cmd = mock_run.call_args[0][0]
            assert "-u" in cmd
            assert "1234" in cmd


class TestReadLogTail:
    def test_reads_last_n_lines(self, tmp_path):
        log = tmp_path / "server.log"
        log.write_text("\n".join(f"line {i}" for i in range(30)))
        tail = _read_log_tail(log, n=5)
        assert tail == "\n".join(f"line {i}" for i in range(25, 30))

    def test_fewer_lines_than_n(self, tmp_path):
        log = tmp_path / "server.log"
        log.write_text("line 0\nline 1\nline 2")
        tail = _read_log_tail(log, n=20)
        assert tail == "line 0\nline 1\nline 2"

    def test_missing_file(self, tmp_path):
        assert _read_log_tail(tmp_path / "nope.log") == "(no log output)"

    def test_empty_file(self, tmp_path):
        log = tmp_path / "empty.log"
        log.write_text("")
        assert _read_log_tail(log) == "(no log output)"


class TestWaitForHealthyOrDeath:
    def test_healthy_immediately(self, tmp_path):
        proc = MagicMock()
        proc.poll.return_value = None
        log = tmp_path / "server.log"
        log.write_text("")
        with patch(f"{MODULE}.discovery.health_check", return_value=True):
            _wait_for_healthy_or_death(proc, "http://test/api", log, timeout=1)

    def test_process_dies_raises_with_log_tail(self, tmp_path):
        proc = MagicMock()
        proc.poll.return_value = 1
        proc.returncode = 1
        log = tmp_path / "server.log"
        log.write_text("Traceback:\n  migration error\nFATAL: schema mismatch")
        with patch(f"{MODULE}.discovery.health_check", return_value=False):
            with pytest.raises(RuntimeError, match="exited during startup") as exc_info:
                _wait_for_healthy_or_death(proc, "http://test/api", log, timeout=5)
            msg = str(exc_info.value)
            assert "exit code 1" in msg
            assert "FATAL: schema mismatch" in msg
            assert "init-db --reset" in msg

    def test_timeout_process_alive(self, tmp_path):
        proc = MagicMock(pid=12345)
        proc.poll.return_value = None
        log = tmp_path / "server.log"
        log.write_text("")
        with patch(f"{MODULE}.discovery.health_check", return_value=False):
            with pytest.raises(RuntimeError, match="not healthy after") as exc_info:
                _wait_for_healthy_or_death(
                    proc, "http://test/api", log, timeout=0.1, poll=0.05
                )
            msg = str(exc_info.value)
            assert "PID 12345 still running" in msg
            assert "prefect-server status" in msg

    def test_process_dies_empty_log(self, tmp_path):
        proc = MagicMock()
        proc.poll.return_value = 1
        proc.returncode = 1
        log = tmp_path / "server.log"
        log.write_text("")
        with patch(f"{MODULE}.discovery.health_check", return_value=False):
            with pytest.raises(RuntimeError, match="no log output"):
                _wait_for_healthy_or_death(proc, "http://test/api", log, timeout=5)


class TestCheckPrefectVersion:
    def test_no_stored_version(self, tmp_config):
        assert _check_prefect_version(tmp_config) is None

    def test_matching_version(self, tmp_config):
        with patch(f"{MODULE}._get_prefect_version", return_value="3.6.23"):
            (tmp_config.data_dir / "prefect_version").write_text("3.6.23\n")
            assert _check_prefect_version(tmp_config) is None

    def test_version_changed(self, tmp_config):
        with patch(f"{MODULE}._get_prefect_version", return_value="3.7.0"):
            (tmp_config.data_dir / "prefect_version").write_text("3.6.23\n")
            warning = _check_prefect_version(tmp_config)
            assert warning is not None
            assert "3.6.23" in warning
            assert "3.7.0" in warning
            assert "migrations" in warning

    def test_read_error(self, tmp_config):
        # Make prefect_version a directory to force an OSError on read
        (tmp_config.data_dir / "prefect_version").mkdir(parents=True, exist_ok=True)
        assert _check_prefect_version(tmp_config) is None


class TestWritePrefectVersion:
    def test_writes_current_version(self, tmp_config):
        with patch(f"{MODULE}._get_prefect_version", return_value="3.6.23"):
            _write_prefect_version(tmp_config)
            content = (tmp_config.data_dir / "prefect_version").read_text()
            assert content.strip() == "3.6.23"


class TestStart:
    def test_background_sqlite_starts_popen_and_writes_discovery(self, tmp_config):
        mock_proc = MagicMock(pid=42)
        with (
            patch(f"{MODULE}.require_binary", return_value="/usr/bin/prefect"),
            patch(f"{MODULE}.subprocess.Popen", return_value=mock_proc) as mock_popen,
            patch(f"{MODULE}.discovery.write_discovery") as mock_write,
            patch(f"{MODULE}._wait_for_healthy_or_death"),
            patch(f"{MODULE}._write_prefect_version"),
        ):
            pid = start(tmp_config, background=True, sqlite=True)

            assert pid == 42
            mock_popen.assert_called_once()
            mock_write.assert_called_once_with(tmp_config, 42, "sqlite")

    def test_background_postgres_starts_pg_first(self, tmp_config):
        mock_proc = MagicMock(pid=99)
        with (
            patch(f"{MODULE}.require_binary", return_value="/usr/bin/prefect"),
            patch(f"{MODULE}.subprocess.Popen", return_value=mock_proc),
            patch(f"{MODULE}.postgres.start") as mock_pg_start,
            patch(f"{MODULE}.discovery.write_discovery"),
            patch(f"{MODULE}._wait_for_healthy_or_death"),
            patch(f"{MODULE}._write_prefect_version"),
        ):
            start(tmp_config, background=True, sqlite=False)
            mock_pg_start.assert_called_once_with(tmp_config)

    def test_background_sqlite_skips_postgres(self, tmp_config):
        mock_proc = MagicMock(pid=99)
        with (
            patch(f"{MODULE}.require_binary", return_value="/usr/bin/prefect"),
            patch(f"{MODULE}.subprocess.Popen", return_value=mock_proc),
            patch(f"{MODULE}.postgres.start") as mock_pg_start,
            patch(f"{MODULE}.discovery.write_discovery"),
            patch(f"{MODULE}._wait_for_healthy_or_death"),
            patch(f"{MODULE}._write_prefect_version"),
        ):
            start(tmp_config, background=True, sqlite=True)
            mock_pg_start.assert_not_called()

    def test_background_creates_log_dir(self, tmp_config):
        mock_proc = MagicMock(pid=1)
        with (
            patch(f"{MODULE}.require_binary", return_value="/usr/bin/prefect"),
            patch(f"{MODULE}.subprocess.Popen", return_value=mock_proc),
            patch(f"{MODULE}.discovery.write_discovery"),
            patch(f"{MODULE}._wait_for_healthy_or_death"),
            patch(f"{MODULE}._write_prefect_version"),
        ):
            start(tmp_config, background=True, sqlite=True)
            assert tmp_config.log_dir.is_dir()

    def test_raises_when_prefect_binary_missing(self, tmp_config):
        with patch(f"{MODULE}.require_binary", side_effect=FileNotFoundError("nope")):
            with pytest.raises(FileNotFoundError, match="nope"):
                start(tmp_config, background=True, sqlite=True)

    def test_foreground_waits_and_cleans_up(self, tmp_config):
        mock_proc = MagicMock(pid=55)
        mock_proc.wait.return_value = 0
        with (
            patch(f"{MODULE}.require_binary", return_value="/usr/bin/prefect"),
            patch(f"{MODULE}.subprocess.Popen", return_value=mock_proc),
            patch(f"{MODULE}.discovery.write_discovery") as mock_write,
            patch(f"{MODULE}.discovery.remove_discovery") as mock_remove,
            patch(f"{MODULE}.postgres.stop") as mock_pg_stop,
            patch(f"{MODULE}.signal.signal"),
            patch(f"{MODULE}._write_prefect_version"),
        ):
            pid = start(tmp_config, background=False, sqlite=True)

            assert pid == 55
            mock_proc.wait.assert_called_once()
            mock_write.assert_called_once()
            mock_remove.assert_called_once_with(tmp_config)
            mock_pg_stop.assert_called_once_with(tmp_config)

    def test_background_no_discovery_on_startup_failure(self, tmp_config):
        mock_proc = MagicMock(pid=42)
        with (
            patch(f"{MODULE}.require_binary", return_value="/usr/bin/prefect"),
            patch(f"{MODULE}.subprocess.Popen", return_value=mock_proc),
            patch(f"{MODULE}.discovery.write_discovery") as mock_write,
            patch(
                f"{MODULE}._wait_for_healthy_or_death",
                side_effect=RuntimeError("server died"),
            ),
        ):
            with pytest.raises(RuntimeError, match="server died"):
                start(tmp_config, background=True, sqlite=True)
            mock_write.assert_not_called()


class TestStop:
    def test_kills_found_pids_and_cleans_up(self, tmp_config):
        with (
            patch(f"{MODULE}._find_server_pids", return_value=[100, 200]),
            patch(f"{MODULE}._kill_pid") as mock_kill,
            patch(f"{MODULE}.discovery.remove_discovery") as mock_remove,
            patch(f"{MODULE}.postgres.stop") as mock_pg_stop,
        ):
            stop(tmp_config)

            assert mock_kill.call_count == 2
            mock_kill.assert_any_call(100, timeout=5.0)
            mock_kill.assert_any_call(200, timeout=5.0)
            mock_remove.assert_called_once_with(tmp_config)
            mock_pg_stop.assert_called_once_with(tmp_config, force=False)

    def test_force_uses_shorter_timeout(self, tmp_config):
        with (
            patch(f"{MODULE}._find_server_pids", return_value=[100]),
            patch(f"{MODULE}._kill_pid") as mock_kill,
            patch(f"{MODULE}.discovery.remove_discovery"),
            patch(f"{MODULE}.postgres.stop") as mock_pg_stop,
        ):
            stop(tmp_config, force=True)

            mock_kill.assert_called_once_with(100, timeout=2.0)
            mock_pg_stop.assert_called_once_with(tmp_config, force=True)

    def test_no_pids_still_cleans_up(self, tmp_config):
        with (
            patch(f"{MODULE}._find_server_pids", return_value=[]),
            patch(f"{MODULE}._kill_pid") as mock_kill,
            patch(f"{MODULE}.discovery.remove_discovery") as mock_remove,
            patch(f"{MODULE}.postgres.stop") as mock_pg_stop,
        ):
            stop(tmp_config)

            mock_kill.assert_not_called()
            mock_remove.assert_called_once()
            mock_pg_stop.assert_called_once()


class TestStatus:
    def test_returns_none_when_no_discovery(self, tmp_config):
        with patch(f"{MODULE}.discovery.read_discovery", return_value=None):
            assert status(tmp_config) is None

    def test_returns_info_with_healthy_check(self, tmp_config):
        info = {"url": "http://test.local:4242/api", "pid": 42}
        with (
            patch(f"{MODULE}.discovery.read_discovery", return_value=info),
            patch(f"{MODULE}.discovery.health_check", return_value=True) as mock_hc,
        ):
            result = status(tmp_config)

            assert result["healthy"] is True
            assert result["pid"] == 42
            mock_hc.assert_called_once_with("http://test.local:4242/api")

    def test_returns_unhealthy_when_check_fails(self, tmp_config):
        info = {"url": "http://test.local:4242/api", "pid": 42}
        with (
            patch(f"{MODULE}.discovery.read_discovery", return_value=info),
            patch(f"{MODULE}.discovery.health_check", return_value=False),
        ):
            result = status(tmp_config)
            assert result["healthy"] is False

    def test_returns_unhealthy_when_url_empty(self, tmp_config):
        info = {"url": "", "pid": 42}
        with patch(f"{MODULE}.discovery.read_discovery", return_value=info):
            result = status(tmp_config)
            assert result["healthy"] is False
