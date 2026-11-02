"""Release publication rejects stale or unrelated built distributions."""

from __future__ import annotations

import io
import subprocess
import sys
import tarfile
from pathlib import Path
from zipfile import ZipFile

import pytest

from scripts.check_release import check_release


def _write_artifact(
    path: Path, *, version: str = "0.1.8", name: str = "prefect-submitit"
) -> None:
    metadata = f"Metadata-Version: 2.4\nName: {name}\nVersion: {version}\n".encode()
    if path.suffix == ".whl":
        with ZipFile(path, "w") as wheel:
            wheel.writestr("prefect_submitit-0.1.8.dist-info/METADATA", metadata)
        return
    with tarfile.open(path, "w:gz") as sdist:
        member = tarfile.TarInfo("prefect_submitit-0.1.8/PKG-INFO")
        member.size = len(metadata)
        sdist.addfile(member, io.BytesIO(metadata))


@pytest.fixture
def dist(tmp_path: Path) -> Path:
    directory = tmp_path / "dist"
    directory.mkdir()
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "prefect-submitit"\n')
    _write_artifact(directory / "prefect_submitit-0.1.8-py3-none-any.whl")
    _write_artifact(directory / "prefect_submitit-0.1.8.tar.gz")
    return directory


@pytest.mark.parametrize(
    ("tag", "version"),
    [
        ("0.1.8", "0.1.8"),
        ("v0.1.8", "0.1.8"),
        ("v0.1.8.dev0", "0.1.8.dev0"),
        ("v0.1.8a1", "0.1.8a1"),
    ],
)
def test_check_release_matching_tag(dist: Path, tag: str, version: str) -> None:
    for artifact in dist.iterdir():
        _write_artifact(artifact, version=version)

    check_release(tag, dist, "prefect-submitit")


@pytest.mark.parametrize("pattern", ["*.whl", "*.tar.gz"])
def test_check_release_stale_metadata_despite_matching_filename(
    dist: Path, pattern: str
) -> None:
    artifact = next(dist.glob(pattern))
    _write_artifact(artifact, version="0.1.2a5")

    with pytest.raises(ValueError, match=r"0\.1\.2a5.*does not match.*v0\.1\.8"):
        check_release("v0.1.8", dist, "prefect-submitit")


@pytest.mark.parametrize("pattern", ["*.whl", "*.tar.gz"])
def test_check_release_wrong_package(dist: Path, pattern: str) -> None:
    _write_artifact(next(dist.glob(pattern)), name="another-package")

    with pytest.raises(ValueError, match="unexpected package name 'another-package'"):
        check_release("v0.1.8", dist, "prefect-submitit")


@pytest.mark.parametrize("pattern", ["*.whl", "*.tar.gz"])
def test_check_release_missing_artifact(dist: Path, pattern: str) -> None:
    next(dist.glob(pattern)).unlink()

    with pytest.raises(ValueError, match="expected exactly one wheel"):
        check_release("v0.1.8", dist, "prefect-submitit")


@pytest.mark.parametrize("pattern", ["*.whl", "*.tar.gz"])
def test_check_release_multiple_artifacts(dist: Path, pattern: str) -> None:
    artifact = next(dist.glob(pattern))
    (dist / f"extra-{artifact.name}").write_bytes(artifact.read_bytes())

    with pytest.raises(ValueError, match="expected exactly one wheel"):
        check_release("v0.1.8", dist, "prefect-submitit")


def test_check_release_missing_directory(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="found 0 wheels and 0 source distributions"):
        check_release("v0.1.8", tmp_path / "missing", "prefect-submitit")


def test_check_release_unexpected_artifact(dist: Path) -> None:
    (dist / "extra.zip").write_bytes(b"unexpected distribution")

    with pytest.raises(ValueError, match="among 3 entries"):
        check_release("v0.1.8", dist, "prefect-submitit")


@pytest.mark.parametrize("pattern", ["*.whl", "*.tar.gz"])
def test_check_release_missing_package_metadata(dist: Path, pattern: str) -> None:
    artifact = next(dist.glob(pattern))
    if artifact.suffix == ".whl":
        with ZipFile(artifact, "w"):
            pass
    else:
        with tarfile.open(artifact, "w:gz"):
            pass

    with pytest.raises(ValueError, match="expected one top-level"):
        check_release("v0.1.8", dist, "prefect-submitit")


@pytest.mark.parametrize(("tag", "returncode"), [("v0.1.8", 0), ("v0.3.0", 1)])
def test_check_release_command_status(dist: Path, tag: str, returncode: int) -> None:
    script = Path(__file__).parents[1] / "scripts" / "check_release.py"
    result = subprocess.run(
        [sys.executable, str(script), tag],
        cwd=dist.parent,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == returncode
    if returncode:
        assert "Release check failed:" in result.stderr
        assert "does not match release tag 'v0.3.0'" in result.stderr


def test_check_release_normalized_name(dist: Path) -> None:
    for artifact in dist.iterdir():
        _write_artifact(artifact, name="Prefect_Submitit")
    check_release("v0.1.8", dist, "prefect-submitit")


def test_check_release_renamed_project(dist: Path) -> None:
    for artifact in dist.iterdir():
        _write_artifact(artifact, name="renamed-project")
    check_release("v0.1.8", dist, "renamed-project")
