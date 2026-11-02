"""Check built distribution metadata against the intended release tag."""

from __future__ import annotations

import argparse
import os
import re
import tarfile
import tomllib
from email.message import Message
from email.parser import BytesParser
from pathlib import Path
from zipfile import ZipFile


def _read_metadata(artifact: Path) -> Message:
    """Read the wheel or source distribution's package metadata."""
    if artifact.suffix == ".whl":
        with ZipFile(artifact) as wheel:
            names = [
                name
                for name in wheel.namelist()
                if name.count("/") == 1 and name.endswith(".dist-info/METADATA")
            ]
            if len(names) != 1:
                message = f"{artifact}: expected one top-level METADATA file"
                raise ValueError(message)
            return BytesParser().parsebytes(wheel.read(names[0]))

    with tarfile.open(artifact, "r:gz") as sdist:
        members = [
            member
            for member in sdist.getmembers()
            if member.isfile()
            and member.name.count("/") == 1
            and member.name.endswith("/PKG-INFO")
        ]
        if len(members) != 1:
            message = f"{artifact}: expected one top-level PKG-INFO file"
            raise ValueError(message)
        stream = sdist.extractfile(members[0])
        assert stream is not None  # The selected member is a regular file.
        with stream:
            return BytesParser().parse(stream)


def check_release(tag: str, dist: Path, name: str) -> None:
    """Require one wheel and sdist matching the project and intended release.

    Args:
        tag: Release version, optionally prefixed with ``v``.
        dist: Directory containing only the two built distributions.
        name: Distribution name from the project manifest.

    Raises:
        ValueError: Artifacts are missing, ambiguous, or have incorrect metadata.
    """
    wheels = list(dist.glob("*.whl"))
    sdists = list(dist.glob("*.tar.gz"))
    entries = list(dist.glob("*"))
    if len(wheels) != 1 or len(sdists) != 1 or len(entries) != 2:
        message = (
            f"{dist}: expected exactly one wheel and one source distribution; "
            f"found {len(wheels)} wheels and {len(sdists)} source distributions "
            f"among {len(entries)} entries"
        )
        raise ValueError(message)
    expected = tag.removeprefix("v")
    expected_name = re.sub(r"[-_.]+", "-", name).lower()
    for artifact in [*wheels, *sdists]:
        metadata = _read_metadata(artifact)
        if re.sub(r"[-_.]+", "-", metadata.get("Name", "")).lower() != expected_name:
            message = f"{artifact}: unexpected package name {metadata['Name']!r}"
            raise ValueError(message)
        if metadata["Version"] != expected:
            message = (
                f"{artifact}: version {metadata['Version']!r} does not match "
                f"release tag {tag!r} (expected {expected!r})"
            )
            raise ValueError(message)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "tag",
        nargs="?",
        default=os.environ.get("RELEASE_VERSION"),
        help="Release tag (or RELEASE_VERSION), optionally prefixed with v",
    )
    args = parser.parse_args()
    if not args.tag:
        parser.error("provide a release tag or set RELEASE_VERSION")
    project = tomllib.loads(Path("pyproject.toml").read_text())["project"]
    try:
        check_release(args.tag, Path("dist"), project["name"])
    except ValueError as error:
        parser.exit(1, f"Release check failed: {error}\n")
