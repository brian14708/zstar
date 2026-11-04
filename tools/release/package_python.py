#!/usr/bin/env python3
"""Build and verify native or portable Python package candidates."""

import argparse
import io
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import zipfile
from pathlib import Path

from checksums import write_checksums

ROOT = Path(__file__).resolve().parents[2]
DESTINATION = ROOT / "dist" / "python"


def run(*args: str | Path, cwd: Path = ROOT) -> None:
    subprocess.run(args, cwd=cwd, check=True)


def prepare_source(sdist: Path, work: Path) -> Path:
    with tarfile.open(sdist) as archive:
        names = archive.getnames()
        prefix = names[0].split("/")[0]
        required = (
            "README.md",
            "LICENSE",
            "Cargo.lock",
            "pyproject.toml",
            "python/rust/request.rs",
            "python/rust/async_archive.rs",
            "python/rust/sync_archive.rs",
            "python/rust/reader.rs",
            "python/rust/writer.rs",
            "python/rust/staged_writer.rs",
            "python/rust/manifest.rs",
            "python/rust/support.rs",
            "python/rust/external.rs",
            "python/rust/fs.rs",
            "python/rust/s3.rs",
            "crates/zstar/src/lib.rs",
            "src/zstar/__init__.py",
            "src/zstar/_archive.py",
            "src/zstar/_file.py",
            "src/zstar/_info.py",
            "src/zstar/_publication.py",
            "src/zstar/_source.py",
            "src/zstar/_writer.py",
            "src/zstar/_fsspec.py",
            "src/zstar/types.py",
            "src/zstar/_types.py",
            "src/zstar/_native.pyi",
            "src/zstar/py.typed",
        )
        for name in required:
            assert f"{prefix}/{name}" in names, f"sdist missing {name}"
        assert not any("/.venv/" in name or "/target/" in name for name in names)
        archive.extractall(work, filter="data")
    return work / prefix


def finalize_sdist(sdist: Path, work: Path) -> Path:
    source = prepare_source(sdist, work)
    prune_lockfile(source)
    lock = (source / "Cargo.lock").read_bytes()
    repaired = sdist.with_name(f"{sdist.name}.tmp")
    with tarfile.open(sdist) as original, tarfile.open(repaired, "w:gz") as archive:
        for member in original:
            if member.name == f"{source.name}/Cargo.lock":
                member.size = len(lock)
                archive.addfile(member, io.BytesIO(lock))
            else:
                archive.addfile(member, original.extractfile(member) if member.isfile() else None)
    repaired.replace(sdist)
    # Verify and build from the exact published archive, without repairing its extraction.
    verified = work / "verified"
    verified.mkdir()
    source = prepare_source(sdist, verified)
    subprocess.run(
        ["cargo", "metadata", "--locked", "--format-version=1"],
        cwd=source,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    return source


def prune_lockfile(source: Path) -> None:
    # Maturin narrows the workspace in the sdist. Cargo must prune entries
    # for omitted members before --locked can apply to this workspace.
    lock = source / "Cargo.lock"

    def registry_packages() -> set[tuple[str, str, str, str | None]]:
        return {
            (p["name"], p["version"], p["source"], p.get("checksum"))
            for p in tomllib.loads(lock.read_text())["package"]
            if "source" in p
        }

    original = registry_packages()
    subprocess.run(
        # Metadata resolves all target dependencies, including ones that
        # native tests may not have downloaded on this runner. Permit
        # fetching them, then enforce that pruning changed no registry pin.
        ["cargo", "metadata", "--format-version=1"],
        cwd=source,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    if not registry_packages() <= original:
        raise RuntimeError("sdist changed pinned registry dependencies")


def build_native(source: Path, work: Path) -> list[Path]:
    run(
        "uv",
        "build",
        "--wheel",
        "--python",
        sys.executable,
        "--config-setting",
        "maturin.build-args=--profile release --locked",
        "--out-dir",
        str(work / "wheels"),
        cwd=source,
    )
    [wheel] = (work / "wheels").glob("*.whl")
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        assert "zstar/py.typed" in names, "wheel missing typing marker"
        assert "zstar/_native.pyi" in names, "wheel missing native type stubs"
    return [wheel]


def build_manylinux(source: Path, work: Path) -> list[Path]:
    destination = work / "wheels"
    run(
        "uv",
        "tool",
        "run",
        "--from",
        "cibuildwheel==4.3.4",
        "cibuildwheel",
        str(source),
        "--platform",
        "linux",
        "--output-dir",
        str(destination),
        # cibuildwheel mounts its working directory into the container and
        # requires the package source to be inside that directory.
        cwd=source,
    )
    wheels = sorted(destination.glob("*.whl"))
    if not wheels or any("manylinux_2_28" not in wheel.name for wheel in wheels):
        raise RuntimeError("cibuildwheel did not produce manylinux_2_28 wheels")
    return wheels


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manylinux", action="store_true", help="build portable Linux wheels with Docker"
    )
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument(
        "--sdist-only", action="store_true", help="verify and publish only the source distribution"
    )
    scope.add_argument(
        "--wheels-only",
        action="store_true",
        help="publish only wheels (the sdist is still built to produce them)",
    )
    args = parser.parse_args()
    DESTINATION.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="zstar-python-package-") as temporary:
        work = Path(temporary)
        run(
            "uv",
            "build",
            str(ROOT / "python"),
            "--sdist",
            "--python",
            sys.executable,
            "--out-dir",
            str(work),
        )
        [sdist] = work.glob("*.tar.gz")
        source = finalize_sdist(sdist, work)
        if args.sdist_only:
            wheels: list[Path] = []
        else:
            wheels = build_manylinux(source, work) if args.manylinux else build_native(source, work)
        artifacts = wheels if args.wheels_only else [sdist, *wheels]
        for artifact in artifacts:
            shutil.copy2(artifact, DESTINATION / artifact.name)
        expected = {artifact.name for artifact in artifacts}
        patterns = []
        if not args.sdist_only:
            patterns.append("*.whl")
        if not args.wheels_only:
            patterns.append("*.tar.gz")
        for pattern in patterns:
            for artifact in DESTINATION.glob(pattern):
                if artifact.name not in expected:
                    artifact.unlink()
    write_checksums(DESTINATION)
    print(f"Verified {len(artifacts)} artifact(s) in {DESTINATION}")


if __name__ == "__main__":
    main()
