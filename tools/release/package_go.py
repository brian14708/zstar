#!/usr/bin/env python3
"""Build a Go module source archive from a clean release commit."""

import subprocess
import tomllib
from pathlib import Path

from checksums import write_checksums

ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)
    if dirty:
        raise SystemExit("Go release candidates require a clean git tree")
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    output = ROOT / "dist/go"
    output.mkdir(parents=True, exist_ok=True)
    artifact = output / f"zstar-go-{version}.tar.gz"
    subprocess.run(
        [
            "git",
            "archive",
            "--format=tar.gz",
            f"--prefix=zstar-go-{version}/",
            f"--output={artifact}",
            "HEAD",
            "go.mod",
            "go.sum",
            "go",
            "testdata",
            "LICENSE",
        ],
        cwd=ROOT,
        check=True,
    )
    for previous in output.glob("*.tar.gz"):
        if previous != artifact:
            previous.unlink()
    write_checksums(output)


if __name__ == "__main__":
    main()
