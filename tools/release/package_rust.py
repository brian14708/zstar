#!/usr/bin/env python3
"""Package and compile every distributable crate with unpublished sibling edges."""

import os
import shutil
import subprocess
import tempfile
import tomllib
from pathlib import Path

from checksums import write_checksums

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = ("zstar", "zstar-cli")


def main() -> None:
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    output = ROOT / "dist/rust"
    output.mkdir(parents=True, exist_ok=True)
    target = ROOT / "target/release-candidates"
    # Cargo stages sibling packages in a temporary crates.io registry. A user's
    # replacement registry can bypass that staging and fail on unpublished
    # dependencies. Use a clean Cargo configuration and a cwd outside the repo;
    # neither the user's config nor source tree is modified.
    with tempfile.TemporaryDirectory(prefix="zstar-rust-package-") as temporary:
        work = Path(temporary)
        cargo_home = work / "cargo"
        cargo_home.mkdir()
        environment = os.environ.copy()
        environment["CARGO_HOME"] = str(cargo_home)
        previous_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
        # Reuse only remote crates.io caches. Cargo's local staging registry
        # reuses package versions across candidates; its cache must be fresh.
        for kind in ("cache", "index", "src"):
            destination = cargo_home / "registry" / kind
            destination.mkdir(parents=True)
            for cached in (previous_home / "registry" / kind).glob("*"):
                if cached.name.startswith(("index.crates.io-", "github.com-")):
                    (destination / cached.name).symlink_to(cached, target_is_directory=True)
        # Release candidates must come from a clean tree; opt in explicitly for
        # local experiments with uncommitted changes.
        dirty = ["--allow-dirty"] if os.environ.get("ZSTAR_ALLOW_DIRTY") == "1" else []
        subprocess.run(
            [
                "cargo",
                "package",
                "--registry",
                "crates-io",
                *[arg for package in PACKAGES for arg in ("--package", package)],
                "--all-features",
                "--locked",
                *dirty,
                "--manifest-path",
                str(ROOT / "Cargo.toml"),
                "--target-dir",
                str(target),
            ],
            cwd=work,
            env=environment,
            check=True,
        )
        for package in PACKAGES:
            crate = target / "package" / f"{package}-{version}.crate"
            shutil.copy2(crate, output / crate.name)
    expected = {f"{package}-{version}.crate" for package in PACKAGES}
    for artifact in output.glob("*.crate"):
        if artifact.name not in expected:
            artifact.unlink()
    write_checksums(output)


if __name__ == "__main__":
    main()
