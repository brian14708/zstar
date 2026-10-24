#!/usr/bin/env python3
"""Select release candidates and require one version across package manifests."""

import os
import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def version(family: str) -> str:
    if family in {"rust", "go"}:
        return tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    if family == "python":
        return tomllib.loads((ROOT / "python/pyproject.toml").read_text())["project"]["version"]
    raise ValueError("unknown package family")


def select(event: str, ref: str, requested: str) -> str:
    release_version = version("rust")
    if version("python") != release_version:
        raise ValueError("Rust and Python package versions must match")
    if event == "workflow_dispatch":
        if requested not in {"all", "rust", "python", "go"}:
            raise ValueError("unknown package family")
        return requested
    if event != "push":
        raise ValueError("unsupported package event")
    match = re.fullmatch(r"refs/tags/v(\d+\.\d+\.\d+)", ref)
    if not match:
        raise ValueError("invalid package tag")
    tagged = match.group(1)
    if tagged != release_version:
        raise ValueError("tag version does not match package manifest")
    return "all"


def main() -> None:
    package = select(
        os.environ["GITHUB_EVENT_NAME"],
        os.environ["GITHUB_REF"],
        os.environ.get("REQUESTED_PACKAGE", ""),
    )
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"package={package}\n")


if __name__ == "__main__":
    main()
