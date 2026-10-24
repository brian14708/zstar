#!/usr/bin/env python3
"""Select a package family and reject tags that disagree with its manifest."""

import os
import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def version(family: str) -> str:
    if family == "rust":
        return tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    if family == "python":
        return tomllib.loads((ROOT / "sdks/python/pyproject.toml").read_text())["project"][
            "version"
        ]
    raise ValueError("unknown package family")


def select(event: str, ref: str, requested: str) -> str:
    if event == "workflow_dispatch":
        version(requested)  # Reject unknown families.
        return requested
    if event != "push":
        raise ValueError("unsupported package event")
    match = re.fullmatch(r"refs/tags/(rust|python)/v(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)", ref)
    if not match:
        raise ValueError("invalid package tag")
    family, tagged = match.groups()
    if tagged != version(family):
        raise ValueError("tag version does not match package manifest")
    return family


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
