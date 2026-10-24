#!/usr/bin/env python3
"""Write portable SHA256SUMS for one artifact directory."""

import hashlib
import sys
from pathlib import Path


def write_checksums(directory: Path | str) -> None:
    root = Path(directory).resolve(strict=True)
    manifest = root / "SHA256SUMS"
    # rglob does not recurse into symlinked directories; skip file links too.
    files = sorted(
        path
        for path in root.rglob("*")
        if path != manifest and not path.is_symlink() and path.is_file()
    )
    if not files:
        raise SystemExit("no release artifacts found")
    lines = []
    for path in files:
        with path.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        lines.append(f"{digest}  {path.relative_to(root).as_posix()}\n")
    manifest.write_text("".join(lines))


if __name__ == "__main__":
    write_checksums(sys.argv[1])
