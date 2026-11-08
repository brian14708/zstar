"""Recognize storage locations without changing local path semantics."""

import os
from urllib.parse import urlsplit


def _location(value: str | os.PathLike[str]) -> tuple[str, bool]:
    """Recognize storage URLs; PathLike values always denote literal local paths."""
    if not isinstance(value, str):
        return os.fspath(value), True
    try:
        parsed = urlsplit(value)
    except ValueError:
        raise ValueError("invalid storage URL") from None
    if parsed.scheme not in ("file", "http", "https", "s3"):
        if parsed.scheme and "://" in value:
            raise ValueError("unsupported storage URL scheme")
        return value, True
    if parsed.scheme != "file" and not parsed.netloc:
        raise ValueError("storage URL requires an authority")
    return value, False
