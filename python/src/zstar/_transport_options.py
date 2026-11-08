"""Partial storage option dictionaries, validated and copied by Rust on open."""

import os
from typing import NotRequired, Required, TypedDict
from urllib.parse import urlsplit

from .types import AsyncRequestProvider, RequestProvider


class Credentials(TypedDict):
    """Explicit S3 credentials."""

    access_key_id: str
    secret_access_key: str
    session_token: NotRequired[str | None]


class S3Options(TypedDict, total=False):
    """S3 settings with an explicit region and credentials or anonymous access.

    Environment variables and ambient credential discovery are not used.
    """

    region: Required[str]
    endpoint: str | None
    anonymous: bool
    credentials: Credentials | None


class RangeCacheOptions(TypedDict, total=False):
    """Native compressed range cache; None disables caching on a source."""

    capacity_bytes: int
    max_entries: int
    read_ahead_bytes: int
    max_fetch_bytes: int
    max_in_flight: int


class _SourceOptions(TypedDict, total=False):
    headers: dict[str, str]
    s3: S3Options | None
    range_cache: RangeCacheOptions | None


class SourceOptions(_SourceOptions, total=False):
    """Native source transport configuration with a blocking request provider."""

    request_provider: RequestProvider | None


class AsyncSourceOptions(_SourceOptions, total=False):
    """Native source transport configuration with an awaitable request provider."""

    request_provider: AsyncRequestProvider | None


class SinkOptions(TypedDict, total=False):
    """Native output transport settings, separate from writer opening flags."""

    s3: S3Options | None


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
