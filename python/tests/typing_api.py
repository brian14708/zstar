"""Consumer checks for the optional fsspec integration.

Core metadata, policy, and storage protocol checks live in typing_core.py; type-check
both files together.
"""

from typing import assert_type

import zstar
from zstar import ArchiveWriter, Entry, Header
from zstar.fsspec import FileInfo, ZstarFileSystem
from zstar.types import (
    AsyncBinaryWriter,
    AsyncRangeSource,
    AsyncRequestProvider,
    BinaryWriter,
    RangeSource,
    RequestProvider,
    SourceOptions,
)


def consume_integrations(
    fs: ZstarFileSystem,
    entry: Entry,
    writer: ArchiveWriter,
    sync_source: RangeSource,
    async_source: AsyncRangeSource,
    async_sink: AsyncBinaryWriter,
) -> None:
    assert_type(fs.info("data"), FileInfo)
    assert_type(fs.ls("", detail=False), list[str])
    header = entry.to_header()
    assert_type(header, Header)
    writer.add("directory", "data")
    zstar.open(async_source)  # pyrefly: ignore[bad-argument-type]
    zstar.open(async_sink, mode="w")  # pyrefly: ignore[no-matching-overload]
    zstar.open(sync_source, options={"resolver": async_source})  # pyrefly: ignore[no-matching-overload]
    writer.addfile(header, object())  # pyrefly: ignore[bad-argument-type]


async def reject_sync_callbacks(sync_source: RangeSource, sync_sink: BinaryWriter) -> None:
    await zstar.open(sync_source, async_=True)  # pyrefly: ignore[no-matching-overload]
    await zstar.open(sync_sink, mode="w", async_=True)  # pyrefly: ignore[no-matching-overload]


def reject_factory_settings() -> None:
    zstar.open("https://example.invalid/archive", options={"source": {"typo": 1}})  # pyrefly: ignore[no-matching-overload]
    zstar.open(object())  # pyrefly: ignore[bad-argument-type]
    zstar.open(object(), mode="w")  # pyrefly: ignore[no-matching-overload]


def shared_factory_providers(sync: RequestProvider, asynchronous: AsyncRequestProvider) -> None:
    # Both callback modes share the same dictionaries; opening enforces the mode.
    SourceOptions(request_provider=sync)
    SourceOptions(request_provider=asynchronous)
    zstar.open(
        "https://example.invalid/archive",
        options={"source": SourceOptions(request_provider=sync)},
    )


async def shared_async_factory_provider(asynchronous: AsyncRequestProvider) -> None:
    await zstar.open(
        "https://example.invalid/archive",
        options={"source": SourceOptions(request_provider=asynchronous)},
        async_=True,
    )


def reject_open_modes() -> None:
    zstar.open("archive", "a")  # pyrefly: ignore[no-matching-overload]
    zstar.open("archive", atomic=True)  # pyrefly: ignore[no-matching-overload]
    zstar.open("archive", "w", options={"verify_external": True})  # pyrefly: ignore[no-matching-overload]
    zstar.open("archive", async_="yes")  # pyrefly: ignore[no-matching-overload]
