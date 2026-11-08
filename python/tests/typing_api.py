"""Consumer checks for the optional filesystem, fsspec, and tarfile integrations.

Core metadata, policy, and storage protocol checks live in typing_core.py; type-check
both files together.
"""

from typing import assert_type

import zstar
from zstar import (
    ArchiveWriter,
    AsyncSourceOptions,
    Entry,
    Header,
    SourceOptions,
)
from zstar.fsspec import FileInfo, ZstarFileSystem
from zstar.tar import from_tarinfo, to_tarinfo
from zstar.types import (
    AsyncBinaryWriter,
    AsyncRangeSource,
    AsyncRequestProvider,
    BinaryWriter,
    RangeSource,
    RequestProvider,
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
    header = from_tarinfo(to_tarinfo(entry), extensions=entry.extensions)
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
    zstar.open("https://example.invalid/archive", storage_options={"typo": 1})  # pyrefly: ignore[no-matching-overload]
    zstar.open(object())  # pyrefly: ignore[bad-argument-type]
    zstar.open(object(), mode="w")  # pyrefly: ignore[no-matching-overload]


def reject_factory_providers(asynchronous: AsyncRequestProvider) -> None:
    SourceOptions(request_provider=asynchronous)  # pyrefly: ignore[bad-argument-type]
    zstar.open(  # pyrefly: ignore[no-matching-overload]
        "https://example.invalid/archive",
        storage_options=AsyncSourceOptions(request_provider=asynchronous),
    )


async def reject_async_factory_providers(sync: RequestProvider) -> None:
    AsyncSourceOptions(request_provider=sync)  # pyrefly: ignore[bad-argument-type]
    await zstar.open(  # pyrefly: ignore[no-matching-overload]
        "https://example.invalid/archive",
        storage_options=SourceOptions(request_provider=sync),
        async_=True,
    )


def reject_open_modes() -> None:
    zstar.open("archive", "a")  # pyrefly: ignore[no-matching-overload]
    zstar.open("archive", atomic=True)  # pyrefly: ignore[no-matching-overload]
    zstar.open("archive", "w", options={"verify_external": True})  # pyrefly: ignore[no-matching-overload]
    zstar.open("archive", async_="yes")  # pyrefly: ignore[no-matching-overload]
