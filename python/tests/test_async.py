"""Async ownership, lazy iteration, and external verification using explicit types."""

import asyncio
import gc
import weakref

import pytest
import zstar
from zstar import (
    AsyncReaderOptions,
    EntryKind,
    ReadRequest,
    ZstarError,
    _native,
)


@pytest.mark.parametrize("ranged", [False, True])
def test_async_iteration_reads_and_close(ranged, inline_data, source_factory):
    async def run():
        source = source_factory(inline_data)
        adapted = (
            _native.BlockingAdapter(source)
            if ranged
            else await _native.AsyncSource.memory(inline_data)
        )
        archive = await zstar.open(adapted, async_=True)
        async with archive:
            iterator = archive.entries()
            first = await anext(iterator)
            expected = [entry.path async for entry in archive]
            assert (await archive.entry(first.path)).path == first.path
            assert [first.path] + [entry.path async for entry in iterator] == expected
            assert await archive.read_many(
                [ReadRequest("a-b", 0, 2), ReadRequest("a-b", 1, 4)]
            ) == [b"he", b"ello"]
            result = await archive.read("a-b", offset=0, length=5)
            assert result == b"hello"
            pending = archive.entries()
        assert not source.closed
        await archive.close()
        with pytest.raises(ValueError, match="closed"):
            await anext(pending)

    asyncio.run(run())


def test_async_close_releases_source_even_with_outstanding_iterator(source_factory):
    async def run():
        source = source_factory()
        reference = weakref.ref(source)
        archive = await zstar.open(_native.BlockingAdapter(source), async_=True)
        pending = archive.entries()
        del source
        assert reference() is not None
        await archive.close()
        gc.collect()
        assert reference() is None
        with pytest.raises(ValueError):
            await anext(pending)

    asyncio.run(run())


@pytest.mark.parametrize("verify", [True, False])
def test_async_external_verification(verify, thin_data, object_resolver):
    async def run():
        options = AsyncReaderOptions(
            verify_external=verify, resolver=_native.BlockingAdapter(object_resolver)
        )
        async with zstar.open(
            await _native.AsyncSource.memory(thin_data), options=options, async_=True
        ) as archive:
            result = await archive.read("hello.txt", offset=0, length=6)
            assert result == b"hello\n"

    asyncio.run(run())


def test_lazy_open_does_not_validate_unselected_content(corpus_dir):
    async def run():
        async with (
            await _native.aopen_source(str(corpus_dir / "invalid/data-checksum.zstar")) as source,
            await zstar.open(source, async_=True) as archive,
        ):
            assert (await archive.entry("a-b")).kind is EntryKind.FILE
            with pytest.raises(ZstarError) as caught:
                await archive.read("a-b")
            assert caught.value.category == "integrity-failure"

    asyncio.run(run())
