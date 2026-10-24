"""The native async reader has the same semantics for bytes and custom sources."""

import asyncio
import gc
import weakref

import pytest
import zstar
import zstar.format


@pytest.mark.parametrize("ranged", [False, True])
def test_async_iteration_reads_reports_verification_and_close(ranged, inline_data, source_factory):
    data = inline_data
    source = source_factory(data)

    async def exercise():
        archive = (
            await zstar.AsyncArchive.from_source(source, len(data))
            if ranged
            else await zstar.AsyncArchive.from_bytes(data)
        )
        async with archive:
            iterator = archive.getmembers()
            assert aiter(iterator) is iterator
            first = await anext(iterator)
            expected = [name async for name in archive.getnames()]
            assert first.name == expected[0]
            assert (await archive.getmember(first.name)).name == first.name
            with pytest.raises(KeyError):
                await archive.getmember("missing")
            assert archive.entry_count == len(expected)
            assert [first.name] + [entry.name async for entry in iterator] == expected
            assert [entry async for entry in iterator] == []
            assert source.calls == (2 if ranged else 0)
            assert await archive.read_many([("a-b", 0, 2), ("a-b", 1, 4)]) == [b"he", b"ello"]
            assert await archive.read("a-b", 1, 4) == b"ello"
            payload, report = await archive.read_with_report("a-b", 0, 5)
            assert payload == b"hello"
            assert report["metadata_authenticated"] and report["data_frames_checked"]
            verified = await archive.verify()
            assert verified["full_archive_verified"] and verified["tar_members"] == len(expected)
            pending = archive.getmembers()
        assert archive.closed and archive.cache_bytes == 0
        assert not source.closed
        await archive.close()
        with pytest.raises(ValueError, match="closed"):
            await anext(pending)
        with pytest.raises(ValueError, match="closed"):
            await archive.read("a-b")
        with pytest.raises(ValueError, match="closed"):
            async with archive:
                pass

    asyncio.run(exercise())


def test_async_open_authenticates_metadata_and_verify_checks_unread_content(corpus_dir):
    data = (corpus_dir / "invalid/data-checksum.zstar").read_bytes()

    async def exercise():
        async with await zstar.AsyncArchive.from_bytes(data) as archive:
            assert (await archive.getmember("a-b")).isfile()
            with pytest.raises(zstar.ZstarError) as caught:
                await archive.verify()
            assert caught.value.category == "malformed-frame"

    asyncio.run(exercise())


def test_async_close_releases_source_even_with_an_outstanding_iterator(source_factory):
    async def exercise():
        data = zstar.pack_file("x", b"hello")
        source = source_factory(data)
        borrowed = weakref.ref(source)
        archive = await zstar.AsyncArchive.from_source(source, len(data))
        iterator = archive.getmembers()
        del source
        assert borrowed() is not None
        await archive.close()
        gc.collect()
        assert borrowed() is None
        with pytest.raises(ValueError, match="closed"):
            await anext(iterator)

    asyncio.run(exercise())


@pytest.mark.parametrize("ranged", [False, True])
@pytest.mark.parametrize("verify_external", [True, False])
def test_async_resolvers_and_verification_policy(
    ranged, verify_external, thin_data, source_factory, object_resolver
):
    async def open_archive(**options):
        if ranged:
            return await zstar.AsyncArchive.from_source(
                source_factory(thin_data), len(thin_data), **options
            )
        return await zstar.AsyncArchive.from_bytes(thin_data, **options)

    async def exercise():
        async with await open_archive(
            resolver=object_resolver, verify_external=verify_external
        ) as archive:
            assert await archive.read("hello.txt") == b"hello\n"
            report = await archive.verify(external=True)
            assert report["external_closure_validated"]
            assert bool(report["external_objects_sha256_checked"]) == verify_external
            assert bool(report["external_objects_unchecked"]) != verify_external
        async with await open_archive(verify_external=verify_external) as archive:
            assert await archive.read("world.txt", resolver=object_resolver) == b"world\n"

    asyncio.run(exercise())
