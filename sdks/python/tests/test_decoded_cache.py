"""Caller-owned caches retain reader-created provenance, never arbitrary bytes."""

import asyncio
import threading

import pytest
import zstar
import zstar.format


class Cache:
    def __init__(self):
        self.values = {}
        self.gets = 0
        self.puts = 0

    def get(self, key):
        self.gets += 1
        return self.values.get(key)

    def put(self, token):
        self.puts += 1
        self.values[token.key] = token


@pytest.fixture
def open_reader(source_factory, inline_data):
    def open(cache, identity=b"fixture-v1", cls=zstar.format.ManifestArchive):
        source = source_factory()
        reader = cls(source, len(inline_data), shared_cache=cache, shared_cache_identity=identity)
        return reader, source

    return open


@pytest.mark.parametrize("cls", [zstar.format.ManifestArchive, zstar.format.IndexedArchive])
def test_custom_cache_reuse_ownership_and_version_keys(cls, open_reader):
    cache = Cache()
    for payload_reads in (1, 0):
        reader, source = open_reader(cache, cls=cls)
        before = source.calls
        with reader:
            assert reader.read("a-b", 0, 5) == b"hello"
            assert source.calls == before + payload_reads
    assert cache.gets == 2
    assert cache.puts == 1
    token = next(iter(cache.values.values()))
    assert isinstance(token, zstar.DecodedChunk)
    assert isinstance(token.data, bytes)
    assert token.weight >= len(token.data)
    with pytest.raises(TypeError):
        zstar.DecodedChunk()
    with pytest.raises(AttributeError):
        token.key = "forged"
    reader, source = open_reader(cache, identity=b"fixture-v2", cls=cls)
    before = source.calls
    with reader:
        assert reader.read("a-b", 0, 5) == b"hello"
        assert source.calls == before + 1
    assert len(cache.values) == 2


@pytest.mark.parametrize("failure", ["get", "put", "forged", "wrong-key"])
def test_custom_cache_failures_are_advisory(failure, open_reader):
    cache = Cache()
    first, _ = open_reader(cache)
    with first:
        first.read("a-b", 0, 5)
    token = next(iter(cache.values.values()))

    class Hostile:
        def get(self, key):
            if failure == "get":
                raise RuntimeError("unavailable cache")
            if failure == "wrong-key":
                return token
            return {"key": key, "data": b"forged"}

        def put(self, token):
            if failure == "put":
                raise RuntimeError("unavailable cache")

    reader, source = open_reader(Hostile(), identity=b"different-version")
    before = source.calls
    with reader:
        assert reader.read("a-b", 0, 5) == b"hello"
        assert source.calls == before + 1


@pytest.mark.parametrize("phase", ["get", "put"])
def test_async_cache_callback_cancellation(phase, inline_data, source_factory):
    entered = threading.Event()
    release = threading.Event()

    class Stalled(Cache):
        def get(self, key):
            if phase == "get":
                entered.set()
                assert release.wait(10)
            return

        def put(self, token):
            if phase == "put":
                entered.set()
                assert release.wait(10)

    async def exercise():
        reader = await zstar.AsyncArchive.from_source(
            source_factory(),
            len(inline_data),
            shared_cache=Stalled(),
            shared_cache_identity=b"fixture-v1",
        )
        work = asyncio.ensure_future(reader.read("a-b", 0, 5))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            work.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(work, 1)
            await reader.close()
        finally:
            release.set()

    asyncio.run(exercise())


def test_memory_archive_accepts_custom_cache(inline_data):
    cache = Cache()
    for _ in range(2):
        with zstar.Archive(
            inline_data, shared_cache=cache, shared_cache_identity=b"fixture-v1"
        ) as reader:
            assert reader.read("a-b", 0, 5) == b"hello"
    assert cache.gets == 2
    assert cache.puts == 1


def test_invalid_custom_cache_contract_rejected_before_source_io(inline_data, source_factory):
    source = source_factory()
    with pytest.raises((TypeError, AttributeError)):
        zstar.format.IndexedArchive(
            source, len(inline_data), shared_cache=object(), shared_cache_identity=b"v1"
        )
    assert source.calls == 0


def test_shared_decoded_cache_reuses_content_between_readers(inline_data, source_factory):
    shared = zstar.DecodedMemoryCache(1 << 20)
    for identity, payload_reads in [(b"v1", 1), (b"v1", 0), (b"v2", 1)]:
        source = source_factory()
        with zstar.format.ManifestArchive(
            source, len(inline_data), shared_cache=shared, shared_cache_identity=identity
        ) as archive:
            before = source.calls
            assert archive.read("a-b", 0, 5) == b"hello"
            assert source.calls == before + payload_reads
            assert shared.used_bytes > 0
