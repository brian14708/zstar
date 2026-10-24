"""Custom spool I/O, aggregate limits, and borrowed-store lifetime."""

import io
from contextlib import ExitStack, suppress
from tempfile import TemporaryFile

import pytest
from zstar import Archive, AsyncArchive, ZstarError
from zstar.format import (
    EntryArchiveWriter,
    IndexedArchive,
    ManifestArchive,
    decode_manifest_block,
    encode_manifest_block,
)


class ShortStore(io.BytesIO):
    def read(self, size=-1):
        return super().read(min(size, 11))

    def write(self, data):
        return super().write(data[:7])


def test_writer_custom_stores_short_io_and_borrowed_lifetime():
    stores = []

    def factory():
        store = ShortStore()
        stores.append(store)
        return store

    output = io.BytesIO()
    writer = EntryArchiveWriter(
        output, memory_bytes=1, block_target=512, create_spill_store=factory
    )
    for i in range(100):
        body = bytes([i]) * 128
        writer.add((f"f{i:04}", 0, 420, 0, 0, 0, ()), io.BytesIO(body), len(body))
    writer.close()
    assert len(stores) >= 2
    assert all(not store.closed for store in stores)
    with Archive(output.getvalue()) as reader:
        assert reader.read("f0099") == bytes([99]) * 128
        reader.verify()
    for store in stores:
        store.close()


@pytest.mark.parametrize("kind", ["bytes", "manifest", "indexed"])
def test_reader_factory_temporary_files_and_cold_lookup(kind, inline_data, source_factory):
    with ExitStack() as stack:
        stores = []

        def factory():
            store = stack.enter_context(TemporaryFile())
            stores.append(store)
            return store

        options = dict(memory_bytes=1, create_spill_store=factory)
        if kind == "bytes":
            reader = Archive(inline_data, **options)
        elif kind == "manifest":
            reader = ManifestArchive(source_factory(), len(inline_data), **options)
        else:
            reader = IndexedArchive(source_factory(), len(inline_data), **options)
        try:
            assert reader.read("a/file", 2047, 3) == bytes([255, 0, 1])
            if kind == "indexed":
                assert reader._entry("link")["kind"] == "symlink"
            elif kind == "bytes":
                assert reader.getmember("link").linkname == "../a//file"
            else:
                assert reader.entry("link")[7]["linkname"] == "../a//file"
            assert stores
        finally:
            reader.close()
        assert all(not store.closed for store in stores)
    assert all(store.closed for store in stores)


@pytest.mark.parametrize("kind", ["bytes", "manifest", "indexed", "writer"])
def test_quota_rejected_before_factory_callback(kind, inline_data, source_factory):
    def factory():
        pytest.fail("quota must be checked before spill-store I/O")

    options = dict(memory_bytes=0, max_spool_bytes=0, create_spill_store=factory)
    with pytest.raises(ZstarError, match="aggregate spool storage"):
        if kind == "bytes":
            Archive(inline_data, **options)
        elif kind == "manifest":
            ManifestArchive(source_factory(), len(inline_data), **options)
        elif kind == "indexed":
            IndexedArchive(source_factory(), len(inline_data), **options)
        else:
            writer = EntryArchiveWriter(io.BytesIO(), **options)
            try:
                writer.add(("file", 0, 420, 0, 0, 0, ()))
                writer.close()
            finally:
                with suppress(ValueError):
                    writer.close()


@pytest.mark.parametrize("failure", ["nonempty", "factory", "write", "read"])
def test_bad_store_failure_is_reported_and_borrowed_store_remains_open(
    failure, inline_data, source_factory
):
    class BadStore(io.BytesIO):
        def write(self, data):
            if failure == "write":
                return len(data) + 1
            return super().write(data)

        def read(self, size=-1):
            if failure == "read":
                return bytes(size + 1)
            return super().read(size)

    store = BadStore(b"nonempty" if failure == "nonempty" else b"")

    def factory():
        if failure == "factory":
            raise RuntimeError("factory failed")
        return store

    with pytest.raises(ZstarError):
        reader = ManifestArchive(
            source_factory(), len(inline_data), memory_bytes=0, create_spill_store=factory
        )
        try:
            reader.entry("a/file")
        finally:
            reader.close()
    assert not store.closed
    store.close()


def test_factory_must_be_callable(inline_data):
    with pytest.raises(TypeError, match="callable"):
        Archive(inline_data, create_spill_store=42)


def test_encode_cbor_local_nesting_boundary():
    value = 0
    for _ in range(64):
        value = (value,)
    assert decode_manifest_block(encode_manifest_block(value)) == value
    with pytest.raises(ZstarError, match="nesting"):
        encode_manifest_block((value,))


def test_async_constructors_apply_spool_controls(inline_data, source_factory):
    import asyncio

    async def run():
        with pytest.raises(ZstarError, match="aggregate spool storage"):
            await AsyncArchive.from_bytes(inline_data, max_spool_bytes=0)
        with pytest.raises(ZstarError, match="aggregate spool storage"):
            await AsyncArchive.from_source(source_factory(), len(inline_data), max_spool_bytes=0)

    asyncio.run(run())
