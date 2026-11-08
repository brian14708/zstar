"""Native sessions retain ownership and one terminal result without facade state."""

import asyncio
import io

import pytest
from _support import pack_file
from zstar import Header, ZstarError, _native


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("owned", [False, True])
def test_native_reader_storage_ownership(tmp_path, asynchronous, owned):
    path = tmp_path / "archive"
    path.write_bytes(pack_file("data", b"hello"))

    async def run():
        source = await _native.aopen_source(str(path), None, local_path=True)
        reader = await _native.AsyncArchive._from_source(
            source, _native.ReaderConfig(None), owned=owned
        )
        await reader.close()
        await reader.close()
        assert source.closed is owned
        await source.aclose()

    if asynchronous:
        asyncio.run(run())
    else:
        source = _native.open_source(str(path), None, local_path=True)
        reader = _native.SyncArchive._from_source(source, _native.ReaderConfig(None), owned=owned)
        reader.close()
        reader.close()
        assert source.closed is owned
        source.close()


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("owned", [False, True])
def test_native_writer_storage_ownership(tmp_path, asynchronous, owned):
    path = tmp_path / "archive"

    async def run():
        sink = await _native.aopen_sink(str(path), None, local_path=True)
        writer = await _native.WriterSession.ato_sink(sink, _native.WriterConfig(None), owned=owned)
        await writer.aappend_bytes("data", b"hello")
        await writer.aclose()
        await writer.aabort()
        await writer.aclose()
        assert sink.closed is owned
        await sink.aclose()

    if asynchronous:
        asyncio.run(run())
    else:
        sink = _native.open_sink(str(path), None, local_path=True)
        writer = _native.WriterSession.to_sink(sink, _native.WriterConfig(None), owned=owned)
        writer.append_bytes("data", b"hello")
        writer.close()
        writer.abort()
        writer.close()
        assert sink.closed is owned
        sink.close()


@pytest.mark.parametrize("asynchronous", [False, True])
def test_native_terminal_error_is_replayed_after_storage_cleanup(tmp_path, asynchronous):
    path = tmp_path / "archive"

    async def run():
        sink = await _native.aopen_sink(str(path), None, local_path=True)
        writer = await _native.WriterSession.ato_sink(sink, _native.WriterConfig(None), owned=True)
        await writer.aappend_bytes("missing/data", b"hello")
        with pytest.raises(ZstarError) as first:
            await writer.aclose()
        assert sink.closed and not path.exists()
        for operation in (writer.aclose, writer.aabort):
            with pytest.raises(ZstarError) as replay:
                await operation()
            assert replay.value is first.value

    if asynchronous:
        asyncio.run(run())
    else:
        sink = _native.open_sink(str(path), None, local_path=True)
        writer = _native.WriterSession.to_sink(sink, _native.WriterConfig(None), owned=True)
        writer.append_bytes("missing/data", b"hello")
        with pytest.raises(ZstarError) as first:
            writer.close()
        assert sink.closed and not path.exists()
        for operation in (writer.close, writer.abort):
            with pytest.raises(ZstarError) as replay:
                operation()
            assert replay.value is first.value


def test_async_callback_failure_releases_admission_with_retained_traceback():
    error = OSError("body failed")

    class Body:
        async def read(self, size):
            raise error

    async def run():
        sink = io.BytesIO()
        writer = await _native.WriterSession.ato_sink(
            _native.BlockingAdapter(sink), _native.WriterConfig(None)
        )
        with pytest.raises(ZstarError) as caught:
            await asyncio.wait_for(writer.aappend(Header.file("data", 1), Body()), 2)
        assert caught.value.__cause__ is error
        await asyncio.wait_for(writer.aabort(), 2)
        assert not sink.closed

    asyncio.run(run())
