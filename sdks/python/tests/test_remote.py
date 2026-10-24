import asyncio
import hashlib
import inspect
import io
import os
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from itertools import count

import pytest
import zstar


@dataclass
class Route:
    data: bytes
    stall: bool = False
    put_stall: bool = False
    calls: list = field(default_factory=list)
    disconnected: threading.Event = field(default_factory=threading.Event)


@pytest.fixture(scope="module")
def serve():
    routes = {}
    identifiers = count()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def route(self):
            identifier = self.path.split("?", 1)[0].rsplit("/", 1)[-1]
            return routes[identifier]

        def do_HEAD(self):
            pytest.fail("signed GET must not need HEAD")

        def do_GET(self):
            route = self.route()
            if self.path.startswith(("/denied", "/failure")):
                code = 403 if self.path.startswith("/denied") else 503
                route.calls.append(("status", code))
                self.send_response(code)
                self.send_header("Content-Length", "0")
                self.send_header("Connection", "close")
                self.end_headers()
                return
            assert self.headers["Accept-Encoding"] == "identity"
            start, end = map(int, self.headers["Range"][6:].split("-"))
            route.calls.append((start, end))
            self.send_response(206)
            self.send_header("ETag", '"' + hashlib.sha256(route.data).hexdigest() + '"')
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(route.data)}")
            self.send_header("Content-Length", str(end - start + 1))
            self.send_header("Connection", "close")
            self.end_headers()
            if route.stall and len(route.calls) == 4:
                self.connection.settimeout(3)
                if self.connection.recv(1) == b"":
                    route.disconnected.set()
                return
            self.wfile.write(route.data[start : end + 1])

        def do_PUT(self):
            route = self.route()
            body = self.rfile.read(int(self.headers["Content-Length"]))
            route.calls.append(("PUT", body, self.headers.get("X-Signed")))
            if route.put_stall:
                self.connection.settimeout(3)
                if self.connection.recv(1) == b"":
                    route.disconnected.set()
                return
            self.send_response(403 if self.path.startswith("/denied") else 200)
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
    )

    @contextmanager
    def register(data, *, stall=False, put_stall=False):
        identifier = str(next(identifiers))
        route = Route(data, stall=stall, put_stall=put_stall)
        routes[identifier] = route
        try:
            url = f"http://127.0.0.1:{server.server_port}/archive/{identifier}?signature=secret"
            yield url, route.calls, route.disconnected
        finally:
            del routes[identifier]

    thread.start()
    try:
        yield register
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_native_http_async_reads_coalesce_and_use_verified_cache(serve):
    data = bytes(zstar.pack_file("hello", b"hello world"))
    with serve(data) as (url, calls, _):

        async def exercise():
            archive = await zstar.AsyncArchive.from_url(url, cache_bytes=65536)
            assert [name async for name in archive.getnames()] == ["hello"]
            assert len(calls) == 3  # GET size probe, footer, TOC
            results = await asyncio.gather(*(archive.read("hello", 6, 5) for _ in range(8)))
            assert results == [b"world"] * 8
            assert len(calls) == 4
            assert 0 < archive.cache_bytes <= 65536
            assert await archive.read("hello") == b"hello world"
            assert len(calls) == 4
            with pytest.raises(ValueError):
                await archive.read("hello", 12, 1)
            assert len(calls) == 4

        asyncio.run(exercise())


def test_remote_spill_factory_limits_and_borrowed_store_lifetime(serve):
    data = bytes(zstar.pack_file("hello", b"hello world"))
    stores = []

    def factory():
        store = io.BytesIO()
        stores.append(store)
        return store

    with serve(data) as (url, _, _):

        async def exercise():
            async with await zstar.AsyncArchive.from_url(
                url, memory_bytes=0, create_spill_store=factory
            ) as archive:
                assert await archive.read("hello") == b"hello world"
                assert archive.entry_count > 0
            assert archive.closed
            assert stores and all(not store.closed for store in stores)
            with pytest.raises(zstar.ZstarError, match="aggregate spool storage"):
                await zstar.AsyncArchive.from_url(url, max_spool_bytes=0)

        asyncio.run(exercise())
    for store in stores:
        store.close()


@pytest.mark.parametrize("readinto", [False, True])
def test_native_http_cancellation_closes_pending_network_io(serve, readinto):
    data = bytes(zstar.pack_file("hello", b"hello world"))
    with serve(data, stall=True) as (url, calls, disconnected):

        async def exercise():
            archive = await zstar.AsyncArchive.from_url(url)
            member = await archive.extractfile("hello")
            buffer = bytearray(b"?" * 11)
            with pytest.raises(TimeoutError):
                operation = member.readinto(buffer) if readinto else archive.read("hello")
                await asyncio.wait_for(operation, 0.1)
            assert await asyncio.to_thread(disconnected.wait, 2), (
                "Rust request survived Python cancellation"
            )
            assert archive.cache_bytes == 0
            assert buffer == b"?" * 11 and await member.tell() == 0
            if readinto:
                assert await member.readinto(buffer) == 11
                assert buffer == b"hello world"
                buffer.extend(b"!")
            else:
                assert await archive.read("hello") == b"hello world"
            assert len(calls) == 5

        asyncio.run(exercise())


def test_remote_close_cancels_shared_network_reads_and_joins_all_closers(serve):
    data = bytes(zstar.pack_file("hello", b"hello world"))
    with serve(data, stall=True) as (url, calls, disconnected):

        async def exercise():
            archive = await zstar.AsyncArchive.from_url(url)
            reads = [asyncio.create_task(archive.read("hello")) for _ in range(8)]

            async def started():
                while len(calls) < 4:
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(started(), 2)
            await asyncio.wait_for(asyncio.gather(archive.close(), archive.close()), 2)
            assert archive.closed and archive.cache_bytes == 0
            outcomes = await asyncio.gather(*reads, return_exceptions=True)
            assert all(isinstance(result, zstar.ZstarError) for result in outcomes)
            assert all(result.category == "unsupported-operation" for result in outcomes)
            assert await asyncio.to_thread(disconnected.wait, 2), "close left a network read alive"
            assert len(calls) == 4
            for operation in [
                lambda: anext(archive.getnames()),
                lambda: archive.getmember("hello"),
                lambda: archive.read("hello"),
                lambda: archive.read_with_report("hello", 0, 1),
                lambda: archive.read_many([("hello", 0, 1)]),
                archive.__aenter__,
            ]:
                with pytest.raises(ValueError, match="closed"):
                    result = operation()
                    if inspect.isawaitable(result):
                        await result
            await archive.close()

        asyncio.run(exercise())


def test_remote_async_context_closes_caches_and_preserves_body_failure(serve):
    data = bytes(zstar.pack_file("hello", b"hello world"))
    with serve(data) as (url, _calls, _disconnected):

        async def exercise():
            archive = await zstar.AsyncArchive.from_url(url)
            with pytest.raises(RuntimeError, match="body failed"):
                async with archive as entered:
                    assert entered is archive and not archive.closed
                    assert await archive.read("hello") == b"hello world"
                    assert archive.cache_bytes > 0
                    raise RuntimeError("body failed")
            assert archive.closed and archive.cache_bytes == 0
            with pytest.raises(ValueError, match="closed"):
                await archive.read("hello")

        asyncio.run(exercise())


def test_native_http_error_redacts_url():
    async def exercise():
        with pytest.raises(ValueError) as error:
            await zstar.AsyncArchive.from_url("invalid://private?signature=secret")
        assert "secret" not in str(error.value)
        assert "private" not in str(error.value)

    asyncio.run(exercise())


def test_put_cancellation_closes_pending_network_io(serve):
    with serve(b"", put_stall=True) as (url, calls, disconnected):

        async def exercise():
            future = zstar.put_presigned(b"upload", url)

            async def started():
                while not calls:
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(started(), 2)
            future.cancel()
            with pytest.raises(asyncio.CancelledError):
                await future
            assert await asyncio.to_thread(disconnected.wait, 2), "PUT survived Python cancellation"
            assert calls == [("PUT", b"upload", None)]

        asyncio.run(exercise())


def test_put_budget_rejects_before_request_provider():
    async def exercise():
        async def provider(method, force_refresh):
            pytest.fail("oversized upload reached provider")

        with pytest.raises(zstar.ZstarError) as error:
            zstar.put_presigned(b"x" * ((64 << 20) + 1), provider=provider)
        assert error.value.category == "resource-limit"

    asyncio.run(exercise())


def test_async_provider_refresh_preserves_loop_context_and_signed_headers(serve):
    import contextvars

    trace = contextvars.ContextVar("trace")
    data = bytes(zstar.pack_file("hello", b"hello provider"))
    with serve(data) as (url, calls, _):

        async def exercise():
            loop = asyncio.get_running_loop()
            trace.set("request-context")
            seen = []
            authorized = False

            async def provider(method, force_refresh):
                nonlocal authorized
                assert asyncio.get_running_loop() is loop
                assert trace.get() == "request-context"
                seen.append((method, force_refresh))
                await asyncio.sleep(0)
                authorized |= force_refresh
                return {
                    "url": url if authorized else url.replace("/archive", "/denied"),
                    "headers": {"X-Signed": "signed-value"},
                    "expires_in": 300,
                }

            archive = await zstar.AsyncArchive.from_url(provider=provider, cache_bytes=0)
            assert await archive.read("hello") == b"hello provider"
            assert seen[:2] == [("GET", False), ("GET", True)]
            assert len(seen) == 5  # rejected size probe + size/footer/TOC/payload
            assert calls[0] == ("status", 403)
            # PUT also refreshes once on authorization failure, replaying exact bytes.
            seen.clear()
            authorized = False
            await zstar.put_presigned(b"uploaded", provider=provider)
            assert seen == [("PUT", False), ("PUT", True)]
            assert calls[-2:] == [("PUT", b"uploaded", "signed-value")] * 2
            await zstar.put_presigned(b"", url, headers={"X-Signed": "empty"})
            assert calls[-1] == ("PUT", b"", "empty")

        asyncio.run(exercise())


def test_provider_expiry_refresh_and_non_auth_errors_do_not_retry(serve):
    data = bytes(zstar.pack_file("hello", b"hello"))
    with serve(data) as (url, _calls, _):

        async def exercise():
            seen = []

            async def expiring(method, force_refresh):
                seen.append(force_refresh)
                return {"url": url, "expires_in": 300 if force_refresh else 0}

            archive = await zstar.AsyncArchive.from_url(provider=expiring)
            assert await archive.read("hello") == b"hello"
            assert seen == [False, True] * 4
            seen.clear()

            async def failing(method, force_refresh):
                seen.append(force_refresh)
                return {"url": url.replace("/archive", "/failure")}

            with pytest.raises(zstar.ZstarError) as error:
                await zstar.AsyncArchive.from_url(provider=failing)
            assert error.value.category == "io"
            assert "secret" not in str(error.value)
            assert seen == [False]

            seen.clear()

            async def denied(method, force_refresh):
                seen.append(force_refresh)
                return {"url": url.replace("/archive", "/denied")}

            with pytest.raises(zstar.ZstarError):
                await zstar.AsyncArchive.from_url(provider=denied)
            assert seen == [False, True]

        asyncio.run(exercise())


@pytest.mark.parametrize("operation", ["GET", "PUT"])
def test_cancellation_cancels_stalled_python_provider(operation):
    async def exercise():
        started, cancelled = asyncio.Event(), asyncio.Event()

        async def provider(method, force_refresh):
            assert method == operation
            started.set()
            try:
                await asyncio.Future()
            finally:
                cancelled.set()

        future = (
            zstar.AsyncArchive.from_url(provider=provider)
            if operation == "GET"
            else zstar.put_presigned(b"x", provider=provider)
        )
        future = asyncio.ensure_future(future)
        await asyncio.wait_for(started.wait(), 2)
        future.cancel()
        with pytest.raises(asyncio.CancelledError):
            await future
        await asyncio.wait_for(cancelled.wait(), 2)

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "result",
    [
        None,
        {},
        {"url": 1},
        {"url": "https://private?secret", "expires_in": float("nan")},
        {"url": "https://private?secret", "headers": {"Authorization": 1}},
    ],
)
def test_invalid_provider_result_is_redacted(result):
    async def exercise():
        async def provider(method, force_refresh):
            return result

        with pytest.raises(zstar.ZstarError) as error:
            await zstar.AsyncArchive.from_url(provider=provider)
        assert "private" not in str(error.value)
        assert "secret" not in str(error.value)
        assert error.value.category == "io"

    asyncio.run(exercise())


def test_provider_exception_and_argument_errors():
    async def exercise():
        async def provider(method, force_refresh):
            raise RuntimeError("https://private?signature=secret")

        with pytest.raises(zstar.ZstarError, match="provider failed") as error:
            await zstar.AsyncArchive.from_url(provider=provider)
        assert "secret" not in str(error.value)
        with pytest.raises(ValueError, match="either"):
            await zstar.AsyncArchive.from_url("https://private", provider=provider)
        with pytest.raises(ValueError, match="either"):
            zstar.put_presigned(b"", provider=provider, headers={})
        with pytest.raises(TypeError):
            await zstar.AsyncArchive.from_url(provider=1)
        with pytest.raises(TypeError):
            await zstar.AsyncArchive.from_url(provider=lambda method, force: {})

    asyncio.run(exercise())


@pytest.mark.skipif(os.name != "posix", reason="Directory capture uses Unix metadata")
def test_parallel_provider_requests_have_independent_contexts(serve, tmp_path):
    import contextvars

    trace = contextvars.ContextVar("trace")
    (tmp_path / "a").write_bytes(b"a" * (1 << 20))
    (tmp_path / "b").write_bytes(b"b" * (1 << 20))
    data = zstar.pack_directory(str(tmp_path))
    with serve(data) as (url, _calls, _):

        async def exercise():
            trace.set("archive-context")
            active = 0
            peak = 0

            class Provider:
                async def __call__(self, method, force_refresh):
                    nonlocal active, peak
                    assert trace.get() == "archive-context"
                    trace.set("private-request-context")
                    active += 1
                    peak = max(peak, active)
                    try:
                        await asyncio.sleep(0.01)
                        return {"url": url}
                    finally:
                        active -= 1

            archive = await zstar.AsyncArchive.from_url(provider=Provider(), cache_bytes=0)
            got = await asyncio.gather(archive.read("a", 0, 1), archive.read("b", 0, 1))
            assert got == [b"a", b"b"]
            assert peak == 2
            assert trace.get() == "archive-context"

        asyncio.run(exercise())


@pytest.mark.parametrize("verify_external", [True, False])
def test_remote_external_policy_cannot_disable_data_checksums(serve, verify_external, corpus_dir):
    data = (corpus_dir / "invalid/data-checksum.zstar").read_bytes()
    with serve(data) as (url, _, _):

        async def exercise():
            archive = await zstar.AsyncArchive.from_url(url, verify_external=verify_external)
            with pytest.raises(zstar.ZstarError) as caught:
                await archive.read("a-b")
            assert caught.value.category == "malformed-frame"
            assert archive.cache_bytes == 0

        asyncio.run(exercise())


def test_remote_batch_ranges_share_a_frame_without_decoded_cache(serve):
    data = bytes(zstar.pack_file("x", b"hello world"))
    with serve(data) as (url, calls, _):

        async def exercise():
            archive = await zstar.AsyncArchive.from_url(url, cache_bytes=0)
            assert len(calls) == 3
            assert await archive.read_many([("x", 6, 5), ("x", 0, 5), ("x", 0, 11)]) == [
                b"world",
                b"hello",
                b"hello world",
            ]
            assert len(calls) == 4
            assert archive.cache_bytes == 0
            with pytest.raises(zstar.ZstarError):
                await archive.read_many([("x", 0, 5), ("x", 12, 1)])
            assert len(calls) == 4

        asyncio.run(exercise())


def test_remote_external_objects_share_encoded_resolver_reads_and_report_scope(
    serve, object_resolver, thin_data
):
    data = thin_data

    resolver = object_resolver
    with serve(data) as (url, calls, _):

        async def exercise():
            archive = await zstar.AsyncArchive.from_url(url)
            assert archive.entry_count > 0
            before = len(calls)
            assert await archive.read_many(
                [("repeat", 0, 12), ("world.txt", 0, 6)], resolver=resolver
            ) == [b"hello\nhello\n", b"world\n"]
            raw, report = await archive.read_with_report("hello.txt", 0, 6)
            assert raw == b"hello\n"
            assert report["metadata_authenticated"]
            assert not report["full_archive_validated"]
            assert report["external_objects_sha256_checked"]
            assert not report["external_objects_unchecked"]
            assert resolver.calls == 1
            assert len(calls) == before  # No physical pointer fetch for selective reads.

        asyncio.run(exercise())


@pytest.mark.parametrize("verify_external", [True, False])
def test_remote_external_hash_policy_and_encoded_resolver_error_categories(
    serve, verify_external, corpus_dir, thin_data
):
    data = thin_data

    class Wrong:
        def get_encoded(self, address, hint, max_bytes):
            return (corpus_dir / "invalid/object-wrong-hash.zst").read_bytes()

    class Missing:
        def get_encoded(self, address, hint, max_bytes):
            raise FileNotFoundError(address)

    with serve(data) as (url, _, _):

        async def exercise():
            archive = await zstar.AsyncArchive.from_url(url, verify_external=verify_external)
            if verify_external:
                with pytest.raises(zstar.ZstarError) as caught:
                    await archive.read("hello.txt", resolver=Wrong())
                assert caught.value.category == "integrity-failure"
                assert archive.cache_bytes == 0
            else:
                raw, report = await archive.read_with_report("hello.txt", 0, 6, resolver=Wrong())
                assert raw == b"wrong\n"
                assert report["external_objects_unchecked"]
                assert not report["external_objects_sha256_checked"]
            # A fresh reader ensures the missing resolver is exercised.
            fresh = await zstar.AsyncArchive.from_url(url)
            with pytest.raises(zstar.ZstarError) as caught:
                await fresh.read("hello.txt", resolver=Missing())
            assert caught.value.category == "missing-external-content"

        asyncio.run(exercise())


def test_provider_exception_is_preserved_as_cause_with_redacted_message(serve):
    class Secret(RuntimeError):
        pass

    async def provider(method, force_refresh):
        raise Secret("https://signed.example/?sig=hunter2")

    async def exercise():
        with pytest.raises(zstar.ZstarError) as caught:
            await zstar.AsyncArchive.from_url(provider=provider)
        assert caught.value.category == "io"
        assert "hunter2" not in str(caught.value)
        assert isinstance(caught.value.__cause__, Secret)

    asyncio.run(exercise())


def test_async_metadata_calls_are_awaitable_and_share_the_loop_with_the_provider(serve):
    """Metadata calls neither block the loop nor deadlock with a same-loop provider."""
    data = bytes(zstar.pack_file("hello", b"hello provider"))
    with serve(data) as (url, _calls, _):

        async def exercise():
            ticks = 0
            running = True

            async def heartbeat():
                nonlocal ticks
                while running:
                    ticks += 1
                    await asyncio.sleep(0)

            async def provider(method, force_refresh):
                await asyncio.sleep(0.01)
                return {"url": url, "expires_in": 300}

            archive = await zstar.AsyncArchive.from_url(provider=provider, cache_bytes=0)
            from zstar import _native

            for awaitable in (
                archive._reader.entry("hello"),
                archive._reader._entry("hello"),
                archive._reader.paths(),
                archive._reader.validate_metadata(),
            ):
                assert inspect.isawaitable(awaitable)
                await awaitable
            beat = asyncio.create_task(heartbeat())
            results = await asyncio.wait_for(
                asyncio.gather(
                    archive.getmember("hello"),
                    archive.read("hello"),
                    archive._reader.paths(),
                    archive.extractfile("hello"),
                ),
                5,
            )
            running = False
            await beat
            assert results[0].name == "hello" and results[1] == b"hello provider"
            assert results[2] == ["hello"] and ticks > 1
            assert _native.AsyncArchive.read_link is not None

        asyncio.run(exercise())


def test_native_http_sync_and_fsspec_read_without_head(serve):
    data = bytes(zstar.pack_file("hello", b"hello world"))
    with serve(data) as (url, calls, _):
        with zstar.Archive.from_url(url) as archive, archive.extractfile("hello") as member:
            assert member.read(5) == b"hello"
            assert member.read() == b" world"
        assert len(calls) == 4
        fs = zstar.ZstarFileSystem(
            storage_options={"headers": {"X-Test": "value"}}, skip_instance_cache=True
        )
        try:
            with fs.open(f"{url}!/hello", block_size=3) as member:
                assert member.read() == b"hello world"
        finally:
            fs.close()
