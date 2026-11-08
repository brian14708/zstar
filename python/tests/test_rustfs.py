"""Opt-in disposable RustFS test. No cloud endpoints or default credentials."""

import asyncio
import os
import uuid
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import zstar
from _support import (
    S3_ACCESS_KEY_ID,
    S3_REGION,
    S3_SECRET_ACCESS_KEY,
    aopen_s3,
    aopen_url,
    open_bytes,
    open_s3,
    pack_directory,
    pack_file,
    s3_client,
    s3_options,
    s3fs_options,
)
from zstar import ReadRequest, _native
from zstar._fsspec import ZstarFileSystem
from zstar.types import RangeCacheOptions, SinkOptions, SourceOptions


@pytest.mark.skipif(not os.getenv("ZSTAR_S3_ENDPOINT"), reason="requires local RustFS")
@pytest.mark.parametrize("asynchronous", [False, True])
def test_native_s3_sink_publication_abort_and_source_snapshot(asynchronous):
    from botocore.exceptions import ClientError

    endpoint = os.environ["ZSTAR_S3_ENDPOINT"]
    parsed = urlsplit(endpoint)
    assert parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    client = s3_client(endpoint)
    bucket = "zstar-native-sink-" + uuid.uuid4().hex
    client.create_bucket(Bucket=bucket)
    config = s3_options(endpoint)
    location = f"s3://{bucket}/archive.zstar"
    # Incompressible input larger than one 8 MiB part forces the native sink
    # through its multipart path.
    payload = os.urandom(10 << 20)

    def unpublished():
        with pytest.raises(ClientError):
            client.head_object(Bucket=bucket, Key="archive.zstar")
        assert client.list_multipart_uploads(Bucket=bucket).get("Uploads")

    async def run():
        sink = await _native.aopen_sink(location, options=SinkOptions(s3=config))
        async with zstar.open(sink, mode="w", async_=True) as writer:
            await writer.writestr("data", payload)
        await asyncio.to_thread(unpublished)
        await sink.aclose()
        async with (
            await _native.aopen_source(location, options=SourceOptions(s3=config)) as source,
            await zstar.open(source, async_=True) as archive,
        ):
            assert await archive.read("data", offset=0, length=5) == payload[:5]
            await asyncio.to_thread(
                client.put_object,
                Bucket=bucket,
                Key="archive.zstar",
                Body=pack_file("data", b"replacement"),
            )
            # The native source retains its initial suffix snapshot; use a
            # middle frame that was neither prefetched nor read before replacement.
            with pytest.raises(zstar.ZstarError):
                await archive.read("data", offset=len(payload) // 2, length=5)
        with pytest.raises(RuntimeError, match="abort"):
            async with await _native.aopen_sink(location, options=SinkOptions(s3=config)) as sink:
                async with zstar.open(sink, mode="w", async_=True) as writer:
                    await writer.writestr("unfinished", payload)
                raise RuntimeError("abort")

    try:
        if asynchronous:
            asyncio.run(run())
        else:
            sink = _native.open_sink(location, options=SinkOptions(s3=config))
            with zstar.open(sink, mode="w") as writer:
                writer.writestr("data", payload)
            unpublished()
            sink.close()
            with (
                _native.open_source(location, options=SourceOptions(s3=config)) as source,
                zstar.open(source) as archive,
            ):
                assert archive.read("data", offset=0, length=5) == payload[:5]
                client.put_object(
                    Bucket=bucket,
                    Key="archive.zstar",
                    Body=pack_file("data", b"replacement"),
                )
                # Fetch an uncached middle frame, outside the pinned suffix.
                with pytest.raises(zstar.ZstarError):
                    archive.read("data", offset=len(payload) // 2, length=5)
            with (
                pytest.raises(RuntimeError, match="abort"),
                _native.open_sink(location, options=SinkOptions(s3=config)) as sink,
            ):
                with zstar.open(sink, mode="w") as writer:
                    writer.writestr("unfinished", payload)
                raise RuntimeError("abort")
        assert not client.list_multipart_uploads(Bucket=bucket).get("Uploads")
        response = client.get_object(Bucket=bucket, Key="archive.zstar")["Body"]
        try:
            with open_bytes(response.read()) as archive:
                assert archive.read("data") == b"replacement"
        finally:
            response.close()
    finally:
        for upload in client.list_multipart_uploads(Bucket=bucket).get("Uploads", []):
            client.abort_multipart_upload(
                Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"]
            )
        client.delete_object(Bucket=bucket, Key="archive.zstar")
        client.delete_bucket(Bucket=bucket)


@pytest.mark.skipif(not os.getenv("ZSTAR_S3_ENDPOINT"), reason="requires local RustFS")
def test_native_s3_and_presigned_archive_and_external_objects(monkeypatch):
    pytest.importorskip("boto3")
    from botocore.config import Config

    endpoint = os.environ["ZSTAR_S3_ENDPOINT"]
    parsed = urlsplit(endpoint)
    assert parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", S3_ACCESS_KEY_ID)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", S3_SECRET_ACCESS_KEY)
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    client = s3_client(endpoint, config=Config(signature_version="s3v4"))
    bucket = "zstar-python-" + uuid.uuid4().hex
    client.create_bucket(Bucket=bucket)
    keys = []
    root = Path(__file__).resolve().parents[2] / "testdata"
    try:
        keys.append("thin.zstar")
        thin = (root / "entry-manifest/thin.zstar").read_bytes()
        client.put_object(Bucket=bucket, Key=keys[-1], Body=thin)
        assert client.head_object(Bucket=bucket, Key=keys[-1])["Metadata"] == {}
        for path in (root / "entry-manifest/objects").glob("*.zst"):
            keys.append(f"objects/{path.name}")
            client.put_object(Bucket=bucket, Key=keys[-1], Body=path.read_bytes())
        portable = bytes(pack_file("hello", b"hello RustFS"))
        keys.append("inline.zstar")
        client.put_object(Bucket=bucket, Key=keys[-1], Body=portable)
        url = client.generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": "inline.zstar"}, ExpiresIn=60
        )

        # Native readers support inline and externally stored members.
        with open_s3(
            bucket,
            "inline.zstar",
            s3=s3_options(endpoint),
        ) as native:
            assert native.read("hello", offset=6, length=6) == b"RustFS"

        with open_s3(
            bucket,
            "thin.zstar",
            s3=s3_options(endpoint),
            object_prefix="objects",
        ) as native:
            assert native.read("hello.txt") == b"hello\n"
        with pytest.raises(zstar.ZstarError) as caught:
            open_s3(
                bucket,
                "missing.zstar",
                s3=s3_options(endpoint),
            )
        assert caught.value.category == "io"

        import fsspec

        with fsspec.open(
            "zstar://hello",
            fo=f"s3://{bucket}/inline.zstar",
            target_options={"client_kwargs": {"endpoint_url": endpoint, "region_name": S3_REGION}},
        ) as member:
            assert member.read(5) == b"hello"
            member.seek(-6, 2)
            assert member.read() == b"RustFS"

        async def exercise():
            native = await aopen_s3(
                bucket,
                "thin.zstar",
                s3=s3_options(endpoint),
                object_prefix="objects",
            )
            expected_files = {
                "hello.txt": b"hello\n",
                "repeat": b"hello\nhello\n",
                "world.txt": b"world\n",
            }
            for path, expected in expected_files.items():
                assert await native.read(path) == expected
            assert len([entry async for entry in native]) == 3
            external = expected_files["hello.txt"]
            assert await native.read_many(
                [
                    ReadRequest("hello.txt", 1, 4),
                    ReadRequest("hello.txt", 0, len(external)),
                    ReadRequest("hello.txt", 0, 0),
                ]
            ) == [external[1:5], external, b""]
            presigned = await aopen_url(url)
            assert await presigned.read("hello", offset=6, length=6) == b"RustFS"
            refreshes = []

            async def get_provider(method, force_refresh):
                refreshes.append((method, force_refresh))
                assert method == "GET"
                if not force_refresh:
                    return {"url": url, "expires_in": 0}
                return {
                    "url": client.generate_presigned_url(
                        "get_object", Params={"Bucket": bucket, "Key": "inline.zstar"}, ExpiresIn=60
                    ),
                    "expires_in": 60,
                }

            refreshed = await aopen_url(provider=get_provider)
            assert await refreshed.read("hello") == b"hello RustFS"
            assert refreshes == [("GET", False), ("GET", True)] * 5
            for key, body in [("uploaded.zstar", portable), ("empty.bin", b"")]:
                keys.append(key)

                async def put_provider(method, force_refresh):
                    assert method == "PUT"
                    return {
                        "url": client.generate_presigned_url(
                            "put_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=60
                        ),
                        "expires_in": 60,
                    }

                await _native.put_presigned(body, provider=put_provider)
                head = client.head_object(Bucket=bucket, Key=key)
                assert head["ContentType"] == (
                    "application/zstd" if body else "application/octet-stream"
                )
                assert head["CacheControl"] == "private, no-cache, no-transform"
                assert "ContentEncoding" not in head
                response = client.get_object(Bucket=bucket, Key=key)
                try:
                    assert response["Body"].read() == body
                finally:
                    response["Body"].close()
            await asyncio.gather(native.close(), presigned.close(), refreshed.close())
            assert native.closed and presigned.closed and refreshed.closed

        asyncio.run(exercise())
    finally:
        for key in keys:
            client.delete_object(Bucket=bucket, Key=key)
        client.delete_bucket(Bucket=bucket)


@pytest.mark.skipif(not os.getenv("ZSTAR_S3_ENDPOINT"), reason="requires local RustFS")
def test_async_fsspec_s3_snapshot_conditions():

    endpoint = os.environ["ZSTAR_S3_ENDPOINT"]
    parsed = urlsplit(endpoint)
    assert parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    client = s3_client(endpoint)
    bucket = "zstar-fsspec-" + uuid.uuid4().hex
    client.create_bucket(Bucket=bucket)
    payload = b"A" * (2 << 20)
    encoded = pack_file("data", payload)
    client.put_object(Bucket=bucket, Key="archive.zstar", Body=encoded)
    try:

        async def exercise():
            # Every read must reach the transport to check snapshot conditions.
            options = s3fs_options(endpoint, default_cache_type="none")
            async with ZstarFileSystem(
                fo=f"s3://{bucket}/archive.zstar",
                target_options=options,
                asynchronous=True,
            ) as fs:
                assert await fs._cat_file("data", start=1, end=6) == b"AAAAA"
                assert await fs._cat_ranges(
                    ["data", "data"], [100, 1 << 20], [105, (1 << 20) + 5]
                ) == [b"AAAAA", b"AAAAA"]
                # Use a fresh region after replacement so a decoded chunk cannot
                # satisfy the request without consulting the pinned transport.
                await asyncio.to_thread(
                    client.put_object,
                    Bucket=bucket,
                    Key="archive.zstar",
                    Body=pack_file("data", b"B" * len(payload)),
                )
                with pytest.raises(zstar.ZstarError):
                    await fs._cat_file("data", start=(2 << 20) - 5)

        asyncio.run(exercise())
    finally:
        client.delete_object(Bucket=bucket, Key="archive.zstar")
        client.delete_bucket(Bucket=bucket)


@pytest.mark.skipif(not os.getenv("ZSTAR_S3_ENDPOINT"), reason="requires local RustFS")
@pytest.mark.parametrize("backend_async", [False, True])
def test_s3fs_explicit_version_chain_uses_snapshot(backend_async):
    pytest.importorskip("s3fs")
    import fsspec.core

    endpoint = os.environ["ZSTAR_S3_ENDPOINT"]
    parsed = urlsplit(endpoint)
    assert parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    client = s3_client(endpoint)
    bucket = "zstar-version-" + uuid.uuid4().hex
    client.create_bucket(Bucket=bucket)
    versions = []
    try:
        client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        for body in [b"old", b"new"]:
            response = client.put_object(
                Bucket=bucket, Key="archive.zstar", Body=pack_file("x", body)
            )
            versions.append(response["VersionId"])

        async def exercise():
            fs, member = fsspec.core.url_to_fs(
                f"zstar://x::s3://{bucket}/archive.zstar?versionId={versions[0]}",
                zstar={"asynchronous": True},
                s3=s3fs_options(endpoint, asynchronous=backend_async, version_aware=True),
            )
            try:
                assert await fs._cat_file(member) == b"old"
            finally:
                await fs.close()

        asyncio.run(exercise())
    finally:
        for version in versions:
            client.delete_object(Bucket=bucket, Key="archive.zstar", VersionId=version)
        client.delete_bucket(Bucket=bucket)


@pytest.mark.skipif(not os.getenv("ZSTAR_S3_ENDPOINT"), reason="requires local RustFS")
@pytest.mark.parametrize("cache_type", ["none", "blockcache"])
def test_fsspec_s3_uses_ranges(tmp_path, cache_type):
    import http.client
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    endpoint = os.environ["ZSTAR_S3_ENDPOINT"]
    upstream = urlsplit(endpoint)
    assert upstream.scheme == "http" and upstream.hostname in {"127.0.0.1", "localhost", "::1"}
    client = s3_client(endpoint)
    bucket = "zstar-native-url-" + uuid.uuid4().hex
    key = "nested/archive v1.zstar"
    payload = os.urandom(5 << 20)
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested/data").write_bytes(payload)
    encoded = bytes(pack_directory(tmp_path))
    client.create_bucket(Bucket=bucket)
    client.put_object(Bucket=bucket, Key=key, Body=encoded)
    requests = []

    class Proxy(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def forward(self):
            requests.append((self.command, self.headers.get("Range")))
            connection = http.client.HTTPConnection(upstream.hostname, upstream.port)
            try:
                connection.request(self.command, self.path, headers=dict(self.headers))
                response = connection.getresponse()
                self.send_response(response.status)
                for name, value in response.getheaders():
                    if name.lower() not in {"connection", "transfer-encoding"}:
                        self.send_header(name, value)
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(response.read())
            finally:
                connection.close()

        do_GET = forward
        do_HEAD = forward

    server = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    worker.start()
    try:
        fs = ZstarFileSystem(
            fo=f"s3://{bucket}/{key}",
            target_options=s3fs_options(
                f"http://127.0.0.1:{server.server_port}",
                default_cache_type=cache_type,
                default_block_size=64 << 10,
            ),
        )
        try:
            with fs.open("nested/data") as member:
                assert member.read(10) == payload[:10]
                member.seek(-10, 2)
                assert member.read(10) == payload[-10:]
        finally:
            fs.close()
        assert any(method == "HEAD" for method, _ in requests)
        ranges = [value for method, value in requests if method == "GET"]
        assert ranges and all(value is not None for value in ranges)
        fetched = 0
        for value in ranges:
            start, end = map(int, value.removeprefix("bytes=").split("-"))
            fetched += end - start + 1
        assert fetched < len(encoded)
    finally:
        server.shutdown()
        server.server_close()
        worker.join()
        client.delete_object(Bucket=bucket, Key=key)
        client.delete_bucket(Bucket=bucket)


@pytest.mark.skipif(not os.getenv("ZSTAR_S3_ENDPOINT"), reason="requires local RustFS")
def test_fsspec_s3_writer_commit_and_abort():
    import fsspec

    endpoint = os.environ["ZSTAR_S3_ENDPOINT"]
    client = s3_client(endpoint)
    bucket = "zstar-write-" + uuid.uuid4().hex
    client.create_bucket(Bucket=bucket)
    options = s3fs_options(endpoint)
    location = f"s3://{bucket}/archive.zstar"
    try:
        with fsspec.filesystem(
            "zstar",
            fo=location,
            mode="w",
            target_options=options,
        ) as fs:
            fs.pipe_file("dir/data", b"hello")
        original = client.get_object(Bucket=bucket, Key="archive.zstar")["Body"]
        try:
            encoded = original.read()
        finally:
            original.close()
        with open_bytes(encoded) as archive:
            assert archive.read("dir/data") == b"hello"

        with (
            pytest.raises(RuntimeError, match="abort"),
            fsspec.filesystem("zstar", fo=location, mode="w", target_options=options) as fs,
        ):
            fs.pipe_file("replacement", b"unfinished")
            raise RuntimeError("abort")
        with fsspec.filesystem("zstar", fo=location, target_options=options) as fs:
            assert fs.cat_file("dir/data") == b"hello"
        assert not client.list_multipart_uploads(Bucket=bucket).get("Uploads")
    finally:
        client.delete_object(Bucket=bucket, Key="archive.zstar")
        client.delete_bucket(Bucket=bucket)


@pytest.mark.skipif(not os.getenv("ZSTAR_S3_ENDPOINT"), reason="requires local RustFS")
@pytest.mark.parametrize("asynchronous", [False, True])
def test_owned_s3_factories_publish_and_abort(asynchronous):

    endpoint = os.environ["ZSTAR_S3_ENDPOINT"]
    client = s3_client(endpoint)
    bucket = "zstar-factory-" + uuid.uuid4().hex
    client.create_bucket(Bucket=bucket)
    location = f"s3://{bucket}/archive.zstar"
    config = s3_options(endpoint)
    storage = SinkOptions(s3=config)
    payload = os.urandom(10 << 20)

    async def run():
        async with zstar.open(location, options={"sink": storage}, mode="w", async_=True) as writer:
            await writer.writestr("data", payload)
        async with zstar.open(
            location,
            options={"source": SourceOptions(s3=config, range_cache=RangeCacheOptions())},
            async_=True,
        ) as archive:
            assert await archive.read("data", offset=12, length=8) == payload[12:20]
        with pytest.raises(RuntimeError, match="abort"):
            async with zstar.open(
                location, options={"sink": storage}, mode="w", async_=True
            ) as writer:
                await writer.writestr("data", payload)
                raise RuntimeError("abort")

    try:
        if asynchronous:
            asyncio.run(run())
        else:
            with zstar.open(location, options={"sink": storage}, mode="w") as writer:
                writer.writestr("data", payload)
            with zstar.open(
                location,
                options={"source": SourceOptions(s3=config, range_cache=RangeCacheOptions())},
            ) as archive:
                assert archive.read("data", offset=12, length=8) == payload[12:20]
            with (
                pytest.raises(RuntimeError, match="abort"),
                zstar.open(location, options={"sink": storage}, mode="w") as writer,
            ):
                writer.writestr("data", payload)
                raise RuntimeError("abort")
        with pytest.raises(ValueError):
            zstar.open(location, mode="x", options={"sink": SinkOptions(s3=config)})
        assert not client.list_multipart_uploads(Bucket=bucket).get("Uploads")
        with zstar.open(location, options={"source": SourceOptions(s3=config)}) as archive:
            assert archive.read("data", offset=12, length=8) == payload[12:20]
    finally:
        for upload in client.list_multipart_uploads(Bucket=bucket).get("Uploads", []):
            client.abort_multipart_upload(
                Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"]
            )
        client.delete_object(Bucket=bucket, Key="archive.zstar")
        client.delete_bucket(Bucket=bucket)
        client.close()


@pytest.mark.skipif(not os.getenv("ZSTAR_S3_ENDPOINT"), reason="requires local RustFS")
def test_native_publication_and_abort_errors_are_both_reported():
    import http.client
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    endpoint = os.environ["ZSTAR_S3_ENDPOINT"]
    upstream = urlsplit(endpoint)
    client = s3_client(endpoint)
    bucket = "zstar-double-failure-" + uuid.uuid4().hex
    client.create_bucket(Bucket=bucket)
    failed = []

    class Proxy(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def forward(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if "uploadId=" in self.path and self.command in ("POST", "DELETE"):
                failed.append(self.command)
                error = (
                    b"<Error><Code>AccessDenied</Code><Message>controlled failure</Message></Error>"
                )
                self.send_response(403)
                self.send_header("Content-Length", str(len(error)))
                self.end_headers()
                self.wfile.write(error)
                return
            connection = http.client.HTTPConnection(upstream.hostname, upstream.port)
            try:
                connection.request(self.command, self.path, body=body, headers=dict(self.headers))
                response = connection.getresponse()
                self.send_response(response.status)
                for name, value in response.getheaders():
                    if name.lower() not in {"connection", "transfer-encoding"}:
                        self.send_header(name, value)
                self.end_headers()
                self.wfile.write(response.read())
            finally:
                connection.close()

        do_POST = do_PUT = do_DELETE = forward

    server = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    worker.start()
    config = s3_options(f"http://127.0.0.1:{server.server_port}")
    storage = SinkOptions(s3=config)
    try:
        with (
            pytest.raises(ExceptionGroup) as caught,
            zstar.open(f"s3://{bucket}/archive", options={"sink": storage}, mode="w") as writer,
        ):
            writer.writestr("data", os.urandom(10 << 20))
        assert "POST" in failed and "DELETE" in failed

        def leaves(error):
            if isinstance(error, BaseExceptionGroup):
                return [leaf for child in error.exceptions for leaf in leaves(child)]
            return [error]

        errors = leaves(caught.value)
        assert len(errors) >= 2 and all(isinstance(error, zstar.ZstarError) for error in errors)
        assert client.list_multipart_uploads(Bucket=bucket).get("Uploads")
    finally:
        server.shutdown()
        server.server_close()
        worker.join()
        for upload in client.list_multipart_uploads(Bucket=bucket).get("Uploads", []):
            client.abort_multipart_upload(
                Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"]
            )
        client.delete_object(Bucket=bucket, Key="archive")
        client.delete_bucket(Bucket=bucket)
        client.close()
