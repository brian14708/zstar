"""Opt-in disposable RustFS test. No cloud endpoints or default credentials."""

import asyncio
import io
import os
import sys
import uuid
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import zstar


@pytest.mark.skipif(not os.getenv("ZSTAR_S3_ENDPOINT"), reason="requires local RustFS")
def test_native_s3_and_presigned_archive_and_external_objects(monkeypatch):
    boto3 = pytest.importorskip("boto3")
    from botocore.config import Config

    endpoint = os.environ["ZSTAR_S3_ENDPOINT"]
    parsed = urlsplit(endpoint)
    assert parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "zstar-test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "zstar-test-secret")
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    client = boto3.client(
        "s3",
        config=Config(signature_version="s3v4"),
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="zstar-test",
        aws_secret_access_key="zstar-test-secret",
    )
    bucket = "zstar-python-" + uuid.uuid4().hex
    client.create_bucket(Bucket=bucket)
    keys = []
    root = Path(__file__).resolve().parents[2] / "testdata"
    try:
        keys.append("thin.zstar")
        thin = (root / "entry-manifest/thin.zstar").read_bytes()
        client.put_object(
            Bucket=bucket, Key=keys[-1], Body=thin, Metadata=zstar.s3_archive_metadata(thin)
        )
        assert client.head_object(Bucket=bucket, Key=keys[-1])[
            "Metadata"
        ] == zstar.s3_archive_metadata(thin)
        for path in (root / "entry-manifest/objects").glob("*.zst"):
            keys.append(f"objects/{path.name}")
            client.put_object(Bucket=bucket, Key=keys[-1], Body=path.read_bytes())
        portable = bytes(zstar.pack_file("hello", b"hello RustFS"))
        keys.append("inline.zstar")
        client.put_object(
            Bucket=bucket, Key=keys[-1], Body=portable, Metadata=zstar.s3_archive_metadata(portable)
        )
        url = client.generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": "inline.zstar"}, ExpiresIn=60
        )

        # Synchronous reads and the fsspec adapter share the native transport.
        with zstar.Archive.from_s3(
            bucket, "inline.zstar", endpoint=endpoint, region="us-east-1"
        ) as native:
            assert native.read("hello", 6, 6) == b"RustFS"
            assert native.verify()["metadata_validated"]
        with zstar.Archive.from_s3(
            bucket, "thin.zstar", endpoint=endpoint, region="us-east-1", object_prefix="objects"
        ) as native:
            assert native.read("hello.txt") == b"hello\n"
        with pytest.raises(zstar.ZstarError) as caught:
            zstar.Archive.from_s3(bucket, "missing.zstar", endpoint=endpoint, region="us-east-1")
        assert caught.value.category == "io"

        import fsspec

        with fsspec.open(
            "zstar://hello",
            fo=f"s3://{bucket}/inline.zstar",
            target_options={"endpoint": endpoint, "region": "us-east-1"},
        ) as member:
            assert member.read(5) == b"hello"
            member.seek(-6, 2)
            assert member.read() == b"RustFS"
        fs = zstar.ZstarFileSystem(
            fo=f"s3://{bucket}/thin.zstar",
            target_options={
                "endpoint": endpoint,
                "region": "us-east-1",
                "object_prefix": "objects",
            },
        )
        try:
            with fs.open("hello.txt") as member:
                assert member.read() == b"hello\n"
        finally:
            fs.close()

        async def exercise():
            stores = []

            def create_store():
                store = io.BytesIO()
                stores.append(store)
                return store

            native = await zstar.AsyncArchive.from_s3(
                bucket,
                "thin.zstar",
                endpoint=endpoint,
                region="us-east-1",
                object_prefix="objects",
                memory_bytes=1,
                create_spill_store=create_store,
            )
            expected_files = {
                "hello.txt": b"hello\n",
                "repeat": b"hello\nhello\n",
                "world.txt": b"world\n",
            }
            for path, expected in expected_files.items():
                assert await native.read(path) == expected
            external = expected_files["hello.txt"]
            assert await native.read_many(
                [("hello.txt", 1, 4), ("hello.txt", 0, len(external)), ("hello.txt", 0, 0)]
            ) == [external[1:5], external, b""]
            presigned = await zstar.AsyncArchive.from_url(url)
            assert await presigned.read("hello", 6, 6) == b"RustFS"
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

            refreshed = await zstar.AsyncArchive.from_url(provider=get_provider)
            assert await refreshed.read("hello") == b"hello RustFS"
            assert refreshes == [("GET", False), ("GET", True)] * 4
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

                await zstar.put_presigned(body, provider=put_provider)
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
            assert stores and all(not store.closed for store in stores)
            for store in stores:
                store.close()

        asyncio.run(exercise())
    finally:
        for key in keys:
            client.delete_object(Bucket=bucket, Key=key)
        client.delete_bucket(Bucket=bucket)


@pytest.mark.skipif(not os.getenv("ZSTAR_S3_ENDPOINT"), reason="requires local RustFS")
def test_async_native_fsspec_s3_snapshot_conditions():
    import boto3

    endpoint = os.environ["ZSTAR_S3_ENDPOINT"]
    parsed = urlsplit(endpoint)
    assert parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="zstar-test",
        aws_secret_access_key="zstar-test-secret",
    )
    bucket = "zstar-fsspec-" + uuid.uuid4().hex
    client.create_bucket(Bucket=bucket)
    payload = b"A" * (2 << 20)
    encoded = zstar.pack_file("data", payload)
    client.put_object(Bucket=bucket, Key="archive.zstar", Body=encoded)
    try:

        async def exercise():
            options = {
                "key": "zstar-test",
                "secret": "zstar-test-secret",
                "client_kwargs": {"endpoint_url": endpoint, "region_name": "us-east-1"},
            }
            async with zstar.ZstarFileSystem(
                fo=f"s3://{bucket}/archive.zstar",
                target_options=options,
                asynchronous=True,
                cache_bytes=0,
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
                    Body=zstar.pack_file("data", b"B" * len(payload)),
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
    import boto3
    import fsspec.core

    endpoint = os.environ["ZSTAR_S3_ENDPOINT"]
    parsed = urlsplit(endpoint)
    assert parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="zstar-test",
        aws_secret_access_key="zstar-test-secret",
    )
    bucket = "zstar-version-" + uuid.uuid4().hex
    client.create_bucket(Bucket=bucket)
    versions = []
    try:
        client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        for body in [b"old", b"new"]:
            response = client.put_object(
                Bucket=bucket, Key="archive.zstar", Body=zstar.pack_file("x", body)
            )
            versions.append(response["VersionId"])

        async def exercise():
            fs, member = fsspec.core.url_to_fs(
                f"zstar://x::s3://{bucket}/archive.zstar?versionId={versions[0]}",
                zstar={"asynchronous": True},
                s3={
                    "asynchronous": backend_async,
                    "key": "zstar-test",
                    "secret": "zstar-test-secret",
                    "version_aware": True,
                    "client_kwargs": {"endpoint_url": endpoint, "region_name": "us-east-1"},
                },
            )
            try:
                assert await fs._cat_file(member) == b"old"
            finally:
                await fs.aclose()

        asyncio.run(exercise())
    finally:
        for version in versions:
            client.delete_object(Bucket=bucket, Key="archive.zstar", VersionId=version)
        client.delete_bucket(Bucket=bucket)


@pytest.mark.skipif(not os.getenv("ZSTAR_S3_ENDPOINT"), reason="requires local RustFS")
def test_async_packed_s3_generation():
    import hashlib

    boto3 = pytest.importorskip("boto3")
    endpoint = os.environ["ZSTAR_S3_ENDPOINT"]
    parsed = urlsplit(endpoint)
    assert parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    config = dict(
        endpoint=endpoint,
        region="us-east-1",
        access_key_id="zstar-test",
        secret_access_key="zstar-test-secret",
    )
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="zstar-test",
        aws_secret_access_key="zstar-test-secret",
    )
    bucket = "zstar-packed-python-" + uuid.uuid4().hex
    client.create_bucket(Bucket=bucket)

    async def run():
        store = zstar.PackStore.s3(bucket, part_size=5 << 20, concurrency=2, **config)
        data = b"".join(
            hashlib.sha256(i.to_bytes(8, "little")).digest() for i in range((8 << 20) // 32)
        )
        async with zstar.PackWriter(store) as packs:
            body = await packs.externalize_async(io.BytesIO(data), len(data))
            catalog = await packs.finish_async()
        thin = io.BytesIO()
        info = zstar.TarInfo("file")
        info.size = len(data)
        with zstar.ArchiveWriter(thin) as writer:
            writer.add_external(info, body)
        _key, generation = await zstar.publish_packed_generation_async(
            store, io.BytesIO(thin.getvalue()), catalog
        )
        resolver = zstar.PackedObjectResolver(store, catalog)
        async with await zstar.AsyncArchive.from_s3(
            bucket, generation.archive_key, resolver=resolver, **config
        ) as archive:
            assert await archive.read("file") == data

    try:
        asyncio.run(run())
    finally:
        for item in client.list_objects_v2(Bucket=bucket).get("Contents", []):
            client.delete_object(Bucket=bucket, Key=item["Key"])
        client.delete_bucket(Bucket=bucket)


@pytest.mark.skipif(not os.getenv("ZSTAR_S3_ENDPOINT"), reason="requires local RustFS")
def test_native_s3_fsspec_url_uses_ranges_without_s3fs(monkeypatch, tmp_path):
    import http.client
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    import boto3
    import fsspec

    endpoint = os.environ["ZSTAR_S3_ENDPOINT"]
    upstream = urlsplit(endpoint)
    assert upstream.scheme == "http" and upstream.hostname in {"127.0.0.1", "localhost", "::1"}
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="zstar-test",
        aws_secret_access_key="zstar-test-secret",
    )
    bucket = "zstar-native-url-" + uuid.uuid4().hex
    key = "nested/archive v1.zstar"
    payload = os.urandom(5 << 20)
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested/data").write_bytes(payload)
    encoded = bytes(zstar.pack_directory(tmp_path))
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
        monkeypatch.setitem(sys.modules, "s3fs", None)
        with fsspec.open(
            f"zstar://nested/data::zstar+s3://{bucket}/nested/archive%20v1.zstar",
            **{
                "zstar+s3": {
                    "endpoint": f"http://127.0.0.1:{server.server_port}",
                    "region": "us-east-1",
                    "access_key_id": "zstar-test",
                    "secret_access_key": "zstar-test-secret",
                }
            },
        ) as member:
            assert member.read(10) == payload[:10]
            member.seek(-10, 2)
            assert member.read(10) == payload[-10:]
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
