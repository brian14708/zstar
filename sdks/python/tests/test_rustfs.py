"""Opt-in disposable RustFS test. No cloud endpoints or default credentials."""

import asyncio
import io
import os
import uuid
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import zstar


@pytest.mark.skipif(not os.getenv("ZSTAR_S3_ENDPOINT"), reason="requires local RustFS")
def test_native_s3_and_presigned_archive_and_external_objects(monkeypatch):
    boto3 = pytest.importorskip("boto3")
    endpoint = os.environ["ZSTAR_S3_ENDPOINT"]
    parsed = urlsplit(endpoint)
    assert parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "zstar-test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "zstar-test-secret")
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="zstar-test",
        aws_secret_access_key="zstar-test-secret",
    )
    bucket = "zstar-python-" + uuid.uuid4().hex
    client.create_bucket(Bucket=bucket)
    keys = []
    root = Path(__file__).resolve().parents[3] / "testdata"
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
