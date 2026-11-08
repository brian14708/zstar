"""Fixture construction using the typed public archive API.

Option translation keeps corpus budgets in their shared, language-neutral schema.
Factories own native sources through the archive's retained reference; borrowed
file-object adapters preserve ownership. Product API contracts are checked directly
in test_core_api.py and typing_core.py.
"""

import inspect
import io
from pathlib import Path

import zstar
from zstar import (
    AsyncReaderOptions,
    AsyncSourceOptions,
    Credentials,
    Limits,
    MetadataReaderOptions,
    ReaderOptions,
    S3Options,
    SourceOptions,
    _native,
)
from zstar._fileobj import fileobj


class Source:
    def __init__(self, data):
        self.data = data
        self.sizes = 0
        self.ranges = []

    def size(self):
        self.sizes += 1
        return len(self.data)

    def read_at(self, offset, length):
        self.ranges.append((offset, length))
        return self.data[offset : offset + length]


class AsyncSource(Source):
    # Ordinary functions returning awaitables also satisfy the callback contract.
    def size(self):
        async def result():
            return super(AsyncSource, self).size()

        return result()

    def read_at(self, offset, length):
        async def result():
            return super(AsyncSource, self).read_at(offset, length)

        return result()


async def maybe(value):
    return await value if inspect.isawaitable(value) else value


async def reader_for(mode, source, options=None):
    return await maybe(zstar.open(source, options=options, async_=mode == "async"))


def pack_file(path, data):
    output = io.BytesIO()
    with zstar.open(output, mode="w") as writer:
        writer.writestr(path, data)
    return output.getvalue()


def pack_directory(path):
    output = io.BytesIO()
    with zstar.open(output, mode="w") as writer:
        for child in sorted(Path(path).iterdir()):
            writer.add(child, child.name)
    return output.getvalue()


def reader_options(options, cls=ReaderOptions):
    # Corpus fixtures use the shared language-neutral budget names.
    values = dict(options)
    limits = Limits(**(values.pop("limits", None) or {}))
    if "memory_bytes" in values:
        values.setdefault("max_page_bytes", values["memory_bytes"])
    for key, field in {
        "memory_bytes": "max_retained_entry_bytes",
        "max_decoded_bytes": "max_page_bytes",
    }.items():
        if key in values:
            values[field] = values.pop(key)
    metadata = MetadataReaderOptions(
        **{key: values.pop(key) for key in MetadataReaderOptions.__annotations__ if key in values},
    )
    return cls(limits=limits, metadata=metadata, **values)


def open_bytes(data, **options):
    return zstar.open(_native.Source.memory(data), options=reader_options(options))


def open_file(file, **options):
    source = _native.open_source(str(file)) if isinstance(file, (str, Path)) else fileobj(file)
    return zstar.open(source, options=reader_options(options))


class SizedSource:
    def __init__(self, source, size):
        self.source, self._size = source, size

    def size(self):
        return self._size

    def read_at(self, offset, length):
        return self.source.read_at(offset, length)


def open_source(source, size, **options):
    return zstar.open(SizedSource(source, size), options=reader_options(options))


async def aopen_bytes(data, **options):
    return await zstar.open(
        await _native.AsyncSource.memory(data),
        options=reader_options(options, AsyncReaderOptions),
        async_=True,
    )


async def aopen_source(source, size, *, blocking=True, **options):
    if blocking:
        source = _native.BlockingAdapter(SizedSource(source, size))
    return await zstar.open(
        source, options=reader_options(options, AsyncReaderOptions), async_=True
    )


def open_url(url, *, headers=None, **options):
    source = _native.open_source(url, options=SourceOptions(headers=headers or {}))
    return zstar.open(source, options=reader_options(options))


async def aopen_url(url=None, *, headers=None, provider=None, **options):
    source = await _native.aopen_source(
        url or "https://signed.invalid/archive",
        options=AsyncSourceOptions(headers=headers or {}, request_provider=provider),
    )
    return await zstar.open(
        source, options=reader_options(options, AsyncReaderOptions), async_=True
    )


class S3Objects:
    def __init__(self, bucket, prefix, options):
        self.bucket, self.prefix, self.options = bucket, prefix, options

    def get_encoded(self, address, hint, max_bytes):
        with _native.open_source(
            f"s3://{self.bucket}/{self.prefix}/{address[7:]}.zst", options=self.options
        ) as source:
            size = source.size()
            if size > max_bytes:
                raise ValueError("encoded object exceeds budget")
            return source.read_at(0, size)


# Disposable RustFS credentials used by the opt-in integration tests.
S3_REGION = "us-east-1"
S3_ACCESS_KEY_ID = "zstar-test"
S3_SECRET_ACCESS_KEY = "zstar-test-secret"


def s3_options(endpoint):
    return S3Options(
        region=S3_REGION,
        endpoint=endpoint,
        credentials=Credentials(
            access_key_id=S3_ACCESS_KEY_ID, secret_access_key=S3_SECRET_ACCESS_KEY
        ),
    )


def s3_client(endpoint, **kwargs):
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        **kwargs,
    )


def s3fs_options(endpoint, **extra):
    return {
        "key": S3_ACCESS_KEY_ID,
        "secret": S3_SECRET_ACCESS_KEY,
        "client_kwargs": {"endpoint_url": endpoint, "region_name": S3_REGION},
        **extra,
    }


def open_s3(bucket, key, *, s3, object_prefix=None, **options):
    transport = SourceOptions(s3=s3)
    if object_prefix:
        options["resolver"] = S3Objects(bucket, object_prefix, transport)
    return zstar.open(
        _native.open_source(f"s3://{bucket}/{key}", options=transport),
        options=reader_options(options),
    )


async def aopen_s3(bucket, key, *, s3, object_prefix=None, **options):
    if object_prefix:
        resolver = S3Objects(bucket, object_prefix, SourceOptions(s3=s3))
        options["resolver"] = _native.BlockingAdapter(resolver)
    source = await _native.aopen_source(f"s3://{bucket}/{key}", options=AsyncSourceOptions(s3=s3))
    return await zstar.open(
        source, options=reader_options(options, AsyncReaderOptions), async_=True
    )
