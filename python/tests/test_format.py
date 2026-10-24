"""Entry-oriented manifests use the same tuples in every SDK."""

import io
import os
import subprocess
import sys
from pathlib import Path

import pytest
import zstar
import zstar.format
from zstar import ZstarError
from zstar.format import (
    EntryArchiveWriter,
    ManifestArchive,
    decode_manifest_block,
    encode_manifest_block,
)


@pytest.fixture
def open_manifest(source_factory):
    return lambda data, **options: ManifestArchive(source_factory(data), len(data), **options)


def test_streaming_encoder_and_restricted_cbor(open_manifest):
    output = io.BytesIO()
    writer = EntryArchiveWriter(output, frame_size=2048, memory_bytes=1)
    for path, data in [("a", b"hello"), ("b", b"world"), ("c", bytes([7]) * 10000)]:
        writer.add((path, 0, 420, 0, 0, 0, ()), io.BytesIO(data), len(data))
    writer.close()
    writer.close()
    archive = open_manifest(output.getvalue())
    try:
        archive.validate()
        assert archive.entry("a")[6][0][1] == archive.entry("b")[6][0][1]
        assert archive.read("c") == bytes([7]) * 10000
    finally:
        archive.close()
    value = {"x-values": (2**64 - 1, -(2**63), b"bytes", True, {"text": "value"})}
    assert decode_manifest_block(encode_manifest_block(value)) == value
    with pytest.raises((ZstarError, TypeError)):
        encode_manifest_block({"x-float": 0.5})


def test_iteration_and_owned_attributes(open_manifest):
    output = io.BytesIO()
    writer = EntryArchiveWriter(output, block_target=512, memory_bytes=1)
    for i in range(100):
        writer.add((f"f{i:04}", 0, 420, 0, 0, 0, (), {"x-map": {"v": i}}))
    writer.close()
    archive = open_manifest(output.getvalue(), memory_bytes=1)
    assert archive.spilled
    iterator = archive.iter_entries()
    assert iter(iterator) is iterator
    assert next(iterator)[0] == "f0000"
    entry = archive.entry("f0020")
    entry[7]["x-map"]["v"] = -1
    assert archive.entry("f0020")[7]["x-map"]["v"] == 20
    assert [e[0] for e in iterator] == [f"f{i:04}" for i in range(1, 100)]
    assert list(iterator) == []
    archive.validate()
    pending = archive.iter_entries()
    archive.close()
    with pytest.raises(ZstarError, match="closed"):
        next(pending)
    with pytest.raises(ZstarError, match="closed"):
        archive.entry("f0000")


def test_metadata_hash_and_read_budget_are_mandatory(inline_data, open_manifest):
    data = inline_data
    with pytest.raises(ZstarError, match="integrity"):
        open_manifest(data[:-1] + bytes([data[-1] ^ 1]))
    archive = open_manifest(data, max_read_bytes=1)
    try:
        archive.validate()
        with pytest.raises(ZstarError, match="budget"):
            archive.read("a-b")
    finally:
        archive.close()


def test_indexed_open_fetches_metadata_then_payload(source_factory):
    data = zstar.pack_file("x", b"hello")

    source = source_factory(data)
    archive = zstar.format.IndexedArchive(source, len(data))
    assert source.calls == 2  # footer, TOC
    assert archive.paths() == ["x"]
    assert archive.read("x", 5, 0) == b""
    assert source.calls == 2
    assert archive.read("x", 1, 3) == b"ell"
    assert source.calls == 3


def test_source_short_reads_are_rejected():
    class ShortSource:
        def read_range(self, offset, length):
            return b""

    with pytest.raises(ValueError, match="short range response"):
        zstar.format.IndexedArchive(ShortSource(), 100)


@pytest.mark.parametrize("archive_type", [ManifestArchive, zstar.format.IndexedArchive])
def test_resolver_errors_preserve_the_original_exception(archive_type, source_factory, thin_data):
    cause = RuntimeError("private resolver details")

    class FailingResolver:
        def get_encoded(self, address, hint, max_bytes):
            raise cause

    with (
        archive_type(source_factory(thin_data), len(thin_data)) as archive,
        pytest.raises(ZstarError, match="Python encoded-object resolver failed") as failure,
    ):
        archive.read("hello.txt", resolver=FailingResolver())
    assert failure.value.category == "io"
    assert failure.value.__cause__ is cause
    assert "private resolver details" not in str(failure.value)


def test_indexed_cache_and_python_external_resolver(object_resolver, source_factory, thin_data):
    data = bytes(zstar.pack_file("x", b"hello"))

    source = source_factory(data)
    archive = zstar.format.IndexedArchive(source, len(data), cache_bytes=65536)
    assert archive.read("x", 0, 2) == b"he"
    assert archive.read("x", 2, 3) == b"llo"
    assert source.calls == 3
    assert 0 < archive.cache_bytes <= 65536

    data = thin_data
    source = source_factory(data)
    archive = zstar.format.IndexedArchive(source, len(data))
    # The corpus object merges a prefix, the member, and a suffix.
    expected = b"hello\n"

    resolver = object_resolver
    assert archive.read("hello.txt", 0, 0, resolver=resolver) == b""
    assert resolver.calls == 0
    assert archive.read("hello.txt", resolver=resolver) == expected
    assert resolver.calls == 1
    assert source.calls == 2  # no physical pointer/frame fetch needed


@pytest.mark.parametrize("verify_external", [True, False])
def test_indexed_external_policy_cannot_skip_archive_checksums(
    verify_external, corpus_dir, source_factory
):
    data = (corpus_dir / "invalid/data-checksum.zstar").read_bytes()

    archive = zstar.format.IndexedArchive(
        source_factory(data), len(data), verify_external=verify_external
    )
    archive.validate_metadata()
    with pytest.raises(zstar.ZstarError) as caught:
        archive.read("a-b")
    assert caught.value.category == "malformed-frame"
    assert archive.cache_bytes == 0
    with pytest.raises(zstar.ZstarError):
        archive.read_many([("a-b", 0, 5)])


def test_indexed_external_policy_reports_and_isolates_unchecked_resolvers(
    object_resolver, source_factory, thin_data
):
    data = thin_data

    resolver = object_resolver
    archive = zstar.format.IndexedArchive(source_factory(data), len(data), verify_external=False)
    payload, report = archive.read_with_report("hello.txt", 0, 6, resolver=resolver)
    assert payload == b"hello\n"
    assert report["metadata_authenticated"]
    assert not report["full_archive_validated"]
    assert report["external_objects_unchecked"]
    assert not report["external_objects_sha256_checked"]
    assert archive.read_many([("repeat", 0, 12), ("world.txt", 0, 6)], resolver=resolver) == [
        b"hello\nhello\n",
        b"world\n",
    ]
    assert resolver.calls == 2  # shared within the batch, not across calls
    assert archive.cache_bytes == 0

    class MissingResolver:
        def get_encoded(self, address, hint, max_bytes):
            raise FileNotFoundError(address)

    with pytest.raises(zstar.ZstarError) as caught:
        archive.read("hello.txt", 0, 6, resolver=MissingResolver())
    assert caught.value.category == "missing-external-content"


def test_indexed_batch_reads_share_frames_preserve_order_and_validate_first(source_factory):
    payload = bytes(range(251)) * 53
    data = zstar.pack_file("x", payload)

    source = source_factory(data)
    archive = zstar.format.IndexedArchive(source, len(data), cache_bytes=0)
    requests = [("x", 10, 4), ("x", 0, len(payload)), ("x", len(payload), 0), ("x", 10, 4)]
    assert archive.read_many(requests) == [payload[10:14], payload, b"", payload[10:14]]
    assert source.calls == 3  # metadata + one shared payload frame, no frame cache
    assert archive.cache_bytes == 0
    assert archive.read_many([]) == []
    with pytest.raises(zstar.ZstarError):
        archive.read_many([("x", 0, 4), ("x", len(payload), 1)])
    with pytest.raises(zstar.ZstarError) as caught:
        archive.read_many([("x", 0, 0)] * 4097)
    assert caught.value.category == "resource-limit"
    assert source.calls == 3

    class BoundedSequence:
        def __len__(self):
            return 2

        def __getitem__(self, index):
            return ("x", index, 1)

        def __iter__(self):
            raise AssertionError("batch must only index the bounded sequence")

    assert archive.read_many(BoundedSequence()) == [payload[:1], payload[1:2]]
    assert source.calls == 4


def test_indexed_open_releases_spool_and_close_preserves_borrowed_source(source_factory):
    data = zstar.pack_file("x", b"hello")

    def temporary_descriptors():
        directory = Path("/proc/self/fd")
        if not directory.exists():
            return set()
        result = set()
        for path in list(directory.iterdir()):
            try:
                target = os.readlink(path)
            except FileNotFoundError:
                continue
            if "(deleted)" in target:
                result.add((path.name, target))
        return result

    source = source_factory(data)
    before = temporary_descriptors()
    with zstar.format.IndexedArchive(source, len(data), memory_bytes=1) as archive:
        assert archive.read("x") == b"hello"
        assert not archive.closed
        assert archive.cache_bytes > 0
        retained = temporary_descriptors() - before
        if Path("/proc/self/fd").exists():
            assert not retained, "decoded spool is released after building the entry index"
    assert archive.closed and archive.cache_bytes == 0
    assert not source.closed
    assert not retained.intersection(temporary_descriptors())
    archive.close()
    for operation in [
        archive.paths,
        archive.validate_metadata,
        lambda: archive.read("x"),
        lambda: archive.read_with_report("x", 0, 1),
        lambda: archive.read_many([("x", 0, 1)]),
        lambda: archive._entry("x"),
        archive.__enter__,
    ]:
        with pytest.raises(zstar.ZstarError, match="closed") as caught:
            operation()
        assert caught.value.category == "unsupported-operation"


def test_indexed_close_preserves_admitted_read_and_rejects_new_calls():
    import threading
    from concurrent.futures import ThreadPoolExecutor

    data = zstar.pack_file("x", b"hello")
    started, release = threading.Event(), threading.Event()

    class Source:
        def read_range(self, offset, length):
            if offset == 0:
                started.set()
                assert release.wait(5), "read was not released"
            return data[offset : offset + length]

    archive = zstar.format.IndexedArchive(Source(), len(data), memory_bytes=1, cache_bytes=0)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(archive.read, "x")
        try:
            assert started.wait(5), "payload callback did not start"
            archive.close()
            assert archive.closed
            with pytest.raises(zstar.ZstarError, match="closed"):
                archive.read("x")
        finally:
            release.set()
        assert future.result(timeout=5) == b"hello"


@pytest.mark.parametrize(
    "operation", ["close", "closed", "entry_count", "cache_bytes", "getmembers"]
)
def test_bytes_reader_locking_releases_gil_while_waiting_for_a_resolver(operation, corpus_dir):
    script = """
import threading
from pathlib import Path
import sys
import zstar
import zstar.format
root = Path(sys.argv[1])
archive = zstar.Archive((root / 'thin.zstar').read_bytes())
started, release = threading.Event(), threading.Event()
outputs = []
class Resolver:
    def get_encoded(self, address, hint, maximum):
        started.set()
        assert release.wait(5)
        return (root / 'objects' / (address[7:] + '.zst')).read_bytes()
def read():
    outputs.append(archive.read('hello.txt', resolver=Resolver()))
worker = threading.Thread(target=read)
worker.start()
assert started.wait(5)
timer = threading.Timer(0.05, release.set)
timer.start()
operation = getattr(archive, sys.argv[2])
if callable(operation):
    result = operation()
    if sys.argv[2] == 'getmembers':
        next(result)
archive.close()
worker.join(5)
timer.join(5)
assert not worker.is_alive() and outputs == [b'hello\\n']
try:
    archive.read('hello.txt')
except ValueError:
    pass
else:
    raise AssertionError('closed reader accepted a read')
print('closed without GIL deadlock')
"""
    root = corpus_dir
    result = subprocess.run(
        [sys.executable, "-c", script, str(root), operation],
        timeout=10,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "closed without GIL deadlock"
