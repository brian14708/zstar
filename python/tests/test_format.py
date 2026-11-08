"""Lazy typed readers preserve format validation, batching, and close semantics."""

import pytest
import zstar
from _support import pack_file
from zstar import ReadRequest, ZstarError, _native
from zstar.types import ReaderOptions


def test_indexed_open_fetches_directory_then_selected_page_and_payload(source_factory):
    source = source_factory(pack_file("data", b"hello"))
    with zstar.open(source) as archive:
        assert source.calls == 2
        assert archive.entry("data").size == 5
        assert source.calls == 3
        assert archive.read("data", offset=5, length=0) == b""
        assert source.calls == 3
        assert archive.read("data", offset=1, length=3) == b"ell"
        assert source.calls == 4


def test_source_short_reads_rejected_as_io_with_cause():
    class Short:
        def size(self):
            return 100

        def read_at(self, offset, length):
            return b""

    with pytest.raises(ZstarError) as caught:
        zstar.open(Short())
    assert caught.value.category == "io"
    assert isinstance(caught.value.__cause__, ValueError)


def test_resolver_failure_preserves_original_exception(thin_data):
    cause = RuntimeError("private resolver details")

    class Resolver:
        def get_encoded(self, address, hint, maximum):
            raise cause

    options = ReaderOptions(resolver=Resolver())
    with (
        zstar.open(_native.Source.memory(thin_data), options=options) as archive,
        pytest.raises(ZstarError) as caught,
    ):
        archive.read("hello.txt")
    assert caught.value.category == "io" and caught.value.__cause__ is cause
    assert "private resolver details" not in str(caught.value)


def test_batch_shares_frames_preserves_order_and_validates_before_content(source_factory):
    payload = bytes(range(251)) * 53
    source = source_factory(pack_file("data", payload))
    with zstar.open(source) as archive:
        requests = [
            ReadRequest("data", 10, 4),
            ReadRequest("data", 0, len(payload)),
            ReadRequest("data", len(payload), 0),
            ReadRequest("data", 10, 4),
        ]
        assert archive.read_many(requests) == [payload[10:14], payload, b"", payload[10:14]]
        assert source.calls == 4
        assert archive.read_many([]) == []
        with pytest.raises(ZstarError):
            archive.read_many([ReadRequest("data", 0, 4), ReadRequest("data", len(payload), 1)])
        with pytest.raises(ZstarError) as caught:
            archive.read_many([ReadRequest("data", 0, 0)] * 4097)
        assert caught.value.category == "resource-limit"
        assert source.calls == 4

        class Bounded:
            def __len__(self):
                return 2

            def __getitem__(self, index):
                return ReadRequest("data", index, 1)

            def __iter__(self):
                raise AssertionError("must index the bounded sequence")

        assert archive.read_many(Bounded()) == [payload[:1], payload[1:2]]


@pytest.mark.parametrize("verify", [False, True])
def test_mandatory_archive_checksums_ignore_external_hash_policy(verify, corpus_dir):
    with (
        _native.open_source(str(corpus_dir / "invalid/data-checksum.zstar")) as source,
        zstar.open(source, options=ReaderOptions(verify_external=verify)) as archive,
    ):
        with pytest.raises(ZstarError) as caught:
            archive.read("a-b")
        assert caught.value.category == "integrity-failure"
