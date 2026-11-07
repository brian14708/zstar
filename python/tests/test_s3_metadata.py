"""S3 user metadata uses footer values without decoding the archive."""

import struct

import pytest
import zstar
from zstar._native import s3_archive_metadata


@pytest.mark.parametrize("wrapper", [bytes, bytearray, memoryview])
def test_archive_metadata_matches_footer(inline_data, wrapper):
    encoded_size = struct.unpack_from("<Q", inline_data, len(inline_data) - 20)[0]
    offset = len(inline_data) - 28 - encoded_size
    assert s3_archive_metadata(wrapper(inline_data)) == {
        "zstar-version": "1",
        "zstar-manifest-position": f"{offset}:{encoded_size}",
    }


@pytest.mark.parametrize("data", [b"", b"\0" * 28])
def test_archive_metadata_rejects_invalid_footer(data):
    with pytest.raises(zstar.ZstarError):
        s3_archive_metadata(data)


def test_archive_metadata_rejects_inconsistent_coverage(inline_data):
    corrupted = bytearray(inline_data)
    struct.pack_into("<Q", corrupted, len(corrupted) - 20, len(corrupted))
    with pytest.raises(zstar.ZstarError, match="coverage"):
        s3_archive_metadata(corrupted)
