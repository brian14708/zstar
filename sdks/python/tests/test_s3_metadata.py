"""S3 user metadata uses footer values without decoding the archive."""

import struct

import pytest
import zstar


@pytest.mark.parametrize("wrapper", [bytes, bytearray, memoryview])
def test_archive_metadata_matches_footer(inline_data, wrapper):
    offset, encoded_size = struct.unpack_from("<QQ", inline_data, len(inline_data) - 56)
    assert zstar.s3_archive_metadata(wrapper(inline_data)) == {
        "zstar-version": "1",
        "zstar-manifest-position": f"{offset}:{encoded_size}",
        "zstar-manifest-sha256": inline_data[-32:].hex(),
    }


@pytest.mark.parametrize("data", [b"", b"not an archive", b"\0" * 80])
def test_archive_metadata_rejects_invalid_footer(data):
    with pytest.raises(zstar.ZstarError):
        zstar.s3_archive_metadata(data)


def test_archive_metadata_rejects_inconsistent_coverage(inline_data):
    corrupted = bytearray(inline_data)
    struct.pack_into("<Q", corrupted, len(corrupted) - 56, 1)
    with pytest.raises(zstar.ZstarError, match="coverage"):
        zstar.s3_archive_metadata(corrupted)
