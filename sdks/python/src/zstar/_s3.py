"""Optional S3 discovery metadata for completed encoded archives."""

from typing_extensions import Buffer

from . import _native


def s3_archive_metadata(data: Buffer) -> dict[str, str]:
    """Return the three S3 user metadata keys by inspecting only the footer.

    Pass the result as an S3 SDK's Metadata mapping; HTTP PUT headers use the
    x-amz-meta- prefix. Readers still validate the actual footer and manifest.
    """
    with memoryview(data).cast("B") as view:
        return _native._s3_archive_metadata(bytes(view[-80:]), len(view))
