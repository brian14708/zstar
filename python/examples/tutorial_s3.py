# /// script
# requires-python = ">=3.11"
# dependencies = ["zstar-py>=0.0.1", "boto3"]
# ///

"""Create an archive, upload it to S3, read ranges, and delete it."""

import os
from io import BytesIO
from uuid import uuid4

import boto3
from botocore.config import Config
from zstar import Archive, ArchiveWriter


def main() -> None:
    # region configure
    bucket = os.environ["ZSTAR_S3_BUCKET"]
    region = os.environ["AWS_REGION"]
    endpoint = os.environ.get("ZSTAR_S3_ENDPOINT")
    credentials = {
        "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "aws_session_token": os.environ.get("AWS_SESSION_TOKEN"),
    }
    client = boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        config=Config(s3={"addressing_style": "path"}) if endpoint else None,
        **credentials,
    )
    # endregion configure
    try:
        # region upload
        with BytesIO() as output:
            with ArchiveWriter(output) as writer:
                writer.write_bytes("example.txt", b"Hello from Zstar\n")
            encoded = output.getvalue()
        key = f"zstar-tutorial/python-{uuid4().hex}.zstar"
        client.put_object(Bucket=bucket, Key=key, Body=encoded, IfNoneMatch="*")
        # endregion upload
        try:
            # region read
            with Archive.from_s3(
                bucket,
                key,
                region=region,
                endpoint=endpoint,
                access_key_id=credentials["aws_access_key_id"],
                secret_access_key=credentials["aws_secret_access_key"],
                session_token=credentials["aws_session_token"],
            ) as archive:
                content = archive.read("example.txt")
                selected = archive.read_range("example.txt", 0, 5)
                assert content == b"Hello from Zstar\n"
                assert selected == b"Hello"
                print(content.decode(), end="")
                print(selected.decode())
            # endregion read
        finally:
            # region cleanup
            client.delete_object(Bucket=bucket, Key=key)
            print("Deleted S3 object")
            # endregion cleanup
    finally:
        client.close()


if __name__ == "__main__":
    main()
