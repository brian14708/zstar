# /// script
# requires-python = ">=3.11"
# dependencies = ["zstar-py>=0.0.1", "boto3"]
# ///

"""Create an archive, upload it to S3, read ranges, and delete it."""

import os
from uuid import uuid4

import boto3
import zstar
from botocore.config import Config
from zstar import Credentials, S3Options, SinkOptions, SourceOptions


def main() -> None:
    # region configure
    bucket = os.environ["ZSTAR_S3_BUCKET"]
    region = os.environ["AWS_REGION"]
    endpoint = os.environ.get("ZSTAR_S3_ENDPOINT")
    access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    session_token = os.environ.get("AWS_SESSION_TOKEN")
    credentials = {
        "aws_access_key_id": access_key_id,
        "aws_secret_access_key": secret_access_key,
        "aws_session_token": session_token,
    }
    client = boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        config=Config(s3={"addressing_style": "path"}) if endpoint else None,
        **credentials,
    )
    s3 = S3Options(
        region=region,
        endpoint=endpoint,
        credentials=Credentials(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
        ),
    )
    # endregion configure
    try:
        # region upload
        key = f"zstar-tutorial/python-{uuid4().hex}.zstar"
        url = f"s3://{bucket}/{key}"
        with zstar.open(url, storage_options=SinkOptions(s3=s3), mode="w") as writer:
            writer.writestr("example.txt", b"Hello from Zstar\n")
        # endregion upload
        try:
            # region read
            with zstar.open(url, storage_options=SourceOptions(s3=s3)) as archive:
                content = archive.read("example.txt")
                selected = archive.read("example.txt", offset=0, length=5)
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
