import os

import aioboto3
import pytest

from async_storages import S3Backend

AWS_ACCESS_KEY_ID = "minioadmin"
AWS_SECRET_ACCESS_KEY = "minioadmin"
AWS_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9000")


@pytest.fixture()
async def storage() -> S3Backend:
    session = aioboto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    async with session.client("s3", endpoint_url=AWS_ENDPOINT_URL) as s3:
        try:
            await s3.create_bucket(ACL="public-read", Bucket="asyncstorages")
        except Exception as ex:
            if "BucketAlreadyOwnedByYou" in str(ex):
                pass

        return S3Backend(
            bucket="asyncstorages",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=AWS_ENDPOINT_URL,
        )
