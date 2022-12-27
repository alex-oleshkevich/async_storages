import io
import os
import pytest

from async_storages import S3Storage
from async_storages.storages import AdaptedBytesIO

pytestmark = [pytest.mark.asyncio]

AWS_ACCESS_KEY_ID = "minioadmin"
AWS_SECRET_ACCESS_KEY = "minioadmin"
AWS_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9000")


@pytest.fixture()
def storage() -> S3Storage:
    return S3Storage(
        bucket="asyncstorages",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=AWS_ENDPOINT_URL,
    )


async def test_operations(storage: S3Storage) -> None:
    path = "asyncstorages/test.txt"
    await storage.write(path, AdaptedBytesIO(io.BytesIO(b"content")))
    assert await storage.exists(path)

    file = await storage.read(path, 10)
    assert await file.read(10) == b"content"
    assert storage.abspath(path) == "asyncstorages/test.txt"
    assert "/asyncstorages/test.txt" in await storage.url(path)
    await storage.delete(path)
    assert not await storage.exists(path)


async def test_s3_raises_file_error_for_missing_key(storage: S3Storage) -> None:
    with pytest.raises(FileNotFoundError):
        await storage.read("missing-file.txt", 1)
