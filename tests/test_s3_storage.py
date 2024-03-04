import io

import pytest

from async_storages.backends.base import AdaptedBytesIO
from async_storages.backends.s3 import S3Backend

pytestmark = [pytest.mark.asyncio]


async def test_operations(storage: S3Backend) -> None:
    path = "asyncstorages/test.txt"
    await storage.write(path, AdaptedBytesIO(io.BytesIO(b"content")))
    assert await storage.exists(path)

    file = await storage.read(path, 10)
    assert await file.read(10) == b"content"
    assert storage.abspath(path) == "asyncstorages/test.txt"
    assert "/asyncstorages/test.txt" in await storage.url(path)
    await storage.delete(path)
    assert not await storage.exists(path)


async def test_s3_raises_file_error_for_missing_key(storage: S3Backend) -> None:
    with pytest.raises(FileNotFoundError):
        await storage.read("missing-file.txt", 1)
