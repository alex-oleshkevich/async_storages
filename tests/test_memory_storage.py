import io
import pathlib
import pytest

from async_storages import MemoryStorage
from async_storages.storages import AdaptedBytesIO

pytestmark = [pytest.mark.asyncio]


@pytest.fixture()
def storage() -> MemoryStorage:
    return MemoryStorage()


async def test_operations(storage: MemoryStorage) -> None:
    path = "asyncstorages/test.txt"
    await storage.write(path, AdaptedBytesIO(io.BytesIO(b"content")))
    assert await storage.exists(path)

    file = await storage.read(path, 10)
    assert await file.read(10) == b"content"
    assert storage.abspath(path) == "asyncstorages/test.txt"
    assert await storage.url(path) == "/asyncstorages/test.txt"
    await storage.delete(path)
    assert not await storage.exists(path)


async def test_memory_storage_raises_exception_for_missing_file(tmp_path: pathlib.Path) -> None:
    with pytest.raises(FileNotFoundError):
        storage = MemoryStorage()
        await storage.read("test.txt", 1)
