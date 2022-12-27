import io
import pathlib
import pytest

from async_storages import LocalStorage
from async_storages.storages import AdaptedBytesIO

pytestmark = [pytest.mark.asyncio]


@pytest.fixture()
def storage(tmp_path: pathlib.Path) -> LocalStorage:
    return LocalStorage(tmp_path, mkdirs=True, base_url="http://example")


async def test_operations(storage: LocalStorage) -> None:
    path = "asyncstorages/test.txt"
    await storage.write(path, AdaptedBytesIO(io.BytesIO(b"content")))
    assert await storage.exists(path)

    file = await storage.read(path, 10)
    assert await file.read(10) == b"content"
    assert storage.abspath(path) == f"{storage.base_dir}/asyncstorages/test.txt"
    assert await storage.url(path) == "http://example/asyncstorages/test.txt"
    await storage.delete(path)
    assert not await storage.exists(path)


def test_local_storage_abspath(tmp_path: pathlib.Path) -> None:
    storage = LocalStorage(base_dir=tmp_path)
    assert storage.abspath("test.txt") == str(tmp_path / "test.txt")


async def test_local_storage_makes_dirs(tmp_path: pathlib.Path) -> None:
    storage = LocalStorage(base_dir=tmp_path, mkdirs=True)
    await storage.write("sample/test.txt", AdaptedBytesIO(io.BytesIO(b"")))

    assert (tmp_path / "sample/test.txt").exists()

    await storage.write("sample/test2.txt", AdaptedBytesIO(io.BytesIO(b"")))
    assert (tmp_path / "sample/test2.txt").exists()
