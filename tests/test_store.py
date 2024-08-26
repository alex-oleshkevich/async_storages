import io
import pathlib

import pytest

from async_storages.backends.base import AdaptedBytesIO, is_rolled
from async_storages.backends.fs import FileSystemBackend
from async_storages.backends.memory import MemoryBackend
from async_storages.backends.s3 import S3Backend
from async_storages.file_storage import FileStorage
from tests.conftest import AWS_ACCESS_KEY_ID, AWS_ENDPOINT_URL, AWS_SECRET_ACCESS_KEY

stores = [
    FileStorage(
        S3Backend(
            bucket="asyncstorages",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=AWS_ENDPOINT_URL,
        )
    ),
    FileStorage(FileSystemBackend(base_dir="/tmp/async_storages", mkdirs=True)),
    FileStorage(MemoryBackend()),
    FileStorage(MemoryBackend(spool_max_size=1)),
]

pytestmark = [pytest.mark.asyncio]


@pytest.mark.parametrize("store", stores)
async def test_operations(store: FileStorage) -> None:
    path = "asyncstorages/test.txt"
    content = b"content"
    await store.write(path, content)
    assert await store.exists(path)
    file = await store.open(path)
    chunk = await file.read(3)
    assert chunk == b"con"
    chunk = await file.read()
    assert chunk == b"tent"
    await store.delete(path)
    assert not await store.exists(path)

    assert "asyncstorages/test.txt" in store.abspath(path)


@pytest.mark.parametrize("store", stores)
async def test_writes_bytes(store: FileStorage) -> None:
    path = "asyncstorages/test.txt"
    await store.write(path, b"content")
    assert await store.exists(path)
    async with await store.open(path) as file:
        assert await file.read() == b"content"

    await store.delete(path)
    assert not await store.exists(path)


@pytest.mark.parametrize("store", stores)
async def test_writes_bytes_io(store: FileStorage) -> None:
    path = "asyncstorages/test.txt"
    content = io.BytesIO(b"content")
    await store.write(path, content)
    assert await store.exists(path)
    async with await store.open(path) as file:
        assert await file.read() == b"content"

    await store.delete(path)
    assert not await store.exists(path)


@pytest.mark.parametrize("store", stores)
async def test_writes_open_file(store: FileStorage, tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "test.txt"
    with open(file_path, "wb") as f:
        f.write(b"content")
    with open(file_path, "rb") as content:
        path = "asyncstorages/test.txt"
        await store.write(path, content)
    assert await store.exists(path)
    await store.delete(path)
    assert not await store.exists(path)


@pytest.mark.parametrize("store", stores)
async def test_writes_async_reader(store: FileStorage, tmp_path: pathlib.Path) -> None:
    reader = AdaptedBytesIO(io.BytesIO(b"content"))

    path = "asyncstorages/test.txt"
    await store.write(path, reader)
    assert await store.exists(path)
    async with await store.open(path) as file:
        assert await file.read() == b"content"

    await store.delete(path)
    assert not await store.exists(path)


@pytest.mark.parametrize("store", stores)
async def test_generates_url(store: FileStorage) -> None:
    path = "asyncstorages/test.txt"
    await store.write(path, b"content")
    url = await store.url(path)
    assert "/asyncstorages/test.txt" in url
    assert await store.exists(path)
    await store.delete(path)
    assert not await store.exists(path)


async def test_memory_store_with_large_file() -> None:
    storage = MemoryBackend(spool_max_size=2)
    store = FileStorage(storage)
    path = "asyncstorages/test.txt"
    await store.write(path, b"a" * 1024 * 20)
    async with await store.open(path) as file:
        assert await file.read()

    assert is_rolled(storage.fs[path])


@pytest.mark.parametrize("store", stores)
async def test_store_iterator(store: FileStorage) -> None:
    path = "asyncstorages/test.txt"
    content = b"content"
    await store.write(path, content)

    iterator = await store.iterator(path)
    read_content = b""
    async for chunk in iterator:
        read_content += chunk

    assert read_content == b"content"
