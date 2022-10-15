import io
import os
import pathlib

import pytest

from async_storages.storages import is_rolled, LocalStorage, MemoryStorage, S3Storage, Store

AWS_ACCESS_KEY_ID = "minioadmin"
AWS_SECRET_ACCESS_KEY = "minioadmin"
AWS_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9000")

stores = [
    Store(
        S3Storage(
            bucket="asyncstorages",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=AWS_ENDPOINT_URL,
        )
    ),
    Store(LocalStorage(base_dir="/tmp/async_storages", mkdirs=True)),
    Store(MemoryStorage()),
]

pytestmark = [pytest.mark.asyncio]


@pytest.mark.parametrize("store", stores)
async def test_operations(store: Store) -> None:
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


@pytest.mark.parametrize("store", stores)
async def test_writes_bytes_io(store: Store) -> None:
    path = "asyncstorages/test.txt"
    content = io.BytesIO(b"content")
    await store.write(path, content)
    assert await store.exists(path)
    await store.delete(path)
    assert not await store.exists(path)


@pytest.mark.parametrize("store", stores)
async def test_writes_open_file(store: Store, tmp_path: pathlib.Path) -> None:
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
async def test_generates_url(store: Store) -> None:
    path = "asyncstorages/test.txt"
    await store.write(path, b"content")
    url = await store.url(path)
    assert "/asyncstorages/test.txt" in url
    assert await store.exists(path)
    await store.delete(path)
    assert not await store.exists(path)


async def test_memory_store_with_large_file() -> None:
    storage = MemoryStorage(spool_max_size=2)
    store = Store(storage)
    path = "asyncstorages/test.txt"
    await store.write(path, b"aa")
    assert not is_rolled(storage.fs[path])
    await store.write(path, b"aaa")
    assert is_rolled(storage.fs[path])
