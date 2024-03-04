import io
import tempfile

import pytest

from async_storages.backends.base import AdaptedBytesIO

pytestmark = [pytest.mark.asyncio]


async def test_read_method_using_bytes() -> None:
    reader = AdaptedBytesIO(io.BytesIO(b"content"))
    assert await reader.read() == b"content"


async def test_read_method_using_spooled_file() -> None:
    file = tempfile.SpooledTemporaryFile()
    file.write(b"content")
    file.seek(0)

    reader = AdaptedBytesIO(file)
    assert await reader.read() == b"content"


async def test_adapter_iterates_bytes() -> None:
    reader = AdaptedBytesIO(io.BytesIO(b"cont\nent"))
    read_bytes = b""
    async for chunk in reader:
        read_bytes += chunk

    assert read_bytes == b"cont\nent"


async def test_adapter_iterates_spooled_file() -> None:
    file = tempfile.SpooledTemporaryFile()
    file.write(b"cont\nent")
    file.seek(0)

    reader = AdaptedBytesIO(file)
    read_bytes = b""
    async for chunk in reader:
        read_bytes += chunk

    assert read_bytes == b"cont\nent"


async def test_adapter_iterates_dumped_spooled_file() -> None:
    file = tempfile.SpooledTemporaryFile(max_size=1)
    file.write(b"cont\nent")
    file.seek(0)

    reader = AdaptedBytesIO(file)
    read_bytes = b""
    async for chunk in reader:
        read_bytes += chunk

    assert read_bytes == b"cont\nent"
