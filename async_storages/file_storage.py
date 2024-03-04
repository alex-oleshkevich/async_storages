import inspect
import io
import os
import typing

from async_storages.backends.base import (
    AdaptedBytesIO,
    AsyncFileLike,
    AsyncReader,
    BaseBackend,
)


class FileStorage:
    def __init__(self, storage: BaseBackend) -> None:
        self.storage = storage

    async def write(
        self,
        path: str | os.PathLike[typing.AnyStr],
        data: bytes | AsyncReader | typing.BinaryIO,
    ) -> None:
        if isinstance(data, bytes):
            data = io.BytesIO(data)

        if not inspect.iscoroutinefunction(data.read):
            data = AdaptedBytesIO(typing.cast(typing.BinaryIO, data))

        await self.storage.write(str(path), typing.cast(AsyncReader, data))

    async def open(self, path: str | os.PathLike[typing.AnyStr]) -> AsyncFileLike:
        return await self.storage.read(str(path), 1)

    async def exists(self, path: str | os.PathLike[typing.AnyStr]) -> bool:
        return await self.storage.exists(str(path))

    async def delete(self, path: str | os.PathLike[typing.AnyStr]) -> None:
        await self.storage.delete(str(path))

    async def url(self, path: str | os.PathLike[typing.AnyStr]) -> str:
        return await self.storage.url(str(path))

    def abspath(self, path: str) -> str:
        return self.storage.abspath(path)

    async def iterator(self, path: str, chunk_size: int = 1024 * 64) -> typing.AsyncIterable[bytes]:
        return await self.storage.read(path, chunk_size)
