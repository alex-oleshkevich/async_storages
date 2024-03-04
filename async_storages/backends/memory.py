import tempfile

import anyio.to_thread

from async_storages.backends.base import (
    AdaptedBytesIO,
    AsyncFileLike,
    AsyncReader,
    BaseBackend,
    is_rolled,
)


class MemoryBackend(BaseBackend):
    def __init__(self, spool_max_size: int = 1024**2) -> None:
        self.spool_max_size = spool_max_size
        self.fs: dict[str, tempfile.SpooledTemporaryFile[bytes]] = {}

    async def write(self, path: str, data: AsyncReader) -> None:
        self.fs[path] = tempfile.SpooledTemporaryFile(max_size=self.spool_max_size)
        while chunk := await data.read(1024 * 16):
            if is_rolled(self.fs[path]):
                await anyio.to_thread.run_sync(self.fs[path].write, chunk)
            else:
                self.fs[path].write(chunk)

    async def read(self, path: str, chunk_size: int) -> AsyncFileLike:
        if path not in self.fs:
            raise FileNotFoundError(f"No such file in memory store: {path}")
        stored_file = self.fs[path]
        await anyio.to_thread.run_sync(stored_file.seek, 0)
        return AdaptedBytesIO(stored_file)

    async def delete(self, path: str) -> None:
        if path in self.fs:
            del self.fs[path]

    async def exists(self, path: str) -> bool:
        return path in self.fs

    async def url(self, path: str) -> str:
        return "/" + path  # not possible to generate URL for memory-based files

    def abspath(self, path: str) -> str:
        return path
