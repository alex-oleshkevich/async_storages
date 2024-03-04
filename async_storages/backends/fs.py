import os
import pathlib
import typing

import anyio.to_thread

from async_storages.backends.base import AsyncFileLike, AsyncReader, BaseBackend


class FileSystemBackend(BaseBackend):
    def __init__(
        self,
        base_dir: str | os.PathLike[typing.AnyStr],
        mkdirs: bool = False,
        base_url: str = "/",
        mkdir_permissions: int = 755,
        mkdir_exists_ok: bool = True,
    ) -> None:
        self.base_url = base_url
        self.base_dir = pathlib.Path(str(base_dir))
        self.mkdirs = mkdirs
        self.mkdir_permissions = mkdir_permissions
        self.mkdir_exists_ok = mkdir_exists_ok

    async def write(self, path: str, data: AsyncReader) -> None:
        full_path = self.base_dir / path
        if self.mkdirs and not await anyio.to_thread.run_sync(full_path.parent.exists):
            await anyio.to_thread.run_sync(
                os.makedirs,
                full_path.parent,
                self.mkdir_permissions,
                self.mkdir_exists_ok,
            )

        async with await anyio.open_file(self.base_dir / path, mode="wb") as f:
            while chunk := await data.read(1024 * 8):
                await f.write(chunk)

    async def read(self, path: str, chunk_size: int) -> AsyncFileLike:
        return await anyio.open_file(self.base_dir / path, mode="rb")

    async def delete(self, path: str) -> None:
        full_path = self.base_dir / path
        if await anyio.to_thread.run_sync(full_path.exists):
            await anyio.to_thread.run_sync(os.remove, full_path)

    async def exists(self, path: str) -> bool:
        return await anyio.to_thread.run_sync(os.path.exists, self.base_dir / path)

    async def url(self, path: str) -> str:
        return os.path.join(self.base_url, path)

    def abspath(self, path: str) -> str:
        return str(self.base_dir / path)
