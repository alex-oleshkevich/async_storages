import abc
import tempfile
import types
import typing

import anyio.to_thread


def is_rolled(file: tempfile.SpooledTemporaryFile[bytes]) -> bool:
    return getattr(file, "_rolled", True)


class AsyncReader(typing.Protocol):  # pragma: no cover
    async def read(self, n: int = -1) -> bytes: ...


class AsyncFileLike(typing.Protocol):  # pragma: no cover
    async def read(self, n: int = -1) -> bytes: ...

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        yield b""

    async def __aenter__(self) -> "AsyncFileLike": ...

    async def __aexit__(
        self, exc_type: type[Exception], exc_val: BaseException, exc_tb: types.TracebackType
    ) -> None: ...


class AdaptedBytesIO:
    def __init__(self, base: typing.BinaryIO | tempfile.SpooledTemporaryFile[bytes]) -> None:
        self.io = base

    async def read(self, n: int = -1) -> bytes:
        if isinstance(self.io, tempfile.SpooledTemporaryFile):
            if is_rolled(self.io):
                return await anyio.to_thread.run_sync(self.io.read, n)
            return self.io.read(n)
        return await anyio.to_thread.run_sync(self.io.read, n)

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        if isinstance(self.io, tempfile.SpooledTemporaryFile) and not is_rolled(self.io):
            for line in self.io.readlines():
                yield line
        else:
            for line in await anyio.to_thread.run_sync(self.io.readlines):  # will it block for large files?
                yield line

    async def __aenter__(self) -> "AdaptedBytesIO":
        return self

    async def __aexit__(self, exc_type: type[Exception], exc_val: BaseException, exc_tb: types.TracebackType) -> None:
        await anyio.to_thread.run_sync(self.io.close)


class BaseBackend(abc.ABC):  # pragma: no cover
    @abc.abstractmethod
    async def write(self, path: str, data: AsyncReader) -> None: ...

    @abc.abstractmethod
    async def read(self, path: str, chunk_size: int) -> AsyncFileLike: ...

    @abc.abstractmethod
    async def delete(self, path: str) -> None: ...

    @abc.abstractmethod
    async def exists(self, path: str) -> bool: ...

    @abc.abstractmethod
    async def url(self, path: str) -> str: ...

    @abc.abstractmethod
    def abspath(self, path: str) -> str: ...
