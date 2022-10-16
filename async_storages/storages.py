import abc
import anyio.to_thread
import inspect
import io
import mimetypes
import os
import pathlib
import tempfile
import typing


class AsyncReader(typing.Protocol):  # pragma: nocover
    async def read(self, n: int = -1) -> bytes:
        ...


class AsyncFileLike(typing.Protocol):  # pragma: nocover
    async def read(self, n: int = -1) -> bytes:
        ...


class AdaptedBytesIO:
    def __init__(self, base: typing.BinaryIO | tempfile.SpooledTemporaryFile[bytes]) -> None:
        self.io = base

    async def read(self, n: int = -1) -> bytes:
        if isinstance(self.io, tempfile.SpooledTemporaryFile):
            if is_rolled(self.io):
                return await anyio.to_thread.run_sync(self.io.read, n)
            return self.io.read(n)
        return await anyio.to_thread.run_sync(self.io.read, n)


class BaseStorage(abc.ABC):
    @abc.abstractmethod
    async def write(self, path: str, data: AsyncReader) -> None:  # pragma: nocover
        ...

    @abc.abstractmethod
    async def read(self, path: str, chunk_size: int) -> AsyncFileLike:  # pragma: nocover
        ...

    @abc.abstractmethod
    async def delete(self, path: str) -> None:  # pragma: nocover
        ...

    @abc.abstractmethod
    async def exists(self, path: str) -> bool:  # pragma: nocover
        ...

    @abc.abstractmethod
    async def url(self, path: str) -> str:  # pragma: nocover
        ...


def is_rolled(file: tempfile.SpooledTemporaryFile[bytes]) -> bool:
    return getattr(file, "_rolled", True)


class MemoryStorage(BaseStorage):
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
        file = self.fs[path]
        await anyio.to_thread.run_sync(file.seek, 0)
        return AdaptedBytesIO(file)

    async def delete(self, path: str) -> None:
        if path in self.fs:
            del self.fs[path]

    async def exists(self, path: str) -> bool:
        return path in self.fs

    async def url(self, path: str) -> str:
        return "/" + path  # not possible to generate URL for memory-based files


class LocalStorage(BaseStorage):
    def __init__(self, base_dir: str | os.PathLike[typing.AnyStr], mkdirs: bool = False, base_url: str = "/") -> None:
        self.base_url = base_url
        self.base_dir = pathlib.Path(str(base_dir))
        self.mkdirs = mkdirs

    async def write(self, path: str, data: AsyncReader) -> None:
        full_path = self.base_dir / path
        if self.mkdirs and not await anyio.to_thread.run_sync(full_path.parent.exists):
            await anyio.to_thread.run_sync(os.makedirs, full_path.parent)

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


class S3Storage(BaseStorage):
    def __init__(
        self,
        bucket: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str | None = None,
        profile_name: str | None = None,
        endpoint_url: str | None = None,
        signed_link_ttl: int = 300,
    ) -> None:
        import aioboto3

        self.bucket = bucket
        self.signed_link_ttl = signed_link_ttl
        self.endpoint_url = endpoint_url.rstrip("/") if endpoint_url else None
        self.region_name = region_name or "us-east-2"
        self.session = aioboto3.Session(
            region_name=region_name,
            profile_name=profile_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    async def write(self, path: str, data: AsyncReader) -> None:
        mime_type = mimetypes.guess_type(path)
        async with self.session.client("s3", endpoint_url=self.endpoint_url) as client:
            await client.upload_fileobj(
                data,
                self.bucket,
                path,
                ExtraArgs={
                    "ContentType": mime_type[0],
                },
            )

    async def read(self, path: str, chunk_size: int) -> AsyncFileLike:
        from botocore.exceptions import ClientError

        async with self.session.client("s3", endpoint_url=self.endpoint_url) as client:
            try:
                s3_object = await client.get_object(Bucket=self.bucket, Key=path)
            except ClientError as ex:
                if ex.response["Error"]["Code"] == "NoSuchKey":
                    raise FileNotFoundError("File not found: %s" % path)
                raise
            else:
                return typing.cast(AsyncFileLike, s3_object["Body"])

    async def delete(self, path: str) -> None:
        from botocore.exceptions import ClientError

        async with self.session.client("s3", endpoint_url=self.endpoint_url) as client:
            try:
                await client.delete_object(Bucket=self.bucket, Key=path)
            except ClientError as ex:
                if ex.response["Error"]["Code"] == "NoSuchKey":
                    raise FileNotFoundError()
                raise

    async def exists(self, path: str) -> bool:
        from botocore.exceptions import ClientError

        async with self.session.client("s3", endpoint_url=self.endpoint_url) as client:
            try:
                await client.get_object(Bucket=self.bucket, Key=path)
            except ClientError as ex:
                if ex.response["Error"]["Code"] == "NoSuchKey":
                    return False
                raise
            else:
                return True

    async def url(self, path: str) -> str:
        async with self.session.client("s3", endpoint_url=self.endpoint_url) as client:
            url = await client.generate_presigned_url(
                ClientMethod="get_object",
                Params={"Bucket": self.bucket, "Key": path},
                ExpiresIn=self.signed_link_ttl,
            )
            return typing.cast(str, url)


class Store:
    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    async def write(self, path: str | os.PathLike[typing.AnyStr], data: bytes | AsyncReader | typing.BinaryIO) -> None:
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
