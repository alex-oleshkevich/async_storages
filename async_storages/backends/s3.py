import mimetypes
import typing

from async_storages.backends.base import AsyncFileLike, AsyncReader, BaseBackend


class S3Backend(BaseBackend):
    def __init__(
        self,
        bucket: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str | None = None,
        profile_name: str | None = None,
        endpoint_url: str | None = None,
        signed_link_ttl: int = 3600,
    ) -> None:
        try:
            import aioboto3
        except ImportError:
            raise ImportError("Install aioboto3 to use s3 backend: pip install async_storages[s3]")

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
                raise  # pragma: nocover
            else:
                return typing.cast(AsyncFileLike, s3_object["Body"])

    async def delete(self, path: str) -> None:
        pass

        async with self.session.client("s3", endpoint_url=self.endpoint_url) as client:
            await client.delete_object(Bucket=self.bucket, Key=path)

    async def exists(self, path: str) -> bool:
        from botocore.exceptions import ClientError

        async with self.session.client("s3", endpoint_url=self.endpoint_url) as client:
            try:
                await client.get_object(Bucket=self.bucket, Key=path)
            except ClientError as ex:
                if ex.response["Error"]["Code"] == "NoSuchKey":
                    return False
                raise  # pragma: nocover
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

    def abspath(self, path: str) -> str:
        return path
