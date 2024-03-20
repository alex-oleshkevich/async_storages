import mimetypes
import os
import typing
from urllib.parse import quote

from starlette.responses import (
    FileResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from starlette.types import Receive, Scope, Send

from async_storages import FileStorage, MemoryBackend

# add uploader
# add file name generator


class FileServer:
    def __init__(
        self,
        storage: FileStorage,
        as_attachment: bool = True,
        redirect_status: int = 301,
    ) -> None:
        self.storage = storage
        self.redirect_status = redirect_status
        self.as_attachment = as_attachment

    async def get_response(self, path: str, scope: Scope) -> Response:
        if scope["method"] not in ("GET", "HEAD"):
            return PlainTextResponse("Method Not Allowed", status_code=405)

        # in case of s3-like storages - they should return URL to the file
        # we will redirect to that destination
        url = await self.storage.url(path)
        if url.startswith("http://") or url.startswith("https://"):
            return RedirectResponse(url, status_code=self.redirect_status)

        if not await self.storage.exists(path):
            return PlainTextResponse("File not found", status_code=404)

        mime_type, _ = mimetypes.guess_type(path)
        disposition = "attachment" if self.as_attachment else "inline"
        if isinstance(self.storage.storage, MemoryBackend):
            reader = await self.storage.storage.read(path, 1024 * 8)

            async def streamer() -> typing.AsyncIterable[bytes]:
                while chunk := await reader.read(1024 * 8):
                    yield chunk

            return StreamingResponse(
                streamer(),
                status_code=200,
                headers={
                    "content-disposition": '{disposition}; filename="{filename}"'.format(
                        disposition=disposition, filename=quote(os.path.basename(path))
                    )
                },
                media_type=mime_type,
            )

        # only for LocalStorage
        path = self.storage.abspath(path)

        return FileResponse(
            path,
            media_type=mime_type,
            filename=os.path.basename(path),
            content_disposition_type=disposition,
        )

    def get_path(self, scope: Scope) -> str:
        file_path = scope["path"].replace(scope["root_path"], "").strip("/")
        return typing.cast(str, os.path.normpath(os.path.join(*file_path.split("/"))))

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        assert scope["type"] == "http"
        path = self.get_path(scope)
        response = await self.get_response(path, scope)
        await response(scope, receive, send)
