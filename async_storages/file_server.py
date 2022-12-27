import mimetypes
import os
import typing
from starlette.responses import FileResponse, PlainTextResponse, RedirectResponse, Response, StreamingResponse
from starlette.types import Receive, Scope, Send
from urllib.parse import quote

from async_storages import FileStorage, MemoryStorage


class FileServer:
    def __init__(self, storage: FileStorage, as_attachment: bool = True) -> None:
        self.storage = storage
        self.as_attachment = as_attachment

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        assert scope["type"] == "http"
        path = self.get_path(scope)
        response = await self.get_response(path, scope)
        await response(scope, receive, send)

    async def get_response(self, path: str, scope: Scope) -> Response:
        if scope["method"] not in ("GET", "HEAD"):
            return PlainTextResponse("Method Not Allowed", status_code=405)

        url = await self.storage.url(path)
        if url.startswith("http"):
            return RedirectResponse(url, status_code=302)

        if not await self.storage.exists(path):
            return PlainTextResponse("File not found", status_code=404)

        mime_type, _ = mimetypes.guess_type(path)
        disposition = "attachment" if self.as_attachment else "inline"
        if isinstance(self.storage.storage, MemoryStorage):
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
        return typing.cast(str, os.path.normpath(os.path.join(*scope["path"].split("/"))))
