import io
import pathlib
import pytest
from starlette.applications import Starlette
from starlette.routing import Mount
from starlette.testclient import TestClient

from async_storages import BaseStorage, FileStorage, MemoryStorage
from async_storages.file_server import FileServer
from async_storages.storages import AdaptedBytesIO, AsyncFileLike, AsyncReader, LocalStorage

pytestmark = [pytest.mark.asyncio]


async def test_file_server_accept_get_or_head_only() -> None:
    storage = MemoryStorage()
    file_server = FileServer(FileStorage(storage))
    app = Starlette(routes=[Mount("/", file_server)])
    await storage.write("test.txt", AdaptedBytesIO(io.BytesIO(b"")))

    client = TestClient(app)
    assert client.get("/test.txt").status_code == 200
    assert client.head("/test.txt").status_code == 200
    assert client.post("/test.txt").status_code == 405


class _RemoteStorage(BaseStorage):  # pragma: nocover
    async def write(self, path: str, data: AsyncReader) -> None:
        pass

    async def read(self, path: str, chunk_size: int) -> AsyncFileLike:
        return AdaptedBytesIO(io.BytesIO(b""))

    async def delete(self, path: str) -> None:
        pass

    async def exists(self, path: str) -> bool:
        return True

    async def url(self, path: str) -> str:
        return f"http://testmediaserver/{path}"

    def abspath(self, path: str) -> str:
        return ""


def test_file_server_sends_redirect() -> None:
    storage = _RemoteStorage()
    file_server = FileServer(FileStorage(storage))
    app = Starlette(routes=[Mount("/", file_server)])

    client = TestClient(app)
    response = client.get("/test.txt", allow_redirects=False)
    assert response.status_code == 302
    assert response.headers["location"] == "http://testmediaserver/test.txt"


async def test_file_server_sends_file(tmp_path: pathlib.Path) -> None:
    storage = LocalStorage(tmp_path, mkdirs=True)
    file_server = FileServer(FileStorage(storage))
    app = Starlette(routes=[Mount("/", file_server)])
    await storage.write("test.txt", AdaptedBytesIO(io.BytesIO(b"content")))

    client = TestClient(app)
    response = client.get("/test.txt")
    assert response.status_code == 200
    assert response.text == "content"


async def test_file_server_returns_404_for_missing_files(tmp_path: pathlib.Path) -> None:
    storage = LocalStorage(tmp_path, mkdirs=True)
    file_server = FileServer(FileStorage(storage))
    app = Starlette(routes=[Mount("/", file_server)])

    client = TestClient(app)
    response = client.get("/test.txt")
    assert response.status_code == 404


async def test_file_server_streams_from_memory_storage() -> None:
    storage = MemoryStorage()
    file_server = FileServer(FileStorage(storage))
    app = Starlette(routes=[Mount("/", file_server)])
    await storage.write("test.txt", AdaptedBytesIO(io.BytesIO(b"content")))

    client = TestClient(app)
    assert client.get("/test.txt").text == "content"


async def test_file_server_sends_content_disposition_attachment() -> None:
    storage = MemoryStorage()
    file_server = FileServer(FileStorage(storage), as_attachment=True)
    app = Starlette(routes=[Mount("/", file_server)])
    await storage.write("test.txt", AdaptedBytesIO(io.BytesIO(b"content")))

    client = TestClient(app)
    assert client.get("/test.txt").headers["content-disposition"] == 'attachment; filename="test.txt"'


async def test_file_server_sends_content_disposition_inline() -> None:
    storage = MemoryStorage()
    file_server = FileServer(FileStorage(storage), as_attachment=False)
    app = Starlette(routes=[Mount("/", file_server)])
    await storage.write("test.txt", AdaptedBytesIO(io.BytesIO(b"content")))

    client = TestClient(app)
    assert client.get("/test.txt").headers["content-disposition"] == 'inline; filename="test.txt"'
