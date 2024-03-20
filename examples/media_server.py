import os

from starlette.applications import Starlette
from starlette.routing import Mount

from async_storages import FileStorage, FileSystemBackend
from async_storages.contrib.starlette import FileServer

BUCKET = os.environ.get("BUCKET", "asyncstorages")
ACCESS_KEY_ID = os.environ.get("ACCESS_KEY_ID", "minioadmin")
ACCESS_KEY_SECRET = os.environ.get("ACCESS_KEY_SECRET", "minioadmin")
REGION_NAME = os.environ.get("REGION_NAME")
PROFILE_NAME = os.environ.get("PROFILE_NAME")
ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:9000")
TEST_FILE_NAME = "async_storages/test.txt"
TEST_FILE_CONTENT = b"CONTENT"

this_dir = os.path.dirname(__file__)
store = FileStorage(FileSystemBackend(base_dir=os.path.join(this_dir, "media"), base_url="/"))
app = Starlette(debug=True, routes=[Mount("/media", FileServer(store))])
