import datetime
import time
import uuid
from unittest import mock

from async_storages import generate_file_path


class UploadFile:
    filename: str = "myfile.txt"


def test_generate_file_path() -> None:
    with mock.patch("uuid.uuid4", return_value=uuid.UUID("00000000-0000-0000-0000-000000000000")):
        destination = "/media/{random}/{uuid}/{name}.{extension}"
        expected = "/media/00000000/00000000-0000-0000-0000-000000000000/myfile.txt"
        assert generate_file_path(UploadFile(), destination) == expected

    # test date time
    today = datetime.datetime.now()
    with mock.patch("async_storages.helpers._get_now", lambda: today):
        expected = f"/media/{today.date()}/{today.isoformat()}/{today.time()}/myfile.txt"
        assert generate_file_path(UploadFile(), "/media/{date}/{datetime}/{time}/{file_name}") == expected

    # test timestamp
    timestamp = int(time.time())
    with mock.patch("async_storages.helpers.time.time", lambda: timestamp):
        expected = f"/media/{timestamp}/myfile.txt"
        assert generate_file_path(UploadFile(), "/media/{timestamp}/{file_name}") == expected
