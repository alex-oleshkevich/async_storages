import datetime
import os
import time
import typing
import uuid

import sanitize_filename


class UploadFileLike(typing.Protocol):  # pragma: no cover
    filename: str


def _get_now() -> datetime.datetime:
    # this is a mock target for tests
    return datetime.datetime.now()


def generate_file_path(
    file: UploadFileLike,
    destination: str,
    extra_tokens: typing.Mapping[str, typing.Any] | None = None,
) -> str:
    """Generate file path interpolation tokens in destination."""
    now = _get_now()
    return destination.format(
        # randomization
        random=uuid.uuid4().hex[:8],
        uuid=str(uuid.uuid4()),
        # date and time
        date=now.date().isoformat(),
        datetime=now.isoformat(),
        time=now.time().isoformat(),
        timestamp=int(time.time()),
        # file name parts
        file_name=sanitize_filename.sanitize(file.filename),
        name=os.path.splitext(file.filename)[0],
        extension=os.path.splitext(file.filename)[1].removeprefix("."),
        **(extra_tokens or {}),
    )
