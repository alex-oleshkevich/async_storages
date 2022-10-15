import asyncio
import os

from async_storages.storages import S3Storage, Store

BUCKET = os.environ.get('BUCKET', 'asyncstorages')
ACCESS_KEY_ID = os.environ.get('ACCESS_KEY_ID', 'minioadmin')
ACCESS_KEY_SECRET = os.environ.get('ACCESS_KEY_SECRET', 'minioadmin')
REGION_NAME = os.environ.get('REGION_NAME')
PROFILE_NAME = os.environ.get('PROFILE_NAME')
ENDPOINT_URL = os.environ.get('ENDPOINT_URL', 'http://localhost:9000')
TEST_FILE_NAME = 'async_storages/test.txt'
TEST_FILE_CONTENT = b'CONTENT'


async def main() -> None:
    store = Store(S3Storage(
        bucket=BUCKET, aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=ACCESS_KEY_SECRET,
        region_name=REGION_NAME, profile_name=PROFILE_NAME, endpoint_url=ENDPOINT_URL,
    ))

    print(f'Writing bytes to {TEST_FILE_NAME}')
    await store.write(TEST_FILE_NAME, TEST_FILE_CONTENT)

    print(f'Test if file exists in the remote bucket')
    assert await store.exists(TEST_FILE_NAME)
    print('It is. Read file contents.')
    print('Delete file')
    await store.delete(TEST_FILE_NAME)
    print('Make sure that file does not exist.')
    assert not await store.exists(TEST_FILE_NAME)
    print('Done. All OK')


asyncio.run(main())
