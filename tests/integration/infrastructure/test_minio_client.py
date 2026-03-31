"""Integration tests for infrastructure/minio/client.py."""

import os
from datetime import datetime, timezone

import pytest
from dotenv import load_dotenv

from infrastructure.minio.client import MinIOClient


@pytest.fixture(scope="module")
def minio_client() -> MinIOClient:
    load_dotenv()
    config = {
        "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        "access_key": os.getenv("MINIO_ROOT_USER", "minioadmin"),
        "secret_key": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        "secure": False,
    }
    client = MinIOClient(config)
    try:
        client.bucket_exists("healthcheck")
    except Exception as exc:
        pytest.skip(f"MinIO is not reachable: {exc}")
    return client


@pytest.fixture
def test_bucket(minio_client: MinIOClient) -> str:
    name = f"it-minio-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
    minio_client.ensure_bucket(name)
    return name


def test_ensure_bucket_and_bucket_exists(minio_client: MinIOClient, test_bucket: str):
    assert minio_client.bucket_exists(test_bucket) is True


def test_json_upload_download_and_object_management(minio_client: MinIOClient, test_bucket: str):
    key = "test/hello.json"
    payload = {
        "message": "Hello, MinIO!",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    minio_client.upload_json(test_bucket, key, payload)

    downloaded = minio_client.download_json(test_bucket, key)
    assert downloaded == payload
    assert minio_client.object_exists(test_bucket, key) is True

    second_payload = {
        "message": "This is a test file.",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    minio_client.upload_json(test_bucket, "test/file1.json", second_payload)
    minio_client.upload_json(test_bucket, "test/file2.json", second_payload)

    listed = list(minio_client.list_objects(test_bucket, prefix="test/"))
    assert key in listed
    assert "test/file1.json" in listed
    assert "test/file2.json" in listed

    minio_client.delete_object(test_bucket, key)
    assert minio_client.object_exists(test_bucket, key) is False
