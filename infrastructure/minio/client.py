"""MinIO client wrapper — thin layer between the data pipeline and MinIO storage."""
import io
import json
import logging
from datetime import datetime, timezone
from typing import Iterator

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


class BucketNotFoundError(Exception):
    """Raised when an operation targets a bucket that does not exist."""


class MinIOClient:
    """Wraps the MinIO Python SDK for use by the data pipeline.

    All methods use structured logging so every storage operation is fully
    traceable.  Credentials are never stored on this object — they are
    consumed by the underlying SDK client at construction time.

    Usage::

        from infrastructure.minio.client import MinIOClient

        client = MinIOClient(config["minio"])
        client.ensure_bucket("raw")
        client.upload_json("raw", "youtube/2024-01-01/search.json", data)
    """

    def __init__(self, config: dict):
        """
        Args:
            config: The ``minio`` section of ``config.yaml``, e.g.::

                endpoint: "localhost:9000"
                access_key: "<from env>"
                secret_key: "<from env>"
                secure: false
        """
        self._client = Minio(
            endpoint=config["endpoint"],
            access_key=config["access_key"],
            secret_key=config["secret_key"],
            secure=config.get("secure", False),
        )
        logger.debug("MinIOClient initialised | endpoint=%s", config["endpoint"])

    # ------------------------------------------------------------------
    # Bucket management
    # ------------------------------------------------------------------

    def ensure_bucket(self, bucket_name: str) -> None:
        """Create ``bucket_name`` if it does not already exist.

        Safe to call on every pipeline run — no-op when the bucket is
        already present.

        Args:
            bucket_name: Name of the bucket to create/verify.
        """
        if not self._client.bucket_exists(bucket_name):
            self._client.make_bucket(bucket_name)
            logger.info("Bucket created | bucket=%s", bucket_name)
        else:
            logger.debug("Bucket already exists | bucket=%s", bucket_name)

    def bucket_exists(self, bucket_name: str) -> bool:
        """Return ``True`` if ``bucket_name`` exists.

        Args:
            bucket_name: Bucket to check.
        """
        return self._client.bucket_exists(bucket_name)

    # ------------------------------------------------------------------
    # JSON helpers (primary use-case: raw YouTube API responses)
    # ------------------------------------------------------------------

    def upload_json(self, bucket: str, key: str, data: dict | list) -> None:
        """Serialise ``data`` to JSON and upload to ``bucket/key``.

        Args:
            bucket: Target bucket name.
            key:    Object key, e.g. ``youtube/2024-01-01/search_page_1.json``.
            data:   Python dict or list to serialise.

        Raises:
            BucketNotFoundError: If ``bucket`` does not exist.
            S3Error:             On any other MinIO / S3 error.
        """
        self._assert_bucket_exists(bucket)

        payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self._put_bytes(bucket, key, payload, content_type="application/json")
        logger.info("JSON uploaded | bucket=%s | key=%s | size_bytes=%d", bucket, key, len(payload))

    def download_json(self, bucket: str, key: str) -> dict | list:
        """Download ``bucket/key`` and deserialise as JSON.

        Args:
            bucket: Source bucket name.
            key:    Object key.

        Returns:
            Parsed Python object (dict or list).

        Raises:
            BucketNotFoundError: If ``bucket`` does not exist.
            S3Error:             On any other MinIO / S3 error.
        """
        self._assert_bucket_exists(bucket)

        response = None
        try:
            response = self._client.get_object(bucket, key)
            data = json.loads(response.read().decode("utf-8"))
            logger.info("JSON downloaded | bucket=%s | key=%s", bucket, key)
            return data
        finally:
            if response:
                response.close()
                response.release_conn()

    # ------------------------------------------------------------------
    # Raw bytes helpers (future use: Parquet, Avro, compressed files)
    # ------------------------------------------------------------------

    def upload_bytes(
        self, bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        """Upload raw ``bytes`` to ``bucket/key``.

        Args:
            bucket:       Target bucket name.
            key:          Object key.
            data:         Raw bytes to upload.
            content_type: MIME type of the object.

        Raises:
            BucketNotFoundError: If ``bucket`` does not exist.
            S3Error:             On any other MinIO / S3 error.
        """
        self._assert_bucket_exists(bucket)
        self._put_bytes(bucket, key, data, content_type)
        logger.info(
            "Bytes uploaded | bucket=%s | key=%s | size_bytes=%d | content_type=%s",
            bucket, key, len(data), content_type,
        )

    def download_bytes(self, bucket: str, key: str) -> bytes:
        """Download ``bucket/key`` and return raw bytes.

        Args:
            bucket: Source bucket name.
            key:    Object key.

        Returns:
            Raw bytes content of the object.

        Raises:
            BucketNotFoundError: If ``bucket`` does not exist.
            S3Error:             On any other MinIO / S3 error.
        """
        self._assert_bucket_exists(bucket)

        response = None
        try:
            response = self._client.get_object(bucket, key)
            data = response.read()
            logger.info(
                "Bytes downloaded | bucket=%s | key=%s | size_bytes=%d", bucket, key, len(data)
            )
            return data
        finally:
            if response:
                response.close()
                response.release_conn()

    # ------------------------------------------------------------------
    # Object management
    # ------------------------------------------------------------------

    def object_exists(self, bucket: str, key: str) -> bool:
        """Return ``True`` if ``bucket/key`` exists.

        Uses ``stat_object`` under the hood — no data is transferred.

        Args:
            bucket: Bucket name.
            key:    Object key.
        """
        try:
            self._client.stat_object(bucket, key)
            return True
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                return False
            raise

    def list_objects(self, bucket: str, prefix: str = "") -> Iterator[str]:
        """Yield object keys in ``bucket`` that start with ``prefix``.

        Args:
            bucket: Bucket to list.
            prefix: Key prefix filter (e.g. ``"youtube/2024-01-01/"``).

        Yields:
            Object key strings.

        Raises:
            BucketNotFoundError: If ``bucket`` does not exist.
        """
        self._assert_bucket_exists(bucket)

        objects = self._client.list_objects(bucket, prefix=prefix, recursive=True)
        for obj in objects:
            yield obj.object_name

    def delete_object(self, bucket: str, key: str) -> None:
        """Delete ``bucket/key``.

        Args:
            bucket: Bucket name.
            key:    Object key to delete.

        Raises:
            BucketNotFoundError: If ``bucket`` does not exist.
            S3Error:             On any other MinIO / S3 error.
        """
        self._assert_bucket_exists(bucket)
        self._client.remove_object(bucket, key)
        logger.info("Object deleted | bucket=%s | key=%s", bucket, key)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _put_bytes(self, bucket: str, key: str, data: bytes, content_type: str) -> None:
        """Internal upload using a BytesIO stream."""
        self._client.put_object(
            bucket_name=bucket,
            object_name=key,
            data=io.BytesIO(data),
            length=len(data),
            content_type=content_type,
        )

    def _assert_bucket_exists(self, bucket: str) -> None:
        """Raise ``BucketNotFoundError`` if ``bucket`` does not exist."""
        if not self._client.bucket_exists(bucket):
            raise BucketNotFoundError(
                f"Bucket '{bucket}' does not exist. "
                "Call ensure_bucket() before any read/write operations."
            )

# integration test for MinIOClient
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()
    config = {
        "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        "access_key": os.getenv("MINIO_ROOT_USER", "minioadmin"),
        "secret_key": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        "secure": False,
    }
    client = MinIOClient(config)
    bucket_name = "test-bucket"

    # check bucket existence and creation
    print("\n=== Bucket existence check ===\n")
    print("Bucket exists:", client.bucket_exists(bucket_name))
    client.ensure_bucket(bucket_name)
    print("Bucket exists after ensure_bucket:", client.bucket_exists(bucket_name))

    # upload and download JSON
    print("\n=== JSON upload/download test ===\n")
    test_data = {"message": "Hello, MinIO!", "timestamp": datetime.now(timezone.utc).isoformat()}
    key = "test/hello.json"
    client.upload_json(bucket_name, key, test_data)
    downloaded_data = client.download_json(bucket_name, key)
    print("Downloaded JSON:", downloaded_data)

    # objects management
    print("\n=== Objects management ===\n")
    print("Object exists:", client.object_exists(bucket_name, key))
    test_data = {"message": "This is a test file.", "timestamp": datetime.now(timezone.utc).isoformat()}
    client.upload_json(bucket_name, "test/file1.json", test_data)
    client.upload_json(bucket_name, "test/file2.json", test_data)
    print("Objects with prefix 'test/':", list(client.list_objects(bucket_name, prefix="test/")))
    client.delete_object(bucket_name, key)
    print("Object exists after deletion:", client.object_exists(bucket_name, key))
