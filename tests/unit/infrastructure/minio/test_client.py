"""Unit tests for infrastructure/minio/client.py.

Every test is fully isolated — no real MinIO instance is needed.
The underlying ``minio.Minio`` SDK is patched at construction time via the
``sdk`` / ``client`` fixtures so each test controls exactly what the SDK returns
or raises.
"""
import json
import pytest
from unittest.mock import MagicMock, patch, call

from minio.error import S3Error

from infrastructure.minio.client import MinIOClient, BucketNotFoundError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MINIO_CONFIG = {
    "endpoint": "localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "secure": False,
}


def _make_s3_error(code: str) -> S3Error:
    """Build a real S3Error instance without invoking the XML parser.

    Uses ``__new__`` to bypass the constructor and sets only the attributes
    that ``MinIOClient`` inspects (``exc.code``).
    """
    exc = S3Error.__new__(S3Error)
    exc.code = code
    exc.message = "test error"
    exc.resource = "/test-bucket/test-key"
    exc.request_id = "test-request-id"
    exc.host_id = "test-host-id"
    exc.response = None
    exc.bucket_name = None
    exc.object_name = None
    return exc


def _make_response(content: bytes) -> MagicMock:
    """Build a mock HTTP response with .read(), .close(), .release_conn()."""
    resp = MagicMock()
    resp.read.return_value = content
    return resp


def _make_list_item(name: str) -> MagicMock:
    """Build a mock MinIO object entry with .object_name."""
    obj = MagicMock()
    obj.object_name = name
    return obj


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sdk() -> MagicMock:
    """A fresh MagicMock standing in for the minio.Minio SDK instance."""
    return MagicMock()


@pytest.fixture
def client(sdk: MagicMock) -> MinIOClient:
    """MinIOClient wired to the mock SDK — no network calls made."""
    with patch("infrastructure.minio.client.Minio", return_value=sdk):
        return MinIOClient(MINIO_CONFIG)


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------

class TestMinIOClientInit:
    def test_passes_endpoint_to_sdk(self):
        with patch("infrastructure.minio.client.Minio") as MockMinio:
            MinIOClient(MINIO_CONFIG)
            assert MockMinio.call_args.kwargs["endpoint"] == "localhost:9000"

    def test_passes_credentials_to_sdk(self):
        with patch("infrastructure.minio.client.Minio") as MockMinio:
            MinIOClient(MINIO_CONFIG)
            kwargs = MockMinio.call_args.kwargs
            assert kwargs["access_key"] == "minioadmin"
            assert kwargs["secret_key"] == "minioadmin"

    def test_passes_secure_flag_to_sdk(self):
        with patch("infrastructure.minio.client.Minio") as MockMinio:
            MinIOClient(MINIO_CONFIG)
            assert MockMinio.call_args.kwargs["secure"] is False

    def test_secure_defaults_to_false_when_omitted(self):
        config = {"endpoint": "localhost:9000", "access_key": "a", "secret_key": "b"}
        with patch("infrastructure.minio.client.Minio") as MockMinio:
            MinIOClient(config)
            assert MockMinio.call_args.kwargs["secure"] is False

    def test_secure_true_is_passed_through(self):
        config = {**MINIO_CONFIG, "secure": True}
        with patch("infrastructure.minio.client.Minio") as MockMinio:
            MinIOClient(config)
            assert MockMinio.call_args.kwargs["secure"] is True


# ---------------------------------------------------------------------------
# ensure_bucket
# ---------------------------------------------------------------------------

class TestEnsureBucket:
    def test_creates_bucket_when_it_does_not_exist(self, client, sdk):
        sdk.bucket_exists.return_value = False

        client.ensure_bucket("raw")

        sdk.make_bucket.assert_called_once_with("raw")

    def test_does_not_create_bucket_when_it_already_exists(self, client, sdk):
        sdk.bucket_exists.return_value = True

        client.ensure_bucket("raw")

        sdk.make_bucket.assert_not_called()

    def test_checks_the_correct_bucket_name(self, client, sdk):
        sdk.bucket_exists.return_value = True

        client.ensure_bucket("processed")

        sdk.bucket_exists.assert_called_once_with("processed")


# ---------------------------------------------------------------------------
# bucket_exists
# ---------------------------------------------------------------------------

class TestBucketExists:
    def test_returns_true_when_bucket_exists(self, client, sdk):
        sdk.bucket_exists.return_value = True
        assert client.bucket_exists("raw") is True

    def test_returns_false_when_bucket_missing(self, client, sdk):
        sdk.bucket_exists.return_value = False
        assert client.bucket_exists("raw") is False

    def test_delegates_to_sdk_with_correct_name(self, client, sdk):
        sdk.bucket_exists.return_value = True

        client.bucket_exists("curated")

        sdk.bucket_exists.assert_called_once_with("curated")


# ---------------------------------------------------------------------------
# upload_json
# ---------------------------------------------------------------------------

class TestUploadJson:
    def test_raises_bucket_not_found_when_bucket_missing(self, client, sdk):
        sdk.bucket_exists.return_value = False

        with pytest.raises(BucketNotFoundError):
            client.upload_json("raw", "test.json", {"key": "value"})

    def test_put_object_called_with_correct_bucket_and_key(self, client, sdk):
        sdk.bucket_exists.return_value = True

        client.upload_json("raw", "youtube/2024-01-01/search.json", {"a": 1})

        kwargs = sdk.put_object.call_args.kwargs
        assert kwargs["bucket_name"] == "raw"
        assert kwargs["object_name"] == "youtube/2024-01-01/search.json"

    def test_content_type_is_application_json(self, client, sdk):
        sdk.bucket_exists.return_value = True

        client.upload_json("raw", "test.json", {})

        assert sdk.put_object.call_args.kwargs["content_type"] == "application/json"

    def test_serialises_dict_correctly(self, client, sdk):
        sdk.bucket_exists.return_value = True
        data = {"video_id": "abc123", "title": "Test Video"}

        client.upload_json("raw", "test.json", data)

        uploaded_bytes = sdk.put_object.call_args.kwargs["data"].read()
        assert json.loads(uploaded_bytes) == data

    def test_serialises_list_correctly(self, client, sdk):
        sdk.bucket_exists.return_value = True
        data = [{"id": "v1"}, {"id": "v2"}]

        client.upload_json("raw", "test.json", data)

        uploaded_bytes = sdk.put_object.call_args.kwargs["data"].read()
        assert json.loads(uploaded_bytes) == data

    def test_length_matches_actual_payload_size(self, client, sdk):
        sdk.bucket_exists.return_value = True
        data = {"key": "value"}

        client.upload_json("raw", "test.json", data)

        expected_len = len(json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"))
        assert sdk.put_object.call_args.kwargs["length"] == expected_len

    def test_non_ascii_characters_are_preserved(self, client, sdk):
        """Sinhala and Tamil keywords must survive JSON round-trip."""
        sdk.bucket_exists.return_value = True
        data = {"title": "ශ්‍රී ලංකා / இலங்கை"}

        client.upload_json("raw", "test.json", data)

        uploaded_bytes = sdk.put_object.call_args.kwargs["data"].read()
        assert json.loads(uploaded_bytes)["title"] == "ශ්‍රී ලංකා / இலங்கை"

    def test_does_not_call_put_object_when_bucket_missing(self, client, sdk):
        sdk.bucket_exists.return_value = False

        with pytest.raises(BucketNotFoundError):
            client.upload_json("raw", "test.json", {})

        sdk.put_object.assert_not_called()


# ---------------------------------------------------------------------------
# download_json
# ---------------------------------------------------------------------------

class TestDownloadJson:
    def test_raises_bucket_not_found_when_bucket_missing(self, client, sdk):
        sdk.bucket_exists.return_value = False

        with pytest.raises(BucketNotFoundError):
            client.download_json("raw", "test.json")

    def test_returns_parsed_dict(self, client, sdk):
        sdk.bucket_exists.return_value = True
        data = {"video_id": "abc123", "views": 1000}
        sdk.get_object.return_value = _make_response(json.dumps(data).encode("utf-8"))

        result = client.download_json("raw", "test.json")

        assert result == data

    def test_returns_parsed_list(self, client, sdk):
        sdk.bucket_exists.return_value = True
        data = [{"id": "v1"}, {"id": "v2"}]
        sdk.get_object.return_value = _make_response(json.dumps(data).encode("utf-8"))

        result = client.download_json("raw", "test.json")

        assert result == data

    def test_calls_get_object_with_correct_bucket_and_key(self, client, sdk):
        sdk.bucket_exists.return_value = True
        sdk.get_object.return_value = _make_response(b"{}")

        client.download_json("raw", "youtube/2024-01-01/search.json")

        sdk.get_object.assert_called_once_with("raw", "youtube/2024-01-01/search.json")

    def test_closes_response_on_success(self, client, sdk):
        sdk.bucket_exists.return_value = True
        mock_resp = _make_response(b'{"key": "val"}')
        sdk.get_object.return_value = mock_resp

        client.download_json("raw", "test.json")

        mock_resp.close.assert_called_once()
        mock_resp.release_conn.assert_called_once()

    def test_closes_response_even_when_json_parse_fails(self, client, sdk):
        """The finally block must fire even when .read() returns invalid JSON."""
        sdk.bucket_exists.return_value = True
        mock_resp = _make_response(b"not-valid-json{{{{")
        sdk.get_object.return_value = mock_resp

        with pytest.raises(Exception):
            client.download_json("raw", "test.json")

        mock_resp.close.assert_called_once()
        mock_resp.release_conn.assert_called_once()


# ---------------------------------------------------------------------------
# upload_bytes
# ---------------------------------------------------------------------------

class TestUploadBytes:
    def test_raises_bucket_not_found_when_bucket_missing(self, client, sdk):
        sdk.bucket_exists.return_value = False

        with pytest.raises(BucketNotFoundError):
            client.upload_bytes("raw", "file.parquet", b"data")

    def test_put_object_called_with_correct_bucket_and_key(self, client, sdk):
        sdk.bucket_exists.return_value = True

        client.upload_bytes("raw", "data/file.parquet", b"binary")

        kwargs = sdk.put_object.call_args.kwargs
        assert kwargs["bucket_name"] == "raw"
        assert kwargs["object_name"] == "data/file.parquet"

    def test_default_content_type_is_octet_stream(self, client, sdk):
        sdk.bucket_exists.return_value = True

        client.upload_bytes("raw", "file.bin", b"data")

        assert sdk.put_object.call_args.kwargs["content_type"] == "application/octet-stream"

    def test_custom_content_type_is_passed_through(self, client, sdk):
        sdk.bucket_exists.return_value = True

        client.upload_bytes("raw", "file.parquet", b"data", content_type="application/parquet")

        assert sdk.put_object.call_args.kwargs["content_type"] == "application/parquet"

    def test_bytes_content_is_uploaded_correctly(self, client, sdk):
        sdk.bucket_exists.return_value = True
        data = b"\x00\x01\x02\x03\xff"

        client.upload_bytes("raw", "file.bin", data)

        uploaded = sdk.put_object.call_args.kwargs["data"].read()
        assert uploaded == data

    def test_length_matches_data_size(self, client, sdk):
        sdk.bucket_exists.return_value = True
        data = b"exactly-this-long"

        client.upload_bytes("raw", "file.bin", data)

        assert sdk.put_object.call_args.kwargs["length"] == len(data)


# ---------------------------------------------------------------------------
# download_bytes
# ---------------------------------------------------------------------------

class TestDownloadBytes:
    def test_raises_bucket_not_found_when_bucket_missing(self, client, sdk):
        sdk.bucket_exists.return_value = False

        with pytest.raises(BucketNotFoundError):
            client.download_bytes("raw", "file.parquet")

    def test_returns_raw_bytes(self, client, sdk):
        sdk.bucket_exists.return_value = True
        expected = b"\x00\x01\x02\x03"
        sdk.get_object.return_value = _make_response(expected)

        result = client.download_bytes("raw", "file.bin")

        assert result == expected

    def test_calls_get_object_with_correct_bucket_and_key(self, client, sdk):
        sdk.bucket_exists.return_value = True
        sdk.get_object.return_value = _make_response(b"data")

        client.download_bytes("raw", "path/to/file.parquet")

        sdk.get_object.assert_called_once_with("raw", "path/to/file.parquet")

    def test_closes_response_on_success(self, client, sdk):
        sdk.bucket_exists.return_value = True
        mock_resp = _make_response(b"binary-content")
        sdk.get_object.return_value = mock_resp

        client.download_bytes("raw", "file.bin")

        mock_resp.close.assert_called_once()
        mock_resp.release_conn.assert_called_once()

    def test_closes_response_even_when_read_raises(self, client, sdk):
        """The finally block must fire even when .read() raises."""
        sdk.bucket_exists.return_value = True
        mock_resp = _make_response(b"")
        mock_resp.read.side_effect = RuntimeError("network failure")
        sdk.get_object.return_value = mock_resp

        with pytest.raises(RuntimeError):
            client.download_bytes("raw", "file.bin")

        mock_resp.close.assert_called_once()
        mock_resp.release_conn.assert_called_once()


# ---------------------------------------------------------------------------
# object_exists
# ---------------------------------------------------------------------------

class TestObjectExists:
    def test_returns_true_when_object_exists(self, client, sdk):
        sdk.stat_object.return_value = MagicMock()

        assert client.object_exists("raw", "test.json") is True

    def test_returns_false_on_no_such_key(self, client, sdk):
        sdk.stat_object.side_effect = _make_s3_error("NoSuchKey")

        assert client.object_exists("raw", "missing.json") is False

    def test_reraises_other_s3_errors(self, client, sdk):
        sdk.stat_object.side_effect = _make_s3_error("AccessDenied")

        with pytest.raises(S3Error):
            client.object_exists("raw", "test.json")

    def test_calls_stat_object_with_correct_args(self, client, sdk):
        sdk.stat_object.return_value = MagicMock()

        client.object_exists("raw", "youtube/2024-01-01/search.json")

        sdk.stat_object.assert_called_once_with("raw", "youtube/2024-01-01/search.json")


# ---------------------------------------------------------------------------
# list_objects
# ---------------------------------------------------------------------------

class TestListObjects:
    def test_raises_bucket_not_found_when_bucket_missing(self, client, sdk):
        sdk.bucket_exists.return_value = False

        with pytest.raises(BucketNotFoundError):
            list(client.list_objects("raw"))

    def test_yields_object_names(self, client, sdk):
        sdk.bucket_exists.return_value = True
        sdk.list_objects.return_value = iter([
            _make_list_item("youtube/2024-01-01/search_p1.json"),
            _make_list_item("youtube/2024-01-01/search_p2.json"),
        ])

        result = list(client.list_objects("raw"))

        assert result == [
            "youtube/2024-01-01/search_p1.json",
            "youtube/2024-01-01/search_p2.json",
        ]

    def test_passes_prefix_to_sdk(self, client, sdk):
        sdk.bucket_exists.return_value = True
        sdk.list_objects.return_value = iter([])

        list(client.list_objects("raw", prefix="youtube/2024-01-01/"))

        sdk.list_objects.assert_called_once_with(
            "raw", prefix="youtube/2024-01-01/", recursive=True
        )

    def test_default_prefix_is_empty_string(self, client, sdk):
        sdk.bucket_exists.return_value = True
        sdk.list_objects.return_value = iter([])

        list(client.list_objects("raw"))

        sdk.list_objects.assert_called_once_with("raw", prefix="", recursive=True)

    def test_empty_bucket_yields_nothing(self, client, sdk):
        sdk.bucket_exists.return_value = True
        sdk.list_objects.return_value = iter([])

        result = list(client.list_objects("raw"))

        assert result == []

    def test_recursive_flag_is_always_true(self, client, sdk):
        """Nested key paths like youtube/date/file.json need recursive=True."""
        sdk.bucket_exists.return_value = True
        sdk.list_objects.return_value = iter([])

        list(client.list_objects("raw", prefix="youtube/"))

        assert sdk.list_objects.call_args.kwargs["recursive"] is True


# ---------------------------------------------------------------------------
# delete_object
# ---------------------------------------------------------------------------

class TestDeleteObject:
    def test_raises_bucket_not_found_when_bucket_missing(self, client, sdk):
        sdk.bucket_exists.return_value = False

        with pytest.raises(BucketNotFoundError):
            client.delete_object("raw", "test.json")

    def test_calls_remove_object_with_correct_args(self, client, sdk):
        sdk.bucket_exists.return_value = True

        client.delete_object("raw", "youtube/2024-01-01/search.json")

        sdk.remove_object.assert_called_once_with("raw", "youtube/2024-01-01/search.json")

    def test_reraises_s3_error_from_remove_object(self, client, sdk):
        sdk.bucket_exists.return_value = True
        sdk.remove_object.side_effect = _make_s3_error("AccessDenied")

        with pytest.raises(S3Error):
            client.delete_object("raw", "test.json")

    def test_does_not_call_remove_when_bucket_missing(self, client, sdk):
        sdk.bucket_exists.return_value = False

        with pytest.raises(BucketNotFoundError):
            client.delete_object("raw", "test.json")

        sdk.remove_object.assert_not_called()
