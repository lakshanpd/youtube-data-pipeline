"""Tests for src/ingestion/video_api.py."""
import pytest
from unittest.mock import MagicMock
from googleapiclient.errors import HttpError

from src.ingestion.video_api import VideoAPIClient
from src.ingestion.search_api import QuotaExceededError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

VIDEO_CONFIG = {
    "parts": "snippet,statistics,contentDetails",
    "max_ids_per_request": 50,
}


def _make_video_response(video_ids: list[str]) -> dict:
    return {
        "kind": "youtube#videoListResponse",
        "etag": "test_etag",
        "pageInfo": {"totalResults": len(video_ids), "resultsPerPage": 50},
        "items": [
            {
                "kind": "youtube#video",
                "etag": "item_etag",
                "id": vid,
                "snippet": {
                    "publishedAt": "2024-01-01T10:00:00Z",
                    "channelId": "UC_test",
                    "title": f"Video {vid}",
                    "channelTitle": "Test Channel",
                },
                "statistics": {
                    "viewCount": "1000",
                    "likeCount": "50",
                    "commentCount": "10",
                },
                "contentDetails": {
                    "duration": "PT5M30S",
                    "dimension": "2d",
                    "definition": "hd",
                },
            }
            for vid in video_ids
        ],
    }


def _make_http_error(status: int) -> HttpError:
    resp = MagicMock()
    resp.status = status
    return HttpError(resp=resp, content=b"API error")


def _make_client(mock_execute_return: dict) -> tuple[VideoAPIClient, MagicMock]:
    service = MagicMock()
    service.videos().list().execute.return_value = mock_execute_return
    return VideoAPIClient(api_key="fake_key", config=VIDEO_CONFIG, service=service), service


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestVideoAPIClientGetVideoDetails:
    def test_returns_raw_response(self):
        ids = ["vid1", "vid2", "vid3"]
        expected = _make_video_response(ids)
        client, _ = _make_client(expected)

        result = client.get_video_details(ids)

        assert result == expected

    def test_passes_correct_params_to_api(self):
        ids = ["abc", "def", "ghi"]
        client, service = _make_client(_make_video_response(ids))

        client.get_video_details(ids)

        call_kwargs = service.videos().list.call_args.kwargs
        assert call_kwargs["part"] == "snippet,statistics,contentDetails"
        assert call_kwargs["id"] == "abc,def,ghi"

    def test_single_video_id(self):
        client, service = _make_client(_make_video_response(["solo_vid"]))

        client.get_video_details(["solo_vid"])

        call_kwargs = service.videos().list.call_args.kwargs
        assert call_kwargs["id"] == "solo_vid"

    def test_raises_value_error_when_ids_exceed_limit(self):
        client, _ = _make_client({})
        oversized = [f"vid{i}" for i in range(51)]

        with pytest.raises(ValueError, match="max_ids_per_request"):
            client.get_video_details(oversized)

    def test_exactly_50_ids_is_allowed(self):
        ids = [f"vid{i}" for i in range(50)]
        expected = _make_video_response(ids)
        client, _ = _make_client(expected)

        result = client.get_video_details(ids)

        assert result == expected

    def test_raises_quota_exceeded_on_403(self):
        service = MagicMock()
        service.videos().list().execute.side_effect = _make_http_error(403)
        client = VideoAPIClient(api_key="fake_key", config=VIDEO_CONFIG, service=service)

        with pytest.raises(QuotaExceededError):
            client.get_video_details(["vid1"])

    def test_reraises_non_quota_http_errors(self):
        service = MagicMock()
        service.videos().list().execute.side_effect = _make_http_error(500)
        client = VideoAPIClient(api_key="fake_key", config=VIDEO_CONFIG, service=service)

        with pytest.raises(HttpError):
            client.get_video_details(["vid1"])

    def test_response_items_contain_all_parts(self):
        ids = ["vid1"]
        expected = _make_video_response(ids)
        client, _ = _make_client(expected)

        result = client.get_video_details(ids)

        item = result["items"][0]
        assert "snippet" in item
        assert "statistics" in item
        assert "contentDetails" in item
