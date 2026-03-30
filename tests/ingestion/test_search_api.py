"""Tests for src/ingestion/search_api.py."""
import pytest
from unittest.mock import MagicMock
from googleapiclient.errors import HttpError

from src.ingestion.search_api import SearchAPIClient, QuotaExceededError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SEARCH_CONFIG = {
    "parts": "snippet",
    "region_code": "LK",
    "order": "date",
    "max_results_per_page": 50,
}

# mock search API response
def _make_search_response(items: list[dict], next_page_token: str | None = None) -> dict:
    response = {
        "kind": "youtube#searchListResponse",
        "etag": "test_etag",
        "regionCode": "LK",
        "pageInfo": {"totalResults": 100, "resultsPerPage": 50},
        "items": items,
    }
    if next_page_token:
        response["nextPageToken"] = next_page_token
    return response

# mock item in search API response
def _make_video_item(video_id: str) -> dict:
    return {
        "kind": "youtube#searchResult",
        "etag": "item_etag",
        "id": {"kind": "youtube#video", "videoId": video_id},
        "snippet": {
            "publishedAt": "2024-01-01T10:00:00Z",
            "channelId": "UC_test",
            "title": f"Test Video {video_id}",
            "channelTitle": "Test Channel",
        },
    }


def _make_http_error(status: int) -> HttpError:
    resp = MagicMock()
    resp.status = status
    return HttpError(resp=resp, content=b"API error")


def _make_client(mock_execute_return: dict) -> tuple[SearchAPIClient, MagicMock]:
    """Return (client, mock_service) with search().list().execute() pre-configured."""
    service = MagicMock()
    service.search().list().execute.return_value = mock_execute_return
    return SearchAPIClient(api_key="fake_key", config=SEARCH_CONFIG, service=service), service


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSearchAPIClientSearch:
    def test_returns_raw_response(self):
        expected = _make_search_response([_make_video_item("vid1")])
        client, _ = _make_client(expected)

        result = client.search(
            query="Sri Lanka news",
            published_after="2024-01-01T00:00:00Z",
            published_before="2024-01-01T23:59:59Z",
        )

        assert result == expected

    def test_passes_correct_params_to_api(self):
        client, service = _make_client(_make_search_response([]))

        client.search(
            query="Lanka",
            published_after="2024-06-01T00:00:00Z",
            published_before="2024-06-01T23:59:59Z",
        )

        call_kwargs = service.search().list.call_args.kwargs
        assert call_kwargs["q"] == "Lanka"
        assert call_kwargs["regionCode"] == "LK"
        assert call_kwargs["publishedAfter"] == "2024-06-01T00:00:00Z"
        assert call_kwargs["publishedBefore"] == "2024-06-01T23:59:59Z"
        assert call_kwargs["maxResults"] == 50
        assert call_kwargs["type"] == "video"
        assert call_kwargs["part"] == "snippet"
        assert call_kwargs["order"] == "date"

    def test_page_token_included_when_provided(self):
        client, service = _make_client(_make_search_response([]))

        client.search(
            query="Colombo",
            published_after="2024-01-01T00:00:00Z",
            published_before="2024-01-01T23:59:59Z",
            page_token="CAUQAA",
        )

        call_kwargs = service.search().list.call_args.kwargs
        assert call_kwargs["pageToken"] == "CAUQAA"

    def test_page_token_absent_when_not_provided(self):
        client, service = _make_client(_make_search_response([]))

        client.search(
            query="Colombo",
            published_after="2024-01-01T00:00:00Z",
            published_before="2024-01-01T23:59:59Z",
        )

        call_kwargs = service.search().list.call_args.kwargs
        assert "pageToken" not in call_kwargs

    def test_raises_quota_exceeded_on_403(self):
        service = MagicMock()
        service.search().list().execute.side_effect = _make_http_error(403)
        client = SearchAPIClient(api_key="fake_key", config=SEARCH_CONFIG, service=service)

        with pytest.raises(QuotaExceededError):
            client.search(
                query="test",
                published_after="2024-01-01T00:00:00Z",
                published_before="2024-01-01T23:59:59Z",
            )

    def test_reraises_non_quota_http_errors(self):
        service = MagicMock()
        service.search().list().execute.side_effect = _make_http_error(500)
        client = SearchAPIClient(api_key="fake_key", config=SEARCH_CONFIG, service=service)

        with pytest.raises(HttpError):
            client.search(
                query="test",
                published_after="2024-01-01T00:00:00Z",
                published_before="2024-01-01T23:59:59Z",
            )

    def test_response_with_next_page_token(self):
        expected = _make_search_response(
            [_make_video_item("vid1")], next_page_token="NEXT_PAGE"
        )
        client, _ = _make_client(expected)

        result = client.search(
            query="Sri Lanka",
            published_after="2024-01-01T00:00:00Z",
            published_before="2024-01-01T23:59:59Z",
        )

        assert result["nextPageToken"] == "NEXT_PAGE"

    def test_empty_results(self):
        expected = _make_search_response([])
        client, _ = _make_client(expected)

        result = client.search(
            query="obscure query",
            published_after="2024-01-01T00:00:00Z",
            published_before="2024-01-01T23:59:59Z",
        )

        assert result["items"] == []
