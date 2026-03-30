"""Tests for src/ingestion/extractor.py."""
import pytest

from src.ingestion.extractor import (
    extract_video_ids,
    extract_next_page_token,
    extract_video_records,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _search_item(video_id: str, kind: str = "youtube#video") -> dict:
    return {
        "kind": "youtube#searchResult",
        "etag": "etag",
        "id": {"kind": kind, "videoId": video_id},
    }


def _search_response(items: list[dict], next_page_token: str | None = None) -> dict:
    resp = {"kind": "youtube#searchListResponse", "items": items}
    if next_page_token:
        resp["nextPageToken"] = next_page_token
    return resp


def _video_response(item_ids: list[str]) -> dict:
    return {
        "kind": "youtube#videoListResponse",
        "items": [{"kind": "youtube#video", "id": vid} for vid in item_ids],
    }


# ---------------------------------------------------------------------------
# extract_video_ids
# ---------------------------------------------------------------------------

class TestExtractVideoIds:
    def test_extracts_ids_from_valid_response(self):
        items = [_search_item("vid1"), _search_item("vid2"), _search_item("vid3")]
        result = extract_video_ids(_search_response(items))
        assert result == ["vid1", "vid2", "vid3"]

    def test_preserves_order(self):
        items = [_search_item("c"), _search_item("a"), _search_item("b")]
        result = extract_video_ids(_search_response(items))
        assert result == ["c", "a", "b"]

    def test_filters_non_video_items(self):
        items = [
            _search_item("vid1", kind="youtube#video"),
            _search_item("ch1", kind="youtube#channel"),
            _search_item("pl1", kind="youtube#playlist"),
            _search_item("vid2", kind="youtube#video"),
        ]
        result = extract_video_ids(_search_response(items))
        assert result == ["vid1", "vid2"]

    def test_empty_items_list(self):
        result = extract_video_ids(_search_response([]))
        assert result == []

    def test_missing_items_key(self):
        result = extract_video_ids({"kind": "youtube#searchListResponse"})
        assert result == []

    def test_single_item(self):
        result = extract_video_ids(_search_response([_search_item("only_one")]))
        assert result == ["only_one"]


# ---------------------------------------------------------------------------
# extract_next_page_token
# ---------------------------------------------------------------------------

class TestExtractNextPageToken:
    def test_returns_token_when_present(self):
        result = extract_next_page_token(_search_response([], next_page_token="CAUQAA"))
        assert result == "CAUQAA"

    def test_returns_none_when_absent(self):
        result = extract_next_page_token(_search_response([]))
        assert result is None

    def test_returns_none_on_empty_dict(self):
        result = extract_next_page_token({})
        assert result is None


# ---------------------------------------------------------------------------
# extract_video_records
# ---------------------------------------------------------------------------

class TestExtractVideoRecords:
    def test_returns_items_list(self):
        response = _video_response(["vid1", "vid2"])
        result = extract_video_records(response)
        assert len(result) == 2
        assert result[0]["id"] == "vid1"
        assert result[1]["id"] == "vid2"

    def test_empty_items(self):
        result = extract_video_records(_video_response([]))
        assert result == []

    def test_missing_items_key(self):
        result = extract_video_records({"kind": "youtube#videoListResponse"})
        assert result == []

    def test_preserves_full_item_structure(self):
        response = {
            "items": [
                {
                    "kind": "youtube#video",
                    "id": "vid1",
                    "snippet": {"title": "Test"},
                    "statistics": {"viewCount": "500"},
                    "contentDetails": {"duration": "PT3M"},
                }
            ]
        }
        records = extract_video_records(response)
        assert records[0]["statistics"]["viewCount"] == "500"
        assert records[0]["contentDetails"]["duration"] == "PT3M"
