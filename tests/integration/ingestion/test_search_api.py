"""Integration tests for SearchAPIClient using the live YouTube API."""

import os

import pytest
from dotenv import load_dotenv

from src.ingestion.search_api import SearchAPIClient


@pytest.fixture(scope="module")
def api_key() -> str:
    load_dotenv()
    key = os.getenv("YOUTUBE_API_KEY")
    if not key:
        pytest.skip("YOUTUBE_API_KEY not set; skipping live integration tests")
    return key


@pytest.fixture(scope="module")
def client(api_key: str) -> SearchAPIClient:
    config = {
        "parts": "id,snippet",
        "region_code": "US",
        "max_results_per_page": 5,
        "order": "date",
    }
    return SearchAPIClient(api_key, config)


@pytest.fixture(scope="module")
def search_response(client: SearchAPIClient) -> dict:
    return client.search(
        query="python programming",
        published_after="2024-01-01T00:00:00Z",
        published_before="2024-01-31T23:59:59Z",
    )


def test_search_returns_response_shape(search_response: dict):
    assert isinstance(search_response, dict)
    assert "items" in search_response
    assert isinstance(search_response["items"], list)


def test_search_returns_video_results_only(search_response: dict):
    if not search_response["items"]:
        pytest.skip("No items returned for the current live query window")

    for item in search_response["items"]:
        assert item["id"]["kind"] == "youtube#video"


def test_next_page_token_type_is_valid(search_response: dict):
    token = search_response.get("nextPageToken")
    assert token is None or isinstance(token, str)
