"""Integration tests for extractor helpers using a live YouTube search call."""

import os

import pytest
from dotenv import load_dotenv

from src.ingestion.extractor import extract_next_page_token, extract_video_ids
from src.ingestion.search_api import SearchAPIClient


@pytest.fixture(scope="module")
def api_key() -> str:
    """Return API key from environment or skip integration tests."""
    load_dotenv()
    key = os.getenv("YOUTUBE_API_KEY")
    if not key:
        pytest.skip("YOUTUBE_API_KEY not set; skipping live integration tests")
    return key


@pytest.fixture(scope="module")
def search_response(api_key: str) -> dict:
    """Fetch one real search response for extractor assertions."""
    client = SearchAPIClient(
        api_key,
        {
            "parts": "id,snippet",
            "region_code": "US",
            "max_results_per_page": 5,
            "order": "date",
        },
    )
    return client.search(
        query="python programming",
        published_after="2024-01-01T00:00:00Z",
        published_before="2024-01-31T23:59:59Z",
    )


def test_extract_video_ids_from_live_search(search_response: dict):
    ids = extract_video_ids(search_response)

    assert isinstance(ids, list)
    assert len(ids) > 0
    assert all(isinstance(video_id, str) and video_id for video_id in ids)


def test_extract_next_page_token_from_live_search(search_response: dict):
    token = extract_next_page_token(search_response)

    assert token is None or isinstance(token, str)