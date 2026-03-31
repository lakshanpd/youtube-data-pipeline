"""Integration tests for VideoAPIClient using the live YouTube API."""

import os

import pytest
from dotenv import load_dotenv

from src.ingestion.extractor import extract_video_ids
from src.ingestion.search_api import SearchAPIClient
from src.ingestion.video_api import VideoAPIClient


@pytest.fixture(scope="module")
def api_key() -> str:
    load_dotenv()
    key = os.getenv("YOUTUBE_API_KEY")
    if not key:
        pytest.skip("YOUTUBE_API_KEY not set; skipping live integration tests")
    return key


@pytest.fixture(scope="module")
def clients(api_key: str) -> tuple[SearchAPIClient, VideoAPIClient]:
    search_config = {
        "parts": "id,snippet",
        "region_code": "US",
        "max_results_per_page": 5,
        "order": "date",
    }
    video_config = {
        "parts": "snippet,statistics,contentDetails",
        "max_ids_per_request": 50,
    }
    return SearchAPIClient(api_key, search_config), VideoAPIClient(api_key, video_config)


@pytest.fixture(scope="module")
def video_details(clients: tuple[SearchAPIClient, VideoAPIClient]) -> tuple[list[str], dict]:
    search_client, video_client = clients
    response = search_client.search(
        query="python programming",
        published_after="2024-01-01T00:00:00Z",
        published_before="2024-01-31T23:59:59Z",
    )
    video_ids = extract_video_ids(response)
    if not video_ids:
        pytest.skip("No video IDs returned for the current live query window")

    details = video_client.get_video_details(video_ids)
    return video_ids, details


def test_get_video_details_returns_items(video_details: tuple[list[str], dict]):
    requested_ids, details = video_details

    assert isinstance(details, dict)
    assert "items" in details
    assert isinstance(details["items"], list)
    assert len(details["items"]) <= len(requested_ids)


def test_get_video_details_contains_requested_parts(video_details: tuple[list[str], dict]):
    _, details = video_details
    if not details["items"]:
        pytest.skip("No video records returned from live details call")

    first = details["items"][0]
    assert "snippet" in first
    assert "statistics" in first
    assert "contentDetails" in first
