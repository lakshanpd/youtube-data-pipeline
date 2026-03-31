"""Integration tests for IngestionOrchestrator using the live YouTube API."""

import os

import pytest
from dotenv import load_dotenv

from src.ingestion.orchestrator import IngestionOrchestrator
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
def summary(api_key: str) -> dict:
    search_config = {
        "parts": "id,snippet",
        "region_code": "US",
        "max_results_per_page": 5,
        "order": "date",
        "keywords": ["python programming"],
        "max_videos_per_run": 10,
    }
    video_config = {
        "parts": "snippet,statistics,contentDetails",
        "max_ids_per_request": 5,
    }
    search_client = SearchAPIClient(api_key, search_config)
    video_client = VideoAPIClient(api_key, video_config)
    orchestrator = IngestionOrchestrator(
        search_client,
        video_client,
        {"search": search_config, "videos": video_config},
    )
    return orchestrator.run(date="2024-01-01")


def test_orchestrator_returns_expected_summary_shape(summary: dict):
    expected_keys = {
        "run_id",
        "date",
        "status",
        "total_video_ids",
        "search_batches",
        "video_batches",
        "search_responses",
        "video_responses",
        "video_ids",
    }
    assert set(summary.keys()) == expected_keys
    assert summary["date"] == "2024-01-01"
    assert summary["status"] in {"completed", "quota_exceeded"}


def test_orchestrator_summary_counts_are_consistent(summary: dict):
    assert isinstance(summary["video_ids"], list)
    assert summary["total_video_ids"] == len(summary["video_ids"])
    assert summary["search_batches"] == len(summary["search_responses"])
    assert summary["video_batches"] == len(summary["video_responses"])

    if summary["status"] == "completed":
        assert summary["total_video_ids"] <= 10
        