"""Tests for src/ingestion/orchestrator.py."""
import pytest
from unittest.mock import MagicMock, call

from src.ingestion.orchestrator import IngestionOrchestrator
from src.ingestion.search_api import QuotaExceededError


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

ORCH_CONFIG = {
    "search": {
        "keywords": ["keyword_a", "keyword_b"],
        "max_videos_per_run": 100,
    },
    "videos": {
        "max_ids_per_request": 50,
    },
}


def _search_response(video_ids: list[str], next_page_token: str | None = None) -> dict:
    items = [
        {"kind": "youtube#searchResult", "id": {"kind": "youtube#video", "videoId": vid}}
        for vid in video_ids
    ]
    resp = {"kind": "youtube#searchListResponse", "items": items}
    if next_page_token:
        resp["nextPageToken"] = next_page_token
    return resp


def _video_response(video_ids: list[str]) -> dict:
    return {
        "kind": "youtube#videoListResponse",
        "items": [{"kind": "youtube#video", "id": vid} for vid in video_ids],
    }


def _make_orchestrator(
    search_side_effect=None,
    video_side_effect=None,
    config: dict = ORCH_CONFIG,
) -> IngestionOrchestrator:
    search_client = MagicMock()
    video_client = MagicMock()

    if search_side_effect is not None:
        search_client.search.side_effect = search_side_effect
    if video_side_effect is not None:
        video_client.get_video_details.side_effect = video_side_effect

    return IngestionOrchestrator(search_client, video_client, config), search_client, video_client


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestIngestionOrchestratorRun:
    def test_returns_summary_keys(self):
        orch, sc, vc = _make_orchestrator(
            search_side_effect=[_search_response(["v1"]), _search_response(["v2"])],
            video_side_effect=[_video_response(["v1"]), _video_response(["v2"])],
        )
        summary = orch.run("2024-01-01")

        assert {"run_id", "date", "status", "total_video_ids",
                "search_batches", "video_batches",
                "search_responses", "video_responses", "video_ids"} == set(summary.keys())

    def test_collects_video_ids_across_keywords(self):
        orch, sc, vc = _make_orchestrator(
            search_side_effect=[
                _search_response(["v1", "v2"]),   # keyword_a, page 1
                _search_response(["v3", "v4"]),   # keyword_b, page 1
            ],
            video_side_effect=[
                _video_response(["v1", "v2"]),
                _video_response(["v3", "v4"]),
            ],
        )
        summary = orch.run("2024-01-01")

        assert summary["video_ids"] == ["v1", "v2", "v3", "v4"]
        assert summary["total_video_ids"] == 4
        assert summary["status"] == "completed"

    def test_deduplicates_video_ids_across_keywords(self):
        # v2 appears in both keyword results
        orch, sc, vc = _make_orchestrator(
            search_side_effect=[
                _search_response(["v1", "v2"]),
                _search_response(["v2", "v3"]),
            ],
            video_side_effect=[
                _video_response(["v1", "v2"]),
                _video_response(["v3"]),   # v2 not re-fetched
            ],
        )
        summary = orch.run("2024-01-01")

        assert summary["video_ids"] == ["v1", "v2", "v3"]
        assert summary["total_video_ids"] == 3

    def test_paginates_within_keyword(self):
        orch, sc, vc = _make_orchestrator(
            search_side_effect=[
                _search_response(["v1"], next_page_token="PAGE2"),  # keyword_a page 1
                _search_response(["v2"]),                            # keyword_a page 2
                _search_response(["v3"]),                            # keyword_b page 1
            ],
            video_side_effect=[
                _video_response(["v1"]),
                _video_response(["v2"]),
                _video_response(["v3"]),
            ],
        )
        summary = orch.run("2024-01-01")

        assert summary["video_ids"] == ["v1", "v2", "v3"]
        # search was called 3 times: keyword_a p1, keyword_a p2, keyword_b p1
        assert summary["search_batches"] == 3

    def test_passes_page_token_to_second_page(self):
        orch, sc, vc = _make_orchestrator(
            search_side_effect=[
                _search_response(["v1"], next_page_token="NEXT"),
                _search_response(["v2"]),
                _search_response([]),  # keyword_b
            ],
            video_side_effect=[
                _video_response(["v1"]),
                _video_response(["v2"]),
            ],
        )
        orch.run("2024-01-01")

        calls = sc.search.call_args_list
        # First call: no page_token
        assert calls[0].kwargs.get("page_token") is None
        # Second call: page_token = "NEXT"
        assert calls[1].kwargs["page_token"] == "NEXT"

    def test_uses_correct_date_range(self):
        orch, sc, vc = _make_orchestrator(
            search_side_effect=[_search_response([]), _search_response([])],
            video_side_effect=[],
        )
        orch.run("2024-06-15")

        first_call = sc.search.call_args_list[0].kwargs
        assert first_call["published_after"] == "2024-06-15T00:00:00Z"
        assert first_call["published_before"] == "2024-06-15T23:59:59Z"

    def test_stops_when_max_videos_reached(self):
        config = {
            "search": {"keywords": ["kw_a", "kw_b"], "max_videos_per_run": 2},
            "videos": {"max_ids_per_request": 50},
        }
        orch, sc, vc = _make_orchestrator(
            search_side_effect=[_search_response(["v1", "v2", "v3"])],
            video_side_effect=[_video_response(["v1", "v2", "v3"])],
            config=config,
        )
        summary = orch.run("2024-01-01")

        # Only keyword_a was processed; keyword_b skipped due to limit
        assert sc.search.call_count == 1
        assert summary["status"] == "completed"

    def test_stops_on_quota_exceeded(self):
        orch, sc, vc = _make_orchestrator(
            search_side_effect=QuotaExceededError("quota"),
        )
        summary = orch.run("2024-01-01")

        assert summary["status"] == "quota_exceeded"
        assert summary["total_video_ids"] == 0

    def test_quota_exceeded_after_partial_collection(self):
        orch, sc, vc = _make_orchestrator(
            search_side_effect=[
                _search_response(["v1", "v2"]),   # keyword_a succeeds
                QuotaExceededError("quota"),        # keyword_b fails
            ],
            video_side_effect=[_video_response(["v1", "v2"])],
        )
        summary = orch.run("2024-01-01")

        assert summary["status"] == "quota_exceeded"
        assert summary["video_ids"] == ["v1", "v2"]
        assert summary["total_video_ids"] == 2

    def test_batches_video_ids_exceeding_max_per_request(self):
        config = {
            "search": {"keywords": ["kw"], "max_videos_per_run": 200},
            "videos": {"max_ids_per_request": 3},
        }
        video_ids = [f"v{i}" for i in range(7)]
        orch, sc, vc = _make_orchestrator(
            search_side_effect=[_search_response(video_ids)],
            video_side_effect=[
                _video_response(video_ids[0:3]),
                _video_response(video_ids[3:6]),
                _video_response(video_ids[6:7]),
            ],
            config=config,
        )
        summary = orch.run("2024-01-01")

        # 7 IDs split into batches of 3 → 3 video API calls
        assert vc.get_video_details.call_count == 3
        assert summary["video_batches"] == 3
        assert summary["total_video_ids"] == 7

    def test_summary_includes_raw_responses(self):
        search_resp = _search_response(["v1"])
        video_resp = _video_response(["v1"])
        orch, sc, vc = _make_orchestrator(
            search_side_effect=[search_resp, _search_response([])],
            video_side_effect=[video_resp],
        )
        summary = orch.run("2024-01-01")

        assert search_resp in summary["search_responses"]
        assert video_resp in summary["video_responses"]

    def test_run_id_is_unique_per_call(self):
        # Each run iterates over 2 keywords → provide 2 empty responses per run
        orch, sc, vc = _make_orchestrator(
            search_side_effect=[
                _search_response([]),  # run 1, keyword_a
                _search_response([]),  # run 1, keyword_b
                _search_response([]),  # run 2, keyword_a
                _search_response([]),  # run 2, keyword_b
            ],
            video_side_effect=[],
        )
        s1 = orch.run("2024-01-01")
        s2 = orch.run("2024-01-02")

        assert s1["run_id"].endswith("Z")
        assert s2["run_id"].endswith("Z")

    def test_empty_results_for_all_keywords(self):
        orch, sc, vc = _make_orchestrator(
            search_side_effect=[_search_response([]), _search_response([])],
            video_side_effect=[],
        )
        summary = orch.run("2024-01-01")

        assert summary["video_ids"] == []
        assert summary["total_video_ids"] == 0
        assert summary["video_batches"] == 0
        assert summary["status"] == "completed"
