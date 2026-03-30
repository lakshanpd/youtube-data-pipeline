"""Pure extraction helpers for raw YouTube API responses."""


import json

from src.ingestion.search_api import SearchAPIClient


def extract_video_ids(search_response: dict) -> list[str]:
    """Return all video IDs from a ``youtube#searchListResponse``.

    Filters out any items that are not ``youtube#video`` (e.g. channels,
    playlists) which can appear when the search ``type`` filter is absent.

    Args:
        search_response: Raw dict returned by ``SearchAPIClient.search()``.

    Returns:
        Ordered list of video ID strings.
    """
    return [
        item["id"]["videoId"]
        for item in search_response.get("items", [])
        if item.get("id", {}).get("kind") == "youtube#video"
    ]


def extract_next_page_token(search_response: dict) -> str | None:
    """Return the ``nextPageToken`` from a search response, or ``None``.

    Args:
        search_response: Raw dict returned by ``SearchAPIClient.search()``.
    """
    return search_response.get("nextPageToken")


def extract_video_records(video_response: dict) -> list[dict]:
    """Return the list of video item dicts from a ``youtube#videoListResponse``.

    Args:
        video_response: Raw dict returned by ``VideoAPIClient.get_video_details()``.
    """
    return video_response.get("items", [])

# integration test for extractor functions
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()
    api_key = os.getenv("YOUTUBE_API_KEY")
    config = {
        "parts": "id,snippet",
        "region_code": "US",
        "max_results_per_page": 5,
        "order": "date",
    }
    client = SearchAPIClient(api_key, config)
    response = client.search(
        query="python programming",
        published_after="2024-01-01T00:00:00Z",
        published_before="2024-01-31T23:59:59Z",
    )

    print("extract_video_ids:", extract_video_ids(response))   
    print("extract_next_page_token:", extract_next_page_token(response))   
