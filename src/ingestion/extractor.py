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
