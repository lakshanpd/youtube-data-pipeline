"""YouTube Videos API client."""
from http import client
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from src.ingestion.extractor import extract_video_ids

from .search_api import QuotaExceededError, SearchAPIClient

logger = logging.getLogger(__name__)


class VideoAPIClient:
    """Wraps the YouTube Data API v3 videos.list endpoint."""

    def __init__(self, api_key: str, config: dict, service=None):
        """
        Args:
            api_key:  YouTube Data API key.
            config:   The ``youtube.videos`` section of config.yaml.
            service:  Pre-built googleapiclient service (injected for tests).
        """
        self._config = config
        self._service = service or build("youtube", "v3", developerKey=api_key)

    def get_video_details(self, video_ids: list[str]) -> dict:
        """Fetch full details for a batch of video IDs.

        The caller is responsible for splitting large ID lists into batches
        of at most ``config["max_ids_per_request"]`` entries.

        Args:
            video_ids: YouTube video IDs to fetch (max 50 per call).

        Returns:
            Raw ``youtube#videoListResponse`` dict.

        Raises:
            ValueError:         If ``video_ids`` exceeds ``max_ids_per_request``.
            QuotaExceededError: On HTTP 403 (quota exhausted).
            HttpError:          On any other API error.
        """
        max_batch = self._config["max_ids_per_request"]
        if len(video_ids) > max_batch:
            raise ValueError(
                f"video_ids length {len(video_ids)} exceeds max_ids_per_request {max_batch}. "
                "Split into smaller batches before calling."
            )

        ids_str = ",".join(video_ids)
        logger.debug("Calling videos.list", extra={"id_count": len(video_ids)})

        try:
            return (
                self._service.videos()
                .list(
                    part=self._config["parts"],
                    id=ids_str,
                )
                .execute()
            )
        except HttpError as exc:
            if exc.resp.status == 403:
                raise QuotaExceededError("YouTube API quota exceeded") from exc
            raise

# integration test for VideoAPIClient
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()
    api_key = os.getenv("YOUTUBE_API_KEY")
    search_config = {
        "parts": "id,snippet",
        "region_code": "US",
        "max_results_per_page": 1,
        "order": "date",
    }
    video_config = {
        "parts": "snippet,statistics,contentDetails",
        "max_ids_per_request": 50,
    }
    search_client = SearchAPIClient(api_key, search_config)
    video_client = VideoAPIClient(api_key, video_config)
    response = search_client.search(
        query="python programming",
        published_after="2024-01-01T00:00:00Z",
        published_before="2024-01-31T23:59:59Z",
    )
    video_ids = extract_video_ids(response)
    video_details = video_client.get_video_details(video_ids)
    print("video_details:", video_details)