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
