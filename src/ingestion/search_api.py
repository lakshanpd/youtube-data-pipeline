"""YouTube Search API client."""
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class QuotaExceededError(Exception):
    """Raised when the YouTube API daily quota is exhausted."""


class SearchAPIClient:
    """Wraps the YouTube Data API v3 search.list endpoint."""

    def __init__(self, api_key: str, config: dict, service=None):
        """
        Args:
            api_key:  YouTube Data API key.
            config:   The ``youtube.search`` section of config.yaml.
            service:  Pre-built googleapiclient service (injected for tests).
        """
        self._config = config
        self._service = service or build("youtube", "v3", developerKey=api_key)

    def search(
        self,
        query: str,
        published_after: str,
        published_before: str,
        page_token: str | None = None,
    ) -> dict:
        """Call search.list and return the raw API response.

        Args:
            query:           Search keyword string.
            published_after: ISO 8601 datetime, e.g. ``2024-01-01T00:00:00Z``.
            published_before: ISO 8601 datetime, e.g. ``2024-01-01T23:59:59Z``.
            page_token:      Continuation token for paginated results.

        Returns:
            Raw ``youtube#searchListResponse`` dict.

        Raises:
            QuotaExceededError: On HTTP 403 (quota exhausted).
            HttpError:          On any other API error.
        """
        params = {
            "part": self._config["parts"],
            "q": query,
            "type": "video",
            "regionCode": self._config["region_code"],
            "publishedAfter": published_after,
            "publishedBefore": published_before,
            "maxResults": self._config["max_results_per_page"],
            "order": self._config["order"],
        }
        if page_token:
            params["pageToken"] = page_token

        logger.debug("Calling search.list", extra={"query": query, "page_token": page_token})

        try:
            return self._service.search().list(**params).execute()
        except HttpError as exc:
            if exc.resp.status == 403:
                raise QuotaExceededError("YouTube API quota exceeded") from exc
            raise

