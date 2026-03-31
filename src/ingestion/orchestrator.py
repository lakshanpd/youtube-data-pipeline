"""Orchestrates the YouTube data ingestion pipeline."""
import logging
from datetime import datetime, timezone

from .search_api import SearchAPIClient, QuotaExceededError
from .video_api import VideoAPIClient
from .extractor import extract_video_ids, extract_next_page_token, extract_video_records

logger = logging.getLogger(__name__)


class IngestionOrchestrator:
    """Drives the full ingestion loop: search → extract IDs → fetch video details.

    Storage and catalog updates are intentionally out of scope; the caller
    receives all raw responses in the returned summary dict.
    """

    def __init__(
        self,
        search_client: SearchAPIClient,
        video_client: VideoAPIClient,
        config: dict,
        catalog_client: object | None = None,
    ):
        """
        Args:
            search_client: Configured ``SearchAPIClient`` instance.
            video_client:  Configured ``VideoAPIClient`` instance.
            config:        Full ``youtube`` section of config.yaml.
            catalog_client: Optional ``CatalogClient`` for metadata tracking.
        """
        self._search = search_client
        self._video = video_client
        self._cfg = config
        self._catalog = catalog_client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, date: str) -> dict:
        """Run ingestion for a single calendar date.

        Iterates over every configured keyword and paginates through search
        results until ``max_videos_per_run`` is reached or all pages are
        exhausted.  Stops immediately if the API quota is exceeded.

        Args:
            date: Target date in ``YYYY-MM-DD`` format.

        Returns:
            Summary dict with keys:
              - ``run_id``          – unique run identifier (UTC timestamp)
              - ``date``            – the date that was ingested
              - ``status``          – ``"completed"`` or ``"quota_exceeded"``
              - ``total_video_ids`` – number of unique IDs collected
              - ``search_batches``  – count of search API calls made
              - ``video_batches``   – count of videos API calls made
              - ``search_responses``– list of raw search API response dicts
              - ``video_responses`` – list of raw videos API response dicts
              - ``video_ids``       – deduplicated list of collected video IDs
        """
        published_after = f"{date}T00:00:00Z"
        published_before = f"{date}T23:59:59Z"
        keywords: list[str] = self._cfg["search"]["keywords"]
        max_videos: int = self._cfg["search"]["max_videos_per_run"]
        max_ids_per_request: int = self._cfg["videos"]["max_ids_per_request"]

        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        logger.info("Ingestion run started | run_id=%s | date=%s", run_id, date)

        all_video_ids: list[str] = []
        search_responses: list[dict] = []
        video_responses: list[dict] = []
        status = "completed"
        catalog_run_id: int | None = None
        catalog_dataset_id: str | None = None
        unexpected_error = False

        if self._catalog:
            catalog_run_id = self._catalog.start_pipeline_run()
            catalog_cfg = self._cfg.get("catalog", {})
            catalog_dataset_id = self._catalog.register_dataset(
                name=catalog_cfg.get("dataset_name", "youtube_ingestion_raw"),
                layer=catalog_cfg.get("layer", "raw"),
                bucket=catalog_cfg.get("bucket", "raw"),
                source=catalog_cfg.get("source", "youtube_data_api"),
            )

        try:
            for keyword in keywords:
                if len(all_video_ids) >= max_videos:
                    logger.info(
                        "max_videos_per_run reached, stopping | run_id=%s | total=%d",
                        run_id, len(all_video_ids),
                    )
                    break

                page_token: str | None = None

                while True:
                    # --- Search ---
                    logger.info(
                        "Fetching search results | run_id=%s | keyword=%r | page_token=%s",
                        run_id, keyword, page_token,
                    )
                    search_response = self._search.search(
                        query=keyword,
                        published_after=published_after,
                        published_before=published_before,
                        page_token=page_token,
                    )
                    search_responses.append(search_response)
                    if self._catalog and catalog_run_id is not None and catalog_dataset_id is not None:
                        search_batch_id = self._catalog.start_batch(
                            catalog_dataset_id,
                            catalog_run_id,
                            f"youtube/{date}/search_{keyword}_{len(search_responses)}.json",
                        )
                        self._catalog.complete_batch(
                            search_batch_id,
                            status="completed",
                            record_count=len(search_response.get("items", [])),
                        )

                    # --- Extract & deduplicate IDs ---
                    new_ids = [
                        vid for vid in extract_video_ids(search_response)
                        if vid not in all_video_ids
                    ]
                    all_video_ids.extend(new_ids)
                    logger.info(
                        "Search batch done | run_id=%s | keyword=%r | new_ids=%d | total=%d",
                        run_id, keyword, len(new_ids), len(all_video_ids),
                    )

                    # --- Fetch video details in batches ---
                    for i in range(0, len(new_ids), max_ids_per_request):
                        batch = new_ids[i : i + max_ids_per_request]
                        logger.info(
                            "Fetching video details | run_id=%s | batch_size=%d",
                            run_id, len(batch),
                        )
                        video_response = self._video.get_video_details(batch)
                        video_responses.append(video_response)
                        if self._catalog and catalog_run_id is not None and catalog_dataset_id is not None:
                            video_batch_id = self._catalog.start_batch(
                                catalog_dataset_id,
                                catalog_run_id,
                                f"youtube/{date}/videos_{keyword}_{len(video_responses)}.json",
                            )
                            self._catalog.complete_batch(
                                video_batch_id,
                                status="completed",
                                record_count=len(extract_video_records(video_response)),
                            )
                        logger.info(
                            "Video details fetched | run_id=%s | records=%d",
                            run_id, len(extract_video_records(video_response)),
                        )

                    # --- Pagination / limit check ---
                    page_token = extract_next_page_token(search_response)
                    if not page_token or len(all_video_ids) >= max_videos:
                        break

        except QuotaExceededError:
            status = "quota_exceeded"
            logger.warning(
                "Quota exceeded, stopping early | run_id=%s | total_collected=%d",
                run_id, len(all_video_ids),
            )
        except Exception:
            unexpected_error = True
            raise
        finally:
            if self._catalog and catalog_run_id is not None:
                pipeline_status = "completed" if status == "completed" and not unexpected_error else "failed"
                self._catalog.complete_pipeline_run(catalog_run_id, status=pipeline_status)

        summary = {
            "run_id": run_id,
            "date": date,
            "status": status,
            "total_video_ids": len(all_video_ids),
            "search_batches": len(search_responses),
            "video_batches": len(video_responses),
            "search_responses": search_responses,
            "video_responses": video_responses,
            "video_ids": all_video_ids,
        }
        logger.info(
            "Ingestion run finished | run_id=%s | status=%s | total_video_ids=%d "
            "| search_batches=%d | video_batches=%d",
            run_id, status, summary["total_video_ids"],
            summary["search_batches"], summary["video_batches"],
        )
        return summary
    
