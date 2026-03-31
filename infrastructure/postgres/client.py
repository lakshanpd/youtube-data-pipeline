"""PostgreSQL data catalog client.

Tracks pipeline runs, datasets, data batches, and field-level schemas.
Uses psycopg2 with RealDictCursor so every row is returned as a plain dict.

Usage::

    from infrastructure.postgres.client import CatalogClient

    with CatalogClient(config["postgres"]) as catalog:
        run_id = catalog.start_pipeline_run()
        dataset_id = catalog.register_dataset(
            name="youtube_search_raw",
            layer="raw",
            bucket="raw",
            source="youtube_search_api",
        )
        batch_id = catalog.start_batch(dataset_id, run_id, "youtube/2024-01-01/search_p1.json")
        catalog.complete_batch(batch_id, status="completed", record_count=48)
        catalog.complete_pipeline_run(run_id, status="completed")
"""
import logging
from contextlib import contextmanager
from typing import Iterator

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class CatalogClient:
    """Manages pipeline metadata in PostgreSQL.

    Designed as a context manager so the connection is always closed::

        with CatalogClient(config["postgres"]) as catalog:
            ...

    Or managed explicitly::

        catalog = CatalogClient(config["postgres"])
        catalog.connect()
        ...
        catalog.close()
    """

    def __init__(self, config: dict):
        """
        Args:
            config: The ``postgres`` section of ``config.yaml``, e.g.::

                host:     localhost
                port:     5432
                dbname:   youtube_pipeline
                user:     pipeline
                password: pipeline
        """
        self._config = config
        self._conn = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self) -> "CatalogClient":
        """Open the database connection.

        Returns:
            self, so ``client.connect()`` is chainable.
        """
        self._conn = psycopg2.connect(
            host=self._config["host"],
            port=self._config["port"],
            dbname=self._config["dbname"],
            user=self._config["user"],
            password=self._config["password"],
        )
        logger.debug(
            "Connected to PostgreSQL | host=%s | db=%s",
            self._config["host"], self._config["dbname"],
        )
        return self

    def close(self) -> None:
        """Close the database connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.debug("PostgreSQL connection closed")

    def __enter__(self) -> "CatalogClient":
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @contextmanager
    def _cursor(self) -> Iterator[RealDictCursor]:
        """Transaction-scoped cursor.

        Commits on success, rolls back on any exception, and always closes
        the cursor.
        """
        cur = self._conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cur
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        finally:
            cur.close()

    # ------------------------------------------------------------------
    # pipeline_runs
    # ------------------------------------------------------------------

    def start_pipeline_run(self) -> int:
        """Insert a new pipeline run with status ``running``.

        Returns:
            Auto-incremented ``pipeline_runs.id``.
        """
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (status, started_at)
                VALUES ('running', NOW())
                RETURNING id
                """,
            )
            run_id: int = cur.fetchone()["id"]
        logger.info("Pipeline run started | run_id=%d", run_id)
        return run_id

    def complete_pipeline_run(self, run_id: int, status: str) -> None:
        """Set the final status and ``completed_at`` timestamp for a run.

        Args:
            run_id: The pipeline run id returned by :meth:`start_pipeline_run`.
            status: One of ``'completed'`` or ``'failed'``.
        """
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_runs
                SET status = %s, completed_at = NOW()
                WHERE id = %s
                """,
                (status, run_id),
            )
        logger.info("Pipeline run finished | run_id=%d | status=%s", run_id, status)

    def get_pipeline_run(self, run_id: int) -> dict | None:
        """Fetch a pipeline run by id.

        Returns:
            Row dict or ``None`` if not found.
        """
        with self._cursor() as cur:
            cur.execute("SELECT * FROM pipeline_runs WHERE id = %s", (run_id,))
            row = cur.fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # datasets
    # ------------------------------------------------------------------

    def register_dataset(
        self, name: str, layer: str, bucket: str, source: str
    ) -> str:
        """Insert a dataset or return the UUID of the existing one.

        Safe to call on every pipeline run — if a dataset with the same
        ``name`` already exists the insert is a no-op and the existing UUID
        is returned.

        Args:
            name:   Human-readable unique name, e.g. ``'youtube_search_raw'``.
            layer:  One of ``'raw'``, ``'processed'``, ``'curated'``.
            bucket: MinIO bucket name.
            source: Data source, e.g. ``'youtube_search_api'``.

        Returns:
            UUID string of the (new or existing) dataset.
        """
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO datasets (name, layer, bucket, source)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (name) DO NOTHING
                RETURNING id
                """,
                (name, layer, bucket, source),
            )
            row = cur.fetchone()
            if row:
                # freshly inserted
                dataset_id = str(row["id"])
            else:
                # already existed — fetch the existing UUID
                cur.execute("SELECT id FROM datasets WHERE name = %s", (name,))
                dataset_id = str(cur.fetchone()["id"])
        logger.info(
            "Dataset registered | name=%s | id=%s | layer=%s", name, dataset_id, layer
        )
        return dataset_id

    def get_dataset(self, dataset_id: str) -> dict | None:
        """Fetch a dataset by UUID.

        Returns:
            Row dict or ``None`` if not found.
        """
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM datasets WHERE id = %s::uuid", (dataset_id,)
            )
            row = cur.fetchone()
        return dict(row) if row else None

    def get_dataset_by_name(self, name: str) -> dict | None:
        """Fetch a dataset by name.

        Returns:
            Row dict or ``None`` if not found.
        """
        with self._cursor() as cur:
            cur.execute("SELECT * FROM datasets WHERE name = %s", (name,))
            row = cur.fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # data_batches
    # ------------------------------------------------------------------

    def start_batch(
        self, dataset_id: str, pipeline_run_id: int, path: str
    ) -> int:
        """Insert a new data batch with status ``running``.

        Args:
            dataset_id:      UUID of the parent dataset.
            pipeline_run_id: Id of the parent pipeline run.
            path:            MinIO object key, e.g.
                             ``'youtube/2024-01-01/search_p1.json'``.

        Returns:
            Auto-incremented ``data_batches.id``.
        """
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_batches
                    (dataset_id, pipeline_run_id, path, status, created_at)
                VALUES (%s::uuid, %s, %s, 'running', NOW())
                RETURNING id
                """,
                (dataset_id, pipeline_run_id, path),
            )
            batch_id: int = cur.fetchone()["id"]
        logger.info(
            "Batch started | batch_id=%d | dataset_id=%s | path=%s",
            batch_id, dataset_id, path,
        )
        return batch_id

    def complete_batch(
        self, batch_id: int, status: str, record_count: int | None = None
    ) -> None:
        """Set the final status and record count for a batch.

        Args:
            batch_id:     The batch id returned by :meth:`start_batch`.
            status:       One of ``'completed'`` or ``'failed'``.
            record_count: Number of records written; ``None`` if unknown or
                          the batch failed before counting.
        """
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE data_batches
                SET status = %s, record_count = %s
                WHERE id = %s
                """,
                (status, record_count, batch_id),
            )
        logger.info(
            "Batch finished | batch_id=%d | status=%s | record_count=%s",
            batch_id, status, record_count,
        )

    def get_batch(self, batch_id: int) -> dict | None:
        """Fetch a data batch by id.

        Returns:
            Row dict or ``None`` if not found.
        """
        with self._cursor() as cur:
            cur.execute("SELECT * FROM data_batches WHERE id = %s", (batch_id,))
            row = cur.fetchone()
        return dict(row) if row else None

    def list_batches(self, pipeline_run_id: int) -> list[dict]:
        """Return all batches belonging to a pipeline run, ordered by id.

        Args:
            pipeline_run_id: Id of the pipeline run.

        Returns:
            List of row dicts (empty list if none found).
        """
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM data_batches WHERE pipeline_run_id = %s ORDER BY id",
                (pipeline_run_id,),
            )
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # schemas
    # ------------------------------------------------------------------

    def register_schema(self, dataset_id: str, fields: list[dict]) -> None:
        """Upsert field definitions for a dataset.

        Each item in ``fields`` must have ``field_name`` and ``field_type``
        keys.  Existing fields are updated; new ones are inserted.

        Args:
            dataset_id: UUID of the dataset.
            fields:     e.g. ``[{"field_name": "video_id", "field_type": "TEXT"}]``.
        """
        with self._cursor() as cur:
            for field in fields:
                cur.execute(
                    """
                    INSERT INTO schemas (dataset_id, field_name, field_type)
                    VALUES (%s::uuid, %s, %s)
                    ON CONFLICT (dataset_id, field_name)
                    DO UPDATE SET field_type = EXCLUDED.field_type
                    """,
                    (dataset_id, field["field_name"], field["field_type"]),
                )
        logger.info(
            "Schema registered | dataset_id=%s | field_count=%d",
            dataset_id, len(fields),
        )

    def get_schema(self, dataset_id: str) -> list[dict]:
        """Return all field definitions for a dataset, ordered by field name.

        Args:
            dataset_id: UUID of the dataset.

        Returns:
            List of ``{"field_name": ..., "field_type": ...}`` dicts.
        """
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT field_name, field_type
                FROM schemas
                WHERE dataset_id = %s::uuid
                ORDER BY field_name
                """,
                (dataset_id,),
            )
            rows = cur.fetchall()
        return [dict(row) for row in rows]

# integration test for CatalogClient
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()
    config = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "dbname": os.getenv("POSTGRES_DB", "youtube_pipeline"),
        "user": os.getenv("POSTGRES_USER", "pipeline"),
        "password": os.getenv("POSTGRES_PASSWORD", "pipeline"),
    }

    with CatalogClient(config) as catalog:
        # pipeline run
        run_id = catalog.start_pipeline_run()
        print("Pipeline run started with id:", run_id)
        catalog.complete_pipeline_run(run_id, status="completed")
        print("Pipeline run after completion:", catalog.get_pipeline_run(run_id))

        # dataset
        dataset_id = catalog.register_dataset(
            name="test_dataset",
            layer="raw",
            bucket="raw",
            source="test_source",
        )
        print("Dataset registered with id:", dataset_id)
        print("Dataset details:", catalog.get_dataset(dataset_id))

        # batch
        batch_id = catalog.start_batch(dataset_id, run_id, "test/path.json")
        print("Batch started with id:", batch_id)
        catalog.complete_batch(batch_id, status="completed", record_count=42)
        print("Batch details:", catalog.get_batch(batch_id))

        # schema
        catalog.register_schema(dataset_id, [
            {"field_name": "video_id", "field_type": "TEXT"},
            {"field_name": "title", "field_type": "TEXT"},
        ])
        print("Schema for dataset:", catalog.get_schema(dataset_id))