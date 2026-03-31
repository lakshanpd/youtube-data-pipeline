"""Integration tests for infrastructure/postgres/client.py."""

import os
from datetime import datetime, timezone

import pytest
from dotenv import load_dotenv

from infrastructure.postgres.client import CatalogClient


@pytest.fixture(scope="module")
def catalog() -> CatalogClient:
    load_dotenv()
    config = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "dbname": os.getenv("POSTGRES_DB", "youtube_pipeline"),
        "user": os.getenv("POSTGRES_USER", "pipeline"),
        "password": os.getenv("POSTGRES_PASSWORD", "pipeline"),
    }

    try:
        with CatalogClient(config) as client:
            yield client
    except Exception as exc:
        pytest.skip(f"PostgreSQL is not reachable: {exc}")


def test_pipeline_run_lifecycle(catalog: CatalogClient):
    run_id = catalog.start_pipeline_run()
    assert isinstance(run_id, int)

    catalog.complete_pipeline_run(run_id, status="completed")
    run = catalog.get_pipeline_run(run_id)

    assert run is not None
    assert run["id"] == run_id
    assert run["status"] == "completed"


def test_dataset_batch_and_schema_flow(catalog: CatalogClient):
    suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    dataset_name = f"test_dataset_{suffix}"

    run_id = catalog.start_pipeline_run()

    dataset_id = catalog.register_dataset(
        name=dataset_name,
        layer="raw",
        bucket="raw",
        source="test_source",
    )
    assert isinstance(dataset_id, str)

    dataset = catalog.get_dataset(dataset_id)
    assert dataset is not None
    assert dataset["name"] == dataset_name

    batch_id = catalog.start_batch(dataset_id, run_id, "test/path.json")
    assert isinstance(batch_id, int)

    catalog.complete_batch(batch_id, status="completed", record_count=42)
    batch = catalog.get_batch(batch_id)
    assert batch is not None
    assert batch["status"] == "completed"
    assert batch["record_count"] == 42

    catalog.register_schema(
        dataset_id,
        [
            {"field_name": "video_id", "field_type": "TEXT"},
            {"field_name": "title", "field_type": "TEXT"},
        ],
    )
    schema = catalog.get_schema(dataset_id)
    schema_map = {row["field_name"]: row["field_type"] for row in schema}

    assert schema_map["video_id"] == "TEXT"
    assert schema_map["title"] == "TEXT"
