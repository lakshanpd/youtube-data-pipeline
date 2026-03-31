"""Integration tests for infrastructure/logging/client.py."""

from datetime import datetime, timezone

from infrastructure.logging.client import PipelineLogger


def test_pipeline_logger_writes_file_and_contains_context(tmp_path):
    with PipelineLogger(run_id="test_run_001", phase="ingestion", log_dir=tmp_path) as logger:
        logger.info("This is an info message.")
        logger.warning("This is a warning message.")
        logger.error("This is an error message.")

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_file = tmp_path / f"{date_str}.log"
    assert log_file.exists()

    content = log_file.read_text(encoding="utf-8")
    assert "test_run_001" in content
    assert "ingestion" in content
    assert "This is an info message." in content
    assert "This is a warning message." in content
    assert "This is an error message." in content


def test_pipeline_logger_appends_multiple_runs_same_day(tmp_path):
    with PipelineLogger(run_id="test_run_001", phase="ingestion", log_dir=tmp_path) as logger:
        logger.info("first run")

    with PipelineLogger(run_id="test_run_002", phase="processing", log_dir=tmp_path) as logger:
        logger.info("second run")

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_file = tmp_path / f"{date_str}.log"
    content = log_file.read_text(encoding="utf-8")

    assert "test_run_001" in content
    assert "test_run_002" in content
    assert "ingestion" in content
    assert "processing" in content
