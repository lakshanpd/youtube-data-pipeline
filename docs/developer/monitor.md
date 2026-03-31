# Pipeline Monitoring Guide

This guide shows how to monitor pipeline activity in real time from the terminal.

## 1. Recommended terminal layout

Use 4 terminals while running the pipeline:

1. Pipeline runner
2. Logs watcher
3. PostgreSQL watcher
4. MinIO watcher

Run the pipeline from project root:

	python3 -m src.ingestion.orchestrator

## 2. Monitor Docker services

Check service status:
	docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'

Watch container resource usage:
	docker stats

Follow logs per container:
	docker logs -f youtube_pipeline_postgres
	docker logs -f youtube_pipeline_minio

## 3. Monitor pipeline logs

## 4. Monitor PostgreSQL data catalog

Open PostgreSQL shell in container:
	docker exec -it youtube_pipeline_postgres psql -U pipeline -d youtube_pipeline

Inside psql, use these checks.

See pipeline runs:
	SELECT id, status, started_at, completed_at
	FROM pipeline_runs;

See datasets:
	SELECT id, name, layer, bucket, source
	FROM datasets;

See batches:
	SELECT id, pipeline_run_id, dataset_id, path, status, record_count
	FROM data_batches;

## 5. Monitor MinIO objects from terminal
