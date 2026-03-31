# Developer Setup Guide

This document explains how to set up and run the project locally as a developer.

## 1. Prerequisites

Install these tools first:

1. Python 3.11
2. Docker Desktop (with Docker Compose)
3. Make
4. Git

Verify installations:

```bash
python3.11 --version
docker --version
docker compose version
make --version
```

## 2. Clone and enter the project

```bash
git clone <your-repo-url>
cd youtube-data-pipeline
```

## 3. Configure environment variables

Create local env file from template:

```bash
cp .env.example .env
```

Update at minimum:

1. YOUTUBE_API_KEY
2. MINIO_ROOT_USER
3. MINIO_ROOT_PASSWORD
4. POSTGRES_HOST
5. POSTGRES_PORT
6. POSTGRES_DB
7. POSTGRES_USER
8. POSTGRES_PASSWORD

Default local database port is 5433 on host.

## 4. Create Python environment and install dependencies

Use the Make targets:

```bash
make setup
source .venv/bin/activate
```

Or manually:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 5. Start infrastructure services

Start MinIO and PostgreSQL:

```bash
make up
```

Check service status:

```bash
make ps
```

Expected containers:

1. youtube_pipeline_minio
2. youtube_pipeline_postgres

## 6. Verify infrastructure connectivity

### MinIO verification

Open MinIO console:

1. URL: http://localhost:9001
2. Login with MINIO_ROOT_USER / MINIO_ROOT_PASSWORD from .env

### PostgreSQL verification

Open psql shell:

```bash
make postgres
```

Run a quick check inside psql:

```sql
SELECT NOW();
\dt
```

## 7. Run tests

Run all tests:

```bash
make test
```

Run ingestion unit tests only:

```bash
make unit-test-ingestion
```

Run MinIO unit tests only:

```bash
make unit-test-minio
```

Run integration tests:

```bash
pytest tests/integration -v
```

## 8. Run the ingestion pipeline

From project root:

```bash
python3 -m src.ingestion.orchestrator
```

This will:

1. Query YouTube Search API
2. Fetch video details
3. Track run and batch metadata in PostgreSQL catalog

## 9. Useful developer commands

Start services:

```bash
make up
```

Stop services:

```bash
make down
```

Restart services:

```bash
make restart
```

Follow infra logs:

```bash
make logs
```

Follow MinIO logs only:

```bash
make logs-minio
```

Clean Python cache files:

```bash
make clean
```

## 10. Troubleshooting

Python command not found:

1. Ensure Python 3.11 is installed
2. Use python3.11 explicitly for venv creation

pytest not found:

1. Activate virtual environment: source .venv/bin/activate
2. Reinstall deps: pip install -r requirements.txt

PostgreSQL connection issues:

1. Ensure container is running: make ps
2. Confirm .env values for POSTGRES_*
3. Ensure host port 5433 is not blocked

MinIO connection/auth issues:

1. Ensure container is running: make ps
2. Check credentials in .env
3. Verify console is reachable at localhost:9001

YouTube API failures:

1. Confirm YOUTUBE_API_KEY is set in .env
2. Check quota usage in Google Cloud Console

## 11. Monitoring during runs

Use the monitoring guide for real-time checks:

See [docs/developer/monitor.md](docs/developer/monitor.md).
