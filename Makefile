# ============================================================
# Configuration
# ============================================================

PYTHON     := python3.11
VENV       := .venv
VENV_BIN   := $(VENV)/bin
PIP        := $(VENV_BIN)/pip
PYTEST     := $(VENV_BIN)/pytest

# ============================================================
# Environment
# ============================================================

.PHONY: venv
venv:				## Create virtual environment
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	@echo "Run 'source .venv/bin/activate' to activate the environment"

.PHONY: install
install:			## Install all dependencies into the active venv
	$(PIP) install -r requirements.txt

.PHONY: setup
setup: venv install		## Create venv and install dependencies in one step

# ============================================================
# Tests
# ============================================================

.PHONY: test
test:				## Run all tests
	$(PYTEST) tests/ -v

.PHONY: test-ingestion
unit-test-ingestion:			## Run ingestion tests only
	$(PYTEST) tests/unit/ingestion/ -v

.PHONY: test-minio
unit-test-minio:			## Run MinIO client tests only
	$(PYTEST) tests/unit/infrastructure/minio/ -v

.PHONY: test-fast
unit-test-fast:			## Run all tests, stop on first failure
	$(PYTEST) tests/unit/ -v -x

# ============================================================
# Infrastructure (Docker)
# ============================================================

.PHONY: up
up:				## Start all Docker services
	docker compose up -d

.PHONY: down
down:				## Stop all Docker services
	docker compose down

.PHONY: restart
restart: down up		## Restart all Docker services

.PHONY: logs
logs:				## Tail logs for all running services
	docker compose logs -f

.PHONY: logs-minio
logs-minio:			## Tail MinIO logs only
	docker compose logs -f minio

.PHONY: ps
ps:				## Show status of all Docker services
	docker compose ps

# ============================================================
# Utilities
# ============================================================

.PHONY: clean
clean:				## Remove Python cache files
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -name "*.pyc" -delete

.PHONY: help
help:				## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
