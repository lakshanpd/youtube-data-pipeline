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

## 1. Create virtual environment
.PHONY: venv
venv:				
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	@echo "Run 'source .venv/bin/activate' to activate the environment"

## 2. Install all dependencies into the active venv
.PHONY: install
install:			
	$(PIP) install -r requirements.txt

## 3. Create venv and install dependencies in one step
.PHONY: setup
setup: venv install		

## 4. Activate the virtual environment
.PHONY: activate
activate:
	@echo "Run 'source .venv/bin/activate' to activate the virtual environment"

## 5. Deactivate the virtual environment
.PHONY: deactivate
deactivate:
	@echo "Run 'deactivate' to exit the virtual environment"

# ============================================================
# Tests
# ============================================================

## 1. Run all tests
.PHONY: test
test:				
	$(PYTEST) tests/ -v

## 2. Run ingestion tests only
.PHONY: test-ingestion
unit-test-ingestion:			
	$(PYTEST) tests/unit/ingestion/ -v

## 3. Run MinIO client tests only
.PHONY: test-minio
unit-test-minio:			
	$(PYTEST) tests/unit/infrastructure/minio/ -v

## 4. Run all tests, stop on first failure
.PHONY: test-fast
unit-test-fast:			
	$(PYTEST) tests/unit/ -v -x

# ============================================================
# Infrastructure (Docker)
# ============================================================

## 1. Start all Docker services
.PHONY: up
up:				
	docker compose up -d

## 2. Stop all Docker services
.PHONY: down
down:				
	docker compose down

## 3. Restart all Docker services
.PHONY: restart
restart: down up		

## 4. Tail logs for all running services
.PHONY: logs
logs:				
	docker compose logs -f

## 5. Tail MinIO logs only
.PHONY: logs-minio
logs-minio:			
	docker compose logs -f minio

## 6. Show status of all Docker services
.PHONY: ps
ps:				
	docker compose ps

## 7. Start a shell session inside the Postgres container
.PHONY: postgres
postgres:
	docker compose exec postgres psql -U pipeline -d youtube_pipeline

# ============================================================
# Utilities
# ============================================================

## Remove Python cache files
.PHONY: clean
clean:				
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -name "*.pyc" -delete
