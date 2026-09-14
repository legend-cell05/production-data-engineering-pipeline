# =============================================================================
# Developer entry points. `make help` lists everything.
# =============================================================================

.DEFAULT_GOAL := help
SHELL := /bin/bash

PYTHON  ?= python3
COMPOSE ?= docker compose
HELIOS  := $(PYTHON) -m helios.cli

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# --- Setup -------------------------------------------------------------------

.PHONY: install
install: ## Install the package and its development dependencies
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e ".[dev]"

.PHONY: env
env: ## Create .env from the example if it does not exist
	@test -f .env || (cp .env.example .env && echo "Created .env -- review it before running.")

# --- Quality -----------------------------------------------------------------

.PHONY: lint
lint: ## Run ruff (lint + format check)
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

.PHONY: format
format: ## Apply ruff formatting and safe fixes
	$(PYTHON) -m ruff check --fix .
	$(PYTHON) -m ruff format .

.PHONY: typecheck
typecheck: ## Run mypy on src/
	$(PYTHON) -m mypy

.PHONY: test
test: ## Run the unit tests (no database needed)
	$(PYTHON) -m pytest tests/unit -v

.PHONY: test-integration
test-integration: ## Run the full suite (requires a live PostgreSQL)
	HELIOS_RUN_INTEGRATION=1 $(PYTHON) -m pytest tests -v

.PHONY: coverage
coverage: ## Full suite with a coverage report
	HELIOS_RUN_INTEGRATION=1 $(PYTHON) -m pytest tests --cov --cov-report=term-missing

.PHONY: check
check: lint typecheck test ## Everything CI runs, minus the database

# --- Pipeline ----------------------------------------------------------------

.PHONY: seed
seed: ## Generate the simulated upstream systems
	$(HELIOS) seed

.PHONY: init-db
init-db: ## Create schemas, partitions, views and register the contracts
	$(HELIOS) init-db

.PHONY: serve
serve: ## Start the API (simulated source + observability)
	$(HELIOS) serve

.PHONY: ingest
ingest: ## Ingest every source into the raw layer
	$(HELIOS) ingest

.PHONY: run
run: ## Run the whole pipeline
	$(HELIOS) run

.PHONY: all
all: seed init-db run ## Seed, initialise and run, in order

.PHONY: watermarks
watermarks: ## Show where each source has got to
	$(HELIOS) watermarks

.PHONY: dlq
dlq: ## Show the dead-letter queue
	$(HELIOS) dlq show

.PHONY: quality
quality: ## Run the data-quality checks
	$(HELIOS) quality

.PHONY: partitions
partitions: ## List the reading partitions
	$(HELIOS) partitions

.PHONY: doctor
doctor: ## Check configuration and connectivity
	$(HELIOS) doctor

# --- Docker ------------------------------------------------------------------

.PHONY: up
up: env ## Start the full stack and run the pipeline
	$(COMPOSE) up --build

.PHONY: down
down: ## Stop the stack (keeps the database volume)
	$(COMPOSE) down

.PHONY: clean-volumes
clean-volumes: ## Stop the stack AND delete the database volume
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Follow the container logs
	$(COMPOSE) logs -f

.PHONY: psql
psql: ## Open a psql shell on the compose database
	$(COMPOSE) exec postgres psql -U $${HELIOS_DB_USER:-helios_app} -d $${HELIOS_DB_NAME:-helios}

.PHONY: docker-build
docker-build: ## Build the application image only
	docker build -t production-data-engineering-pipeline:local .

# --- Housekeeping ------------------------------------------------------------

.PHONY: clean
clean: ## Remove caches and generated data (keeps .env)
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage coverage.xml
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	rm -f data/upstream/* data/landing/*
	touch data/upstream/.gitkeep data/landing/.gitkeep
