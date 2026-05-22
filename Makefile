# ootils-core — canonical dev commands.
# Run `make help` for a summary.

.PHONY: help install test test-unit test-integration test-fast lint lint-fix coverage pre-commit-install pre-commit run docker-up docker-down clean

PYTHON ?= python
PYTEST ?= $(PYTHON) -m pytest
PYTEST_FAST_ARGS = -q --ignore=tests/integration --ignore=tests/smoke --ignore=tests/legacy

help:           ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk -F ':.*?## ' '{printf "%-22s %s\n", $$1, $$2}'

install:        ## Install package + dev extras editable.
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e ".[dev]"

test: test-unit ## Default test target — runs unit + feature tests, skips integration.

test-unit:      ## Unit / feature tests (no DB required).
	$(PYTEST) tests/ $(PYTEST_FAST_ARGS)

test-fast: test-unit  ## Alias for test-unit.

test-integration:  ## Integration tests — requires DATABASE_URL pointing at a throwaway DB.
	@test -n "$$DATABASE_URL" || (echo "Set DATABASE_URL to a throwaway Postgres before running integration tests." && exit 1)
	$(PYTEST) tests/integration/ -q --tb=short

lint:           ## Ruff lint on src/ (CI scope).
	$(PYTHON) -m ruff check src/

lint-fix:       ## Ruff lint + auto-fix on src/ and tests/.
	$(PYTHON) -m ruff check --fix src/ tests/

coverage:       ## Unit tests with coverage report. Set COV_FAIL_UNDER (default 80).
	$(PYTEST) tests/ $(PYTEST_FAST_ARGS) \
		--cov=src/ootils_core --cov-report=term-missing --cov-fail-under=$${COV_FAIL_UNDER:-80}

pre-commit-install: ## Install the pre-commit hooks declared in .pre-commit-config.yaml.
	$(PYTHON) -m pip install pre-commit
	pre-commit install

pre-commit:     ## Run all pre-commit hooks against the whole repo.
	pre-commit run --all-files

run:            ## Run the API locally via uvicorn (reload mode).
	OOTILS_API_TOKEN=$${OOTILS_API_TOKEN:-dev-token} \
		$(PYTHON) -m uvicorn ootils_core.api.app:app --reload --host 127.0.0.1 --port 8000

docker-up:      ## Bring up Postgres + API via docker compose.
	docker compose up -d

docker-down:    ## Stop docker compose stack.
	docker compose down

clean:          ## Remove caches, coverage artifacts, build dirs.
	rm -rf .pytest_cache .ruff_cache .coverage htmlcov build dist *.egg-info
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
