########################
# General Targets
########################

.PHONY: clean
clean: ## remove artifacts
	@rm -rf site/
	@rm -rf .mypy_cache/



########################
# Development Setup Targets
########################

.PHONY: setup-ci
setup-ci: ## install dev requirements for CI (no pipenv)
	pip install -r dev-requirements.txt

.PHONY: setup
setup: install-pipenv install-requirements ## set up dev environment

.PHONY: install-requirements
install-requirements: ## install requirements
	pipenv install --dev

.PHONY: install-pipenv
install-pipenv: ## install pipenv
	pip install pipenv

.PHONY: generate-requirements
generate-requirements: ## generate requirements.txt and dev-requirements.txt
	pipenv requirements > requirements.txt
	pipenv requirements --dev-only > dev-requirements.txt

.PHONY: outdated
outdated: ## list all outdated dependencies
	pipenv update --outdated

.PHONY: update
update: ## update all outdated dependencies
	pipenv update


########################
# Test Targets
########################

.PHONY: test
test: docs-build test-unit test-integration test-end-to-end ## run all tests

.PHONY: test-unit
test-unit: ## run unit tests
	pipenv run coverage run -m pytest tests/unit/

.PHONY: test-integration
test-integration: ## run integration tests
	pipenv run coverage run -m pytest tests/integration/

.PHONY: coverage-report
coverage-report: ## run end-to-end tests
	pipenv run coverage run -m pytest --cov --cov-report=xml



########################
# Documentation Targets
########################

.PHONY: docs-build
docs-build: ## build the docs
	@pipenv run mkdocs build

.PHONY: docs-build-ci
docs-build-ci: ## build the docs (CI = no pipenv)
	mkdocs build

.PHONY: docs-run
docs-run: ## run the docs locally
	@pipenv run mkdocs serve



########################
# Help Target
########################

.PHONY: help
help: ## this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

.DEFAULT_GOAL := help
