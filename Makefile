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

.PHONY: setup
setup: install-pipenv install-requirements ## set up dev environment

.PHONY: install-requirements
install-requirements: ## install requirements
	pipenv install --dev

.PHONY: install-pipenv
install-pipenv: ## install pipenv
	pip install pipenv
# TODO: think about if it makes sense to have pipenv here


########################
# Test Targets
########################

.PHONY: test
test: docs-build test-unit test-integration test-end-to-end ## run all tests

.PHONY: test-unit
test-unit: ## run unit tests \
	# TODO: implement
	@echo "Not yet implemented"

.PHONY: test-integration
test-integration: ## run integration tests \
	# TODO: implement
	@echo "Not yet implemented"

.PHONY: test-end-to-end
test-end-to-end: ## run end-to-end tests \
	## TODO: implement
	@echo "Not yet implemented"



########################
# Documentation Targets
########################

.PHONY: docs-build
docs-build: ## build the docs
	@pipenv run tests/mkdocs build

.PHONY: docs-run
docs-run: ## run the docs locally
	@pipenv run tests/mkdocs serve



########################
# Help Target
########################

.PHONY: help
help: ## this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

.DEFAULT_GOAL := help
