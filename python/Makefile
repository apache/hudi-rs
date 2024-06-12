.DEFAULT_GOAL := help

VENV := .venv
MATURIN_VERSION := $(shell grep 'requires =' pyproject.toml | cut -d= -f2- | tr -d '[ "]')
PACKAGE_VERSION := $(shell grep version Cargo.toml | head -n 1 | awk '{print $$3}' | tr -d '"' )

.PHONY: setup-venv
setup-venv: ## Setup the virtualenv
	$(info --- Setup virtualenv ---)
	python -m venv $(VENV)

.PHONY: setup
setup: ## Setup the requirements
	$(info --- Setup dependencies ---)
	pip install "$(MATURIN_VERSION)"

.PHONY: build
build: setup ## Build Python binding of delta-rs
	$(info --- Build Python binding ---)
	maturin build $(MATURIN_EXTRA_ARGS)

.PHONY: develop
develop: setup ## Install Python binding of delta-rs
	$(info --- Develop with Python binding ---)
	maturin develop --extras=devel,pandas $(MATURIN_EXTRA_ARGS)
