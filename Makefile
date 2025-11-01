# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

SHELL := /bin/bash

.DEFAULT_GOAL := help

# Check if uv is installed
UV_CHECK := $(shell command -v uv 2> /dev/null)
ifndef UV_CHECK
$(error "uv is not installed. Please install it first: curl -LsSf https://astral.sh/uv/install.sh | sh")
endif

VENV := .venv
PYTHON_DIR = python
MATURIN_VERSION := $(shell grep 'requires =' $(PYTHON_DIR)/pyproject.toml | cut -d= -f2- | tr -d '[ "]')
PACKAGE_VERSION := $(shell grep version Cargo.toml | head -n 1 | awk '{print $$3}' | tr -d '"' )

.PHONY: setup-venv
setup-venv: ## Setup the virtualenv
	$(info --- Setup virtualenv ---)
	uv venv $(VENV)

.PHONY: setup
setup: ## Setup the requirements
	$(info --- Setup dependencies ---)
	uv pip install "$(MATURIN_VERSION)"

.PHONY: build
build: setup ## Build Python binding of hudi-rs
	$(info --- Build Python binding ---)
	./build-wrapper.sh maturin build --features datafusion $(MATURIN_EXTRA_ARGS) -m $(PYTHON_DIR)/Cargo.toml

.PHONY: develop
develop: setup ## Install Python binding of hudi-rs
	$(info --- Develop with Python binding ---)
	./build-wrapper.sh maturin develop --extras=devel,datafusion --features datafusion $(MATURIN_EXTRA_ARGS) -m $(PYTHON_DIR)/Cargo.toml

.PHONY: format
format: format-rust format-python ## Format Rust and Python code

.PHONY: format-rust
format-rust: ## Format Rust code
	$(info --- Format Rust code ---)
	./build-wrapper.sh cargo fmt --all

.PHONY: format-python
format-python: ## Format Python code
	$(info --- Format Python code ---)
	ruff format $(PYTHON_DIR)

.PHONY: check
check: check-rust check-python ## Run check on Rust and Python

.PHONY: check-rust
check-rust: ## Run check on Rust
	$(info --- Check Rust clippy ---)
	./build-wrapper.sh cargo clippy --all-targets --all-features --workspace --no-deps -- -D warnings
	$(info --- Check Rust format ---)
	./build-wrapper.sh cargo fmt --all -- --check

.PHONY: check-python
check-python: ## Run check on Python
	$(info --- Check Python format ---)
	ruff format --check --diff $(PYTHON_DIR)
	$(info --- Check Python linting ---)
	ruff check $(PYTHON_DIR)
	$(info --- Check Python typing ---)
	pushd $(PYTHON_DIR); mypy .; popd

.PHONY: test
test: test-rust test-python ## Run tests on Rust and Python

.PHONY: test-rust
test-rust: ## Run tests on Rust
	$(info --- Run Rust tests ---)
	./build-wrapper.sh cargo test --no-fail-fast --all-targets --all-features --workspace

.PHONY: test-python
test-python: ## Run tests on Python
	$(info --- Run Python tests ---)
	uv run pytest -s $(PYTHON_DIR)
