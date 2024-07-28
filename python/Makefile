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

.DEFAULT_GOAL := help

VENV := venv
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
    pip install pre-commit
    pre-commit install

.PHONY: build
build: setup ## Build Python binding of delta-rs
	$(info --- Build Python binding ---)
	maturin build $(MATURIN_EXTRA_ARGS)

.PHONY: develop
develop: setup ## Install Python binding of delta-rs
	$(info --- Develop with Python binding ---)
	maturin develop --extras=devel,pandas $(MATURIN_EXTRA_ARGS)

.PHONY: check-rust
check-rust: ## Run check on Rust
	$(info --- Check Rust clippy ---)
	cargo clippy --all-targets --all-features --workspace -- -D warnings
	$(info --- Check Rust format ---)
	cargo fmt --all -- --check

.PHONY: check-python
check-python: ## Run check on Python
	$(info --- Check Python code quality ---)
	ruff .
	ruff format .
	mypy .

.PHONY: test-rust
test-rust: ## Run tests on Rust
	$(info --- Run Rust tests ---)
	cargo test --no-fail-fast --all-targets --all-features --workspace

.PHONY: test-python
test-python: ## Run tests on Python
	$(info --- Run Python tests ---)
	pytest -s
