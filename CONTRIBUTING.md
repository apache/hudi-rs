<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Contributing to hudi-rs

Welcome to the Apache Hudi community! We appreciate your interest in contributing to this open-source data lake
platform. This guide will walk you through the process of making your first contribution.

## File an issue

Testing and reporting bugs are also valueable contributions. Please follow
the [issue template](https://github.com/apache/hudi-rs/issues/new?template=bug_report.yml) to file bug reports.

## Prepare for development

- Install Rust, e.g. as described [here](https://doc.rust-lang.org/cargo/getting-started/installation.html)
- Have a compatible Python version installed (check [`python/pyproject.toml`](./python/pyproject.toml) for current
  requirement)

## Commonly used dev commands

For most of the time, use dev commands specified in [`python/Makefile`](./python/Makefile), it applies to both Python
and Rust modules. You don't need to `cd` to the root directory and run `cargo` commands.

To setup python virtual env, run

```shell
make setup-venv
```

> [!NOTE]
> This will run `python` command to setup the virtual environment. You can either change that to `python3.X`,
> or simply alias `python` to your local `python3.X` installation, for example:
> ```shell
> echo "alias python=/Library/Frameworks/Python.framework/Versions/3.12/bin/python3" >> ~/.zshrc`
> ```

Once activate virtual env, build the project for development by

```shell
make develop
```

This will install `hudi` dependency built from your local repo to the virtual env.

## Run tests locally


For Rust,

```shell
# for all tests
make test-rust
# or
cargo test --workspace

# for all tests in a crate / package
cargo test -p hudi-core

# for a specific test case
cargo test -p hudi-core table::tests::hudi_table_get_schema
```

For Python,

```shell
# for all tests
make test-python
# or
pytest -s

# for a specific test case
pytest tests/test_table_read.py -s -k "test_sample_table"
```

## Before creating a pull request

Run check command and follow the suggestions to fix the code.

```shell
make check-rust
```

## Create a pull request

### Title

The pull request title must follow the format outlined in
the [conventional commits spec](https://www.conventionalcommits.org). [Conventional commits](https://www.conventionalcommits.org)
is a standardized format for commit messages, and also allows us to auto-generate change logs and release notes. Since
only the `main` branch requires this format, and we always squash commits and then merge the PR, incremental commits'
messages
do not need to conform to it.

### Code coverage

We use [codecov](https://app.codecov.io/github/apache/hudi-rs) to generate code coverage report and enforce net positive
coverage change for PRs, with a 5% lenacy.

## Code of Conduct

We expect all community members to follow
our [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).
