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

## Starter issues

If you are new to the project, we recommend starting with issues listed
in https://github.com/apache/hudi-rs/contribute.

## File an issue

Testing and reporting bugs are also valuable contributions. Please follow
the [issue template](https://github.com/apache/hudi-rs/issues/new?template=bug_report.yml) to file bug reports.

## Issue tracking

All issues tagged for a release can be found in the corresponding milestone page,
see https://github.com/apache/hudi-rs/milestones.

Features, bugs, and `p0` issues that are targeting the next release can be found in
this [project view](https://github.com/orgs/apache/projects/356/views/4). Pull requests won't be tracked in the project
view, instead, they will be linked to the corresponding issues.

## Prepare for development

- Install Rust, e.g. as described [here](https://doc.rust-lang.org/cargo/getting-started/installation.html)
- Install uv, the fast Python package manager, as described [here](https://docs.astral.sh/uv/getting-started/installation/)
- Have a compatible Python version installed (check [`python/pyproject.toml`](./python/pyproject.toml) for current
  requirement)

## Commonly used dev commands

For most of the time, use dev commands specified in the [`Makefile`](Makefile).

> [!NOTE]
> This project uses [uv](https://github.com/astral-sh/uv) as the Python package manager for faster dependency resolution and installation. All Python-related commands in the Makefile have been configured to use uv.

To setup python virtual env, run

```shell
cd python
make setup-venv
```

> [!NOTE]
> This will use uv to set up the virtual environment in `.venv/`.
> Activate the virtual environment by running `source .venv/bin/activate` for example.

Once a virtual environment is activated, build the project for development by

```shell
make develop
```

This will install `hudi` dependency built from your local repo to the virtual env.

## Run tests locally

For Rust,

```shell
# For all tests
make test-rust
# or
cargo test --workspace

# For all tests in a crate / package
cargo test -p hudi-core

# For a specific test case
cargo test -p hudi-core table::tests::hudi_table_get_schema
```

For Python,

```shell
# For all tests
make test-python
# or
pytest -s python/tests

# For a specific test case
pytest python/tests/test_table_read.py -s -k "test_read_table_has_correct_schema"
```

## Debugging on VSCode

Debugging is a crucial part of developing/maintaining the project. This tutorial will guide you through setting up Visual Studio Code for debugging hudi-rs using the CodeLLDB extension. Assuming you have Visual Studio Code installed:

1. Download the CodeLLDB VSCode extension.

2. Open the **hudi-rs** project in VSCode.

3. Add a `.launch` file (seen below) in your `.vscode` (if it does not appear in your root directory, consult [here] (https://code.visualstudio.com/docs/editor/debugging#_launch-configurations)):

<details><summary><code><b>launch.json</b></code></summary>

    ```json
    {
        "configurations": [
            {
                "name": "Debug Rust/Python",
                "type": "debugpy",
                "request": "launch",
                "program": "${workspaceFolder}/tools/attach_debugger.py",
                "args": [
                    "${file}"
                ],
                "console": "internalConsole",
                "serverReadyAction": {
                    "pattern": "pID = ([0-9]+)",
                    "action": "startDebugging",
                    "name": "Rust LLDB"
                }
            },
            {
                "name": "Rust LLDB",
                "pid": "0",
                "type": "lldb",
                "request": "attach",
                "program": "${command:python.interpreterPath}",
                "sourceLanguages": [
                    "rust"
                ],
            }
        ]
    }
    ```

    </details>

### Using the Debugger

1. Create a Python file in your python environment which imports code from the hudi module. 

2. On the left of VSCode, there should be '**Run and Debug**' option. At the top-left of your screen, 
you can select '**Debug Rust/Python**' in the dropdown options.

Breakpoints can be added in the code to pinpoint debugging instances

## Before creating a pull request

Run the below command and fix issues if any:

```shell
make format check test
```

## Create a pull request

When submitting a pull request, please follow these guidelines:

1. **Title Format**: The pull request title must follow the format outlined in
   the [conventional commits spec](https://www.conventionalcommits.org). This is a standardized format for commit
   messages, and also allows us to auto-generate change logs and release notes. Since only the `main` branch requires
   this format, and we always squash commits and then merge the PR, incremental commits' messages do not need to conform
   to it.
2. **Line Count**: A general guideline is to keep the PR's diff, i.e., max(added lines, deleted lines), **less than 1000
   lines**. Keeping PRs concise makes it easier for reviewers to thoroughly examine changes without experiencing
   fatigue. If your changes exceed this limit, consider breaking them down into smaller, logical PRs that address
   specific aspects of the feature or bug fix.
3. **Coverage Requirements**: All new features and bug fixes **must** include appropriate unit tests to ensure
   functionality and prevent regressions. Tests should cover both typical use cases and edge cases. Ensure that new
   tests pass locally before submitting the PR.
4. **Code Comments**: Properly designed APIs and code should be self-explanatory and make in-code comments redundant. In
   case that complex logic or non-obvious implementations are absolutely unavoidable, please add comments to explain the
   code's purpose and behavior.

### Code coverage

We use [codecov](https://app.codecov.io/github/apache/hudi-rs) to generate code coverage report and enforce code 
coverage rate. See [codecov.yml](./codecov.yml) for the configuration.

## Learning

To help with contributing to the project, please explore [Hudi's documentation](https://hudi.apache.org/docs/overview)
for further learning.

## Code of Conduct

We expect all community members to follow
our [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).
