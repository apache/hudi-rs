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

# Changelog

All notable changes to this project will be documented in this file.

## [0.5.0] - 2026-09-10

### 🚀 Features

- Add `get_file_slices_splits_between` API (#411) by @yunchipang
- Expose datafusion custom table provider to python (#412) by @brayanjuls
- Remove table version 5 support (#409) by @yunchipang
- Support picking up storage options from `HOODIE_ENV_` env vars (#410) by @yunchipang
- Add table v8+ timeline support for loading old and new table versions (#395) by @codope
- Add hfile reader (#490) by @xushiyan
- Support reading log files from `files` partition in metadata table (#492) by @xushiyan
- Add API to read file slice from base file and list of log files (#446) by @yunchipang
- Support reading metadata table (files) (#499) by @xushiyan
- Implement table v8 file slicing for read path (#501) by @xushiyan
- Add streaming table and file group APIs (#508) by @xushiyan
- Support column project and row filter pushdown (#510) by @xushiyan
- Support column stats pruning (#516) by @xushiyan
- Modernize Table API with async-only design (#537) by @xushiyan
- Support IN and NOT IN operators in partition filter (#566) by @yunchipang
- Export `HudiTableConfig` and `HudiReadConfig` as python enums (#433) by @yunchipang
- Unify Hudi read APIs around ReadOptions and add Python bindings (#587) by @xushiyan
- *(core)* Enrich FileSlice with stats and streamline config flow (#592) by @xushiyan
- *(core)* Add general config alias support with ordering fields as primary config (#598) by @xushiyan
- *(core)* Add lance base file format support (#596) by @xushiyan
- *(datafusion)* Support Lance scan and broaden MOR snapshot e2e coverage (#608) by @xushiyan
- *(core)* Select the file group reader by version, defaulting to 2 (#677) by @linliu-code
- *(core)* Serve merge-on-read reads with file group reader version 2 (#660) by @linliu-code
- *(core)* Read the metadata table as Arrow, and expose hudi-rs over a C ABI for JVM callers (#700) by @linliu-code
- *(reader)* Merge-aware pushdown gate, row-group pruning, and read-volume counters (#708) by @linliu-code
- *(core)* Serve Hudi metadata tables through reader v2, sharded and memory-bounded (#706) by @linliu-code
- *(deps)* Upgrade to arrow 58, DataFusion 54, object_store 0.13, and Lance 11 (#711) by @yihua
- *(benchmark)* Add AWS EC2 setup for the TPC-H harness (#718) by @yihua
- *(release)* Verify the artifacts a release actually published (#759) by @yihua

### 🐛 Bug Fixes

- Handle non partition in MDT files partition (#505) by @xushiyan
- Avoid dashmap deadlock for correlated queries (#539) by @xushiyan
- Make timestamp keygen work for partition pruning (#462) by @yunchipang
- Order partition schema by config and error on missing fields (#581) by @sushiljacksparrow
- Stop .gitignore from swallowing test fixtures (#676) by @linliu-code
- *(core)* Gate the log-block scan on instant state (#678) by @linliu-code
- *(core)* Fold partial updates in both ordering directions (#679) by @linliu-code
- *(core)* Keep the custom merger's own columns in the required schema (#704) by @linliu-code
- *(python)* Compile the datafusion feature into every python build (#714) by @yihua
- *(release)* Align the release guide and scripts with their actual behavior (#717) by @yihua
- *(release)* Set the macOS deployment target for the x86_64 wheel build (#722) by @yihua
- *(benchmark)* Stop the parquet run overwriting the Hudi benchmark results (#747) by @yihua
- *(core)* Decline base-read pushdown when a predicate reads a repaired column (#748) by @linliu-code
- *(ci)* Install a modern protoc for the manylinux wheel build (#751) by @yihua
- *(release)* Reject an expired signing key in the src release verification (#753) by @yihua
- *(ci)* Publish the release only after every artifact has built (#755) by @yihua
- *(python)* Declare the readme so the published package has a description (#762) by @yihua

### 🚜 Refactor

- Rename variable names in splitting table APIs (#424) by @yunchipang
- Replace custom `.to::()` with `into()` for `HudiConfig` conversions (#432) by @yunchipang
- Move .crc filtering logic from table to storage layer (#458) by @yunchipang
- Define avro model for parsing commit metadata (#477) by @xushiyan
- Add targeted MDT lookup APIs for partition pruning (#502) by @xushiyan
- Consolidate file metadata population for snapshot and incremental queries (#586) by @xushiyan
- *(core)* Move parquet io into base file reader (#599) by @xushiyan

### 📚 Documentation

- Update release guide (#287) by @xushiyan
- Add AGENTS.md as canonical AI coding agent guide (#591) by @xushiyan
- Deduplicate AI agent guidance with sub-directory AGENTS.md (#594) by @xushiyan
- Correct read config scoping and MOR streaming, document reader v2 knobs (#709) by @yihua
- Fix stale API claims in README, reader spec, and agent guides (#716) by @yihua

### ⚡ Performance

- *(core)* Skip decoding out-of-range log blocks and add unit test (#420) by @codope
- Setup tpch benchmark infra (#538) by @xushiyan
- Improve datafusion integration (#548) by @xushiyan
- *(core)* Copy an HFile record rather than its whole block per record (#697) by @linliu-code
- *(core)* Bound the merge chunk rather than moving the merge off the executor (#689) by @linliu-code

### 🛠️ Build

- Use `uv` for Python package management (#391) by @codephage2020
- *(deps)* Bump pytest from 8.3.5 to 8.4.1 in /python (#355) by @dependabot[bot]
- *(deps)* Bump astral-sh/setup-uv from 5 to 6 (#399) by @dependabot[bot]
- *(deps)* Bump actions/checkout from 4 to 5 (#416) by @dependabot[bot]
- *(deps)* Bump actions/download-artifact from 4 to 5 (#417) by @dependabot[bot]
- *(deps)* Bump mypy from 1.15 to 1.17.1 in /python (#413) by @dependabot[bot]
- *(deps)* Bump actions/labeler from 5 to 6 (#443) by @dependabot[bot]
- *(deps)* Bump actions/setup-python from 5 to 6 (#444) by @dependabot[bot]
- *(deps)* Bump actions/setup-node from 4 to 5 (#442) by @dependabot[bot]
- *(deps)* Bump pytest from 8.4.1 to 8.4.2 in /python (#441) by @dependabot[bot]
- *(deps)* Fix integration test setup issue with pyarrow and rust versions (#467) by @xushiyan
- *(deps)* Bump astral-sh/setup-uv from 6 to 7 (#465) by @dependabot[bot]
- *(deps)* Bump coverage from 7.8 to 7.10.7 in /python (#456) by @dependabot[bot]
- *(deps)* Bump mypy from 1.17.1 to 1.18.2 in /python (#454) by @dependabot[bot]
- *(deps)* Bump actions/setup-node from 5 to 6 (#469) by @dependabot[bot]
- *(deps)* Bump ruff from 0.12 to 0.14.1 in /python (#472) by @dependabot[bot]
- Use build wrapper to handle macos 26 SDK loading (#479) by @xushiyan
- *(deps)* Bump apache/skywalking-eyes from 0.7.0 to 0.8.0 (#468) by @dependabot[bot]
- *(deps)* Bump actions/checkout from 5 to 6 (#485) by @dependabot[bot]
- *(deps)* Bump actions/download-artifact from 5 to 7 (#496) by @dependabot[bot]
- *(deps)* Bump actions/upload-artifact from 4 to 6 (#495) by @dependabot[bot]
- *(deps)* Bump actions/cache from 4 to 5 (#494) by @dependabot[bot]
- *(deps-dev)* Bump mypy from 1.18.2 to 1.19.1 in /python (#497) by @dependabot[bot]
- *(deps)* Update tokio requirement from ~1.45 to ~1.48 (#470) by @dependabot[bot]
- *(deps)* Upgrade apache-avro to v0.21.0 (#503) by @xushiyan
- *(deps)* Upgrade datafusion, arrow, and other deps (#504) by @xushiyan
- *(deps)* Upgrade rust and make deps verisons more compatible (#506) by @xushiyan
- *(deps-dev)* Bump ruff from 0.14.1 to 0.14.13 in /python (#526) by @dependabot[bot]
- *(deps)* Update strum requirement from 0.27 to 0.28 (#533) by @dependabot[bot]
- *(deps)* Bump actions/upload-artifact from 6 to 7 (#531) by @dependabot[bot]
- *(deps)* Upgrade datafusion to 52, require python 3.10 (#535) by @xushiyan
- *(deps)* Bump actions/download-artifact from 7 to 8 (#532) by @dependabot[bot]
- *(deps)* Bump docker/setup-buildx-action from 3 to 4 (#540) by @dependabot[bot]
- *(deps)* Update strum_macros requirement from 0.27 to 0.28 (#541) by @dependabot[bot]
- *(deps)* Update zip requirement from 4 to 8 (#545) by @dependabot[bot]
- *(deps-dev)* Bump datafusion from 52.0.0 to 52.3.0 in /python (#544) by @dependabot[bot]
- *(deps)* Bump actions/checkout from 5 to 6 (#568) by @dependabot[bot]
- *(deps)* Bump codecov/codecov-action from 5 to 6 (#567) by @dependabot[bot]
- *(deps-dev)* Bump pytest from 8.4.2 to 9.0.3 in /python (#577) by @dependabot[bot]
- *(deps-dev)* Bump mypy from 1.19.1 to 1.20.2 in /python (#584) by @dependabot[bot]
- *(deps-dev)* Bump ruff from 0.14.13 to 0.15.12 in /python (#585) by @dependabot[bot]
- Upgrade rust dev toolchain to 1.94 (#595) by @xushiyan
- *(deps)* Bump actions/cache from 5 to 6 (#635) by @dependabot[bot]
- *(deps)* Bump actions/checkout from 6 to 7 (#632) by @dependabot[bot]
- *(deps-dev)* Bump pytest from 9.0.3 to 9.1.1 in /python (#634) by @dependabot[bot]
- *(deps)* Bump codecov/codecov-action from 6 to 7 (#628) by @dependabot[bot]
- *(deps-dev)* Bump ruff from 0.15.12 to 0.15.18 in /python (#633) by @dependabot[bot]
- *(deps)* Bump astral-sh/setup-uv from 8.3.2 to 10.0.1 (#626) by @dependabot[bot]
- *(deps-dev)* Bump coverage from 7.10.7 to 7.16.0 in /python (#625) by @dependabot[bot]
- *(deps)* Bump docker/setup-buildx-action from 4.2.0 to 4.3.0 (#617) by @dependabot[bot]
- *(deps-dev)* Bump mypy from 1.20.2 to 2.3.1 in /python (#615) by @dependabot[bot]
- Raise MSRV to 1.94.1 to match what the dependency graph requires (#715) by @yihua
- Adopt the MSRV-aware resolver and check the MSRV in CI (#721) by @yihua
- *(python)* Bound the datafusion extra to the 54.x major (#720) by @yihua
- Mark hudi-jvm-ffi as not published (#719) by @yihua

### 🧪 Testing

- Add basic v8 test tables (#394) by @xushiyan
- Add v9 test tables and test cases (#536) by @xushiyan
- *(core)* Expand v9 sample-table coverage in core tests (#578) by @xushiyan
- *(core)* Cover event-time ordering losing in the gold corpus (#680) by @linliu-code
- *(core)* Add hfile compression codec coverage. (#622) by @slfan1989

### ⚙️ Miscellaneous Tasks

- Update src verify script (#387) by @xushiyan
- Update changlog for 0.4.0 (#390) by @xushiyan
- Update asf.yaml for PR requirements (#393) by @xushiyan
- Bump script should use -dev suffix (#408) by @yunchipang
- Enable discussions (#431) by @xushiyan
- *(ci)* Update rust toolchain to 1.85 (#439) by @codope
- Reduce CI run time (#478) by @xushiyan
- Support generating local coverage report (#491) by @xushiyan
- Add copilot review instructions (#509) by @xushiyan
- Pin third-party actions to Apache-approved SHAs (#550) by @xushiyan
- Add pre-commit hooks for code formatting and linting (#103) by @muyihao
- Run rust-tests on windows (#92) by @crrow
- Remove write perm and fix vulnerability (#572) by @xushiyan
- Use action refs the ASF allowlist accepts (#674) by @linliu-code
- Let consumers opt out of the merge map's on-disk tier (#681) by @linliu-code

## New Contributors

* @yihua made their first contribution in #711

* @linliu-code made their first contribution in #674

* @slfan1989 made their first contribution in #622

* @yunchipang made their first contribution in #408

* @sushiljacksparrow made their first contribution in #581

* @crrow made their first contribution in #92

* @brayanjuls made their first contribution in #412

* @codephage2020 made their first contribution in #391

<!-- generated by git-cliff -->

## [0.4.0] - 2025-07-03

### 🚀 Features

- Support reading avro data block for MOR tables (#309) by @xushiyan
- Add table and timeline APIs for retrieving useful info (#313) by @xushiyan
- Add blocking APIs for `Table` and `FileGroupReader` (#321) by @xushiyan
- Support more timestamp formats for time-travel and incremental APIs (#302) by @hanbings
- Add C++ APIs for `FileGroupReader` (#322) by @xushiyan
- Support reading MOR delete block (#356) by @xushiyan
- Update python setup to support 3.13 (#361) by @xushiyan

### 🐛 Bug Fixes

- Make local timezone test more robust (#308) by @xushiyan
- Resolve env vars for creating standalone file group reader (#345) by @xushiyan
- Handle zero event time ordering (#357) by @xushiyan
- Handle schema resolution for empty commit (#359) by @xushiyan

### 🚜 Refactor

- Move `Makefile` to root directory (#283) by @nakul-py
- Use `OptionResolver` to handle options (#341) by @xushiyan
- Improve schema resolution flow (#364) by @xushiyan

### 📚 Documentation

- Update readme docs and the project description (#348) by @xushiyan

### 🛠️ Build

- *(deps)* Upgrade arrow, pyo3, datafusion, rustc (#297) by @xushiyan
- *(deps)* Bump apache/skywalking-eyes from 0.6.0 to 0.7.0 (#300) by @dependabot[bot]
- *(deps)* Bump ruff from 0.5.2 to 0.9.10 in /python (#307) by @dependabot[bot]
- *(deps)* Bump ruff from 0.11.0 to 0.11.2 in /python (#315) by @dependabot[bot]
- *(deps)* Bump ruff from 0.11.2 to 0.11.7 in /python (#327) by @dependabot[bot]
- *(deps)* Bump mypy from 1.10.1 to 1.15.0 in /python (#298) by @dependabot[bot]
- *(deps)* Update zip-extract requirement from 0.2 to 0.3 (#332) by @dependabot[bot]
- *(deps)* Add `-dev` for current development version (#362) by @xushiyan
- *(deps)* Upgrade datafusion & arrow, and restrict deps upgrade to patch-level (#386) by @xushiyan

### ⚙️ Miscellaneous Tasks

- Fix src verify script (#279) by @xushiyan
- Update release guide and issue templates (#282) by @xushiyan
- Update changelog for 0.3.0 (#288) by @xushiyan
- Update dependabot settings and readme links (#294) by @xushiyan
- Fix codecov report generation (#316) by @xushiyan
- Fix codecov upload issue (#318) by @xushiyan
- *(ci)* Update python test setup and upload nightly wheels (#320) by @xushiyan
- *(ci)* Parallel run integration tests (#340) by @xushiyan
- Update `asf.yaml` for more automation (#349) by @xushiyan
- *(ci)* Replace archived actions-rs actions (#351) by @assignUser

## New Contributors

* @assignUser made their first contribution in #351

* @hanbings made their first contribution in #302

* @nakul-py made their first contribution in #283

<!-- generated by git-cliff -->

## [0.3.0] - 2025-02-02

### 🚀 Features

- Define Hudi error types across hudi-core (#124) by @gohalo
- Support filter pushdown for datafusion (#203) by @jonathanc-n
- Add demo app and integration tests (#226) by @xushiyan
- Add `TimelineSelector` to support timeline loading (#233) by @xushiyan
- Add `hoodie.read.listing.parallelism` config (#235) by @xushiyan
- Support row filters for `FileGroupReader` (#237) by @xushiyan
- Implement incremental query for COW tables (#236) by @xushiyan
- Implement log file reader for parquet log block (#244) by @xushiyan
- Implement basic record merge semantics (#249) by @xushiyan
- Add APIs for MOR snapshot reads (#247) by @xushiyan
- Support time travel query for MOR tables (#256) by @xushiyan
- Support incremental read MOR tables (#258) by @xushiyan
- Support MOR read-optimized query (#259) by @xushiyan
- Support reading MOR with rollback (#264) by @xushiyan
- Align python table APIs with rust (#267) by @xushiyan
- Add APIs to support incremental query impl (#272) by @xushiyan

### 🐛 Bug Fixes

- Simplify partition filter format by taking tuple of strings (#170) by @kazdy
- Improve api to get file slices splits (#185) by @xushiyan
- Handle schema retrieval for datafusion api (#187) by @xushiyan
- Include commit_seqno for merge order (#250) by @xushiyan
- Format Hudi config enum should show the full config key (#254) by @Kunal-Singh-Dadhwal
- Derive record merge strategy based on table configs (#260) by @xushiyan
- Handle as-of timestamp for excluding file groups (#268) by @xushiyan
- Build up incremental file groups (#273) by @xushiyan

### 🚜 Refactor

- Reorganize custom error types (#215) by @xushiyan
- Add API stubs for performing incremental queries (#220) by @xushiyan
- Enhance `Filter` and related structs (#221) by @xushiyan
- Improve `TimelineSelector` API (#234) by @xushiyan
- Improve `BaseFile` APIs (#239) by @xushiyan
- Improve file system view's listing flow (#251) by @xushiyan
- Use static MetaField schema for incr query (#252) by @xushiyan
- Rename crate `hudi-tests` to `hudi-test` (#262) by @xushiyan
- Remove use of `Filter` from public APIs (#266) by @xushiyan

### 📚 Documentation

- Update README examples (#194) by @xushiyan
- Update release and dev guides (#195) by @xushiyan
- Add example to `hudi-datafusion` crate (#202) by @jonathanc-n
- Add `CREATE EXTERNAL TABLE` example in datafusion crate (#213) by @jonathanc-n
- Clarify issues in the dev guide  (#224) by @xushiyan
- Add in-code docs for `FileGroup` (#269) by @xushiyan
- Update `README.md` to show table API examples (#274) by @xushiyan

### 🛠️ Build

- *(deps)* Bump codecov/codecov-action from 4 to 5 (#184) by @dependabot[bot]
- *(deps)* Upgrade datafusion and object store (#182) by @kazdy
- *(deps)* Upgrade datafusion to 42.2.0 (#192) by @xushiyan
- *(deps)* Upgrade Datafusion, Arrow, and Rust versions (#197) by @jonathanc-n
- *(deps)* Update pyo3 requirement from 0.22.2 to 0.22.4 (#212) by @jonathanc-n
- *(deps)* Clean up dependencies (#240) by @xushiyan
- *(dep)* Upgrade rustc, arrow, and tarpaulin setting (#276) by @xushiyan

### ⚙️ Miscellaneous Tasks

- Update release script and guide (#200) by @xushiyan
- Update changelog for 0.2.0 (#201) by @xushiyan
- Update pull request guidelines for contributors (#204) by @jonathanc-n
- Add more dev commands and update the project's short description (#217) by @xushiyan
- Update codecov threshold (#222) by @xushiyan
- Update codecov config (#245) by @xushiyan
- Update codecov-action to v5 (#248) by @K-dash
- *(ci)* Add rust dependency caching with rust-cache action (#265) by @K-dash
- Fix src verify script (#279)
- Update release guide and issue templates (#282)

## New Contributors

* @K-dash made their first contribution in #265

* @Kunal-Singh-Dadhwal made their first contribution in #254

* @jonathanc-n made their first contribution in #203

<!-- generated by git-cliff -->

## [0.2.0] - 2024-11-25

### 🚀 Features

- Support loading hudi global configs (#118) by @zzhpro
- Add base file records' in-memory size to `FileStats` (#140) by @xushiyan
- Support partition prune api (#119) by @KnightChess
- Add partition filter arg in Python APIs (#153) by @xushiyan
- Add `HudiFileGroupReader` with consolidated APIs to read records (#164) by @xushiyan
- Add `TableBuilder` API for creating `Table` instances (#163) by @kazdy
- Implement datafusion `TableProviderFactory` (#162) by @kazdy

### 🐛 Bug Fixes

- Register object store with datafusion (#107) by @abyssnlp
- Handle validating table when `DropsPartitionFields` not present (#142) by @xushiyan
- Make partition loading more efficient (#152) by @xushiyan
- Simplify partition filter format by taking tuple of strings (#170)
- Improve api to get file slices splits (#185)
- Handle schema retrieval for datafusion api (#187)

### 🚜 Refactor

- Extract common test code for creating table (#117) by @gohalo
- Improve APIs for handling options (#161) by @xushiyan
- Improve `TableBuilder` API for taking single option (#171) by @xushiyan
- Minor improvement to fix coverage report status (#173) by @xushiyan

### 📚 Documentation

- Update readme logo and example (#65) by @xushiyan
- Update in-code comments (#132) by @KnightChess
- Add hudi core API docs with examples (#113) by @KnightChess
- Add in-code docs to hudi-core APIs (#166) by @xushiyan
- Add python binding docstrings (#169) by @kazdy
- Add step-by-step release guide (#66) by @xushiyan

### 🎨 Styling

- Enforce Python code style (#101) by @muyihao

### 🛠️ Build

- Use exact versions for arrow and datafusion (#105) by @xushiyan
- Bump up datafusion to version 41, arrow to 52.2 (#120) by @yjshen
- *(deps)* Update zip-extract requirement from 0.1.3 to 0.2.1 (#130) by @dependabot[bot]
- *(deps)* Upgrade datafusion, pyarrow, pyo3, python versions  (#149) by @kazdy
- *(deps)* Upgrade arrow dependencies (#168) by @kazdy
- *(release)* Bump version to 0.2.0-rc.1
- *(deps)* Upgrade datafusion and object store (#182)
- *(deps)* Upgrade datafusion to 42.2.0 (#192)
- *(release)* Bump version to 0.2.0-rc.2

### ⚙️ Miscellaneous Tasks

- Improve release scripts (#68) by @xushiyan
- Add `CHANGELOG.md` with git-cliff config (#69) by @xushiyan
- Configure labeler for PRs from forked repos (#83) by @xushiyan
- Fix labeler config (#85) by @xushiyan
- Fix labeler config for dev-x (#87) by @xushiyan
- Merge python code coverage report with rust (#67) by @xushiyan
- Add pull request template (#89) by @xushiyan
- Enable dependabot (#94) by @xushiyan
- Add path ignore files for ci workflow (#93) by @abyssnlp
- Improve workflows for code checking and PR (#110) by @xushiyan
- Disable labeler due to permission and policy (#115) by @xushiyan
- *(ci)* Fix PR title linting to support change scope (#138) by @kazdy
- Add feature request template for GH issues (#167) by @kazdy

## New Contributors

* @KnightChess made their first contribution in #119

* @gohalo made their first contribution in #117

* @zzhpro made their first contribution in #118

* @yjshen made their first contribution in #120

* @abyssnlp made their first contribution in #107

* @muyihao made their first contribution in #101

<!-- generated by git-cliff -->

## [0.1.0] - 2024-07-15

### 🚀 Features

- Initial rust implementation to integrate with datafusion (#1) by @xushiyan
- Add python binding (#21) by @xushiyan
- Implement `HudiTable` as python API (#23) by @xushiyan
- Use `object_store` for common storage APIs (#25) by @xushiyan
- Implement Rust and Python APIs to read file slices (#28) by @xushiyan
- Add APIs for time-travel read (#33) by @xushiyan
- Implement datafusion API using ParquetExec (#35) by @xushiyan
- Add `HudiConfigs` for parsing and managing named configs (#37) by @xushiyan
- Add config validation when creating table (#49) by @xushiyan
- Add internal config to skip validation (#51) by @xushiyan
- Support time travel with read option (#52) by @xushiyan
- Support taking env vars for cloud storages (#55) by @xushiyan

### 🐛 Bug Fixes

- Handle replacecommit for loading file slices (#53) by @xushiyan

### 🚜 Refactor

- Use `anyhow` for generic errors (#26) by @xushiyan
- Use `object_store` API for Timeline (#27) by @xushiyan
- Make APIs async (#31) by @xushiyan
- Improve thread safety and error handling (#32) by @xushiyan
- Improve error handling in storage module (#34) by @xushiyan
- Adjust table APIs to skip passing options (#56) by @xushiyan

### 📚 Documentation

- Update readme, contributing guide, and issue template (#57) by @xushiyan
- Update CONTRIBUTING with minor changes (#58) by @codope

### 🎨 Styling

- Enforce rust code style (#14) by @xushiyan

### 🛠️ Build

- Clean up and trim down dependencies (#54) by @xushiyan
- Add info for rust and python artifacts (#60) by @xushiyan
- Add release workflow (#63) by @xushiyan

### 🧪 Testing

- Add tests crate and adopt testing tables (#30) by @xushiyan
- Add test cases for different table setup (#36) by @xushiyan

### ⚙️ Miscellaneous Tasks

- Setup ci for license file and headers (#2) by @xushiyan
- Fix failing check and test case (#10) by @xushiyan
- Fix asf notification (#11) by @xushiyan
- Add commit linting (#12) by @xushiyan
- Use cargo tarpaulin to generate code coverage (#15) by @xushiyan
- Remove codecov to keep ci green (#17) by @xushiyan
- Fix codecov setup (#20) by @xushiyan
- Configure codecov (#50) by @xushiyan
- Add scripts to streamline source release (#64) by @xushiyan

## New Contributors

* @codope made their first contribution in #58
* @xushiyan made their first contribution in #1

<!-- generated by git-cliff -->
