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

<p align="center">
  <a href="https://hudi.apache.org/">
    <img src="https://hudi.apache.org/assets/images/hudi_logo_transparent_1400x600.png" alt="Hudi logo" height="120px">
  </a>
</p>
<p align="center">
  The native Rust implementation for Apache Hudi, with C++ & Python API bindings.
  <br>
  <br>
  <a href="https://github.com/apache/hudi-rs/actions/workflows/ci.yml">
    <img alt="hudi-rs ci" src="https://github.com/apache/hudi-rs/actions/workflows/ci.yml/badge.svg">
  </a>
  <a href="https://codecov.io/github/apache/hudi-rs">
    <img alt="hudi-rs codecov" src="https://codecov.io/github/apache/hudi-rs/graph/badge.svg">
  </a>
  <a href="https://join.slack.com/t/apache-hudi/shared_invite/zt-2ggm1fub8-_yt4Reu9djwqqVRFC7X49g">
    <img alt="join hudi slack" src="https://img.shields.io/badge/slack-%23hudi-72eff8?logo=slack&color=48c628">
  </a>
  <a href="https://x.com/apachehudi">
    <img alt="follow hudi x/twitter" src="https://img.shields.io/twitter/follow/apachehudi?label=apachehudi">
  </a>
  <a href="https://www.linkedin.com/company/apache-hudi">
    <img alt="follow hudi linkedin" src="https://img.shields.io/badge/apache%E2%80%93hudi-0077B5?logo=linkedin">
  </a>
</p>

The Hudi-rs project aims to standardize the core [Apache Hudi](https://github.com/apache/hudi) APIs, and broaden the
Hudi integration in the data ecosystems for a diverse range of users and projects.

| Source                  | Downloads                   | Installation Command |
|-------------------------|-----------------------------|----------------------|
| [**PyPi.org**][pypi]    | [![][pypi-badge]][pypi]     | `pip install hudi`   |
| [**Crates.io**][crates] | [![][crates-badge]][crates] | `cargo add hudi`     |

[pypi]: https://pypi.org/project/hudi/
[pypi-badge]: https://img.shields.io/pypi/dm/hudi?style=flat-square&color=51AEF3
[crates]: https://crates.io/crates/hudi
[crates-badge]: https://img.shields.io/crates/d/hudi?style=flat-square&color=163669

## Usage Examples

> [!NOTE]
> These examples expect a Hudi table exists at `/tmp/trips_table`, created using
> the [quick start guide](https://hudi.apache.org/docs/quick-start-guide).

### Snapshot Query

Snapshot query reads the latest version of the data from the table. The table API also accepts partition filters.

#### Python

```python
from hudi import HudiTableBuilder
import pyarrow as pa

hudi_table = HudiTableBuilder.from_base_uri("/tmp/trips_table").build()
batches = hudi_table.read_snapshot(filters=[("city", "=", "san_francisco")])

# convert to PyArrow table
arrow_table = pa.Table.from_batches(batches)
result = arrow_table.select(["rider", "city", "ts", "fare"])
print(result)
```

#### Rust

```rust
use hudi::error::Result;
use hudi::table::builder::TableBuilder as HudiTableBuilder;
use arrow::compute::concat_batches;

#[tokio::main]
async fn main() -> Result<()> {
    let hudi_table = HudiTableBuilder::from_base_uri("/tmp/trips_table").build().await?;
    let batches = hudi_table.read_snapshot(&[("city", "=", "san_francisco")]).await?;
    let batch = concat_batches(&batches[0].schema(), &batches)?;
    let columns = vec!["rider", "city", "ts", "fare"];
    for col_name in columns {
        let idx = batch.schema().index_of(col_name).unwrap();
        println!("{}: {}", col_name, batch.column(idx));
    }
    Ok(())
}
```

To run read-optimized (RO) query on Merge-on-Read (MOR) tables, set `hoodie.read.use.read_optimized.mode` when creating the table.

#### Python

```python
hudi_table = (
    HudiTableBuilder
    .from_base_uri("/tmp/trips_table")
    .with_option("hoodie.read.use.read_optimized.mode", "true")
    .build()
)
```

#### Rust

```rust
let hudi_table = 
    HudiTableBuilder::from_base_uri("/tmp/trips_table")
    .with_option("hoodie.read.use.read_optimized.mode", "true")
    .build().await?;
```

### Time-Travel Query

Time-travel query reads the data at a specific timestamp from the table. The table API also accepts partition filters.

#### Python

```python
batches = (
    hudi_table
    .read_snapshot_as_of("20241231123456789", filters=[("city", "=", "san_francisco")])
)
```

#### Rust

```rust
let batches = 
    hudi_table
    .read_snapshot_as_of("20241231123456789", &[("city", "=", "san_francisco")]).await?;
```

<details>
<summary>Supported timestamp formats</summary>

The supported formats for the timestamp argument are:
- Hudi Timeline format (highest matching precedence): `yyyyMMddHHmmssSSS` or `yyyyMMddHHmmss`.
- Unix epoch time in seconds, milliseconds, microseconds, or nanoseconds.
- ISO 8601 format including but not limited to:
  - `yyyy-MM-dd'T'HH:mm:ss.SSS+00:00`
  - `yyyy-MM-dd'T'HH:mm:ss.SSSZ`
  - `yyyy-MM-dd'T'HH:mm:ss.SSS`
  - `yyyy-MM-dd'T'HH:mm:ss+00:00`
  - `yyyy-MM-dd'T'HH:mm:ssZ`
  - `yyyy-MM-dd'T'HH:mm:ss`
  - `yyyy-MM-dd`
</details>

### Incremental Query

Incremental query reads the changed data from the table for a given time range.

#### Python

```python
# read the records between t1 (exclusive) and t2 (inclusive)
batches = hudi_table.read_incremental_records(t1, t2)

# read the records after t1
batches = hudi_table.read_incremental_records(t1)
```

#### Rust

```rust
// read the records between t1 (exclusive) and t2 (inclusive)
let batches = hudi_table.read_incremental_records(t1, Some(t2)).await?;

// read the records after t1
let batches = hudi_table.read_incremental_records(t1, None).await?;
```

*Incremental queries support the same timestamp formats as time-travel queries.*

### File Group Reading (Experimental)

File group reading allows you to read data from a specific file slice. This is useful when integrating with query
engines, where the plan provides file paths.

#### Python

```python
from hudi import HudiFileGroupReader

reader = HudiFileGroupReader(
    "/table/base/path", {"hoodie.read.file_group.start_timestamp": "0"})

# Returns a PyArrow RecordBatch
record_batch = reader.read_file_slice_by_base_file_path("relative/path.parquet")
```

#### Rust

```rust
use hudi::file_group::reader::FileGroupReader;

let reader = FileGroupReader::new_with_options(
    "/table/base/path", [("hoodie.read.file_group.start_timestamp", "0")])?;

// Returns an Arrow RecordBatch
let record_batch = reader.read_file_slice_by_base_file_path("relative/path.parquet").await?;
```

#### C++

```cpp
#include "cxx.h"
#include "src/lib.rs.h"
#include "arrow/c/abi.h"

auto reader = new_file_group_reader_with_options(
    "/table/base/path", {"hoodie.read.file_group.start_timestamp=0"});

// Returns an ArrowArrayStream pointer
ArrowArrayStream* stream_ptr = reader->read_file_slice_by_base_file_path("relative/path.parquet");
```

## Query Engine Integration

Hudi-rs provides APIs to support integration with query engines. The sections below highlight some commonly used APIs.

### Table API

Create a Hudi table instance using its constructor or the `TableBuilder` API.

| Stage           | API                                       | Description                                                                    |
|-----------------|-------------------------------------------|--------------------------------------------------------------------------------|
| Query planning  | `get_file_slices()`                       | For snapshot query, get a list of file slices.                                 |
|                 | `get_file_slices_splits()`                | For snapshot query, get a list of file slices in splits.                       |
|                 | `get_file_slices_as_of()`                 | For time-travel query, get a list of file slices at a given time.              |
|                 | `get_file_slices_splits_as_of()`          | For time-travel query, get a list of file slices in splits at a given time.    |
|                 | `get_file_slices_between()`               | For incremental query, get a list of changed file slices between a time range. |
| Query execution | `create_file_group_reader_with_options()` | Create a file group reader instance with the table instance's configs.         |

### File Group API

Create a Hudi file group reader instance using its constructor or the Hudi table API `create_file_group_reader_with_options()`.

| Stage           | API                                   | Description                                                                                                                                                                        |
|-----------------|---------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Query execution | `read_file_slice()`                   | Read records from a given file slice; based on the configs, read records from only base file, or from base file and log files, and merge records based on the configured strategy. |
|                 | `read_file_slice_by_base_file_path()` | Read records from a given base file path; log files will be ignored                                                                                                                |


### Apache DataFusion

Enabling the `hudi` crate with `datafusion` feature will provide a [DataFusion](https://datafusion.apache.org/) 
extension to query Hudi tables.

<details>
<summary>Add crate hudi with datafusion feature to your application to query a Hudi table.</summary>

```shell
cargo new my_project --bin && cd my_project
cargo add tokio@1 datafusion@45
cargo add hudi --features datafusion
```

Update `src/main.rs` with the code snippet below then `cargo run`.

</details>

```rust
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::prelude::{DataFrame, SessionContext};
use hudi::HudiDataSource;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let hudi = HudiDataSource::new_with_options(
        "/tmp/trips_table",
        [("hoodie.read.input.partitions", "5")]).await?;
    ctx.register_table("trips_table", Arc::new(hudi))?;
    let df: DataFrame = ctx.sql("SELECT * from trips_table where city = 'san_francisco'").await?;
    df.show().await?;
    Ok(())
}
```

### Other Integrations

Hudi is also integrated with

- [Daft](https://www.getdaft.io/projects/docs/en/stable/integrations/hudi/)
- [Ray](https://docs.ray.io/en/latest/data/api/doc/ray.data.read_hudi.html#ray.data.read_hudi)

### Work with cloud storage

Ensure cloud storage credentials are set properly as environment variables, e.g., `AWS_*`, `AZURE_*`, or `GOOGLE_*`.
Relevant storage environment variables will then be picked up. The target table's base uri with schemes such
as `s3://`, `az://`, or `gs://` will be processed accordingly.

Alternatively, you can pass the storage configuration as options via Table APIs.

#### Python

```python
from hudi import HudiTableBuilder

hudi_table = (
    HudiTableBuilder
    .from_base_uri("s3://bucket/trips_table")
    .with_option("aws_region", "us-west-2")
    .build()
)
```

#### Rust

```rust
use hudi::table::builder::TableBuilder as HudiTableBuilder;

async fn main() -> Result<()> {
    let hudi_table = 
        HudiTableBuilder::from_base_uri("s3://bucket/trips_table")
        .with_option("aws_region", "us-west-2")
        .build().await?;
}
```

## Contributing

Check out the [contributing guide](./CONTRIBUTING.md) for all the details about making contributions to the project.
