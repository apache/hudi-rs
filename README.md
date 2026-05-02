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
  <a href="https://hudi.apache.org/slack">
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

For the full reader API reference (`ReadOptions`, filter expressions, behavioral guarantees), see [docs/reader-spec.md](docs/reader-spec.md).

### Snapshot Query

Snapshot query reads the latest version of the data from the table. The table API also accepts column filters that drive partition + file pruning and row-level filtering.

#### Python

```python
from hudi import HudiReadOptions, HudiTableBuilder
import pyarrow as pa

hudi_table = HudiTableBuilder.from_base_uri("/tmp/trips_table").build()
batches = hudi_table.read(
    HudiReadOptions(filters=[("city", "=", "san_francisco")])
)

# convert to PyArrow table
arrow_table = pa.Table.from_batches(batches)
result = arrow_table.select(["rider", "city", "ts", "fare"])
print(result)
```

#### Rust

```rust
use hudi::error::Result;
use hudi::table::ReadOptions;
use hudi::table::builder::TableBuilder as HudiTableBuilder;
use arrow::compute::concat_batches;

#[tokio::main]
async fn main() -> Result<()> {
    let hudi_table = HudiTableBuilder::from_base_uri("/tmp/trips_table").build().await?;
    let options = ReadOptions::new().with_filters([("city", "=", "san_francisco")])?;
    let batches = hudi_table.read(&options).await?;
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
from hudi import HudiReadConfig, HudiTableBuilder

hudi_table = (
    HudiTableBuilder
    .from_base_uri("/tmp/trips_table")
    .with_option(HudiReadConfig.USE_READ_OPTIMIZED_MODE, "true")
    .build()
)
```

`HudiReadConfig` and `HudiTableConfig` enum members are accepted by `with_hudi_option` and `with_option`. For the bulk variants `with_hudi_options` / `with_options`, pass string keys (or `member.value`) until they are updated to coerce enum keys.

#### Rust

```rust
let hudi_table = 
    HudiTableBuilder::from_base_uri("/tmp/trips_table")
    .with_option("hoodie.read.use.read_optimized.mode", "true")
    .build().await?;
```

### Time-Travel Query

Time-travel query reads the data at a specific timestamp from the table. The table API also accepts column filters that drive partition + file pruning and row-level filtering.

#### Python

```python
batches = hudi_table.read(
    HudiReadOptions(filters=[("city", "=", "san_francisco")])
    .with_as_of_timestamp("20241231123456789")
)
```

#### Rust

```rust
let options = ReadOptions::new()
    .with_as_of_timestamp("20241231123456789")
    .with_filters([("city", "=", "san_francisco")])?;
let batches = hudi_table.read(&options).await?;
```

<details>
<summary>Supported timestamp formats</summary>

The supported formats for the timestamp argument are:
- Hudi Timeline format (highest matching precedence): `yyyyMMddHHmmssSSS` or `yyyyMMddHHmmss`.
- Unix epoch time in seconds, milliseconds, microseconds, or nanoseconds.
- RFC 3339 / ISO 8601 with timezone offset, including:
  - `yyyy-MM-dd'T'HH:mm:ss.SSS+00:00`
  - `yyyy-MM-dd'T'HH:mm:ss.SSSZ`
  - `yyyy-MM-dd'T'HH:mm:ss+00:00`
  - `yyyy-MM-dd'T'HH:mm:ssZ`

Timestamp strings without a timezone offset (for example `yyyy-MM-dd'T'HH:mm:ss`) and date-only strings (for example `yyyy-MM-dd`) are not accepted.
</details>

### Incremental Query

Incremental query reads the changed data from the table for a given time range.

#### Python

```python
from hudi import HudiQueryType

# read the records between t1 (exclusive) and t2 (inclusive)
batches = hudi_table.read(
    HudiReadOptions()
    .with_query_type(HudiQueryType.Incremental)
    .with_start_timestamp(t1)
    .with_end_timestamp(t2)
)

# read the records after t1 (end defaults to the latest commit)
batches = hudi_table.read(
    HudiReadOptions()
    .with_query_type(HudiQueryType.Incremental)
    .with_start_timestamp(t1)
)

# with column filters applied to the changed records
batches = hudi_table.read(
    HudiReadOptions(filters=[("city", "=", "san_francisco")])
    .with_query_type(HudiQueryType.Incremental)
    .with_start_timestamp(t1)
    .with_end_timestamp(t2)
)
```

#### Rust

```rust
use hudi::table::QueryType;

// read the records between t1 (exclusive) and t2 (inclusive)
let options = ReadOptions::new()
    .with_query_type(QueryType::Incremental)
    .with_start_timestamp(t1)
    .with_end_timestamp(t2);
let batches = hudi_table.read(&options).await?;

// read the records after t1 (end defaults to the latest commit)
let options = ReadOptions::new()
    .with_query_type(QueryType::Incremental)
    .with_start_timestamp(t1);
let batches = hudi_table.read(&options).await?;

// with column filters applied to the changed records
let options = ReadOptions::new()
    .with_query_type(QueryType::Incremental)
    .with_start_timestamp(t1)
    .with_end_timestamp(t2)
    .with_filters([("city", "=", "san_francisco")])?;
let batches = hudi_table.read(&options).await?;
```

*Incremental queries support the same timestamp formats as time-travel queries.*

### Streaming Read

Streaming reads yield `RecordBatch`es one at a time without loading the full result into memory.
The same `ReadOptions` knobs apply, plus `batch_size` and `projection`.

#### Python

```python
options = (
    HudiReadOptions(
        filters=[("city", "=", "san_francisco")],
        projection=["rider", "city", "ts", "fare"],
    )
    .with_batch_size(4096)
)
for batch in hudi_table.read_stream(options):
    print(batch.num_rows)
```

#### Rust

```rust
use futures::StreamExt;

let options = ReadOptions::new()
    .with_filters([("city", "=", "san_francisco")])?
    .with_projection(["rider", "city", "ts", "fare"])
    .with_batch_size(4096)?;
let mut stream = hudi_table.read_stream(&options).await?;
while let Some(batch) = stream.next().await {
    let batch = batch?;
    println!("{}", batch.num_rows());
}
```

### File Group Reading (Experimental)

File group reading allows you to read data from a specific file slice. This is useful when integrating with query
engines, where the plan provides file paths.

#### Python

```python
from hudi import HudiFileGroupReader

reader = HudiFileGroupReader(
    "/table/base/path", {"hoodie.read.start.timestamp": "0"})

# Returns a PyArrow RecordBatch
record_batch = reader.read_file_slice_from_paths("relative/path.parquet", [])
```

#### Rust

```rust,ignore
use hudi::file_group::reader::FileGroupReader;
use hudi::table::ReadOptions;

// Inside an async context
let reader = FileGroupReader::new_with_options(
    "/table/base/path", [("hoodie.read.start.timestamp", "0")]).await?;

// Returns an Arrow RecordBatch
let record_batch = reader
    .read_file_slice_from_paths(
        "relative/path.parquet",
        Vec::<&str>::new(),
        &ReadOptions::new(),
    )
    .await?;
```

#### C++

```cpp
#include "cxx.h"
#include "src/lib.rs.h"
#include "arrow/c/abi.h"

// Functions may throw rust::Error on failure
auto reader = new_file_group_reader_with_options(
    "/table/base/path", {"hoodie.read.start.timestamp=0"});

// Returns an ArrowArrayStream pointer
std::vector<std::string> log_file_paths{};
ArrowArrayStream* stream_ptr = reader->read_file_slice_from_paths("relative/path.parquet", log_file_paths);
```

## Query Engine Integration

Hudi-rs provides APIs to support integration with query engines. The sections below highlight some commonly used APIs.

### Table API

Create a Hudi table instance using its constructor or the `TableBuilder` API.

All read APIs accept a `ReadOptions` (Rust) / `HudiReadOptions` (Python) value. It stores three fields — `filters`, `projection`, and `hudi_options` — and exposes chainable `with_*` builders for the rest. The available knobs:

- `query_type` (`with_query_type`) — `Snapshot` (default) or `Incremental`. Drives dispatch in `read`, `read_stream`, and `get_file_slices`.
- `filters` — column filters as `(field, op, value)` tuples. The field can be any column (partition or data). Used for partition pruning, file-level stats pruning (snapshot only), and row-level filtering.
- `projection` — columns to return. Streaming pushes the projection down to the parquet reader; eager reads project after merging.
- `batch_size` (`with_batch_size`) — rows per batch (streaming only; eager reads return one batch per file slice).
- `as_of_timestamp` (`with_as_of_timestamp`) — snapshot/time-travel timestamp (defaults to latest commit).
- `start_timestamp` / `end_timestamp` (`with_start_timestamp` / `with_end_timestamp`) — incremental range (defaults to earliest…latest).
- `hudi_options` — per-read Hudi configs that override table-level defaults (e.g. `hoodie.read.use.read_optimized.mode`).

| Stage           | API                                                              | Description                                                                                              |
|-----------------|------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| Query planning  | `get_file_slices(options)`                                       | Get the file slices the read targets, dispatched on `options.query_type`. To bucket for parallel reads, call `hudi::util::collection::split_into_chunks` on the result. |
|                 | `compute_table_stats()`                                          | Estimated `(num_rows, byte_size)` for scan planning. Returns `None` when the metadata table is disabled. |
| Query execution | `create_file_group_reader_with_options(read_options, extras)`    | Create a file group reader with the table's configs. Both args are optional; `read_options.hudi_options` overrides table-level defaults (excluding the four `Table`-owned read keys), and `extras` always wins. |
|                 | `read(options)` / `read_stream(options)`                         | Record-read APIs. Dispatch on `options.query_type`. `read_stream` errors on `Incremental` for now. Per-slice streaming lives on `FileGroupReader`. |

### File Group API

Create a Hudi file group reader instance using its constructor or the Hudi table API `create_file_group_reader_with_options()`.

| Stage           | API                                     | Description                                                                                                                                                                        |
|-----------------|-----------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Query execution | `read_file_slice()`                     | Read records from a given file slice; based on the configs, read records from only base file, or from base file and log files, and merge records based on the configured strategy. |
|                 | `read_file_slice_from_paths()`          | Read records from an explicit base file path and a list of log file paths. Pass an empty log path list to read just the base file.                                                 |
|                 | `read_file_slice_stream()`              | Streaming version of `read_file_slice()`. Yields true streaming batches when the slice is base-file-only or read-optimized; for MOR slices with log files, falls back to a single merged batch. |
|                 | `read_file_slice_from_paths_stream()`   | Streaming version of `read_file_slice_from_paths()`.                                                                                                                               |


### Apache DataFusion

Enabling the `hudi` crate with `datafusion` feature will provide a [DataFusion](https://datafusion.apache.org/) 
extension to query Hudi tables.

<details>
<summary>Add crate hudi with datafusion feature to your application to query a Hudi table.</summary>

```shell
cargo new my_project --bin && cd my_project
cargo add tokio@1 datafusion@52
cargo add hudi --features datafusion
```

Update `src/main.rs` with the code snippet below then `cargo run`.

</details>

<details>
<summary>Add python hudi with datafusion feature to your application to query a Hudi table.</summary>

```shell
pip install hudi[datafusion]
```
</details>

#### Rust
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

#### Python
```python
from datafusion import SessionContext
from hudi import HudiDataFusionDataSource

table = HudiDataFusionDataSource(
    "/tmp/trips_table", [("hoodie.read.input.partitions", "5")]
)
ctx = SessionContext()
ctx.register_table_provider("trips", table)
ctx.sql("SELECT max(fare), city from trips group by city order by 1 desc").show()
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
use hudi::error::Result;
use hudi::table::builder::TableBuilder as HudiTableBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    let hudi_table = HudiTableBuilder::from_base_uri("s3://bucket/trips_table")
        .with_option("aws_region", "us-west-2")
        .build().await?;
    Ok(())
}
```

## Contributing

Check out the [contributing guide](./CONTRIBUTING.md) for all the details about making contributions to the project.
