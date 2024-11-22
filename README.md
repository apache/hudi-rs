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
  A native Rust library for Apache Hudi, with bindings to Python
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

The `hudi-rs` project aims to broaden the use of [Apache Hudi](https://github.com/apache/hudi) for a diverse range of
users and projects.

| Source        | Installation Command |
|---------------|----------------------|
| **PyPi**      | `pip install hudi`   |
| **Crates.io** | `cargo add hudi`     |

## Example usage

> [!NOTE]
> These examples expect a Hudi table exists at `/tmp/trips_table`, created using
> the [quick start guide](https://hudi.apache.org/docs/quick-start-guide).

### Python

Read a Hudi table into a PyArrow table.

```python
from hudi import HudiTableBuilder
import pyarrow as pa

hudi_table = (
    HudiTableBuilder
    .from_base_uri("/tmp/trips_table")
    .with_option("hoodie.read.as.of.timestamp", "0")
    .build()
)
records = hudi_table.read_snapshot(filters=[("city", "=", "san_francisco")])

arrow_table = pa.Table.from_batches(records)
result = arrow_table.select(["rider", "city", "ts", "fare"])
print(result)
```

### Rust (DataFusion)

<details>
<summary>Add crate hudi with datafusion feature to your application to query a Hudi table.</summary>

```shell
cargo new my_project --bin && cd my_project
cargo add tokio@1 datafusion@42
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
        [("hoodie.read.as.of.timestamp", "20241122010827898")]).await?;
    ctx.register_table("trips_table", Arc::new(hudi))?;
    let df: DataFrame = ctx.sql("SELECT * from trips_table where city = 'san_francisco'").await?;
    df.show().await?;
    Ok(())
}
```

### Work with cloud storage

Ensure cloud storage credentials are set properly as environment variables, e.g., `AWS_*`, `AZURE_*`, or `GOOGLE_*`.
Relevant storage environment variables will then be picked up. The target table's base uri with schemes such
as `s3://`, `az://`, or `gs://` will be processed accordingly.

Alternatively, you can pass the storage configuration as options to the `HudiTableBuilder` or `HudiDataSource`.

### Python

```python
from hudi import HudiTableBuilder

hudi_table = (
    HudiTableBuilder
    .from_base_uri("s3://bucket/trips_table")
    .with_option("aws_region", "us-west-2")
    .build()
)
```

### Rust (DataFusion)

```rust
use hudi::HudiDataSource;

async fn main() -> Result<()> {
    let hudi = HudiDataSource::new_with_options(
        "s3://bucket/trips_table",
        [("aws_region", "us-west-2")]
    ).await?;
}

```

## Contributing

Check out the [contributing guide](./CONTRIBUTING.md) for all the details about making contributions to the project.
