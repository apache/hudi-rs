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
    <img src="https://hudi.apache.org/assets/images/hudi-logo-medium.png" alt="Hudi logo" height="80px">
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

### Python

Read a Hudi table into a PyArrow table.

```python
from hudi import HudiTable

hudi_table = HudiTable("/tmp/trips_table")
records = hudi_table.read_snapshot()

import pyarrow as pa
import pyarrow.compute as pc

arrow_table = pa.Table.from_batches(records)
result = arrow_table.select(
    ["rider", "ts", "fare"]).filter(
    pc.field("fare") > 20.0)
print(result)
```

### Rust

<details>
<summary>Add crate `hudi` with `datafusion` feature to your application to query a Hudi table.</summary>

```yaml
[dependencies]
hudi = { version = "0" , features = ["datafusion"] }
tokio = "1"
datafusion = "39.0.0"
```
</details>

```rust
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::prelude::{DataFrame, SessionContext};
use hudi::HudiDataSource;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let hudi = HudiDataSource::new("/tmp/trips_table").await?;
    ctx.register_table("trips_table", Arc::new(hudi))?;
    let df: DataFrame = ctx.sql("SELECT * from trips_table where fare > 20.0").await?;
    df.show().await?;
    Ok(())
}
```

### Work with cloud storage

Ensure cloud storage credentials are set properly as environment variables, e.g., `AWS_*`, `AZURE_*`, or `GOOGLE_*`.
Relevant storage environment variables will then be picked up. The target table's base uri with schemes such
as `s3://`, `az://`, or `gs://` will be processed accordingly.

## Contributing

Check out the [contributing guide](./CONTRIBUTING.md) for all the details about making contributions to the project.
