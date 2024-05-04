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

# hudi-rs

## Quick Start

### Apache DataFusion

```rust
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::prelude::{DataFrame, SessionContext};

use hudi_datafusion::HudiDataSource;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let hudi = HudiDataSource::new("/tmp/trips_table");
    ctx.register_table("trips_table", Arc::new(hudi))?;
    let df: DataFrame = ctx.sql("SELECT * from trips_table where fare > 20.0").await?;
    df.show().await?;
    Ok(())
}
```
