// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

pub mod commits;
pub mod diagnose;
pub mod files;
pub mod schema;
pub mod table;
pub mod timeline;

use std::collections::HashMap;

use anyhow::Result;
use hudi_core::table::Table;

// Re-export for use across commands
pub use hudi_core::config::util::empty_filters;

/// Open a Hudi table from a base URI with optional storage options.
pub async fn open_table(
    path: &str,
    storage_opts: &HashMap<String, String>,
) -> Result<Table> {
    let opts: Vec<(&str, &str)> = storage_opts
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let table = Table::new_with_options(path, opts).await?;
    Ok(table)
}
