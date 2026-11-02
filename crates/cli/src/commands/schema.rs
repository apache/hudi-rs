// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

use std::collections::HashMap;

use anyhow::Result;

use crate::output::{CommandOutput, OutputSection};

use super::open_table;

/// Show current table schema.
pub async fn show(
    path: &str,
    storage_opts: &HashMap<String, String>,
) -> Result<CommandOutput> {
    let table = open_table(path, storage_opts).await?;
    let schema = table.get_schema().await?;

    let rows: Vec<Vec<String>> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            vec![
                i.to_string(),
                field.name().clone(),
                format!("{}", field.data_type()),
                if field.is_nullable() { "YES" } else { "NO" }.to_string(),
            ]
        })
        .collect();

    Ok(CommandOutput::single(OutputSection {
        title: "Schema".into(),
        headers: vec![
            "#".into(),
            "Field Name".into(),
            "Data Type".into(),
            "Nullable".into(),
        ],
        rows,
        metadata: vec![("Total fields".into(), schema.fields().len().to_string())],
    }))
}
