// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

use std::collections::HashMap;

use anyhow::Result;

use crate::output::{CommandOutput, OutputSection};

use super::open_table;

/// Show table info: type, version, partition fields, record key, etc.
pub async fn info(path: &str, storage_opts: &HashMap<String, String>) -> Result<CommandOutput> {
    let table = open_table(path, storage_opts).await?;
    let schema = table.get_schema().await?;
    let timeline = table.get_timeline();
    let latest_ts = timeline
        .get_latest_commit_timestamp()
        .unwrap_or_else(|_| "none".to_string());
    let completed = timeline.completed_commits.len();

    let hudi_opts = table.hudi_options();

    let get = |key: &str| -> String {
        hudi_opts
            .get(key)
            .cloned()
            .unwrap_or_else(|| "N/A".to_string())
    };

    let metadata = vec![
        ("Table Name".into(), table.table_name()),
        ("Base Path".into(), path.to_string()),
        ("Table Type".into(), table.table_type()),
        ("Table Version".into(), get("hoodie.table.version")),
        ("Record Key Fields".into(), get("hoodie.table.recordkey.fields")),
        ("Precombine Field".into(), get("hoodie.table.precombine.field")),
        ("Partition Fields".into(), get("hoodie.table.partition.fields")),
        ("Base File Format".into(), get("hoodie.table.base.file.format")),
        ("Timeline Timezone".into(), table.timezone()),
        ("Is MoR".into(), table.is_mor().to_string()),
        ("Completed Commits".into(), completed.to_string()),
        ("Latest Commit".into(), latest_ts),
        ("Schema Fields".into(), format!("{} columns", schema.fields().len())),
    ];

    Ok(CommandOutput::single(OutputSection {
        title: "Table Info".into(),
        headers: vec![],
        rows: vec![],
        metadata,
    }))
}

/// Show table configuration, optionally filtered by prefix.
pub async fn config(
    path: &str,
    storage_opts: &HashMap<String, String>,
    filter: Option<&str>,
) -> Result<CommandOutput> {
    let table = open_table(path, storage_opts).await?;
    let all_configs = table.hudi_options();

    let mut configs: Vec<(String, String)> = all_configs
        .into_iter()
        .filter(|(k, _)| {
            filter.map(|f| k.starts_with(f)).unwrap_or(true)
        })
        .collect();
    configs.sort_by(|a, b| a.0.cmp(&b.0));

    let count = configs.len();
    let rows: Vec<Vec<String>> = configs
        .into_iter()
        .map(|(k, v)| vec![k, v])
        .collect();

    Ok(CommandOutput::single(OutputSection {
        title: format!(
            "Configuration{}",
            filter.map(|f| format!(" (filter: {f})")).unwrap_or_default()
        ),
        headers: vec!["Key".into(), "Value".into()],
        rows,
        metadata: vec![("Total configs".into(), count.to_string())],
    }))
}
