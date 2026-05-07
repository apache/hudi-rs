// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

use std::collections::HashMap;

use anyhow::Result;

use crate::output::{CommandOutput, OutputSection};

use super::open_table;

/// Show recent commits with write statistics.
pub async fn show(
    path: &str,
    storage_opts: &HashMap<String, String>,
    limit: usize,
) -> Result<CommandOutput> {
    let table = open_table(path, storage_opts).await?;
    let timeline = table.get_timeline();

    let total = timeline.completed_commits.len();

    let mut instants: Vec<_> = timeline.completed_commits.iter().collect();
    instants.reverse();
    instants.truncate(limit);

    let rows: Vec<Vec<String>> = instants
        .iter()
        .map(|i| {
            vec![
                i.timestamp.clone(),
                i.action.as_ref().to_string(),
                format!("{:?}", i.state),
            ]
        })
        .collect();

    Ok(CommandOutput::single(OutputSection {
        title: "Commits".into(),
        headers: vec![
            "Timestamp".into(),
            "Action".into(),
            "State".into(),
        ],
        rows,
        metadata: vec![
            ("Total commits".into(), total.to_string()),
            ("Showing".into(), format!("latest {}", instants.len())),
        ],
    }))
}
