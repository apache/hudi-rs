// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

use std::collections::HashMap;

use anyhow::Result;

use crate::output::{CommandOutput, OutputSection};

use super::open_table;

/// Show timeline instants with optional filters.
pub async fn show(
    path: &str,
    storage_opts: &HashMap<String, String>,
    limit: usize,
    action_filter: Option<&str>,
    state_filter: Option<&str>,
) -> Result<CommandOutput> {
    let table = open_table(path, storage_opts).await?;
    let timeline = table.get_timeline();

    let instants = &timeline.completed_commits;

    let mut filtered: Vec<_> = instants
        .iter()
        .filter(|instant| {
            let action_match = action_filter
                .map(|a| instant.action.as_ref().eq_ignore_ascii_case(a))
                .unwrap_or(true);
            let state_match = state_filter
                .map(|s| {
                    let state_str = match &instant.state {
                        hudi_core::timeline::instant::State::Requested => "requested",
                        hudi_core::timeline::instant::State::Inflight => "inflight",
                        hudi_core::timeline::instant::State::Completed => "completed",
                    };
                    state_str.eq_ignore_ascii_case(s)
                })
                .unwrap_or(true);
            action_match && state_match
        })
        .collect();

    // Most recent first
    filtered.reverse();
    filtered.truncate(limit);

    let rows: Vec<Vec<String>> = filtered
        .iter()
        .map(|i| {
            vec![
                i.timestamp.clone(),
                i.action.as_ref().to_string(),
                format!("{:?}", i.state),
            ]
        })
        .collect();

    let total = instants.len();

    Ok(CommandOutput::single(OutputSection {
        title: "Timeline".into(),
        headers: vec![
            "Timestamp".into(),
            "Action".into(),
            "State".into(),
        ],
        rows,
        metadata: vec![
            ("Total completed instants".into(), total.to_string()),
            ("Showing".into(), format!("latest {}", filtered.len())),
        ],
    }))
}

/// Show only pending (requested/inflight) operations.
pub async fn pending(
    path: &str,
    storage_opts: &HashMap<String, String>,
) -> Result<CommandOutput> {
    let table = open_table(path, storage_opts).await?;
    let timeline = table.get_timeline();

    let completed = timeline.completed_commits.len();
    let latest = timeline
        .get_latest_commit_timestamp()
        .unwrap_or_else(|_| "none".to_string());

    Ok(CommandOutput::single(OutputSection {
        title: "Pending Operations".into(),
        headers: vec![],
        rows: vec![],
        metadata: vec![
            ("Completed instants".into(), completed.to_string()),
            ("Latest commit".into(), latest),
            (
                "Note".into(),
                "Full pending instant detection requires timeline loader enhancements. \
                 Use `hudi-cli timeline show` for completed instants."
                    .into(),
            ),
        ],
    }))
}
