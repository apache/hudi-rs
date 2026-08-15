// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::output::{format_bytes, CommandOutput, OutputSection};

use super::{empty_filters, open_table};

/// List file slices per partition.
pub async fn list(
    path: &str,
    storage_opts: &HashMap<String, String>,
    partition_filter: Option<&str>,
    limit: usize,
) -> Result<CommandOutput> {
    let table = open_table(path, storage_opts).await?;
    let file_slices = table.get_file_slices(empty_filters()).await?;

    let mut rows: Vec<Vec<String>> = Vec::new();

    for fs in &file_slices {
        let partition = &fs.partition_path;
        let partition_display = if partition.is_empty() {
            "(root)"
        } else {
            partition.as_str()
        };

        if let Some(filter) = partition_filter {
            if !partition_display.contains(filter) {
                continue;
            }
        }

        let base_size = fs
            .base_file
            .file_metadata
            .as_ref()
            .map(|m| m.size)
            .unwrap_or(0);
        let num_logs = fs.log_files.len();
        let file_id = fs.file_id();

        rows.push(vec![
            partition_display.to_string(),
            file_id.to_string(),
            format_bytes(base_size),
            base_size.to_string(),
            num_logs.to_string(),
            if fs.has_log_file() { "Yes" } else { "No" }.to_string(),
        ]);

        if rows.len() >= limit {
            break;
        }
    }

    Ok(CommandOutput::single(OutputSection {
        title: "File Slices".into(),
        headers: vec![
            "Partition".into(),
            "File ID".into(),
            "Base Size".into(),
            "Base Bytes".into(),
            "Log Files".into(),
            "Has Logs".into(),
        ],
        rows,
        metadata: vec![("Total file slices".into(), file_slices.len().to_string())],
    }))
}

/// Show file size distribution and small file analysis.
pub async fn stats(
    path: &str,
    storage_opts: &HashMap<String, String>,
) -> Result<CommandOutput> {
    let table = open_table(path, storage_opts).await?;
    let file_slices = table.get_file_slices(empty_filters()).await?;

    let mut base_sizes: Vec<u64> = Vec::new();
    let mut total_log_files: usize = 0;
    let mut partitions: HashSet<String> = HashSet::new();

    for fs in &file_slices {
        let size = fs
            .base_file
            .file_metadata
            .as_ref()
            .map(|m| m.size)
            .unwrap_or(0);
        base_sizes.push(size);
        total_log_files += fs.log_files.len();
        let p = if fs.partition_path.is_empty() {
            "(root)".to_string()
        } else {
            fs.partition_path.clone()
        };
        partitions.insert(p);
    }

    if base_sizes.is_empty() {
        return Ok(CommandOutput::single(OutputSection {
            title: "File Statistics".into(),
            headers: vec![],
            rows: vec![],
            metadata: vec![("Status".into(), "No files found".into())],
        }));
    }

    base_sizes.sort();
    let total_files = base_sizes.len();
    let total_size: u64 = base_sizes.iter().sum();
    let avg_size = total_size / total_files as u64;
    let min_size = base_sizes[0];
    let max_size = base_sizes[total_files - 1];
    let median_size = base_sizes[total_files / 2];
    let p10_size = base_sizes[total_files * 10 / 100];
    let p90_size = base_sizes[total_files * 90 / 100];

    // Small file analysis (< 100MB default threshold)
    let small_threshold: u64 = 100 * 1024 * 1024;
    let small_files = base_sizes.iter().filter(|&&s| s < small_threshold).count();
    let small_pct = (small_files as f64 / total_files as f64) * 100.0;

    // Size distribution buckets
    let buckets: Vec<(&str, u64, u64)> = vec![
        ("< 1 MB", 0, 1024 * 1024),
        ("1-10 MB", 1024 * 1024, 10 * 1024 * 1024),
        ("10-50 MB", 10 * 1024 * 1024, 50 * 1024 * 1024),
        ("50-100 MB", 50 * 1024 * 1024, 100 * 1024 * 1024),
        ("100-200 MB", 100 * 1024 * 1024, 200 * 1024 * 1024),
        ("200-500 MB", 200 * 1024 * 1024, 500 * 1024 * 1024),
        ("> 500 MB", 500 * 1024 * 1024, u64::MAX),
    ];

    let dist_rows: Vec<Vec<String>> = buckets
        .iter()
        .map(|(label, lo, hi)| {
            let count = base_sizes.iter().filter(|&&s| s >= *lo && s < *hi).count();
            let pct = (count as f64 / total_files as f64) * 100.0;
            vec![label.to_string(), count.to_string(), format!("{pct:.1}%")]
        })
        .filter(|row| row[1] != "0")
        .collect();

    let sections = vec![
        OutputSection {
            title: "File Statistics".into(),
            headers: vec![],
            rows: vec![],
            metadata: vec![
                ("Total partitions".into(), partitions.len().to_string()),
                ("Total file groups".into(), total_files.to_string()),
                ("Total log files".into(), total_log_files.to_string()),
                ("Total size".into(), format_bytes(total_size)),
                ("Avg file size".into(), format_bytes(avg_size)),
                ("Min file size".into(), format_bytes(min_size)),
                ("Max file size".into(), format_bytes(max_size)),
                ("Median file size".into(), format_bytes(median_size)),
                ("P10 file size".into(), format_bytes(p10_size)),
                ("P90 file size".into(), format_bytes(p90_size)),
                (
                    "Small files (<100MB)".into(),
                    format!("{small_files} ({small_pct:.1}%)"),
                ),
            ],
        },
        OutputSection {
            title: "Size Distribution".into(),
            headers: vec!["Bucket".into(), "Count".into(), "Percentage".into()],
            rows: dist_rows,
            metadata: vec![],
        },
    ];

    Ok(CommandOutput::multi(sections))
}
