// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

use std::collections::{HashMap, HashSet};

use anyhow::Result;

use hudi_core::timeline::instant::Action;

use crate::output::{format_bytes, CommandOutput, OutputSection};

use super::{empty_filters, open_table};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Severity {
    Ok,
    #[allow(dead_code)]
    Info,
    Warning,
    Critical,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Severity::Ok => write!(f, "OK"),
            Severity::Info => write!(f, "INFO"),
            Severity::Warning => write!(f, "WARNING"),
            Severity::Critical => write!(f, "CRITICAL"),
        }
    }
}

struct Finding {
    severity: Severity,
    category: String,
    message: String,
    recommendation: String,
}

/// Run comprehensive health check on a Hudi table.
pub async fn run(
    path: &str,
    storage_opts: &HashMap<String, String>,
    _detailed: bool,
) -> Result<CommandOutput> {
    let table = open_table(path, storage_opts).await?;
    let mut findings: Vec<Finding> = Vec::new();

    // === 1. Basic table info ===
    let table_type = table.table_type();
    let is_mor = table.is_mor();
    let hudi_opts = table.hudi_options();
    let table_version = hudi_opts
        .get("hoodie.table.version")
        .cloned()
        .unwrap_or_else(|| "unknown".into());

    // === 2. Timeline health ===
    let timeline = table.get_timeline();
    let completed = &timeline.completed_commits;

    if completed.is_empty() {
        findings.push(Finding {
            severity: Severity::Warning,
            category: "Timeline".into(),
            message: "No completed commits found.".into(),
            recommendation: "Verify the table path is correct and the table has been written to."
                .into(),
        });
    }

    // Check timeline size (active instants)
    if completed.len() > 100 {
        findings.push(Finding {
            severity: Severity::Warning,
            category: "Timeline".into(),
            message: format!(
                "{} completed instants in active timeline. Consider archiving.",
                completed.len()
            ),
            recommendation:
                "Check hoodie.keep.max.commits and trigger archival if needed.".into(),
        });
    }

    // MoR-specific: check compaction frequency
    if is_mor && !completed.is_empty() {
        let delta_commits: Vec<_> = completed
            .iter()
            .filter(|i| matches!(i.action, Action::DeltaCommit))
            .collect();
        let replace_commits: Vec<_> = completed
            .iter()
            .filter(|i| matches!(i.action, Action::ReplaceCommit))
            .collect();

        if !delta_commits.is_empty() && replace_commits.is_empty() {
            findings.push(Finding {
                severity: Severity::Warning,
                category: "Compaction".into(),
                message: format!(
                    "{} delta commits but no compactions detected. Read performance may be degraded.",
                    delta_commits.len()
                ),
                recommendation:
                    "Enable compaction (inline or async). Check hoodie.compact.inline and hoodie.compact.inline.max.delta.commits."
                        .into(),
            });
        } else if delta_commits.len() > 20 {
            let ratio = if replace_commits.is_empty() {
                delta_commits.len() as f64
            } else {
                delta_commits.len() as f64 / replace_commits.len() as f64
            };
            if ratio > 10.0 {
                findings.push(Finding {
                    severity: Severity::Warning,
                    category: "Compaction".into(),
                    message: format!(
                        "Delta-to-compaction ratio is {ratio:.1}:1. Compaction may not be keeping up."
                    ),
                    recommendation:
                        "Reduce hoodie.compact.inline.max.delta.commits or add compaction resources."
                            .into(),
                });
            }
        }
    }

    // === 3. File health ===
    let file_slices = table.get_file_slices(empty_filters()).await?;
    let total_files = file_slices.len();
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

    if !base_sizes.is_empty() {
        base_sizes.sort();
        let total_size: u64 = base_sizes.iter().sum();
        let avg_size = total_size / total_files as u64;

        // Small file check
        let small_threshold: u64 = 100 * 1024 * 1024;
        let small_files = base_sizes.iter().filter(|&&s| s < small_threshold).count();
        let small_pct = (small_files as f64 / total_files as f64) * 100.0;

        if small_pct > 50.0 {
            findings.push(Finding {
                severity: Severity::Critical,
                category: "File Sizing".into(),
                message: format!(
                    "{small_files}/{total_files} files ({small_pct:.0}%) are under 100MB. Avg: {}.",
                    format_bytes(avg_size)
                ),
                recommendation:
                    "Enable clustering to merge small files. Tune hoodie.parquet.small.file.limit and hoodie.parquet.max.file.size."
                        .into(),
            });
        } else if small_pct > 25.0 {
            findings.push(Finding {
                severity: Severity::Warning,
                category: "File Sizing".into(),
                message: format!(
                    "{small_files}/{total_files} files ({small_pct:.0}%) are under 100MB."
                ),
                recommendation: "Consider clustering or adjusting file sizing configs.".into(),
            });
        }

        // Log file accumulation (MoR)
        if is_mor && total_files > 0 && total_log_files > total_files * 3 {
            findings.push(Finding {
                severity: Severity::Warning,
                category: "Log Files".into(),
                message: format!(
                    "{total_log_files} log files across {total_files} file groups ({:.1} avg/group).",
                    total_log_files as f64 / total_files as f64
                ),
                recommendation:
                    "High log-to-base ratio degrades reads. Compact more frequently.".into(),
            });
        }
    }

    // === 4. All good? ===
    if findings.is_empty() {
        findings.push(Finding {
            severity: Severity::Ok,
            category: "Overall".into(),
            message: "No issues detected.".into(),
            recommendation: String::new(),
        });
    }

    findings.sort_by(|a, b| b.severity.cmp(&a.severity));

    let overall = findings
        .iter()
        .map(|f| f.severity)
        .max()
        .unwrap_or(Severity::Ok);

    let total_size: u64 = base_sizes.iter().sum();
    let summary_metadata = vec![
        ("Table".into(), path.to_string()),
        ("Type".into(), table_type),
        ("Version".into(), table_version),
        ("Health".into(), overall.to_string()),
        ("Completed instants".into(), completed.len().to_string()),
        ("Total file groups".into(), total_files.to_string()),
        ("Partitions".into(), partitions.len().to_string()),
        ("Total size".into(), format_bytes(total_size)),
        ("Total log files".into(), total_log_files.to_string()),
    ];

    let finding_rows: Vec<Vec<String>> = findings
        .iter()
        .map(|f| {
            vec![
                f.severity.to_string(),
                f.category.clone(),
                f.message.clone(),
                f.recommendation.clone(),
            ]
        })
        .collect();

    Ok(CommandOutput::multi(vec![
        OutputSection {
            title: "Diagnostic Summary".into(),
            headers: vec![],
            rows: vec![],
            metadata: summary_metadata,
        },
        OutputSection {
            title: "Findings".into(),
            headers: vec![
                "Severity".into(),
                "Category".into(),
                "Finding".into(),
                "Recommendation".into(),
            ],
            rows: finding_rows,
            metadata: vec![],
        },
    ]))
}
