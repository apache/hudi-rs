// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `hudi-cli` - A lightweight CLI for inspecting and diagnosing Apache Hudi tables.
//!
//! Designed for both human operators and AI agents. Every command supports
//! `--output json` for machine-parseable output.

mod commands;
mod output;

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(
    name = "hudi-cli",
    version,
    about = "Inspect and diagnose Apache Hudi tables",
    long_about = "A lightweight CLI for inspecting and diagnosing Apache Hudi tables.\n\
                   Works with local filesystem, S3, GCS, and Azure storage.\n\
                   No Spark or JVM required.",
    after_help = "EXAMPLES:\n  \
        hudi-cli table info s3://bucket/my_table\n  \
        hudi-cli timeline show /data/my_table --limit 20\n  \
        hudi-cli files stats /data/my_table --output json\n  \
        hudi-cli diagnose s3://bucket/my_table --output json"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Output format
    #[arg(long, global = true, default_value = "table", env = "HUDI_OUTPUT")]
    output: OutputFormat,

    /// Storage options (key=value pairs, e.g. aws_region=us-east-1)
    #[arg(long = "storage-opt", short = 'S', global = true, value_parser = parse_key_val)]
    storage_options: Vec<(String, String)>,
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    /// Human-readable ASCII table
    Table,
    /// Machine-parseable JSON
    Json,
    /// Compact JSON (one object per line)
    Jsonl,
}

#[derive(Subcommand)]
enum Commands {
    /// Table metadata and configuration
    #[command(subcommand)]
    Table(TableCommands),

    /// Timeline inspection
    #[command(subcommand)]
    Timeline(TimelineCommands),

    /// Commit history and write statistics
    #[command(subcommand)]
    Commits(CommitsCommands),

    /// File listing and size analysis
    #[command(subcommand)]
    Files(FilesCommands),

    /// Schema inspection and evolution history
    #[command(subcommand)]
    Schema(SchemaCommands),

    /// Comprehensive health check
    Diagnose {
        /// Table base path (local, s3://, gs://, az://)
        path: String,

        /// Include detailed file-level analysis (slower)
        #[arg(long, default_value = "false")]
        detailed: bool,
    },
}

// -- Table subcommands --

#[derive(Subcommand)]
enum TableCommands {
    /// Show table type, version, partition fields, record key, precombine
    Info {
        /// Table base path
        path: String,
    },
    /// Show effective table configuration
    Config {
        /// Table base path
        path: String,
        /// Filter configs by prefix (e.g. "hoodie.compaction")
        #[arg(long)]
        filter: Option<String>,
    },
}

// -- Timeline subcommands --

#[derive(Subcommand)]
enum TimelineCommands {
    /// Show timeline instants (all actions and states)
    Show {
        /// Table base path
        path: String,
        /// Maximum number of instants to show
        #[arg(long, short, default_value = "30")]
        limit: usize,
        /// Filter by action type (commit, deltacommit, compaction, clean, rollback, etc.)
        #[arg(long)]
        action: Option<String>,
        /// Filter by state (requested, inflight, completed)
        #[arg(long)]
        state: Option<String>,
    },
    /// Show only pending (requested/inflight) operations
    Pending {
        /// Table base path
        path: String,
    },
}

// -- Commits subcommands --

#[derive(Subcommand)]
enum CommitsCommands {
    /// Show recent commits with write statistics
    Show {
        /// Table base path
        path: String,
        /// Maximum number of commits to show
        #[arg(long, short, default_value = "20")]
        limit: usize,
    },
}

// -- Files subcommands --

#[derive(Subcommand)]
enum FilesCommands {
    /// List file groups per partition
    List {
        /// Table base path
        path: String,
        /// Filter by partition path
        #[arg(long)]
        partition: Option<String>,
        /// Maximum number of entries
        #[arg(long, short, default_value = "50")]
        limit: usize,
    },
    /// Show file size distribution and small file analysis
    Stats {
        /// Table base path
        path: String,
    },
}

// -- Schema subcommands --

#[derive(Subcommand)]
enum SchemaCommands {
    /// Show current table schema
    Show {
        /// Table base path
        path: String,
    },
}

fn parse_key_val(s: &str) -> Result<(String, String), String> {
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=VALUE: no `=` found in `{s}`"))?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let storage_opts: std::collections::HashMap<String, String> =
        cli.storage_options.into_iter().collect();
    let fmt = cli.output;

    let result = match cli.command {
        Commands::Table(sub) => match sub {
            TableCommands::Info { path } => commands::table::info(&path, &storage_opts).await,
            TableCommands::Config { path, filter } => {
                commands::table::config(&path, &storage_opts, filter.as_deref()).await
            }
        },
        Commands::Timeline(sub) => match sub {
            TimelineCommands::Show {
                path,
                limit,
                action,
                state,
            } => {
                commands::timeline::show(
                    &path,
                    &storage_opts,
                    limit,
                    action.as_deref(),
                    state.as_deref(),
                )
                .await
            }
            TimelineCommands::Pending { path } => {
                commands::timeline::pending(&path, &storage_opts).await
            }
        },
        Commands::Commits(sub) => match sub {
            CommitsCommands::Show { path, limit } => {
                commands::commits::show(&path, &storage_opts, limit).await
            }
        },
        Commands::Files(sub) => match sub {
            FilesCommands::List {
                path,
                partition,
                limit,
            } => commands::files::list(&path, &storage_opts, partition.as_deref(), limit).await,
            FilesCommands::Stats { path } => {
                commands::files::stats(&path, &storage_opts).await
            }
        },
        Commands::Schema(sub) => match sub {
            SchemaCommands::Show { path } => {
                commands::schema::show(&path, &storage_opts).await
            }
        },
        Commands::Diagnose { path, detailed } => {
            commands::diagnose::run(&path, &storage_opts, detailed).await
        }
    };

    match result {
        Ok(output) => {
            output.render(&fmt);
            Ok(())
        }
        Err(e) => {
            match fmt {
                OutputFormat::Json | OutputFormat::Jsonl => {
                    let err = serde_json::json!({
                        "error": true,
                        "message": format!("{e:#}")
                    });
                    eprintln!("{}", serde_json::to_string_pretty(&err).unwrap());
                }
                OutputFormat::Table => {
                    eprintln!("Error: {e:#}");
                }
            }
            std::process::exit(1);
        }
    }
}
