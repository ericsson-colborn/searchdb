mod commands;
#[cfg(feature = "delta")]
mod compact;
#[cfg(feature = "delta")]
mod delta;
mod error;
mod es_dsl;
#[cfg(feature = "delta")]
mod ingest;
mod merge_policy;
mod schema;
mod searcher;
mod storage;
mod writer;

use clap::{Parser, Subcommand, ValueEnum};
use storage::Storage;

/// Output format for commands that return data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// NDJSON to stdout (default when piped)
    Json,
    /// Human-readable table (default in terminal)
    Text,
}

impl OutputFormat {
    /// Resolve the output format: use explicit choice or auto-detect from TTY.
    fn resolve(explicit: Option<OutputFormat>) -> OutputFormat {
        match explicit {
            Some(f) => f,
            None => {
                if atty::is(atty::Stream::Stdout) {
                    OutputFormat::Text
                } else {
                    OutputFormat::Json
                }
            }
        }
    }
}

#[derive(Parser)]
#[command(
    name = "dsrch",
    about = "deltasearch — ES in your pocket. Embedded search backed by tantivy + Delta Lake.",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Data directory for indexes
    #[arg(long, global = true, default_value = ".dsrch")]
    data_dir: String,

    /// Output format: json or text (default: text in terminal, json when piped)
    #[arg(long, global = true, value_enum)]
    output: Option<OutputFormat>,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new index (schema optional — inferred from data if omitted)
    New {
        /// Index name
        name: String,
        /// Schema JSON: {"fields":{"name":"keyword","notes":"text"}}
        #[arg(long)]
        schema: Option<String>,
        /// Overwrite if index already exists
        #[arg(long, default_value_t = false)]
        overwrite: bool,
        /// Infer schema from an NDJSON sample file (file is NOT indexed)
        #[arg(long)]
        infer_from: Option<String>,
        /// Print inferred schema as JSON and exit without creating
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },

    /// Bulk index NDJSON documents (stdin or file)
    Index {
        /// Index name
        name: String,
        /// Path to NDJSON file (default: read from stdin)
        #[arg(short = 'f', long)]
        file: Option<String>,
    },

    /// Search an index with a query string or ES DSL
    Search {
        /// Index name
        name: String,
        /// Tantivy query string (Lucene-like syntax)
        #[arg(short, long, conflicts_with = "dsl")]
        query: Option<String>,
        /// Elasticsearch query DSL (JSON string, @file, or - for stdin)
        #[arg(long, conflicts_with = "query")]
        dsl: Option<String>,
        /// Maximum number of results
        #[arg(long, default_value_t = 20)]
        limit: usize,
        /// Skip first N results (pagination)
        #[arg(long, default_value_t = 0)]
        offset: usize,
        /// Comma-separated list of fields to return
        #[arg(long, value_delimiter = ',')]
        fields: Option<Vec<String>>,
        /// Include relevance score in output
        #[arg(long, default_value_t = false)]
        score: bool,
    },

    /// Get a single document by _id
    Get {
        /// Index name
        name: String,
        /// Document _id
        doc_id: String,
        /// Comma-separated list of fields to return
        #[arg(long, value_delimiter = ',')]
        fields: Option<Vec<String>>,
    },

    /// Show index statistics
    Stats {
        /// Index name
        name: String,
    },

    /// Delete an index and all its data
    Drop {
        /// Index name
        name: String,
    },

    /// Attach a Delta Lake source and perform initial full load
    #[cfg(feature = "delta")]
    ConnectDelta {
        /// Index name
        name: String,
        /// Path or URI to Delta table root
        #[arg(long)]
        source: String,
        /// Schema JSON (optional — inferred from Arrow schema if omitted)
        #[arg(long)]
        schema: Option<String>,
        /// Print inferred schema as JSON and exit without creating
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },

    /// Manual incremental sync from Delta Lake source
    #[cfg(feature = "delta")]
    Sync {
        /// Index name
        name: String,
    },

    /// Full rebuild from Delta Lake source
    #[cfg(feature = "delta")]
    Reindex {
        /// Index name
        name: String,
        /// Rebuild as of a specific Delta version
        #[arg(long)]
        as_of_version: Option<i64>,
    },

    /// Run the compaction worker (segment creation + merge)
    #[cfg(feature = "delta")]
    Compact {
        /// Index name
        name: String,
        /// Rows per segment before commit
        #[arg(long, default_value_t = 10_000)]
        segment_size: usize,
        /// Minutes between merge checks
        #[arg(long, default_value_t = 5)]
        merge_interval: u64,
        /// Merge when segment count exceeds this
        #[arg(long, default_value_t = 10)]
        max_segments: usize,
        /// Seconds between Delta polls
        #[arg(long, default_value_t = 10)]
        poll_interval: u64,
        /// Force commit after N seconds even if under segment_size
        #[arg(long, default_value_t = 60)]
        max_segment_age: u64,
        /// One-shot: merge all segments into one and exit
        #[arg(long)]
        force_merge: bool,
        /// One-shot: poll once, segment if needed, merge if needed, exit
        #[arg(long)]
        once: bool,
    },

    /// Ingest raw files (NDJSON, JSON, CSV, Parquet) into a Delta Lake table
    #[cfg(feature = "delta")]
    Ingest {
        /// Source file(s) — glob pattern or single path (omit for stdin)
        #[arg(long)]
        source: Option<String>,
        /// Delta Lake table destination (path or URI)
        #[arg(long)]
        delta: String,
        /// File format: ndjson, json, csv, parquet (auto-detected from extension)
        #[arg(long)]
        format: Option<String>,
        /// Write mode: overwrite (default) or append
        #[arg(long, default_value = "overwrite")]
        mode: String,
        /// Rows per read batch
        #[arg(long, default_value_t = 10_000)]
        batch_size: usize,
    },
}

#[cfg(feature = "delta")]
#[tokio::main]
async fn main() {
    run_cli().await;
}

#[cfg(not(feature = "delta"))]
fn main() {
    run_cli_sync();
}

#[cfg(feature = "delta")]
async fn run_cli() {
    env_logger::init();
    let cli = Cli::parse();
    let storage = Storage::new(&cli.data_dir);
    let fmt = OutputFormat::resolve(cli.output);

    let result = match cli.command {
        Commands::New {
            name,
            schema,
            overwrite,
            infer_from,
            dry_run,
        } => commands::new_index::run(
            &storage,
            &name,
            schema.as_deref(),
            overwrite,
            infer_from.as_deref(),
            dry_run,
        ),
        Commands::Index { name, file } => commands::index::run(&storage, &name, file.as_deref()),
        Commands::Search {
            name,
            query,
            dsl,
            limit,
            offset,
            fields,
            score,
        } => {
            let (gap_rows, gap_versions) = read_gap(&storage, &name).await;
            let result = commands::search::run(
                &storage,
                &name,
                query.as_deref(),
                dsl.as_deref(),
                limit,
                offset,
                fields,
                score,
                fmt,
                &gap_rows,
            );
            maybe_spawn_sync(&cli.data_dir, &name, gap_versions);
            result
        }
        Commands::Get {
            name,
            doc_id,
            fields,
        } => {
            let (gap_rows, gap_versions) = read_gap(&storage, &name).await;
            let result = commands::get::run(&storage, &name, &doc_id, fields, fmt, &gap_rows);
            maybe_spawn_sync(&cli.data_dir, &name, gap_versions);
            result
        }
        Commands::Stats { name } => {
            let gap_info = read_gap_info(&storage, &name).await;
            commands::stats::run(&storage, &name, fmt, gap_info)
        }
        Commands::Drop { name } => commands::drop::run(&storage, &name),
        Commands::ConnectDelta {
            name,
            source,
            schema,
            dry_run,
        } => {
            commands::connect_delta::run(&storage, &name, &source, schema.as_deref(), dry_run).await
        }
        Commands::Sync { name } => commands::sync::run(&storage, &name).await,
        Commands::Reindex {
            name,
            as_of_version,
        } => commands::reindex::run(&storage, &name, as_of_version).await,
        Commands::Compact {
            name,
            segment_size,
            merge_interval,
            max_segments,
            poll_interval,
            max_segment_age,
            force_merge,
            once,
        } => {
            let opts = compact::CompactOptions {
                segment_size,
                merge_interval_secs: merge_interval * 60,
                max_segments,
                poll_interval_secs: poll_interval,
                max_segment_age_secs: max_segment_age,
                force_merge,
                once,
            };
            commands::compact::run(&storage, &name, opts).await
        }
        Commands::Ingest {
            source,
            delta,
            format,
            mode,
            batch_size,
        } => {
            commands::ingest::run(
                source.as_deref(),
                &delta,
                format.as_deref(),
                &mode,
                batch_size,
            )
            .await
        }
    };

    handle_error(result, fmt);
}

/// Read un-indexed Delta rows (the gap between index_version and HEAD).
/// Returns (gap_rows, gap_size_in_versions) without acquiring any lock.
#[cfg(feature = "delta")]
async fn read_gap(storage: &Storage, name: &str) -> (Vec<serde_json::Value>, i64) {
    if !storage.exists(name) {
        return (vec![], 0);
    }
    let config = match storage.load_config(name) {
        Ok(c) => c,
        Err(_) => return (vec![], 0),
    };
    let source = match config.delta_source.as_deref() {
        Some(s) => s,
        None => return (vec![], 0),
    };

    let delta_sync = crate::delta::DeltaSync::new(source);
    let current_version = match delta_sync.current_version().await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[dsrch] Delta read warning: {e}");
            return (vec![], 0);
        }
    };
    let index_version = config.index_version.unwrap_or(-1);

    if current_version <= index_version {
        return (vec![], 0);
    }

    let gap_versions = current_version - index_version;
    let rows = match delta_sync.rows_added_since(index_version).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[dsrch] Delta gap read warning: {e}");
            return (vec![], gap_versions);
        }
    };
    (rows, gap_versions)
}

/// Fire-and-forget background sync if gap exceeds threshold.
#[cfg(feature = "delta")]
fn maybe_spawn_sync(data_dir: &str, name: &str, gap_versions: i64) {
    const GAP_THRESHOLD: i64 = 10;
    if gap_versions <= GAP_THRESHOLD {
        return;
    }
    let exe = match std::env::current_exe() {
        Ok(e) => e,
        Err(_) => return,
    };
    let _ = std::process::Command::new(exe)
        .args(["sync", name, "--data-dir", data_dir])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}

/// Read Delta version info for stats display.
#[cfg(feature = "delta")]
async fn read_gap_info(storage: &Storage, name: &str) -> Option<(i64, i64)> {
    let config = storage.load_config(name).ok()?;
    let source = config.delta_source.as_deref()?;
    let delta_sync = crate::delta::DeltaSync::new(source);
    let delta_version = delta_sync.current_version().await.ok()?;
    let index_version = config.index_version.unwrap_or(-1);
    Some((delta_version, delta_version - index_version))
}

#[cfg(not(feature = "delta"))]
fn run_cli_sync() {
    env_logger::init();
    let cli = Cli::parse();
    let storage = Storage::new(&cli.data_dir);
    let fmt = OutputFormat::resolve(cli.output);

    let result = match cli.command {
        Commands::New {
            name,
            schema,
            overwrite,
            infer_from,
            dry_run,
        } => commands::new_index::run(
            &storage,
            &name,
            schema.as_deref(),
            overwrite,
            infer_from.as_deref(),
            dry_run,
        ),
        Commands::Index { name, file } => commands::index::run(&storage, &name, file.as_deref()),
        Commands::Search {
            name,
            query,
            dsl,
            limit,
            offset,
            fields,
            score,
        } => commands::search::run(
            &storage,
            &name,
            query.as_deref(),
            dsl.as_deref(),
            limit,
            offset,
            fields,
            score,
            fmt,
            &[],
        ),
        Commands::Get {
            name,
            doc_id,
            fields,
        } => commands::get::run(&storage, &name, &doc_id, fields, fmt, &[]),
        Commands::Stats { name } => commands::stats::run(&storage, &name, fmt, None),
        Commands::Drop { name } => commands::drop::run(&storage, &name),
    };

    handle_error(result, fmt);
}

/// Handle command result — structured JSON error in json mode, plain text otherwise.
fn handle_error(result: error::Result<()>, fmt: OutputFormat) {
    if let Err(e) = result {
        match fmt {
            OutputFormat::Json => {
                let error_json = serde_json::json!({"error": e.to_string()});
                eprintln!("{}", serde_json::to_string(&error_json).unwrap());
            }
            OutputFormat::Text => {
                eprintln!("[dsrch] Error: {e}");
            }
        }
        std::process::exit(1);
    }
}
