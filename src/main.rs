mod commands;
#[cfg(feature = "delta")]
mod delta;
mod error;
#[allow(dead_code)] // Wired into search command in a later task
mod es_dsl;
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
    name = "searchdb",
    about = "ES in your pocket — embedded search backed by tantivy + Delta Lake",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Data directory for indexes
    #[arg(long, global = true, default_value = ".searchdb")]
    data_dir: String,

    /// Output format: json or text (default: text in terminal, json when piped)
    #[arg(long, global = true, value_enum)]
    output: Option<OutputFormat>,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new index from a schema declaration
    New {
        /// Index name
        name: String,
        /// Schema JSON: {"fields":{"name":"keyword","notes":"text"}}
        #[arg(long)]
        schema: String,
        /// Overwrite if index already exists
        #[arg(long, default_value_t = false)]
        overwrite: bool,
    },

    /// Bulk index NDJSON documents (stdin or file)
    Index {
        /// Index name
        name: String,
        /// Path to NDJSON file (default: read from stdin)
        #[arg(short = 'f', long)]
        file: Option<String>,
    },

    /// Search an index with a query string
    Search {
        /// Index name
        name: String,
        /// Tantivy query string (Lucene-like syntax)
        #[arg(short, long)]
        query: String,
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
        /// Schema JSON: {"fields":{"name":"keyword","notes":"text"}}
        #[arg(long)]
        schema: String,
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
        } => commands::new_index::run(&storage, &name, &schema, overwrite),
        Commands::Index { name, file } => commands::index::run(&storage, &name, file.as_deref()),
        Commands::Search {
            name,
            query,
            limit,
            offset,
            fields,
            score,
        } => {
            let (gap_rows, gap_versions) = read_gap(&storage, &name).await;
            let result = commands::search::run(
                &storage, &name, &query, limit, offset, fields, score, fmt, &gap_rows,
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
        } => commands::connect_delta::run(&storage, &name, &source, &schema).await,
        Commands::Sync { name } => commands::sync::run(&storage, &name).await,
        Commands::Reindex {
            name,
            as_of_version,
        } => commands::reindex::run(&storage, &name, as_of_version).await,
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
            eprintln!("[searchdb] Delta read warning: {e}");
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
            eprintln!("[searchdb] Delta gap read warning: {e}");
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
        } => commands::new_index::run(&storage, &name, &schema, overwrite),
        Commands::Index { name, file } => commands::index::run(&storage, &name, file.as_deref()),
        Commands::Search {
            name,
            query,
            limit,
            offset,
            fields,
            score,
        } => commands::search::run(
            &storage,
            &name,
            &query,
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
                eprintln!("[searchdb] Error: {e}");
            }
        }
        std::process::exit(1);
    }
}
