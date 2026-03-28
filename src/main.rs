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
#[cfg(feature = "metrics")]
mod metrics;
#[cfg(feature = "delta")]
#[allow(dead_code)]
mod object_store_dir;
mod pipeline;
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
        use std::io::IsTerminal;

        match explicit {
            Some(f) => f,
            None => {
                if std::io::stdout().is_terminal() {
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
    name = "dewey",
    about = "Dewey — search your lakehouse. Embedded search backed by tantivy + Delta Lake.",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Data directory for indexes (also settable via DEWEY_DATA_DIR env var)
    #[arg(
        long,
        short = 'd',
        global = true,
        default_value = ".dewey",
        env = "DEWEY_DATA_DIR"
    )]
    data_dir: String,

    /// Output format: json or text (default: text in terminal, json when piped)
    #[arg(long, global = true, value_enum)]
    output: Option<OutputFormat>,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new index (schema inferred from Delta source or provided explicitly)
    Create {
        /// Index name
        name: String,
        /// Delta Lake source (path or URI) — schema inferred from Arrow metadata
        #[arg(long)]
        source: Option<String>,
        /// Schema JSON: {"fields":{"name":"keyword","notes":"text"}}
        #[arg(long)]
        schema: Option<String>,
        /// Overwrite if index already exists
        #[arg(long, default_value_t = false)]
        overwrite: bool,
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

    /// Search an index
    Search {
        /// Index name
        name: String,
        /// Search query (multi-match across all fields). Positional.
        query: Option<String>,
        /// Tantivy filter syntax (e.g., '+field:value -field:value')
        #[arg(short = 'f', long = "filter")]
        filter: Option<String>,
        /// Elasticsearch query DSL (JSON string, @file, or - for stdin)
        #[arg(long, conflicts_with_all = ["query", "filter"])]
        dsl: Option<String>,
        /// Maximum number of results
        #[arg(short = 'l', long, default_value_t = 20)]
        limit: usize,
        /// Skip first N results (pagination)
        #[arg(long, default_value_t = 0)]
        offset: usize,
        /// Comma-separated list of fields to return
        #[arg(long, value_delimiter = ',')]
        fields: Option<Vec<String>>,
        /// Sort by field (e.g., "views", "views:asc", "published:desc")
        #[arg(short = 's', long)]
        sort: Option<String>,
        /// Include relevance score in output
        #[arg(long, default_value_t = false)]
        score: bool,
        /// Aggregation request JSON (ES-compatible, e.g. '{"by_status": {"terms": {"field": "status"}}}')
        #[arg(long)]
        agg: Option<String>,
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

    /// Full rebuild from Delta Lake source
    #[cfg(feature = "delta")]
    Reindex {
        /// Index name
        name: String,
        /// Rebuild as of a specific Delta version
        #[arg(long)]
        as_of_version: Option<i64>,
    },

    /// Run the librarian — keeps the index fresh from Delta Lake
    #[cfg(feature = "delta")]
    Librarian {
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
        /// Port for Prometheus /metrics endpoint (0 to disable)
        #[cfg(feature = "metrics")]
        #[arg(long, default_value_t = 9090)]
        metrics_port: u16,
    },

    /// Convert raw files (NDJSON, JSON, CSV, Parquet) into a Delta Lake table
    #[cfg(feature = "delta")]
    ToDelta {
        /// Source file(s) — glob pattern, single path, or - for stdin
        source: String,
        /// Delta Lake table destination (path or URI)
        target: String,
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
    // Register cloud storage handlers so deltalake recognizes az://, s3://, gs:// URIs
    deltalake::azure::register_handlers(None);
    deltalake::aws::register_handlers(None);
    deltalake::gcp::register_handlers(None);
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
        Commands::Create {
            name,
            source,
            schema,
            overwrite,
            dry_run,
        } => {
            if let Some(ref src) = source {
                create_from_delta(&storage, &name, src, overwrite, dry_run).await
            } else {
                commands::new_index::run(&storage, &name, schema.as_deref(), overwrite, dry_run)
            }
        }
        Commands::Index { name, file } => commands::index::run(&storage, &name, file.as_deref()),
        Commands::Search {
            name,
            query,
            filter,
            dsl,
            limit,
            offset,
            fields,
            sort,
            score,
            agg,
        } => {
            let (gap_rows, _) = read_gap(&storage, &name).await;
            commands::search::run(
                &storage,
                &name,
                query.as_deref(),
                filter.as_deref(),
                dsl.as_deref(),
                limit,
                offset,
                fields,
                score,
                fmt,
                &gap_rows,
                sort.as_deref(),
                agg.as_deref(),
            )
        }
        Commands::Get {
            name,
            doc_id,
            fields,
        } => {
            let (gap_rows, _) = read_gap(&storage, &name).await;
            commands::get::run(&storage, &name, &doc_id, fields, fmt, &gap_rows)
        }
        Commands::Stats { name } => {
            let gap_info = read_gap_info(&storage, &name).await;
            commands::stats::run(&storage, &name, fmt, gap_info)
        }
        Commands::Drop { name } => commands::drop::run(&storage, &name),
        Commands::Reindex {
            name,
            as_of_version,
        } => commands::reindex::run(&storage, &name, as_of_version).await,
        Commands::Librarian {
            name,
            segment_size,
            merge_interval,
            max_segments,
            poll_interval,
            max_segment_age,
            force_merge,
            once,
            #[cfg(feature = "metrics")]
            metrics_port,
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
            commands::compact::run(
                &storage,
                &name,
                opts,
                #[cfg(feature = "metrics")]
                metrics_port,
            )
            .await
        }
        Commands::ToDelta {
            source,
            target,
            format,
            mode,
            batch_size,
        } => {
            let src = if source == "-" {
                None
            } else {
                Some(source.as_str())
            };
            commands::ingest::run(src, &target, format.as_deref(), &mode, batch_size).await
        }
    };

    handle_error(result, fmt);
}

/// Create an index from a Delta Lake source, inferring schema from Arrow metadata.
#[cfg(feature = "delta")]
async fn create_from_delta(
    storage: &Storage,
    name: &str,
    src: &str,
    overwrite: bool,
    dry_run: bool,
) -> error::Result<()> {
    let delta = crate::delta::DeltaSync::new(src);
    let arrow_schema = delta.arrow_schema().await.map_err(|e| {
        crate::error::SearchDbError::Delta(format!("failed to read Delta schema: {e}"))
    })?;
    let inferred = crate::schema::from_arrow_schema(&arrow_schema);
    let schema_json = serde_json::to_string(&inferred)?;

    commands::new_index::run(storage, name, Some(&schema_json), overwrite, dry_run)?;

    if !dry_run {
        let mut config = storage.load_config(name)?;
        config.delta_source = Some(src.to_string());
        config.inferred = true;
        storage.save_config(name, &config)?;
        eprintln!("[dewey] Delta source set to {src}");
    }
    Ok(())
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
            eprintln!("[dewey] Delta read warning: {e}");
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
            eprintln!("[dewey] Delta gap read warning: {e}");
            return (vec![], gap_versions);
        }
    };
    (rows, gap_versions)
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
        Commands::Create {
            name,
            schema,
            overwrite,
            dry_run,
            ..
        } => commands::new_index::run(&storage, &name, schema.as_deref(), overwrite, dry_run),
        Commands::Index { name, file } => commands::index::run(&storage, &name, file.as_deref()),
        Commands::Search {
            name,
            query,
            filter,
            dsl,
            limit,
            offset,
            fields,
            sort,
            score,
            agg,
        } => commands::search::run(
            &storage,
            &name,
            query.as_deref(),
            filter.as_deref(),
            dsl.as_deref(),
            limit,
            offset,
            fields,
            score,
            fmt,
            &[],
            sort.as_deref(),
            agg.as_deref(),
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
                eprintln!("[dewey] Error: {e}");
            }
        }
        std::process::exit(1);
    }
}
