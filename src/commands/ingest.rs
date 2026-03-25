use crate::error::{Result, SearchDbError};
use crate::ingest::{self, WriteMode};
use arrow::record_batch::RecordBatch;

/// Run the ingest command: read source files and write to a Delta table.
///
/// Resolves source files (glob or single path or stdin), detects format,
/// reads into Arrow RecordBatches, writes to Delta.
pub async fn run(
    source: Option<&str>,
    delta_uri: &str,
    format: Option<&str>,
    mode: &str,
    batch_size: usize,
) -> Result<()> {
    let write_mode = match mode {
        "append" => WriteMode::Append,
        _ => WriteMode::Overwrite,
    };

    let batches = match source {
        Some(src) => read_from_source(src, format, batch_size)?,
        None => read_from_stdin(format, batch_size)?,
    };

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total_rows == 0 {
        eprintln!("[dsrch] No rows to ingest");
        return Ok(());
    }

    eprintln!("[dsrch] Read {total_rows} row(s) from source");

    let version = ingest::write_delta(delta_uri, batches, write_mode).await?;
    eprintln!("[dsrch] Wrote {total_rows} row(s) to Delta table at version {version}");

    Ok(())
}

/// Read all files matching a source pattern.
fn read_from_source(
    source: &str,
    format_override: Option<&str>,
    batch_size: usize,
) -> Result<Vec<RecordBatch>> {
    let files = ingest::expand_source(source)?;
    eprintln!("[dsrch] Found {} source file(s)", files.len());

    let mut all_batches = Vec::new();
    for file_path in &files {
        let format = match format_override {
            Some(f) => ingest::parse_format(f)?,
            None => ingest::detect_format(file_path).ok_or_else(|| {
                SearchDbError::Schema(format!(
                    "Cannot detect format for '{}'. Use --format to specify.",
                    file_path
                ))
            })?,
        };

        let batches = ingest::read_file(file_path, format, batch_size)?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        eprintln!("[dsrch]   {file_path}: {rows} row(s)");
        all_batches.extend(batches);
    }

    Ok(all_batches)
}

/// Read from stdin (requires --format).
fn read_from_stdin(format_override: Option<&str>, batch_size: usize) -> Result<Vec<RecordBatch>> {
    let format_str = format_override.ok_or_else(|| {
        SearchDbError::Schema("Reading from stdin requires --format (cannot auto-detect)".into())
    })?;
    let format = ingest::parse_format(format_str)?;

    if atty::is(atty::Stream::Stdin) {
        return Err(SearchDbError::Schema(
            "No source specified. Use --source or pipe data to stdin.".into(),
        ));
    }

    eprintln!("[dsrch] Reading from stdin ({format_str})");
    let stdin = std::io::stdin();
    ingest::read_from_reader(stdin.lock(), format, batch_size)
}
