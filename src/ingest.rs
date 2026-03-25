use arrow::record_batch::RecordBatch;
use deltalake::protocol::SaveMode;
use deltalake::DeltaOps;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};

use crate::error::{Result, SearchDbError};

/// Supported input file formats for ingest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputFormat {
    /// One JSON object per line
    Ndjson,
    /// JSON array of objects
    Json,
    /// CSV with header row
    Csv,
    /// Apache Parquet (columnar)
    Parquet,
}

/// Write mode for Delta table output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    /// Replace existing table data (or create new).
    Overwrite,
    /// Append to existing table.
    Append,
}

/// Detect input format from file extension.
///
/// Returns None for unrecognized extensions.
pub fn detect_format(path: &str) -> Option<InputFormat> {
    let lower = path.to_lowercase();
    if lower.ends_with(".ndjson") || lower.ends_with(".jsonl") {
        Some(InputFormat::Ndjson)
    } else if lower.ends_with(".json") {
        Some(InputFormat::Json)
    } else if lower.ends_with(".csv") {
        Some(InputFormat::Csv)
    } else if lower.ends_with(".parquet") {
        Some(InputFormat::Parquet)
    } else {
        None
    }
}

/// Parse a format string from CLI --format flag.
pub fn parse_format(s: &str) -> Result<InputFormat> {
    match s.to_lowercase().as_str() {
        "ndjson" | "jsonl" => Ok(InputFormat::Ndjson),
        "json" => Ok(InputFormat::Json),
        "csv" => Ok(InputFormat::Csv),
        "parquet" => Ok(InputFormat::Parquet),
        other => Err(SearchDbError::Schema(format!(
            "Unknown format '{other}'. Supported: ndjson, json, csv, parquet"
        ))),
    }
}

/// Read a file into Arrow RecordBatches.
///
/// Dispatches to format-specific readers based on `format`.
pub fn read_file(path: &str, format: InputFormat, batch_size: usize) -> Result<Vec<RecordBatch>> {
    match format {
        InputFormat::Ndjson => read_ndjson_file(path, batch_size),
        InputFormat::Json => read_json_file(path, batch_size),
        InputFormat::Csv => read_csv_file(path, batch_size),
        InputFormat::Parquet => read_parquet_file(path),
    }
}

/// Read NDJSON file into RecordBatches using arrow-json.
fn read_ndjson_file(path: &str, batch_size: usize) -> Result<Vec<RecordBatch>> {
    let file = File::open(path).map_err(SearchDbError::Io)?;
    read_ndjson_reader(BufReader::new(file), batch_size)
}

/// Read NDJSON from any BufRead source into RecordBatches.
fn read_ndjson_reader(reader: impl BufRead, batch_size: usize) -> Result<Vec<RecordBatch>> {
    // Collect non-empty lines
    let lines: Vec<String> = reader
        .lines()
        .filter_map(|l| l.ok())
        .filter(|l| !l.trim().is_empty())
        .collect();

    if lines.is_empty() {
        return Err(SearchDbError::Schema(
            "No data rows found in NDJSON input".into(),
        ));
    }

    // Infer schema from the NDJSON lines, then build reader
    let joined = lines.join("\n");
    let mut cursor = std::io::Cursor::new(joined.as_bytes().to_vec());

    let (schema, _) = arrow_json::reader::infer_json_schema_from_seekable(&mut cursor, None)
        .map_err(|e| SearchDbError::Schema(format!("Failed to infer NDJSON schema: {e}")))?;

    let schema_ref = std::sync::Arc::new(schema);
    let decoder = arrow_json::ReaderBuilder::new(schema_ref)
        .with_batch_size(batch_size)
        .build(cursor)
        .map_err(|e| SearchDbError::Schema(format!("Failed to build NDJSON reader: {e}")))?;

    let mut batches = Vec::new();
    for batch_result in decoder {
        let batch = batch_result
            .map_err(|e| SearchDbError::Schema(format!("Error reading NDJSON batch: {e}")))?;
        batches.push(batch);
    }

    Ok(batches)
}

/// Read a JSON file (array of objects or single object) into RecordBatches.
///
/// Parses the entire file as JSON, extracts individual objects,
/// converts each to an NDJSON line, and feeds through the NDJSON reader.
fn read_json_file(path: &str, batch_size: usize) -> Result<Vec<RecordBatch>> {
    let content = std::fs::read_to_string(path).map_err(SearchDbError::Io)?;
    let parsed: serde_json::Value = serde_json::from_str(&content)?;

    let objects = match parsed {
        serde_json::Value::Array(arr) => arr,
        serde_json::Value::Object(_) => vec![parsed],
        _ => {
            return Err(SearchDbError::Schema(
                "JSON file must contain an array of objects or a single object".into(),
            ))
        }
    };

    if objects.is_empty() {
        return Err(SearchDbError::Schema("JSON array is empty".into()));
    }

    // Convert to NDJSON lines and feed through NDJSON reader
    let ndjson: String = objects
        .iter()
        .map(|obj| serde_json::to_string(obj).unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n");

    read_ndjson_reader(
        BufReader::new(std::io::Cursor::new(ndjson)),
        batch_size,
    )
}

/// Read a CSV file (with header row) into RecordBatches.
fn read_csv_file(path: &str, batch_size: usize) -> Result<Vec<RecordBatch>> {
    // Infer schema first
    let format = arrow_csv::reader::Format::default().with_header(true);
    let infer_file = File::open(path).map_err(SearchDbError::Io)?;
    let (schema, _) = format
        .infer_schema(infer_file, None)
        .map_err(|e| SearchDbError::Schema(format!("Failed to infer CSV schema: {e}")))?;

    // Build reader with inferred schema
    let file = File::open(path).map_err(SearchDbError::Io)?;
    let reader = arrow_csv::ReaderBuilder::new(std::sync::Arc::new(schema))
        .with_header(true)
        .with_batch_size(batch_size)
        .build(file)
        .map_err(|e| SearchDbError::Schema(format!("Failed to build CSV reader: {e}")))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| SearchDbError::Schema(format!("Error reading CSV batch: {e}")))?;
        batches.push(batch);
    }

    if batches.is_empty() {
        return Err(SearchDbError::Schema("CSV file has no data rows".into()));
    }

    Ok(batches)
}

/// Read a Parquet file into RecordBatches.
///
/// Direct passthrough: Parquet is already in columnar Arrow format.
fn read_parquet_file(path: &str) -> Result<Vec<RecordBatch>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(path).map_err(SearchDbError::Io)?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| SearchDbError::Schema(format!("Invalid Parquet file '{path}': {e}")))?;
    let reader = builder
        .build()
        .map_err(|e| SearchDbError::Schema(format!("Failed to build Parquet reader: {e}")))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| SearchDbError::Schema(format!("Error reading Parquet batch: {e}")))?;
        batches.push(batch);
    }

    if batches.is_empty() {
        return Err(SearchDbError::Schema(
            "Parquet file has no row groups".into(),
        ));
    }

    Ok(batches)
}

/// Write RecordBatches to a Delta Lake table.
///
/// For `WriteMode::Overwrite`: creates a new table or replaces all data.
/// For `WriteMode::Append`: adds rows to an existing table.
///
/// Returns the Delta table version after the write.
pub async fn write_delta(
    delta_uri: &str,
    batches: Vec<RecordBatch>,
    mode: WriteMode,
) -> Result<i64> {
    if batches.is_empty() {
        return Err(SearchDbError::Schema(
            "No data to write to Delta table".into(),
        ));
    }

    let save_mode = match mode {
        WriteMode::Overwrite => SaveMode::Overwrite,
        WriteMode::Append => SaveMode::Append,
    };

    // Try to open existing table; if it fails, create via write
    let table = match deltalake::open_table(delta_uri).await {
        Ok(t) => DeltaOps(t)
            .write(batches)
            .with_save_mode(save_mode)
            .await
            .map_err(|e| SearchDbError::Delta(format!("Delta write failed: {e}")))?,
        Err(_) => {
            // Table doesn't exist yet — create it via write
            DeltaOps::try_from_uri(delta_uri)
                .await
                .map_err(|e| {
                    SearchDbError::Delta(format!(
                        "Cannot access Delta URI '{delta_uri}': {e}"
                    ))
                })?
                .write(batches)
                .with_save_mode(save_mode)
                .await
                .map_err(|e| SearchDbError::Delta(format!("Delta write failed: {e}")))?
        }
    };

    Ok(table.version())
}

/// Expand a source path (possibly a glob pattern) into a sorted list of file paths.
///
/// If the path contains glob metacharacters (* ? [ ]), expands the pattern.
/// If it's a plain path to an existing file, returns that single file.
/// Returns an error if no files match.
pub fn expand_source(source: &str) -> Result<Vec<String>> {
    let has_glob = source.contains('*')
        || source.contains('?')
        || source.contains('[')
        || source.contains(']');

    if has_glob {
        let entries: Vec<String> = glob::glob(source)
            .map_err(|e| SearchDbError::Schema(format!("Invalid glob pattern '{source}': {e}")))?
            .filter_map(|entry| entry.ok())
            .map(|p| p.to_string_lossy().to_string())
            .collect();

        if entries.is_empty() {
            return Err(SearchDbError::Schema(format!(
                "No files match pattern '{source}'"
            )));
        }

        let mut sorted = entries;
        sorted.sort();
        Ok(sorted)
    } else {
        // Single file path
        if !std::path::Path::new(source).exists() {
            return Err(SearchDbError::Schema(format!(
                "Source file not found: '{source}'"
            )));
        }
        Ok(vec![source.to_string()])
    }
}

/// Read data from a generic reader (stdin, pipe, etc.) into RecordBatches.
///
/// Parquet is not supported from non-seekable sources.
pub fn read_from_reader(
    reader: impl Read,
    format: InputFormat,
    batch_size: usize,
) -> Result<Vec<RecordBatch>> {
    match format {
        InputFormat::Ndjson => read_ndjson_reader(BufReader::new(reader), batch_size),
        InputFormat::Json => {
            let mut content = String::new();
            BufReader::new(reader)
                .read_to_string(&mut content)
                .map_err(SearchDbError::Io)?;
            let parsed: serde_json::Value = serde_json::from_str(&content)?;
            let objects = match parsed {
                serde_json::Value::Array(arr) => arr,
                serde_json::Value::Object(_) => vec![parsed],
                _ => {
                    return Err(SearchDbError::Schema(
                        "JSON input must be an array of objects or a single object".into(),
                    ))
                }
            };
            if objects.is_empty() {
                return Err(SearchDbError::Schema("JSON array is empty".into()));
            }
            let ndjson: String = objects
                .iter()
                .map(|obj| serde_json::to_string(obj).unwrap_or_default())
                .collect::<Vec<_>>()
                .join("\n");
            read_ndjson_reader(BufReader::new(std::io::Cursor::new(ndjson)), batch_size)
        }
        InputFormat::Csv => read_csv_from_reader(reader, batch_size),
        InputFormat::Parquet => Err(SearchDbError::Schema(
            "Parquet format requires a seekable file source. Use --source with a file path."
                .into(),
        )),
    }
}

/// Read CSV from a reader with schema inference.
fn read_csv_from_reader(reader: impl Read, batch_size: usize) -> Result<Vec<RecordBatch>> {
    // Read all content into memory so we can infer schema then parse
    let mut content = Vec::new();
    BufReader::new(reader)
        .read_to_end(&mut content)
        .map_err(SearchDbError::Io)?;

    let format = arrow_csv::reader::Format::default().with_header(true);
    let (schema, _) = format
        .infer_schema(std::io::Cursor::new(&content), None)
        .map_err(|e| SearchDbError::Schema(format!("Failed to infer CSV schema: {e}")))?;

    let csv_reader = arrow_csv::ReaderBuilder::new(std::sync::Arc::new(schema))
        .with_header(true)
        .with_batch_size(batch_size)
        .build(std::io::Cursor::new(content))
        .map_err(|e| SearchDbError::Schema(format!("Failed to build CSV reader: {e}")))?;

    let mut batches = Vec::new();
    for batch_result in csv_reader {
        let batch = batch_result
            .map_err(|e| SearchDbError::Schema(format!("Error reading CSV: {e}")))?;
        batches.push(batch);
    }
    if batches.is_empty() {
        return Err(SearchDbError::Schema("CSV input has no data rows".into()));
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_ndjson() {
        assert_eq!(detect_format("data.ndjson"), Some(InputFormat::Ndjson));
        assert_eq!(detect_format("data.jsonl"), Some(InputFormat::Ndjson));
    }

    #[test]
    fn test_detect_json() {
        assert_eq!(detect_format("data.json"), Some(InputFormat::Json));
    }

    #[test]
    fn test_detect_csv() {
        assert_eq!(detect_format("data.csv"), Some(InputFormat::Csv));
    }

    #[test]
    fn test_detect_parquet() {
        assert_eq!(detect_format("data.parquet"), Some(InputFormat::Parquet));
    }

    #[test]
    fn test_detect_unknown() {
        assert_eq!(detect_format("data.xml"), None);
        assert_eq!(detect_format("data"), None);
    }

    #[test]
    fn test_detect_case_insensitive() {
        assert_eq!(detect_format("DATA.JSON"), Some(InputFormat::Json));
        assert_eq!(detect_format("file.CSV"), Some(InputFormat::Csv));
        assert_eq!(detect_format("file.Parquet"), Some(InputFormat::Parquet));
    }

    #[test]
    fn test_detect_with_path() {
        assert_eq!(
            detect_format("/tmp/data/file.ndjson"),
            Some(InputFormat::Ndjson)
        );
        assert_eq!(
            detect_format("./relative/path.csv"),
            Some(InputFormat::Csv)
        );
    }

    #[test]
    fn test_parse_format_string() {
        assert_eq!(parse_format("ndjson").unwrap(), InputFormat::Ndjson);
        assert_eq!(parse_format("jsonl").unwrap(), InputFormat::Ndjson);
        assert_eq!(parse_format("json").unwrap(), InputFormat::Json);
        assert_eq!(parse_format("csv").unwrap(), InputFormat::Csv);
        assert_eq!(parse_format("parquet").unwrap(), InputFormat::Parquet);
    }

    #[test]
    fn test_parse_format_invalid() {
        assert!(parse_format("xml").is_err());
    }

    #[test]
    fn test_read_parquet_file() {
        use arrow::array::{Float64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.parquet");

        // Create a small Parquet file
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["glucose", "a1c"])),
                Arc::new(Float64Array::from(vec![95.0, 6.1])),
            ],
        )
        .unwrap();

        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Now read it back
        let batches = read_file(path.to_str().unwrap(), InputFormat::Parquet, 1024).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_read_csv_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.csv");
        std::fs::write(
            &path,
            "name,value,active\nglucose,95.0,true\na1c,6.1,false\n",
        )
        .unwrap();

        let batches = read_file(path.to_str().unwrap(), InputFormat::Csv, 1024).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        let schema = batches[0].schema();
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("value").is_ok());
        assert!(schema.field_with_name("active").is_ok());
    }

    #[test]
    fn test_read_csv_with_nulls() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nulls.csv");
        std::fs::write(&path, "name,value\nglucose,95.0\na1c,\n").unwrap();

        let batches = read_file(path.to_str().unwrap(), InputFormat::Csv, 1024).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_read_ndjson_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.ndjson");
        std::fs::write(
            &path,
            r#"{"name":"glucose","value":95.0}
{"name":"a1c","value":6.1}
{"name":"creatinine","value":1.2}
"#,
        )
        .unwrap();

        let batches = read_file(path.to_str().unwrap(), InputFormat::Ndjson, 1024).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        // Verify schema has expected columns
        let schema = batches[0].schema();
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("value").is_ok());
    }

    #[test]
    fn test_read_json_array_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.json");
        std::fs::write(
            &path,
            r#"[
                {"name": "glucose", "value": 95.0},
                {"name": "a1c", "value": 6.1}
            ]"#,
        )
        .unwrap();

        let batches = read_file(path.to_str().unwrap(), InputFormat::Json, 1024).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_read_json_single_object_wraps() {
        // A single JSON object (not in array) should also work
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("single.json");
        std::fs::write(&path, r#"{"name": "glucose", "value": 95.0}"#).unwrap();

        let batches = read_file(path.to_str().unwrap(), InputFormat::Json, 1024).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[test]
    fn test_read_ndjson_empty_lines_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.ndjson");
        std::fs::write(
            &path,
            r#"{"name":"glucose"}

{"name":"a1c"}
"#,
        )
        .unwrap();

        let batches = read_file(path.to_str().unwrap(), InputFormat::Ndjson, 1024).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_read_ndjson_from_reader() {
        let data = r#"{"name":"glucose","value":95.0}
{"name":"a1c","value":6.1}
"#;
        let cursor = std::io::Cursor::new(data);
        let batches = read_from_reader(cursor, InputFormat::Ndjson, 1024).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_read_csv_from_reader() {
        let data = "name,value\nglucose,95.0\na1c,6.1\n";
        let cursor = std::io::Cursor::new(data);
        let batches = read_from_reader(cursor, InputFormat::Csv, 1024).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_read_parquet_from_reader_errors() {
        let cursor = std::io::Cursor::new(b"not parquet data");
        let result = read_from_reader(cursor, InputFormat::Parquet, 1024);
        assert!(result.is_err());
    }

    #[test]
    fn test_expand_glob() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.json"), "{}").unwrap();
        std::fs::write(dir.path().join("b.json"), "{}").unwrap();
        std::fs::write(dir.path().join("c.csv"), "x").unwrap();

        let pattern = format!("{}/*.json", dir.path().to_str().unwrap());
        let files = expand_source(&pattern).unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.iter().all(|f| f.ends_with(".json")));
    }

    #[test]
    fn test_expand_single_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.ndjson");
        std::fs::write(&path, "{}").unwrap();

        let files = expand_source(path.to_str().unwrap()).unwrap();
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn test_expand_no_matches() {
        let dir = tempfile::tempdir().unwrap();
        let pattern = format!("{}/*.xyz", dir.path().to_str().unwrap());
        let result = expand_source(&pattern);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_delta_new_table() {
        use arrow::array::{Float64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_out");
        let delta_str = delta_path.to_str().unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["glucose", "a1c"])),
                Arc::new(Float64Array::from(vec![95.0, 6.1])),
            ],
        )
        .unwrap();

        let version = write_delta(delta_str, vec![batch], WriteMode::Overwrite)
            .await
            .unwrap();
        assert!(version >= 0);

        // Verify the Delta table was created and has data
        let table = deltalake::open_table(delta_str).await.unwrap();
        assert!(table.version() >= 0);
    }

    #[tokio::test]
    async fn test_write_delta_append() {
        use arrow::array::{Float64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_out");
        let delta_str = delta_path.to_str().unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        // First write
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["glucose"])),
                Arc::new(Float64Array::from(vec![95.0])),
            ],
        )
        .unwrap();
        write_delta(delta_str, vec![batch1], WriteMode::Overwrite)
            .await
            .unwrap();

        // Append
        let batch2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a1c"])),
                Arc::new(Float64Array::from(vec![6.1])),
            ],
        )
        .unwrap();
        write_delta(delta_str, vec![batch2], WriteMode::Append)
            .await
            .unwrap();

        // Read back via DeltaSync
        let sync = crate::delta::DeltaSync::new(delta_str);
        let rows = sync.full_load(None).await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn test_end_to_end_ndjson_ingest_to_delta() {
        let dir = tempfile::tempdir().unwrap();

        // Create NDJSON source file
        let source_path = dir.path().join("data.ndjson");
        std::fs::write(
            &source_path,
            r#"{"_id":"d1","name":"glucose","value":95.0}
{"_id":"d2","name":"a1c","value":6.1}
{"_id":"d3","name":"creatinine","value":1.2}
"#,
        )
        .unwrap();

        // Ingest to Delta
        let delta_path = dir.path().join("delta_out");
        let delta_str = delta_path.to_str().unwrap();
        let batches =
            read_file(source_path.to_str().unwrap(), InputFormat::Ndjson, 1024).unwrap();
        let version = write_delta(delta_str, batches, WriteMode::Overwrite)
            .await
            .unwrap();
        assert!(version >= 0);

        // Verify Delta table has the data
        let sync = crate::delta::DeltaSync::new(delta_str);
        let rows = sync.full_load(None).await.unwrap();
        assert_eq!(rows.len(), 3);

        // Verify we can read the Delta table's schema
        let arrow_schema = sync.arrow_schema().await.unwrap();
        assert!(arrow_schema.field_with_name("name").is_ok());
        assert!(arrow_schema.field_with_name("value").is_ok());
    }

    #[tokio::test]
    async fn test_end_to_end_csv_ingest_to_delta() {
        let dir = tempfile::tempdir().unwrap();

        let source_path = dir.path().join("data.csv");
        std::fs::write(&source_path, "name,value\nglucose,95.0\na1c,6.1\n").unwrap();

        let delta_path = dir.path().join("delta_out");
        let delta_str = delta_path.to_str().unwrap();
        let batches = read_file(source_path.to_str().unwrap(), InputFormat::Csv, 1024).unwrap();
        let version = write_delta(delta_str, batches, WriteMode::Overwrite)
            .await
            .unwrap();
        assert!(version >= 0);

        let sync = crate::delta::DeltaSync::new(delta_str);
        let rows = sync.full_load(None).await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn test_end_to_end_append_mode() {
        let dir = tempfile::tempdir().unwrap();

        let delta_path = dir.path().join("delta_out");
        let delta_str = delta_path.to_str().unwrap();

        // First batch
        let source1 = dir.path().join("batch1.ndjson");
        std::fs::write(&source1, r#"{"name":"glucose","value":95.0}"#).unwrap();
        let batches1 = read_file(source1.to_str().unwrap(), InputFormat::Ndjson, 1024).unwrap();
        write_delta(delta_str, batches1, WriteMode::Overwrite)
            .await
            .unwrap();

        // Append second batch
        let source2 = dir.path().join("batch2.ndjson");
        std::fs::write(&source2, r#"{"name":"a1c","value":6.1}"#).unwrap();
        let batches2 = read_file(source2.to_str().unwrap(), InputFormat::Ndjson, 1024).unwrap();
        write_delta(delta_str, batches2, WriteMode::Append)
            .await
            .unwrap();

        let sync = crate::delta::DeltaSync::new(delta_str);
        let rows = sync.full_load(None).await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn test_multi_file_glob_ingest() {
        let dir = tempfile::tempdir().unwrap();

        // Create multiple source files
        std::fs::write(
            dir.path().join("part1.ndjson"),
            r#"{"name":"glucose","value":95.0}"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("part2.ndjson"),
            r#"{"name":"a1c","value":6.1}"#,
        )
        .unwrap();

        // Expand glob
        let pattern = format!("{}/*.ndjson", dir.path().to_str().unwrap());
        let files = expand_source(&pattern).unwrap();
        assert_eq!(files.len(), 2);

        // Read all files
        let mut all_batches = Vec::new();
        for f in &files {
            let batches = read_file(f, InputFormat::Ndjson, 1024).unwrap();
            all_batches.extend(batches);
        }

        // Write to Delta
        let delta_path = dir.path().join("delta_out");
        let delta_str = delta_path.to_str().unwrap();
        write_delta(delta_str, all_batches, WriteMode::Overwrite)
            .await
            .unwrap();

        let sync = crate::delta::DeltaSync::new(delta_str);
        let rows = sync.full_load(None).await.unwrap();
        assert_eq!(rows.len(), 2);
    }
}
