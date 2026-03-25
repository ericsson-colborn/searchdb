use arrow::record_batch::RecordBatch;
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

fn read_parquet_file(_path: &str) -> Result<Vec<RecordBatch>> {
    todo!("Parquet reader — Task 6")
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
}
