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
}
