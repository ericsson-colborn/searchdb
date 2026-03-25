use tantivy::Index;

use crate::error::{Result, SearchDbError};
use crate::searcher;
use crate::storage::Storage;
use crate::OutputFormat;

/// Retrieve a single document by _id with optional field projection.
/// Checks gap rows first (newer data), falls back to persistent index.
pub fn run(
    storage: &Storage,
    name: &str,
    doc_id: &str,
    fields: Option<Vec<String>>,
    fmt: OutputFormat,
    gap_rows: &[serde_json::Value],
) -> Result<()> {
    if !storage.exists(name) {
        return Err(SearchDbError::IndexNotFound(name.to_string()));
    }

    let index = Index::open_in_dir(storage.tantivy_dir(name))?;

    match searcher::get_with_gap(&index, doc_id, gap_rows)? {
        Some(mut doc) => {
            // Apply field projection
            if let Some(ref field_list) = fields {
                if let Some(obj) = doc.as_object() {
                    let projected: serde_json::Map<String, serde_json::Value> = obj
                        .iter()
                        .filter(|(k, _)| *k == "_id" || field_list.iter().any(|f| f == *k))
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    doc = serde_json::Value::Object(projected);
                }
            }

            match fmt {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string(&doc)?);
                }
                OutputFormat::Text => {
                    if let Some(obj) = doc.as_object() {
                        for (k, v) in obj {
                            let val = match v {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            };
                            println!("{k}: {val}");
                        }
                    }
                }
            }
            Ok(())
        }
        None => {
            match fmt {
                OutputFormat::Json => {
                    eprintln!(
                        "{}",
                        serde_json::json!({"error": "not_found", "index": name, "doc_id": doc_id})
                    );
                }
                OutputFormat::Text => {
                    eprintln!("[dsrch] Document '{doc_id}' not found in '{name}'");
                }
            }
            std::process::exit(3);
        }
    }
}
