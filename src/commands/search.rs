use std::io::Read;

use tantivy::Index;

use crate::error::{Result, SearchDbError};
use crate::searcher;
use crate::storage::Storage;
use crate::OutputFormat;

/// Execute a search query against an index and print results.
/// Supports both query string (-q) and ES DSL (--dsl) modes.
#[allow(clippy::too_many_arguments)]
pub fn run(
    storage: &Storage,
    name: &str,
    query: Option<&str>,
    dsl: Option<&str>,
    limit: usize,
    offset: usize,
    fields: Option<Vec<String>>,
    include_score: bool,
    fmt: OutputFormat,
    gap_rows: &[serde_json::Value],
) -> Result<()> {
    if query.is_none() && dsl.is_none() {
        return Err(SearchDbError::Schema(
            "either --query (-q) or --dsl is required".into(),
        ));
    }

    if !storage.exists(name) {
        return Err(SearchDbError::IndexNotFound(name.to_string()));
    }

    let config = storage.load_config(name)?;
    let index = Index::open_in_dir(storage.tantivy_dir(name))?;

    let results = if let Some(dsl_input) = dsl {
        let dsl_json = resolve_dsl_input(dsl_input)?;
        searcher::search_dsl_with_gap(
            &index,
            &config.schema,
            &dsl_json,
            limit,
            offset,
            fields.as_deref(),
            include_score,
            gap_rows,
        )?
    } else {
        let query_str = query.expect("query or dsl must be provided");
        searcher::search_with_gap(
            &index,
            &config.schema,
            query_str,
            limit,
            offset,
            fields.as_deref(),
            include_score,
            gap_rows,
        )?
    };

    print_results(&results, fmt)?;

    eprintln!("[dsrch] {} result(s)", results.len());
    Ok(())
}

/// Resolve DSL input: literal JSON string, @file path, or - for stdin.
fn resolve_dsl_input(input: &str) -> Result<String> {
    if input == "-" {
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf)?;
        Ok(buf)
    } else if let Some(path) = input.strip_prefix('@') {
        Ok(std::fs::read_to_string(path)?)
    } else {
        Ok(input.to_string())
    }
}

/// Print search results in the requested format.
fn print_results(results: &[searcher::SearchHit], fmt: OutputFormat) -> Result<()> {
    match fmt {
        OutputFormat::Json => {
            for hit in results {
                println!("{}", serde_json::to_string(&hit.doc)?);
            }
        }
        OutputFormat::Text => {
            if results.is_empty() {
                eprintln!("[dsrch] No results");
                return Ok(());
            }
            let first = results[0].doc.as_object().unwrap();
            let cols: Vec<&String> = first.keys().collect();

            let header: Vec<String> = cols.iter().map(|c| c.to_string()).collect();
            println!("{}", header.join("\t"));
            println!(
                "{}",
                header
                    .iter()
                    .map(|h| "-".repeat(h.len().max(8)))
                    .collect::<Vec<_>>()
                    .join("\t")
            );

            for hit in results {
                let obj = hit.doc.as_object().unwrap();
                let row: Vec<String> = cols
                    .iter()
                    .map(|c| match obj.get(*c) {
                        Some(serde_json::Value::String(s)) => s.clone(),
                        Some(v) => v.to_string(),
                        None => String::new(),
                    })
                    .collect();
                println!("{}", row.join("\t"));
            }
        }
    }
    Ok(())
}
