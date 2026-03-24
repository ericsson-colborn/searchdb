use thiserror::Error;

#[derive(Error, Debug)]
pub enum SearchDbError {
    #[error("Index '{0}' not found")]
    IndexNotFound(String),

    #[error("Index '{0}' already exists. Use --overwrite to replace it.")]
    IndexExists(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Tantivy error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, SearchDbError>;
