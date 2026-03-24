use crate::error::{Result, SearchDbError};
use crate::schema::Schema;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

/// Persisted index configuration — schema + optional Delta metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    pub schema: Schema,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_indexed_version: Option<i64>,
}

/// Manages the on-disk layout for SearchDB indexes.
///
/// Layout:
/// ```text
/// {data_dir}/
/// ├── {index_name}/
/// │   ├── searchdb.json   ← schema + delta metadata
/// │   └── index/          ← tantivy segment files
/// ```
pub struct Storage {
    data_dir: PathBuf,
}

impl Storage {
    pub fn new(data_dir: &str) -> Self {
        Self {
            data_dir: PathBuf::from(data_dir),
        }
    }

    pub fn index_dir(&self, name: &str) -> PathBuf {
        self.data_dir.join(name)
    }

    pub fn tantivy_dir(&self, name: &str) -> PathBuf {
        self.data_dir.join(name).join("index")
    }

    pub fn config_path(&self, name: &str) -> PathBuf {
        self.data_dir.join(name).join("searchdb.json")
    }

    pub fn exists(&self, name: &str) -> bool {
        self.config_path(name).exists()
    }

    pub fn create_dirs(&self, name: &str) -> Result<()> {
        fs::create_dir_all(self.tantivy_dir(name))?;
        Ok(())
    }

    pub fn save_config(&self, name: &str, config: &IndexConfig) -> Result<()> {
        let json = serde_json::to_string_pretty(config)?;
        fs::write(self.config_path(name), json)?;
        Ok(())
    }

    pub fn load_config(&self, name: &str) -> Result<IndexConfig> {
        if !self.exists(name) {
            return Err(SearchDbError::IndexNotFound(name.to_string()));
        }
        let json = fs::read_to_string(self.config_path(name))?;
        let config: IndexConfig = serde_json::from_str(&json)?;
        Ok(config)
    }

    pub fn drop(&self, name: &str) -> Result<()> {
        let dir = self.index_dir(name);
        if dir.exists() {
            fs::remove_dir_all(dir)?;
        }
        Ok(())
    }

    pub fn list_indexes(&self) -> Result<Vec<String>> {
        if !self.data_dir.exists() {
            return Ok(vec![]);
        }
        let mut names = vec![];
        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let name = entry.file_name().to_string_lossy().to_string();
                if self.exists(&name) {
                    names.push(name);
                }
            }
        }
        names.sort();
        Ok(names)
    }
}
