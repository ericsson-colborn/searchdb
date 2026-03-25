use crate::error::{Result, SearchDbError};
use crate::schema::Schema;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

/// Compaction worker metadata — persisted to searchdb.json.
/// Written by the compact worker; ignored by search/get clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactMeta {
    pub segment_size: usize,
    pub merge_interval_secs: u64,
    pub max_segments: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_segment_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_merge_at: Option<String>,
}

/// Persisted index configuration — schema + optional Delta metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    pub schema: Schema,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_source: Option<String>,
    #[serde(
        alias = "last_indexed_version",
        skip_serializing_if = "Option::is_none"
    )]
    pub index_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compact: Option<CompactMeta>,
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

    #[allow(dead_code)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldType, Schema};
    use std::collections::BTreeMap;

    #[test]
    fn test_compact_meta_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());
        storage.create_dirs("test").unwrap();

        // Create a minimal index so save_config works
        let schema = Schema {
            fields: BTreeMap::from([("name".into(), FieldType::Keyword)]),
        };
        let tantivy_schema = schema.build_tantivy_schema();
        tantivy::Index::create_in_dir(storage.tantivy_dir("test"), tantivy_schema).unwrap();

        let config = IndexConfig {
            schema,
            delta_source: Some("s3://bucket/labs".into()),
            index_version: Some(42),
            compact: Some(CompactMeta {
                segment_size: 50_000,
                merge_interval_secs: 600,
                max_segments: 20,
                last_segment_at: Some("2026-03-24T14:30:00Z".into()),
                last_merge_at: Some("2026-03-24T14:25:00Z".into()),
            }),
        };
        storage.save_config("test", &config).unwrap();

        let loaded = storage.load_config("test").unwrap();
        let compact = loaded.compact.unwrap();
        assert_eq!(compact.segment_size, 50_000);
        assert_eq!(compact.merge_interval_secs, 600);
        assert_eq!(compact.max_segments, 20);
        assert_eq!(
            compact.last_segment_at.as_deref(),
            Some("2026-03-24T14:30:00Z")
        );
        assert_eq!(
            compact.last_merge_at.as_deref(),
            Some("2026-03-24T14:25:00Z")
        );
    }

    #[test]
    fn test_config_without_compact_meta_loads() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());
        storage.create_dirs("test").unwrap();

        let schema = Schema {
            fields: BTreeMap::from([("name".into(), FieldType::Keyword)]),
        };
        let tantivy_schema = schema.build_tantivy_schema();
        tantivy::Index::create_in_dir(storage.tantivy_dir("test"), tantivy_schema).unwrap();

        // Save config WITHOUT compact field (simulates pre-compact indexes)
        let config = IndexConfig {
            schema,
            delta_source: None,
            index_version: None,
            compact: None,
        };
        storage.save_config("test", &config).unwrap();

        let loaded = storage.load_config("test").unwrap();
        assert!(loaded.compact.is_none());
    }
}
