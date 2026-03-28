use crate::error::{Result, SearchDbError};
use crate::schema::Schema;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

/// Compaction worker metadata — persisted to searchdb.json (legacy filename, kept for compatibility).
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
    /// Whether the schema was inferred (evolution enabled) or explicit (evolution disabled).
    #[serde(default)]
    pub inferred: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_source: Option<String>,
    /// Remote storage URI for the tantivy index (e.g., "az://container/path").
    /// When set, the index lives on object storage with local disk cache.
    /// When absent, the index lives in the local data_dir.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_store: Option<String>,
    #[serde(
        alias = "last_indexed_version",
        skip_serializing_if = "Option::is_none"
    )]
    pub index_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compact: Option<CompactMeta>,
}

/// Manages the on-disk layout for deltasearch indexes.
///
/// Layout:
/// ```text
/// {data_dir}/
/// ├── {index_name}/
/// │   ├── searchdb.json   ← schema + delta metadata (legacy filename, kept for compatibility)
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

    /// Open (or create) a tantivy Index using the appropriate directory.
    ///
    /// If the index config has `index_store` set, uses ObjectStoreDirectory
    /// (remote storage with local cache). Otherwise uses the local MmapDirectory.
    #[cfg(feature = "delta")]
    #[allow(dead_code)]
    pub fn open_index(&self, name: &str) -> Result<tantivy::Index> {
        let config = self.load_config(name)?;
        if let Some(ref store_uri) = config.index_store {
            let (store, prefix) = crate::object_store_dir::parse_object_store_uri(store_uri)
                .map_err(|e| SearchDbError::Delta(format!("invalid index_store URI: {e}")))?;
            let cache_dir = self.tantivy_dir(name); // local cache
            fs::create_dir_all(&cache_dir)?;
            let rt = tokio::runtime::Handle::current();
            let dir =
                crate::object_store_dir::ObjectStoreDirectory::new(store, prefix, cache_dir, rt)
                    .map_err(SearchDbError::Io)?;
            let index = tantivy::Index::open(dir)?;
            Ok(index)
        } else {
            let index = tantivy::Index::open_in_dir(self.tantivy_dir(name))?;
            Ok(index)
        }
    }

    /// Create a new tantivy Index using the appropriate directory.
    #[cfg(feature = "delta")]
    #[allow(dead_code)]
    pub fn create_index(
        &self,
        name: &str,
        schema: tantivy::schema::Schema,
        index_store: Option<&str>,
    ) -> Result<tantivy::Index> {
        if let Some(store_uri) = index_store {
            let (store, prefix) = crate::object_store_dir::parse_object_store_uri(store_uri)
                .map_err(|e| SearchDbError::Delta(format!("invalid index_store URI: {e}")))?;
            let cache_dir = self.tantivy_dir(name);
            fs::create_dir_all(&cache_dir)?;
            let rt = tokio::runtime::Handle::current();
            let dir =
                crate::object_store_dir::ObjectStoreDirectory::new(store, prefix, cache_dir, rt)
                    .map_err(SearchDbError::Io)?;
            let index = tantivy::Index::create(dir, schema, tantivy::IndexSettings::default())?;
            Ok(index)
        } else {
            self.create_dirs(name)?;
            let index = tantivy::Index::create_in_dir(self.tantivy_dir(name), schema)?;
            Ok(index)
        }
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
            inferred: false,
            delta_source: Some("s3://bucket/labs".into()),
            index_store: None,
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
            inferred: false,
            delta_source: None,
            index_store: None,
            index_version: None,
            compact: None,
        };
        storage.save_config("test", &config).unwrap();

        let loaded = storage.load_config("test").unwrap();
        assert!(loaded.compact.is_none());
    }

    #[test]
    fn test_inferred_flag_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());
        storage.create_dirs("test").unwrap();

        let schema = Schema {
            fields: BTreeMap::from([("name".into(), FieldType::Keyword)]),
        };
        let tantivy_schema = schema.build_tantivy_schema();
        tantivy::Index::create_in_dir(storage.tantivy_dir("test"), tantivy_schema).unwrap();

        let config = IndexConfig {
            schema,
            inferred: true,
            delta_source: None,
            index_store: None,
            index_version: None,
            compact: None,
        };
        storage.save_config("test", &config).unwrap();

        let loaded = storage.load_config("test").unwrap();
        assert!(loaded.inferred);
    }

    #[test]
    fn test_inferred_flag_defaults_false_when_absent() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());
        storage.create_dirs("test").unwrap();

        let schema = Schema {
            fields: BTreeMap::from([("name".into(), FieldType::Keyword)]),
        };
        let tantivy_schema = schema.build_tantivy_schema();
        tantivy::Index::create_in_dir(storage.tantivy_dir("test"), tantivy_schema).unwrap();

        // Write JSON manually WITHOUT "inferred" field (simulates old config)
        let json = r#"{"schema":{"fields":{"name":"keyword"}}}"#;
        std::fs::write(storage.config_path("test"), json).unwrap();

        let loaded = storage.load_config("test").unwrap();
        assert!(!loaded.inferred); // defaults to false
    }
}
