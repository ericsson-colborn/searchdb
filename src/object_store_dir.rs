use std::io::{self, BufWriter, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use object_store::ObjectStore;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    AntiCallToken, Directory, FileHandle, OwnedBytes, TerminatingWrite, WatchCallback, WatchHandle,
    WritePtr,
};
use tantivy::HasLen;

/// A tantivy Directory implementation backed by object storage with local caching.
///
/// Segment files are immutable once committed, so they are cached forever on
/// local disk. Only `meta.json` (which changes on every commit) is re-fetched
/// from the remote store on each read.
#[derive(Clone, Debug)]
pub struct ObjectStoreDirectory {
    /// The remote object store (S3, GCS, Azure, local, etc.)
    store: Arc<dyn ObjectStore>,
    /// Prefix/path within the object store (e.g., "indexes/myindex")
    prefix: object_store::path::Path,
    /// Local cache directory for downloaded segment files
    cache_dir: PathBuf,
    /// Tokio runtime handle for sync-async bridging
    rt: tokio::runtime::Handle,
}

impl ObjectStoreDirectory {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: object_store::path::Path,
        cache_dir: PathBuf,
        rt: tokio::runtime::Handle,
    ) -> io::Result<Self> {
        std::fs::create_dir_all(&cache_dir)?;
        Ok(Self {
            store,
            prefix,
            cache_dir,
            rt,
        })
    }

    /// Full object store path for a tantivy file.
    fn object_path(&self, path: &Path) -> object_store::path::Path {
        let file_name = path.to_string_lossy();
        self.prefix.child(file_name.as_ref())
    }

    /// Local cache path for a file.
    fn cache_path(&self, path: &Path) -> PathBuf {
        self.cache_dir.join(path)
    }

    /// Is this file a mutable metadata file (never permanently cached)?
    fn is_meta_file(path: &Path) -> bool {
        let name = path.to_string_lossy();
        name == "meta.json" || name.ends_with(".lock")
    }

    /// Synchronously fetch all bytes from the object store.
    fn blocking_get(&self, obj_path: &object_store::path::Path) -> io::Result<Vec<u8>> {
        tokio::task::block_in_place(|| {
            self.rt.block_on(async {
                let result = self.store.get(obj_path).await.map_err(io::Error::from)?;
                let bytes = result.bytes().await.map_err(io::Error::from)?;
                Ok(bytes.to_vec())
            })
        })
    }

    /// Synchronously put bytes to the object store.
    fn blocking_put(&self, obj_path: &object_store::path::Path, data: Vec<u8>) -> io::Result<()> {
        tokio::task::block_in_place(|| {
            self.rt.block_on(async {
                self.store
                    .put(obj_path, data.into())
                    .await
                    .map_err(io::Error::from)?;
                Ok(())
            })
        })
    }

    /// Download a file from object store to local cache (if not cached).
    ///
    /// Meta files are always re-fetched. Segment files are cached forever.
    fn ensure_cached(&self, path: &Path) -> io::Result<PathBuf> {
        let cached = self.cache_path(path);

        // Meta files are never permanently cached (they change).
        if Self::is_meta_file(path) {
            let obj_path = self.object_path(path);
            let data = self.blocking_get(&obj_path)?;
            if let Some(parent) = cached.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&cached, &data)?;
            return Ok(cached);
        }

        // Segment files: cache forever once present.
        if cached.exists() {
            return Ok(cached);
        }

        let obj_path = self.object_path(path);
        let data = self.blocking_get(&obj_path)?;
        if let Some(parent) = cached.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&cached, &data)?;
        Ok(cached)
    }
}

// ---------------------------------------------------------------------------
// In-memory FileHandle
// ---------------------------------------------------------------------------

/// Serves reads from a `Vec<u8>` held in memory.
struct InMemoryFileHandle {
    data: Vec<u8>,
}

impl std::fmt::Debug for InMemoryFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InMemoryFileHandle({} bytes)", self.data.len())
    }
}

impl HasLen for InMemoryFileHandle {
    fn len(&self) -> usize {
        self.data.len()
    }
}

#[async_trait]
impl FileHandle for InMemoryFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if range.end > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "read range {:?} exceeds file size {}",
                    range,
                    self.data.len()
                ),
            ));
        }
        Ok(OwnedBytes::new(self.data[range].to_vec()))
    }
}

// ---------------------------------------------------------------------------
// ObjectStoreWriter
// ---------------------------------------------------------------------------

/// Buffers writes in memory, then uploads to object store on `terminate_ref`.
struct ObjectStoreWriter {
    store: Arc<dyn ObjectStore>,
    obj_path: object_store::path::Path,
    cache_path: PathBuf,
    buffer: Vec<u8>,
    rt: tokio::runtime::Handle,
}

impl ObjectStoreWriter {
    fn new(
        store: Arc<dyn ObjectStore>,
        obj_path: object_store::path::Path,
        cache_path: PathBuf,
        rt: tokio::runtime::Handle,
    ) -> Self {
        Self {
            store,
            obj_path,
            cache_path,
            buffer: Vec::new(),
            rt,
        }
    }
}

impl Write for ObjectStoreWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl TerminatingWrite for ObjectStoreWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        let data = std::mem::take(&mut self.buffer);

        // Cache locally.
        if let Some(parent) = self.cache_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&self.cache_path, &data)?;

        // Upload to object store.
        tokio::task::block_in_place(|| {
            self.rt.block_on(async {
                self.store
                    .put(&self.obj_path, data.into())
                    .await
                    .map_err(|e| io::Error::other(e.to_string()))
            })
        })?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Directory trait implementation
// ---------------------------------------------------------------------------

impl Directory for ObjectStoreDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let cached = self
            .ensure_cached(path)
            .map_err(|e| OpenReadError::wrap_io_error(e, path.to_path_buf()))?;

        let data = std::fs::read(&cached)
            .map_err(|e| OpenReadError::wrap_io_error(e, path.to_path_buf()))?;

        Ok(Arc::new(InMemoryFileHandle { data }))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        let obj_path = self.object_path(path);
        tokio::task::block_in_place(|| {
            self.rt.block_on(async {
                // Best-effort delete on object store.
                self.store.delete(&obj_path).await.ok();
            })
        });
        // Also remove from local cache.
        let cached = self.cache_path(path);
        std::fs::remove_file(cached).ok();
        Ok(())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        // Check cache first (for non-meta files).
        if self.cache_path(path).exists() && !Self::is_meta_file(path) {
            return Ok(true);
        }
        let obj_path = self.object_path(path);
        let exists = tokio::task::block_in_place(|| {
            self.rt
                .block_on(async { self.store.head(&obj_path).await.is_ok() })
        });
        Ok(exists)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let obj_path = self.object_path(path);
        let cache_path = self.cache_path(path);
        if let Some(parent) = cache_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| OpenWriteError::wrap_io_error(e, path.to_path_buf()))?;
        }

        let writer =
            ObjectStoreWriter::new(self.store.clone(), obj_path, cache_path, self.rt.clone());
        Ok(BufWriter::new(Box::new(writer)))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let obj_path = self.object_path(path);
        self.blocking_get(&obj_path).map_err(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                OpenReadError::FileDoesNotExist(path.to_path_buf())
            } else {
                OpenReadError::wrap_io_error(e, path.to_path_buf())
            }
        })
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        let obj_path = self.object_path(path);
        self.blocking_put(&obj_path, data.to_vec())?;

        // Update local cache so subsequent reads hit cache.
        let cached = self.cache_path(path);
        if let Some(parent) = cached.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(cached, data)?;
        Ok(())
    }

    fn sync_directory(&self) -> io::Result<()> {
        // Object store writes are durable on completion.
        Ok(())
    }

    fn watch(&self, _watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        // No filesystem watch on object storage. Return a no-op handle.
        // For remote storage the reader should be explicitly reloaded.
        Ok(WatchHandle::empty())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_atomic_read_write() {
        let store = Arc::new(InMemory::new());
        let prefix = object_store::path::Path::from("test-index");
        let cache_dir = tempfile::tempdir().unwrap();
        let rt = tokio::runtime::Handle::current();

        let dir =
            ObjectStoreDirectory::new(store, prefix, cache_dir.path().to_path_buf(), rt).unwrap();

        dir.atomic_write(Path::new("meta.json"), b"test data")
            .unwrap();

        let data = dir.atomic_read(Path::new("meta.json")).unwrap();
        assert_eq!(data, b"test data");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_exists() {
        let store = Arc::new(InMemory::new());
        let prefix = object_store::path::Path::from("test-index");
        let cache_dir = tempfile::tempdir().unwrap();
        let rt = tokio::runtime::Handle::current();

        let dir =
            ObjectStoreDirectory::new(store, prefix, cache_dir.path().to_path_buf(), rt).unwrap();

        assert!(!dir.exists(Path::new("nonexistent")).unwrap());

        dir.atomic_write(Path::new("meta.json"), b"hello").unwrap();
        assert!(dir.exists(Path::new("meta.json")).unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_open_write_and_read_back() {
        let store = Arc::new(InMemory::new());
        let prefix = object_store::path::Path::from("test-index");
        let cache_dir = tempfile::tempdir().unwrap();
        let rt = tokio::runtime::Handle::current();

        let dir =
            ObjectStoreDirectory::new(store, prefix, cache_dir.path().to_path_buf(), rt).unwrap();

        // Write via open_write (the segment-file path).
        let mut writer = dir.open_write(Path::new("segment.bin")).unwrap();
        writer.write_all(b"segment data").unwrap();
        writer.terminate().unwrap();

        // Read it back.
        let handle = dir.get_file_handle(Path::new("segment.bin")).unwrap();
        let bytes = handle.read_bytes(0..12).unwrap();
        assert_eq!(bytes.as_slice(), b"segment data");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_delete() {
        let store = Arc::new(InMemory::new());
        let prefix = object_store::path::Path::from("test-index");
        let cache_dir = tempfile::tempdir().unwrap();
        let rt = tokio::runtime::Handle::current();

        let dir =
            ObjectStoreDirectory::new(store, prefix, cache_dir.path().to_path_buf(), rt).unwrap();

        dir.atomic_write(Path::new("to_delete"), b"bye").unwrap();
        assert!(dir.exists(Path::new("to_delete")).unwrap());

        dir.delete(Path::new("to_delete")).unwrap();
        assert!(!dir.exists(Path::new("to_delete")).unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tantivy_index_round_trip() {
        let store = Arc::new(InMemory::new());
        let prefix = object_store::path::Path::from("test-index");
        let cache_dir = tempfile::tempdir().unwrap();
        let rt = tokio::runtime::Handle::current();

        let dir =
            ObjectStoreDirectory::new(store, prefix, cache_dir.path().to_path_buf(), rt).unwrap();

        // Build a tantivy schema and create an index on object storage.
        let mut schema_builder = tantivy::schema::SchemaBuilder::new();
        schema_builder.add_text_field("title", tantivy::schema::TEXT | tantivy::schema::STORED);
        let schema = schema_builder.build();

        let index =
            tantivy::Index::create(dir, schema.clone(), tantivy::IndexSettings::default()).unwrap();

        let mut writer = index.writer(15_000_000).unwrap();
        let title = schema.get_field("title").unwrap();
        writer
            .add_document(tantivy::doc!(title => "hello world"))
            .unwrap();
        writer.commit().unwrap();

        // Search.
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let query_parser = tantivy::query::QueryParser::for_index(&index, vec![title]);
        let query = query_parser.parse_query("hello").unwrap();
        let top_docs = searcher
            .search(&query, &tantivy::collector::TopDocs::with_limit(10))
            .unwrap();
        assert_eq!(top_docs.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_meta_file_always_refetched() {
        let store = Arc::new(InMemory::new());
        let prefix = object_store::path::Path::from("idx");
        let cache_dir = tempfile::tempdir().unwrap();
        let rt = tokio::runtime::Handle::current();

        let dir = ObjectStoreDirectory::new(
            store.clone(),
            prefix.clone(),
            cache_dir.path().to_path_buf(),
            rt.clone(),
        )
        .unwrap();

        // Write meta.json with initial content.
        dir.atomic_write(Path::new("meta.json"), b"v1").unwrap();
        let data = dir.atomic_read(Path::new("meta.json")).unwrap();
        assert_eq!(data, b"v1");

        // Overwrite meta.json.
        dir.atomic_write(Path::new("meta.json"), b"v2").unwrap();

        // Even via get_file_handle (which goes through cache), should see v2
        // because meta.json is always re-fetched.
        let handle = dir.get_file_handle(Path::new("meta.json")).unwrap();
        let bytes = handle.read_bytes(0..2).unwrap();
        assert_eq!(bytes.as_slice(), b"v2");
    }
}
