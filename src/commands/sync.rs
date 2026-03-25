use crate::compact::{CompactOptions, CompactWorker};
use crate::error::{Result, SearchDbError};
use crate::storage::Storage;

/// Manual incremental sync from a Delta Lake source.
///
/// DEPRECATED: This command is now an alias for `compact --once`.
/// Use `dsrch compact <name> --once` instead.
pub async fn run(storage: &Storage, name: &str) -> Result<()> {
    eprintln!("[dsrch] WARNING: 'sync' is deprecated. Use 'compact --once' instead.");

    if !storage.exists(name) {
        return Err(SearchDbError::IndexNotFound(name.to_string()));
    }

    let opts = CompactOptions {
        once: true,
        ..CompactOptions::default()
    };

    let worker = CompactWorker::new(storage, name, opts);
    let (_tx, rx) = tokio::sync::watch::channel(false);
    worker.run(rx).await
}
