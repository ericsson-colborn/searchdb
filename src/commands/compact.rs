use crate::compact::{CompactOptions, CompactWorker};
use crate::error::{Result, SearchDbError};
use crate::storage::Storage;

/// Run the compact worker with the given CLI options.
pub async fn run(storage: &Storage, name: &str, opts: CompactOptions) -> Result<()> {
    if !storage.exists(name) {
        return Err(SearchDbError::IndexNotFound(name.to_string()));
    }

    let worker = CompactWorker::new(storage, name, opts);

    // Set up shutdown signal handler
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Spawn signal handler
    let signal_handle = tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to install SIGTERM handler");
            tokio::select! {
                _ = ctrl_c => {
                    eprintln!("\n[searchdb] compact: received SIGINT, shutting down gracefully...");
                }
                _ = sigterm.recv() => {
                    eprintln!("[searchdb] compact: received SIGTERM, shutting down gracefully...");
                }
            }
        }
        #[cfg(not(unix))]
        {
            ctrl_c.await.ok();
            eprintln!("\n[searchdb] compact: received Ctrl+C, shutting down gracefully...");
        }
        let _ = shutdown_tx.send(true);
    });

    let result = worker.run(shutdown_rx).await;

    // Cancel signal handler if worker finished on its own (--once / --force-merge)
    signal_handle.abort();

    result
}
