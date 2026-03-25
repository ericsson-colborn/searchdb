# Tiered Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `dsrch compact` command that implements a long-lived tiered compaction worker: Level 1 polls Delta Lake and commits small segments with NoMergePolicy; Level 2 merges segments on a timer when segment count exceeds a threshold. This replaces the all-or-nothing `sync` command for production workloads.

**Architecture:** Two-tier compaction loop. Level 1 (segmentation) polls Delta for new rows, accumulates up to `segment_size` rows, and commits each batch as a separate tantivy segment using `NoMergePolicy`. Level 2 (merge) runs on a timer and merges segments when the count exceeds `max_segments`. The worker is the single writer; search/get clients are read-only. Crash safety is guaranteed by committing tantivy before advancing the `index_version` watermark in `searchdb.json`.

**Tech Stack:** Rust, tantivy 0.25 (`NoMergePolicy`, `IndexWriter::merge`, `IndexWriter::wait_merging_threads`), deltalake crate (async), tokio (signals, timers, `Instant`), clap 4

**Spec:** `docs/superpowers/specs/2026-03-24-tiered-compaction-design.md`

---

## File Structure

```
src/
├── main.rs              # MODIFY — add Compact command variant, dispatch to commands::compact::run
├── compact.rs           # NEW — core compaction logic (CompactWorker struct, segment loop, merge logic)
├── storage.rs           # MODIFY — add CompactMeta to IndexConfig
├── error.rs             # MODIFY — add WriterLocked error variant
├── writer.rs            # unchanged
├── schema.rs            # unchanged
├── delta.rs             # unchanged
├── searcher.rs          # unchanged
├── commands/
│   ├── compact.rs       # NEW — CLI entry point, parses options, delegates to compact::CompactWorker
│   ├── sync.rs          # MODIFY — deprecation warning, delegate to compact --once
│   ├── stats.rs         # MODIFY — add segment count + compaction metadata to output
│   ├── mod.rs           # MODIFY — add pub mod compact
│   ├── connect_delta.rs # unchanged
│   ├── reindex.rs       # unchanged
│   ├── new_index.rs     # unchanged
│   ├── index.rs         # unchanged
│   ├── search.rs        # unchanged
│   ├── get.rs           # unchanged
│   └── drop.rs          # unchanged
```

---

## Task 1: Add `CompactMeta` to `IndexConfig` in `storage.rs`

**Files:**
- Modify: `/Users/ericssoncolborn/Documents/searchdb-rs/src/storage.rs`

This task adds the optional `compact` section to `searchdb.json` so the compact worker can persist its configuration and timestamps.

- [ ] **Step 1: Write a failing test for CompactMeta serialization**

Add to the bottom of `/Users/ericssoncolborn/Documents/searchdb-rs/src/storage.rs`, inside a new `#[cfg(test)] mod tests { ... }` block (or append to an existing one if present — currently there is no test module in this file):

```rust
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
        assert_eq!(compact.last_segment_at.as_deref(), Some("2026-03-24T14:30:00Z"));
        assert_eq!(compact.last_merge_at.as_deref(), Some("2026-03-24T14:25:00Z"));
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
```

Run: `cargo test --lib storage::tests`
Expected: **Fails** because `CompactMeta` struct and `compact` field do not exist yet.

- [ ] **Step 2: Add `CompactMeta` struct and update `IndexConfig`**

In `/Users/ericssoncolborn/Documents/searchdb-rs/src/storage.rs`, add the `CompactMeta` struct **above** the `IndexConfig` struct definition, and add the `compact` field to `IndexConfig`:

```rust
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
```

Then update the `IndexConfig` struct to add the `compact` field at the end:

```rust
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
```

- [ ] **Step 3: Fix all existing `IndexConfig` construction sites to include `compact: None`**

Every place in the codebase that constructs an `IndexConfig` literal must now include the `compact` field. Search for `IndexConfig {` in these files and add `compact: None,` to each:

In `/Users/ericssoncolborn/Documents/searchdb-rs/src/commands/connect_delta.rs` (around line 47):
```rust
    let config = IndexConfig {
        schema,
        delta_source: Some(source.to_string()),
        index_version: Some(version),
        compact: None,
    };
```

In `/Users/ericssoncolborn/Documents/searchdb-rs/src/commands/new_index.rs`, find where `IndexConfig` is constructed and add `compact: None,`.

In `/Users/ericssoncolborn/Documents/searchdb-rs/src/commands/reindex.rs`, find where `IndexConfig` is constructed and add `compact: None,`.

To find all construction sites, run:
```bash
grep -rn "IndexConfig {" src/
```

- [ ] **Step 4: Run tests**

Run: `cargo test`
Expected: All tests pass, including the two new storage tests.

- [ ] **Step 5: Commit**

```bash
git add src/storage.rs src/commands/connect_delta.rs src/commands/new_index.rs src/commands/reindex.rs
git commit -m "feat(storage): add CompactMeta to IndexConfig for compaction worker metadata"
```

---

## Task 2: Add `WriterLocked` error variant

**Files:**
- Modify: `/Users/ericssoncolborn/Documents/searchdb-rs/src/error.rs`

The compact worker needs to detect when another writer holds the lock and report a clear error.

- [ ] **Step 1: Add the error variant**

In `/Users/ericssoncolborn/Documents/searchdb-rs/src/error.rs`, add a new variant to `SearchDbError` after the existing `Delta` variant:

```rust
    #[error("Another writer is already running for index '{0}'. Only one compact worker can run per index.")]
    WriterLocked(String),
```

Run: `cargo test`
Expected: All tests pass (no behavior change).

- [ ] **Step 2: Commit**

```bash
git add src/error.rs
git commit -m "feat(error): add WriterLocked error variant for compact worker lock detection"
```

---

## Task 3: Implement the core `CompactWorker` struct with Level 1 segmentation

**Files:**
- Create: `/Users/ericssoncolborn/Documents/searchdb-rs/src/compact.rs`
- Modify: `/Users/ericssoncolborn/Documents/searchdb-rs/src/main.rs` (add `mod compact;`)

This is the heart of the feature: a `CompactWorker` that polls Delta, accumulates rows, splits them into batches of `segment_size`, and commits each batch as a tantivy segment using `NoMergePolicy`.

- [ ] **Step 1: Register the module**

In `/Users/ericssoncolborn/Documents/searchdb-rs/src/main.rs`, add after the `mod writer;` line (around line 8):

```rust
#[cfg(feature = "delta")]
mod compact;
```

- [ ] **Step 2: Create `src/compact.rs` with the `CompactWorker` struct and segmentation logic**

Create the file `/Users/ericssoncolborn/Documents/searchdb-rs/src/compact.rs` with this content:

```rust
use std::time::Instant;

use tantivy::merge_policy::NoMergePolicy;
use tantivy::Index;

use crate::delta::DeltaSync;
use crate::error::{Result, SearchDbError};
use crate::storage::{CompactMeta, IndexConfig, Storage};
use crate::writer;

/// Configuration for the compact worker, populated from CLI flags.
#[derive(Debug, Clone)]
pub struct CompactOptions {
    pub segment_size: usize,
    pub merge_interval_secs: u64,
    pub max_segments: usize,
    pub poll_interval_secs: u64,
    pub max_segment_age_secs: u64,
    pub force_merge: bool,
    pub once: bool,
}

impl Default for CompactOptions {
    fn default() -> Self {
        Self {
            segment_size: 10_000,
            merge_interval_secs: 300,
            max_segments: 10,
            poll_interval_secs: 10,
            max_segment_age_secs: 60,
            force_merge: false,
            once: false,
        }
    }
}

/// The compact worker: polls Delta, creates segments (L1), merges them (L2).
pub struct CompactWorker<'a> {
    storage: &'a Storage,
    name: String,
    opts: CompactOptions,
}

impl<'a> CompactWorker<'a> {
    pub fn new(storage: &'a Storage, name: &str, opts: CompactOptions) -> Self {
        Self {
            storage,
            name: name.to_string(),
            opts,
        }
    }

    /// Run the compaction loop. Returns when:
    /// - `--once` mode: after one poll+segment+merge cycle
    /// - `--force-merge` mode: after merging all segments
    /// - Signal received (SIGINT/SIGTERM)
    pub async fn run(&self, shutdown: tokio::sync::watch::Receiver<bool>) -> Result<()> {
        let config = self.storage.load_config(&self.name)?;
        let source = config.delta_source.as_deref().ok_or_else(|| {
            SearchDbError::Delta(format!(
                "index '{}' has no Delta source — use connect-delta first",
                self.name
            ))
        })?;

        let tantivy_schema = config.schema.build_tantivy_schema();
        let index = Index::open_in_dir(self.storage.tantivy_dir(&self.name))?;
        let mut index_writer = match index.writer(50_000_000) {
            Ok(w) => w,
            Err(tantivy::TantivyError::LockFailure(_, _)) => {
                return Err(SearchDbError::WriterLocked(self.name.clone()));
            }
            Err(e) => return Err(SearchDbError::Tantivy(e)),
        };

        // Disable automatic merges — we control merges explicitly in Level 2
        index_writer.set_merge_policy(Box::new(NoMergePolicy));

        let delta = DeltaSync::new(source);
        let id_field = tantivy_schema
            .get_field("_id")
            .map_err(|_| SearchDbError::Schema("missing _id field".into()))?;

        // Handle --force-merge: merge all segments and exit
        if self.opts.force_merge {
            eprintln!("[dsrch] compact: force-merging all segments...");
            self.force_merge_all(&index, &mut index_writer).await?;
            return Ok(());
        }

        let mut last_merge_check = Instant::now();

        // Main loop
        loop {
            if *shutdown.borrow() {
                eprintln!("[dsrch] compact: shutdown signal received, finishing...");
                break;
            }

            // Level 1: Poll and segment
            let mut current_config = self.storage.load_config(&self.name)?;
            let index_version = current_config.index_version.unwrap_or(-1);

            let current_version = match delta.current_version().await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("[dsrch] compact: Delta poll error: {e}");
                    if self.opts.once {
                        return Err(e);
                    }
                    self.sleep_or_shutdown(&shutdown).await;
                    continue;
                }
            };

            if current_version > index_version {
                let gap = current_version - index_version;
                eprintln!(
                    "[dsrch] compact: polling Delta... HEAD={current_version}, \
                     index={index_version}, gap={gap} versions"
                );

                let rows = delta.rows_added_since(index_version).await?;

                if rows.is_empty() {
                    eprintln!("[dsrch] compact: no new rows to index");
                    current_config.index_version = Some(current_version);
                    self.save_config_with_compact(&current_config)?;
                } else {
                    eprintln!(
                        "[dsrch] compact: read {} rows from Delta v{}..v{}",
                        rows.len(),
                        index_version,
                        current_version
                    );

                    // Split into batches of segment_size
                    let batches: Vec<&[serde_json::Value]> =
                        rows.chunks(self.opts.segment_size).collect();
                    let num_batches = batches.len();

                    for (i, batch) in batches.into_iter().enumerate() {
                        for row in batch {
                            let doc_id = writer::make_doc_id(row);
                            let doc = writer::build_document(
                                &tantivy_schema,
                                &current_config.schema,
                                row,
                                &doc_id,
                            )?;
                            writer::upsert_document(&index_writer, id_field, doc, &doc_id);
                        }

                        index_writer.commit()?;

                        eprintln!(
                            "[dsrch] compact: committed segment {}/{} ({} docs)",
                            i + 1,
                            num_batches,
                            batch.len()
                        );
                    }

                    // Update watermark AFTER all segments committed (crash safety)
                    current_config.index_version = Some(current_version);
                    self.save_compact_meta(&mut current_config, true, false)?;
                    self.storage.save_config(&self.name, &current_config)?;

                    eprintln!(
                        "[dsrch] compact: now at Delta v{current_version}"
                    );
                }
            } else {
                eprintln!("[dsrch] compact: up to date at Delta v{current_version}");
            }

            // Level 2: Check for merge opportunity
            let merge_elapsed = last_merge_check.elapsed().as_secs();
            let should_merge = self.opts.once || merge_elapsed >= self.opts.merge_interval_secs;

            if should_merge {
                last_merge_check = Instant::now();
                self.maybe_merge(&index, &mut index_writer).await?;
            }

            // Exit if --once mode
            if self.opts.once {
                break;
            }

            // Sleep until next poll
            self.sleep_or_shutdown(&shutdown).await;
        }

        // Clean shutdown
        index_writer.wait_merging_threads()?;
        eprintln!("[dsrch] compact: shutdown complete");
        Ok(())
    }

    /// Level 2: Check segment count and merge if above threshold.
    async fn maybe_merge(
        &self,
        index: &Index,
        index_writer: &mut tantivy::IndexWriter,
    ) -> Result<()> {
        let reader = index
            .reader()
            .map_err(|e| SearchDbError::Schema(format!("failed to open reader: {e}")))?;
        let searcher = reader.searcher();
        let segment_count = searcher.segment_readers().len();

        eprintln!(
            "[dsrch] compact: merge check: {segment_count} segments, \
             threshold={}",
            self.opts.max_segments
        );

        if segment_count <= self.opts.max_segments {
            return Ok(());
        }

        // Collect segment IDs to merge
        let segment_ids: Vec<tantivy::SegmentId> = index
            .searchable_segment_ids()
            .map_err(|e| SearchDbError::Tantivy(e))?;

        if segment_ids.len() <= 1 {
            return Ok(());
        }

        eprintln!(
            "[dsrch] compact: merging {} segments...",
            segment_ids.len()
        );

        let merge_future = index_writer.merge(&segment_ids);
        match merge_future.await {
            Ok(meta) => {
                eprintln!(
                    "[dsrch] compact: merged {} segments into 1",
                    segment_ids.len()
                );
                let _ = meta;
            }
            Err(e) => {
                eprintln!("[dsrch] compact: merge failed: {e}");
            }
        }

        // Garbage collect old segment files
        let _ = index_writer.garbage_collect_files().await;

        // Update compaction metadata
        let mut config = self.storage.load_config(&self.name)?;
        self.save_compact_meta(&mut config, false, true)?;
        self.storage.save_config(&self.name, &config)?;

        Ok(())
    }

    /// Force-merge all segments into one, then exit.
    async fn force_merge_all(
        &self,
        index: &Index,
        index_writer: &mut tantivy::IndexWriter,
    ) -> Result<()> {
        let segment_ids: Vec<tantivy::SegmentId> = index
            .searchable_segment_ids()
            .map_err(|e| SearchDbError::Tantivy(e))?;

        if segment_ids.len() <= 1 {
            eprintln!(
                "[dsrch] compact: already {} segment(s), nothing to merge",
                segment_ids.len()
            );
            return Ok(());
        }

        eprintln!(
            "[dsrch] compact: force-merging {} segments into 1...",
            segment_ids.len()
        );

        let merge_future = index_writer.merge(&segment_ids);
        match merge_future.await {
            Ok(_) => {
                eprintln!("[dsrch] compact: force-merge complete");
            }
            Err(e) => {
                return Err(SearchDbError::Tantivy(e));
            }
        }

        let _ = index_writer.garbage_collect_files().await;

        // Update metadata
        let mut config = self.storage.load_config(&self.name)?;
        self.save_compact_meta(&mut config, false, true)?;
        self.storage.save_config(&self.name, &config)?;

        index_writer.wait_merging_threads()?;
        Ok(())
    }

    /// Update the CompactMeta in the config with current timestamps.
    fn save_compact_meta(
        &self,
        config: &mut IndexConfig,
        did_segment: bool,
        did_merge: bool,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let meta = config.compact.get_or_insert(CompactMeta {
            segment_size: self.opts.segment_size,
            merge_interval_secs: self.opts.merge_interval_secs,
            max_segments: self.opts.max_segments,
            last_segment_at: None,
            last_merge_at: None,
        });
        meta.segment_size = self.opts.segment_size;
        meta.merge_interval_secs = self.opts.merge_interval_secs;
        meta.max_segments = self.opts.max_segments;
        if did_segment {
            meta.last_segment_at = Some(now.clone());
        }
        if did_merge {
            meta.last_merge_at = Some(now);
        }
        Ok(())
    }

    /// Helper to save config with compact metadata without segment/merge timestamps.
    fn save_config_with_compact(&self, config: &IndexConfig) -> Result<()> {
        let mut config = config.clone();
        self.save_compact_meta(&mut config, false, false)?;
        self.storage.save_config(&self.name, &config)
    }

    /// Sleep for poll_interval, returning early if shutdown is signaled.
    async fn sleep_or_shutdown(&self, shutdown: &tokio::sync::watch::Receiver<bool>) {
        let poll_duration = tokio::time::Duration::from_secs(self.opts.poll_interval_secs);
        let mut shutdown = shutdown.clone();
        tokio::select! {
            _ = tokio::time::sleep(poll_duration) => {}
            _ = shutdown.changed() => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compact_options_defaults() {
        let opts = CompactOptions::default();
        assert_eq!(opts.segment_size, 10_000);
        assert_eq!(opts.merge_interval_secs, 300);
        assert_eq!(opts.max_segments, 10);
        assert_eq!(opts.poll_interval_secs, 10);
        assert_eq!(opts.max_segment_age_secs, 60);
        assert!(!opts.force_merge);
        assert!(!opts.once);
    }
}
```

- [ ] **Step 3: Verify compilation**

Run: `cargo build`
Expected: Compiles successfully.

Run: `cargo test compact::tests`
Expected: `test_compact_options_defaults` passes.

- [ ] **Step 4: Commit**

```bash
git add src/compact.rs src/main.rs
git commit -m "feat(compact): add CompactWorker with Level 1 segmentation and Level 2 merge logic"
```

---

## Task 4: Implement the `dsrch compact` CLI command

**Files:**
- Create: `/Users/ericssoncolborn/Documents/searchdb-rs/src/commands/compact.rs`
- Modify: `/Users/ericssoncolborn/Documents/searchdb-rs/src/commands/mod.rs`
- Modify: `/Users/ericssoncolborn/Documents/searchdb-rs/src/main.rs`

This task wires up the compact worker to the CLI with all the flags specified in the design spec.

- [ ] **Step 1: Create `src/commands/compact.rs`**

Create `/Users/ericssoncolborn/Documents/searchdb-rs/src/commands/compact.rs` with:

```rust
use crate::compact::{CompactOptions, CompactWorker};
use crate::error::{Result, SearchDbError};
use crate::storage::Storage;

/// Run the compact worker with the given CLI options.
pub async fn run(
    storage: &Storage,
    name: &str,
    segment_size: usize,
    merge_interval: u64,
    max_segments: usize,
    poll_interval: u64,
    max_segment_age: u64,
    force_merge: bool,
    once: bool,
) -> Result<()> {
    if !storage.exists(name) {
        return Err(SearchDbError::IndexNotFound(name.to_string()));
    }

    let opts = CompactOptions {
        segment_size,
        merge_interval_secs: merge_interval * 60, // CLI takes minutes, internal uses seconds
        max_segments,
        poll_interval_secs: poll_interval,
        max_segment_age_secs: max_segment_age,
        force_merge,
        once,
    };

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
                    eprintln!("\n[dsrch] compact: received SIGINT, shutting down gracefully...");
                }
                _ = sigterm.recv() => {
                    eprintln!("[dsrch] compact: received SIGTERM, shutting down gracefully...");
                }
            }
        }
        #[cfg(not(unix))]
        {
            ctrl_c.await.ok();
            eprintln!("\n[dsrch] compact: received Ctrl+C, shutting down gracefully...");
        }
        let _ = shutdown_tx.send(true);
    });

    let result = worker.run(shutdown_rx).await;

    // Cancel signal handler if worker finished on its own (--once / --force-merge)
    signal_handle.abort();

    result
}
```

- [ ] **Step 2: Register the module in `src/commands/mod.rs`**

In `/Users/ericssoncolborn/Documents/searchdb-rs/src/commands/mod.rs`, add after the `pub mod connect_delta;` line (inside the `#[cfg(feature = "delta")]` block):

```rust
#[cfg(feature = "delta")]
pub mod compact;
```

The full file should look like:

```rust
#[cfg(feature = "delta")]
pub mod compact;
#[cfg(feature = "delta")]
pub mod connect_delta;
pub mod drop;
pub mod get;
pub mod index;
pub mod new_index;
#[cfg(feature = "delta")]
pub mod reindex;
pub mod search;
pub mod stats;
#[cfg(feature = "delta")]
pub mod sync;
```

- [ ] **Step 3: Add the `Compact` command variant to `main.rs`**

In `/Users/ericssoncolborn/Documents/searchdb-rs/src/main.rs`, add the following variant to the `Commands` enum, after the `Sync` variant (around line 143):

```rust
    /// Tiered compaction worker: poll Delta, create segments, merge on schedule
    #[cfg(feature = "delta")]
    Compact {
        /// Index name
        name: String,
        /// Rows per segment before commit
        #[arg(long, default_value_t = 10_000)]
        segment_size: usize,
        /// Minutes between merge checks
        #[arg(long, default_value_t = 5)]
        merge_interval: u64,
        /// Merge when segment count exceeds this
        #[arg(long, default_value_t = 10)]
        max_segments: usize,
        /// Seconds between Delta polls
        #[arg(long, default_value_t = 10)]
        poll_interval: u64,
        /// Force commit after N seconds even if under segment_size threshold
        #[arg(long, default_value_t = 60)]
        max_segment_age: u64,
        /// One-shot: merge all segments into one and exit
        #[arg(long, default_value_t = false)]
        force_merge: bool,
        /// One-shot: poll once, segment if needed, merge if needed, exit
        #[arg(long, default_value_t = false)]
        once: bool,
    },
```

- [ ] **Step 4: Add the dispatch arm in `run_cli()`**

In `/Users/ericssoncolborn/Documents/searchdb-rs/src/main.rs`, inside the `run_cli()` function's `match cli.command` block (around line 173), add a new match arm before the closing brace of the match. Add it after the `Commands::Reindex` arm (around line 219):

```rust
        Commands::Compact {
            name,
            segment_size,
            merge_interval,
            max_segments,
            poll_interval,
            max_segment_age,
            force_merge,
            once,
        } => {
            commands::compact::run(
                &storage,
                &name,
                segment_size,
                merge_interval,
                max_segments,
                poll_interval,
                max_segment_age,
                force_merge,
                once,
            )
            .await
        }
```

- [ ] **Step 5: Verify compilation and help output**

Run: `cargo build`
Expected: Compiles successfully.

Run: `cargo run -- compact --help`
Expected: Shows help with all flags (`--segment-size`, `--merge-interval`, `--max-segments`, `--poll-interval`, `--max-segment-age`, `--force-merge`, `--once`).

- [ ] **Step 6: Commit**

```bash
git add src/commands/compact.rs src/commands/mod.rs src/main.rs
git commit -m "feat(cli): add dsrch compact command with all flags"
```

---

## Task 5: Make `dsrch sync` an alias for `compact --once` with deprecation warning

**Files:**
- Modify: `/Users/ericssoncolborn/Documents/searchdb-rs/src/commands/sync.rs`

- [ ] **Step 1: Replace the sync implementation with a deprecation wrapper**

Replace the entire contents of `/Users/ericssoncolborn/Documents/searchdb-rs/src/commands/sync.rs` with:

```rust
use crate::compact::{CompactOptions, CompactWorker};
use crate::error::{Result, SearchDbError};
use crate::storage::Storage;

/// Manual incremental sync from a Delta Lake source.
///
/// DEPRECATED: This command is now an alias for `compact --once`.
/// Use `dsrch compact <name> --once` instead.
pub async fn run(storage: &Storage, name: &str) -> Result<()> {
    eprintln!(
        "[dsrch] WARNING: 'sync' is deprecated. Use 'compact --once' instead."
    );

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
```

- [ ] **Step 2: Verify existing sync behavior**

Run: `cargo build`
Expected: Compiles successfully.

Run: `cargo test`
Expected: All tests pass. (The existing sync tests in `delta.rs` test the Delta layer directly, not through the sync command, so they are unaffected.)

- [ ] **Step 3: Commit**

```bash
git add src/commands/sync.rs
git commit -m "feat(sync): deprecate sync command, make it an alias for compact --once"
```

---

## Task 6: Add compaction metadata to `dsrch stats`

**Files:**
- Modify: `/Users/ericssoncolborn/Documents/searchdb-rs/src/commands/stats.rs`

- [ ] **Step 1: Write a failing test for compaction metadata in stats output**

Add to the tests module in `/Users/ericssoncolborn/Documents/searchdb-rs/src/commands/stats.rs`:

```rust
    #[test]
    fn test_stats_with_compact_meta() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_str().unwrap());
        new_index::run(&storage, "test", r#"{"fields":{"name":"keyword"}}"#, false).unwrap();

        // Manually inject compact metadata
        let mut config = storage.load_config("test").unwrap();
        config.compact = Some(crate::storage::CompactMeta {
            segment_size: 50_000,
            merge_interval_secs: 600,
            max_segments: 20,
            last_segment_at: Some("2026-03-24T14:30:00Z".into()),
            last_merge_at: Some("2026-03-24T14:25:00Z".into()),
        });
        storage.save_config("test", &config).unwrap();

        // Should not crash; verifies the compact metadata is read and displayed
        run(&storage, "test", OutputFormat::Json, None).unwrap();
    }
```

Run: `cargo test --lib commands::stats::tests::test_stats_with_compact_meta`
Expected: **Passes** even before modifying stats output (it just doesn't display compact info yet). The test verifies no crash. We will enhance it after adding the display logic.

- [ ] **Step 2: Update the JSON output in stats to include compaction metadata**

In `/Users/ericssoncolborn/Documents/searchdb-rs/src/commands/stats.rs`, in the `run` function, find the `OutputFormat::Json` branch (around line 34). Replace it with:

```rust
        OutputFormat::Json => {
            let mut stats = serde_json::json!({
                "index": name,
                "num_docs": num_docs,
                "num_segments": num_segments,
                "fields": &config.schema.fields,
                "index_version": config.index_version,
            });
            if let Some((delta_ver, gap_ver)) = gap_info {
                stats["delta_version"] = serde_json::json!(delta_ver);
                stats["gap_versions"] = serde_json::json!(gap_ver);
            }
            if let Some(ref compact) = config.compact {
                stats["compaction"] = serde_json::json!({
                    "segment_size": compact.segment_size,
                    "merge_interval_secs": compact.merge_interval_secs,
                    "max_segments": compact.max_segments,
                    "last_segment_at": compact.last_segment_at,
                    "last_merge_at": compact.last_merge_at,
                });
            }
            println!("{}", serde_json::to_string(&stats)?);
        }
```

- [ ] **Step 3: Update the Text output to include compaction metadata**

In the `OutputFormat::Text` branch (around line 48), add after the Delta line (before the closing `}` of the Text branch):

```rust
            if let Some(ref compact) = config.compact {
                println!("Compaction:");
                println!("  Segment size: {}", compact.segment_size);
                println!("  Merge interval: {}s", compact.merge_interval_secs);
                println!("  Max segments: {}", compact.max_segments);
                if let Some(ref ts) = compact.last_segment_at {
                    println!("  Last segment: {ts}");
                }
                if let Some(ref ts) = compact.last_merge_at {
                    println!("  Last merge:   {ts}");
                }
            }
```

- [ ] **Step 4: Run tests**

Run: `cargo test`
Expected: All tests pass including the new `test_stats_with_compact_meta`.

- [ ] **Step 5: Commit**

```bash
git add src/commands/stats.rs
git commit -m "feat(stats): display compaction metadata in stats output"
```

---

## Task 7: Integration test — compact --once with a local Delta table

**Files:**
- Create: `/Users/ericssoncolborn/Documents/searchdb-rs/tests/compact_integration.rs`

This test creates a Delta table, connects an index, adds more rows, then runs compact --once and verifies segments were created correctly.

- [ ] **Step 1: Create the integration test file**

Create `/Users/ericssoncolborn/Documents/searchdb-rs/tests/compact_integration.rs` with:

```rust
//! Integration test: compact worker with a local Delta table.
//!
//! Creates a Delta table, connects an index via connect-delta,
//! adds more rows to Delta, runs compact --once, and verifies
//! the index was updated with correct segment structure.

#[cfg(feature = "delta")]
mod tests {
    use arrow::array::{Float64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use deltalake::operations::create::CreateBuilder;
    use deltalake::DeltaOps;
    use std::sync::Arc;

    fn test_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn make_batch(rows: &[(&str, &str, f64)]) -> RecordBatch {
        let schema = test_arrow_schema();
        let ids: Vec<&str> = rows.iter().map(|(id, _, _)| *id).collect();
        let names: Vec<&str> = rows.iter().map(|(_, name, _)| *name).collect();
        let values: Vec<f64> = rows.iter().map(|(_, _, val)| *val).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    async fn create_delta_table(path: &str, rows: &[(&str, &str, f64)]) {
        let schema = test_arrow_schema();
        let batch = make_batch(rows);

        let table = CreateBuilder::new()
            .with_location(path)
            .with_columns(
                deltalake::kernel::StructType::try_from(schema.as_ref())
                    .unwrap()
                    .fields()
                    .cloned(),
            )
            .await
            .unwrap();

        DeltaOps(table).write(vec![batch]).await.unwrap();
    }

    async fn append_to_delta(path: &str, rows: &[(&str, &str, f64)]) {
        let batch = make_batch(rows);
        let table = deltalake::open_table(path).await.unwrap();
        DeltaOps(table).write(vec![batch]).await.unwrap();
    }

    #[tokio::test]
    async fn test_compact_once_creates_segments() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();
        let data_dir = dir.path().join("dsrch_data");
        let data_str = data_dir.to_str().unwrap();

        // 1. Create Delta table with initial data
        create_delta_table(
            delta_str,
            &[
                ("d1", "glucose", 100.0),
                ("d2", "a1c", 5.7),
            ],
        )
        .await;

        // 2. Connect index
        let storage = searchdb::storage::Storage::new(data_str);
        searchdb::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            r#"{"fields":{"name":"keyword","value":"numeric"}}"#,
        )
        .await
        .unwrap();

        // Verify initial state
        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 2);

        // 3. Append more rows to Delta (creates a gap)
        append_to_delta(
            delta_str,
            &[
                ("d3", "creatinine", 1.2),
                ("d4", "bun", 15.0),
                ("d5", "sodium", 140.0),
            ],
        )
        .await;

        // 4. Run compact --once
        let opts = searchdb::compact::CompactOptions {
            segment_size: 2, // small batches to test splitting
            once: true,
            ..searchdb::compact::CompactOptions::default()
        };
        let worker = searchdb::compact::CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        worker.run(rx).await.unwrap();

        // 5. Verify: should have original docs + new docs
        // Reload index to pick up new segments
        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 5, "should have 5 total documents");

        // Verify segments: 1 from connect-delta + 2 from compact (3 rows / segment_size=2 = 2 batches)
        let segment_count = searcher.segment_readers().len();
        assert!(
            segment_count >= 2,
            "should have at least 2 segments, got {segment_count}"
        );

        // 6. Verify config was updated
        let config = storage.load_config("lab").unwrap();
        assert!(config.compact.is_some(), "compact metadata should be saved");
    }

    #[tokio::test]
    async fn test_compact_force_merge_reduces_segments() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();
        let data_dir = dir.path().join("dsrch_data");
        let data_str = data_dir.to_str().unwrap();

        // Create Delta table
        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        // Connect and create index
        let storage = searchdb::storage::Storage::new(data_str);
        searchdb::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            r#"{"fields":{"name":"keyword","value":"numeric"}}"#,
        )
        .await
        .unwrap();

        // Add rows in multiple appends to create multiple segments
        for i in 2..=6 {
            append_to_delta(delta_str, &[(&format!("d{i}"), "test", i as f64)]).await;

            // Run compact --once with segment_size=1 to force one segment per batch
            let opts = searchdb::compact::CompactOptions {
                segment_size: 1,
                once: true,
                ..searchdb::compact::CompactOptions::default()
            };
            let worker = searchdb::compact::CompactWorker::new(&storage, "lab", opts);
            let (_tx, rx) = tokio::sync::watch::channel(false);
            worker.run(rx).await.unwrap();
        }

        // Check we have multiple segments
        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        let before_merge = reader.searcher().segment_readers().len();
        assert!(
            before_merge > 1,
            "should have multiple segments before force-merge, got {before_merge}"
        );

        // Force-merge
        let opts = searchdb::compact::CompactOptions {
            force_merge: true,
            ..searchdb::compact::CompactOptions::default()
        };
        let worker = searchdb::compact::CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        worker.run(rx).await.unwrap();

        // Verify: should have exactly 1 segment after force-merge
        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        let after_merge = reader.searcher().segment_readers().len();
        assert_eq!(after_merge, 1, "should have 1 segment after force-merge");

        // Verify doc count is preserved
        assert_eq!(reader.searcher().num_docs(), 6);
    }

    #[tokio::test]
    async fn test_compact_upsert_dedup() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();
        let data_dir = dir.path().join("dsrch_data");
        let data_str = data_dir.to_str().unwrap();

        // Create Delta table with initial data
        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        // Connect
        let storage = searchdb::storage::Storage::new(data_str);
        searchdb::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            r#"{"fields":{"name":"keyword","value":"numeric"}}"#,
        )
        .await
        .unwrap();

        // Append row with same _id but different value (upsert scenario)
        append_to_delta(delta_str, &[("d1", "glucose_updated", 200.0)]).await;

        // Run compact --once
        let opts = searchdb::compact::CompactOptions {
            once: true,
            ..searchdb::compact::CompactOptions::default()
        };
        let worker = searchdb::compact::CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        worker.run(rx).await.unwrap();

        // Force-merge to consolidate deletes
        let opts = searchdb::compact::CompactOptions {
            force_merge: true,
            ..searchdb::compact::CompactOptions::default()
        };
        let worker = searchdb::compact::CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        worker.run(rx).await.unwrap();

        // Should have only 1 doc after merge (upsert replaced old d1)
        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        assert_eq!(
            reader.searcher().num_docs(),
            1,
            "upsert should keep only latest version"
        );
    }

    #[tokio::test]
    async fn test_compact_writer_lock_detection() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();
        let data_dir = dir.path().join("dsrch_data");
        let data_str = data_dir.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        let storage = searchdb::storage::Storage::new(data_str);
        searchdb::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            r#"{"fields":{"name":"keyword","value":"numeric"}}"#,
        )
        .await
        .unwrap();

        // Acquire the writer lock
        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let _writer = index.writer(50_000_000).unwrap(); // holds the lock

        // Try to start compact — should get WriterLocked error
        let opts = searchdb::compact::CompactOptions {
            once: true,
            ..searchdb::compact::CompactOptions::default()
        };
        let worker = searchdb::compact::CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let result = worker.run(rx).await;
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Another writer"),
            "error should mention writer lock: {err_msg}"
        );
    }
}
```

- [ ] **Step 2: Make crate types public for integration tests**

Integration tests (in the `tests/` directory) can only access `pub` items from the crate's public API. We need to ensure the relevant types are accessible.

In `/Users/ericssoncolborn/Documents/searchdb-rs/src/main.rs`, add `pub` visibility to the module declarations that the integration tests need. Find these lines near the top of the file:

```rust
mod commands;
```
Change to:
```rust
pub mod commands;
```

```rust
mod storage;
```
Change to:
```rust
pub mod storage;
```

```rust
mod compact;
```
Change to:
```rust
pub mod compact;
```

Also make `schema`, `writer`, `searcher`, `delta`, and `error` modules public if they are not already. Specifically, these lines should all use `pub mod`:

```rust
pub mod commands;
#[cfg(feature = "delta")]
pub mod delta;
pub mod error;
pub mod schema;
pub mod searcher;
pub mod storage;
pub mod writer;
#[cfg(feature = "delta")]
pub mod compact;
```

Note: The binary crate needs to be also a library crate for integration tests to import from it. Add a `[lib]` section to `Cargo.toml` if not present:

In `/Users/ericssoncolborn/Documents/searchdb-rs/Cargo.toml`, add after the `[[bin]]` section:

```toml
[lib]
name = "searchdb"
path = "src/lib.rs"
```

Then create `/Users/ericssoncolborn/Documents/searchdb-rs/src/lib.rs` with:

```rust
pub mod commands;
#[cfg(feature = "delta")]
pub mod compact;
#[cfg(feature = "delta")]
pub mod delta;
pub mod error;
pub mod schema;
pub mod searcher;
pub mod storage;
pub mod writer;
```

And update `/Users/ericssoncolborn/Documents/searchdb-rs/src/main.rs` to re-use the library modules instead of declaring them directly. Replace the module declarations at the top of `main.rs` with:

```rust
use searchdb::commands;
#[cfg(feature = "delta")]
use searchdb::delta;
use searchdb::error;
use searchdb::schema;
use searchdb::searcher;
use searchdb::storage;
use searchdb::storage::Storage;
use searchdb::writer;
#[cfg(feature = "delta")]
use searchdb::compact;
```

Remove the original `mod` declarations for those modules. Keep the `use clap::{Parser, Subcommand, ValueEnum};` and the rest of the file unchanged.

**Important:** The `OutputFormat` enum and `Cli` struct stay in `main.rs` since they are binary-only. But move `OutputFormat` to `lib.rs` so that `commands::stats` and `commands::search` can reference it:

Add to `/Users/ericssoncolborn/Documents/searchdb-rs/src/lib.rs`:

```rust
use clap::ValueEnum;

/// Output format for commands that return data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// NDJSON to stdout (default when piped)
    Json,
    /// Human-readable table (default in terminal)
    Text,
}
```

Then in `main.rs`, replace the `OutputFormat` definition with:

```rust
pub use searchdb::OutputFormat;
```

And update any `use crate::OutputFormat` in command files to `use crate::OutputFormat` (which now resolves through lib.rs).

**Alternative simpler approach:** If the above refactor is too invasive, keep all modules in `main.rs` and instead write the integration tests as `#[cfg(test)]` unit tests within `src/compact.rs` that use `#[tokio::test]` with the delta feature. This avoids the lib/bin split entirely:

Move the integration tests into `/Users/ericssoncolborn/Documents/searchdb-rs/src/compact.rs` as additional test functions within its existing `#[cfg(test)] mod tests` block. This is the **recommended approach** to avoid restructuring the crate.

So instead of creating `tests/compact_integration.rs`, add all the test functions from Step 1 into the `#[cfg(test)] mod tests` block at the bottom of `/Users/ericssoncolborn/Documents/searchdb-rs/src/compact.rs`. Replace `searchdb::` prefixes with `crate::` since we are inside the crate:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compact_options_defaults() {
        let opts = CompactOptions::default();
        assert_eq!(opts.segment_size, 10_000);
        assert_eq!(opts.merge_interval_secs, 300);
        assert_eq!(opts.max_segments, 10);
        assert_eq!(opts.poll_interval_secs, 10);
        assert_eq!(opts.max_segment_age_secs, 60);
        assert!(!opts.force_merge);
        assert!(!opts.once);
    }

    // --- Delta integration tests below require the "delta" feature ---

    use arrow::array::{Float64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use deltalake::operations::create::CreateBuilder;
    use deltalake::DeltaOps;
    use std::sync::Arc;

    fn test_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn make_batch(rows: &[(&str, &str, f64)]) -> RecordBatch {
        let schema = test_arrow_schema();
        let ids: Vec<&str> = rows.iter().map(|(id, _, _)| *id).collect();
        let names: Vec<&str> = rows.iter().map(|(_, name, _)| *name).collect();
        let values: Vec<f64> = rows.iter().map(|(_, _, val)| *val).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    async fn create_delta_table(path: &str, rows: &[(&str, &str, f64)]) {
        let schema = test_arrow_schema();
        let batch = make_batch(rows);

        let table = CreateBuilder::new()
            .with_location(path)
            .with_columns(
                deltalake::kernel::StructType::try_from(schema.as_ref())
                    .unwrap()
                    .fields()
                    .cloned(),
            )
            .await
            .unwrap();

        DeltaOps(table).write(vec![batch]).await.unwrap();
    }

    async fn append_to_delta(path: &str, rows: &[(&str, &str, f64)]) {
        let batch = make_batch(rows);
        let table = deltalake::open_table(path).await.unwrap();
        DeltaOps(table).write(vec![batch]).await.unwrap();
    }

    #[tokio::test]
    async fn test_compact_once_creates_segments() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();
        let data_dir = dir.path().join("dsrch_data");
        let data_str = data_dir.to_str().unwrap();

        create_delta_table(
            delta_str,
            &[("d1", "glucose", 100.0), ("d2", "a1c", 5.7)],
        )
        .await;

        let storage = crate::storage::Storage::new(data_str);
        crate::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            r#"{"fields":{"name":"keyword","value":"numeric"}}"#,
        )
        .await
        .unwrap();

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 2);

        append_to_delta(
            delta_str,
            &[
                ("d3", "creatinine", 1.2),
                ("d4", "bun", 15.0),
                ("d5", "sodium", 140.0),
            ],
        )
        .await;

        let opts = CompactOptions {
            segment_size: 2,
            once: true,
            ..CompactOptions::default()
        };
        let worker = CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        worker.run(rx).await.unwrap();

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 5, "should have 5 total documents");

        let segment_count = searcher.segment_readers().len();
        assert!(
            segment_count >= 2,
            "should have at least 2 segments, got {segment_count}"
        );

        let config = storage.load_config("lab").unwrap();
        assert!(config.compact.is_some(), "compact metadata should be saved");
    }

    #[tokio::test]
    async fn test_compact_force_merge_reduces_segments() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();
        let data_dir = dir.path().join("dsrch_data");
        let data_str = data_dir.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        let storage = crate::storage::Storage::new(data_str);
        crate::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            r#"{"fields":{"name":"keyword","value":"numeric"}}"#,
        )
        .await
        .unwrap();

        for i in 2..=6 {
            append_to_delta(delta_str, &[(&format!("d{i}"), "test", i as f64)]).await;

            let opts = CompactOptions {
                segment_size: 1,
                once: true,
                ..CompactOptions::default()
            };
            let worker = CompactWorker::new(&storage, "lab", opts);
            let (_tx, rx) = tokio::sync::watch::channel(false);
            worker.run(rx).await.unwrap();
        }

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        let before_merge = reader.searcher().segment_readers().len();
        assert!(
            before_merge > 1,
            "should have multiple segments before force-merge, got {before_merge}"
        );

        let opts = CompactOptions {
            force_merge: true,
            ..CompactOptions::default()
        };
        let worker = CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        worker.run(rx).await.unwrap();

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        let after_merge = reader.searcher().segment_readers().len();
        assert_eq!(after_merge, 1, "should have 1 segment after force-merge");
        assert_eq!(reader.searcher().num_docs(), 6);
    }

    #[tokio::test]
    async fn test_compact_upsert_dedup() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();
        let data_dir = dir.path().join("dsrch_data");
        let data_str = data_dir.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        let storage = crate::storage::Storage::new(data_str);
        crate::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            r#"{"fields":{"name":"keyword","value":"numeric"}}"#,
        )
        .await
        .unwrap();

        append_to_delta(delta_str, &[("d1", "glucose_updated", 200.0)]).await;

        let opts = CompactOptions {
            once: true,
            ..CompactOptions::default()
        };
        let worker = CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        worker.run(rx).await.unwrap();

        let opts = CompactOptions {
            force_merge: true,
            ..CompactOptions::default()
        };
        let worker = CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        worker.run(rx).await.unwrap();

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let reader = index.reader().unwrap();
        assert_eq!(
            reader.searcher().num_docs(),
            1,
            "upsert should keep only latest version"
        );
    }

    #[tokio::test]
    async fn test_compact_writer_lock_detection() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();
        let data_dir = dir.path().join("dsrch_data");
        let data_str = data_dir.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        let storage = crate::storage::Storage::new(data_str);
        crate::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            r#"{"fields":{"name":"keyword","value":"numeric"}}"#,
        )
        .await
        .unwrap();

        let index = tantivy::Index::open_in_dir(storage.tantivy_dir("lab")).unwrap();
        let _writer = index.writer(50_000_000).unwrap();

        let opts = CompactOptions {
            once: true,
            ..CompactOptions::default()
        };
        let worker = CompactWorker::new(&storage, "lab", opts);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let result = worker.run(rx).await;
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Another writer"),
            "error should mention writer lock: {err_msg}"
        );
    }
}
```

- [ ] **Step 3: Run integration tests**

Run: `cargo test compact::tests`
Expected: All 5 compact tests pass plus the original `test_compact_options_defaults`.

- [ ] **Step 4: Commit**

```bash
git add src/compact.rs
git commit -m "test(compact): add integration tests for compact --once, force-merge, upsert, and lock detection"
```

---

## Task 8: Signal handling for graceful shutdown

**Files:**
- Already implemented in: `/Users/ericssoncolborn/Documents/searchdb-rs/src/commands/compact.rs` (Task 4)
- Already implemented in: `/Users/ericssoncolborn/Documents/searchdb-rs/src/compact.rs` (Task 3 — `sleep_or_shutdown`, `wait_merging_threads`)

Signal handling was built into the compact command in Tasks 3 and 4. This task verifies it works correctly.

- [ ] **Step 1: Write a test for graceful shutdown**

Add to the test module in `/Users/ericssoncolborn/Documents/searchdb-rs/src/compact.rs`:

```rust
    #[tokio::test]
    async fn test_compact_shutdown_signal() {
        let dir = tempfile::tempdir().unwrap();
        let delta_path = dir.path().join("delta_table");
        let delta_str = delta_path.to_str().unwrap();
        let data_dir = dir.path().join("dsrch_data");
        let data_str = data_dir.to_str().unwrap();

        create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

        let storage = crate::storage::Storage::new(data_str);
        crate::commands::connect_delta::run(
            &storage,
            "lab",
            delta_str,
            r#"{"fields":{"name":"keyword","value":"numeric"}}"#,
        )
        .await
        .unwrap();

        // Run compact in continuous mode but immediately signal shutdown
        let opts = CompactOptions {
            poll_interval_secs: 1,
            ..CompactOptions::default()
        };
        let worker = CompactWorker::new(&storage, "lab", opts);
        let (tx, rx) = tokio::sync::watch::channel(false);

        // Send shutdown signal immediately
        tx.send(true).unwrap();

        // Worker should exit quickly after seeing the signal
        let result = tokio::time::timeout(
            tokio::time::Duration::from_secs(5),
            worker.run(rx),
        )
        .await;

        assert!(result.is_ok(), "worker should exit within 5 seconds of shutdown signal");
        assert!(result.unwrap().is_ok(), "worker should exit cleanly on shutdown");
    }
```

- [ ] **Step 2: Run the test**

Run: `cargo test compact::tests::test_compact_shutdown_signal`
Expected: Passes. The worker sees the shutdown signal and exits.

- [ ] **Step 3: Commit**

```bash
git add src/compact.rs
git commit -m "test(compact): add graceful shutdown signal test"
```

---

## Task 9: Final verification — fmt + clippy + test + full E2E

**Files:** None modified. This is a verification-only task.

- [ ] **Step 1: Format**

Run: `cargo fmt`
Expected: No changes (or formats files cleanly).

- [ ] **Step 2: Clippy**

Run: `cargo clippy -- -D warnings`
Expected: No warnings or errors.

If clippy reports issues, fix them. Common ones:
- `map_err(|e| SearchDbError::Tantivy(e))` can be simplified to `map_err(SearchDbError::Tantivy)` (use the function pointer form).
- Unused imports.
- `&segment_ids` vs `segment_ids.as_slice()`.

Fix any issues and re-run until clean.

- [ ] **Step 3: Full test suite**

Run: `cargo test`
Expected: All tests pass (existing tests + new compact tests + storage tests + stats tests).

The expected test count should include at least these new tests:
- `compact::tests::test_compact_options_defaults`
- `compact::tests::test_compact_once_creates_segments`
- `compact::tests::test_compact_force_merge_reduces_segments`
- `compact::tests::test_compact_upsert_dedup`
- `compact::tests::test_compact_writer_lock_detection`
- `compact::tests::test_compact_shutdown_signal`
- `storage::tests::test_compact_meta_round_trip`
- `storage::tests::test_config_without_compact_meta_loads`
- `commands::stats::tests::test_stats_with_compact_meta`

- [ ] **Step 4: Manual E2E smoke test**

Run:
```bash
# Create a test index
cargo run -- new smoketest --schema '{"fields":{"name":"keyword","value":"numeric"}}'

# Verify compact --help
cargo run -- compact --help

# Verify compact errors without Delta source
cargo run -- compact smoketest --once 2>&1 | grep "no Delta source"

# Clean up
cargo run -- drop smoketest
```

Expected: Each command behaves as expected. The `compact smoketest --once` should fail with a message about no Delta source.

- [ ] **Step 5: Final commit (if any fixes were needed)**

```bash
git add -A
git commit -m "fix: address clippy and fmt issues from final verification"
```

---

## Summary of Deliverables

| Task | What | Files |
|------|------|-------|
| 1 | `CompactMeta` struct + `compact` field in `IndexConfig` | `src/storage.rs`, `src/commands/connect_delta.rs`, `src/commands/new_index.rs`, `src/commands/reindex.rs` |
| 2 | `WriterLocked` error variant | `src/error.rs` |
| 3 | `CompactWorker` with L1 segmentation + L2 merge | `src/compact.rs`, `src/main.rs` |
| 4 | `dsrch compact` CLI command with all flags | `src/commands/compact.rs`, `src/commands/mod.rs`, `src/main.rs` |
| 5 | Deprecate `sync` as alias for `compact --once` | `src/commands/sync.rs` |
| 6 | Compaction metadata in `dsrch stats` | `src/commands/stats.rs` |
| 7 | Integration tests (segment creation, merge, upsert, lock) | `src/compact.rs` |
| 8 | Signal handling test | `src/compact.rs` |
| 9 | Final verification (fmt, clippy, test, E2E) | (no new files) |
