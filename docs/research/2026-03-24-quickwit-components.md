# Quickwit Component Evaluation for SearchDB

**Date:** 2026-03-24
**Status:** Research complete
**Repo:** https://github.com/quickwit-oss/quickwit (commit 4864139b on main)

## Executive Summary

We evaluated four quickwit subsystems for potential reuse in SearchDB: quickwit-indexing, quickwit-metastore, quickwit-directories, and the merge policy code. The findings are mixed. The merge policy algorithm (StableLogMergePolicy) is the standout win -- it solves exactly the problem our tiered compaction worker needs, and it can be extracted in ~300 lines. The rest of quickwit's indexing and metastore infrastructure is tightly coupled to a distributed actor system that is fundamentally different from SearchDB's single-binary, single-writer model. Borrowing ideas from the metastore's split lifecycle model and the directories crate's bundle format is worthwhile, but taking actual code is not.

---

## 1. quickwit-indexing

### What It Does

quickwit-indexing implements a distributed indexing pipeline as a chain of message-passing actors: Source -> DocProcessor -> Indexer -> IndexSerializer -> Packager -> Uploader -> Sequencer -> Publisher. Each actor runs in its own task, connected by typed mailboxes. The crate also contains a parallel merge pipeline: MergePlanner -> MergeSplitDownloader -> MergeExecutor -> Packager -> Uploader -> Publisher.

The crate is the heart of quickwit's write path. It handles reading data from Kafka/Kinesis/Pulsar/files, parsing documents, building tantivy segments ("splits" in quickwit terminology), bundling them into a single-file format, uploading to object storage, and registering them in the metastore. The merge pipeline runs alongside, downloading immature splits, merging them via tantivy's `IndexWriter::merge()`, and re-uploading the result.

### Key Implementation Details

**Indexer actor (batching/buffering):**
- Uses `SingleSegmentIndexWriter` (not the standard `IndexWriter`) to write one segment per commit cycle.
- Buffers documents in an `IndexingWorkbench` containing multiple `IndexedSplitBuilder` instances (one per partition).
- Commits are triggered by four conditions: memory limit (heap usage >= configured threshold), document count (>= split_num_docs_target), timeout (configurable commit timeout), or forced commit (graceful shutdown / queue drain).
- Memory tracking is precise: measures `index_writer.mem_usage()` before and after each document add, accumulates deltas.

**Backpressure:**
- Actor mailboxes have bounded capacity. When a downstream actor is slow, upstream actors block on mailbox send.
- A `CooperativeIndexing` mechanism coordinates multiple pipelines to take turns rather than competing for CPU/disk. Each pipeline sleeps for part of the commit cycle to stagger resource usage.
- A semaphore limits concurrent pipeline spawns to 10.

**IndexSerializer actor:**
- Calls `index_writer.finalize()` on each `IndexedSplitBuilder` to produce a complete tantivy segment.
- Runs in a blocking tokio thread pool (CPU-heavy operation).

**Packager actor:**
- Lists all segment files from the scratch directory.
- Extracts tag field values from the term dictionary (up to 1,000 unique values per tag field).
- Generates a "hotcache" -- pre-warmed byte ranges for term dictionaries and doc stores.
- Bundles everything into quickwit's single-file split format.

**MergeExecutor:**
- Creates a `UnionDirectory` stacking all input split directories plus a RAM directory for combined metadata.
- Opens the union as a single tantivy `Index`.
- Calls `index_writer.merge(&segment_ids)` to produce a single merged segment.
- Uses a 15MB writer buffer for controlled resource usage during merges.
- For delete-and-merge operations, applies deletion queries before merging.

**Uploader actor:**
- Stages splits in the metastore before uploading (metadata first, data second).
- Concurrent upload semaphores (separate limits for indexing vs. merge uploads).
- Builds a split payload streamer from: split files + serialized field metadata + hotcache bytes.

### What's Useful for SearchDB

**1. StableLogMergePolicy algorithm (HIGH VALUE)**
Located in `merge_policy/stable_log_merge_policy.rs`. This is a logarithmic tiering strategy that:
- Sorts splits by timestamp (newest first) for deterministic ordering.
- Assigns splits to levels with 3x growth factor between levels (level 0 holds up to 3x `min_level_num_docs`, each subsequent level holds 3x the previous, capped at `split_num_docs_target`).
- Within each level, merges consecutive groups of `merge_factor` to `max_merge_factor` splits.
- Never merges splits that would exceed `split_num_docs_target`.
- Has a maturity concept: splits above the target size are "mature" and excluded from merge consideration.
- Has a `finalize_operations()` method for end-of-day style final merges.

This is directly applicable to SearchDB's Level 2 merge strategy. Our current spec uses a simple "group by exponential bucket, merge if count > factor" approach. The StableLogMergePolicy is a more sophisticated version of the same idea.

**2. Memory-based commit triggering (MEDIUM VALUE)**
The pattern of tracking `index_writer.mem_usage()` before/after each document add, and triggering commit when heap exceeds a threshold. This is a better approach than our current "commit after N documents" strategy because it accounts for variable document sizes. A 10KB JSON document uses more memory than a 100-byte one.

**3. ConstWriteAmplificationMergePolicy (LOW-MEDIUM VALUE)**
Alternative merge policy that targets a fixed number of merge operations rather than a fixed segment size. Useful when partition sizes vary dramatically (e.g., multi-tenant with small and large tenants). Groups splits by `num_merge_ops` count and only merges within the same merge level. We probably don't need this for v1, but it's worth knowing about.

**4. Controlled directory pattern (LOW VALUE)**
`ControlledDirectory` wraps tantivy's directory with a kill switch and progress tracking. Uses `ArcSwap<IoControls>` for hot-swappable configuration. Useful for cancelling long-running merges, but SearchDB's compact worker handles this via SIGINT/SIGTERM and tantivy's built-in merge cancellation.

### What's NOT Useful (and Why)

**Actor system:** quickwit-indexing is built on `quickwit-actors`, a custom actor framework with typed mailboxes, supervision trees, health checks, and exponential backoff restarts. SearchDB is a single-threaded event loop, not a distributed actor system. The actor infrastructure is ~40% of the crate's code and provides zero value to us.

**Source connectors:** Kafka, Kinesis, Pulsar, GCP PubSub, SQS sources are irrelevant. SearchDB reads from Delta Lake only.

**DocProcessor/VRL transforms:** Document parsing, VRL scripting, OTLP format support. SearchDB handles its own JSON parsing and schema validation.

**Split bundling format:** quickwit bundles all segment files + hotcache into a single file for object storage upload. SearchDB stores the tantivy index directory directly on local disk. We don't need a bundle format unless we move to remote-only storage.

**Cooperative indexing:** Coordinates multiple concurrent indexing pipelines. SearchDB has one writer per index.

**Uploader/Publisher/Sequencer:** These actors handle remote storage uploads, metastore registration, and ordering guarantees for a distributed system. SearchDB writes to local disk and tracks state in `searchdb.json`.

### Dependency Cost

quickwit-indexing has 15+ internal quickwit crate dependencies (quickwit-actors, quickwit-proto, quickwit-config, quickwit-storage, quickwit-doc-mapper, quickwit-ingest, quickwit-metastore, quickwit-cluster, quickwit-common, quickwit-directories, quickwit-query, quickwit-aws, quickwit-opentelemetry) plus heavy external deps (rdkafka, aws-sdk-kinesis, aws-sdk-sqs, google-cloud-pubsub, pulsar). **It is completely impossible to depend on this crate.** The only viable approach is to vendor specific algorithms.

### Recommendation: **Vendor the merge policy, inspire from memory tracking**

- **Vendor** `StableLogMergePolicy` (~200 lines of algorithm + ~100 lines of supporting types). Strip the quickwit-specific types (SplitMetadata, MergeOperation) and replace with SearchDB equivalents. The algorithm itself is pure logic with no external dependencies.
- **Inspire** from the memory-based commit trigger pattern. Implement our own version using `index_writer.mem_usage()` alongside the document count threshold.
- **Skip** everything else. The actor system, source connectors, split bundling, and distributed coordination are architecturally incompatible.

### Estimated Lines if Vendoring

- StableLogMergePolicy algorithm: ~300 lines (after stripping quickwit types and adapting to our SplitMetadata equivalent)
- MergePolicy trait definition: ~50 lines
- ConstWriteAmplificationMergePolicy (optional, for later): ~200 lines
- Total: ~350-550 lines

---

## 2. quickwit-metastore

### What It Does

quickwit-metastore provides a metadata storage service that tracks indexes, splits (segments), and their lifecycle states. It is the source of truth for what data exists in a quickwit cluster -- which splits are staged, published, or marked for deletion, what checkpoint each source has reached, and what schema each index uses.

The crate defines a `MetastoreService` trait with operations for:
- Index CRUD (create, update, delete, list, exists)
- Split lifecycle management (stage, publish, mark for deletion, delete, list with filtering)
- Source management (add, update, remove sources)
- Checkpoint tracking (per-source, per-partition progress through input data)

Two backends implement this trait:
1. **File-backed metastore:** Stores metadata as JSON files on any storage backend (local disk, S3, etc.). Uses `Arc<RwLock<MetastoreState>>` for concurrency within a process and per-index `Arc<Mutex<FileBackedIndex>>` for fine-grained locking. Designed for single-node and testing.
2. **PostgreSQL metastore:** Full SQL backend with migrations, connection pooling, and concurrent access from multiple nodes. Production-grade for distributed quickwit clusters. 11 Rust files plus SQL query templates.

### Key Implementation Details

**Split lifecycle model:**
Splits transition through three states:
- `Staged` -- Split metadata registered, files may be partially uploaded. This is the "intent to publish" state.
- `Published` -- Split is live and searchable. Files fully uploaded and verified.
- `MarkedForDeletion` -- Split is no longer needed (replaced by a merge, or explicitly deleted). Files will be cleaned up.

State transitions are validated: `Staged -> Published -> MarkedForDeletion -> deleted`. Publishing a non-staged split is an error. Publishing can atomically mark replaced splits for deletion (used during merges: the merged split is published while the source splits are marked for deletion in one operation).

**Checkpoint system:**
`SourceCheckpoint` tracks per-partition progress as `BTreeMap<PartitionId, Position>`. Deltas represent `(from, to]` intervals. The system validates that deltas move forward: backwards movement is rejected (prevents duplicate indexing), gaps are tolerated (messages may have been pruned by retention). `IndexCheckpoint` wraps per-source checkpoints for multi-source tracking.

**File-backed index internals:**
`FileBackedIndex` stores splits in a `HashMap<SplitId, Split>` for O(1) lookup. Tracks a `recently_modified` flag to optimize polling (skip disk reads when nothing changed). Uses a `Stamper` for auto-incrementing operation IDs. Supports lazy loading via `LazyFileBackedIndex` -- indexes are loaded from disk on first access rather than eagerly at startup.

**Manifest system:**
The file-backed metastore maintains a manifest file listing all indexes and their states (`Creating`, `Active`, `Deleting`). This prevents data loss during failures: if index creation crashes after writing the manifest but before writing the metadata file, the manifest tracks the incomplete state and cleanup can proceed on restart.

**Concurrency model (file-backed):**
- `RwLock` on the global `MetastoreState` for index-level operations.
- Per-index `Mutex` for split-level mutations.
- `mutate()` method pattern: acquire lock, apply changes, persist to storage; on storage failure, discard cached copy to force reload on next access.
- Optional background polling for read replicas to detect external changes.

### What's Useful for SearchDB

**1. Split lifecycle state machine (MEDIUM VALUE)**
The Staged -> Published -> MarkedForDeletion state model is a clean way to handle the "segment is being written but not yet visible to readers" problem. In SearchDB's compact worker, we currently rely on tantivy's `meta.json` atomicity for this. But if we ever move to a model where segments need explicit promotion (e.g., remote storage), the three-state model is the right abstraction.

Right now, our `searchdb.json` is a flat file with `index_version`. We could evolve it to track per-segment state for better operational visibility (`searchdb stats` showing which segments are being merged, which are mature, etc.).

**2. Checkpoint delta model (LOW-MEDIUM VALUE)**
The `SourceCheckpointDelta` concept of tracking `(from, to]` progress intervals with forward-only validation is elegant. It prevents duplicate indexing while tolerating gaps. SearchDB currently uses a single `index_version` integer (Delta Lake version watermark), which is simpler but less flexible. If we ever support multiple Delta sources per index, the per-partition checkpoint model becomes valuable.

**3. SplitMetadata structure (LOW VALUE)**
The `SplitMetadata` struct tracks useful per-segment information: `num_docs`, `uncompressed_docs_size_in_bytes`, `time_range`, `num_merge_ops`, `create_timestamp`, `maturity`. Some of this feeds into the merge policy (e.g., `num_merge_ops` determines which merge level a segment belongs to, `maturity` determines if it should be considered for merging). We need equivalent metadata for our compact worker's merge decisions.

### What's NOT Useful (and Why)

**PostgreSQL backend:** SearchDB is single-binary, zero-deps. A Postgres dependency would violate the core design principle. The entire `postgres/` directory (11 files + SQL queries) is irrelevant.

**MetastoreService trait / gRPC interface:** The metastore is accessed via a gRPC service (`MetastoreServiceClient`) in quickwit. SearchDB reads/writes `searchdb.json` directly. We don't need a service abstraction.

**Index template matching:** quickwit supports index templates (patterns that auto-create indexes for matching data). SearchDB creates indexes explicitly.

**Shard management:** Per-source shard tracking for Kafka/Kinesis partitions. Irrelevant for Delta Lake.

**Manifest system:** The Creating/Active/Deleting index states with manifest tracking handles distributed cleanup scenarios. SearchDB creates indexes locally and atomically -- no need for distributed state tracking.

**Lazy loading / polling:** Designed for large metastore instances with many indexes that need lazy initialization and change detection. SearchDB loads one index at a time.

### Dependency Cost

quickwit-metastore depends on 6 internal quickwit crates (quickwit-common, quickwit-config, quickwit-doc-mapper, quickwit-proto, quickwit-query, quickwit-storage) plus optional sqlx/sea-query for Postgres. Lighter than quickwit-indexing but still too coupled to the quickwit ecosystem for a direct dependency.

### Recommendation: **Inspire from the split lifecycle and metadata model**

- **Inspire** from the three-state split lifecycle (Staged/Published/MarkedForDeletion). Implement a simpler version in `searchdb.json` that tracks per-segment metadata: state, document count, merge operation count, time range, creation timestamp. This enables smarter merge decisions and better observability.
- **Inspire** from `SplitMetadata` fields. Add `num_merge_ops` tracking to our segment metadata so the merge policy can make level-aware decisions.
- **Skip** the metastore crate entirely as a dependency. The infrastructure (service trait, gRPC, Postgres, manifest, shards, templates) is for a distributed system we don't need.

### Estimated Lines if Inspiring

- Segment metadata struct (equivalent to SplitMetadata subset): ~50 lines
- Segment state enum + transitions: ~30 lines
- Per-segment tracking in searchdb.json: ~80 lines of serde structs + validation
- Total new code: ~160 lines (written from scratch, informed by quickwit's design)

---

## 3. quickwit-directories

### What It Does

quickwit-directories provides custom implementations of tantivy's `Directory` trait for use with remote object storage. The crate contains six directory implementations:

1. **StorageDirectory** -- Wraps a `Storage` trait (quickwit's abstraction over S3/GCS/local) to implement tantivy's `Directory`. Async-only reads; synchronous operations return errors. Read-only (write operations are blocked by a `read_only_directory!` macro).

2. **BundleDirectory** -- Read-only directory over quickwit's single-file split format. The bundle format is: `[Files][FilesMetadata][FilesMetadata length (8 bytes LE)][Hotcache][Hotcache length (8 bytes LE)]`. Parses the footer to build a path-to-byte-range mapping, then serves file reads as slices of the bundle file.

3. **HotDirectory** -- Static caching layer placed in front of another directory. Pre-warms critical byte ranges (term dictionaries, doc store indices) into a cache file during packaging. On read, checks cache first, falls through to underlying storage on miss. Caps non-essential files at 10MB cache size.

4. **CachingDirectory** -- Dynamic caching layer (decorator pattern). Wraps any directory with a `ByteRangeCache`. Both sync and async reads check cache before delegating. Verified by tests showing two sequential reads produce only one underlying read.

5. **UnionDirectory** -- Stacks multiple directories into a single view. First directory that contains a path wins for reads (shadowing). First directory receives all writes. Deletes cascade across all directories. Used during merges to combine multiple split directories with a RAM directory for merged metadata.

6. **DebugProxyDirectory** -- Transparent proxy that records all read operations (file path, byte offset, count, duration, timestamp). Used for hotcache generation (identifies which byte ranges to pre-warm) and performance debugging.

### What's Useful for SearchDB

**1. UnionDirectory for merge operations (MEDIUM VALUE)**
The merge executor uses `UnionDirectory` to present multiple segments as a single tantivy index for merging. This is the same pattern our compact worker's Level 2 merge would use if we ever need to merge segments from different directories. Currently, tantivy's built-in `IndexWriter::merge(&segment_ids)` handles this for us because all our segments live in one directory. But if we move to a model with segments in separate directories (e.g., downloaded from remote storage for merge), the UnionDirectory pattern is the solution.

**2. BundleDirectory format for remote storage (LOW VALUE, FUTURE)**
If SearchDB eventually supports searching directly from object storage (without a local copy), the bundle format is ideal: one GET request retrieves the entire split, and the footer-based metadata allows efficient seeks. But this is a post-v1 concern. Our current model is local disk only.

**3. HotDirectory / caching patterns (LOW VALUE, FUTURE)**
The hotcache concept (pre-warm term dictionaries and store indices) is valuable for reducing cold-start latency when opening indexes. But it only matters for remote storage where each read is an HTTP request. On local disk (SearchDB's model), the OS page cache handles this for free.

**4. DebugProxyDirectory for profiling (LOW VALUE)**
Useful for understanding access patterns during search or indexing. Could be wrapped around our index directory during development to identify optimization opportunities. Very small (~100 lines) and self-contained.

### What's NOT Useful (and Why)

**StorageDirectory:** Depends on `quickwit-storage`, which is a heavy abstraction over S3/GCS/local with retry logic, rate limiting, and credential management. SearchDB uses `deltalake` for blob storage access (reading Delta tables) and tantivy's `MmapDirectory` for the local index. No need for a custom storage layer.

**CachingDirectory:** Only relevant for remote storage. On local disk, OS page cache provides equivalent functionality.

### Dependency Cost

quickwit-directories depends on quickwit-common, quickwit-storage, tantivy, async-trait, postcard (for serialization), tokio, and tracing. The quickwit-storage dependency is the blocker -- it pulls in the entire quickwit storage abstraction layer. The directory implementations themselves are relatively small but they cannot be used without the storage layer.

### Recommendation: **Skip for now, revisit for remote storage**

- **Skip** the entire crate as a dependency or vendor. The local-disk model makes most of these irrelevant, and the quickwit-storage dependency makes vendoring impractical without significant rewrites.
- **Bookmark** the BundleDirectory format and HotDirectory pattern for when/if SearchDB adds remote storage support. At that point, vendor BundleDirectory (~200 lines) and the hotcache generation logic (~300 lines) with our own storage abstraction replacing quickwit-storage.
- **Optionally inspire** from UnionDirectory if our merge executor needs to combine segments from different directories.

### Estimated Lines if Vendoring (Future)

- BundleDirectory (adapted to our storage): ~200 lines
- HotDirectory + hotcache generation: ~400 lines
- UnionDirectory: ~150 lines
- Total: ~750 lines (but only if we add remote storage support)

---

## 4. Merge Policies (Cross-Cutting)

### What Quickwit Offers vs. Tantivy's Default

Tantivy ships with `LogMergePolicy` as its default merge policy. It works by:
- Grouping segments into levels by document count (powers of `level_log_size`, default 0.75).
- Merging segments within a level when the count exceeds `merge_factor` (default 10).
- Has a `min_num_segments` parameter (default 8) below which no merging occurs.
- No concept of maturity, finalization, or write amplification control.

Quickwit provides three alternatives:

**1. StableLogMergePolicy**
- Logarithmic tiering with 3x growth factor between levels.
- Level 0 holds up to `3 * min_level_num_docs` total documents.
- Each subsequent level: `min(previous_level * 3, split_num_docs_target)`.
- Sorts by timestamp (newest first) for deterministic, time-aware merging.
- Splits exceeding `split_num_docs_target` are "mature" and excluded.
- `finalize_operations()` for end-of-lifecycle merges.
- `split_maturity()` returns Mature/Immature with a maturation period.

**2. ConstWriteAmplificationMergePolicy**
- Targets a fixed number of merge operations rather than a target segment size.
- Groups segments by `num_merge_ops` (merge level).
- Only merges within the same level, preventing small segments from being repeatedly re-merged with large ones.
- Configurable `max_merge_ops` cap.
- Designed for multi-tenant workloads with highly variable partition sizes.

**3. NopMergePolicy**
- Returns no merge operations. Used during bulk indexing when merges should be deferred.
- Equivalent to tantivy's `NoMergePolicy`.

### Comparison with SearchDB's Planned Merge Strategy

Our tiered compaction spec describes:
- "Group segments into tiers by document count (exponential buckets)."
- "Within each tier, if the number of segments exceeds a merge factor (default: 8), merge them into one."
- "Never merge segments above --max-segment-docs (default: 5,000,000) -- these are mature and left alone."

This is essentially a simplified version of StableLogMergePolicy. The key differences:

| Feature | SearchDB Spec | StableLogMergePolicy |
|---------|--------------|---------------------|
| Level sizing | Exponential buckets (unspecified base) | 3x growth factor, explicit |
| Ordering | Not specified | Timestamp-based, deterministic |
| Maturity | Size-based only | Size + time-based (maturation period) |
| Finalization | Not specified | Explicit `finalize_operations()` |
| Write amplification | Not controlled | Implicit via level structure |

The StableLogMergePolicy is a strict improvement over what we described in the spec.

### Recommendation: **Vendor StableLogMergePolicy**

This is the highest-value component across all four quickwit subsystems. The algorithm is:
- Pure logic with no I/O or async.
- Well-tested (quickwit uses it in production).
- Directly applicable to our Level 2 merge strategy.
- ~300 lines after adapting types.

Adaptation plan:
1. Define our own `SegmentMeta` struct with fields: `segment_id`, `num_docs`, `num_merge_ops`, `create_timestamp`, `time_range`.
2. Define our own `MergeOperation` struct: `merge_id`, `segment_ids`, `operation_type`.
3. Define a `MergePolicy` trait with `operations()`, `finalize_operations()`, `segment_maturity()`.
4. Port the StableLogMergePolicy algorithm, replacing quickwit's `SplitMetadata` with our `SegmentMeta`.
5. Port the NopMergePolicy (trivial).
6. Optionally port ConstWriteAmplificationMergePolicy later if multi-tenant use cases arise.

---

## Summary Table

| Component | Recommendation | Action | Est. Lines | Value |
|-----------|---------------|--------|-----------|-------|
| StableLogMergePolicy | **Vendor** | Port algorithm, adapt types | ~300 | High |
| ConstWriteAmplificationMergePolicy | Vendor (later) | Port when multi-tenant needed | ~200 | Medium |
| MergePolicy trait | **Vendor** | Port trait definition | ~50 | High |
| Memory-based commit triggers | **Inspire** | Use `mem_usage()` pattern in compact worker | ~20 | Medium |
| Split lifecycle model | **Inspire** | Add segment state tracking to searchdb.json | ~160 | Medium |
| SplitMetadata fields | **Inspire** | Add num_merge_ops, maturity to segment meta | ~50 | Medium |
| Checkpoint delta model | Skip (for now) | Revisit if multi-source support added | -- | Low |
| UnionDirectory | Skip (for now) | Revisit for remote storage merges | -- | Low |
| BundleDirectory | Skip (for now) | Revisit for remote storage | -- | Low |
| HotDirectory / hotcache | Skip (for now) | Revisit for remote storage | -- | Low |
| Actor system | **Skip** | Architecturally incompatible | -- | None |
| Source connectors | **Skip** | We use Delta Lake only | -- | None |
| MetastoreService trait | **Skip** | We use searchdb.json directly | -- | None |
| Postgres backend | **Skip** | Violates single-binary principle | -- | None |
| StorageDirectory | **Skip** | We use local MmapDirectory | -- | None |

### Total Vendoring Estimate (v1)

~350 lines of ported code:
- MergePolicy trait: ~50 lines
- StableLogMergePolicy: ~300 lines

### Total Inspiration Estimate (v1)

~230 lines of new code informed by quickwit designs:
- Segment metadata struct with merge-aware fields: ~100 lines
- Memory-based commit trigger in compact worker: ~20 lines
- Segment state tracking in searchdb.json: ~110 lines

### Next Steps

1. Port the StableLogMergePolicy algorithm into `src/merge_policy.rs` as part of the tiered compaction implementation.
2. Add `num_merge_ops` and `create_timestamp` to our per-segment metadata in `searchdb.json`.
3. Integrate memory-based commit triggering alongside the existing row-count threshold in the compact worker.
4. Defer remote storage directory work until post-v1.
