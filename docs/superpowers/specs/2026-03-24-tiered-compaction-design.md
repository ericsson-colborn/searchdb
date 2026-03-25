# Tiered Compaction Worker for SearchDB

**Date:** 2026-03-24
**Status:** Draft
**Supersedes:** Delta-as-buffer architecture (gap-at-query-time model)

## Problem

SearchDB's current `sync` command does an all-or-nothing index update: read all new Delta rows, upsert them into one tantivy index, commit once. For large Delta tables with frequent writes, this causes two problems:

1. **Long sync times.** A single commit that upserts 100K rows rebuilds substantial portions of the index. During this time, the IndexWriter lock is held and no other sync can proceed.
2. **Stale reads.** The gap-at-query-time model (reading un-indexed Parquet files during search) adds latency proportional to gap size. When the gap grows because sync is slow, query latency degrades.

The fix is to decouple ingestion from consolidation. New rows become small, fast segments. Segments get merged in the background on a schedule. This is the standard LSM-tree / tiered compaction pattern used by Cassandra, RocksDB, and Delta Lake's own OPTIMIZE command.

## Design Principles

1. **Single writer.** The compact worker is the only process that writes to the tantivy index. Clients (search, get) are read-only.
2. **Two tiers.** Level 1 creates segments (fast, incremental). Level 2 merges segments (slower, runs on a schedule).
3. **Crash-safe at every step.** A crash at any point leaves the index in the state of the last successful commit. No corruption, no partial state.
4. **Observable.** The worker logs what it is doing: polling, segmenting, merging, idle. Operators can watch stderr or redirect to a log file.
5. **Configurable.** Segment size and merge interval are tunable for different workloads. Defaults work for the common case.

## Architecture

### The Compact Worker

`dsrch compact <name>` runs a long-lived event loop:

```
loop {
    1. Poll Delta for new version
    2. If new rows >= segment_size threshold:
       - Read rows from Delta
       - Build tantivy documents
       - Upsert into IndexWriter (delete_term + add_document)
       - Commit (creates a new segment)
       - Update index_version watermark in searchdb.json
    3. If merge timer has elapsed:
       - Check segment count
       - If segments > merge_threshold, trigger merge
       - Wait for merge to complete
    4. Sleep for poll_interval
}
```

### Component Diagram

```
                                     On Disk
                                  +-----------+
  Delta Lake  --(poll)-->  [ compact worker ]  -->  tantivy index/
  (blob storage)           |                 |      meta.json
                           | L1: segment     |      segment files
                           | L2: merge       |      searchdb.json
                           +-----------------+
                                  ^
                                  | reads index directly (no lock)
                           [ search / get clients ]
```

## Level 1: Segment Creation

### How Tantivy Segments Work

Key facts about tantivy's segment model (from tantivy 0.25):

- A tantivy index is a collection of smaller, immutable, independent sub-indexes called **segments**.
- Each call to `IndexWriter::commit()` flushes buffered documents into one or more new segment files and atomically updates `meta.json` to include them.
- Existing segments are never modified. New data always creates new segments.
- `meta.json` is the single source of truth for which segments constitute the index. Its update is atomic (write-then-rename). If a crash happens before the rename, the old `meta.json` is still valid.

This means we get segment boundaries for free: every `commit()` creates a segment. We control segment size by controlling how many documents we buffer before calling `commit()`.

### Segmentation Strategy

The worker accumulates rows from Delta until one of two conditions is met:

1. **Row count threshold (`--segment-size`, default 10,000).** When the accumulated row count reaches this threshold, commit immediately. This bounds the memory footprint of the in-flight batch.
2. **Time pressure.** If the poll discovers new Delta versions but the row count is below the threshold, the rows are held in memory until the next poll. If `max_segment_age` seconds elapse since the first row was buffered (default: 60s), commit what we have regardless of count. This ensures freshness under low-throughput workloads.

The result is segments of roughly uniform size (approximately `segment_size` documents each), with occasional smaller segments when data arrives slowly.

### Delta Version Tracking

The worker tracks two version numbers:

- **`index_version`** (in `searchdb.json`): The Delta version up to which the tantivy index has been built. Updated atomically after each successful segment commit.
- **`poll_version`** (in memory only): The last Delta HEAD version the worker observed. Used to detect new data.

On each poll cycle:

1. Call `DeltaSync::current_version()` to get HEAD.
2. If HEAD > `index_version`, call `DeltaSync::rows_added_since(index_version)` to get new rows.
3. Buffer the rows. When the threshold is met, commit and advance `index_version` to the Delta version that produced those rows.

### Batch Splitting for Large Deltas

If a single Delta version gap contains more rows than `segment_size`, the worker splits them into batches:

```
rows = delta.rows_added_since(index_version)
for batch in rows.chunks(segment_size):
    upsert batch into IndexWriter
    commit()  // creates one segment per batch
// update index_version to HEAD after all batches committed
```

This prevents a single massive segment from forming when catching up after downtime.

### Disabling Automatic Merges During Segmentation

By default, tantivy's `IndexWriter` runs a `LogMergePolicy` that automatically triggers background merges after each commit. Since we want explicit control over when merges happen (Level 2), the worker sets `NoMergePolicy` during normal operation:

```rust
use tantivy::merge_policy::NoMergePolicy;
index_writer.set_merge_policy(Box::new(NoMergePolicy));
```

This ensures that `commit()` only creates segments and never triggers a background merge. Merges are exclusively controlled by the Level 2 timer.

## Level 2: Segment Merge

### Why Merge

Without merging, the segment count grows linearly with the number of commits. Each search query must visit every segment. At 10 segments, this is fine. At 1,000 segments, search latency degrades and file descriptor usage grows. Merging combines small segments into larger ones, bounding the total count.

### Merge Strategy

The worker uses **manual merge triggering** on a timer, not tantivy's automatic merge policy. This gives us explicit control:

1. **Merge trigger:** Every `--merge-interval` minutes (default: 5), the worker checks the segment count.
2. **Merge decision:** If the number of searchable segments exceeds `--max-segments` (default: 10), select segments to merge.
3. **Segment selection:** Use a log-structured approach inspired by tantivy's `LogMergePolicy`:
   - Group segments into tiers by document count (exponential buckets).
   - Within each tier, if the number of segments exceeds a merge factor (default: 8), merge them into one.
   - Never merge segments above `--max-segment-docs` (default: 5,000,000) -- these are "mature" and left alone.
4. **Merge execution:** Call `IndexWriter::merge(&segment_ids)` which returns a future. The worker awaits completion. During the merge, the old segments remain searchable -- tantivy's `IndexReader` holds a snapshot. After the merge commits, the old segments become candidates for garbage collection.
5. **Cleanup:** Call `IndexWriter::garbage_collect_files()` to remove the now-unused segment files from disk.

### Merge Parameters

| Parameter | Flag | Default | Description |
|-----------|------|---------|-------------|
| Merge interval | `--merge-interval` | 5 min | How often to check for merge opportunities |
| Max segments | `--max-segments` | 10 | Trigger merge when segment count exceeds this |
| Merge factor | (internal) | 8 | Max segments to merge in one operation |
| Max segment docs | (internal) | 5,000,000 | Segments above this size are never merged |

### Force Merge

For operators who want to consolidate the index into a single segment (e.g., before a read-heavy workload), the compact command supports a one-shot mode:

```
dsrch compact <name> --force-merge
```

This merges all segments into one, then exits. It does not enter the polling loop. Useful as a maintenance operation.

## CLI Interface

```
dsrch compact <name> [OPTIONS]

Options:
  --segment-size <N>       Rows per segment before commit (default: 10000)
  --merge-interval <MIN>   Minutes between merge checks (default: 5)
  --max-segments <N>       Merge when segment count exceeds this (default: 10)
  --poll-interval <SEC>    Seconds between Delta polls (default: 10)
  --max-segment-age <SEC>  Force commit after N seconds even if under threshold (default: 60)
  --force-merge            One-shot: merge all segments and exit
  --once                   One-shot: poll once, segment if needed, merge if needed, exit
  --data-dir <DIR>         Data directory for indexes (default: .dsrch)
```

### Examples

```bash
# Run as a long-lived worker (production)
dsrch compact labs --segment-size 50000 --merge-interval 10

# One-shot segment + merge (cron job)
dsrch compact labs --once

# Force-merge to single segment (maintenance)
dsrch compact labs --force-merge

# High-throughput: larger segments, less frequent merges
dsrch compact labs --segment-size 100000 --merge-interval 30 --max-segments 20
```

## How This Replaces Existing Commands

### `dsrch sync` -- Deprecated

The current `sync` command is replaced by `dsrch compact --once`. The behavior is nearly identical (poll Delta, upsert new rows, commit), but compact adds:

- Batch splitting for large gaps (sync would create one massive segment)
- NoMergePolicy to prevent automatic merges from interfering
- Optional merge pass after segmentation

The `sync` command will remain as an alias for `compact --once` during a transition period, then be removed.

### `dsrch search` / `dsrch get` -- Simplified

With the compact worker handling all writes, clients no longer need the two-tier gap search. The query path simplifies to:

**Before (gap-at-query-time):**
1. Check Delta HEAD
2. Read gap Parquet files
3. Build ephemeral in-memory index
4. Search both indexes, dedup
5. Maybe spawn background sync

**After (compact worker):**
1. Open tantivy index
2. Search it
3. Return results

Clients need no Delta access and no credentials. The index is always reasonably fresh because the compact worker runs continuously. The `read_gap`, `search_with_gap`, `build_ephemeral_index`, and `maybe_spawn_sync` code paths can be removed from the client.

### `dsrch reindex` -- Unchanged

Reindex remains as the escape hatch for full rebuilds. It destroys the tantivy index directory and rebuilds from Delta. The compact worker should be stopped before running reindex (it will fail to acquire the IndexWriter lock if the worker is running).

### `dsrch connect-delta` -- Unchanged

Sets up the Delta source and performs the initial full load. After connect-delta completes, the operator starts the compact worker to keep the index current.

### `dsrch stats` -- Enhanced

Stats will report additional compaction metadata:

```json
{
  "index": "labs",
  "num_docs": 150000,
  "num_segments": 7,
  "index_version": 342,
  "delta_version": 345,
  "gap_versions": 3,
  "compaction": {
    "last_segment_at": "2026-03-24T14:30:00Z",
    "last_merge_at": "2026-03-24T14:25:00Z",
    "segments_since_last_merge": 3
  }
}
```

## Crash Safety Analysis

Every state transition in the compact worker is designed to be crash-safe. The analysis below considers what happens if the process is killed (SIGKILL, OOM, power loss) at each step.

### During Segment Creation (Level 1)

| Crash point | State after restart |
|-------------|-------------------|
| After reading Delta rows, before commit | Rows are lost from memory. `index_version` unchanged. Worker re-reads the same rows on next poll. No data loss. |
| During `IndexWriter::commit()` | tantivy's commit is atomic via `meta.json` rename. Either the old `meta.json` is in place (commit failed, no segment visible) or the new one is (commit succeeded, segment visible). No corruption. |
| After commit, before updating `searchdb.json` | The segment exists in tantivy but `index_version` was not advanced. On restart, the worker re-reads the same Delta rows and upserts them again. Upsert semantics (delete_term + add_document) ensure no duplicates -- the old documents are tombstoned and the new identical ones replace them. Wasted work, not data corruption. |
| After updating `searchdb.json` | Clean state. No recovery needed. |

### During Segment Merge (Level 2)

| Crash point | State after restart |
|-------------|-------------------|
| During `IndexWriter::merge()` | tantivy writes the merged segment to new files, then atomically updates `meta.json` to swap the old segment list for the new one. If the crash happens before the `meta.json` update, the old segments remain and the partially-written merged segment is orphaned. It will be cleaned up by `garbage_collect_files()` on the next run. |
| After merge commit, before garbage collection | The merged segment is live. The old segments are no longer referenced in `meta.json` but their files still exist on disk. `garbage_collect_files()` on the next run will remove them. Wasted disk space, not corruption. |

### The searchdb.json / meta.json Ordering Problem

The critical ordering question: we have two files that must stay in sync -- `searchdb.json` (our version watermark) and `meta.json` (tantivy's segment list). They cannot be updated atomically together.

**Our ordering:** Commit tantivy first, then update `searchdb.json`.

**Why this is safe:** If we crash between the two updates, the tantivy index has the new segment but `searchdb.json` still shows the old `index_version`. On restart, we re-read the Delta rows and upsert them again. Because upsert deletes by `_id` before adding, this produces the same result -- no duplicates, no data loss. The cost is re-reading and re-indexing rows we already have, which is wasted work but not incorrect.

**The unsafe alternative would be:** Update `searchdb.json` first, then commit tantivy. If we crash between these, `index_version` claims we have data that the index does not contain. Those rows would be silently lost. This ordering must never be used.

### Summary

The invariant is: **`index_version` in `searchdb.json` is always <= the actual data in the tantivy index.** The index may contain data slightly ahead of the watermark (if we crashed after commit but before updating the watermark), but it never contains less. This means re-reads are possible but data loss is not.

## Concurrent Reader Safety

### How Tantivy Handles Concurrent Reads and Writes

Tantivy's concurrency model is designed for exactly this pattern:

- **`IndexReader`** holds an `Arc`-wrapped snapshot of the segment list. Once created, it provides a `Searcher` that sees a frozen view of the index.
- **`IndexReader::reload()`** checks `meta.json` for new segments and creates a new snapshot. The old snapshot remains valid until all references to it are dropped.
- **`IndexWriter::commit()`** writes new segment files and atomically updates `meta.json`. It does not interfere with existing `IndexReader` snapshots.
- **`IndexWriter::merge()`** writes a new merged segment and atomically updates `meta.json`. Old segments remain on disk until garbage collected. Any `Searcher` still using the old segments continues to work.

### Client Behavior During Compaction

Clients (`dsrch search`, `dsrch get`) open the tantivy index with `Index::open_in_dir()` and create a reader. The reader picks up whatever segments exist in `meta.json` at that moment. If the compact worker commits a new segment between two client invocations, the second client sees the new data. If a commit happens during a client's search, the client sees the old snapshot -- consistent, just slightly stale.

There is no lock contention between the compact worker and clients. The IndexWriter holds a write lock (via a lockfile in the index directory) that prevents other writers, but readers never contend with it.

### Multiple Compact Workers

Only one compact worker can run per index. The tantivy `IndexWriter` acquires a file-based lock on creation. If a second worker attempts to start, `Index::writer()` returns `TantivyError::LockFailure`. The worker should detect this and exit with a clear error message:

```
[dsrch] Error: another compact worker is already running for index 'labs'
```

## On-Disk Layout Changes

### searchdb.json Additions

```json
{
  "schema": {"fields": {"name": "keyword", "notes": "text"}},
  "delta_source": "s3://bucket/labs",
  "index_version": 342,
  "compact": {
    "segment_size": 10000,
    "merge_interval_secs": 300,
    "max_segments": 10,
    "last_segment_at": "2026-03-24T14:30:00Z",
    "last_merge_at": "2026-03-24T14:25:00Z"
  }
}
```

The `compact` section is optional. It is written by the compact worker to record its configuration and last activity timestamps. Clients ignore it. If the worker is restarted with different parameters, the new values overwrite the old ones.

## Operational Considerations

### Running the Compact Worker

The compact worker is a long-lived process, not a daemon. Run it under a process supervisor:

```bash
# systemd, supervisord, or just a terminal
dsrch compact labs --segment-size 50000 --merge-interval 10
```

For simpler deployments (cron), use `--once`:

```bash
# Every minute via cron
* * * * * dsrch compact labs --once --data-dir /data/searchdb
```

### Monitoring

The worker logs to stderr with structured messages:

```
[dsrch] compact: polling Delta... HEAD=345, index=342, gap=3 versions
[dsrch] compact: read 1,247 rows from Delta v342..v345
[dsrch] compact: committed segment (1,247 docs), now at Delta v345
[dsrch] compact: merge check: 12 segments, threshold=10, merging...
[dsrch] compact: merged 8 segments into 1 (47,832 docs), 5 segments remaining
[dsrch] compact: idle, next poll in 10s
```

### Graceful Shutdown

On SIGINT or SIGTERM:

1. Finish any in-progress commit (do not abandon a half-written segment).
2. If a merge is running, wait for it to complete (tantivy merges are not cancellable without leaving orphaned files).
3. Call `IndexWriter::wait_merging_threads()` to ensure clean shutdown.
4. Exit.

### Storage Credentials

The compact worker needs credentials to access blob storage (S3, GCS, ADLS) for Delta Lake reads. These are provided via environment variables, following the `deltalake` crate's conventions:

- **S3:** `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
- **GCS:** `GOOGLE_APPLICATION_CREDENTIALS`
- **ADLS:** `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`

Clients (search, get) never access Delta and need no credentials.

## Migration Path

### Phase 1: Add `compact` command

- Implement the compact worker as described in this spec.
- Keep existing `sync` command unchanged.
- Keep gap-at-query-time in search/get (fallback for users not running the worker).

### Phase 2: Default to compact

- `dsrch connect-delta` prints a message: "Run `dsrch compact <name>` to keep the index current."
- `dsrch sync` becomes an alias for `compact --once` and prints a deprecation warning.
- search/get still support gap-at-query-time but log a hint if gap > 0: "Consider running a compact worker."

### Phase 3: Remove gap-at-query-time

- Remove `search_with_gap`, `build_ephemeral_index`, `get_with_gap`, `read_gap`, `maybe_spawn_sync`.
- Remove `sync` command.
- Clients are pure readers. No Delta access, no credentials, no gap logic.

## Acceptance Criteria

### Core Functionality

- [ ] `dsrch compact <name>` starts a long-lived worker that polls Delta, creates segments, and merges them.
- [ ] Each commit creates exactly one new tantivy segment (NoMergePolicy is active).
- [ ] Segments are created when accumulated rows reach `--segment-size` or `--max-segment-age` seconds elapse.
- [ ] Large Delta gaps are split into multiple segments of at most `segment_size` rows.
- [ ] Segment merges are triggered every `--merge-interval` minutes when the segment count exceeds `--max-segments`.
- [ ] `--force-merge` merges all segments into one and exits.
- [ ] `--once` polls once, segments if needed, merges if needed, and exits.

### Crash Safety

- [ ] Killing the worker mid-segment-creation leaves the index unchanged (tantivy commit atomicity).
- [ ] Killing the worker mid-merge leaves the index with the old segments intact (tantivy merge atomicity).
- [ ] `index_version` is never advanced past what the tantivy index actually contains.
- [ ] Restarting after a crash re-reads any rows that were committed to tantivy but not yet watermarked, producing correct results via upsert dedup.

### Concurrency

- [ ] Search and get clients work without contention while the worker is running.
- [ ] Clients see newly committed segments on their next invocation (tantivy `meta.json` reload).
- [ ] Only one compact worker can run per index (IndexWriter lock).
- [ ] A second worker attempting to start gets a clear error, not a hang.

### Observability

- [ ] Worker logs poll results, segment commits, merge operations, and idle periods to stderr.
- [ ] `dsrch stats` reports segment count, index_version, delta_version, and gap size.

### Backward Compatibility

- [ ] Existing indexes created by `connect-delta` + `sync` work with the compact worker without migration.
- [ ] `dsrch sync` continues to work during the transition period.
