# Compaction

The compact worker keeps your tantivy index in sync with Delta Lake. It polls for new rows, writes them into segments, and merges segments to keep search fast. This page explains how compaction works and how to tune it.

## Overview

Compaction has two levels:

- **Level 1 (Segmentation):** Read new rows from Delta Lake and write them into tantivy segments. Fast, append-only, never touches existing segments.
- **Level 2 (Merge):** Combine multiple small segments into fewer large ones. Keeps search performance stable as data grows.

Both levels are atomic. If the worker crashes, the index remains in its last valid state.

## Running the compact worker

### Continuous mode (default)

```bash
searchdb compact my_index
```

The worker runs in a loop:
1. Poll Delta Lake for new rows
2. If new rows exist, create segments (L1)
3. Check if merge is needed (L2)
4. Sleep for `--poll-interval` seconds
5. Repeat

Stop with Ctrl+C (SIGINT) or SIGTERM. The worker finishes its current operation and shuts down cleanly.

### One-shot mode

```bash
searchdb compact my_index --once
```

Poll once, create segments if needed, merge if needed, then exit. Useful for cron jobs or CI pipelines.

### Force merge

```bash
searchdb compact my_index --force-merge
```

Merge all segments into a single segment, then exit. Useful after a large bulk load or before taking a snapshot of the index.

## Configuration

All parameters are passed as CLI flags. There is no configuration file.

### Level 1 parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--segment-size` | `10000` | Maximum rows per segment before commit |
| `--poll-interval` | `10` | Seconds between Delta polls |
| `--max-segment-age` | `60` | Force commit after N seconds even if under segment-size |

`--segment-size` controls the tradeoff between segment count and commit latency. Smaller values mean more frequent commits (lower latency for new data to become searchable) but more segments (which increases merge pressure). Larger values create fewer, bigger segments but delay visibility of new data.

### Level 2 parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--merge-interval` | `5` | Minutes between merge checks |
| `--max-segments` | `10` | Merge when segment count exceeds this |

`--max-segments` is the merge trigger threshold. When the total segment count exceeds this number, the merge policy selects groups of segments to combine. The merge policy uses a stable log merge algorithm (similar to Lucene's TieredMergePolicy) that groups segments by size tier and merges within tiers.

## Example configurations

### Low-latency (near-real-time search)

```bash
searchdb compact my_index \
  --segment-size 1000 \
  --poll-interval 2 \
  --merge-interval 1 \
  --max-segments 5
```

New data becomes searchable within a few seconds. Higher merge frequency keeps segment count bounded despite the smaller segment size.

### High-throughput (batch processing)

```bash
searchdb compact my_index \
  --segment-size 100000 \
  --poll-interval 60 \
  --merge-interval 30 \
  --max-segments 20
```

Larger segments reduce merge overhead. Longer poll interval reduces Delta table reads. Good for data that arrives in large batches.

### One-shot pipeline

```bash
# Load data into Delta (your ETL tool here)
# ...

# Sync and compact in one step
searchdb compact my_index --once

# Optionally optimize for read performance
searchdb compact my_index --force-merge
```

## How segments work

When the compact worker creates a segment, it:

1. Reads new rows from Delta (everything since the last watermark version)
2. For each row: deletes any existing document with the same `_id` (upsert), then adds the new document
3. Commits the segment to tantivy
4. Updates the version watermark in `searchdb.json`

Each commit produces a new segment file on disk. tantivy searches across all segments and merges results, so data is immediately searchable after commit.

### Why upsert matters

Every write includes a delete-by-`_id` before the insert. This means:

- Updated rows in Delta automatically replace their previous version
- No duplicate documents even if the same row appears in multiple Delta versions
- After a force merge, deleted documents are physically removed and disk space is reclaimed

## Monitoring

Check the current state with `searchdb stats`:

```bash
searchdb stats my_index
```

This shows:
- Total document count
- Number of segments
- Delta version watermark (index_version vs. Delta HEAD)
- Last segmentation and merge timestamps (if compaction metadata exists)

If the gap between index_version and Delta HEAD is growing, your compact worker is falling behind. Consider increasing `--segment-size` or running more frequent polls.

## Prerequisites

The compact worker requires:

1. An index connected to a Delta source (`searchdb connect-delta` must have been run first)
2. Credentials for the blob storage backend (S3, GCS, Azure) if your Delta table is on remote storage
3. Exclusive write access -- only one compact worker can run per index at a time (enforced by tantivy's write lock)

If a second worker tries to start on the same index, it will exit with a "writer locked" error.
