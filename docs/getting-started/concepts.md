# Concepts

SearchDB sits at the intersection of three ideas: full-text search (tantivy), versioned data lakes (Delta Lake), and embedded databases (DuckDB's philosophy). This page explains how they fit together.

## tantivy: the search engine

[tantivy](https://github.com/quickwit-oss/tantivy) is a Rust full-text search library, similar to Apache Lucene. It provides:

- **Inverted indexes** for fast text lookup
- **BM25 scoring** for relevance ranking
- **Segments** as the unit of storage (more on this below)
- **Fast fields** for numeric and date range queries (like Elasticsearch's doc values)

SearchDB uses tantivy as a library, not as a server. There is no HTTP daemon -- your CLI commands read and write the index files directly on disk.

### How tantivy stores data

tantivy organizes documents into **segments**. Each segment is a self-contained mini-index with its own inverted index, stored fields, and fast fields. When you search, tantivy queries all segments and merges the results.

```
index/
├── meta.json              # Tracks which segments are live
├── <segment-uuid>.term    # Inverted index (terms → doc IDs)
├── <segment-uuid>.store   # Stored field values (_source, etc.)
├── <segment-uuid>.fast    # Columnar data for numeric/date fields
└── ...
```

New documents are written to a new segment. Over time, segments accumulate. Too many small segments slow down search because tantivy must scan each one. That is where compaction comes in.

## Delta Lake: the source of truth

[Delta Lake](https://delta.io/) is an open table format that adds ACID transactions, versioning, and schema enforcement on top of Parquet files in blob storage (S3, GCS, Azure, or local filesystem).

In SearchDB's architecture, Delta Lake is the source of truth. The tantivy index is a **disposable cache** -- it can be rebuilt from Delta at any time. This means:

- **No data loss** if the index corrupts -- rebuild with `searchdb reindex`
- **Version tracking** -- SearchDB records which Delta version the index has seen (`index_version` watermark)
- **Incremental sync** -- only new rows since the last watermark are read, not the entire table

### Connecting to Delta

```bash
# Attach a Delta table to an index
searchdb connect-delta my_index --source s3://bucket/my_table/

# Or a local Delta table
searchdb connect-delta my_index --source /path/to/delta_table/
```

When you connect, SearchDB reads the Delta table's current snapshot, indexes all rows, and records the version number. Subsequent syncs only read rows added after that version.

## Compaction: keeping the index fast

SearchDB uses a two-level compaction model inspired by LSM trees:

### Level 1: Segmentation

When new rows arrive from Delta, the compact worker writes them into a new tantivy segment and commits. This is fast -- it never touches existing segments.

**Default:** one segment per 10,000 rows (`--segment-size`).

### Level 2: Merge

Periodically, the worker checks if there are too many segments. If so, it merges small segments into larger ones. This keeps search performance stable as data grows.

**Default:** merge check every 5 minutes (`--merge-interval`), trigger when segment count exceeds 10 (`--max-segments`).

### The compact worker

The compact worker is a long-running process that polls Delta for changes and manages both levels:

```bash
# Run in the background (default: continuous mode)
searchdb compact my_index

# One-shot: poll once, segment, merge, exit
searchdb compact my_index --once

# Force all segments into one
searchdb compact my_index --force-merge
```

All compaction is **atomic**. If the worker crashes mid-operation, the index remains in its last valid state. The version watermark is only updated after segments are committed.

### Visualization

```
Delta Lake HEAD: v42
                  │
  ┌───────────────┤
  │  rows v38-v42 │ ← gap: rows not yet indexed
  └───────┬───────┘
          │
    Level 1: Segmentation
          │
          v
  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
  │  segment A   │ │  segment B   │ │  segment C   │
  │  (3K docs)   │ │  (10K docs)  │ │  (8K docs)   │
  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
         │                │                │
         └────────┬───────┘                │
                  │                        │
            Level 2: Merge                 │
                  │                        │
                  v                        │
          ┌──────────────┐                 │
          │  segment AB   │                │
          │  (13K docs)   │                │
          └───────────────┘                │
                                           │
    (segment C stays — not enough segments to trigger another merge)
```

## Client/worker separation

SearchDB separates read and write access:

**Clients** (read-only):
- `searchdb search` -- query the tantivy index
- `searchdb get` -- fetch a document by ID
- `searchdb stats` -- view index metadata

Clients only read tantivy files on local disk. They do not access Delta Lake and do not need blob storage credentials.

**Workers** (write):
- `searchdb compact` -- poll Delta, create segments, merge
- `searchdb sync` -- one-time incremental sync
- `searchdb reindex` -- full rebuild

Workers read from Delta Lake and write to the tantivy index. They need credentials for whatever storage backend your Delta table uses.

This separation means you can deploy the search client to machines that should not have write access to your data lake.

## The `_source` field

Every document indexed by SearchDB stores its original JSON in a field called `_source` (borrowed from Elasticsearch's convention). When you search, results are returned from `_source`, which means:

- Round-trip fidelity: what you put in is exactly what you get back
- No data loss from type coercion or field mapping
- You can project specific fields with `--fields name,price` to reduce output size

## The `_id` field

Every document has an `_id` field used for deduplication. If you provide `_id` in your JSON, SearchDB uses it. If you do not, SearchDB generates a UUID.

When a document with a duplicate `_id` is indexed, the old version is deleted and the new one takes its place (upsert semantics). This is how updates work -- there is no separate update API.

## Next steps

- [Schema guide](../guides/schema.md) -- how field types map to tantivy configuration
- [Searching guide](../guides/searching.md) -- query syntax and Elasticsearch DSL
- [Compaction guide](../guides/compaction.md) -- tuning the compact worker
