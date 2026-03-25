# SearchDB (deltasearch)

**Embedded search for the lakehouse. ES in your pocket.**

SearchDB is a single-binary search engine that combines tantivy (Rust full-text search) with Delta Lake (versioned data lake). Point it at JSON files or blob storage and you're searching in minutes -- no cluster, no daemon, no JVM. The index is a disposable cache that builds itself from your data.

## Quick Start

```bash
# 1. Create an index
searchdb new products --schema '{"fields":{"name":"text","category":"keyword","price":"numeric"}}'

# 2. Index some data
echo '{"_id":"1","name":"Wireless Headphones","category":"electronics","price":79.99}
{"_id":"2","name":"Running Shoes","category":"sports","price":129.00}
{"_id":"3","name":"Coffee Maker","category":"kitchen","price":49.99}' | searchdb index products

# 3. Search
searchdb search products -q "headphones"
searchdb search products -q '+category:"electronics" +price:[50 TO 100]'

# 4. Get a single document
searchdb get products 1

# 5. Check index stats
searchdb stats products
```

## Features

- **Full-text search** -- BM25 scoring, stemming, phrase matching via tantivy
- **Four field types** -- `keyword` (exact match), `text` (full-text), `numeric` (ranges), `date` (time ranges)
- **Delta Lake sync** -- Attach a Delta table and the index stays current via incremental sync
- **Tiered compaction** -- Background worker creates segments (L1) and merges them (L2), never rebuilds the full index
- **Elasticsearch query DSL** -- Drop-in `bool`/`term`/`match`/`range`/`exists` queries via `--dsl`
- **Schema inference** -- Skip schema declaration; types are inferred from your data automatically
- **Upsert semantics** -- Documents with the same `_id` are deduplicated on write
- **Single binary, zero dependencies** -- One `cargo build --release` and you're done

## Architecture

```
                       ┌──────────────┐
                       │  NDJSON /    │
                       │  JSON files  │
                       └──────┬───────┘
                              │
                              v
┌──────────────────────────────────────────────────────┐
│                    Delta Lake                        │
│         (S3 / GCS / Azure / local filesystem)        │
│           versioned, append-only, Parquet             │
└──────────────────────┬───────────────────────────────┘
                       │
            ┌──────────┴──────────┐
            │   compact worker    │
            │  L1: new rows →     │
            │      new segment    │
            │  L2: segments →     │
            │      merged index   │
            └──────────┬──────────┘
                       │
                       v
┌──────────────────────────────────────────────────────┐
│               tantivy index (local)                  │
│          segments, _source, fast fields              │
└──────────────────────┬───────────────────────────────┘
                       │
            ┌──────────┴──────────┐
            │   search client     │
            │  searchdb search    │
            │  searchdb get       │
            │  (read-only, no     │
            │   credentials)      │
            └─────────────────────┘
```

**Clients read.** They query the tantivy index directly. No Delta access needed.

**Workers write.** The compact worker reads Delta, creates tantivy segments, and merges them. Workers need blob storage credentials.

## Installation

### From source

```bash
cargo install --git https://github.com/ericsson-colborn/searchdb
```

### GitHub Releases

Download prebuilt binaries from the [Releases](https://github.com/ericsson-colborn/searchdb/releases) page.

### Build from source

```bash
git clone https://github.com/ericsson-colborn/searchdb.git
cd searchdb
cargo build --release
# Binary at target/release/searchdb
```

## Commands

| Command | Description |
|---------|-------------|
| `searchdb new <name>` | Create a new index (schema optional) |
| `searchdb index <name>` | Bulk index NDJSON from stdin or file |
| `searchdb search <name> -q <query>` | Search with Lucene-like syntax |
| `searchdb search <name> --dsl <json>` | Search with Elasticsearch query DSL |
| `searchdb get <name> <doc_id>` | Retrieve a single document by `_id` |
| `searchdb stats <name>` | Show index statistics |
| `searchdb drop <name>` | Delete an index |
| `searchdb connect-delta <name>` | Attach a Delta Lake source |
| `searchdb compact <name>` | Run the compaction worker |
| `searchdb sync <name>` | Manual incremental sync from Delta |
| `searchdb reindex <name>` | Full rebuild from Delta source |

## Documentation

See the [`docs/`](docs/) directory for detailed guides:

- [Quickstart](docs/getting-started/quickstart.md) -- 5-minute end-to-end tutorial
- [Concepts](docs/getting-started/concepts.md) -- Delta Lake, tantivy, segments, compaction
- [Schema](docs/guides/schema.md) -- Field types, inference, `--infer-from`
- [Searching](docs/guides/searching.md) -- Query syntax and Elasticsearch DSL
- [Compaction](docs/guides/compaction.md) -- L1/L2, configurable parameters, `--force-merge`

## License

Apache 2.0. See [LICENSE](LICENSE) for details.
