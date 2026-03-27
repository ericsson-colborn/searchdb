# deltasearch Product Requirements

This is the canonical source of truth for what deltasearch needs to do and how we measure success. Everything else — CLAUDE.md, README, GitHub issues — derives from this document.

## Product Vision

DuckDB proved you don't need a data warehouse server. deltasearch proves you don't need a search cluster.

For teams with data in a lakehouse, deltasearch lets you search your lake directly. Delta Lake is the source of truth. The index is incrementally hydrated from it — a persistent asset that stays fresh via streaming compaction. No cluster, no daemon, no JVM. Single binary, zero config.

## UX Principle

**Zero to hero in 60 seconds.** A new user can create an index, connect a Delta source, start the compaction worker, and run their first search in under a minute. Adoption and ease of use are the priority at this stage. Every command should have sensible defaults. No config files, no YAML, no setup wizards. If a user has to read docs before their first search, we've failed.

```bash
dsrch compact labs --source s3://bucket/labs &   # creates index, infers schema, loads, polls
dsrch search labs -q "glucose"                    # searching
```

## Core Invariants

These are non-negotiable design properties. Any feature that violates these is wrong.

1. **Delta Lake is the source of truth.** The index is derived from Delta. If they diverge, Delta wins.
2. **The index is a persistent asset.** It's expensive to rebuild at scale. Incremental hydration is the normal path. Full rebuild (`reindex`) is an escape hatch, not a workflow.
3. **Always-fresh search.** Clients see Delta HEAD, not just indexed data. The two-tier gap search ensures correctness even when compaction is behind.
4. **Serverless.** No daemon, no HTTP server. CLI commands against shared storage. DuckDB model.
5. **Client/worker separation.** Clients read, workers write.
6. **Crash-safe.** All compaction is atomic. If the worker crashes, the index is unchanged.
7. **Single binary.** `cargo build --release` produces one binary with zero runtime dependencies.
8. **Cloud-native.** The Delta Lake and index can both be stored in the customer's cloud without sacrificing performance. Similar to Quickwit.
9. **Built for scale.** Built to search TBs of data with comparable performance to Elasticsearch, AWS OpenSearch, and Quickwit.

## Target Users

Mid-size organizations (initial wedge: healthcare) with:
- Data already in a lakehouse (Azure/OneLake/S3 + Delta Lake)
- Elasticsearch fatigue (cost, ops, complexity)
- Compliance sensitivity (HIPAA — we never see their data)
- Engineers who want to search their lake without standing up infrastructure

See `docs/business-model.md` for the full go-to-market strategy.

## Success Criteria

### Milestone 1: "Replace ES for Read Path" (current target)

A team currently using Elasticsearch for search-only (no aggregation dashboards, no ingest pipelines) can switch to deltasearch with:

- [ ] **Query compatibility.** All ES DSL query types used by 80% of read-path workloads: bool, term, terms, match, match_phrase, range, exists, multi_match, wildcard, prefix. Tantivy query string as a power-user alternative.
- [ ] **Schema compatibility.** Nested object flattening (dot notation). At least 6 field types: keyword, text, numeric (f64), date, boolean, ip.
- [ ] **Result quality.** Sorting by field value (not just relevance). Offset/limit pagination sufficient (no scroll needed for M1).
- [ ] **Scale.** 10M documents indexed in under 10 minutes. 100M documents indexed in under 2 hours. Bounded memory (streaming compaction, no OOM on large backfills).
- [ ] **Freshness.** New Delta rows searchable within `poll_interval` seconds (default 10s) via compaction, or immediately via two-tier gap search.
- [ ] **Operational simplicity.** Zero config for search. One command to connect + compact (`dsrch compact --source`). Stats command shows health.

### Milestone 2: "Production-Ready"

The enterprise tier from `docs/business-model.md` is deliverable:

- [ ] **Object storage.** Tantivy Directory trait over S3/GCS/Azure. Index lives on blob storage, not local disk.
- [ ] **Aggregations.** At least: terms, range, histogram, date_histogram, avg, sum, min, max, count. Enough for a basic dashboard.
- [ ] **Monitoring.** Prometheus metrics on compact worker. Grafana dashboard template.
- [ ] **Distribution.** GitHub Releases with prebuilt binaries. `cargo install deltasearch`. Homebrew formula.
- [ ] **Freshness SLA tooling.** Compact worker reports gap age. Alerting when gap exceeds threshold.
- [ ] **Drop _source.** Hydrate full documents from Delta on demand. Reduces index size by ~50%.

### Milestone 3: "Ecosystem"

- [ ] **Python bindings (PyO3).** `import deltasearch` for notebook workflows.
- [ ] **Search UI.** Preact frontend bundled in `dsrch serve`.
- [ ] **ES API endpoint.** HTTP `_search` and `_bulk` endpoints for ES client library compatibility.	

## Elasticsearch Compatibility Matrix

### Targeted (M1)

These are the ES features we explicitly aim to support:

| ES Feature | deltasearch Implementation | Status |
|-----------|---------------------------|--------|
| `bool` query | `es_dsl::BoolQuery` | done |
| `term` query | `es_dsl::TermQuery` | done |
| `terms` query | `es_dsl::TermsQuery` | done |
| `match` query | `es_dsl::MatchQuery` | done |
| `match_phrase` query | `es_dsl::MatchPhraseQuery` | done |
| `range` query | `es_dsl::RangeQuery` | done |
| `exists` query | `es_dsl::ExistsQuery` | done |
| `match_all` / `match_none` | `es_dsl::MatchAllQuery` / `MatchNoneQuery` | done |
| `multi_match` query | `es_dsl::MultiMatchQuery` | #6 |
| `wildcard` query | `es_dsl::WildcardQuery` | #6 |
| `prefix` query | `es_dsl::PrefixQuery` | #6 |
| Dynamic mapping | Schema inference from data | done |
| Keyword field | `FieldType::Keyword` (raw tokenizer) | done |
| Text field | `FieldType::Text` (en_stem tokenizer) | done |
| Numeric field | `FieldType::Numeric` (f64) | done |
| Date field | `FieldType::Date` (ISO 8601) | done |
| Boolean field | `FieldType::Boolean` | planned |
| IP field | `FieldType::Ip` | planned |
| Nested objects | Dot-notation flattening | #15 |
| Sort by field | `--sort` flag | #9 |
| `from`/`size` pagination | `--offset`/`--limit` | done |

### Targeted (M2)

| ES Feature | Status |
|-----------|--------|
| `terms` aggregation | #8 |
| `range` aggregation | #8 |
| `histogram` / `date_histogram` | #8 |
| Metric aggregations (avg, sum, min, max, count) | #8 |
| `search_after` pagination | #10 |
| `_search` HTTP endpoint | M3 |

### Explicitly Out of Scope

These ES features we do **not** plan to implement. They're either architecturally incompatible or not worth the complexity:

| ES Feature | Why Not |
|-----------|---------|
| Nested document type (separate Lucene docs) | Use JSON field + dot notation instead |
| Ingest pipelines / processors | Transform data before it hits Delta, not at index time |
| Index lifecycle management (ILM) | Delta handles data lifecycle; index follows |
| Cross-cluster search / replication | Single-node embedded model |
| Script fields / runtime fields | Complexity not worth it for embedded use case |
| Geo queries | Low priority; add if demand materializes |
| Percolator (reverse search) | Niche feature |
| ML / anomaly detection | Out of scope entirely |
| Security / RBAC | Handled at filesystem/blob storage ACL level |
| Snapshot / restore | Delta IS the backup; index can be rebuilt |
| _bulk HTTP endpoint | Indexing is done by the compaction worker from the Delta Lake source, not HTTP payloads |

## Performance Targets

| Metric | Target | How to Measure |
|--------|--------|---------------|
| Index throughput | >20K docs/sec (parallel pipeline) | `dsrch compact` on local Parquet files |
| Search latency (p50) | <10ms for 10M doc index | `dsrch search` with tantivy query string |
| Search latency (p99) | <100ms for 10M doc index | Same, measure across query types |
| Memory (compaction) | <50MB heap regardless of dataset size | Monitor RSS during 10M row compact |
| Memory (search) | <100MB heap for 10M doc index | Monitor RSS during search |
| Index size | <2x raw data size (with _source) | Compare index dir size to source NDJSON |
| Index size (M2) | <1x raw data size (without _source) | After #28 (drop _source) |
| Cold start | Index 1M rows in <60 seconds | `dsrch connect-delta` on fresh index |
| Gap freshness | Delta HEAD visible in <1 second | Two-tier search latency |

## Feature Areas & Acceptance Criteria

### Search

- Query string (Lucene syntax) and ES DSL are both first-class
- ES DSL accepts JSON via `--dsl` flag, `@file`, or stdin (`-`)
- Results include `_id` and requested fields
- `--score` flag includes BM25 relevance score
- `--fields` for field projection
- TTY gets human-readable table; pipe gets NDJSON
- Two-tier search: gap rows from Delta merged with persistent index, deduped by `_id`

### Compaction

- `dsrch compact` is a long-running worker process
- Polls Delta every `--poll-interval` seconds (default 10)
- Creates segments of `--segment-size` rows (default 10,000)
- Merges when segment count exceeds `--max-segments` (default 10)
- `--once` flag: one poll-segment-merge cycle, then exit
- `--force-merge` flag: merge all segments into one, then exit
- Graceful shutdown on SIGINT/SIGTERM
- Single-writer lock per index (only one compact worker at a time)
- Crash-safe: watermark updated only after segment commit, can be picked up seamlessly by the next worker. 0 index corruption, 100% correct search results during crash and recovery.
- CDF-based sync: when Delta table has Change Data Feed enabled, use row-level change tracking instead of file-level diffing. Enables merge semantics — partial updates merge into existing documents instead of replacing them. (#41)
- `--merge-mode partial|full` flag (default: partial). Partial = incoming fields merge into existing doc. Full = incoming row replaces entire doc.

### Schema

- 4 field types today (keyword, text, numeric, date), expanding to 6+ (bool, ip)
- Schema can be explicit (`--schema` JSON) or inferred from data
- `--infer-from` samples an NDJSON file and infers types
- `--dry-run` prints inferred schema without creating index
- Inferred schemas enable schema evolution (new fields auto-added)
- Explicit schemas are strict (unknown fields rejected)
- Array values expanded to multi-valued fields automatically

### Ingestion

- `dsrch ingest` converts raw files to Delta Lake tables
- Supports NDJSON, JSON, CSV, Parquet input formats
- Auto-detects format from file extension
- `--mode overwrite` (default) or `--mode append`
- Glob patterns for multi-file ingest (`--source "data/*.csv"`)

### CLI & Output

- Binary name: `dsrch`
- Global `--data-dir` flag (default: `.dsrch/`)
- Global `--output` flag: `json` or `text` (auto-detected from TTY)
- Errors go to stderr; data goes to stdout
- JSON errors on stderr when `--output json`
- Exit code 0 for success, 1 for errors, 3 for not-found (get command)

## Development Process

This project is designed for agentic development. The owner is the product lead; Claude coordinates implementation.

- **Product decisions** (what to build, priority, UX) — escalate to owner
- **Technical decisions** (how to build, architecture, code patterns) — Claude decides, guided by CLAUDE.md coding standards
- **GitHub issues** are the backlog. Each issue should reference this document's milestone and acceptance criteria.
- **Plans** live in `docs/superpowers/plans/`. Each plan covers one issue or a group of related issues.
- **CI must pass** on every merge: `cargo fmt --check` + `cargo clippy -- -D warnings` + `cargo test`

## Document History

- 2026-03-27: Initial draft. Corrected "disposable cache" framing to "incrementally hydrated persistent asset."
