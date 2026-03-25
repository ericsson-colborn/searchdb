# Delta-as-Buffer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate IndexWriter lock contention by reading un-indexed Delta rows at query time and promoting them to the tantivy index via fire-and-forget background sync.

**Architecture:** Two-tier search — persistent tantivy index (cold, covers data up to `index_version`) + ephemeral in-memory tantivy index built from Delta gap rows (hot, covers `index_version` to Delta HEAD). Background promotion via spawning `dsrch sync` as a detached child process.

**Tech Stack:** Rust, tantivy 0.25 (RamDirectory for ephemeral index), deltalake crate (async), tokio, clap 4

**Spec:** `docs/superpowers/specs/2026-03-24-buffer-promote-architecture-design.md`

---

## File Structure

```
src/
├── main.rs              # MODIFY — replace auto_sync with gap-read + fire-and-forget spawn
├── searcher.rs          # MODIFY — add build_ephemeral_index, search_with_gap, get_with_gap
├── storage.rs           # MODIFY — rename last_indexed_version → index_version
├── writer.rs            # unchanged
├── schema.rs            # unchanged
├── delta.rs             # unchanged (rows_added_since already exists)
├── error.rs             # unchanged
├── commands/
│   ├── search.rs        # MODIFY — accept gap rows, call search_with_gap
│   ├── get.rs           # MODIFY — accept gap rows, call get_with_gap
│   ├── sync.rs          # MODIFY — exit silently on IndexWriter lock failure
│   ├── stats.rs         # MODIFY — report gap size
│   ├── connect_delta.rs # MODIFY — use index_version field name
│   ├── reindex.rs       # MODIFY — use index_version field name
│   ├── new_index.rs     # unchanged
│   ├── index.rs         # unchanged
│   ├── drop.rs          # unchanged
│   └── mod.rs           # unchanged
```

---

## Task 1: Rename `last_indexed_version` → `index_version`

**Files:**
- Modify: `src/storage.rs:14`
- Modify: `src/commands/sync.rs:27,39,61`
- Modify: `src/commands/connect_delta.rs:48`
- Modify: `src/commands/reindex.rs:58`
- Modify: `src/commands/new_index.rs:28`

- [ ] **Step 1: Update the `IndexConfig` field in `storage.rs`**

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
}
```

The `alias` ensures backward compatibility — existing `searchdb.json` files with `last_indexed_version` still deserialize correctly, but new writes use `index_version`.

- [ ] **Step 2: Update all references in command files**

In `src/commands/sync.rs`, replace `config.last_indexed_version` with `config.index_version` (3 occurrences).

In `src/commands/connect_delta.rs`, replace `last_indexed_version` with `index_version` in the `IndexConfig` construction.

In `src/commands/reindex.rs`, replace `last_indexed_version` with `index_version` in the `IndexConfig` construction.

In `src/commands/new_index.rs`, replace `last_indexed_version` with `index_version` in the `IndexConfig` construction.

- [ ] **Step 3: Run tests to verify backward compat**

Run: `cargo test`
Expected: All existing tests pass (no behavior change, just field rename with alias).

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "refactor: rename last_indexed_version to index_version"
```

---

## Task 2: Build Ephemeral In-Memory Tantivy Index

**Files:**
- Modify: `src/searcher.rs`

This is the core building block — given a schema and a `Vec<serde_json::Value>`, build a tantivy `Index` in `RamDirectory` that can be searched with the full query parser.

- [ ] **Step 1: Write failing test for `build_ephemeral_index`**

Add to `src/searcher.rs` tests:

```rust
#[test]
fn test_build_ephemeral_index() {
    let schema = Schema {
        fields: BTreeMap::from([
            ("name".into(), FieldType::Keyword),
            ("notes".into(), FieldType::Text),
        ]),
    };

    let rows = vec![
        serde_json::json!({"_id": "d1", "name": "glucose", "notes": "fasting sample"}),
        serde_json::json!({"_id": "d2", "name": "a1c", "notes": "borderline diabetic"}),
    ];

    let index = build_ephemeral_index(&schema, &rows).unwrap();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    assert_eq!(searcher.num_docs(), 2);
}

#[test]
fn test_ephemeral_index_supports_query() {
    let schema = Schema {
        fields: BTreeMap::from([
            ("name".into(), FieldType::Keyword),
            ("notes".into(), FieldType::Text),
        ]),
    };

    let rows = vec![
        serde_json::json!({"_id": "d1", "name": "glucose", "notes": "fasting sample"}),
        serde_json::json!({"_id": "d2", "name": "a1c", "notes": "borderline diabetic"}),
    ];

    let index = build_ephemeral_index(&schema, &rows).unwrap();
    let results = search(&index, &schema, "notes:diabetes", 10, 0, None, false).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].doc["_id"], "d2");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test test_build_ephemeral`
Expected: FAIL — `build_ephemeral_index` not defined.

- [ ] **Step 3: Implement `build_ephemeral_index`**

Add to `src/searcher.rs`:

```rust
use tantivy::directory::RamDirectory;

/// Build a temporary in-memory tantivy index from JSON rows.
///
/// Used for searching un-indexed Delta gap rows with full query syntax
/// and proper BM25 scoring. The index lives in RamDirectory and is
/// dropped when the caller discards it.
pub fn build_ephemeral_index(
    app_schema: &Schema,
    rows: &[serde_json::Value],
) -> Result<Index> {
    let tv_schema = app_schema.build_tantivy_schema();
    let dir = RamDirectory::create();
    let index = Index::create(dir, tv_schema.clone())?;
    let mut writer = index.writer(3_000_000)?; // Small heap — gap is <5K rows by design

    let id_field = tv_schema
        .get_field("_id")
        .map_err(|_| SearchDbError::Schema("missing _id field".into()))?;

    for row in rows {
        let doc_id = crate::writer::make_doc_id(row);
        let doc = crate::writer::build_document(&tv_schema, app_schema, row, &doc_id)?;
        crate::writer::upsert_document(&writer, id_field, doc, &doc_id);
    }

    writer.commit()?;

    Ok(index)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test test_build_ephemeral`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add src/searcher.rs && git commit -m "feat: build_ephemeral_index for in-memory gap search"
```

---

## Task 3: Two-Tier Search with Dedup

**Files:**
- Modify: `src/searcher.rs`

Add `search_with_gap()` — searches both the persistent index and an ephemeral gap index, deduplicates by `_id` (gap wins). Gap hits appear first (newer data), then persistent hits sorted by score. Cross-index BM25 score comparison is not meaningful (different corpus statistics), so we don't merge by score across tiers.

- [ ] **Step 1: Write failing test for `search_with_gap`**

Add to `src/searcher.rs` tests:

```rust
#[test]
fn test_search_with_gap_dedup() {
    // Persistent index has d1=original, d2
    let dir = tempfile::tempdir().unwrap();
    let schema = Schema {
        fields: BTreeMap::from([
            ("name".into(), FieldType::Keyword),
            ("notes".into(), FieldType::Text),
        ]),
    };
    let tv_schema = schema.build_tantivy_schema();
    let index = Index::create_in_dir(dir.path(), tv_schema.clone()).unwrap();
    let mut w = index.writer(50_000_000).unwrap();
    let id_field = tv_schema.get_field("_id").unwrap();

    for doc_json in &[
        serde_json::json!({"_id": "d1", "name": "glucose", "notes": "old version"}),
        serde_json::json!({"_id": "d2", "name": "a1c", "notes": "unchanged"}),
    ] {
        let doc_id = writer::make_doc_id(doc_json);
        let doc = writer::build_document(&tv_schema, &schema, doc_json, &doc_id).unwrap();
        writer::upsert_document(&w, id_field, doc, &doc_id);
    }
    w.commit().unwrap();

    // Gap has updated d1 + new d3
    let gap_rows = vec![
        serde_json::json!({"_id": "d1", "name": "glucose", "notes": "NEW version"}),
        serde_json::json!({"_id": "d3", "name": "creatinine", "notes": "kidney function"}),
    ];

    let results = search_with_gap(
        &index, &schema, "+__present__:__all__", 10, 0, None, false, &gap_rows,
    ).unwrap();

    // Should have 3 docs: d1 (gap version), d2 (index), d3 (gap)
    assert_eq!(results.len(), 3);

    let d1 = results.iter().find(|h| h.doc["_id"] == "d1").unwrap();
    assert_eq!(d1.doc["notes"], "NEW version"); // gap wins

    assert!(results.iter().any(|h| h.doc["_id"] == "d2"));
    assert!(results.iter().any(|h| h.doc["_id"] == "d3"));
}

#[test]
fn test_search_with_gap_empty_gap() {
    let dir = tempfile::tempdir().unwrap();
    let (index, schema, _) = setup_test_index(dir.path());

    // Empty gap — should behave like normal search
    let results = search_with_gap(
        &index, &schema, "+__present__:__all__", 10, 0, None, false, &[],
    ).unwrap();
    assert_eq!(results.len(), 3);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test test_search_with_gap`
Expected: FAIL — `search_with_gap` not defined.

- [ ] **Step 3: Implement `search_with_gap`**

Add to `src/searcher.rs`:

```rust
/// Two-tier search: persistent index + ephemeral gap index, dedup by _id.
///
/// Gap rows win over persistent index rows for the same _id (newer data).
/// Gap hits appear first (newer), then persistent hits by score.
/// NOTE: BM25 scores are not comparable across indexes (different corpus stats),
/// so we don't merge by score across tiers.
#[allow(clippy::too_many_arguments)]
pub fn search_with_gap(
    persistent_index: &Index,
    app_schema: &Schema,
    query_str: &str,
    limit: usize,
    offset: usize,
    fields: Option<&[String]>,
    include_score: bool,
    gap_rows: &[serde_json::Value],
) -> Result<Vec<SearchHit>> {
    if gap_rows.is_empty() {
        return search(persistent_index, app_schema, query_str, limit, offset, fields, include_score);
    }

    // 1. Build ephemeral index from gap rows
    let gap_index = build_ephemeral_index(app_schema, gap_rows)?;

    // 2. Search the gap index (collect all — gap is small by design)
    let gap_hits = search(
        &gap_index, app_schema, query_str, gap_rows.len(), 0, fields, include_score,
    )?;

    // 3. Collect gap _ids for dedup
    let gap_ids: std::collections::HashSet<String> = gap_hits
        .iter()
        .filter_map(|h| h.doc.get("_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();

    // 4. Search persistent index, excluding gap _ids
    let persistent_hits = search(
        persistent_index, app_schema, query_str,
        limit + offset, 0, fields, include_score,
    )?;

    let filtered_persistent: Vec<SearchHit> = persistent_hits
        .into_iter()
        .filter(|h| {
            h.doc.get("_id")
                .and_then(|v| v.as_str())
                .map(|id| !gap_ids.contains(id))
                .unwrap_or(true)
        })
        .collect();

    // 5. Gap hits first (newer data), then persistent hits (by score), paginate
    let mut merged = gap_hits;
    merged.extend(filtered_persistent);

    let paginated: Vec<SearchHit> = merged.into_iter().skip(offset).take(limit).collect();

    Ok(paginated)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test test_search_with_gap`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add src/searcher.rs && git commit -m "feat: two-tier search_with_gap with _id dedup"
```

---

## Task 4: Two-Tier Get with Gap Priority

**Files:**
- Modify: `src/searcher.rs`

Add `get_with_gap()` — checks gap rows for `_id` first, falls back to persistent index.

- [ ] **Step 1: Write failing tests**

Add to `src/searcher.rs` tests:

```rust
#[test]
fn test_get_with_gap_found_in_gap() {
    let dir = tempfile::tempdir().unwrap();
    let (index, _, _) = setup_test_index(dir.path());

    let gap_rows = vec![
        serde_json::json!({"_id": "d1", "name": "UPDATED", "notes": "new version"}),
    ];

    let doc = get_with_gap(&index, "d1", &gap_rows).unwrap();
    assert!(doc.is_some());
    assert_eq!(doc.unwrap()["name"], "UPDATED"); // gap wins
}

#[test]
fn test_get_with_gap_falls_back_to_index() {
    let dir = tempfile::tempdir().unwrap();
    let (index, _, _) = setup_test_index(dir.path());

    // d2 is only in the persistent index, not the gap
    let gap_rows = vec![
        serde_json::json!({"_id": "d99", "name": "unrelated"}),
    ];

    let doc = get_with_gap(&index, "d2", &gap_rows).unwrap();
    assert!(doc.is_some());
    assert_eq!(doc.unwrap()["_id"], "d2");
}

#[test]
fn test_get_with_gap_not_found_anywhere() {
    let dir = tempfile::tempdir().unwrap();
    let (index, _, _) = setup_test_index(dir.path());

    let doc = get_with_gap(&index, "nonexistent", &[]).unwrap();
    assert!(doc.is_none());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test test_get_with_gap`
Expected: FAIL — `get_with_gap` not defined.

- [ ] **Step 3: Implement `get_with_gap`**

Add to `src/searcher.rs`:

```rust
/// Two-tier get: check gap rows first (by _id), fall back to persistent index.
pub fn get_with_gap(
    persistent_index: &Index,
    doc_id: &str,
    gap_rows: &[serde_json::Value],
) -> Result<Option<serde_json::Value>> {
    // Check gap first — newer data wins
    for row in gap_rows {
        if let Some(id) = row.get("_id").and_then(|v| v.as_str()) {
            if id == doc_id {
                let mut doc = row.clone();
                if let Some(obj) = doc.as_object_mut() {
                    obj.insert("_id".to_string(), serde_json::Value::String(doc_id.to_string()));
                }
                return Ok(Some(doc));
            }
        }
    }

    // Fall back to persistent index
    get_by_id(persistent_index, doc_id)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test test_get_with_gap`
Expected: PASS (all 3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/searcher.rs && git commit -m "feat: get_with_gap for gap-first document lookup"
```

---

## Task 5: Sync Command — Silent Exit on Lock Contention

**Files:**
- Modify: `src/commands/sync.rs:46`

- [ ] **Step 1: Modify sync to catch lock failure**

In `src/commands/sync.rs`, replace the direct `index.writer(50_000_000)?` call with a match that catches lock failure:

```rust
    let mut index_writer = match index.writer(50_000_000) {
        Ok(w) => w,
        Err(tantivy::TantivyError::LockFailure(_, _)) => {
            eprintln!("[dsrch] Index locked by another process, skipping sync");
            return Ok(());
        }
        Err(e) => return Err(SearchDbError::Tantivy(e)),
    };
```

This makes `dsrch sync` safe to spawn as a fire-and-forget process — if the lock is held, it exits cleanly with code 0.

- [ ] **Step 2: Run tests**

Run: `cargo test`
Expected: All existing tests pass (the lock failure path isn't triggered in tests).

- [ ] **Step 3: Commit**

```bash
git add src/commands/sync.rs && git commit -m "feat: sync exits silently on IndexWriter lock contention"
```

---

## Task 6: Wire Up Search and Get Commands

**Files:**
- Modify: `src/main.rs`
- Modify: `src/commands/search.rs`
- Modify: `src/commands/get.rs`

This replaces the current `auto_sync()` (which blocks on IndexWriter) with gap reading at query time.

- [ ] **Step 1: Add `read_gap` helper to `main.rs`**

Replace the `auto_sync` function with a `read_gap` function that returns gap rows without acquiring any lock:

```rust
/// Read un-indexed Delta rows (the gap between index_version and HEAD).
/// Returns (gap_rows, gap_size_in_versions) without acquiring any lock.
#[cfg(feature = "delta")]
async fn read_gap(storage: &Storage, name: &str) -> (Vec<serde_json::Value>, i64) {
    if !storage.exists(name) {
        return (vec![], 0);
    }
    let config = match storage.load_config(name) {
        Ok(c) => c,
        Err(_) => return (vec![], 0),
    };
    let source = match config.delta_source.as_deref() {
        Some(s) => s,
        None => return (vec![], 0),
    };

    let delta = crate::delta::DeltaSync::new(source);
    let current_version = match delta.current_version().await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[dsrch] Delta read warning: {e}");
            return (vec![], 0);
        }
    };
    let index_version = config.index_version.unwrap_or(-1);

    if current_version <= index_version {
        return (vec![], 0);
    }

    let gap_versions = current_version - index_version;
    let rows = match delta.rows_added_since(index_version).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[dsrch] Delta gap read warning: {e}");
            return (vec![], gap_versions);
        }
    };
    (rows, gap_versions)
}
```

- [ ] **Step 2: Add `maybe_spawn_sync` helper to `main.rs`**

```rust
/// Fire-and-forget background sync if gap exceeds threshold.
#[cfg(feature = "delta")]
fn maybe_spawn_sync(data_dir: &str, name: &str, gap_versions: i64) {
    const GAP_THRESHOLD: i64 = 10;
    if gap_versions <= GAP_THRESHOLD {
        return;
    }
    let exe = match std::env::current_exe() {
        Ok(e) => e,
        Err(_) => return,
    };
    let _ = std::process::Command::new(exe)
        .args(["sync", name, "--data-dir", data_dir])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}
```

- [ ] **Step 3: Update search command to accept gap rows**

Modify `src/commands/search.rs` — change `run` signature to accept gap rows:

```rust
#[allow(clippy::too_many_arguments)]
pub fn run(
    storage: &Storage,
    name: &str,
    query: &str,
    limit: usize,
    offset: usize,
    fields: Option<Vec<String>>,
    include_score: bool,
    fmt: OutputFormat,
    gap_rows: &[serde_json::Value],
) -> Result<()> {
```

Replace `searcher::search(...)` call with `searcher::search_with_gap(...)`, passing `gap_rows`.

- [ ] **Step 4: Update get command to accept gap rows**

Modify `src/commands/get.rs` — change `run` signature to accept gap rows:

```rust
pub fn run(
    storage: &Storage,
    name: &str,
    doc_id: &str,
    fields: Option<Vec<String>>,
    fmt: OutputFormat,
    gap_rows: &[serde_json::Value],
) -> Result<()> {
```

Replace `searcher::get_by_id(...)` with `searcher::get_with_gap(...)`, passing `gap_rows`.

- [ ] **Step 5: Update `run_cli()` in main.rs**

Replace the `auto_sync` calls with `read_gap` + `maybe_spawn_sync`:

```rust
Commands::Search { name, query, limit, offset, fields, score } => {
    let (gap_rows, gap_versions) = read_gap(&storage, &name).await;
    let result = commands::search::run(
        &storage, &name, &query, limit, offset, fields, score, fmt, &gap_rows,
    );
    maybe_spawn_sync(&cli.data_dir, &name, gap_versions);
    result
}
Commands::Get { name, doc_id, fields } => {
    let (gap_rows, gap_versions) = read_gap(&storage, &name).await;
    let result = commands::get::run(
        &storage, &name, &doc_id, fields, fmt, &gap_rows,
    );
    maybe_spawn_sync(&cli.data_dir, &name, gap_versions);
    result
}
```

Do the same for `run_cli_sync()` (non-delta build) — pass empty gap rows:

```rust
Commands::Search { .. } => {
    commands::search::run(&storage, &name, &query, limit, offset, fields, score, fmt, &[])
}
Commands::Get { .. } => {
    commands::get::run(&storage, &name, &doc_id, fields, fmt, &[])
}
```

- [ ] **Step 6: Remove `auto_sync` function from `main.rs`**

Delete the `auto_sync` function entirely.

- [ ] **Step 7: Run full test suite**

Run: `cargo fmt && cargo clippy -- -D warnings && cargo test`
Expected: All tests pass. The existing tests pass empty gap rows implicitly.

- [ ] **Step 8: Commit**

```bash
git add -A && git commit -m "feat: wire up two-tier search with Delta gap reading and fire-and-forget sync"
```

---

## Task 7: Update Stats Command

**Files:**
- Modify: `src/commands/stats.rs`

- [ ] **Step 1: Add gap info to stats output**

The stats command should report `index_version` and, when a Delta source is configured, the current Delta HEAD version and gap size. Pass gap info as an `Option<(i64, i64)>` tuple `(delta_version, gap_versions)` to avoid cross-module struct dependencies.

In `main.rs`, before calling `stats::run`, read the gap info:

```rust
// In run_cli() (delta-enabled):
Commands::Stats { name } => {
    let gap_info = read_gap_info(&storage, &name).await;
    commands::stats::run(&storage, &name, fmt, gap_info)
}

// In run_cli_sync() (non-delta):
Commands::Stats { name } => commands::stats::run(&storage, &name, fmt, None),
```

Add a helper in `main.rs`:

```rust
/// Read Delta version info for stats display.
#[cfg(feature = "delta")]
async fn read_gap_info(storage: &Storage, name: &str) -> Option<(i64, i64)> {
    let config = storage.load_config(name).ok()?;
    let source = config.delta_source.as_deref()?;
    let delta = crate::delta::DeltaSync::new(source);
    let delta_version = delta.current_version().await.ok()?;
    let index_version = config.index_version.unwrap_or(-1);
    Some((delta_version, delta_version - index_version))
}
```

Update `stats::run` signature to accept `gap_info: Option<(i64, i64)>`:

```rust
pub fn run(storage: &Storage, name: &str, fmt: OutputFormat, gap_info: Option<(i64, i64)>) -> Result<()> {
```

Update stats JSON output to include:

```json
{
  "index": "labs",
  "num_docs": 10000,
  "num_segments": 3,
  "index_version": 103,
  "delta_version": 108,
  "gap_versions": 5,
  "fields": {"name": "keyword", "notes": "text"}
}
```

Update stats text output to include:

```
Index:      labs
Documents:  10000
Segments:   3
Version:    103 (Delta HEAD: 108, gap: 5 versions)
Fields:
  name: Keyword
  notes: Text
```

**Note:** The stats JSON output field changes from `last_indexed_version` to `index_version`. This is a breaking change to the CLI output contract, aligned with the Task 1 field rename.

- [ ] **Step 2: Update stats tests**

Update all test calls to pass `None` for `gap_info`:

```rust
run(&storage, "test", OutputFormat::Json, None).unwrap();
```

Update the error test similarly:

```rust
let result = run(&storage, "nope", OutputFormat::Json, None);
```

- [ ] **Step 3: Run tests**

Run: `cargo fmt && cargo clippy -- -D warnings && cargo test`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat: stats reports gap size between index and Delta HEAD"
```

---

## Task 8: Integration Test — Full Two-Tier Flow

**Files:**
- Modify: `src/delta.rs` (add integration test)

- [ ] **Step 1: Write integration test**

Add to `src/delta.rs` tests — a test that exercises the full flow: create Delta table → connect → search → add more rows to Delta → search again (gap read) → sync → search (no gap):

```rust
#[tokio::test]
async fn test_two_tier_search_with_delta_gap() {
    let dir = tempfile::tempdir().unwrap();
    let delta_path = dir.path().join("delta_table");
    let delta_str = delta_path.to_str().unwrap();
    let index_dir = dir.path().join("index_data");
    let index_str = index_dir.to_str().unwrap();

    // Create Delta table with initial data
    create_delta_table(delta_str, &[("d1", "glucose", 100.0)]).await;

    // Connect — full load into tantivy index
    let storage = crate::storage::Storage::new(index_str);
    crate::commands::connect_delta::run(
        &storage, "test", delta_str,
        r#"{"fields":{"name":"keyword","value":"numeric"}}"#,
    ).await.unwrap();

    // Verify initial search works
    let config = storage.load_config("test").unwrap();
    let index = tantivy::Index::open_in_dir(storage.tantivy_dir("test")).unwrap();
    let results = crate::searcher::search_with_gap(
        &index, &config.schema, "+__present__:__all__", 10, 0, None, false, &[],
    ).unwrap();
    assert_eq!(results.len(), 1);

    // Add more rows to Delta (creates a gap)
    append_to_delta(delta_str, &[("d2", "a1c", 5.7)]).await;

    // Read gap rows
    let sync = DeltaSync::new(delta_str);
    let index_version = config.index_version.unwrap_or(-1);
    let gap_rows = sync.rows_added_since(index_version).await.unwrap();
    assert!(!gap_rows.is_empty());

    // Two-tier search should find both docs
    let results = crate::searcher::search_with_gap(
        &index, &config.schema, "+__present__:__all__", 10, 0, None, false, &gap_rows,
    ).unwrap();
    assert_eq!(results.len(), 2);
}
```

- [ ] **Step 2: Run test**

Run: `cargo test test_two_tier_search_with_delta_gap`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/delta.rs && git commit -m "test: integration test for two-tier search with Delta gap"
```

---

## Task 9: Final Verification

- [ ] **Step 1: Format + lint + test**

```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo test
```

Expected: All pass, 0 warnings.

- [ ] **Step 2: E2E smoke test**

```bash
rm -rf /tmp/dsrch_e2e
./target/debug/dsrch --data-dir /tmp/dsrch_e2e new labs \
  --schema '{"fields":{"name":"keyword","notes":"text"}}'

echo '{"_id":"1","name":"glucose","notes":"fasting sample"}
{"_id":"2","name":"a1c","notes":"borderline diabetic"}' \
  | ./target/debug/dsrch --data-dir /tmp/dsrch_e2e index labs

./target/debug/dsrch --data-dir /tmp/dsrch_e2e search labs -q 'diabetes' --output json
# Expected: doc 2 (stemmed match)

./target/debug/dsrch --data-dir /tmp/dsrch_e2e get labs 1 --output json
# Expected: doc 1

./target/debug/dsrch --data-dir /tmp/dsrch_e2e stats labs --output json
# Expected: num_docs: 2
```

- [ ] **Step 3: Commit all remaining changes**

```bash
git add -A && git commit -m "feat: Delta-as-buffer architecture — two-tier search with background promotion"
```
