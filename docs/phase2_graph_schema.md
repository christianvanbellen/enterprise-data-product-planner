# Phase 2 Graph Schema

---

## What Phase 2 produces

`StructuralGraphCompiler` consumes a `CanonicalBundle` (Phase 1 output) and produces a
property graph persisted as two JSON files:

| file              | description                              | golden run count |
|-------------------|------------------------------------------|-----------------|
| `nodes.json`      | Array of node objects                    | 3 172           |
| `edges.json`      | Array of edge objects                    | 3 365           |

Every node and edge carries an `EvidenceRecord` with a `build_id` that ties it to the
exact pipeline run that created it.

---

## Node types

| label        | count (golden) | node_id prefix   | description                                               |
|--------------|---------------|------------------|-----------------------------------------------------------|
| `Asset`      | 207           | `asset_`         | A dbt model or source table                               |
| `Column`     | 2 654         | `col_`           | A column belonging to an asset                            |
| `Schema`     | 7             | `schema_`        | Logical container grouping assets under a `(database, schema_name)` key |
| `Test`       | 96            | `test_`          | A dbt data quality test attached to a column              |
| `DocObject`  | 207           | `doc_`           | Documentation presence record for an asset                |
| `_BuildMeta` | 1             | `_build_`        | Internal: records pipeline run metadata (not for queries) |

### Asset node properties

| property          | type            | description                                              |
|-------------------|-----------------|----------------------------------------------------------|
| `asset_id`        | string          | Stable hash ID (`asset_` + sha256[:16])                  |
| `name`            | string          | Raw asset name from dbt                                  |
| `normalized_name` | string          | Snake-case normalised name                               |
| `asset_type`      | string          | `"table"`, `"view"`, `"incremental"`, etc.               |
| `schema_name`     | string          | dbt schema (logical container)                           |
| `database`        | string\|null    | Target database name                                     |
| `materialization` | string\|null    | `"table"`, `"view"`, `"incremental"`, `"ephemeral"`      |
| `row_count`       | int\|null       | Last-known row count                                     |
| `path`            | string\|null    | SQL file path relative to project root                   |
| `tags`            | list[string]    | dbt tags                                                 |
| `domain_candidates` | list[string] | Inferred domain labels from keyword scan                 |
| `grain_keys`      | list[string]    | Column names identified as grain keys (identifiers)      |
| `is_enabled`      | bool            | Whether the dbt model is enabled                         |
| `version_hash`    | string          | Stable hash of the source entity JSON (`sha256[:16]`)    |

### Column node properties

| property          | type         | description                                              |
|-------------------|--------------|----------------------------------------------------------|
| `column_id`       | string       | Stable hash ID (`col_` + sha256[:16])                    |
| `name`            | string       | Column name                                              |
| `normalized_name` | string       | Snake-case normalised name                               |
| `data_type_family`| string       | Normalised type: `string`, `numeric`, `timestamp`, etc.  |
| `raw_data_type`   | string\|null | Original type string from dbt metadata                   |
| `column_role`     | string       | Semantic role: `identifier`, `measure`, `timestamp`, etc.|
| `is_nullable`     | bool\|null   | Nullability from `information_schema`                    |
| `ordinal_position`| int\|null    | Column position in the table                             |

### Test node properties

| property      | type   | description                                   |
|---------------|--------|-----------------------------------------------|
| `test_id`     | string | Stable hash ID (`test_` + sha256[:16])        |
| `test_type`   | string | e.g. `"quoted_not_null"`, `"unique"`          |
| `column_name` | string | The column being tested                       |
| `status`      | string | `"unknown"` (Phase 1 does not run tests)      |

### Schema node properties

| property        | type          | description                                                |
|-----------------|---------------|------------------------------------------------------------|
| `schema_id`     | string        | Stable hash ID (`schema_` + sha256[:16] of `database::schema_name`) |
| `name`          | string        | Raw schema name from dbt (e.g. `hx`, `publish`, `liberty_link`) |
| `database`      | string\|null  | Physical database name (e.g. `mart_lii_hx_rating_db`); `null` if absent |
| `asset_count`   | int           | Number of assets contained by this schema — useful for graph summary views |

One Schema node is emitted per unique `(database, schema_name)` pair observed on bundle assets.
Assets with no `schema_name` are skipped entirely and do not appear in any `CONTAINS` edge.

### DocObject node properties

| property        | type   | description                                     |
|-----------------|--------|-------------------------------------------------|
| `doc_id`        | string | Stable hash ID (`doc_` + sha256[:16])           |
| `asset_id`      | string | The asset this doc record belongs to            |
| `has_description` | bool | Whether the asset has a non-empty description   |

---

## Edge types

| edge_type      | source label | target label | count (golden) | description                                      |
|----------------|--------------|--------------|---------------|--------------------------------------------------|
| `HAS_COLUMN`   | Asset        | Column       | 2 654         | Asset owns column                                |
| `CONTAINS`     | Schema       | Asset        | 207           | Schema logically contains asset                  |
| `DEPENDS_ON`   | Asset        | Asset        | 201           | Asset depends on upstream asset                  |
| `TESTED_BY`    | Column       | Test         | 96            | Column has a dbt test                            |
| `DOCUMENTED_BY`| Asset        | DocObject    | 207           | Asset has a documentation record                 |

All `CONTAINS` edge source IDs resolve to real Schema nodes in `nodes.json` (prior to April 2026
the Schema endpoints were virtual dangling references — see `docs/backlog.md` for the fix entry).

### DEPENDS_ON edge properties

| property          | type   | description                                              |
|-------------------|--------|----------------------------------------------------------|
| `confidence`      | float  | Always `1.0` for explicit dbt upstream_dependencies     |
| `relation_type`   | string | Always `"depends_on"`                                    |

Evidence `rule_id` is `"lineage.explicit_upstream"`.
Evidence `derivation_method` is `"explicit_metadata"`.

---

## Evidence model

Every non-`_BuildMeta` node and edge carries an `evidence` object:

```json
{
  "build_id":        "build_693e66d1c1ed580f",
  "confidence":      1.0,
  "created_by":      "structural_compiler_v1",
  "evidence_sources": [
    {"type": "canonical_asset",     "value": "asset_ae98a8c1f4ff0ffa"},
    {"type": "derivation_method",   "value": "explicit_metadata"}
  ],
  "review_status":   "auto",
  "rule_id":         "structural.asset_node",
  "timestamp_utc":   "2026-04-14T19:34:28.737589+00:00"
}
```

| field             | values                                       |
|-------------------|----------------------------------------------|
| `review_status`   | `"auto"` (machine-generated, not reviewed)   |
| `confidence`      | 0.0–1.0; all Phase 2 nodes are 1.0           |
| `created_by`      | `"structural_compiler_v1"`                   |
| `rule_id`         | See table below                              |

### rule_id reference

| rule_id                      | produced by step       |
|------------------------------|------------------------|
| `structural.asset_node`      | Step 1 — asset nodes   |
| `structural.column_node`     | Step 2 — column nodes  |
| `structural.has_column`      | Step 2 — HAS_COLUMN edges |
| `structural.schema_node`     | Step 3 — Schema nodes  |
| `structural.containment`     | Step 3 — CONTAINS edges |
| `lineage.explicit_upstream`  | Step 4 — DEPENDS_ON edges |
| `structural.test_node`       | Step 6 — Test nodes    |
| `structural.tested_by`       | Step 6 — TESTED_BY edges |
| `structural.doc_node`        | Step 7 — DocObject nodes |
| `structural.documented_by`   | Step 7 — DOCUMENTED_BY edges |

---

## Graph store interface

Phase 2 ships two graph store implementations:

### JsonGraphStore (default)

Zero external dependencies. In-memory dict; persists to `nodes.json` + `edges.json`.

```python
from graph.store.json_store import JsonGraphStore
store = JsonGraphStore(output_dir=Path("output/graph"))
store.upsert_nodes(nodes)
store.upsert_edges(edges)
store.export_json(Path("output/graph"))

# BFS traversal
reachable = store.query_lineage("asset_abc123", direction="upstream", depth=5)
```

`query_lineage` direction semantics:
- `"upstream"` — follows `DEPENDS_ON` source→target (find what an asset depends on)
- `"downstream"` — follows `DEPENDS_ON` target→source (find what depends on an asset)

### Neo4jGraphStore (optional)

Requires `ENABLE_NEO4J=true` environment variable and a running Neo4j instance.
Not used in the default pipeline. Same interface as `JsonGraphStore`.

---

## Limitations

- **SQL column lineage disabled** — Step 5 (`_create_sql_column_edges`) is gated by
  `ENABLE_SQL_LINEAGE=true`. It is not implemented; the gate is a placeholder.

- **Test status is always `unknown`** — Phase 1 captures test definitions from dbt
  metadata but does not run tests. `Test.status` is always `"unknown"`.

- **No deduplication across adapters** — `CanonicalBundle.merge()` concatenates without
  collision detection. If two adapters emit the same `internal_id`, both appear in the
  graph.

- **`_BuildMeta` node has no evidence** — the build metadata node has `evidence: {}`.
  It is for operational tracking, not for queries.

---

## Querying locally

```bash
# Run all 7 structural validation checks
python scripts/validate_graph.py --graph output/graph/ --bundle output/bundle.json

# Run 4 analytical queries
python scripts/query_graph.py --graph output/graph/

# Inspect graph files directly
python -c "
import json
nodes = json.loads(open('output/graph/nodes.json').read())
edges = json.loads(open('output/graph/edges.json').read())
print(len(nodes), 'nodes,', len(edges), 'edges')
"
```
