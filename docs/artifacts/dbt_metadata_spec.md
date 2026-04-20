# dbt Metadata Artifact Spec

**Artifact name:** `dbt_metadata_enriched.json`
**Current version confirmed against:** dbt 1.10.11 / manifest schema v12

---

## What this artifact is

`dbt_metadata_enriched.json` is a post-processed export produced by a dbt manifest
enrichment script that runs after `dbt compile` or `dbt docs generate`. It takes the
standard dbt `manifest.json` and augments it with live Redshift information-schema data
(column types, row counts, table sizes) and pre-computed lineage edges. The file
represents the full analytical model graph of a single dbt project (`gen2_mart`),
including both model nodes and source table references, with all column-level metadata
the Redshift catalog could provide.

---

## Root structure

```jsonc
{
  "metadata": {                        // dbt build metadata — always present
    "dbt_schema_version": "...",
    "dbt_version": "1.10.11",
    "generated_at": "2026-04-13T09:03:28.531237Z",
    "project_name": "gen2_mart"
  },
  "entities": [ ... ],                 // array of model/source objects — always present
  "lineage_edges": [ ... ],            // pre-computed edge list (source/target/relationship)
                                       // NOTE: adapter ignores this; uses upstream_dependencies
                                       //       on each entity instead
  "summary": {                         // aggregate statistics — informational only
    "total_models": 165,
    "total_sources": 42,
    "total_columns": 2654,
    "total_lineage_edges": 201,
    ...
  }
}
```

---

## Entity object — required fields

| field_name             | type             | description                                                       | used_for                          | required? |
|------------------------|------------------|-------------------------------------------------------------------|-----------------------------------|-----------|
| `unique_id`            | string           | Fully-qualified dbt node ID, e.g. `model.gen2_mart.hx_raw`       | stable `asset_id` hash input      | required  |
| `name`                 | string           | Short model/source name, e.g. `hx_raw`                           | `CanonicalAsset.name`             | required  |
| `resource_type`        | string           | `"model"` or `"source"`                                           | determines `asset_type`           | required  |
| `description`          | string or `null` | Free-text description; may be empty string `""`                   | `CanonicalAsset.description`      | optional  |
| `database`             | string or `null` | Redshift database name                                            | `CanonicalAsset.database`         | optional  |
| `schema`               | string or `null` | Redshift schema name                                              | `CanonicalAsset.schema_name`      | optional  |
| `materialized`         | string or `null` | `"table"`, `"view"`, `"incremental"`, `null` for sources         | `CanonicalAsset.materialization` / `asset_type` | optional |
| `tags`                 | list[string]     | User-defined tags, e.g. `["HX", "bookends"]`; may be empty list  | `CanonicalAsset.tags`             | optional  |
| `columns`              | list[object]     | Column metadata; may be an empty list                             | `CanonicalColumn` objects         | required  |
| `upstream_dependencies`| list[string]     | List of upstream `unique_id` strings; empty list for root nodes   | `CanonicalLineageEdge` objects    | required  |
| `path`                 | string or `null` | Relative path to the `.sql` or `.yml` file                        | `CanonicalAsset.path`             | optional  |
| `row_count`            | string or `null` | Row count as a **string** (e.g. `"2191"`), not an integer         | `CanonicalAsset.row_count`        | optional  |
| `size_mb`              | int or `null`    | Table size in megabytes                                           | `CanonicalAsset.size_mb`          | optional  |

**Source-only fields** (present when `resource_type == "source"`):

| field_name    | type   | description                             | used_for                     |
|---------------|--------|-----------------------------------------|------------------------------|
| `source_name` | string | dbt source group name, e.g. `"hx"`     | informational; not ingested  |

---

## Entity object — columns array

`columns` is a **list of objects**, not a keyed dict. Each object has:

```jsonc
{
  "name":            "breadth_of_cover_change",   // string — column name
  "description":     "For Rate Monitoring ...",    // string or null
  "data_type":       "numeric(16,4)",              // string — may be "" (empty)
  "meta":            {},                           // dict — adapter ignores contents
  "tests":           [],                           // list[string|object] — test names/configs
  "is_nullable":     "YES",                        // "YES", "NO", or null — NOT a boolean
  "ordinal_position": 21                           // int — physical column order
}
```

| field_name       | type              | description                                                    | used_for                             | required? |
|------------------|-------------------|----------------------------------------------------------------|--------------------------------------|-----------|
| `name`           | string            | Column name exactly as in Redshift                             | `CanonicalColumn.name`, hash input   | required  |
| `description`    | string or `null`  | Free-text description; may be empty string `""`                | `CanonicalColumn.description`        | optional  |
| `data_type`      | string            | Redshift SQL type; empty string `""` treated as no type        | `data_type_family`, `column_role`    | optional  |
| `meta`           | dict              | Arbitrary metadata from dbt schema YAML                        | `CanonicalColumn.meta`               | optional  |
| `tests`          | list              | dbt test references; items are strings or dicts                | `CanonicalColumn.tests`              | optional  |
| `is_nullable`    | string or `null`  | `"YES"` → `True`, `"NO"` → `False`, `null` → `None`           | `CanonicalColumn.is_nullable`        | optional  |
| `ordinal_position` | int or `null`   | Physical column position; used as `ordinal_position`           | `CanonicalColumn.ordinal_position`   | optional  |

**Known data types in this file:** `numeric(p,s)`, `character varying(n)`, `integer`,
`bigint`, `boolean`, `date`, `timestamp without time zone`, `super`, `""` (empty).

---

## Lineage fields

**Where upstream dependencies live:**

```
entity.upstream_dependencies  →  list[str]   ← adapter uses this
entity.depends_on              →  NOT present in this artifact
root.lineage_edges             →  list[{source, target, relationship}]  ← adapter ignores
```

`upstream_dependencies` is a flat list of dbt `unique_id` strings for nodes this entity
depends on. For model nodes the IDs follow the pattern `model.<project>.<name>`; for source
nodes they follow `source.<project>.<source_group>.<name>`.

The adapter maps each upstream `unique_id` to a `source_asset_id` by hashing
`stable_hash("dbt", upstream_unique_id)` — producing the same `asset_` prefix ID that
the upstream entity itself produces when parsed. This is the determinism guarantee:
any model that references `model.gen2_mart.hx_raw` as a dependency will produce the
same edge `source_asset_id` as `hx_raw` produces for its own `internal_id`.

Root-level `lineage_edges` (the `source`/`target`/`relationship` array) is present
in the file but is **not read by the adapter**. All lineage is derived from
`entity.upstream_dependencies`.

---

## Accepted variants

| Variant | Trigger condition | Adapter behaviour |
|---------|-------------------|-------------------|
| Root object with `entities` key | `isinstance(raw, dict)` and `"entities" in raw` | Uses `raw["entities"]` — this is the canonical form for this file |
| Root list | `isinstance(raw, list)` | Treats the list directly as the entities array |
| Single root object with no `entities` key | `isinstance(raw, dict)` and `"entities" not in raw` | Wraps in a single-element list: `[raw]` |
| Columns as list | `isinstance(entity["columns"], list)` | Iterates, uses `col["name"]` as the key — this is the canonical form |
| Columns as dict | `isinstance(entity["columns"], dict)` | Converts to list: `[{"name": k, **v} for k, v in cols.items()]` |
| `upstream_dependencies` present | `isinstance(deps, list)` | Iterates directly |
| `depends_on.nodes` present (standard dbt manifest) | `isinstance(deps, dict)` | Uses `deps["nodes"]` |

---

## What the adapter does NOT handle

- **Nested source references in `upstream_dependencies`**: The adapter creates an
  `asset_` ID for every upstream unique_id it encounters, including source references
  like `source.gen2_mart.liberty_link.tbl_*`. If that source entity is present in the
  same file it will resolve; if not (e.g. cross-project references) the edge will show
  as an unresolved reference in `GraphBuildArtifact.unresolved_lineage`.

- **Root-level `lineage_edges` array**: The `{source, target, relationship}` objects
  at root are ignored. They represent the same information as `upstream_dependencies`
  but the per-entity field is used exclusively.

- **Multiple files / multi-project merging**: The adapter assumes a single-file,
  single-project input. Merging two projects' outputs requires running the adapter
  twice and using `CanonicalBundle.merge()`, with the risk of ID collisions for
  shared source tables.

- **SQL body parsing**: `raw_sql` and `compiled_sql` are present on model entities but
  are not read. SQL-level column lineage is deferred to `SqlArtifactAdapter` (Phase 2
  optional, gated by `ENABLE_SQL_LINEAGE`).

- **`row_count` as a string**: The artifact stores `row_count` as a string
  (e.g. `"2191"`), not an integer. The adapter casts it with `int(row_count_raw)`. A
  non-numeric string (e.g. `"N/A"`) will raise `ValueError` and is not handled.

---

## Sample minimal valid file

This example is consistent with `tests/ingestion/golden/minimal_dbt_sample.json`:

```json
{
  "metadata": {
    "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
    "dbt_version": "1.10.0",
    "generated_at": "2026-01-01T00:00:00.000000Z",
    "project_name": "test_project"
  },
  "entities": [
    {
      "unique_id": "model.test_project.raw_quotes",
      "name": "raw_quotes",
      "resource_type": "model",
      "description": "Raw quote data",
      "database": "test_db",
      "schema": "raw",
      "materialized": "table",
      "tags": ["raw"],
      "upstream_dependencies": [],
      "path": "raw/raw_quotes.sql",
      "row_count": "1000",
      "size_mb": 10,
      "columns": [
        {
          "name": "quote_id",
          "description": "Unique quote identifier",
          "data_type": "varchar(36)",
          "meta": {},
          "tests": ["not_null", "unique"],
          "is_nullable": "NO",
          "ordinal_position": 1
        },
        {
          "name": "premium_amount",
          "description": "Gross written premium",
          "data_type": "numeric(16,4)",
          "meta": {},
          "tests": [],
          "is_nullable": "YES",
          "ordinal_position": 2
        }
      ]
    },
    {
      "unique_id": "model.test_project.enriched_quotes",
      "name": "enriched_quotes",
      "resource_type": "model",
      "description": "Enriched quotes with scores",
      "database": "test_db",
      "schema": "mart",
      "materialized": "table",
      "tags": ["mart"],
      "upstream_dependencies": ["model.test_project.raw_quotes"],
      "path": "mart/enriched_quotes.sql",
      "row_count": "950",
      "size_mb": 12,
      "columns": [
        {
          "name": "quote_id",
          "description": "Unique quote identifier",
          "data_type": "varchar(36)",
          "meta": {},
          "tests": ["not_null"],
          "is_nullable": "NO",
          "ordinal_position": 1
        },
        {
          "name": "risk_score",
          "description": "Computed risk score",
          "data_type": "numeric(5,2)",
          "meta": {},
          "tests": [],
          "is_nullable": "YES",
          "ordinal_position": 2
        }
      ]
    }
  ]
}
```
