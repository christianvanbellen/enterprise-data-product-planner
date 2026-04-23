# Phase 1 — Data Contracts

Phase 1 produces a `CanonicalBundle` — a typed, immutable container that is the
**only** artefact passed from the ingestion layer to the graph compiler.
All downstream phases (2–5) read from the bundle or from files it produces.
Adapters never write to the graph directly.

All contracts are Pydantic v2 frozen models. Fields are validated on construction;
mutation after construction raises `ValidationError`.

---

## CanonicalBundle

**Module:** `ingestion/contracts/bundle.py`

The top-level container. Produced by `IngestionPipeline.run()`.

| Field | Type | Description |
|-------|------|-------------|
| `assets` | `List[CanonicalAsset]` | All discovered data assets (dbt models, source tables) |
| `columns` | `List[CanonicalColumn]` | All columns belonging to the assets above |
| `lineage_edges` | `List[CanonicalLineageEdge]` | Explicit upstream dependency edges |
| `business_terms` | `List[CanonicalBusinessTerm]` | Business glossary terms from the conformed schema |
| `metadata` | `Dict[str, Any]` | Pipeline run metadata (timestamps, adapter versions) |

Key methods:

```python
bundle.merge(other: CanonicalBundle) -> CanonicalBundle
# Returns a new bundle concatenating all list fields.
# Does NOT deduplicate — collision detection is a deferred item.

bundle.to_json(path: Path) -> None
# Writes the bundle to an indented JSON file.

CanonicalBundle.from_json(path: Path) -> CanonicalBundle
# Loads and validates a bundle from a JSON file.

bundle.summary() -> str
# Returns a human-readable summary (asset/column/edge/term counts).
```

---

## CanonicalAsset

**Module:** `ingestion/contracts/asset.py`

Represents a single data asset: a dbt model, source table, or view.

| Field | Type | Description |
|-------|------|-------------|
| `internal_id` | `str` | Stable hash ID (`asset_` + sha256[:16] of source key) |
| `name` | `str` | Raw asset name from the source system |
| `normalized_name` | `str` | Snake-case lowercased name |
| `asset_type` | `str` | `"table"`, `"view"`, `"incremental"`, `"source"`, etc. |
| `schema_name` | `str` | Logical container (dbt schema) |
| `database` | `Optional[str]` | Target database |
| `materialization` | `Optional[str]` | `"table"`, `"view"`, `"incremental"`, `"ephemeral"` |
| `row_count` | `Optional[int]` | Last-known row count |
| `path` | `Optional[str]` | SQL file path relative to project root |
| `tags` | `List[str]` | dbt tags (used for product line tagging in Phase 4) |
| `description` | `Optional[str]` | Asset-level documentation |
| `domain_candidates` | `List[str]` | Inferred domain labels from keyword scan |
| `grain_keys` | `List[str]` | Columns identified as grain keys (identifiers) |
| `is_enabled` | `bool` | Whether the dbt model is enabled |
| `tag_dimensions` | `Dict[str, List[str]]` | Generalised per-dimension tag classification derived from dbt tags. Keys are dimension names registered in `ontology/tag_mappings.yaml` (e.g. `lineage_layer`, `product_line`). Values are lists of mapped tag values, in tag order, deduplicated. Example: `{"lineage_layer": ["historic_exchange", "conformed_bookends"], "product_line": ["directors_and_officers"]}`. Dimensions with no matching tags are omitted — an empty dict means no dbt tags matched any registered dimension. |
| `upstream_dependents` | `int` | Count of DEPENDS_ON edges pointing to this asset (populated in Phase 2) |
| `version_hash` | `str` | Stable hash of the source entity JSON |
| `provenance` | `Provenance` | Source system attribution record |

---

## CanonicalColumn

**Module:** `ingestion/contracts/asset.py`

Represents a single column belonging to an asset.

| Field | Type | Description |
|-------|------|-------------|
| `internal_id` | `str` | Stable hash ID (`col_` + sha256[:16]) |
| `asset_internal_id` | `str` | Parent asset's `internal_id` |
| `name` | `str` | Column name |
| `normalized_name` | `str` | Snake-case lowercased name |
| `data_type_family` | `str` | Normalised type: `numeric`, `string`, `timestamp`, `date`, `boolean`, `semi_structured`, `unknown` |
| `raw_data_type` | `Optional[str]` | Original type string from the source |
| `column_role` | `str` | Semantic role: `identifier`, `measure`, `dimension`, `timestamp`, `flag`, `text`, `unknown` |
| `is_nullable` | `Optional[bool]` | Nullability from information schema |
| `ordinal_position` | `Optional[int]` | Column position in the table |
| `description` | `Optional[str]` | Column-level documentation from dbt |
| `semantic_candidates` | `List[str]` | Candidate semantic labels from keyword matching |
| `tests` | `List[str]` | dbt test names attached to this column |
| `meta` | `Dict[str, Any]` | Raw dbt `meta` block pass-through |
| `version_hash` | `str` | Hash of column content |
| `provenance` | `Provenance` | Source system attribution |

---

## CanonicalLineageEdge

**Module:** `ingestion/contracts/lineage.py`

Represents an explicit upstream dependency between two assets.

| Field | Type | Description |
|-------|------|-------------|
| `internal_id` | `str` | Stable hash ID (`edge_` + sha256[:16]) |
| `source_internal_id` | `str` | The asset being depended on (the upstream) |
| `target_internal_id` | `str` | The asset that depends on source (the downstream) |
| `confidence` | `float` | Always `1.0` for explicit dbt upstream_dependencies |
| `relation_type` | `str` | Always `"depends_on"` in Phase 1 |
| `derivation_method` | `str` | `"explicit_metadata"` for dbt lineage |
| `provenance` | `Provenance` | Source system attribution |

Direction semantics: a DEPENDS_ON edge means *target depends on source*. The source
is the upstream foundation; the target is the downstream consumer.

---

## CanonicalBusinessTerm

**Module:** `ingestion/contracts/business.py`

Represents a business concept from the conformed schema — either a top-level
entity group or a field within one.

| Field | Type | Description |
|-------|------|-------------|
| `internal_id` | `str` | Stable hash ID (`term_` + sha256[:16]) |
| `name` | `str` | Term name (e.g. `"policy"`, `"commission"`) |
| `normalized_name` | `str` | Snake-case lowercased name |
| `term_type` | `str` | `"conformed_concept"` for schema-derived terms |
| `parent_term_id` | `Optional[str]` | `None` for top-level groups; parent's `internal_id` for child fields |
| `attributes` | `Dict[str, Any]` | Raw attributes from the conformed schema (e.g. `data_type`, `field_count`) |
| `version_hash` | `str` | Hash of term content |
| `provenance` | `Provenance` | Source system attribution |

The conformed schema produces a two-level hierarchy: parent terms (entity groups
like `policy`, `coverage`, `rate_monitoring`) and child terms (fields within each
group). `ConformedFieldBinder` in Phase 3 uses this hierarchy to bind assets to
entity groups.

**Current entity groups in the bundle:**

| Group | Child term count |
|-------|-----------------|
| `coverage` | 21 |
| `policy` | ~30 |
| `policy_totals` | ~25 |
| `profitability_measures` | ~20 |
| `profitability_measures_totals` | ~20 |
| `rate_monitoring` | 29 |
| `rate_monitoring_totals` | ~28 |

Note: `pricing_component` is **not** currently a registered group. See
`docs/backlog.md` for the remediation.

---

## Provenance

**Module:** `ingestion/contracts/asset.py`

Attached to every canonical contract. Traces each record to its source.

| Field | Type | Description |
|-------|------|-------------|
| `source_system` | `str` | `"dbt"`, `"conformed_schema"`, `"erd"`, etc. |
| `source_type` | `str` | Adapter class name: `"DbtMetadataAdapter"`, `"ConformedSchemaAdapter"` |
| `source_native_id` | `str` | The primary key used in the source system |
| `raw_record_hash` | `str` | Hash of the raw source record |
| `extraction_timestamp_utc` | `str` | ISO-8601 timestamp of extraction |

---

## Normalisation utilities

**Module:** `ingestion/normalisation/`

| Function | Module | Behaviour |
|----------|--------|-----------|
| `normalize_name(s)` | `names.py` | Strips whitespace, converts hyphens to underscores, splits camelCase, lowercases |
| `classify_data_type(raw)` | `dtypes.py` | Maps raw SQL type string → `numeric / boolean / timestamp / date / string / semi_structured / unknown` |
| `infer_column_role(name, dtype, description)` | `roles.py` | Heuristic assignment: `identifier / measure / dimension / timestamp / flag / text / unknown` |
| `stable_hash(*parts)` | `hashing.py` | SHA-256 of `"||".join(parts)`, returns first 16 hex chars |
| `utc_now_iso()` | `hashing.py` | Returns current UTC time as ISO-8601 string |

---

## Adapter inventory

| Adapter | Status | Input | Output |
|---------|--------|-------|--------|
| `DbtMetadataAdapter` | Production | `dbt_metadata_enriched.json` (dbt manifest v12) | Assets, columns, lineage edges, test nodes |
| `ConformedSchemaAdapter` | Production | `conformed_schema.json` (JSON Schema draft-04) | BusinessTerms (entity groups + child fields) |
| `InformationSchemaAdapter` | Stub | `information_schema.columns` export | *(not implemented)* |
| `GlossaryAdapter` | Stub | Custom glossary JSON or dbt catalog.json | *(not implemented)* |
| `ERDAdapter` | Stub | ERD JSON export | *(not implemented)* |
