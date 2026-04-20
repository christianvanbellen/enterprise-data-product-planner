# Phase 1 Execution Flow

---

## Inputs

| input_name           | adapter                   | file_path_config_key      | status      |
|----------------------|---------------------------|---------------------------|-------------|
| dbt enriched metadata | `DbtMetadataAdapter`     | `dbt_metadata_path`       | implemented |
| Conformed schema JSON | `ConformedSchemaAdapter` | `conformed_schema_path`   | implemented |
| Information schema    | `InformationSchemaAdapter`| `info_schema_path`        | stub        |
| Business glossary     | `GlossaryAdapter`         | `glossary_path`           | stub        |
| ERD JSON              | `ERDAdapter`              | `erd_path`                | stub        |

---

## Execution sequence

1. Caller constructs `PipelineConfig` with one or more input file paths.
2. Caller calls `IngestionPipeline(config).run()` or `run_and_save(output_path)`.
3. `IngestionPipeline.run()` iterates `adapter_specs` — one entry per configured path.
4. If a path is `None`, the adapter is skipped (`logger.debug` — "no path configured").
5. If a path does not exist on disk, the adapter is skipped (`logger.warning` — "file not found").
6. For each active adapter, `adapter_cls()` is instantiated.
7. If the adapter exposes `detect()`, it is called first; result is logged at `DEBUG`.
8. Any non-fatal `warnings` from `detect()` are emitted at `WARNING` level.
9. `adapter.parse_file(path)` is called; raises `ValueError` if `detect()` returned `compatible=False`.
10. The returned `CanonicalBundle` is merged into `merged` via `CanonicalBundle.merge()`.
11. Adapter output counts are logged at `INFO` level.
12. If the adapter raises `NotImplementedError` (stub adapters), it is skipped with `logger.warning`.
13. Any other exception propagates — the pipeline does not swallow unexpected errors.
14. After all adapters, `merged` (a `CanonicalBundle`) is returned to the caller.
15. `run_and_save()` additionally calls `bundle.to_json(output_path)`, creating parent dirs as needed.

---

## Output: CanonicalBundle

| field           | type                          | populated_by                 | count (golden run) |
|-----------------|-------------------------------|------------------------------|--------------------|
| `assets`        | `list[CanonicalAsset]`        | `DbtMetadataAdapter`         | 207                |
| `columns`       | `list[CanonicalColumn]`       | `DbtMetadataAdapter`         | 2654               |
| `lineage_edges` | `list[CanonicalLineageEdge]`  | `DbtMetadataAdapter`         | 201                |
| `business_terms`| `list[CanonicalBusinessTerm]` | `ConformedSchemaAdapter`     | 0 (no input file)  |

---

## What is NOT in scope for Phase 1

- **Duplicate/collision resolution across adapters** — if two adapters produce an asset
  with the same `internal_id`, `CanonicalBundle.merge()` will include both. No dedup logic exists.
- **Field-mapping configuration** — adapters have hardcoded field mappings. There is no
  configurable mapping layer (e.g. `source_field → canonical_field`).
- **Full schema validation beyond detect()** — `detect()` checks for required keys but
  does not validate field types, value ranges, or cross-field consistency.
- **ConformedSchemaAdapter hardening** — the adapter parses a recursive concept tree
  but has no formal semantic tree spec or validation against the ontology.
- **InformationSchemaAdapter, GlossaryAdapter, ERDAdapter implementation** — all three
  raise `NotImplementedError` and are skipped by the pipeline.
