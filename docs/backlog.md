<!-- Last updated: April 2026 -->
# Backlog

Open items, known limitations, and deferred work. Items are grouped by phase and status.

---

## TAG_TO_ENTITY migrated to tag_mappings.yaml [DONE — April 2026]

Previously the `TAG_TO_ENTITY` dict in `graph/semantic/entity_mapper.py` hardcoded the
product_line → entity mapping (all current values → `line_of_business`). Migrated to a
new `product_line_to_entity` block in `ontology/tag_mappings.yaml` so all tag-derived
mappings now live in one governed file: `tag_to_lineage_layer`, `tag_to_product_line`,
and `product_line_to_entity`.

This closes the governance gap where adding a new product line required edits in two
places (the mapping in tag_mappings.yaml plus the binding in entity_mapper.py).

---

## product_line_segmentation required_tags naming mismatch [FIXED — April 2026]

`ontology/primitives.yaml` declared `required_tags: [eupi, d_o, general_aviation, contingency]`
for the `product_line_segmentation` primitive, but `asset.product_lines` contains the *mapped*
values from `tag_to_product_line` — `european_professional_indemnity`, `directors_and_officers`,
etc. The set intersection in `CapabilityPrimitiveExtractor` therefore silently dropped all
`eupi`- and `d_o`-tagged assets; `general_aviation` and `contingency` happened to match only
because they are self-mapped.

Impact before fix: the primitive reported 44 supporting assets when it should have matched
78–89. `product_line_performance_dashboard` carried an understated asset count in its spec.

Fixed by updating `required_tags` to use the mapped product_line values and adding the two
previously-excluded lines (`cash_in_transit_and_specie`, `digital_platform`) so the
segmentation view covers the full product portfolio.

After fix: 89 supporting assets (every product-line-tagged asset in the warehouse).

---

## Lineage-layer tag loss [FIXED — April 2026]

Previously `CanonicalAsset.lineage_layer` was `Optional[str]` populated by first-match over
the `tag_to_lineage_layer` mapping. On the Liberty warehouse 205/207 assets carry two
layer-relevant tags (typically a pipeline-stage tag like `hx`/`ll`/`gen2` paired with a
conformance-grade tag like `bookends`/`semi_conformed`), so the first-match rule was
silently discarding the secondary signal on 99% of assets.

Fixed by replacing the field with `lineage_layers: List[str]` containing every tag that
maps to a known layer, in tag order, deduplicated. `_infer_table_type()` Signal 1 now scans
the whole list against its `_LAYER_TO_TYPE` mapping rather than only the first entry.

Impact on graph coverage:
- 5 distinct lineage layers present before → 7 distinct layers present after
- Previously-lost signals now captured: `semi_conformed_mart` (136 assets), `source_table`
  (41 assets), `conformed_bookends` (25 assets)

Impact on analytical output:
- 40 assets previously classified by composition signal now classify as `source`/`snapshot`
  via Signal 1. Specifically the 39 `(ll, source)` pairs and 1 `(ll, raw)` pair.
- This feeds through to `DataRequisite.table_type` and `JoinAssessment.join_direction`
  for those assets.

Not yet built on top of this: per-layer trust weighting in Phase 3 binding, conformance-
based primary source selection in Phase 5, or a Phase 6 "conformance debt" report. Those
become possible now that the signal is preserved.

---

## Schema nodes materialized [FIXED — April 2026]

Previously `CONTAINS` edge source IDs (`schema_*`) were documented as "virtual" — they
appeared in `edges.json` but had no corresponding entries in `nodes.json`, leaving 207
dangling edge references that no graph query could resolve.

Fixed by emitting one `Schema` node per unique `(database, schema_name)` pair during
Phase 2 compilation. The new `Schema` label carries `schema_id`, `name`, `database`, and
`asset_count` properties. Total node count grew from 3,165 to 3,172 (7 new Schema nodes on
the Liberty warehouse). `structural.schema_node` added as a new rule_id.

Database nodes were deliberately **not** added — all 207 assets live in one database
(`mart_lii_hx_rating_db`), so a Database node with a single `CONTAINS → Schema` fan-out
would add visual noise without informational value. If the system later ingests from a
multi-database warehouse, a Database node layer can be introduced with the same shape.

---

## Python configuration constants migrated to YAML [DONE — April 2026]

Six editorial configuration constants previously hardcoded in Python have been migrated
to ontology YAML files, making them editable without touching code:

- `ENTITY_GROUPS` + `OVERLAP_THRESHOLD` → `ontology/entity_groups.yaml`
- `DOMAIN_KEYWORDS` → `ontology/domain_keywords.yaml`
- `SEMANTIC_MAP` → `ontology/semantic_map.yaml`
- `GRAIN_KEY_CANDIDATES` → `ontology/grain_keys.yaml`
- `PRIMITIVE_DEFINITIONS` → `ontology/primitives.yaml`
- `_DELIVERY` → `ontology/delivery_heuristics.yaml`
- `_INITIATIVE_SCORES` merged into `ontology/initiative_research.yaml` (collapsed
  two-file split into one complete initiative definition per entry)

Only `_SYSTEM_PROMPT` in `renderer.py` remains in Python — it contains sentinel logic
and prompt structure tightly coupled to the renderer code.

See `docs/inputs.md` for the complete configuration reference.

---

## SpecLog deduplication bug [FIXED — April 2026]

`index.json` was appending a new entry on every run regardless of whether the initiative
already existed in the log. The spec file on disk was correctly overwritten (stable
`spec_id` hash ensures idempotent writes), but the index grew unboundedly. A second full
run of Phase 5 produced 38 entries instead of 19.

**Root cause:** `_upsert_index()` deduplicated by `spec_id`, which changes when the graph
is rebuilt (new `build_id` → new `spec_id`). Re-running with the same graph build did not
add duplicates, but the typical workflow (re-run Phase 4 then Phase 5) changed the
`spec_id` for every initiative, each producing a new appended entry.

**Fix:** Restructured `output/spec_log/` to use initiative-named directories with
`current.json` / `current.md` stable pointers and versioned history files
(`v{N}_{date}_{hash8}.json`). `_upsert_index()` now deduplicates by `initiative_id` and
updates the entry in place. The index always contains exactly one entry per initiative.

**Test coverage:** `test_index_no_duplicate_initiative_ids_after_multiple_writes`,
`test_migrate_deduplicates_index`, and related new tests in `tests/spec/test_spec_log.py`.

---

## Phase 1 — Deferred items

- **Configurable field-mapping layer** — adapters have hardcoded source→canonical field
  mappings. A Level 3 flexibility layer would let operators remap fields via config
  without modifying adapter code.

- **Collision detection and dedupe on bundle merge** — `CanonicalBundle.merge()` does
  not detect duplicate `internal_id` values. Assets or columns from two adapters with
  matching IDs will both appear in the merged bundle without warning.

- **Validation stage between `detect()` and `parse()`** — `detect()` checks required keys
  but there is no intermediate validation of field types, value ranges, or cross-field
  consistency before `parse_file()` runs.

- **ConformedSchemaAdapter: formal semantic tree spec** — the adapter recursively walks
  any nested dict/list structure but there is no documented or validated shape for the
  conformed schema JSON input.

- **InformationSchemaAdapter: Redshift/Postgres `information_schema` support** — stub only.
  Intended to ingest `information_schema.columns` query exports to supplement or replace
  column type data where dbt metadata is absent.

- **GlossaryAdapter: dbt `catalog.json` or custom glossary JSON support** — stub only.
  Intended to produce `CanonicalBusinessTerm` objects from a business glossary or dbt
  catalog file.

- **ERDAdapter: JSON ERD export support** — stub only. Intended to ingest entity-relationship
  diagram exports as additional lineage and relationship evidence.

- **`source_system_name` wiring from `PipelineConfig` into adapter provenance** —
  `PipelineConfig.source_system_name` is captured but not passed through to individual
  adapters. Adapter provenance always uses hardcoded source system names.

---

## Phase 3 — Deferred items

- **Distribution domain keyword gap** — `facility`, `master_umr_lineslip_binder`,
  `coverholder`, `mga`, `binder`, `lineslip` are present in wide assets (`hx_do_quote`,
  85 columns) but absent from the `distribution` keyword list. Adding them would
  significantly raise distribution's apparent footprint in the graph. Requires review
  of what `facility` means in context (Lloyd's facility vs. a building) before committing.

---

## Phase 5 — Known limitations and deferred items

- **LLM non-determinism** — `SpecRenderer` calls `claude-sonnet-4-20250514` without a
  fixed random seed. Two renders of the same `SpecDocument` will produce similar but not
  identical markdown. The spec log stores the rendered text verbatim; callers who need
  reproducibility should use the cached version rather than forcing a re-render.

- **Column enrichment limited to top-5 assets** — Only the five supporting assets with the
  highest `upstream_dependents` count receive full `ColumnDetail` records. Assets ranked
  6+ are present as `AssetDetail` with an empty `columns` list.

- **Grain join paths not routed through lineage** — `JoinPath` is computed from shared
  `grain_keys` on asset metadata, not from actual DEPENDS_ON lineage paths. Two assets may
  share a grain key without a direct join being possible in the current pipeline topology.

- **No spec diffing** — When the graph is rebuilt (new `build_id`), a new `spec_id` is
  generated and the previous spec is orphaned in the log. A diff view showing what changed
  between spec versions is deferred.

- **Renderer error recovery is silent** — If the LLM call fails, `rendered` is set to `""`
  and `render_error` is recorded in the log entry. The pipeline does not retry; callers
  must use `--force-render` on a subsequent run.

- **No multi-initiative joint spec** — Initiatives in the same `composes_with` group could
  share a combined spec describing the joint architecture. Currently each initiative receives
  an independent spec.

- **Gap briefs do not include enablement paths** — For `needs_foundational_work` initiatives,
  the spec does not yet describe the sequence of gap-closes that would promote the initiative
  to `ready_with_enablement`. Deferred pending a dependency-resolution layer on top of the
  gap graph.

---

## Schema gaps

### `pricing_component` schema group absent from conformed schema [OPEN]

**Root cause:** The `pricing_decomposition` primitive requires the `pricing_component`
entity group. That group is not registered in `ENTITY_GROUPS` in
`graph/semantic/conformed_binder.py`, and no `pricing_component` parent business term
exists in the conformed schema. The five required columns (`commission`, `modtech_gnwp`,
`sold_gnwp`, `tech_elc`, `tech_gnwp`) are currently parented to `rate_monitoring` and
`coverage` groups.

**What the pipeline detects:** All five required columns exist in `ll_quote_policy_detail`
and five related assets by exact name, with full descriptions. Column coverage is 5/5.
Entity coverage is 0/1.

**Result in the graph:** `pricing_decomposition` appears as an **inferred** primitive
(amber hexagon in the graph explorer) — detected analytically, not confirmed by schema.
`supporting_asset_ids` is empty.

**Impact:** Seven initiatives show `pricing_decomposition` as a missing or inferred
primitive. Data requisites for `underwriting_decision_support`, `renewal_prioritisation`,
and `broker_performance_intelligence` are missing pricing dimension columns they would
otherwise surface.

**Remediation — Option A (preferred):**
Add `pricing_component` as a parent business term to the conformed schema YAML, with
child terms: `commission`, `modtech_gnwp`, `sold_gnwp`, `tech_elc`, `tech_gnwp`.
Then add `"pricing_component"` to `ENTITY_GROUPS` in `graph/semantic/conformed_binder.py`.
Re-run Phase 1 to regenerate the bundle.

**Remediation — Option B (interim):**
Add a `SyntheticTermInjector` post-processing step after Phase 1 that injects
`pricing_component` as a synthetic parent term and re-parents the five columns. No schema
change required, but this diverges from the source schema.

**Demo talking point:** "The pipeline detected this capability exists in your schema but
isn't formally registered as a data group. That's a one-line fix on the data engineering
side and it unlocks six more initiatives immediately."

See `ARCHITECTURE.md — The pricing_decomposition gap` for the full cross-cutting analysis.

---

### `sold_to_plan` absent from warehouse [KNOWN GAP]

The `profitability_decomposition` primitive requires four columns: `sold_to_modtech`,
`modtech_to_tech`, `sold_to_plan`, `target_to_plan`. Three of the four exist in the
warehouse; `sold_to_plan` is absent from all catalogued assets.

**Impact:** `profitability_decomposition` maturity is 3/4 (75%). Direct plan vs. sold
comparison cannot be computed. The `profitability_decomposition_assistant` initiative
carries this as a known column gap. No remediation is possible without a source system
addition.

---

### `hx_rate_monitoring` has no column metadata [KNOWN GAP]

`hx_rate_monitoring` has zero columns catalogued in the dbt metadata. Historical trend
analysis against this asset carries schema-discovery risk — any initiative that references
it cannot verify column availability until the asset is inspected directly.

**Impact:** Rate monitoring initiatives that should use this asset for historical trending
cannot assert column presence. The gap is surfaced in affected specs.

**Remediation:** Add column documentation to `hx_rate_monitoring` in the dbt project.

---

## Data quality gaps

### Test coverage on warehouse assets critically low [KNOWN GAP]

96 dbt tests exist on 2,654 columns — a coverage rate of approximately 0–4% per asset.
Data quality assertions cannot be made with confidence. Initiatives classified as
`ready_now` are ready from a schema perspective; the `low_test_coverage` gap type
flags this in affected specs.

**Remediation:** Add dbt `not_null` and `unique` tests to grain key columns across the
warehouse. Priority: `quote_id` (present in 147 assets), `layer_id`, `pas_id`.

---

## Spec rendering gaps

### `broker_performance_intelligence` empty `minimal_source_assets` [OPEN]

When `minimal_source_assets` is empty (no supporting assets contribute any column with
a non-absent derivation), the renderer outputs "Source: —" in the build contract section.

**Root cause:** This occurs when the primary source asset selection resolves to an
empty string because no supporting assets carry primitive-matched columns.

**Fix:** Render a "not yet determined — run with conformed schema updated" message when
`minimal_source_assets` is empty, rather than attempting to render a build note with an
empty source list. This is a renderer cosmetic fix.

---

### `product_line_performance_dashboard` empty measures [KNOWN GAP]

The `product_line_segmentation` primitive covers 40+ assets, but column-level descriptions
are absent across most product line assets (`eupi_*`, `ll_eupi_*`, `tbl_EU_PI_*`, etc.).
The positive-inclusion filter correctly excludes undescribed columns, leaving the `measures`
section of the spec empty.

**This is not a bug.** The filter is working as designed. Undescribed columns are excluded
to avoid polluting specs with low-quality metadata.

**Fix:** Add dbt column descriptions to product line assets. Once descriptions are present,
the filter will automatically include the columns in the next spec generation run.

---

## Phase 6 — Not implemented [SCOPED]

The architecture alignment report (Phase 6) compares the architect's schema groups against
the analytics capability model and produces a structured enablement backlog.

**Status:** Scoped. Not yet implemented. All required graph data is available.

**Estimated effort:** 1–2 days for JSON + markdown generation. Optional stakeholder
presentation adds another 1 day.

**Blocked on:** Nothing. Phase 6 reads from `output/graph/nodes.json`,
`output/spec_log/index.json`, and `output/bundle.json` — all produced by Phases 1–5.

See `docs/phase6_architecture_alignment.md` for the full design.
