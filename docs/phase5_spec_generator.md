<!-- Last updated: April 2026 -->
# Phase 5 — Spec Generator

---

## Overview

Phase 5 produces a structured, versioned data-product specification for each initiative
identified in Phase 4. For initiatives that are ready to build (`ready_now` or
`ready_with_enablement`) it emits a **full spec** — a technical brief covering output
schema, grain, join paths, dimensional model contracts, and delivery recommendations.
For initiatives that are blocked (`needs_foundational_work` or `not_currently_feasible`)
it emits a **gap brief** — a concise summary of what must change before the initiative
becomes viable.

Every spec is assembled deterministically from the graph state before the LLM is called.
Re-running Phase 5 with the same graph `build_id` and the same initiative list is
idempotent: the cache is checked first, and a new LLM call is only made when
`--force-render` is set or when no spec exists for the current `(initiative_id,
graph_build_id)` pair.

---

## Architecture

```
CanonicalBundle ──┐
JsonGraphStore  ──┤──► SpecAssembler ──► SpecDocument ──► SpecRenderer ──► rendered_md
OpportunityResult ┘          │                                                  │
                              └─────────────────────────────────────────────────┘
                                                                                │
                                                                          SpecLog.save()
                                                                                │
                                                                      output/spec_log/
                                                                        index.json
                                                                        {spec_id}.json
                                                                        {spec_id}.md
```

| Class | Module | Role |
|-------|--------|------|
| `SpecAssembler` | `graph/spec/assembler.py` | Pure-deterministic. Queries graph and bundle, builds `SpecDocument` Pydantic model. No LLM. |
| `SpecRenderer` | `graph/spec/renderer.py` | Single bounded LLM call. Converts `SpecDocument` to markdown. Never raises. |
| `SpecLog` | `graph/spec/log.py` | Persistent, versioned storage. Writes JSON + markdown per spec, maintains `index.json`. |
| `SpecGenerationPipeline` | `graph/spec/pipeline.py` | Orchestrates the three classes above. Returns `SpecGenerationReport`. |

The CLI entry point is `scripts/run_phase5.py`.

---

## LLM usage policy

- **Only Phase 5 calls an LLM.** Phases 1–4 are fully deterministic.
- The LLM receives only data that is already present in the graph or bundle.
  It cannot access external systems, perform searches, or call other tools.
- The LLM is given a 1,500-token output budget for full specs and an 800-token
  budget for gap briefs. It cannot exceed these limits.
- All LLM output is stored verbatim in the spec log for auditability.
- The LLM call is wrapped in a graceful error handler. If it fails, the
  `SpecDocument` is still saved; the `rendered` field is set to an empty string
  and `render_error` records the exception message.
- Model: `claude-sonnet-4-20250514` (pinned).
- Non-determinism: two renders of the same `SpecDocument` will produce similar but
  not identical markdown. Use the cached version for reproducibility.

### What the LLM receives per call

The LLM prompt contains the full `SpecDocument` JSON plus pre-rendered sentinel blocks:

| Sentinel | Content |
|----------|---------|
| `{{BOM}}` | Bill of materials — all supporting asset names per primitive |
| `{{ASSET_SCHEMATIC}}` | Primitive-to-asset mapping table |
| `{{OUTPUT_STRUCTURE}}` | Grain, dimensions, measures, time columns |
| `{{DATA_REQUISITES}}` | Full `DataRequisite` rendered as a structured table with join guidance |
| `{{JOIN_PATHS}}` | `JoinAssessment` objects rendered as a join safety guide |

### What the LLM writes

Overview paragraph, business objective expansion, key measures narrative, delivery guidance,
composability narrative.

### What the LLM does NOT write

Data requisites, join assessments, readiness evidence, gap chains, primitive maturity scores,
blocker details. These are pre-rendered from graph state and embedded as sentinel blocks.
See `ARCHITECTURE.md` for the design rationale.

### Approximate token counts per call

| Field group | Full spec | Gap brief |
|-------------|-----------|-----------|
| Initiative metadata | ~80 | ~80 |
| Business objective, output type, target users | ~60 | ~60 |
| Available primitives (name, description, maturity, assets) | ~350 | ~100 |
| Column details for top-5 assets | ~300 | — |
| Data requisite and join assessments | ~200 | — |
| Grain join paths | ~80 | — |
| Blockers (gap type + description) | ~60 | ~200 |
| Missing primitives | ~40 | ~150 |
| System prompt | ~120 | ~120 |
| **Total input (approx)** | **~1,290** | **~710** |

---

## SpecDocument schema

### Core fields

| Field | Type | Description |
|-------|------|-------------|
| `spec_id` | `str` | `stable_hash(initiative_id, graph_build_id)` — 16 hex chars |
| `spec_type` | `str` | `"full_spec"` or `"gap_brief"` |
| `initiative_id` | `str` | Snake_case initiative identifier |
| `initiative_name` | `str` | Human-readable name from research artifact |
| `archetype` | `str` | Initiative archetype (monitoring, decision_support, …) |
| `readiness` | `str` | ready_now / ready_with_enablement / needs_foundational_work / not_currently_feasible |
| `composite_score` | `float` | Opportunity composite score |
| `business_value_score` | `float` | Editorial business value constant from YAML |
| `implementation_effort_score` | `float` | Editorial effort constant from YAML |
| `business_objective` | `str` | One-sentence purpose statement |
| `output_type` | `str` | monitoring_dashboard / decision_support / ai_agent / … |
| `target_users` | `List[str]` | Intended consumers |
| `composes_with` | `List[str]` | Initiative IDs sharing ≥ 2 primitives |
| `available_primitives` | `List[PrimitiveDetail]` | Primitives confirmed in the warehouse |
| `missing_primitives` | `List[Any]` | String IDs absent from warehouse; dicts for virtual YAML gaps |
| `blockers` | `List[BlockerDetail]` | Gap type + description for each blocker |
| `grain_join_paths` | `List[JoinPath]` | Pairs of supporting assets sharing ≥ 2 grain keys |
| `primitive_to_assets` | `Dict[str, List[str]]` | Primitive ID → supporting asset names |
| `all_supporting_asset_names` | `List[str]` | Sorted union of all supporting asset names |
| `feasibility_rationale` | `Optional[str]` | Research YAML feasibility note (not_currently_feasible initiatives) |
| `output_structure` | `Optional[OutputStructure]` | Pre-computed output table schema (full specs only) |
| `data_requisite` | `Optional[DataRequisite]` | Pre-computed build contract (full specs only) |
| `graph_build_id` | `str` | The Phase 4 build_id this spec was assembled from |
| `assembled_at_utc` | `str` | ISO-8601 assembly timestamp |

### PrimitiveDetail

| Field | Type |
|-------|------|
| `primitive_id` | `str` |
| `primitive_name` | `str` |
| `description` | `str` |
| `maturity_score` | `float` |
| `matched_columns` | `List[str]` |
| `missing_columns` | `List[str]` |
| `supporting_assets` | `List[AssetDetail]` |

### AssetDetail

| Field | Type | Notes |
|-------|------|-------|
| `asset_id` | `str` | `internal_id` from CanonicalAsset |
| `name` | `str` | |
| `description` | `Optional[str]` | |
| `domain_candidates` | `List[str]` | |
| `grain_keys` | `List[str]` | |
| `upstream_dependents` | `int` | Count of DEPENDS_ON edges pointing to this asset |
| `columns` | `List[ColumnDetail]` | Populated only for top-5 assets by `upstream_dependents` |

### ColumnDetail

| Field | Type |
|-------|------|
| `name` | `str` |
| `data_type_family` | `str` |
| `column_role` | `str` |
| `description` | `Optional[str]` |
| `tests` | `List[str]` |
| `semantic_candidates` | `List[str]` |

### JoinPath

| Field | Type |
|-------|------|
| `left_asset` | `str` |
| `right_asset` | `str` |
| `shared_grain_keys` | `List[str]` |

Note: `JoinPath` is computed from shared `grain_keys` metadata, not from actual DEPENDS_ON
lineage paths. Two assets may share a grain key without a direct join being possible in the
current pipeline topology. See `docs/backlog.md` for the deferred improvement.

---

## OutputStructure schema

`OutputStructure` is pre-computed by `SpecAssembler._build_output_structure()` for all
full specs. It describes the output table's schema before the LLM is called.

| Field | Type | Description |
|-------|------|-------------|
| `structure_type` | `str` | `monitoring_dashboard` / `decision_support` / `analytics_product` / `ai_agent` / `gap_brief` |
| `primary_grain` | `List[str]` | Output grain key columns |
| `grain_description` | `str` | Human-readable grain description (e.g. "one row per quote") |
| `primary_source_asset` | `str` | Driving fact/source asset name (two-stage ranked) |
| `summary_source_asset` | `Optional[str]` | `_totals` or `_total_our_share_usd` asset if present |
| `dimensions` | `List[OutputColumn]` | Categorical columns (up to 6, positive-inclusion filtered) |
| `measures` | `List[OutputColumn]` | Numeric columns (up to 8, positive-inclusion filtered) |
| `time_columns` | `List[OutputColumn]` | Timestamp columns, excluding `_pdm_last_update_timestamp` |
| `pipeline_timestamp` | `Optional[str]` | `"_pdm_last_update_timestamp"` if present in supporting assets |
| `target_variable` | `Optional[TargetVariable]` | Decision-support archetypes only: outcome column or gap |
| `feature_columns` | `Optional[List[OutputColumn]]` | Prediction archetypes only |
| `history_note` | `Optional[str]` | Set to fixed string if any supporting asset starts with `hx_` |

`_pdm_last_update_timestamp` is separated into `pipeline_timestamp` rather than appearing
in `time_columns` because it is a pipeline operational timestamp, not a business event time.

### Primary source asset selection

Assets are ranked using a two-stage key: (preference_tier, -upstream_dependents).

- Tier 0 (preferred): names containing `_quote`, `_policy`, `_detail`, `_summary`,
  `_experience`, `_monitoring`, `_performance`, `_measures`
- Tier 1 (neutral): everything else
- Tier 2 (deprioritised): names containing `_rating`, `_factor`, `_load`, `_war_`,
  `_ops_`, `_inputs`, `_modifiers`

Within the preferred-tier candidates, the asset with the most `upstream_dependents` that
also carries at least one primitive-matched column is selected as primary.

### Column positive-inclusion filter

A column is included in `dimensions`, `measures`, or `time_columns` only if it satisfies
at least one of three signals:

| Signal | Condition |
|--------|-----------|
| A | Non-empty `description` |
| B | At least one `semantic_candidate` label |
| C | Name appears in the primitive's `matched_columns` set |

Columns with no signal are excluded. This is a deliberate design choice — undescribed,
unsemantic, unmatched columns add noise without analytical value. The fix is to add dbt
column descriptions, not to relax the filter.

**Priority within included columns:**
- Priority 0: Signal A AND Signal C (described + primitive-matched)
- Priority 1: Signal A only (has description)
- Priority 2: Signal B or Signal C without description

---

## DataRequisite schema

`DataRequisite` is a build contract pre-computed by `SpecAssembler._build_data_requisite()`
for all full specs. It is embedded in the spec as the `{{DATA_REQUISITES}}` sentinel block
and stored verbatim in `SpecDocument.data_requisite`.

| Field | Type | Description |
|-------|------|-------------|
| `initiative_id` | `str` | |
| `canonical_table_name` | `str` | Output table name: `{initiative_id}_dashboard` or `{initiative_id}_mart` |
| `grain_description` | `str` | Human-readable grain description |
| `grain_keys` | `List[str]` | Output grain key columns |
| `table_type` | `str` | Inferred type of primary source asset (see dimensional role inference) |
| `primary_source_asset` | `str` | Driving source asset |
| `minimal_source_assets` | `List[str]` | Sorted set of all assets contributing at least one column |
| `build_complexity` | `str` | `single_table` / `simple_join` / `complex_join` |
| `build_notes` | `str` | Auto-generated one-sentence build instruction |
| `columns` | `List[DataRequisiteColumn]` | Identifiers first, then join dimensions, then output structure columns |
| `source_asset_types` | `Dict[str, str]` | Inferred type per source asset |
| `source_asset_grains` | `Dict[str, List[str]]` | Grain keys per source asset |
| `join_assessments` | `List[JoinAssessment]` | One per unique (primary, right_asset) pair |

### DataRequisiteColumn

| Field | Type | Values |
|-------|------|--------|
| `column_name` | `str` | |
| `description` | `Optional[str]` | From dbt column documentation |
| `data_type` | `str` | `float` / `string` / `date` / `boolean` |
| `role` | `str` | `identifier` / `dimension` / `measure` / `time` |
| `source_asset` | `Optional[str]` | Warehouse asset that provides this column |
| `source_column` | `Optional[str]` | Column name in source asset |
| `derivation` | `str` | `direct_read` / `join` / `absent` |
| `join_key` | `Optional[str]` | Join column when `derivation == "join"` |

Column assembly order:
1. Grain key identifier columns (from primary source asset)
2. Dimension columns from joinable supporting assets (grain-compatible, column-overlap)
3. Dimension columns from cross-bundle dimension scan (all bundle assets, type=dimension, upstream_dependents>1)
4. Remaining columns from `output_structure.dimensions + measures + time_columns`

### JoinAssessment

| Field | Type | Values |
|-------|------|--------|
| `left_asset` | `str` | Always the primary source asset |
| `left_type` | `str` | Inferred table type |
| `right_asset` | `str` | Joined asset |
| `right_type` | `str` | Inferred table type |
| `join_key` | `str` | Column used for the join |
| `join_direction` | `str` | `fact_to_dimension` / `fact_to_fact` / `fact_to_snapshot` / `fact_to_bridge` / `other` |
| `join_safety` | `str` | `safe` / `risky` / `aggregation_required` |
| `grain_match` | `bool` | True if both assets share identical grain key sets |
| `aggregation_needed` | `bool` | True if right asset grain is finer than left asset grain |
| `aggregation_note` | `Optional[str]` | Set when `aggregation_needed` is True |
| `safety_note` | `str` | One sentence explaining the safety classification |

Safety classification rules:
- `fact_to_dimension` + `grain_match=True` → safe ("Dimension join on shared grain")
- `fact_to_dimension` + `grain_match=False` → risky ("Dimension grain finer than fact grain")
- `fact_to_fact` → risky ("Verify grain alignment and aggregation strategy")
- `fact_to_snapshot` → safe ("Filter by pas_id or date range to avoid row multiplication")
- `fact_to_bridge` → risky ("May produce multiple rows per fact row")
- all others → risky ("Verify grain alignment before building ETL")

---

## Dimensional role inference

`SpecAssembler._infer_table_type()` classifies every warehouse asset into one of
`fact | dimension | bridge | snapshot | source | unknown`. The full algorithm is documented
in `ARCHITECTURE.md`. Brief summary of the four signals:

1. **Grain key count ≥ 4** → bridge (checked first, definitive)
2. **`lineage_layer`** → source_table/raw_layer → source; historic_exchange → snapshot (definitive for those layers)
3. **gen2_mart + dim_ratio < 0.6** → override composition signal to fact (mart refinement)
4. **Column composition** → fact_ratio > 0.55 → fact; dim_ratio > 0.45 → dimension

Returns `unknown` if the asset has zero columns or zero grain keys.

---

## Output structure

### Full spec contents

- Initiative metadata (id, name, archetype, readiness, composite score)
- Business objective, output type, target users
- All available primitives with maturity scores, matched/missing columns, supporting assets
- Column details for top-5 assets by `upstream_dependents`
- `OutputStructure` (grain, primary source, dimensions, measures, time columns)
- `DataRequisite` (build contract with join assessments)
- Grain join paths
- LLM-rendered narrative (overview, business objective expansion, key measures, delivery guidance)

### Gap brief contents

- Initiative metadata
- Missing primitives and blockers
- Feasibility rationale (for not_currently_feasible initiatives)
- LLM-rendered narrative (what must change before the initiative becomes viable)
- No `OutputStructure` or `DataRequisite` (structure_type is `gap_brief`)

---

## Spec log structure

All specs are written to `output/spec_log/`.

```
output/spec_log/
├── index.json               # Array of SpecLogEntry objects
├── {spec_id}.json           # Serialised SpecDocument
└── {spec_id}.md             # LLM-rendered markdown (empty string if --no-render)
```

`index.json` entries:

```json
{
  "spec_id": "a3f1c2b4d5e6f7a8",
  "initiative_id": "underwriting_decision_support",
  "spec_type": "full_spec",
  "readiness": "ready_now",
  "graph_build_id": "opp_abc123",
  "assembled_at_utc": "2026-04-15T10:23:44+00:00",
  "rendered": true
}
```

`spec_id` is deterministic: `stable_hash(initiative_id, graph_build_id)`. Re-running with
the same graph `build_id` writes to the same file path — the log never accumulates stale
duplicates unless the graph is rebuilt.

---

## Cost model

Model: `claude-sonnet-4-20250514` — $3.00 / 1M input tokens, $15.00 / 1M output tokens.

A full run over all 19 initiatives:

| Batch | Count | Approx cost/call | Batch cost |
|-------|-------|-----------------|------------|
| full_spec (ready_now + ready_with_enablement) | 11 | ~$0.026 | ~$0.29 |
| gap_brief (needs_foundational_work + not_feasible) | 8 | ~$0.014 | ~$0.11 |
| **Total** | **19** | | **~$0.37–$0.42** |

Actual spend varies by ±20% depending on graph state. The caching layer means a re-run
over unchanged initiatives costs $0.00 unless `--force-render` is passed.

---

## CLI usage and cost controls

```bash
# Generate specs for all 19 initiatives (uses cache — free if already generated)
uv run python scripts/run_phase5.py \
  --bundle output/bundle.json \
  --graph output/graph \
  --initiatives all

# Generate specs only for ready_now initiatives (cheapest useful subset)
uv run python scripts/run_phase5.py \
  --bundle output/bundle.json \
  --graph output/graph \
  --initiatives ready_now

# Generate specs for specific initiatives
uv run python scripts/run_phase5.py \
  --bundle output/bundle.json \
  --graph output/graph \
  --initiatives underwriting_decision_support,pricing_adequacy_monitoring

# Assemble without calling the LLM (free — saves SpecDocument JSON only)
uv run python scripts/run_phase5.py \
  --bundle output/bundle.json \
  --graph output/graph \
  --initiatives all \
  --no-render

# Force re-render even if a cached spec exists (incurs LLM cost)
uv run python scripts/run_phase5.py \
  --bundle output/bundle.json \
  --graph output/graph \
  --initiatives all \
  --force-render
```

Cost controls:
- Default: `--render` is on but the cache is checked first. Each `(initiative_id,
  graph_build_id)` pair is rendered at most once unless `--force-render` is passed.
- Use `--no-render` to populate the spec log with structured JSON only (zero LLM cost).
- Use `--initiatives ready_now` to limit spend to the highest-value subset.

---

## Column-level enrichment rationale

The top-5 assets by `upstream_dependents` are selected for full column detail because:

- High `upstream_dependents` correlates with assets that sit at the semantic core of a
  primitive — many downstream models depend on them, suggesting they carry the canonical
  representation of the domain concept.
- Surfacing column names, data types, and roles gives the LLM enough signal to reason
  about grain alignment, join feasibility, and type compatibility without needing to
  inspect the full graph.
- Limiting to 5 assets keeps the LLM context manageable while covering the analytically
  most significant assets.
- `ColumnDetail.description` is the single highest-leverage LLM input — it allows the
  LLM to distinguish, for example, a `rate` column that is a pricing rate from one that
  is an exchange rate.

Assets ranked 6+ are present in `AssetDetail` form with an empty `columns` list. A
future pass could populate all assets or allow a configurable threshold.

---

## Known limitations

- **LLM non-determinism.** Two renders of the same `SpecDocument` produce similar but not
  identical markdown. Use the cached version for reproducibility.

- **Grain join paths not routed through lineage.** `JoinPath` is computed from shared
  `grain_keys` on asset metadata, not from actual DEPENDS_ON lineage paths. Two assets may
  share a grain key without a direct join being possible in the current pipeline topology.

- **No spec diffing.** When the graph is rebuilt (new `build_id`), a new `spec_id` is
  generated and the previous spec is orphaned in the log. A diff view is deferred.

- **Renderer error recovery is silent.** If the LLM call fails, `rendered` is set to `""`
  and `render_error` is recorded. The pipeline does not retry; use `--force-render`.

- **`broker_performance_intelligence` may show empty source.** When `minimal_source_assets`
  is empty (no supporting assets), the renderer outputs "Source: —". See `docs/backlog.md`.

- **No multi-initiative joint spec.** Initiatives in the same `composes_with` group each
  receive an independent spec. A combined spec for the joint architecture is deferred.

- **Gap briefs do not include enablement paths.** For `needs_foundational_work` initiatives,
  the spec does not yet describe the sequence of gap-closes that would promote the initiative
  to `ready_with_enablement`. Deferred pending a dependency-resolution layer.
