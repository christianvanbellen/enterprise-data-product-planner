<!-- Last updated: April 2026 -->
# Architecture

Cross-cutting design decisions for the Enterprise Data Product Planner. This document
captures the reasoning behind choices that span multiple phases — things that a reader
of a single phase doc would not encounter but that a contributor needs to understand to
make coherent changes.

For phase-specific detail see the docs/ directory. For running the system see README.md.

---

## Design principles

Five principles governed every architectural decision in this system.

**1. Determinism before LLM.**
Phases 1–4 are fully deterministic. Zero model calls in ingestion, structural compilation,
semantic enrichment, or opportunity analysis. The graph state — which primitives are
confirmed, which initiatives are ready, which gaps block which initiatives — is computed
entirely from the data and the YAML ontology files. The LLM enters only in Phase 5 as a
narrator, not as a reasoner. It writes prose around structural facts that are pre-computed.
The graph never changes based on how a question is phrased.

*Why:* Reproducibility and auditability. A data architect should be able to re-run the
pipeline on Monday and get the same portfolio ranking they got on Friday.

**2. Positive inclusion over negative exclusion.**
Output columns in specs and data requisites are included only if they carry at least one
semantic signal: a non-empty description, a `semantic_candidate` label, or membership in
a primitive's `matched_columns` set. There are no blocklists of "bad" column names.

*Why:* Blocklists require maintenance and produce surprising omissions. A positive signal
requirement is self-maintaining — adding a dbt column description to a previously excluded
column immediately promotes it into relevant specs.

**3. IDs are hashes, not UUIDs.**
Every object in the system — assets, columns, edges, specs — has a stable ID derived from
`stable_hash(*parts)`, which is the first 16 hex chars of SHA-256 of the `"||"`-joined
parts. Re-ingesting the same file produces identical IDs.

*Why:* Determinism across reruns. Downstream graph operations can upsert safely. A UUID
would require a registry to avoid ID drift between runs.

**4. Schema-grounded, not pattern-matched.**
Dimensional role inference (`_infer_table_type()`) uses four graph signals: `lineage_layer`,
column composition ratios, grain key count, and mart-layer refinement. It never inspects
asset names for patterns like `_dim_` or `_fact_`.

*Why:* Name-based inference breaks on any warehouse that doesn't follow a naming
convention. The four signals used here are properties of the graph itself and generalise
to any dbt project.

**5. The spec is a contract.**
The `data_requisite` section of every full spec is pre-rendered from deterministic graph
state by `SpecAssembler._build_data_requisite()`. It names source assets, output columns,
join keys, join safety classifications, and build complexity. The LLM does not write any
of this. It answers a structural question; the answer must not vary between runs.

---

## The two-model architecture

Phase 3 and Phase 4 each have a model of the warehouse, and they are deliberately separate.

**ConformedFieldBinder (Phase 3)** maps warehouse assets to entity groups defined by the
warehouse architect in the conformed schema. The groups (`coverage`, `policy`, `policy_totals`,
`profitability_measures`, `rate_monitoring`) are the architect's classification of business
concepts. `ConformedFieldBinder` uses column-name overlap (threshold 0.5) to bind assets
to these groups. The binding populates `BusinessEntityNode` objects in the semantic graph.

The full current list is in `ENTITY_GROUPS` in `graph/semantic/conformed_binder.py`:

```python
ENTITY_GROUPS = ["coverage", "policy", "policy_totals",
                 "profitability_measures", "rate_monitoring"]
```

**CapabilityPrimitiveExtractor (Phase 4)** maps entity groups to analytical capability
primitives defined by the analytics team in `PRIMITIVE_DEFINITIONS`
(`graph/opportunity/primitive_extractor.py`). The nine primitives represent analytical
building blocks grounded in insurance analytics literature. Each primitive specifies which
entity groups it requires — it looks up which assets are bound to those groups and uses
those as its supporting assets.

These are deliberately separate because the architect's schema grouping and the analytics
team's capability model are different views of the same data. The architect groups columns
by operational meaning (`rate_monitoring` = columns related to the rate monitoring process).
The analytics team groups capabilities by analytical function (`rate_change_monitoring` =
the ability to compute risk-adjusted rate change across renewals). The two vocabularies do
not have to align one-to-one.

**The gap between them is the primary output of Phase 6.** When a capability primitive
requires an entity group that isn't registered in `ENTITY_GROUPS`, the pipeline surfaces
this as an inferred primitive (amber hexagon in the graph explorer) rather than failing
silently. The `pricing_decomposition` case is the canonical example — see below.

---

## Dimensional role inference

`SpecAssembler._infer_table_type()` (`graph/spec/assembler.py:548`) classifies every
warehouse asset into one of `fact | dimension | bridge | snapshot | source | unknown`
using four graph signals in priority order. No asset name patterns are used.

**Signal 3 — grain key count (checked first, definitive for bridges)**
If an asset has four or more grain keys, it is classified as `bridge`. A bridge table
is defined by its multi-key grain, not by its name. This signal is checked before all
others because it is unambiguous.

**Signal 1 — lineage layer (definitive for source/snapshot)**
`lineage_layer` is a tag derived from the dbt `tags` field during Phase 1 ingestion.
Direct mappings:
- `source_table` → `source`
- `raw_layer` → `source`
- `historic_exchange` → `snapshot`

If the asset's `lineage_layer` maps to a type, that type is returned. This covers all
true source tables and the `hx_*` historical exchange snapshot family.

**Signal 4 — mart-layer composition lean**
For `gen2_mart` assets, the pipeline applies a refinement: if the composition signal
would classify the asset as `dimension` but the dimension ratio is below 0.6, the
classification is overridden to `fact`. This reflects the empirical observation that
gen2_mart assets in this warehouse are predominantly fact tables.

**Signal 2 — column composition ratios (tiebreaker for mart assets)**
Counts columns by role:
- `fact_ratio` = (measure + numeric_attribute columns) / total columns
- `dim_ratio` = (categorical_attribute + attribute columns) / total columns

Thresholds: `fact_ratio > 0.55` → fact; `dim_ratio > 0.45` → dimension; otherwise ambiguous.

If a grain key count of zero is detected, `unknown` is returned immediately (no point
applying composition signals to an asset with no known grain).

The resulting `table_type` is stored on `DataRequisite.table_type` and drives
`JoinAssessment.join_direction` classification.

---

## The data requisite contract

`DataRequisite` is a pre-rendered build specification that every full spec carries. It
answers: "given the current warehouse state, what is the minimum set of assets and
columns needed to build this data product, and how should they be joined?"

**Fields:**

| Field | Meaning |
|-------|---------|
| `canonical_table_name` | Snake_case output table name (initiative_id + `_dashboard` or `_mart`) |
| `table_type` | Inferred type of the primary source asset |
| `primary_source_asset` | The driving fact/source asset, selected by two-stage ranking |
| `grain_keys` | Output grain (list of key column names) |
| `minimal_source_assets` | Minimum set of assets needed to produce all columns |
| `build_complexity` | `single_table` / `simple_join` / `complex_join` based on source count |
| `build_notes` | One sentence describing how to build (auto-generated) |
| `columns` | Ordered list of `DataRequisiteColumn` objects: identifiers first, then join dimensions, then output structure columns |
| `join_assessments` | One `JoinAssessment` per unique (primary, right_asset) pair |
| `source_asset_types` | Inferred type for each source asset |
| `source_asset_grains` | Grain keys for each source asset |

**`DataRequisiteColumn`** carries: `column_name`, `description`, `data_type`, `role`
(identifier / dimension / measure / time), `source_asset`, `source_column`, `derivation`
(direct_read / join / absent), and `join_key`.

**`JoinAssessment`** carries per-pair join metadata:

| Field | Values |
|-------|--------|
| `join_direction` | `fact_to_dimension` / `fact_to_fact` / `fact_to_snapshot` / `fact_to_bridge` / `other` |
| `join_safety` | `safe` / `risky` / `aggregation_required` |
| `grain_match` | `True` if both assets share identical grain key sets |
| `aggregation_needed` | `True` if right asset grain is finer than left asset grain |
| `safety_note` | One sentence explaining the safety classification |

**Why it's pre-rendered, not LLM-generated:**
The data requisite answers a structural question that has a single correct answer given
the current graph state. If the LLM generated it, the answer would vary between renders,
and a spec would not be reliably usable as a build contract. Pre-rendering ensures that
the same graph build always produces the same data requisite.

**Primary source asset selection uses two-stage ranking:**
1. Assets are filtered to those that contain at least one primitive-matched column.
2. Within that set, assets are ranked by a preference tier (preference for `_quote`,
   `_policy`, `_detail`, `_summary`, `_experience` suffixes; deprioritisation of
   `_rating`, `_factor`, `_load`, `_war_`, `_ops_`, `_inputs`, `_modifiers` suffixes),
   then by `upstream_dependents` descending. The highest-ranked asset becomes the primary.

**Cross-bundle dimension enrichment:**
After collecting columns from the primitive's supporting assets, `_build_data_requisite()`
performs a second scan over all bundle assets to find dimension-type assets that are
joinable (primary grain keys ⊆ asset grain keys) and structurally significant
(`upstream_dependents > 1`). This fills in dimension context that the primitive's
entity-group binding may not have captured.

---

## The `pricing_decomposition` gap

This is documented here because it is a cross-cutting architectural finding, not a bug
in any single phase.

**What the pipeline detects:** The `pricing_decomposition` primitive requires five columns:
`tech_gnwp`, `modtech_gnwp`, `sold_gnwp`, `tech_elc`, `commission`. All five exist in
`ll_quote_policy_detail` (and five related assets) by exact name, with full descriptions.

**What blocks confirmation:** The primitive's `required_entities` is `["pricing_component"]`.
That entity group is not registered in `ENTITY_GROUPS` in `conformed_binder.py`. The
conformed schema does not define a `pricing_component` parent business term — the five
columns are currently parented to `rate_monitoring` and `coverage` groups.

**Result:** `CapabilityPrimitiveExtractor` finds zero supporting assets for
`pricing_decomposition` because no asset is bound to the `pricing_component` entity.
The primitive has `maturity_score = 0` for entity coverage, even though column coverage
is 5/5. The primitive appears as an **inferred** primitive (amber hexagon) in the graph
explorer — detected analytically, not confirmed by schema.

**Impact:** Seven initiatives show `pricing_decomposition` as a missing or inferred
primitive. Data requisites for `underwriting_decision_support`, `renewal_prioritisation`,
and `broker_performance_intelligence` are missing pricing dimension columns.

**This is a schema registration gap, not a data gap.** The data exists. The pipeline
is correct to surface it as inferred rather than confirmed. The remedy is a one-line
change:

```python
# graph/semantic/conformed_binder.py
ENTITY_GROUPS = ["coverage", "policy", "policy_totals",
                 "profitability_measures", "rate_monitoring",
                 "pricing_component"]   # add this
```

Plus adding `pricing_component` as a parent business term to the conformed schema YAML,
with child terms: `commission`, `modtech_gnwp`, `sold_gnwp`, `tech_elc`, `tech_gnwp`.

**Demo talking point:** "The pipeline detected this capability exists in your schema but
isn't formally registered as a data group. That's a one-line fix on the data engineering
side and it unlocks six more initiatives immediately."

See `docs/backlog.md` for full remediation paths.

---

## LLM usage

The system calls an LLM exactly once per initiative in Phase 5. Everything else is deterministic.

**What the LLM receives (per call):**
- Full `SpecDocument` JSON serialisation
- Pre-rendered sentinel blocks (embedded in the prompt as `{{BLOCK_NAME}}`):
  - `{{BOM}}` — bill of materials: all supporting asset names
  - `{{ASSET_SCHEMATIC}}` — primitive-to-asset mapping table
  - `{{OUTPUT_STRUCTURE}}` — grain, dimensions, measures, time columns
  - `{{DATA_REQUISITES}}` — full `DataRequisite` rendered as a structured table
  - `{{JOIN_PATHS}}` — `JoinAssessment` objects rendered as a join guide

**What the LLM writes:**
- Initiative overview paragraph
- Business objective expansion
- Key measures narrative (which columns matter and why)
- Delivery guidance (refresh cadence, SLA expectations, output type)
- Composability narrative (how this initiative relates to composes_with peers)

**What the LLM does NOT write:**
Data requisites, join assessments, readiness evidence, gap chains, primitive maturity
scores, blocker details. These are all pre-rendered from graph state before the LLM
is called.

**Model and cost:**
- Model: `claude-sonnet-4-20250514` (pinned)
- Max tokens: 1,500 for full specs, 800 for gap briefs
- Approximate cost: ~$0.37 for a full 19-initiative render
- Caching: specs are cached by `stable_hash(initiative_id, graph_build_id)`. Re-running
  Phase 5 over an unchanged graph costs $0.00. Use `--force-render` to regenerate.

---

## Graph explorer layers

The `output/graph_explorer.html` vis-network explorer has three switchable layers.

### Structural layer

Shows Asset nodes coloured by `lineage_layer` tag, connected by DEPENDS_ON edges.
Useful for understanding the physical lineage of the warehouse. The main cluster
(187 assets rooted at `hx_landing`) and the isolated D&P micro-cluster are visible here.

### Semantic layer

Shows BusinessEntityNode, DomainNode, and MetricNode objects connected by REPRESENTS,
BELONGS_TO_DOMAIN, and METRIC_BELONGS_TO_ENTITY edges. Shows which assets are bound
to which entity groups and which domains they belong to.

### Opportunity layer

Shows the initiative portfolio, capability primitives, and gap chains.

**Node types:**

| Node type | Shape | Colour | Meaning |
|-----------|-------|--------|---------|
| InitiativeNode | Circle | Teal / amber / grey / red by readiness | One per initiative; size proportional to `composite_score` |
| CapabilityPrimitiveNode (confirmed) | Hexagon | Solid teal fill | Entity group registered AND supporting assets bound |
| CapabilityPrimitiveNode (inferred) | Hexagon | Amber outline, no fill | Columns present in warehouse but schema group unregistered |
| CapabilityPrimitiveNode (unresolved) | Hexagon | Grey outline, no fill | Columns not found in warehouse |
| GapNode | Triangle | Red | Structured gap preventing an initiative from advancing |

**Node colour by readiness:**

| Readiness | Colour |
|-----------|--------|
| `ready_now` | Solid teal |
| `ready_with_enablement` | Amber |
| `needs_foundational_work` | Grey |
| `not_currently_feasible` | Red |

**Edge types:**

| Edge | Style | Direction | Meaning |
|------|-------|-----------|---------|
| ENABLES | Dashed teal | Primitive → Initiative | Primitive contributes to this initiative |
| BLOCKED_BY | Dashed red | Initiative → Gap (rendered inverted as Gap → Initiative) | Gap prevents initiative from proceeding |
| COMPOSES_WITH | Solid light-grey | Bidirectional | Initiatives sharing ≥ 2 primitives |

**Primitive states — the `pricing_decomposition` worked example:**

`pricing_decomposition` appears as an amber-outline hexagon (inferred state) because:
1. Column coverage is 5/5 — all required columns exist in the warehouse.
2. Entity coverage is 0/1 — `pricing_component` is not registered in `ENTITY_GROUPS`.
3. `supporting_asset_ids` is empty, so the primitive state is inferred (not unresolved,
   because it still has ENABLES edges to initiatives — the extractor detects the
   analytical capability even without formal entity binding).

An unresolved primitive (grey hexagon) would have both zero supporting assets AND no
ENABLES edges — meaning neither the entity group nor the columns exist in the warehouse.
