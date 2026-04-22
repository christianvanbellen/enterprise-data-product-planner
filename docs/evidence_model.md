# Evidence Model

Every node and edge in the graph carries an `evidence` object that records how it was
created, with what confidence, and during which pipeline run. This enables auditability,
stale-artifact detection, and reproducibility across reruns.

---

## EvidenceRecord structure

```json
{
  "build_id":         "build_693e66d1c1ed580f",
  "confidence":       1.0,
  "created_by":       "structural_compiler_v1",
  "evidence_sources": [
    { "type": "canonical_asset",   "value": "asset_ae98a8c1f4ff0ffa" },
    { "type": "derivation_method", "value": "explicit_metadata" }
  ],
  "review_status":    "auto",
  "rule_id":          "structural.asset_node",
  "timestamp_utc":    "2026-04-14T19:34:28.737589+00:00"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `build_id` | string | Identifies the pipeline run that created this artifact. Prefixed by phase: `build_` (Phase 2), `sem_` (Phase 3), `opp_` (Phase 4). |
| `confidence` | float 0–1 | How certain the compiler is that this node/edge is correct. All Phase 1–2 edges are `1.0`. Semantic edges range from `0.5–1.0`. |
| `created_by` | string | The compiler component that emitted this record. |
| `evidence_sources` | list | Typed pointers to the source data that justified this record. |
| `review_status` | string | Always `"auto"` (machine-generated). A future review workflow would flip this to `"approved"` or `"rejected"`. |
| `rule_id` | string | The specific inference rule that produced this record. See tables below. |
| `timestamp_utc` | string | ISO-8601 timestamp of creation. |

---

## Confidence constants

Defined in `graph/compiler/evidence.py`:

| Constant | Value | Used for |
|----------|-------|---------|
| `EXPLICIT_DEP` | `1.0` | Explicit dbt `upstream_dependencies` edges |
| `DIRECT_COL` | `0.95` | Direct column reference in SQL expression |
| `EXPRESSION_COL` | `0.75` | Column inferred from SQL expression (not a direct reference) |
| `AMBIGUOUS` | `0.50` | Ambiguous match; more than one candidate |

---

## Phase 2 rule_id reference

| rule_id | Node/edge type | Produced by |
|---------|---------------|-------------|
| `structural.asset_node` | Asset node | Step 1 — asset node creation |
| `structural.column_node` | Column node | Step 2 — column node creation |
| `structural.has_column` | HAS_COLUMN edge | Step 2 — column attachment |
| `structural.schema_node` | Schema node | Step 3 — schema node creation |
| `structural.containment` | CONTAINS edge | Step 3 — schema containment |
| `lineage.explicit_upstream` | DEPENDS_ON edge | Step 4 — dbt upstream lineage |
| `structural.test_node` | Test node | Step 6 — dbt test nodes |
| `structural.tested_by` | TESTED_BY edge | Step 6 — test attachment |
| `structural.doc_node` | DocObject node | Step 7 — documentation records |
| `structural.documented_by` | DOCUMENTED_BY edge | Step 7 — doc attachment |

---

## Phase 3 rule_id reference

| rule_id | Edge type | Produced by |
|---------|-----------|-------------|
| `semantic.represents` | REPRESENTS | EntityMapper — asset to BusinessEntity binding |
| `semantic.belongs_to_domain` | BELONGS_TO_DOMAIN | DomainAssigner — domain membership |
| `semantic.conformed_binding` | CONFORMS_TO | ConformedFieldBinder — column group binding |
| `semantic.metric_node` | MetricNode | MetricNode creation from business terms |
| `semantic.metric_belongs_to_entity` | METRIC_BELONGS_TO_ENTITY | Metric to entity attachment |

---

## Phase 4 rule_id reference

| rule_id | Node/edge type | Produced by |
|---------|---------------|-------------|
| `opportunity.primitive_node` | CapabilityPrimitiveNode | CapabilityPrimitiveExtractor |
| `opportunity.initiative_node` | InitiativeNode | OpportunityPlanner |
| `opportunity.gap_node` | GapNode | GapAnalyser |
| `opportunity.enables` | ENABLES edge | OpportunityPlanner — primitive to initiative |
| `opportunity.blocked_by` | BLOCKED_BY edge | GapAnalyser — initiative blocked by gap |
| `opportunity.composes_with` | COMPOSES_WITH edge | OpportunityPlanner — initiative composition |

---

## build_id

Each phase produces a stable `build_id` derived from the input data hash:

| Phase | Prefix | Derivation |
|-------|--------|-----------|
| Phase 2 | `build_` | Hash of CanonicalBundle content |
| Phase 3 | `sem_` | Hash of structural graph build_id + semantic config |
| Phase 4 | `opp_` | Hash of semantic graph build_id + initiative library version |
| Phase 5 | *(spec_id, not build_id)* | `stable_hash(initiative_id, graph_build_id)` |

Every node and edge carries the `build_id` of the phase that created it. This means
after a graph rebuild, stale artifacts from the previous run are identifiable by their
old `build_id`. The graph explorer always loads the latest JSON files, so stale
artifacts only persist if you don't re-run the relevant phase.

---

## Evidence sources

The `evidence_sources` array contains typed pointers. Common types:

| type | value | meaning |
|------|-------|---------|
| `canonical_asset` | asset internal_id | This record was derived from a specific asset |
| `derivation_method` | string | How the inference was made (e.g. `"explicit_metadata"`, `"keyword_scan"`, `"lineage_inheritance"`) |
| `primitive_id` | string | This record relates to a specific capability primitive |
| `initiative_id` | string | This record relates to a specific initiative |
| `readiness` | string | Readiness state that triggered this evidence record |

---

## Invariants

1. **Every non-`_BuildMeta` node has a non-empty `evidence` object.** The `_BuildMeta`
   node is the sole exception; it carries `evidence: {}`.
2. **Confidence is always between 0.0 and 1.0 inclusive.** No compiler emits values
   outside this range.
3. **`rule_id` is namespaced by phase.** `structural.*` rules are Phase 2 only;
   `semantic.*` are Phase 3 only; `opportunity.*` are Phase 4 only. This prevents
   rule_id collisions across phases.
4. **`timestamp_utc` is always UTC ISO-8601.** Timezone-naive timestamps are a bug.
