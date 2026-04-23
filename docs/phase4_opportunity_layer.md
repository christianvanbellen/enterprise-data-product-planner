<!-- Last updated: April 2026 -->
# Phase 4 — Opportunity Layer

---

## Purpose

Phase 4 adds an **opportunity layer** on top of the semantic graph produced by Phase 3.
It answers the question: *given what this warehouse can currently support, which analytics
initiatives are viable, which are close, and which are fundamentally blocked?*

The output is a ranked portfolio of 19 initiatives, each with a readiness state, composite
score, and a complete evidence record explaining the rating.

Phase 4 is fully deterministic — no LLM calls. Every node and score is computed from the
graph state and the ontology YAMLs.

---

## Architecture

```
SemanticGraph (Phase 3 output)
CanonicalBundle (Phase 1 output)
ontology/initiative_research.yaml
        │
        ▼
CapabilityPrimitiveExtractor  ─────► 9 CapabilityPrimitive objects
        │
        ▼
OpportunityPlanner  ──────────────► 19 OpportunityResult objects
        │
        ▼
GapAnalyser  ─────────────────────► GapRecord objects (per missing primitive)
        │
        ▼
OpportunityGraphCompiler  ────────► nodes + edges appended to graph
```

Four classes:

| Class | Module | Role |
|-------|--------|------|
| `CapabilityPrimitiveExtractor` | `graph/opportunity/primitive_extractor.py` | Scans the semantic graph for 9 defined primitives; emits a `CapabilityPrimitive` per match |
| `InitiativeArchetypeLibrary` | `graph/opportunity/archetype_library.py` | Loads 19 initiative definitions from `ontology/initiative_research.yaml` |
| `OpportunityPlanner` | `graph/opportunity/planner.py` | Maps primitives to initiatives; computes readiness, composite score, blockers |
| `GapAnalyser` | `graph/opportunity/gap_analyser.py` | Produces structured `GapRecord` objects for absent or weak primitives |
| `OpportunityGraphCompiler` | `graph/opportunity/compiler.py` | Orchestrates the above; appends opportunity nodes and edges to the live graph |

---

## Capability primitives

Nine capability primitives are defined in `PRIMITIVE_DEFINITIONS`
(`graph/opportunity/primitive_extractor.py`). Each is matched against the semantic graph
by checking that the required entity groups and columns are present.

| Primitive | Required entities | Key columns | Domain |
|-----------|------------------|-------------|--------|
| `quote_lifecycle` | policy, coverage | quote_id, inception_date, expiry_date, new_renewal, policyholder_name | underwriting |
| `pricing_decomposition` | pricing_component | tech_gnwp, modtech_gnwp, sold_gnwp, tech_elc, commission | pricing |
| `rate_change_monitoring` | pricing_component | gross_rarc, net_rarc, claims_inflation, breadth_of_cover_change | portfolio_monitoring, pricing |
| `claims_experience` | claim | incurred, paid, burn_rate_ulr, gg_ulr, gn_ulr | underwriting |
| `profitability_decomposition` | profitability_component | sold_to_modtech, modtech_to_tech, sold_to_plan, target_to_plan | profitability |
| `broker_attribution` | broker, policy | broker_primary, broker_code, brokerage_pct | distribution |
| `renewal_tracking` | policy, policyholder | new_renewal, quote_id, inception_date, expiry_date | underwriting |
| `product_line_segmentation` | coverage | *(tag-based: eupi, d_o, general_aviation, contingency)* | underwriting |
| `exposure_structure` | exposure, coverage | exposure, limit_100, deductible_value, excess, policy_coverage_jurisdiction | underwriting |

### Primitive states

A primitive is classified into one of three states based on the semantic graph:

| State | Condition | Visual (graph explorer) |
|-------|-----------|------------------------|
| **Confirmed** | `supporting_asset_ids.length > 0` — the required entity group is defined in the conformed schema and assets are bound to it | Solid teal hexagon |
| **Inferred** | `supporting_asset_ids.length == 0` AND the primitive has ENABLES edges to initiatives — columns exist in the warehouse but the schema group is unregistered | Amber-outline hexagon |
| **Unresolved** | `supporting_asset_ids.length == 0` AND no ENABLES edges — columns not found | Grey hexagon |

The `pricing_decomposition` primitive is currently **inferred**: all five required columns
exist in `ll_quote_policy_detail` and five related assets, but `pricing_component` is not
registered as a schema group. See `docs/backlog.md` for remediation options.

---

## Initiative portfolio

Nineteen initiatives are defined in `ontology/initiative_research.yaml`, each grounded in
published insurance analytics research. Each initiative specifies required primitives,
optional primitives, business value/effort scores, and a literature reference.

### Readiness states

| State | Meaning | Composite score multiplier |
|-------|---------|---------------------------|
| `ready_now` | All required primitives confirmed at maturity ≥ 0.5 | 1.0 |
| `ready_with_enablement` | All required primitives present but some below maturity threshold, or YAML feasibility ceiling is `ready_with_enablement` | 0.8 |
| `needs_foundational_work` | One or more required primitives absent from warehouse | 0.4 |
| `not_currently_feasible` | YAML research artifact sets feasibility to `not_currently_feasible` (data fundamentally unavailable) | 0.1 |

Readiness is the **minimum** of primitive-based readiness and the
`feasibility_against_warehouse` ceiling from the research artifact.

### Entity-binding confidence threshold (`--min-entity-confidence`)

By default, any REPRESENTS edge with `confidence ≥ MIN_CONFIDENCE` (0.4, set in
`entity_bindings.yaml`) counts as evidence that a primitive's `required_entities`
are met. This is the **pragmatic** view — latent capabilities surface via
discovery-layer signals (entity signatures, asset-name patterns, tag bindings)
even if the conformed schema hasn't formalised them.

`run_phase4.py --min-entity-confidence <value>` tightens this threshold:

| Value | Effective view | Typical use |
|-------|---------------|-------------|
| `0.0` (default) | Any binding counts — discovery and governance treated equally | Opportunity ideation, internal portfolio reviews |
| `0.6` | Drops flat Signal-3/4 bindings (tag-dim, asset-name) | Mid-confidence filter when you want some discovery signal but not substring matches |
| `0.8` | ≈ governed-only — keeps Signal 1 (conformed-schema overlap up to 1.0) and the strongest Signal-2 signatures | Formal readiness reports, steering-committee slides |
| `1.0` | Strictly Signal 1 with perfect column overlap | Audit or certification contexts |

Raising the threshold can *only* reduce an initiative's entity_score, never
increase it, so every run at a higher threshold is a strict subset of the
default run. See `CapabilityPrimitiveExtractor.extract` for the implementation.

### Composite score

```
composite_score = business_value_score
               × readiness_multiplier
               × (1 + 0.2 × optional_available_ratio)
               × (1 − 0.3 × implementation_effort_score)
```

The composite score ranks initiatives for prioritisation. It is always between 0 and 1.

### Initiative archetypes

| Archetype | Description |
|-----------|-------------|
| `monitoring` | Continuous portfolio surveillance dashboard |
| `decision_support` | Context surfaced at point of underwriting or actuarial decision |
| `prioritization` | Ranking or triage of renewals, submissions, or accounts |
| `prediction` | Statistical forecasting or ML-based projection |
| `copilot` | Natural language interface over structured data |
| `recommendation` | Ranked suggestions (e.g. channel mix, pricing tier) |

---

## Gap analysis

`GapAnalyser` produces a `GapRecord` for each primitive that prevents an initiative
from reaching `ready_now`. Gap types are defined in `ontology/gap_types.yaml`:

| Gap type | Meaning |
|----------|---------|
| `missing_history` | Time-series or development triangles required but absent |
| `weak_identifier` | Key column exists as a name string but lacks a stable ID for joining |
| `incomplete_relationship` | A required entity relationship cannot be traced through lineage |
| `missing_conformed_entity` | The schema group for the required entity is not registered |
| `insufficient_documentation` | Column descriptions missing, reducing analytical confidence |
| `low_test_coverage` | Fewer than two dbt tests on grain key columns |
| `insufficient_outcome_labels` | Training labels absent (required for prediction archetypes) |
| `missing_event_timeline` | Temporal event sequences absent (claims development, premium movement) |
| `missing_source_system` | The source system that would provide the required data is not in the warehouse |

---

## Graph nodes added by Phase 4

| Label | node_id prefix | Description |
|-------|----------------|-------------|
| `CapabilityPrimitiveNode` | `primitive_` | One per primitive; carries `supporting_asset_ids`, `maturity_score`, `primitive_name` |
| `InitiativeNode` | `initiative_` | One per initiative; carries `readiness`, `composite_score`, `available_primitives`, `missing_primitives`, `blocker_details` |
| `GapNode` | `gap_` | One per structured gap; carries `gap_type`, `description`, `missing_columns`, `leverage_score` |

---

## Graph edges added by Phase 4

| edge_type | Source → Target | Description |
|-----------|----------------|-------------|
| `ENABLES` | CapabilityPrimitiveNode → InitiativeNode | Primitive contributes to this initiative |
| `BLOCKED_BY` | InitiativeNode → GapNode | Initiative cannot proceed until this gap is resolved |
| `COMPOSES_WITH` | InitiativeNode → InitiativeNode | Initiatives sharing ≥ 2 primitives (bidirectional) |

---

## Output artifacts

Phase 4 appends directly to the graph files written by Phase 2/3:

| Artifact | Description |
|----------|-------------|
| `output/graph/nodes.json` | Extended with CapabilityPrimitiveNode, InitiativeNode, GapNode entries |
| `output/graph/edges.json` | Extended with ENABLES, BLOCKED_BY, COMPOSES_WITH edges |
| `output/graph_explorer.html` | Interactive vis-network explorer; Opportunity layer shows all three node types and their relationships |

---

## Running Phase 4

```bash
python scripts/run_phase4.py \
  --bundle output/bundle.json \
  --graph output/graph \
  --output output/graph
```

Requires Phase 1–3 output. Appends to `output/graph/nodes.json` and
`output/graph/edges.json`. Idempotent: re-running with the same inputs produces
the same `build_id` and overwrites prior opportunity nodes.

---

## Exploring the output

```bash
# Open the graph explorer
cd output && python -m http.server 8080
# Then open http://localhost:8080/graph_explorer.html
# Switch to "Opportunity" layer
```

In Opportunity mode the explorer shows:
- **Coloured boxes** — initiative nodes, coloured by readiness state
- **Solid teal hexagons** — confirmed primitives with bound assets
- **Amber-outline hexagons** — inferred primitives (columns present, schema group absent)
- **Grey hexagons** — unresolved primitives
- **Red triangles** — gap nodes
- **Dashed teal edges** — ENABLES (primitive → initiative)
- **Dashed red edges** — BLOCKED_BY, direction inverted to show gap → initiative

---

## Key design decisions

1. **Readiness is a ceiling, not a floor.** The YAML `feasibility_against_warehouse`
   field caps readiness — a primitive match cannot promote an initiative above its
   research-derived feasibility ceiling.

2. **Primitives are scored at maturity, not binary.** `maturity_score` is the average
   overlap between the primitive's required columns and the supporting assets' column
   sets. An initiative using a low-maturity primitive is penalised in composite score.

3. **`pricing_decomposition` is a known inferred primitive.** The pipeline intentionally
   surfaces it as amber (detected, unconfirmed) rather than silently promoting or
   demoting it. The remedy is a schema registration, not an ETL change.
   See `docs/backlog.md` and `docs/phase6_architecture_alignment.md`.

4. **Gap leverage scores enable prioritisation.** Each `GapNode` carries a
   `leverage_score` = fraction of all initiatives it blocks. Resolving the highest-
   leverage gap delivers the most initiative unlocks per engineering day.

---

## Graph explorer — Opportunity layer visual encoding

*Added April 2026. Documents the visual encoding as shipped after the April 2026 overhaul.*

### Node types and visual states

| Node type | Shape | Fill | Border | Meaning |
|-----------|-------|------|--------|---------|
| InitiativeNode — `ready_now` | Circle | Solid teal | Teal | All required primitives confirmed at maturity ≥ 0.5 |
| InitiativeNode — `ready_with_enablement` | Circle | Solid amber | Amber | Primitives present but below maturity threshold, or YAML ceiling caps at this state |
| InitiativeNode — `needs_foundational_work` | Circle | Solid grey | Grey | One or more required primitives absent from warehouse |
| InitiativeNode — `not_currently_feasible` | Circle | Solid red | Red | YAML research artifact sets fundamental infeasibility |
| CapabilityPrimitiveNode — confirmed | Hexagon | Solid teal fill | Teal | Entity group registered AND supporting assets bound via REPRESENTS edges |
| CapabilityPrimitiveNode — inferred | Hexagon | No fill | Amber outline | Columns present in warehouse but schema group not registered in ENTITY_GROUPS |
| CapabilityPrimitiveNode — unresolved | Hexagon | No fill | Grey outline | Neither entity group nor required columns found in warehouse |
| GapNode | Triangle | Solid red | Red | Structured gap preventing an initiative from advancing |

**InitiativeNode size** is proportional to `composite_score`. Higher-scoring initiatives
render with a larger circle.

### Edge types

| Edge type | Style | Colour | Direction | Meaning |
|-----------|-------|--------|-----------|---------|
| ENABLES | Dashed | Teal | Primitive → Initiative | This primitive contributes to the initiative's readiness |
| BLOCKED_BY | Dashed | Red | Initiative → Gap (rendered inverted as Gap → Initiative) | This gap prevents the initiative from advancing |
| COMPOSES_WITH | Solid | Light grey | Bidirectional | Initiatives sharing ≥ 2 primitives (joint build opportunity) |

### Primitive states

A primitive's visual state is determined by two properties of `CapabilityPrimitiveNode`:

| State | Condition | Example |
|-------|-----------|---------|
| **Confirmed** | `supporting_asset_ids.length > 0` — the entity group is registered and at least one asset is bound to it | `quote_lifecycle`, `broker_attribution` |
| **Inferred** | `supporting_asset_ids.length == 0` AND the primitive has ENABLES edges to initiatives — the pipeline detected analytical capability but the schema group is unregistered | `pricing_decomposition` |
| **Unresolved** | `supporting_asset_ids.length == 0` AND no ENABLES edges — columns not found anywhere in the warehouse | Any primitive with zero column matches |

### Worked example: `pricing_decomposition` (inferred)

`pricing_decomposition` is the canonical inferred primitive in this warehouse. Its visual
state is an amber-outline hexagon because:

1. **Column coverage is 5/5.** All five required columns (`tech_gnwp`, `modtech_gnwp`,
   `sold_gnwp`, `tech_elc`, `commission`) exist in `ll_quote_policy_detail` and related
   assets by exact name.

2. **Entity coverage is 0/1.** `pricing_component` is not registered in `ENTITY_GROUPS` in
   `graph/semantic/conformed_binder.py`. No asset is bound to the `pricing_component` entity
   via REPRESENTS edges. `supporting_asset_ids` is therefore empty.

3. **ENABLES edges are present.** Despite zero supporting assets, the extractor detects
   the capability (because column coverage is 5/5) and creates ENABLES edges to the seven
   initiatives that depend on it. This is what makes the primitive *inferred* rather than
   *unresolved* — the capability exists, it just isn't formally registered.

4. **Remedy is a schema registration, not ETL.** Adding `pricing_component` to `ENTITY_GROUPS`
   and to the conformed schema YAML would flip this primitive to confirmed (solid teal hexagon)
   on the next Phase 3+4 run. No data pipeline change is required.

See `docs/backlog.md` for the full remediation plan and `ARCHITECTURE.md` for the
architectural analysis.
