<!-- Last updated: April 2026 -->
# Input and configuration reference

This document is the single reference for every editable artifact in the pipeline —
external data files, ontology YAML files, and the one remaining Python configuration
constant. For each artifact it states: what it is, where it lives, which phase reads it,
and what effect a change has on the pipeline output.

For the overall pipeline architecture and design decisions see `ARCHITECTURE.md`.
For phase-specific implementation detail see `docs/phase*.md`.

**Recent change (April 2026):** Five previously-Python configuration constants were
migrated to ontology YAML files. `ENTITY_GROUPS` + `OVERLAP_THRESHOLD`, `DOMAIN_KEYWORDS`,
`SEMANTIC_MAP`, `GRAIN_KEY_CANDIDATES`, `PRIMITIVE_DEFINITIONS`, and `_DELIVERY` are now
editable without touching Python. `_INITIATIVE_SCORES` was merged into `initiative_research.yaml`
collapsing the previous two-file split into one complete initiative definition per entry.

---

## How inputs flow through the pipeline

```
EXTERNAL DATA INPUTS                          ONTOLOGY CONFIGURATION
  data/dbt_metadata_enriched.json    ─┐       ontology/tag_mappings.yaml
  data/conformed_schema.json          ├────►  ontology/domain_keywords.yaml
                                      │       ontology/semantic_map.yaml
                                      │       ontology/grain_keys.yaml
                                      ▼
                                   Phase 1 ─► CanonicalBundle (output/bundle.json)
                                      │
                                      ▼
                                   Phase 2 ─► Structural graph
                                      │       (no editorial inputs)
                                      │
                                      │       ontology/insurance_entities.yaml
                                      │       ontology/entity_groups.yaml
                                      ▼
                                   Phase 3 ─► Semantic graph
                                      │
                                      │       ontology/initiative_research.yaml
                                      │       ontology/primitives.yaml
                                      ▼
                                   Phase 4 ─► Opportunity graph
                                      │
                                      │       ontology/delivery_heuristics.yaml
                                      │       [Python] _SYSTEM_PROMPT
                                      │       .env → ANTHROPIC_API_KEY
                                      ▼
                                   Phase 5 ─► output/spec_log/
```

**Re-run cost of a change:**

| Change to | Re-run from | LLM cost |
|-----------|-------------|----------|
| `dbt_metadata_enriched.json` / `conformed_schema.json` / `tag_mappings.yaml` / `domain_keywords.yaml` / `semantic_map.yaml` / `grain_keys.yaml` | Phase 1 → 5 | Yes (Phase 5 re-render) |
| `entity_groups.yaml` / `insurance_entities.yaml` | Phase 3 → 5 | Yes (Phase 5 re-render) |
| `initiative_research.yaml` / `primitives.yaml` | Phase 4 → 5 | Yes (Phase 5 re-render) |
| `delivery_heuristics.yaml` | Phase 5 only with `--force-render` | No (pre-rendered section) |
| `_SYSTEM_PROMPT` in `renderer.py` | Phase 5 only with `--force-render` | Yes |

Reference-only files (`gap_types.yaml`, `relationship_types.yaml`) are not loaded at runtime.

---

## External data inputs

### `data/dbt_metadata_enriched.json`

**What it is:** JSON export of the dbt project manifest, enriched with column-level
descriptions from `schema.yml` files. The primary structural input to the pipeline.
Produced by `dbt docs generate` — the analytics team does not author this file directly,
but the quality of its column descriptions is the most impactful editorial decision
affecting spec quality downstream.

**Read by:** `DbtMetadataAdapter` (Phase 1)

**Contains:**
- Model names, file paths, materialisation types
- Column names, raw data types, and column descriptions (from `schema.yml`)
- dbt tests attached to columns
- dbt model tags — used for `lineage_layers` and `product_lines` assignment via `tag_mappings.yaml`
- `depends_on.nodes` — the explicit upstream lineage graph (201 `DEPENDS_ON` edges)

**Effect of changes:**
- Adding column descriptions → more columns pass the positive-inclusion filter in Phase 5
  specs; reduces the undescribed-column warnings in the WHEN section
- Adding dbt tests → raises test coverage %, reduces the ⚠ data quality warnings
- Adding or changing model tags → changes `lineage_layers` and `product_lines` assignments,
  which affects dimensional role inference (`_infer_table_type`) in Phase 5
- Adding new models → new `Asset` nodes; may add new primitive support if they match
  required entity groups and columns

**Current state (Liberty warehouse):** 207 models, 2,654 columns, 343 with descriptions
(12.9% coverage), 201 lineage edges.

---

### `data/conformed_schema.json`

**What it is:** JSON Schema file defining the canonical business vocabulary for the warehouse.
Authored by the warehouse modelling architect. The most architecturally significant input
to the pipeline — it defines which business concepts the analytics layer can formally recognise.

**Full structural spec:** see `docs/artifacts/conformed_schema_spec.md` for the exact JSON
Schema shape, the two group shapes the adapter recognises (array vs object-of-objects),
the term hierarchy produced, and the pricing_component gap remediation.

**Read by:** `ConformedSchemaAdapter` (Phase 1)

**Contains:**
- Parent business term groups (e.g. `coverage`, `policy`, `rate_monitoring`)
- Child field names under each parent — the canonical column names belonging to each group

**Effect of changes:**
- Adding a new parent group (e.g. `pricing_component`) AND registering it in
  `ontology/entity_groups.yaml` → unlocks primitive binding for that concept. The
  `pricing_decomposition` gap is resolved by adding `pricing_component` here with five
  child fields.
- Adding child fields to an existing group → raises the overlap score for assets binding
  to that group; may promote additional assets to bound status
- Removing child fields → may drop assets below `overlap_threshold` and remove entity bindings

**The pricing_component gap:** The conformed schema currently defines 7 groups. Adding
`pricing_component` with children `commission`, `modtech_gnwp`, `sold_gnwp`, `tech_elc`,
`tech_gnwp` — combined with adding `pricing_component` to `entity_groups.yaml` — resolves
the amber hexagon on `pricing_decomposition` and unblocks 7 initiatives. See `ARCHITECTURE.md`.

---

## Ontology YAML files

All YAML files live in `ontology/`. Changes take effect on the next pipeline run from
the phase that reads them.

---

### `ontology/tag_mappings.yaml`

**Read by:** `DbtMetadataAdapter` at module load (Phase 1)

**What it controls:** Two mappings applied to every dbt model's tag list during ingestion:

`tag_to_lineage_layer` — maps each dbt tag to a lineage-layer string. Every matching tag
is collected into the asset's `lineage_layers` list (in tag order, deduplicated), so an
asset tagged `['hx', 'bookends']` yields `['historic_exchange', 'conformed_bookends']`.
`_infer_table_type()` in Phase 5 scans the whole list against its `_LAYER_TO_TYPE` table.

| Tag | lineage_layer value | Table type inferred |
|-----|--------------|---------------------|
| `hx` | `historic_exchange` | snapshot |
| `ll` | `liberty_link` | (composition-based) |
| `gen2` | `gen2_mart` | fact (with refinement) |
| `raw` | `raw_layer` | source |
| `source` | `source_table` | source |
| `bookends` | `conformed_bookends` | (composition-based) |
| `semi_conformed` | `semi_conformed_mart` | (composition-based) |

`tag_to_product_line` — maps a dbt tag to a `product_lines` entry. Used by
`product_line_segmentation` primitive matching in Phase 4 (tag-based, not column-based).

---

### `ontology/domain_keywords.yaml` *(migrated April 2026)*

**Read by:** `DbtMetadataAdapter` at module load (Phase 1)

**What it controls:** Keyword → domain assignment. During ingestion, each model's name,
description, tags, and column names are scanned against these keyword lists. Any domain
whose list matches is added to the asset's `domain_candidates`. These flow into Phase 3
`BELONGS_TO_DOMAIN` edges and into the graph explorer's domain colour coding.

**Format:** `{ domain_name: [keyword, ...] }` — substring match, case-insensitive.

**Known gap:** The `distribution` domain is under-captured — candidate additions include
`coverholder`, `mga`, `binder`, `lineslip`, `delegated`, `facility_fee`. Review context
before adding (e.g. `facility` may mean Lloyd's facility or a building). See
`docs/phase3_design_brief.md` — Distribution domain gap.

---

### `ontology/semantic_map.yaml` *(migrated April 2026)*

**Read by:** `DbtMetadataAdapter` at module load (Phase 1)

**What it controls:** Column name/description substring → semantic candidate label.
Matched labels are stored on `CanonicalColumn.semantic_candidates` and used in Phase 5
as a positive-inclusion signal for column selection in specs — alongside column
descriptions and membership in a primitive's `matched_columns` set.

**Format:** `substring: semantic_label` — flat dict, substring match.

**Effect of changes:**
- Adding a new entry → columns matching that substring gain a semantic candidate, making
  them eligible for inclusion in specs even if undescribed
- Changing a label → propagates through to spec column tables and the LLM's understanding
  of what the column represents

---

### `ontology/grain_keys.yaml` *(migrated April 2026)*

**Read by:** `DbtMetadataAdapter` at module load (Phase 1)

**What it controls:** Which column names are recognised as grain keys during ingestion.
An asset's `grain_keys` list is populated by exact matching against the `candidates` list.
`grain_keys` is used by `_infer_table_type()` (bridge detection: ≥4 keys → bridge), by
`_build_data_requisite()` for identifier column selection, and by `JoinAssessment`
grain match computation.

**Format:** Flat list under a `candidates:` key.

**Effect of changes:**
- Adding a column name → assets containing that column gain it as a grain key, which
  can change their inferred table type and the join assessments in their specs
- Removing a name → may cause bridge tables to be reclassified if they drop below 4 keys

---

### `ontology/insurance_entities.yaml`

**Read by:** `OntologyLoader.allowed_entities()` (Phase 3) — used as a validation whitelist

**What it is:** The registry of recognised semantic entity labels. When Phase 3 creates
`BusinessEntityNode` objects, the entity labels must appear in this list.

**Current entities (10):** `policyholder`, `broker`, `line_of_business`, `claim`, `coverage`,
`policy`, `pricing_component`, `profitability_component`, `exposure`, `underwriter`

**Effect of changes:**
- Adding an entity here is a necessary but not sufficient step to activate it — you must
  also add it to `entity_groups.yaml` and add a corresponding parent term to the conformed
  schema JSON for asset binding to occur
- Removing an entity → any asset currently bound to it loses its entity binding in Phase 3

---

### `ontology/entity_groups.yaml` *(migrated April 2026)*

**Read by:** `ConformedFieldBinder` at class load (Phase 3)

**What it controls:** The list of conformed schema parent groups that assets can be bound
to. `ConformedFieldBinder` scans every asset's columns against the child fields of each
group in this list. If the column-name overlap score meets `overlap_threshold`, a
`REPRESENTS` edge is created from the asset to the corresponding `BusinessEntityNode`.
These edges are the foundation of Phase 4 primitive matching.

**Format:**
```yaml
overlap_threshold: 0.5   # fraction of a group's fields an asset must contain to bind
groups:
  - coverage
  - policy
  - ...
```

**Current groups:** `coverage`, `policy`, `policy_totals`, `profitability_measures`,
`rate_monitoring`

**Effect of changes:**
- Adding `"pricing_component"` → resolves the `pricing_decomposition` amber hexagon,
  promotes 7 initiatives from inferred to confirmed primitive. Requires `pricing_component`
  to also exist as a parent term in the conformed schema JSON with appropriate child fields.
- Removing a group → all assets currently bound to it lose their entity binding; any
  primitive requiring that entity loses its supporting assets
- Lowering `overlap_threshold` → broader binding coverage but risks spurious matches on
  wide assets; raising it tightens binding but may exclude legitimate assets with
  partial column sets

This is the highest-impact single edit in the pipeline.

---

### `ontology/initiative_research.yaml` *(expanded April 2026 — now contains full initiative definitions)*

**Read by:** `InitiativeArchetypeLibrary` at module load (Phases 4 and 5)

**What it is:** The complete definition of every initiative — scoring, primitive wiring,
and research grounding in one place. Previously the scoring fields lived in
`_INITIATIVE_SCORES` in `archetype_library.py`; that Python constant has been removed
and everything is now YAML.

**Contains two top-level sections:**

**`sources`** — bibliography of research references. Each source has an `id`, `title`,
`publisher`, `year`, and optionally a `url`. Source IDs are referenced by initiatives
and appear in spec `literature_source_ids` fields.

**`initiative_taxonomy`** — one entry per initiative with these fields:

| Category | Field | Effect |
|----------|-------|--------|
| **Scoring** | `archetype` | `monitoring` / `decision_support` / `prediction` / `copilot` / `automation` / `prioritization` / `recommendation` / `anomaly_detection` |
| | `required_primitives` | Must be confirmed for `ready_now` readiness |
| | `optional_primitives` | Bonus to composite score when present |
| | `business_value_score` | 0–1 editorial weight; drives composite score ranking |
| | `implementation_effort_score` | 0–1; higher = more expensive to build (penalises score) |
| | `target_users` | Surfaced in WHO section of rendered specs |
| | `business_objective` | One sentence surfaced in WHAT section |
| | `output_type` | `monitoring_dashboard` / `decision_support` / `analytics_product` / `ai_agent` |
| **Research** | `category` | Reporting category — underwriting / pricing / claims / portfolio / distribution / profitability / copilot / monitoring |
| | `literature_name` | Human-readable initiative name displayed in specs |
| | `sources` | List of source IDs — surfaced in WHAT section |
| | `literature_quote` | Specific research finding quoted in WHAT section |
| | `feasibility_against_warehouse` | **Ceiling cap on readiness** — overrides primitive-based readiness |
| | `feasibility_rationale` | Explanation shown in WHEN section for capped initiatives |
| | `data_gaps` | Structural gaps declared as YAML; become `BlockerDetail` objects in specs |

**Composite score formula:**
```
composite_score = business_value_score
               × readiness_multiplier          # ready_now=1.0 / ready_with_enablement=0.8 /
                                               # needs_foundational_work=0.4 / not_feasible=0.1
               × (1 + 0.2 × optional_ratio)   # bonus for available optional primitives
               × (1 − 0.3 × effort_score)     # penalty for high implementation effort
```

**Effect of changes:**
- `business_value_score` / `implementation_effort_score` → reranks the portfolio
- `feasibility_against_warehouse` → immediately changes readiness state and composite score
- Adding a `data_gaps` entry → adds a blocker to the initiative's gap_brief
- `literature_quote` / `sources` → updates WHAT section (re-run Phase 5 with `--force-render`)
- `required_primitives` → may demote readiness if the primitive isn't confirmed
- `target_users` / `business_objective` → updates WHO and WHAT sections

---

### `ontology/primitives.yaml` *(migrated April 2026)*

**Read by:** `CapabilityPrimitiveExtractor` at module load (Phase 4)

**What it controls:** The 9 analytical capability primitives. Each primitive defines
what the warehouse must provide for it to be "confirmed". Primitive confirmation drives
initiative readiness, composite scores, and the structural content of specs.

**Format per primitive:**
```yaml
- id: quote_lifecycle
  required_entities: [policy, coverage]     # entity group labels from entity_groups.yaml
  required_columns:                          # column normalised_names that must exist
    - quote_id
    - inception_date
    - ...
  supporting_domains: [underwriting]        # domain filter for candidate assets
  description: "End-to-end quote and policy lifecycle tracking"
```

Special case — `product_line_segmentation` uses `required_tags` instead of
`required_columns` (matched against asset `product_lines` tags rather than column names).

**Maturity score:** `(entity_score × 0.5) + (column_score × 0.5)`. A score below 0.5
triggers a `⚠ partial` warning in the WHEN section.

**Effect of changes:**
- Adding a new primitive → becomes available as a `required_primitives` entry for
  initiatives; its state (confirmed/inferred/unresolved) is computed from current graph state
- Adding a required column → raises the bar for confirmation; may demote a confirmed
  primitive to inferred if the new column is absent
- Removing a required entity → may promote an inferred primitive to confirmed
- Changing the description → propagates to the WHEN primitive table in rendered specs

---

### `ontology/delivery_heuristics.yaml` *(migrated April 2026)*

**Read by:** `SpecRenderer` at module load (Phase 5, pre-rendered section)

**What it controls:** Per-archetype defaults for the Delivery subsection of the WHEN block:
refresh cadence, SLA expectation, and output format. These are heuristics — the LLM does
not write this section.

**Format:**
```yaml
delivery:
  monitoring:
    refresh: "Daily or weekly"
    sla:     "T+1 from upstream mart refresh"
    format:  "BI / Streamlit dashboard"
  decision_support:
    ...
```

**Effect of changes:** Updates the Delivery line in the WHEN section of all specs with
that archetype. Requires `--force-render` to see the change in existing specs.

---

### `ontology/gap_types.yaml` *(reference only — not loaded at runtime)*

**Read by:** Nobody. This file is not imported by any pipeline code.

**What it is:** A reference vocabulary of the 9 gap type labels used in `data_gaps`
entries in `initiative_research.yaml` and in gap analysis output. Documents the controlled
vocabulary for the `gap_type` field; the pipeline uses the string values directly.

**Gap types:** `missing_history`, `weak_identifier`, `incomplete_relationship`,
`missing_conformed_entity`, `insufficient_documentation`, `low_test_coverage`,
`insufficient_outcome_labels`, `missing_event_timeline`, `missing_source_system`

---

### `ontology/relationship_types.yaml` *(reference only — not loaded at runtime)*

**Read by:** Nobody. This file is not imported by any pipeline code.

**What it is:** A reference listing of all edge types by graph layer (structural, semantic,
opportunity). Documents the vocabulary of `edge_type` values in `nodes.json` / `edges.json`.

---

## Python configuration constants

After the April 2026 migration only one configuration constant remains in Python.

---

### `_SYSTEM_PROMPT` — `graph/spec/renderer.py`

**Phase:** 5 (spec rendering, LLM instruction)

**What it controls:** The full instruction set given to the LLM for every spec render.
Defines the 5W2H section structure, what the LLM writes vs reproduces verbatim (via
`{{WHEN}}` and `{{HOW}}` sentinels), tone, and grounding requirements.

**Why not migrated:** The prompt contains sentinel token logic (`{{WHEN}}`, `{{HOW}}`),
Python-side formatting instructions, and prompt engineering decisions tightly coupled to
the renderer code. Externalising to YAML would decouple text from structure in a way
that makes it fragile. Stays in Python and gets version control visibility.

**Effect of changes:** Propagates to all re-rendered specs. The highest-leverage single
edit for changing spec style, tone, or section content across the full portfolio. Use
`--force-render` to regenerate after a prompt change.

---

## Summary: what to change to achieve a specific outcome

| Goal | Change |
|------|--------|
| Activate `pricing_decomposition` primitive | Add `pricing_component` to conformed schema JSON + add to `ontology/entity_groups.yaml` |
| Add a new business domain | Add keyword list to `ontology/domain_keywords.yaml` |
| Change initiative priority ranking | Adjust `business_value_score` or `implementation_effort_score` in `ontology/initiative_research.yaml` |
| Add a new initiative | Add a new entry to `initiative_taxonomy` in `ontology/initiative_research.yaml` |
| Add a new capability primitive | Add entry to `ontology/primitives.yaml` + wire into `required_primitives` in `initiative_research.yaml` |
| Improve column inclusion in specs | Add column descriptions to `data/dbt_metadata_enriched.json` |
| Add a new semantic candidate label | Add entry to `ontology/semantic_map.yaml` |
| Add a new grain key name | Add to `candidates` in `ontology/grain_keys.yaml` |
| Change an initiative's spec reading | Edit `literature_quote`, `feasibility_rationale`, or `data_gaps` in `initiative_research.yaml`, then `--force-render` |
| Change spec section structure/tone | Edit `_SYSTEM_PROMPT` in `renderer.py`, then `--force-render` |
| Change a delivery profile | Edit `ontology/delivery_heuristics.yaml`, then `--force-render` |
| Add a new product line tag | Add to `tag_to_product_line` in `ontology/tag_mappings.yaml` |
| Add a new lineage layer | Add to `tag_to_lineage_layer` in `ontology/tag_mappings.yaml` |

---

## File index — ontology/

| File | Runtime | Loaded by | Phase |
|------|---------|-----------|-------|
| `tag_mappings.yaml` | ✓ | DbtMetadataAdapter | 1 |
| `domain_keywords.yaml` | ✓ | DbtMetadataAdapter | 1 |
| `semantic_map.yaml` | ✓ | DbtMetadataAdapter | 1 |
| `grain_keys.yaml` | ✓ | DbtMetadataAdapter | 1 |
| `insurance_entities.yaml` | ✓ | OntologyLoader | 3 |
| `entity_groups.yaml` | ✓ | ConformedFieldBinder | 3 |
| `initiative_research.yaml` | ✓ | InitiativeArchetypeLibrary | 4, 5 |
| `primitives.yaml` | ✓ | CapabilityPrimitiveExtractor | 4 |
| `delivery_heuristics.yaml` | ✓ | SpecRenderer | 5 |
| `gap_types.yaml` | — | (reference only) | — |
| `relationship_types.yaml` | — | (reference only) | — |
