<!-- Last updated: April 2026 -->
# Enterprise Data Product Planner

A compiler-style Python pipeline that reads dbt metadata and a conformed schema, maps
capability primitives, ranks 19 data product initiatives by readiness and business value,
generates implementation specs with dimensional model contracts, and produces a working
mock data product — all without an LLM until Phase 5. It was built against the Liberty
Specialty Markets analytics warehouse but is warehouse-agnostic: all dimensional inference
uses graph signals, not asset name patterns.

---

## What it produces

Three concrete outputs a reader can verify exist after a full pipeline run:

**`output/spec_log/`** — 19 initiative specs (11 full specs + 8 gap briefs), each containing
data requisites, join assessments, dimensional role classification, and LLM-rendered
narrative. An `index.json` file lists all specs with readiness and `spec_id`.

**`output/graph_explorer.html`** — Interactive vis-network explorer with three switchable
layers: Structural (asset lineage), Semantic (entity/domain/metric graph), and Opportunity
(initiatives ranked by composite score, primitives, and gap chains).

**`output/mock_data/pricing_adequacy_monitoring.csv` + `scripts/demo_pricing_adequacy.py`** —
300-row synthetic dataset and a working Streamlit dashboard demonstrating the
pricing adequacy monitoring initiative end-to-end.

---

## Architecture

Six phases, two modes. Phases 1–4 are fully deterministic — zero LLM calls, byte-identical
output on repeated runs of the same input. Phase 5 uses a single bounded LLM call per
initiative to produce a human-readable data-product spec. Phase 6 is scoped but not yet
implemented. See [ARCHITECTURE.md](ARCHITECTURE.md) for cross-cutting design decisions.

**Layer 1 — Canonical ingestion (Phase 1)**
`DbtMetadataAdapter` and `ConformedSchemaAdapter` parse heterogeneous inputs into a typed
`CanonicalBundle` (207 assets, 2,654 columns, 201 lineage edges, 392 business terms). All
objects receive stable SHA-256-derived IDs; re-ingesting the same file is idempotent.

**Layer 2 — Structural graph (Phase 2)**
`StructuralGraphCompiler` converts the bundle into a property graph: Asset, Column, Test,
and DocObject nodes with DEPENDS_ON, HAS_COLUMN, TESTED_BY, and DOCUMENTED_BY edges.
Every node and edge carries an `EvidenceRecord` with a `build_id`. Output: 3,165 nodes
and 3,365 edges in `output/graph/`.

**Layer 3 — Semantic graph (Phase 3)**
`ConformedFieldBinder` maps assets to entity groups defined in the conformed schema
(ENTITY_GROUPS). `SemanticGraphCompiler` adds BusinessEntityNode, DomainNode, and
MetricNode objects with REPRESENTS, BELONGS_TO_DOMAIN, and METRIC_BELONGS_TO_ENTITY edges.
Binding confidence ranges from 0.5–1.0 based on column-overlap score.

**Layer 4 — Opportunity graph (Phase 4)**
`CapabilityPrimitiveExtractor` scans the semantic graph for 9 defined capability primitives.
`OpportunityPlanner` maps those primitives to 19 initiatives from `ontology/initiative_research.yaml`,
computing readiness states and composite scores. `GapAnalyser` produces structured GapRecord
objects for absent primitives. All three node types (primitive, initiative, gap) and their
edges are appended to the graph files. See `docs/phase4_opportunity_layer.md`.

**Layer 5 — Spec generation (Phase 5)**
`SpecAssembler` builds a `SpecDocument` deterministically from graph state: data requisites,
join assessments, output structure, dimensional role classification, and column provenance.
`SpecRenderer` then makes a single bounded LLM call (claude-sonnet-4-20250514) to produce
human-readable narrative around the pre-rendered structural sections. See
`docs/phase5_spec_generator.md`.

**Layer 6 — Architecture alignment (Phase 6, scoped)**
Will produce a structured alignment report comparing the architect's schema groups against
the analytics capability model — identifying schema registration gaps (like
`pricing_decomposition`) that block initiatives. Not yet implemented. See
`docs/phase6_architecture_alignment.md`.

---

## Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) (`pip install uv` or `brew install uv`)
- Docker (optional — only required for Neo4j)
- `ANTHROPIC_API_KEY` in `.env` (only required for Phase 5 rendering)

---

## Installation

```bash
git clone <repo>
cd enterprise-data-product-planner
uv sync --extra dev
cp .env.example .env
# Add ANTHROPIC_API_KEY to .env if using Phase 5
```

---

## Running the pipeline

### Phase 1 — Ingestion

```bash
uv run python scripts/run_phase1.py \
  --dbt-metadata data/dbt_metadata_enriched.json \
  --conformed-schema data/conformed_schema.json \
  --output output/bundle.json
```

Output: `output/bundle.json` — 207 assets, 2,654 columns, 201 lineage edges, 392 business terms.

### Phase 2 — Structural graph

```bash
uv run python scripts/run_phase2.py \
  --bundle output/bundle.json \
  --store json \
  --output output/graph/
```

Output: `output/graph/nodes.json` (3,165 nodes) and `output/graph/edges.json` (3,365 edges).

To use Neo4j instead of the local JSON store:

```bash
docker run -d --name neo4j-dev -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/password neo4j:5
# Set ENABLE_NEO4J=true in .env, then:
uv run python scripts/run_phase2.py \
  --bundle output/bundle.json \
  --store neo4j \
  --output output/graph/
```

### Phase 3 — Semantic enrichment

```bash
uv run python scripts/run_phase3.py \
  --bundle output/bundle.json \
  --graph output/graph
```

Appends BusinessEntityNode, DomainNode, MetricNode, and REPRESENTS / BELONGS_TO_DOMAIN /
METRIC_BELONGS_TO_ENTITY edges to the graph files.

### Phase 4 — Opportunity layer

```bash
uv run python scripts/run_phase4.py \
  --bundle output/bundle.json \
  --graph output/graph \
  --output output/graph
```

Appends CapabilityPrimitiveNode, InitiativeNode, GapNode, and ENABLES / BLOCKED_BY /
COMPOSES_WITH edges.

```bash
# Explore the result
cd output && python -m http.server 8080
# Open http://localhost:8080/graph_explorer.html → switch to Opportunity layer
```

### Phase 5 — Data-product spec generation

Requires `ANTHROPIC_API_KEY` in `.env` for rendered markdown output.

```bash
# All 19 initiatives (~$0.37 total, uses cache — free on re-run)
uv run python scripts/run_phase5.py \
  --bundle output/bundle.json \
  --graph output/graph \
  --initiatives all \
  --render

# Ready-now initiatives only (~$0.12)
uv run python scripts/run_phase5.py \
  --bundle output/bundle.json \
  --graph output/graph \
  --initiatives ready_now

# Specific initiative
uv run python scripts/run_phase5.py \
  --bundle output/bundle.json \
  --graph output/graph \
  --initiatives pricing_adequacy_monitoring \
  --force-render

# Structured JSON only — zero LLM cost
uv run python scripts/run_phase5.py \
  --bundle output/bundle.json \
  --graph output/graph \
  --initiatives all \
  --no-render
```

Output written to `output/spec_log/`: one `.json` + one `.md` per initiative, plus `index.json`.

---

## Tests

```bash
# Recommended on Windows (uses project venv directly)
.venv/Scripts/pytest --tb=short -q

# With coverage
.venv/Scripts/pytest --cov=ingestion --cov=graph

# Stable ordering (avoids a known pre-existing fixture ordering issue)
.venv/Scripts/pytest -p no:randomly
```

Current test count: 303 passing.

---

## Demo — Pricing Adequacy Monitor

```bash
# Generate mock data (300 rows, reproducible seed=42)
uv run python scripts/generate_mock_data.py

# Launch the Streamlit dashboard
uv run streamlit run scripts/demo_pricing_adequacy.py
```

The demo shows a single-page pricing adequacy monitor with sidebar filters, portfolio
overview metrics, an at-risk quote action list, and a RARC decomposition view.
Seeded from `output/mock_data/pricing_adequacy_monitoring.csv` (300 rows × 24 columns).

---

## Current results (Liberty Specialty Markets warehouse)

| Metric | Value |
|--------|-------|
| Assets | 207 |
| Columns | 2,654 |
| Lineage edges | 201 |
| Graph nodes (Phases 2–4) | 3,165+ |
| Entity nodes | 9 |
| Domain nodes | 5 |
| Metric nodes | 33 |
| **Initiatives** | **19** |
| — ready_now | 10 |
| — ready_with_enablement | 1 |
| — needs_foundational_work | 3 |
| — not_currently_feasible | 5 |
| Top initiative | underwriting_decision_support (score: 1.056) |
| Tests passing | 303 |

---

## Known limitations and open questions

- **`pricing_decomposition` primitive is inferred, not confirmed.** The five required columns
  (`tech_gnwp`, `modtech_gnwp`, `sold_gnwp`, `tech_elc`, `commission`) exist in
  `ll_quote_policy_detail` by exact name, but `pricing_component` is not registered as a
  conformed schema group. Seven initiatives are affected. Remedy: add `pricing_component` to
  `ENTITY_GROUPS` in `graph/semantic/conformed_binder.py` and to the conformed schema.
  See `docs/backlog.md` for the full remediation plan.

- **`product_line_performance_dashboard` has empty measures.** The
  `product_line_segmentation` primitive covers 40+ assets, but column-level descriptions
  are absent across most product line assets. The positive-inclusion filter correctly
  excludes undescribed columns, leaving the measures section empty in the spec.

- **`sold_to_plan` absent from warehouse.** The `profitability_decomposition` primitive
  matches 3/4 required columns. `sold_to_plan` does not exist in any warehouse asset;
  direct plan vs. sold comparison cannot be computed without a source system addition.

- **`hx_rate_monitoring` has no column metadata.** Zero columns catalogued. Any initiative
  relying on historical rate monitoring carries schema-discovery risk.

- **Test coverage on warehouse assets is critically low (0–4% per asset).** dbt tests exist
  on 96 columns out of 2,654. Data quality assertions cannot be made with confidence.

- **Phase 6 not implemented.** The architecture alignment report comparing schema groups
  to capability primitives is scoped but not yet built. Estimated effort: 1–2 days.

---

## Repository structure

```
enterprise-data-product-planner/
├── ingestion/              # Phase 1 — canonical ingestion
│   ├── contracts/          # Pydantic v2 data contracts (CanonicalBundle et al.)
│   ├── adapters/           # Source-specific parsers (dbt, conformed schema, stubs)
│   ├── normalisation/      # Hashing, name/type/role utilities
│   └── pipeline.py         # IngestionPipeline orchestrator
├── graph/                  # Phases 2–5 — graph compilation and analysis
│   ├── schema/             # Node and edge dataclasses
│   ├── compiler/           # StructuralGraphCompiler, evidence model
│   ├── store/              # JsonGraphStore, Neo4jGraphStore
│   ├── semantic/           # Phase 3: ConformedFieldBinder, entity/domain binding
│   ├── opportunity/        # Phase 4: PRIMITIVE_DEFINITIONS, planner, gap analyser
│   └── spec/               # Phase 5: SpecAssembler, SpecRenderer, SpecLog
├── ontology/               # YAML controlled vocabularies
│   ├── initiative_research.yaml    # 19 initiatives with research citations
│   ├── insurance_entities.yaml     # 10 entity definitions
│   ├── gap_types.yaml              # 9 gap type definitions
│   ├── relationship_types.yaml     # All edge types by layer
│   └── tag_mappings.yaml           # Product line and lineage layer tags
├── scripts/                # CLI entry points (Typer + Rich)
│   ├── run_phase1.py       # Ingestion
│   ├── run_phase2.py       # Structural compilation
│   ├── run_phase3.py       # Semantic enrichment
│   ├── run_phase4.py       # Opportunity layer
│   ├── run_phase5.py       # Spec generation
│   ├── generate_mock_data.py       # Synthetic data for demos (seed=42)
│   ├── demo_pricing_adequacy.py    # Streamlit pricing adequacy demo
│   ├── validate_graph.py           # 7 structural correctness checks
│   └── query_graph.py              # 4 analytical queries
├── tests/                  # pytest + hypothesis test suite (303 tests)
│   ├── ingestion/          # Phase 1 adapter and contract tests
│   ├── graph/              # Phases 2–4 compiler and opportunity tests
│   └── spec/               # Phase 5 assembler, renderer, and log tests
├── data/                   # Input files (dbt metadata, conformed schema)
├── docs/                   # Architecture and design documentation
├── output/                 # Generated artefacts (git-ignored except graph_explorer.html)
│   ├── bundle.json         # Phase 1 output
│   ├── graph/              # Phase 2–4 output (nodes.json, edges.json)
│   ├── mock_data/          # Demo synthetic data
│   ├── spec_log/           # Phase 5 output (per-initiative JSON + markdown)
│   └── graph_explorer.html # Interactive vis-network explorer
├── api/                    # FastAPI service (stub — not yet implemented)
├── storage/                # SQLAlchemy models (stub — not yet implemented)
├── pyproject.toml          # uv/hatch project metadata and dependencies
└── .env.example            # Environment variable template
```

---

## Documentation index

| Document | What it covers | Last updated |
|----------|---------------|--------------|
| `ARCHITECTURE.md` | Cross-cutting design decisions: determinism principle, two-model architecture, dimensional role inference, data requisite contract, LLM usage, graph explorer encoding | April 2026 |
| `docs/phase1_contracts.md` | CanonicalBundle, CanonicalAsset, CanonicalColumn, CanonicalLineageEdge, CanonicalBusinessTerm, normalisation utilities | April 2026 |
| `docs/phase1_execution_flow.md` | Ingestion pipeline flow, adapter sequence, merge strategy | April 2026 |
| `docs/phase2_graph_schema.md` | Node types, edge types, property schemas, golden run counts | April 2026 |
| `docs/evidence_model.md` | EvidenceRecord structure, confidence constants, build_id tracking, rule_id namespaces | April 2026 |
| `docs/phase3_design_brief.md` | Graph topology analysis, connected components, semantic design decisions | April 2026 |
| `docs/phase4_opportunity_layer.md` | Capability primitives, initiative portfolio, readiness model, gap analysis, graph explorer visual encoding | April 2026 |
| `docs/phase5_spec_generator.md` | SpecDocument schema (incl. DataRequisite, JoinAssessment, OutputStructure), LLM policy, dimensional role inference, cost model, CLI usage | April 2026 |
| `docs/phase6_architecture_alignment.md` | Architecture alignment report design, pricing_decomposition case study, output artefacts | April 2026 |
| `docs/backlog.md` | Deferred items, known limitations, open schema gaps | April 2026 |
| `docs/artifacts/dbt_metadata_spec.md` | Expected dbt manifest JSON structure and version compatibility | April 2026 |
| `docs/artifacts/conformed_schema_spec.md` | Conformed schema JSON structure, adapter behaviour, and the pricing_component gap | April 2026 |

---

## Key design decisions

1. **Determinism before LLM.** Phases 1–4 are fully deterministic. The LLM enters only
   in Phase 5 as narrator, not reasoner. The graph never changes based on how a question
   is phrased. See `ARCHITECTURE.md`.

2. **IDs are hashes, not UUIDs.** `stable_hash(*parts)` produces the first 16 hex chars
   of SHA-256. Determinism across reruns is more important than global uniqueness.

3. **Positive inclusion over negative exclusion.** Output columns are included only if they
   have semantic signal (description, `semantic_candidate`, or primitive match). No blocklists.

4. **Schema-grounded dimensional inference.** `_infer_table_type()` uses `lineage_layer`,
   column composition ratios, and grain key count. No asset name pattern matching.

5. **The spec is a contract.** The `data_requisite` section of every spec is pre-rendered
   from deterministic graph state, not LLM-generated. It is a build contract, not a narrative.

6. **Inferred primitives are surfaced, not silenced.** When the pipeline detects a capability
   analytically but the schema group is unregistered, the primitive appears as amber (inferred)
   rather than being quietly dropped. The remedy is a schema registration, not an ETL change.

7. **JsonGraphStore ships on day 1.** No one needs a running Neo4j to develop or test.
   Neo4j is an optional production store behind an `ENABLE_NEO4J` environment flag.

8. **Every edge has a `build_id`.** Stale graph artefacts from old builds are traceable
   and purgeable. Each phase prefixes its build_id (`build_`, `sem_`, `opp_`).

9. **Specs are cached by `(initiative_id, graph_build_id)`.** Re-running Phase 5 over an
   unchanged graph is free. LLM cost is only incurred on the first render or with `--force-render`.
