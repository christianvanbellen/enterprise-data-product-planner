# Composable Data Product Planner — Phase 1 & 2
## Foundation Architecture Plan

> **Product thesis:** A compiler-style system that converts dbt metadata, SQL lineage, conformed schemas, and ERD context into a deterministic multi-layer graph of analytical assets and business semantics. The moat is not LLM reasoning over raw data — it is the structured intermediate pipeline that makes every output inspectable, repeatable, and evidence-backed.

---

## Repository structure

```
composable-planner/
├── README.md
├── pyproject.toml
├── .env.example
│
├── ingestion/                    # Phase 1 — Canonical ingestion
│   ├── __init__.py
│   ├── contracts/
│   │   ├── __init__.py
│   │   ├── asset.py              # CanonicalAsset, CanonicalColumn
│   │   ├── lineage.py            # CanonicalLineageEdge
│   │   ├── business.py           # CanonicalBusinessTerm, ConformedConcept
│   │   └── bundle.py             # CanonicalBundle (merge / serialise)
│   ├── adapters/
│   │   ├── __init__.py
│   │   ├── base.py               # BaseAdapter ABC
│   │   ├── dbt_metadata.py       # DbtMetadataAdapter
│   │   ├── conformed_schema.py   # ConformedSchemaAdapter
│   │   ├── info_schema.py        # InformationSchemaAdapter (stub)
│   │   ├── glossary.py           # GlossaryAdapter (stub)
│   │   └── erd.py                # ERDAdapter (stub)
│   ├── normalisation/
│   │   ├── __init__.py
│   │   ├── names.py              # normalize_name, normalize_text
│   │   ├── dtypes.py             # classify_data_type
│   │   ├── roles.py              # infer_column_role
│   │   └── hashing.py            # stable_hash, version_hash
│   └── pipeline.py               # IngestionPipeline: runs adapters → CanonicalBundle
│
├── graph/                        # Phase 2 — Graph compiler
│   ├── __init__.py
│   ├── schema/
│   │   ├── __init__.py
│   │   ├── nodes.py              # Node dataclasses: Asset, Column, Transformation …
│   │   └── edges.py              # Edge dataclasses + EdgeType enum
│   ├── compiler/
│   │   ├── __init__.py
│   │   ├── structural.py         # StructuralGraphCompiler
│   │   ├── sql_lineage.py        # SQL parsing → column-level DERIVES_FROM
│   │   └── evidence.py           # EvidenceRecord, confidence scoring
│   ├── store/
│   │   ├── __init__.py
│   │   ├── neo4j_store.py        # Neo4jGraphStore: upsert, query, tag build
│   │   └── json_store.py         # JsonGraphStore: local export (no Neo4j dep)
│   └── build.py                  # GraphBuild: orchestrates compiler + store
│
├── ontology/                     # Controlled vocabularies (Phase 3 prep)
│   ├── insurance_entities.yaml
│   ├── insurance_domains.yaml
│   ├── relationship_types.yaml
│   └── gap_types.yaml
│
├── storage/                      # Postgres models (SQLAlchemy / SQLModel)
│   ├── __init__.py
│   ├── models.py
│   └── migrations/
│
├── api/                          # FastAPI service (thin wrapper)
│   ├── __init__.py
│   ├── main.py
│   └── routes/
│       ├── ingest.py
│       └── graph.py
│
├── tests/
│   ├── ingestion/
│   │   ├── test_dbt_adapter.py
│   │   ├── test_conformed_adapter.py
│   │   └── golden/               # golden dbt_metadata_enriched.json slice
│   └── graph/
│       ├── test_structural_compiler.py
│       └── golden/               # expected node/edge counts for golden input
│
├── scripts/
│   ├── run_phase1.py             # CLI: ingest → print bundle summary
│   └── run_phase2.py             # CLI: ingest → compile → export graph JSON
│
└── docs/
    ├── phase1_contracts.md       # Data contracts (this doc section)
    ├── phase2_graph_schema.md    # Graph node/edge schema
    └── evidence_model.md         # How evidence fields work
```

---

## Phase 1 — Canonical ingestion

### Goals

1. Parse every heterogeneous input into a single internal schema.
2. Produce stable, hash-identified objects across reruns.
3. Persist a `CanonicalBundle` as the durable hand-off to Phase 2.

### Inputs (MVP scope)

| Source | Adapter | Status |
|--------|---------|--------|
| dbt enriched metadata JSON | `DbtMetadataAdapter` | Implemented (from ChatGPT session code) |
| Conformed / bookends schema JSON | `ConformedSchemaAdapter` | Implemented |
| Information schema export | `InformationSchemaAdapter` | Stub |
| Business glossary / catalogue | `GlossaryAdapter` | Stub |
| ERD JSON export | `ERDAdapter` | Stub |
| Raw SQL transformation files | `SqlArtifactAdapter` | Future |

### Canonical contracts

#### `CanonicalAsset`

```python
class CanonicalAsset(BaseModel):
    internal_id: str          # stable hash of {source}:{unique_id}
    asset_type: Literal[
        "dbt_model", "table", "view",
        "source_table", "conformed_concept_group", "unknown"
    ]
    name: str
    normalized_name: str      # snake_case, lowercase
    database: Optional[str]
    schema_name: Optional[str]
    path: Optional[str]
    description: Optional[str]
    tags: List[str]
    materialization: Optional[str]
    row_count: Optional[int]
    size_mb: Optional[float]
    grain_keys: List[str]     # columns matching known identifier patterns
    domain_candidates: List[str]
    is_enabled: bool
    version_hash: str
    provenance: Provenance
```

#### `CanonicalColumn`

```python
class CanonicalColumn(BaseModel):
    internal_id: str          # stable hash of {asset_id}:{column_name}
    asset_internal_id: str
    name: str
    normalized_name: str
    description: Optional[str]
    raw_data_type: Optional[str]
    data_type_family: str     # numeric | boolean | timestamp | date | string | semi_structured | unknown
    column_role: str          # identifier | measure | categorical_attribute | timestamp | boolean_flag | numeric_attribute | attribute | semi_structured | unknown
    ordinal_position: Optional[int]
    is_nullable: Optional[bool]
    tests: List[str]
    meta: Dict[str, Any]
    semantic_candidates: List[str]   # controlled vocab hints (e.g. "quote", "premium")
    version_hash: str
    provenance: Provenance
```

#### `CanonicalLineageEdge`

```python
class CanonicalLineageEdge(BaseModel):
    internal_id: str
    source_asset_id: str
    target_asset_id: str
    relation_type: Literal["depends_on", "downstream_of"]
    derivation_method: Literal["explicit_metadata", "reverse_index", "parsed_sql"]
    confidence: float         # 0.0–1.0
    version_hash: str
    provenance: Provenance
```

#### `CanonicalBusinessTerm`

```python
class CanonicalBusinessTerm(BaseModel):
    internal_id: str
    term_type: Literal["conformed_concept", "business_term"]
    name: str
    normalized_name: str
    parent_term_id: Optional[str]
    attributes: Dict[str, Any]
    version_hash: str
    provenance: Provenance
```

#### `Provenance`

```python
class Provenance(BaseModel):
    source_system: str        # "dbt" | "conformed_schema" | "glossary" | "erd"
    source_type: str          # adapter class label
    source_native_id: Optional[str]
    extraction_timestamp_utc: str
    raw_record_hash: Optional[str]
```

#### `CanonicalBundle`

```python
class CanonicalBundle(BaseModel):
    assets: List[CanonicalAsset]
    columns: List[CanonicalColumn]
    lineage_edges: List[CanonicalLineageEdge]
    business_terms: List[CanonicalBusinessTerm]
    metadata: Dict[str, Any]

    def merge(self, other: "CanonicalBundle") -> "CanonicalBundle": ...
    def to_json(self, path: Path) -> None: ...
    def summary(self) -> str: ...   # human-readable count report
```

### ID strategy

Every object gets a **stable internal ID** that is deterministic across reruns:

```
asset_id   = "asset_" + sha256[:16]( f"{source_system}::{unique_id}" )
column_id  = "col_"   + sha256[:16]( f"{asset_id}::{column_name}" )
edge_id    = "lin_"   + sha256[:16]( f"{source_asset_id}::{target_asset_id}::{relation_type}" )
term_id    = "term_"  + sha256[:16]( f"{source_system}::{path_token_chain}" )
```

This ensures that re-ingesting the same file produces identical IDs. Downstream graph operations can upsert safely.

### Field normalisation rules

| Target field | Rule |
|---|---|
| `normalized_name` | `strip → replace(- →_) → sub(\s+→_) → sub([^a-z0-9_]→_) → lower()` |
| `data_type_family` | keyword match on raw datatype string |
| `column_role` | rules: name ending `_id` → identifier; numeric → measure; `*status*/*type*` → categorical; etc. |
| `tags` | lowercase + dedup |
| `description` | collapse whitespace, strip None |

### Domain inference (DbtMetadataAdapter)

Infer `domain_candidates` from a keyword scan over model name + description + tags + column names:

```yaml
pricing:        [premium, rate, pricing, elr, commission, rarc]
profitability:  [profitability, sold_to, modtech, tech_gnwp, tech_gnwp]
underwriting:   [underwriter, quote, coverage, policyholder]
distribution:   [broker, channel, branch]
portfolio_monitoring: [inflation, change, monitoring, expiring]
```

### Column semantic candidates (DbtMetadataAdapter)

Pre-seeded synonym map for insurance domain:

```yaml
quote_id:          quote
layer_id:          coverage_layer
coverage_id:       coverage
policyholder_name: policyholder
broker_primary:    broker
underwriter:       underwriter
jurisdiction:      jurisdiction
premium:           premium
exposure:          exposure
commission:        commission
elr:               expected_loss_ratio
rarc:              risk_adjusted_rate_change
```

### Acceptance criteria — Phase 1

- [ ] ≥ 95 % of dbt model objects loaded as `CanonicalAsset` nodes
- [ ] ≥ 95 % of columns parsed as `CanonicalColumn` objects
- [ ] All explicit `upstream_dependencies` edges emitted as `CanonicalLineageEdge` (confidence 1.0)
- [ ] Re-running ingestion on identical input produces byte-identical JSON output
- [ ] `CanonicalBundle.summary()` prints counts for each object type + metadata completeness %

---

## Phase 2 — Structural graph compilation

### Goals

1. Compile a near-deterministic structural graph from the canonical bundle.
2. Store in Neo4j (or local JSON export for dev).
3. Tag every node and edge with full evidence metadata.
4. Enable lineage traversal queries.

### Graph node types

| Label | Source | Key fields |
|---|---|---|
| `SourceSystem` | adapter metadata | `system_id`, `name`, `type` |
| `Schema` | asset.schema_name | `schema_id`, `database`, `schema_name` |
| `Asset` | CanonicalAsset | `asset_id`, `name`, `asset_type`, `materialization`, `domain_candidates` |
| `Column` | CanonicalColumn | `col_id`, `name`, `data_type_family`, `column_role`, `semantic_candidates` |
| `Transformation` | SQL text (future) | `transform_id`, `sql_snippet`, `operation_type` |
| `Test` | dbt tests | `test_id`, `test_type`, `column_name`, `status` |
| `DocObject` | dbt descriptions | `doc_id`, `has_description` |

### Graph edge types

| Type | From → To | Derivation |
|---|---|---|
| `CONTAINS` | Schema → Asset | deterministic |
| `HAS_COLUMN` | Asset → Column | deterministic |
| `DEPENDS_ON` | Asset → Asset | explicit metadata (confidence 1.0) |
| `DERIVES_FROM` | Column → Column | parsed SQL (confidence varies) |
| `TESTED_BY` | Column → Test | deterministic |
| `DOCUMENTED_BY` | Asset → DocObject | deterministic |

### Evidence model

Every node and edge carries a standard evidence block:

```python
@dataclass
class EvidenceRecord:
    created_by: str           # e.g. "structural_compiler_v1"
    rule_id: str              # e.g. "lineage.explicit_upstream"
    evidence_sources: List[Dict]  # [{"type": "manifest_field", "value": "depends_on.nodes"}]
    confidence: float
    review_status: str        # "auto" | "confirmed" | "overridden" | "rejected"
    build_id: str             # ties back to a graph build run
    timestamp_utc: str
```

Confidence scale:

| Derivation | Confidence |
|---|---|
| Explicit dbt manifest dependency | 1.0 |
| Direct `SELECT col FROM table` lineage | 0.95 |
| Expression / computed column lineage | 0.75 |
| Ambiguous union / CTE lineage | 0.50 |
| Name-similarity inference (future) | ≤ 0.40 |

### Compiler pipeline

```
CanonicalBundle
      │
      ▼
1. create_asset_nodes()        # one Asset node per CanonicalAsset
      │
      ▼
2. create_column_nodes()       # one Column per CanonicalColumn, HAS_COLUMN edges
      │
      ▼
3. create_containment_edges()  # Schema → Asset CONTAINS
      │
      ▼
4. create_lineage_edges()      # CanonicalLineageEdge → DEPENDS_ON (confidence 1.0)
      │
      ▼
5. create_sql_column_edges()   # parsed SQL → DERIVES_FROM (optional, gated)
      │
      ▼
6. attach_test_nodes()         # CanonicalColumn.tests → TESTED_BY
      │
      ▼
7. attach_doc_nodes()          # description presence → DOCUMENTED_BY
      │
      ▼
GraphBuildArtifact (nodes + edges + build_id + coverage_report)
```

Each step is independently testable. Step 5 is gated behind a `--with-sql-lineage` flag so early runs don't require full SQL parsing.

### Graph store interface

```python
class GraphStore(ABC):
    def upsert_nodes(self, nodes: List[GraphNode]) -> int: ...
    def upsert_edges(self, edges: List[GraphEdge]) -> int: ...
    def tag_build(self, build_id: str, metadata: dict) -> None: ...
    def query_lineage(self, asset_id: str, direction: str, depth: int) -> List[GraphNode]: ...
    def export_json(self, path: Path) -> None: ...
```

Two implementations:
- `Neo4jGraphStore` — production, uses `neo4j` Python driver
- `JsonGraphStore` — local dev, no external dependencies, outputs `nodes.json` + `edges.json`

### Neo4j Cypher patterns

```cypher
// Upsert an Asset node
MERGE (a:Asset {asset_id: $asset_id})
SET a += $properties
SET a.build_id = $build_id

// Create DEPENDS_ON edge
MATCH (src:Asset {asset_id: $source_id})
MATCH (tgt:Asset {asset_id: $target_id})
MERGE (src)-[r:DEPENDS_ON]->(tgt)
SET r.confidence = $confidence
SET r.build_id = $build_id

// Lineage traversal (downstream, 5 hops)
MATCH path = (start:Asset {asset_id: $id})-[:DEPENDS_ON*1..5]->(downstream)
RETURN downstream.name, downstream.domain_candidates, length(path) AS hops
ORDER BY hops
```

### Graph build metadata

Every build is tagged with a `GraphBuildArtifact`:

```python
@dataclass
class GraphBuildArtifact:
    build_id: str
    ingestion_run_id: str
    timestamp_utc: str
    node_counts: Dict[str, int]   # {"Asset": 110, "Column": 2400, ...}
    edge_counts: Dict[str, int]
    lineage_coverage_pct: float   # % of assets with at least one lineage edge
    unresolved_lineage: List[str] # asset IDs with missing upstream references
    compiler_version: str
    ontology_version: str
```

### SQL lineage extraction (gated, Phase 2 optional)

Use `sqlglot` for parsing. Strategy:

1. Parse compiled SQL into AST.
2. Walk `SELECT` expressions; resolve column references to upstream table aliases.
3. Emit `DERIVES_FROM` edges for direct column mappings (confidence 0.95).
4. Flag expressions (CASE, functions, arithmetic) as `unresolved_expression` (confidence 0.75).
5. Never hallucinate — if the derivation is ambiguous, emit a `review_required` edge instead.

```python
import sqlglot

def extract_column_lineage(sql: str, asset_id: str) -> List[CanonicalLineageEdge]:
    ...
```

### Acceptance criteria — Phase 2

- [ ] Every `CanonicalAsset` becomes an `Asset` node in the graph
- [ ] Every explicit upstream dependency becomes a `DEPENDS_ON` edge (confidence 1.0)
- [ ] Structural graph output is byte-identical across two runs on same input
- [ ] `GraphBuildArtifact` reports ≥ 90 % lineage coverage for the insurance dataset
- [ ] Lineage traversal query: "all models descending from hx_raw" returns correct set
- [ ] `JsonGraphStore` export works with zero external dependencies (local dev)

---

## Tech stack

| Layer | Choice | Rationale |
|---|---|---|
| Language | Python 3.11+ | matches existing Phase 1 code |
| Data contracts | Pydantic v2 | strict typing, JSON serialisation |
| SQL parsing | sqlglot | multi-dialect, no server needed |
| Graph store | Neo4j 5.x (+ JsonGraphStore fallback) | graph traversal; local dev unblocked |
| Relational store | PostgreSQL 15 (+ SQLModel) | canonical tables, build metadata |
| API layer | FastAPI | thin wrapper, async |
| Pipeline orchestration | Dagster (optional Phase 2+) | observable, restartable |
| Testing | pytest + hypothesis | golden dataset + property tests |
| Package management | uv / pyproject.toml | modern, fast |

---

## Environment setup

```bash
# Clone and install
git clone <repo>
cd composable-planner
uv sync

# Copy env template
cp .env.example .env

# Start local Neo4j (Docker)
docker run -d \
  --name neo4j-dev \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/password \
  neo4j:5

# Run Phase 1 (outputs canonical bundle to ./output/)
python scripts/run_phase1.py \
  --dbt-metadata data/dbt_metadata_enriched.json \
  --conformed-schema data/output_schema_conformed_data_only.json \
  --output output/bundle.json

# Run Phase 2 (compiles bundle → graph JSON, or Neo4j if configured)
python scripts/run_phase2.py \
  --bundle output/bundle.json \
  --store json \
  --output output/graph/
```

---

## `.env.example`

```env
# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password

# Postgres
DATABASE_URL=postgresql://user:pass@localhost:5432/composable_planner

# Feature flags
ENABLE_SQL_LINEAGE=false
ENABLE_NEO4J=false    # false = JsonGraphStore (local dev)

# Ontology
ONTOLOGY_DIR=./ontology
INSURANCE_DOMAIN=true
```

---

## Testing strategy

### Golden dataset test

Use the uploaded `dbt_metadata_enriched.json` as the golden input. Assert:

```python
# Phase 1 golden test
bundle = DbtMetadataAdapter().parse_file(Path("tests/ingestion/golden/dbt_metadata_enriched.json"))
assert len(bundle.assets) >= 5                          # hx_quote_setup + 4 siblings
assert len(bundle.lineage_edges) >= 4                   # known upstream deps
assert bundle.assets[0].internal_id == bundle.assets[0].internal_id  # stable ID

# Phase 2 golden test
artifact = StructuralGraphCompiler().compile(bundle)
assert artifact.node_counts["Asset"] == len(bundle.assets)
assert artifact.edge_counts["DEPENDS_ON"] == len(bundle.lineage_edges)
assert artifact.lineage_coverage_pct >= 0.90
```

### Property tests (hypothesis)

```python
@given(st.text())
def test_normalize_name_idempotent(s):
    assert normalize_name(normalize_name(s)) == normalize_name(s)

@given(st.text())
def test_stable_hash_deterministic(s):
    assert stable_hash(s) == stable_hash(s)
```

---

## What Phase 3 needs from this foundation

Phase 3 (Semantic graph) will build on top of Phase 1 + 2 without modifying them:

- It reads `CanonicalColumn.semantic_candidates` and `CanonicalAsset.domain_candidates` as pre-computed hints
- It reads `CanonicalBusinessTerm` objects from the conformed schema adapter
- It queries the Phase 2 structural graph for co-occurrence patterns (which columns appear together, which assets share grain keys)
- It writes new node types (`BusinessEntity`, `Metric`, `Domain`) and new edge types (`REPRESENTS`, `BELONGS_TO_DOMAIN`) into the same graph store alongside the structural layer

**No Phase 1 or Phase 2 contracts change.** Semantic enrichment is an additive layer.

---

## Key design decisions (do not change without discussion)

1. **IDs are hashes, not UUIDs.** Determinism across reruns is more important than global uniqueness.
2. **Confidence is a float 0–1 on every edge.** Never emit an edge without a confidence score.
3. **LLMs are not in Phase 1 or Phase 2.** Zero model calls in ingestion or structural compilation.
4. **JsonGraphStore ships on day 1.** No one should need a running Neo4j to develop or test.
5. **CanonicalBundle is the only hand-off between Phase 1 and Phase 2.** Adapters do not write to the graph directly.
6. **Every edge has a `build_id`.** Stale graph artifacts from old builds are traceable and purgeable.
