# Phase 3 Design Brief

---

## Graph topology

*Derived from targeted structural analysis of the Phase 2 output (207 assets,
201 DEPENDS_ON edges, build `build_693e66d1c1ed580f`).*

### Connected components

Union-find over DEPENDS_ON edges (treated as undirected) reveals three components
plus a pool of isolated assets:

| component | assets | description |
|-----------|--------|-------------|
| Main cluster | 187 | All HX and LL product lines; the vast majority of the graph |
| `dp_landing → dp_raw` | 2 | Standalone D&P sub-pipeline; no edges to the main cluster |
| `bad_records` pair | 2 | Two distinct assets both named `bad_records` (different node IDs); one DEPENDS_ON the other — likely a naming collision in the source dbt project |
| Isolated (no edges) | 16 | Source-table leaves with zero upstream and zero downstream connections |

**Main cluster (187 assets):**
- Root by downstream reach: `hx_landing` (140 reachable descendants)
- Domain coverage: underwriting (156 asset-domain tags), pricing (109),
  portfolio_monitoring (36), profitability (35), distribution (11)
- Grain keys: `quote_id` (147 assets), `layer_id` (12), `pas_id` (10),
  `coverage_id` (3), `policy_id` (2)
- `quote_id` is effectively the universal grain — present in 79% of connected assets

**D&P micro-cluster (`dp_landing → dp_raw`):**
- Completely disconnected from the main graph
- `dp_landing` has domains `[pricing, underwriting]`, grain `[quote_id]`
- Represents a Directors & Professionals (or Direct Products) sub-pipeline
  that shares no lineage with HX/LL models
- Phase 3 implication: `dp_landing` is a candidate root node for a separate
  D&P product entity that will not inherit domain assignments from the main cluster

### Bridge assets

None. The three components are fully disconnected — no DEPENDS_ON edge crosses
a component boundary. There are no bridge assets in the current graph.

Phase 3 implication: domain assignment, entity mapping, and semantic compilation
can treat the main cluster and D&P micro-cluster as independent subgraphs. The
`bad_records` pair should be flagged as a data quality anomaly (duplicate name)
before Phase 3 semantic compilation runs.

### eupi / additional coverage cluster

24 assets contain `eupi_` or `additional_coverage` in their name. These are
**not** a separate cluster — they are fully embedded in the main 187-asset
component, connected to it via `core_policy_view`.

**Structure (three-layer pipeline):**
```
eupi_*          (7 source tables, zero upstream, zero columns in some)
    ↓ DEPENDS_ON
ll_eupi_*       (7 "liberty link" aggregates, one-to-one with eupi_*)
    ↓ DEPENDS_ON
tbl_EU_PI_*     (10 terminal read views, zero columns — external Redshift views)
    ↓ DEPENDS_ON
core_policy_view  (connects eupi sub-pipeline to the main policy graph)
```

**Entity interpretation:** EU PI = European Professional Indemnity product line.
The `eupi_*` sources represent pre-aggregated actuarial inputs specific to the
EU PI book of business. The `ll_*` prefix ("Liberty Link") is a consistent
naming pattern across the whole graph indicating a staging/standardisation layer.

**Grain:** `quote_id` throughout — aligned with the main cluster's universal grain.

**Domains:** pricing, underwriting, profitability (no distribution signal).

**Phase 3 implication:** `eupi_exposure_model` (44 columns) is the richest source
of EU PI semantic signal. It should map to a distinct `BusinessEntity` rather than
being collapsed into the generic `exposure_model` entity. The seven `eupi_*` root
nodes are strong candidates for a `product_line = "eu_pi"` entity attribute that
Phase 3 can propagate downstream via lineage.

### Distribution domain gap

Distribution is **not absent** from the graph — 11 assets have `distribution` in
their `domain_candidates` list. The apparent gap in the HTML explorer arises
because distribution is almost always a *secondary* domain: assets are color-coded
by their first/primary domain candidate, which is typically `underwriting` or
`pricing`. Distribution assets therefore appear in those colors.

**What the current keyword set captures** (`['broker', 'channel', 'branch']`):
- `hx_general_aviation_summary_brokerage` and its mirror
- `hx_quote_setup` / `ll_quote_setup` / `quote_setup` (columns: `branch`, `broker_primary`)
- `hx_do_layer_pricing` / `do_layer_pricing` (column: `brokerage_pct`)
- `hx_do_expiring_layers` / `do_expiring_layers` (column: `brokerage_pct`)
- `hx_general_aviation_summary_details` (column: `summary_hl_brokerage`)

**Column-level signals not yet captured** (present in the graph but missed by
the current keyword corpus):
- `facility`, `master_umr_lineslip_binder` — in `hx_do_quote` (85 cols), the
  widest asset in the graph
- `facility_fee`, `facility_fee_flag` — in `general_aviation_summary_brokerage`
- `coverholder`, `mga`, `binder`, `delegated`, `lineslip` — alt distribution vocab

**Fix:** Add `['coverholder', 'mga', 'binder', 'lineslip', 'delegated', 'facility_fee']`
to the `distribution` entry in `DOMAIN_KEYWORDS`. This would promote `hx_do_quote`
and `do_quote` to distribution co-candidates, significantly raising the domain's
apparent footprint in the graph. This is a Phase 3 backlog item — it requires
review of what `facility` means in context (Lloyd's facility vs. a building)
before committing to the keyword.
