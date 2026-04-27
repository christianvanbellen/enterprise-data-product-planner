# Risks detected — Pack B specs vs implementation reality

Recorded during the Tier 1 mart build (2026-04-26). Each risk lists the
initiative whose spec exhibits it, the symptom in `output/spec_log/<init>/current.md`,
the actual Pack B reality, and how the implementation in `dbt_demo/models/marts/`
resolves it. The intent of this doc is two-fold:

1. Feed back into the planner / spec-generator so future spec passes are
   coherent with the warehouse the consumer actually has.
2. Give consumers of the demo marts an audit trail of judgment calls.

---

## R-1 — pricing_adequacy_monitoring spec lists a 4th source that doesn't exist in Pack B  ✅ RESOLVED

**Original symptom.** Spec listed four sources including a separate
layer-grain `rate_monitoring` table; original Pack B shipped only one
quote-grain rate seed.

**Status: RESOLVED via Pack B extension (2026-04-26).** The mock generator
now emits a layer-grain `rate_monitoring.csv` seed at (quote_id, layer_id)
grain alongside the existing quote-grain `rate_monitoring_total_our_share_usd.csv`.
The two reconcile by construction: the quote-grain seed is produced
exclusively by **premium-weighted aggregation** of the layer-grain seed
(weight = `tech_gnwp_full` per layer). A consumer who premium-weights the
layer rows back up will exactly match the quote-grain seed.

**What this unlocks.** The canonical `mart_pricing_adequacy` (and its
`view_portfolio_drift_heatmap`) LEFT JOINs `stg_rate_monitoring_layer`
on `(quote_id, layer_id)` — each layer reports its own rate change
rather than inheriting a quote-level average. The original "carry
quote-level rarc down to every layer" artefact is gone.

**Planner / spec implication.** Pack B definition lesson: when the
underlying source warehouse offers signal at multiple grains, the minimal
mock set must preserve the finest grain a `ready_now` initiative
genuinely needs — not collapse to a shared coarser grain for compactness.
The reconciliation invariant (layer aggregates → quote) is the design
pattern that lets the demo offer both grains without inconsistency.

---

## R-2 — layer_rate_adequacy_monitoring spec assumes layer_id on rate_monitoring  ✅ RESOLVED

**Original symptom.** Spec joined `rate_monitoring` on `layer_id + quote_id`;
original Pack B's seed had no `layer_id`.

**Status: RESOLVED via Pack B extension (2026-04-26).** Same fix as R-1.
The new `stg_rate_monitoring_layer` view exposes the layer-grain seed
and the canonical `mart_pricing_adequacy` (consumed by
`view_layer_attachment_ladder`) joins on the full `(quote_id, layer_id)`
key as the spec prescribed.

**Planner / spec implication.** Same root cause as R-1. Pack B definition
should drive from the spec's prescribed join keys: every join key the
target initiatives need must resolve to a real column on a real seed.

---

## R-3 — rate_change_attribution_analytics spec under-utilises the rate-monitoring seed

**Spec says.** Pull `expiring_inception_date` and `expiring_expiry_date` only
from `rate_monitoring_total_our_share_usd`.

**Pack B reality.** That same seed carries the *entire* rate-change
decomposition: `gross_rarc`, `net_rarc`, `claims_inflation`,
`breadth_of_cover_change`, `gross_exposure_change`,
`gross_limits_and_excess_change`, `policy_term_change`, `other_changes`.

**Why this matters.** The whole point of the
rate_change_attribution_analytics initiative is to surface the *named drivers*
of headline rate movement. The spec selects two date columns and ignores the
seven decomposition columns sitting next to them.

**Resolution in code.** The canonical `mart_pricing_adequacy` (consumed by `view_rate_change_waterfall`)
pulls all decomposition columns through and adds a derived
`gross_rarc_residual_check` column to flag rows where the components don't
algebraically reconcile to gross_rarc.

**Planner / spec implication.** The spec's column-selection step is too
conservative when an asset is multi-purpose. Recommend a generation rule:
"if an asset is the primary source for an initiative's named primitive
(here: rate_change_monitoring), pull every column tagged to that primitive,
not just the columns named in the join key."

---

## R-4 — rate_change_attribution_analytics spec frames the mart as ready_now without flagging the renewal-only constraint

**Spec says.** `Readiness: ready_now`, no caveat about new-business rows
having no rate-change signal.

**Pack B reality.** `rate_monitoring_total_our_share_usd` is renewal-only. Of
the 250 quotes in `ll_quote_setup`, only 182 are renewals (per
`dbt_demo/README.md`); the remaining 68 new-business quotes have no
rate-change row. Joining as INNER drops them; joining as LEFT keeps them with
NULL decomposition columns.

**Resolution in code.** The canonical `mart_pricing_adequacy` (consumed by `view_rate_change_waterfall`)
filters to `new_renewal = 'Renewal'` in the final select — this mart is
deliberately renewal-only. Pricing-adequacy and layer-adequacy marts use LEFT
JOINs to keep new business present (where the adequacy gap is still
computable from sold/tech ratios).

**Planner / spec implication.** Add a "population coverage" diagnostic to the
spec: for each source whose foreign key is sparse against the primary, report
the row-count overlap. When a fact-to-fact join produces <100% coverage, the
spec should flag whether the initiative population is intentionally restricted
or whether the source is sparse.

---

## R-5 — Bridge fan-out flagged but not resolved by the spec

**Spec says.** All four marts mark the `ll_quote_coverage_detail` join as
"⚠ risky" with the note: "Bridge table join may produce multiple rows per
fact row. Apply coverage_id or other filter."

**What's missing.** The spec stops at the warning. It doesn't recommend a
collapse strategy, doesn't say which categorical attribute to keep on
multi-coverage layers, and doesn't define which structural measures are
additive across coverages on the same layer.

**Resolution in code.** Added `int_coverage_layer_rollup` (an ephemeral
intermediate model) which collapses coverage rows to layer grain by:
- picking the *primary* coverage's categorical attributes
  (`is_primary_coverage = true`, falling back to lowest `coverage_id`),
- SUM-ing additive measures (exposure, limit, excess, deductible),
- exposing `coverage_count` and `has_multi_coverage_rollup` so consumers can
  audit affected rows.

The "limit and excess sum across multi-coverage layers" choice is a demo
default. A real-world implementation should validate per-layer limit-stack
semantics with the carrier — for some layer programmes the limit is shared
across coverages (so MAX, not SUM, is correct).

**Planner / spec implication.** When the planner identifies a bridge
fan-out, it should *propose* the rollup model rather than flag it as a
downstream risk. A standard rollup pattern is reusable across many
initiatives sharing the same bridge.

---

## R-6 — renewal_prioritisation spec flagged stale data-quality warnings

**Spec says.** `ll_quote_setup` has 0% test coverage; calls it out as a
production risk.

**Current Pack B reality.** `dbt_demo/models/staging/_stg_models.yml` adds
not_null + unique on `quote_id`, accepted_values on `premium_currency` and
`carrier_branch`, plus dbt_utils sanity checks. Test coverage is now
substantively non-zero.

**Resolution.** No mart change needed; the warning is stale.

**Planner / spec implication.** When specs are persisted as `current.md` they
freeze the data-quality snapshot at write-time. Either (a) regenerate
`current.md` after staging-layer changes, or (b) compute the data-quality
table at spec-render time from the live manifest rather than from a frozen
snapshot.

---

## R-7 — Spec lacks per-mart derived-measure recommendations

Across all four specs, the column tables enumerate raw source columns only —
the specs do not propose derived analytical measures (adequacy gaps, rate on
line, ELR drift, priority scores). These are exactly the headline KPIs that
make each mart consumable.

**Resolution.** All four marts add documented derived columns plus
categorical bands (`adequacy_band`, `priority_band`) suitable for direct BI
consumption.

**Planner / spec implication.** The spec generator stops at column lineage. A
follow-on step — a "headline-measures synthesizer" — could propose derived
measures from the initiative's `archetype` (monitoring vs prioritization vs
analytics_product) and the available primitives. This is the single highest-
leverage spec-quality improvement; without it, every consumer reinvents the
same ratios.

---

## R-8 — Subscription-share columns present but not exploited by Tier 1 specs

`ll_quote_policy_detail` carries `our_share_pct`, `our_share_pct_london`,
`our_share_pct_non_london`, `london_estimated_signed_line`,
`london_order_percentage`. These are all London-Market-distinctive signals.
None of the four Tier 1 specs reference them as analytical measures.

**Resolution.** The pricing_adequacy and layer_rate_adequacy marts surface
these columns through to consumers (so a BI layer can pivot by London vs
non-London position) even though the spec didn't prescribe them.

**Planner / spec implication.** Aligns with R-7 — the spec column-selection
step is too conservative. For specialty London Market workloads, subscription-
share columns should be available in any pricing-adjacent mart by default.

---

## Cross-cutting summary

The four specs are **directionally correct** but have systematic gaps:

| Theme | Specs affected | Severity | Status |
|---|---|---|---|
| Source-list / grain mismatch with Pack B (R-1, R-2) | pricing_adequacy_monitoring, layer_rate_adequacy_monitoring | High — would cause join failures on first run | ✅ Resolved via Pack B extension (layer-grain rate_monitoring seed) |
| Under-selected columns from already-joined sources (R-3, R-7, R-8) | rate_change_attribution_analytics + all four | Medium — marts run, but headline KPIs missing | Mitigated in mart layer; planner improvement still recommended |
| Population-scope ambiguity (R-4) | rate_change_attribution_analytics | Medium — affects mart row count without warning | Mitigated in mart layer (renewal-only filter) |
| Resolved risks left as warnings rather than as remediation patterns (R-5) | All four | Medium — pushes design choices to every implementer | Mitigated via int_coverage_layer_rollup |
| Stale data-quality snapshots (R-6) | renewal_prioritisation | Low | No action needed |

**Strongest recommendation for the planner:** add a post-spec
"materialise it" pass that (a) verifies join keys against actual asset grain,
(b) proposes derived measures for the initiative archetype, and (c)
reconciles the `current.md` data-quality table with the live test manifest
each time it's regenerated.
