# Question-Driven Data Products — Demo Runbook

A walkthrough of six business questions from the specialty insurance domain,
each answered by a dedicated data product running live in Snowflake. Each
question maps to one query; each query reads from a single dbt view that has
a documented business contract.

---

## The workflow this demo proves

> **Business question → catalogue lookup → governed mart → query that returns the answer.**

Most data warehouse demos show schemas and lineage diagrams — *infrastructure
as the deliverable*. This one inverts the framing: we lead with the question
an underwriter or pricing lead would actually ask, and trace it through to a
queryable, tested, and governed answer in Snowflake.

Every initiative carries:

- a **single sharp business question** in the asker's own voice
- a defined **asker** (the role consuming the answer)
- a defined **cadence** (when the question gets asked)
- a **canonical mart** that holds the data needed to answer it
- a **named view** materialised in Snowflake that projects exactly the
  columns the question requires.

In production, the same pattern would compose against the real Liberty
warehouse — the underlying data shape (pricing decomposition, rate-change
attribution, coverage exposure) is what's already produced by the upstream
rating engine and PAS systems.

---

## Architecture in one paragraph

Six source seeds (mocking real warehouse assets at the same grain) feed six
staging views, one ephemeral coverage rollup, two canonical mart tables
(`mart_pricing_adequacy` and `mart_renewal_decision_support`), and six
question-specific views over those marts. 80 data tests verify primary-key
uniqueness, accepted-value contracts, bound checks on derived percentages,
and referential integrity from layer to quote. The decomposition of rate
change into named drivers reconciles to the headline figure by construction
— the same invariant a real rating engine would produce.

---

## Snowflake setup (run once per worksheet)

```sql
use database BASE_PLAIN_SNOWFLAKE;
use schema   DS_DEV_CHRIS_MART_PACK_B;
use warehouse ML_WAREHOUSE;
```

After this, every query below works with unqualified view names.

---

## The six business questions

| # | Question | Asker | Cadence | View |
|---|---|---|---|---|
| 1 | Of my open renewal queue, which renewals warrant active negotiation versus a light-touch pass? | Underwriter | Weekly | `view_renewal_priority_queue` |
| 2 | Where in my portfolio is sold premium drifting below technical price, and by how much? | Pricing lead | Weekly | `view_portfolio_drift_heatmap` |
| 3 | How much of this renewal's rate change came from deliberate pricing action versus structural changes in exposure, limit, breadth, or claims inflation? | Pricing actuary | Per renewal | `view_rate_change_waterfall` |
| 4 | Which layers in my specialty excess book are written below technical adequacy, and is the gap widening on renewal? | Pricing analyst | Weekly | `view_layer_attachment_ladder` |
| 5 | How does our written rate compare to technical price across segments, brokers, and underwriters? | Pricing lead | Monthly | `view_segment_benchmark` |
| 6 | For this renewal, what is our pricing position versus technical, and how has the risk shape moved year-on-year? | Underwriter | Per quote | `view_underwriting_risk_context` |

Reading the table: each row is one *data product* — one mart view, with one
defined business owner, one defined cadence, and one defined question. If
two rows had the same question, one would be redundant. If a row's question
were vague, the asker wouldn't know what they were getting.

---

# Query 1 — Renewal Priority Queue

## Business question

**Of my open renewal queue, which renewals warrant active negotiation versus a light-touch pass?**

*Asker: Underwriter · Cadence: Weekly · View: `view_renewal_priority_queue`*

## Business context

Underwriter capacity is the binding constraint on the renewal book. A
specialty insurance team can have hundreds of renewals open at any time;
applying equal effort to a clean, well-priced renewal as to a deteriorating
one destroys throughput and increases the chance of mispriced retention.
This view ranks open renewals so the underwriter can concentrate their time
on the ones where active negotiation will move the needle.

The ranking blends five independent signals:
- how far below the technical benchmark the price has fallen
- the modelled expected loss ratio (proxy for profitability)
- the year-on-year rate trajectory net of inflation
- the layer's premium scale (bigger layers warrant more attention)
- time pressure (days until renewal inception)

## Query

```sql
select
    policyholder_name,
    section,
    days_to_renewal_inception,
    sold_gnwp,
    adequacy_gap_modtech_pct,
    net_rarc,
    priority_score,
    priority_band
from view_renewal_priority_queue
where priority_band = 'high'
order by priority_score desc
limit 10;
```

## How to read a row

Each row is one renewing layer. Reading left to right tells you a complete
operational story:

- **policyholder_name** — who the insured is
- **section** — which class of business
- **days_to_renewal_inception** — how long until this renewal incepts
  (negative = already incepted)
- **sold_gnwp** — Liberty's premium (USD, net of broker commission)
- **adequacy_gap_modtech_pct** — how far the sold price is below the
  underwriter's own modified-technical benchmark; *negative is concerning*
- **net_rarc** — year-on-year rate movement *after stripping out claims
  inflation and breadth-of-cover changes*; *negative means real rate
  softening*
- **priority_score** — composite 0–100 ranking
- **priority_band** — high / medium / low triage band

A high-priority renewal typically combines: a meaningful adequacy gap, a
softening rate trajectory, and a non-trivial premium amount.

## What to say live

> "Underwriter walks in Monday morning. This is their queue — top ten high-priority
> renewals, ranked by a composite score that blends pricing adequacy, expected
> loss ratio, rate trajectory, premium scale, and time pressure. Each row tells
> them not just *which* renewal to focus on, but *why* it's high. The score is
> tunable; the weights are documented."

---

# Query 2 — Portfolio Drift Heatmap

## Business question

**Where in my portfolio is sold premium drifting below technical price, and by how much?**

*Asker: Pricing lead · Cadence: Weekly · View: `view_portfolio_drift_heatmap`*

## Business context

Pricing discipline erodes quietly. Individual underwriters may each make
defensible concessions on individual layers, but the aggregate of those
concessions can be a structurally underpriced book. By the time the loss
ratio reflects the drift, it is too late to remediate the cohort.

This view surfaces drift in real time, segmented by section and underwriter
so the pricing lead can see *where* the discipline is slipping — not just
that drift is happening at the portfolio level. The categorical
`adequacy_band` column buckets every layer into a colour code suitable for
a heatmap-style dashboard.

## Query

```sql
select
    section,
    underwriter,
    adequacy_band,
    count(*)                                    as layer_count,
    round(sum(sold_gnwp), 0)                    as total_sold_gnwp_usd,
    round(avg(adequacy_gap_modtech_pct), 4)     as avg_adequacy_gap
from view_portfolio_drift_heatmap
group by 1, 2, 3
order by 1, 2, 3;
```

## How to read a row

Each row is a (section, underwriter, adequacy band) cell:

- **section** — class of business
- **underwriter** — the lead underwriter assigned
- **adequacy_band** — categorical bucket of the adequacy gap
  - `below_modtech_severe`: written more than 10% below modified-technical
  - `below_modtech_mild`: between −10% and −2.5%
  - `on_modtech`: within ±2.5%
  - `above_modtech`: above +2.5%
- **layer_count** — how many layers fall in this cell
- **total_sold_gnwp_usd** — total premium in this cell
- **avg_adequacy_gap** — average gap percentage

Cells in `below_modtech_severe` with high layer_count and high
total_sold_gnwp_usd are where the most premium is being written furthest
below adequate price. Those are the cells the pricing lead should
intervene on first.

## What to say live

> "Pricing lead asks: where is my book drifting? Two-dimensional pivot — sections
> down the side, adequacy bands across the top, layer count and premium dollars
> per cell. The `below_modtech_severe` rows are where to act. This isn't a
> point-in-time number; refreshed weekly, it's a trend you can watch erode or
> recover."

---

# Query 3 — Rate-Change Waterfall

## Business question

**How much of this renewal's rate change came from deliberate pricing action versus structural changes in exposure, limit, breadth, or claims inflation?**

*Asker: Pricing actuary · Cadence: Per renewal · View: `view_rate_change_waterfall`*

## Business context

Headline rate change is a poor signal of pricing health on its own. A book
can show +5% rate while quietly losing pricing discipline, because most of
that 5% came from claims inflation absorption and the underlying layer is
actually softening in real terms. Equally, a −2% headline can disguise
genuine rate gains that were eaten by exposure growth.

This view decomposes every renewal's headline rate change into the named
drivers the rating engine produces at quote time — claims inflation,
breadth-of-cover change, exposure change, limit/excess change, policy term
change, and a catch-all "other". By construction the named drivers
reconcile to the headline figure; an audit column on every row makes this
verifiable.

The decomposition is *not computed in the warehouse modeller* — it comes
from the upstream rating engine. The warehouse preserves the source-truth
decomposition; the data product surfaces it in a queryable form.

## Query

```sql
select
    policyholder_name,
    section,

    -- Headline movement
    round(gross_rarc, 4)                       as headline_rate_change,

    -- The two drivers the actuary strips out to get the "real" rate change
    round(claims_inflation, 4)                 as claims_inflation,
    round(breadth_of_cover_change, 4)          as breadth_of_cover,

    -- Net of the strip-outs — the real-terms rate movement
    round(net_rarc, 4)                         as real_rate_change_net,

    -- The remaining structural drivers
    round(gross_exposure_change, 4)            as exposure_change,
    round(gross_limits_and_excess_change, 4)   as limit_change,
    round(policy_term_change, 4)               as term_change,
    round(other_changes, 4)                    as other_unclassified,

    -- Audit: should be ~0 if the decomposition reconciles
    round(gross_rarc_residual_check, 4)        as decomposition_audit
from view_rate_change_waterfall
order by abs(gross_rarc) desc
limit 10;
```

## How to read a row

A row is one renewing layer. Reading top to bottom:

- **headline_rate_change** — the big number; what management sees
- **claims_inflation** — losses are getting more expensive year-on-year;
  this much of the price increase is just absorbing inflation
- **breadth_of_cover** — wider/narrower cover at renewal contributing to
  the price change
- **real_rate_change_net** — the actuarial signal: the price movement
  *after* inflation and breadth are removed. This is the column an actuary
  would lead with.
- **exposure_change**, **limit_change**, **term_change**, **other_unclassified**
  — the structural drivers that, with inflation and breadth, sum to the
  headline figure
- **decomposition_audit** — the arithmetic check; should be a tiny number
  (within ±0.015 in this dataset). Persistent non-zero values would flag a
  source-data integrity issue.

## What to say live

> "Most pricing dashboards stop at the headline rate change. This view shows
> what's *inside* the headline — every renewal's rate movement broken into the
> seven named drivers. The audit column on the right shows the decomposition
> reconciles to within a couple of basis points. That's not dbt magic — it's
> the rating engine's own decomposition, surfaced cleanly. The actuarial
> question 'are we keeping pace with inflation' is now a queryable column."

---

# Query 4 — Layer Attachment Ladder

## Business question

**Which layers in my specialty excess book are written below technical adequacy, and is the gap widening on renewal?**

*Asker: Pricing analyst · Cadence: Weekly · View: `view_layer_attachment_ladder`*

## Business context

In specialty excess insurance, a single account can carry multiple layers
stacked on top of one another (primary, first-excess, second-excess). The
economics differ materially by layer — a layer attaching at $50M with a
$50M limit prices very differently from a primary $0–$50M layer on the
same account. Account-level rate averages can mask serious problems: the
primary may be priced well while the excess layer is structurally
underpriced, and the average looks fine.

This view forces every layer to stand on its own and surfaces the
specialty-excess benchmark — Rate-on-Line (premium per unit of layer
limit). Sorted by widest adequacy gap, it produces an actionable list of
individual layers where pricing discipline has slipped.

## Query

```sql
select
    section,
    coverage,
    round(rate_on_line, 4)                     as actual_rol,
    round(technical_rate_on_line, 4)           as technical_rol,
    round(adequacy_gap_modtech_pct, 4)         as gap_pct,
    round(year_on_year_premium_change_pct, 4)  as yoy_premium_change,
    adequacy_band
from view_layer_attachment_ladder
where adequacy_band in ('below_modtech_severe', 'below_modtech_mild')
  and new_renewal = 'Renewal'
order by adequacy_gap_modtech_pct asc
limit 20;
```

## How to read a row

Each row is one renewing layer that has been written below modified-technical:

- **section** / **coverage** — what class of business and specific cover
- **actual_rol** — *Rate-on-Line*: sold premium ÷ layer limit. The
  specialty-excess pricing benchmark. Lower means cheaper.
- **technical_rol** — the same calculation using the actuarial technical
  premium. The model's view of what the rate-on-line should be.
- **gap_pct** — how far the sold price falls below the modified-technical
  benchmark; negative means concession
- **yoy_premium_change** — year-on-year change in this layer's premium
  amount; *negative means the price actually fell at renewal*
- **adequacy_band** — concentration into a categorical bucket

The damning combination is **`gap_pct < 0`** AND **`yoy_premium_change < 0`**:
last year's price went down, AND the technical model says the price
should have held. That's structural softening on an individual layer.

## What to say live

> "In specialty excess the layer is the unit of analysis, not the account.
> Twenty rows. Each row is a layer where the price moved *down* year-on-year
> AND the model says it should have held. This is what quietly erodes the loss
> ratio. The Rate-on-Line columns put each layer on the same scale regardless
> of size — comparable across a primary and a high-excess layer."

---

# Query 5 — Segment Benchmark

## Business question

**How does our written rate compare to technical price across segments, brokers, and underwriters?**

*Asker: Pricing lead · Cadence: Monthly · View: `view_segment_benchmark`*

## Business context

Aggregate adequacy signals point you to *what* is happening; segment-level
benchmarks tell you *who* is consistently driving it. Brokers vary in the
quality of business they place; underwriters vary in their pricing
discipline; sections vary in how soft the local market is. By aggregating
the layer-level adequacy signal up to (section, broker) cells, this view
produces an accountability surface.

A specific use case: identify brokers who consistently place business at
material discounts to modtech across a meaningful number of layers. That
informs broker-relationship conversations and channel-mix decisions.

## Query

```sql
select
    section,
    broker_primary,
    count(*)                                    as layer_count,
    round(sum(tech_gnwp), 0)                    as total_tech_premium_usd,
    round(sum(sold_gnwp), 0)                    as total_sold_premium_usd,
    round(avg(adequacy_gap_modtech_pct), 4)     as avg_gap_modtech,
    round(avg(adequacy_gap_tech_pct), 4)        as avg_gap_tech
from view_segment_benchmark
group by 1, 2
having count(*) >= 3
order by avg_gap_modtech asc
limit 25;
```

## How to read a row

Each row is one (section × broker) cell with at least 3 layers placed:

- **section** / **broker_primary** — the segment cut
- **layer_count** — number of layers in the cell
- **total_tech_premium_usd** — the actuarial benchmark price summed across the cell
- **total_sold_premium_usd** — what was actually written summed across the cell
- **avg_gap_modtech** — average gap to the modified-technical benchmark
  (the negotiation give-up signal)
- **avg_gap_tech** — average gap to the actuarial floor (the "is the price
  technically inadequate" signal)

The cells at the top of the list (sorted by `avg_gap_modtech` ascending)
are the broker-section combinations where Liberty is most consistently
writing below modtech. The `having count(*) >= 3` filter removes single-
layer noise.

## What to say live

> "Drift is a portfolio question; *who* is driving the drift is a relationship
> question. This view aggregates the adequacy signal up to broker-section cells.
> Brokers consistently bringing material discounts across multiple layers
> become visible — that's a relationship conversation, not a model fix."

---

# Query 6 — Underwriting Risk Context

## Business question

**For this renewal, what is our pricing position versus technical, and how has the risk shape moved year-on-year?**

*Asker: Underwriter · Cadence: Per quote · View: `view_underwriting_risk_context`*

## Business context

Where the priority queue (Query 1) tells the underwriter *which* renewals to
focus on, the risk-context view answers *what they need to know* before
binding a specific one. It is the "decision card" that pulls every relevant
data point onto a single screen — pricing position, layer structure, prior-
year context, forward-looking signals — so the underwriter can decide
without juggling four tabs.

Single-row consumption (one quote at a time), unlike the queue or heatmap.

## Query

```sql
select *
from view_underwriting_risk_context
where quote_id = '<paste a quote_id here>';
```

To find a specific high-priority quote_id, run Query 1 first and pick a row.

## How to read a row

The view contains the full layer context for one renewing quote:

- **Header**: policyholder, underwriter, broker, branch, currency
- **Coverage shape**: section, coverage, exposure type, limit type,
  deductible type, claims trigger, jurisdiction
- **Layer structure**: exposure, coverage limit, excess, deductible
- **Pricing position**: tech / modtech / sold premium decomposition,
  expected loss cost, ELR ratios, sold-to-modtech ratio
- **Layer benchmark**: rate-on-line, adequacy gap to modtech
- **Renewal context**: this renewal's incept/expire dates, the expiring
  policy's incept/expire dates, expiring premium, year-on-year change
- **Forward-looking signals**: gross rate change, net rate change, claims
  inflation assumption

It's deliberately a wide row, not a summary. The underwriter is not
filtering or aggregating — they're reading one decision card.

## What to say live

> "Once an underwriter has been routed to a specific renewal by the priority
> queue, this is what they see. Every signal that bears on the bind decision —
> pricing position, layer structure, renewal context, forward-looking rate —
> on a single screen. One row, one decision. No tab-juggling."

## Out of scope today

The original framing of this question also asked about the insured's
*claims history*. That branch was moved out of scope because the current
warehouse cut has no claims data. When a claims-experience source is added
to the warehouse, the view extends to include prior-year incurred and paid
amounts, and the question broadens to its full original form.

---

## The closing slide

If a single statement summarises the demo, this is it:

> *"Six business questions. Six data products. One audit trail from question
> to answer. Each query is a one-screen deliverable that an underwriter,
> pricing lead, or actuary asked for in their own voice — not a generic
> dashboard. The lineage from question → mart → view → answer is queryable
> and tested. That is what 'governed data product' looks like in practice."*

---

## Appendix — Underlying architecture

For audiences who want to see one level deeper:

- **6 source seeds** (`raw_pack_b` schema) — quote header, layer-grain pricing
  (Liberty share + full share), coverage-grain attributes, layer-grain rate
  monitoring, quote-grain rate monitoring (premium-weighted aggregation of
  the layer-grain seed by construction)
- **6 staging views + 1 ephemeral coverage rollup** (`stg_pack_b` schema)
- **2 canonical mart tables** — `mart_pricing_adequacy` (carries every column
  the four pricing-cluster questions need) and `mart_renewal_decision_support`
  (carries every column the two renewal-cluster questions need)
- **6 question-specific views** (`mart_pack_b` schema) — one per business
  question, each projecting only the columns required to answer it, with
  the question, asker, and cadence documented in the view header
- **80 data tests** — primary-key uniqueness, accepted-value contracts on
  categorical bands, bound checks on derived percentages, referential
  integrity from layer to quote, audit on the rate-change decomposition

A `mart_plan.yaml` file in the repo's `ontology/` directory is the audit
trail mapping each business question to its canonical mart and its
fulfilment view's SQL file.
