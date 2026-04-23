# Commercial specialty P&C insurance — synthesised domain taxonomy

**Nature of this document:** This is a **synthesised reference**, not a reproduction
of any single authoritative source. It was assembled from publicly-documented
taxonomies used across the commercial specialty P&C industry — primarily Lloyd's
market publications, ACORD capability framing, McKinsey / BCG insurance practice
writing, and IFoA / CAS actuarial practice definitions — to provide defensible
grounding for the domain taxonomy research script. Each domain below cites the
public sources that inform its scope.

**When a proper internal reference is available** (Lloyd's MDC data model, an
LSM capability map, an enterprise data catalogue), add it as a separate file in
this directory and rerun the research. Comparing the two briefs (this synthesis
vs an authoritative reference) is itself a useful diagnostic.

**Scope:** Commercial specialty P&C insurance and reinsurance, carrier / MGA /
Lloyd's syndicate perspective. Adjust if your book differs materially.

---

## Value chain (top-level domains)

Numbered in rough value-chain order (new business → in-force → loss → financial
close). Cross-cutting functions (portfolio monitoring, compliance) are listed
last; they sit alongside the value chain rather than within it.

1. **Distribution** — acquisition channels and the coverage structures they create
2. **Underwriting** — risk selection, terms, and bind
3. **Pricing** — technical rate-making and rate-change analytics
4. **Exposure management** — aggregation, concentration, catastrophe
5. **Operations** — in-force policy servicing
6. **Claims** — loss notification, investigation, payment
7. **Reserving** — IBNR and case-reserve estimation
8. **Reinsurance** — inward assumed, outward ceded, retrocession, recoveries
9. **Finance & profitability** — earned premium, combined ratio, technical result
10. **Portfolio monitoring** *(cross-cutting)* — KPIs, drift, concentration
11. **Compliance & regulatory** *(cross-cutting)* — sanctions, conduct, reporting

---

## Domain definitions

### 1. Distribution

**Scope.** The channel(s) through which risk reaches the carrier, and the
contractual structures governing those channels. Includes direct broker
placements, coverholder / MGA / binding-authority arrangements, delegated
authority, facility placements, and line-slips.

**Belongs here:** broker submissions, broker commission, coverholder references,
binding authority agreements, master UMR / line-slip / binder identifiers,
delegated authority reporting (bordereaux), MGA performance.

**Does NOT belong here:** the underwriting decision itself (see Underwriting),
the technical rate applied (see Pricing), or the resulting portfolio mix (see
Portfolio monitoring). Distribution is about *how* risk arrived, not *whether
we accepted it* or *at what price*.

**Adjacent-domain boundary:** A broker commission column belongs here.
A `brokerage_pct` on a priced quote *also* belongs here because it's part of
how the distribution economics work. A `premium` column does not — even on
a bordereau — because premium is a pricing/financial measure.

**Sources:** Lloyd's Coverholder Reporting Standards; Lloyd's Delegated
Authority framework; ACORD Insurance Capability Model — "Distribution
Management" domain.

### 2. Underwriting

**Scope.** Risk selection, evaluation, and the act of binding. The decision
process that determines whether the carrier accepts a risk and on what terms.

**Belongs here:** quote lifecycle (submission → quote → bound / declined),
underwriter identity and workflow, risk appetite assessment, underwriting
guidelines, policyholder identity and risk characteristics, coverage terms
negotiated, line-size and share written, bind date.

**Does NOT belong here:** the technical price itself (see Pricing), the
commissions paid to distributors (see Distribution), or what happens to the
risk after bind (see Operations / Claims / Portfolio monitoring).

**Adjacent-domain boundary:** A `quote_id` is underwriting. A `premium` on the
quote is pricing (because it's derived from a rating model). A `broker_primary`
on the quote is distribution. An asset frequently matches multiple domains
because it represents the *quote* — this is expected and not a taxonomy bug.

**Sources:** McKinsey, "How data and analytics are redefining excellence in
P&C underwriting" (2021); Lloyd's Underwriting Principles; IFoA practice
area — Underwriting.

### 3. Pricing

**Scope.** Technical rate-making, rate monitoring, and rate benchmarking.
Everything that answers "what price should this risk command" and "how has
that price moved over time."

**Belongs here:** technical premium, modified technical premium, sold premium,
rate change, ELR / expected loss ratio, rate adequacy measures, benchmarking
against peers or portfolio, loadings and discounts, commission as a rate
input, RARC / risk-adjusted rate change.

**Does NOT belong here:** the financial *accounting* view of premium (GWP
vs NWP, earned vs written, accounting deferrals) — that's Finance. The act
of deciding whether to bind at a given price — that's Underwriting.

**Adjacent-domain boundary:** `premium` on a quote table is pricing.
`gwp_to_nwp_mapping` is finance. `premium_earned` is finance. `rate_change`
is pricing. `technical_loss_ratio` is pricing when used for adequacy
monitoring, but profitability when used for result attribution.

**Sources:** IFoA and CAS pricing practice areas; hyperexponential public
writing on pricing adequacy; Lloyd's Performance Management Framework
guidance on rate monitoring.

### 4. Exposure management

**Scope.** The aggregation of risks across the portfolio to measure
concentration, catastrophe potential, and regulatory capital implications.
Answers "if event X happens, how much is at stake?"

**Belongs here:** probable maximum loss (PML), zonal aggregates, catastrophe
model outputs (AIR, RMS), Solvency II SCR-related exposure views, line-of-
business concentration, geographic concentration, single-risk limits.

**Does NOT belong here:** the actual loss when an event occurs (that's
Claims). Individual-risk underwriting appetite (that's Underwriting).

**Adjacent-domain boundary:** A `pml_25yr_return_period` value is exposure
management. A `total_incurred` column on a loss table is claims. A
`portfolio_pml_mean` value on a rolling-up monitoring dashboard is portfolio
monitoring *informed by* exposure management — classify as the latter if
it's about drift over time.

**Sources:** Lloyd's Realistic Disaster Scenarios (RDS) framework; Solvency
II SCR reporting under EIOPA guidance; Swiss Re sigma publications on cat
modelling.

### 5. Operations

**Scope.** In-force policy servicing — everything that happens to a policy
between bind and either expiry or claim. Endorsements, mid-term adjustments
(MTAs), cancellations, renewals (the servicing side of renewal).

**Belongs here:** endorsement records, MTA transactions, cancellation
reasons and refunds, policy status transitions, renewal processing
(distinct from renewal *decisions* which may be underwriting).

**Does NOT belong here:** quote-to-bind (that's Underwriting). Premium
accounting adjustments from endorsements (that's Finance). Claims events
on the policy (that's Claims).

**Adjacent-domain boundary:** The decision to renew at revised terms may
be underwriting; the *processing* of the renewal (systems, dates, status)
is operations. A data warehouse often doesn't distinguish — flag ambiguity
explicitly in the brief.

**Sources:** ACORD Policy Lifecycle Model; industry standard BPMN process
definitions for P&C policy administration.

### 6. Claims

**Scope.** Loss notification, investigation, assessment, reserving at case
level, settlement, recovery. The entire lifecycle of a claim from FNOL
(first notification of loss) through final closure.

**Belongs here:** claim identifiers, FNOL date, claim status, incurred
amounts (case reserves + paid), indemnity vs expense split, subrogation
recoveries (net of reinsurance which is separate), fraud indicators,
adjuster assignments, claim age and duration.

**Does NOT belong here:** IBNR or actuarial bulk reserves (see Reserving).
Reinsurance recoveries on claims (see Reinsurance). Catastrophe events at
the portfolio level (see Exposure management).

**Adjacent-domain boundary:** `case_reserve` is claims. `ibnr` is reserving.
`incurred_but_not_reported_expense` is reserving. `paid_indemnity` is claims.
A column named `ultimate_loss` is typically reserving (actuarial ultimate),
not claims.

**Sources:** ACORD Claims Capability Model; IFoA / CAS claims management
practice area; McKinsey claims automation writing.

### 7. Reserving

**Scope.** Actuarial estimation of ultimate losses not yet fully reported
or paid. IBNR (incurred but not reported), case reserve adequacy, loss
development patterns.

**Belongs here:** IBNR amounts, ultimate loss estimates (actuarial),
loss development factors (LDFs), Bornhuetter-Ferguson estimates, Chain
Ladder outputs, ultimate loss ratio, reserve adequacy metrics, held
reserves vs indicated reserves.

**Does NOT belong here:** individual case reserves (that's claims — they
are set by adjusters, not actuaries). Paid losses (claims). The capital
implications of reserves (finance / solvency).

**Adjacent-domain boundary:** A claims table's `case_reserve` is claims.
An actuarial triangle's `bf_ultimate` is reserving. A financial statement's
`gross_reserves_held` is finance, populated from reserving inputs.

**Sources:** CAS Statement of Principles Regarding Property and Casualty
Loss and Loss Adjustment Expense Reserves; IFoA practice area — Reserving;
Swiss Re sigma on loss reserving.

### 8. Reinsurance

**Scope.** Both inward (assumed) and outward (ceded) reinsurance, including
retrocession and recoveries. Treaties, facultative placements, event
allocations.

**Belongs here:** treaty identifiers, facultative placements, reinsurance
premium ceded or assumed, reinsurance recoveries on claims, retro cessions,
treaty terms (layer, share, reinstatement), reinsurance commission.

**Does NOT belong here:** gross premium on the underlying business (that's
pricing / finance depending on view). The underlying claim itself (claims).

**Adjacent-domain boundary:** A reinsurance recovery is a reinsurance
column on a claims transaction. Net-of-reinsurance premium on a portfolio
dashboard is finance, informed by reinsurance. Classify based on primary
intent: if the column exists to track the treaty economics, reinsurance;
if it exists to reconcile financial reporting, finance.

**Sources:** Lloyd's Reinsurance Trust Fund framework; ACORD Reinsurance
Capability Model; Swiss Re publications on proportional and non-proportional
reinsurance structures.

### 9. Finance & profitability

**Scope.** Accounting and financial reporting views of the business.
Premium earning patterns, combined ratio, technical result, expenses,
profitability attribution.

**Belongs here:** GWP, NWP, earned premium, unearned premium reserves
(UPR), deferred acquisition costs (DAC), combined ratio, expense ratio,
loss ratio (when used for result reporting rather than rate adequacy),
technical result, operating result, P&L attribution, target vs plan,
sold-to-plan, modified technical premium *when used for profitability
decomposition*.

**Does NOT belong here:** Rate adequacy monitoring (pricing). Portfolio
concentration drift (portfolio monitoring). Claim-level financials
(claims / reserving).

**Adjacent-domain boundary:** `gnwp_to_nwp` is finance. `tech_gnwp` in
a rate-adequacy context is pricing; in a P&L attribution context, finance.
This overlap is genuine — expect assets to double-match and use
hit-count + name-level signal to disambiguate.

**Sources:** IFRS 17 / Solvency II financial reporting guidance; Lloyd's
Statement of Actuarial Opinion (SAO) requirements; EIOPA reporting
templates.

### 10. Portfolio monitoring

**Scope.** Cross-cutting oversight of portfolio health over time.
Distinct from its components: this is the *monitoring* view, not the
underlying underwriting, pricing, claims, or finance activities.

**Belongs here:** KPI dashboards, concentration drift, rate-change
monitoring at portfolio level, renewal-retention trending, exposure
drift, loss ratio trending, benchmark comparisons over time.

**Does NOT belong here:** point-in-time pricing (that's Pricing).
Underwriting decisions on specific risks (Underwriting). Claims
development at individual claim level (Claims / Reserving).

**Adjacent-domain boundary:** A `rate_monitoring` table is portfolio
monitoring. A `hx_do_layer_pricing` table with a `rate` column is
pricing. The distinction: monitoring rolls up and tracks drift;
pricing computes for individual risks.

**Sources:** Lloyd's Performance Management Framework; McKinsey
"real-time portfolio monitoring" writing (already cited in
`initiative_research.yaml`).

### 11. Compliance & regulatory

**Scope.** Sanctions screening, conduct-risk monitoring, regulatory
reporting (Solvency II, IFRS 17, individual regulator templates), data-
protection obligations.

**Belongs here:** sanctions flags and screening results, conduct-risk
indicators, regulatory return preparation (QRTs, SFCR inputs),
Solvency II pillar-3 outputs, GDPR / data classification tags.

**Does NOT belong here:** anything that's merely *reported to* a
regulator but exists primarily for another purpose (an ordinary loss
ratio reported in an SFCR is finance; the SFCR template preparation
artefact itself is compliance).

**Adjacent-domain boundary:** A `sanctions_check_result` column is
compliance. A `solvency_ii_scr_contribution` column is compliance when
it's the output of a capital calculation; finance when it's an input
to financial reporting.

**Sources:** EIOPA Solvency II guidance; FCA / PRA conduct frameworks
for UK-authorised insurers; Lloyd's Minimum Standards documentation.

---

## Known overlaps / edge cases

- **Underwriting vs Pricing.** Quotes carry both underwriting identity
  (underwriter, quote id, bind decision) and pricing artefacts (premium,
  rate, commission). Most quote-style assets legitimately double-match.
  The primary-domain coloring should reflect whichever function the
  asset primarily supports in practice — the tiebreaker should fall out
  of total keyword hits.

- **Pricing vs Finance & profitability.** Technical premium and
  modified technical premium live at the boundary. Rule of thumb: if
  the column is used for *rate monitoring* (comparing technical price
  to sold price to plan), it's pricing; if it's used for *P&L
  attribution* (sold-to-plan, target-to-plan), it's finance.

- **Claims vs Reserving.** Case reserves are claims (set by adjusters);
  IBNR and ultimate losses are reserving (estimated by actuaries).

- **Portfolio monitoring vs everything.** Portfolio monitoring overlaps
  every value-chain domain because it's a view *over* them. Assets
  named `*_monitoring`, `*_dashboard`, or `*_drift` should primary-
  match this domain; the underlying drivers should primary-match
  their value-chain domain.

- **Reinsurance vs Finance.** Net-of-reinsurance columns on financial
  reporting tables are finance. Treaty-economics tables are reinsurance.

- **Distribution vs Underwriting.** Broker and coverholder identity is
  distribution. Terms negotiated with the broker (line size, deductible)
  is underwriting.

---

## Sources

- Lloyd's Market Publications (public-facing): Coverholder Reporting
  Standards, Delegated Authority framework, Performance Management
  Framework, Realistic Disaster Scenarios, Minimum Standards.
- ACORD Insurance Capability Model (publicly documented framework).
- CAS / IFoA practice area definitions (actuarial professional bodies).
- EIOPA Solvency II guidance and reporting templates.
- Swiss Re sigma publications.
- McKinsey "How data and analytics are redefining excellence in P&C
  underwriting" (2021) — already cited in `ontology/initiative_research.yaml`.
- BCG "Multi-agent AI systems in underwriting" (2025) — already cited.
- hyperexponential insurance-AI writing (2025) — already cited.

Specific URLs and document references should be verified by the curator
before taking any taxonomy proposal to production use. This file is
grounding for an LLM research prompt, not a substitute for due diligence.
