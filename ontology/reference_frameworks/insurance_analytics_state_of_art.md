# Insurance analytics — state-of-art capability framework

**Nature of this document.** This is a **scoping anchor** for the initiative
research script, not a comprehensive catalogue. It defines what counts as a
load-bearing initiative for this project, lists the capability areas the
research should cover, and provides a citation pool. When
`--web-research` is enabled on `scripts/research_initiatives.py`, the model
may add sources beyond those listed here; those should be merged back into
the bibliography below on accepted briefs.

**Scope.** Commercial specialty P&C insurance and reinsurance (carrier / MGA
/ Lloyd's syndicate perspective). Capability areas are aligned with the
`category` field in `ontology/initiative_research.yaml`.

**When this file is stale.** If the `sources` list in
`ontology/initiative_research.yaml` drifts beyond what's bundled here, or if
a new capability area emerges (e.g. embedded insurance, cyber-specific
analytics), rerun research with `--web-research` and merge. Re-curate this
file once a year at minimum.

---

## What counts as an "initiative"

An initiative is a **named analytical or operational capability that a
business owner would fund and deploy**. Criteria:

1. **Load-bearing business outcome.** Answers a question that has a
   decision-maker and a decision cadence — not an analytical curiosity.
2. **Bounded scope.** Has a defined output (dashboard, scoring service, AI
   agent, decision support surface) — not "improve underwriting broadly".
3. **Traceable to primitives.** Uses one or more capability primitives (see
   `ontology/primitives.yaml`). If no primitive enables it, it's either a
   new primitive or a gap.
4. **Citable.** Grounded in published market research, carrier case studies,
   or academic actuarial practice. Aspirational initiatives are allowed but
   must cite an external source AND a reason the current warehouse blocks
   them.

## Editorial bar

- Favour depth over breadth. Ten well-grounded initiatives beats fifty
  loosely-named ones.
- Prefer initiatives with clear output type (`monitoring_dashboard`,
  `decision_support`, `ai_agent`, `analytics_product`, `automation`,
  `prioritization`) over vague labels.
- Aspirational initiatives are explicitly welcomed when they carry
  `blocker_class` (data_source_missing / schema_group_missing /
  tool_missing / governance_missing) and `expected_signal` fields.
- Avoid platform initiatives ("build a data lake") — those are enablers,
  not analytical capabilities.

---

## Capability areas

Ordered by value-chain position. Each area lists canonical state-of-art
examples that the research should consider — *not as a menu to pick from,
but as calibration for what "state of art" means in that area*.

### 1. Distribution

**Scope.** Channel economics, broker performance, coverholder/MGA
oversight, bordereau monitoring.

**State-of-art examples.** Broker attribution scoring (which brokers
produce profitable business), coverholder bordereau anomaly detection,
commission-adequacy monitoring, delegated-authority portfolio drift
alerts.

**Sources.** Lloyd's Coverholder Reporting Standards; McKinsey P&C
underwriting 2021 (distribution analytics section); hyperexponential 2025
on distribution automation.

### 2. Underwriting

**Scope.** Risk selection, decision support at point of bind, submission
triage, renewal prioritisation, risk appetite monitoring.

**State-of-art examples.** AI-assisted underwriting workbench with
real-time context at point of quote, automated submission triage from
unstructured broker emails, underwriter copilot for known-account
lookup and prior-year comparison, renewal portfolio prioritisation
scoring (which renewals deserve deep review vs light-touch),
concentration/appetite monitoring by geography / industry / peril.

**Sources.** McKinsey P&C underwriting 2021 (quote-to-bind efficiency);
BCG agentic insurance 2025 (multi-agent underwriting systems); LMA AI
Survey 2025 (carrier adoption rates); hyperexponential 2025 (workbench
architectures); Hiscox case study (quote turnaround 3 days → 3 minutes).

### 3. Pricing

**Scope.** Technical rate-making, rate-change analytics, rate adequacy
monitoring, rate-change attribution, layer pricing.

**State-of-art examples.** Real-time rate adequacy monitoring at
microsegment level, rate-change decomposition (exposure change / limit
change / commission change / other), technical-price vs sold-price
benchmarking against the market, layer-level rate monitoring for
specialty excess business, portfolio rate roll-up monitoring.

**Sources.** McKinsey P&C underwriting 2021 (real-time microsegment
monitoring); hyperexponential 2025 (pricing platforms); actuarial
literature on rate-change attribution (CAS / IFoA practice notes).

### 4. Claims

**Scope.** Claims experience analytics, loss development tracking,
reserving support, claims leakage detection.

**State-of-art examples.** Ultimate loss ratio monitoring with credibility
blending, loss-development triangles with ML-assisted tail selection,
claims experience analytics by microsegment, reserve adequacy monitoring,
claims leakage detection (paid-vs-should-have-paid).

**Sources.** CAS / IFoA reserving practice notes; McKinsey claims analytics
2023-2024; carrier case studies on ML-assisted reserving.

### 5. Profitability

**Scope.** Technical result decomposition, plan-variance analysis,
combined-ratio drivers, portfolio profitability attribution.

**State-of-art examples.** Sold-to-tech vs sold-to-plan decomposition,
portfolio profitability attribution by distribution channel / product
line / geography, combined-ratio waterfall analysis, renewal profitability
comparison.

**Sources.** Finance practice (IFRS 17 drivers); McKinsey insurance
profitability 2022-2024.

### 6. Portfolio monitoring *(cross-cutting)*

**Scope.** Concentration risk, appetite monitoring, portfolio drift,
exposure aggregation.

**State-of-art examples.** Real-time concentration dashboards by
geography / industry / peril, appetite-vs-written drift alerts, exposure
aggregation for CAT-prone portfolios, portfolio-level rate-monitoring
roll-ups.

**Sources.** Lloyd's aggregation management standards; hyperexponential
2025 (portfolio management agents); Solvency II / Lloyd's Minimum
Standards on exposure management.

### 7. Monitoring *(cross-cutting)*

**Scope.** Real-time KPI dashboards, drift detection, operational
monitoring that isn't specific to another area.

**State-of-art examples.** Rate-change monitoring, quote volume
monitoring, binder performance monitoring, operational KPI dashboards.

### 8. Copilot / AI agents *(cross-cutting)*

**Scope.** LLM-powered assistance for underwriters, analysts, or
operations — grounded in warehouse data via RAG or structured tool-calls.

**State-of-art examples.** Underwriter copilot (natural-language query
over policy history), claims assistant, pricing copilot, portfolio
manager agent (monitors concentration, flags anomalies, surfaces context).

**Sources.** BCG agentic insurance 2025; Evident AI insurance tracker
Q4 2025 (87% YoY deployment growth); Lloyd's on AI governance.

---

## Bibliography pool

The current bibliography is defined in
`ontology/initiative_research.yaml:sources`. That block is the canonical
list; this section documents the editorial standard for what belongs
there:

- Peer-reviewed actuarial sources (CAS, IFoA, SOA Variance journal)
- Major consultancy insurance-practice publications (McKinsey, BCG, Bain,
  Deloitte, EY, PwC) — prefer reports over blog posts
- Carrier / MGA case studies from named organisations (Hiscox, Allianz,
  Munich Re, etc.)
- Vendor technical white papers where the vendor is a market leader
  (Guidewire, Duck Creek, hyperexponential, Akur8)
- Regulatory and market publications (Lloyd's, PRA, ACORD)
- Industry survey data (LMA AI Survey, Evident AI tracker)

Explicitly out of scope: personal blog posts, LinkedIn thought-leadership
posts, speculative op-eds, vendor marketing without technical substance.

---

## How to read this file as the research script

- **Capability areas** scope what to consider — don't propose initiatives
  outside these, or if you do, flag the new area explicitly.
- **State-of-art examples** set the calibration bar — your recommendations
  should be at or above this bar.
- **Sources lists** are the citation floor — additions are welcome but
  must meet the bibliography editorial standard above.
- **Aspirational initiatives** are welcomed; require `blocker_class` and
  `expected_signal` fields per the tri-state gap-aware schema.
