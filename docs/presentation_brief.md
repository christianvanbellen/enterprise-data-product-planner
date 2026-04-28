# Presentation Brief — Enterprise Data Product Planner

**Audience:** Business executives (Heads of Data, CDO, business stakeholders).
Some technical literacy, no dbt/Snowflake expertise.

**Length target:** ~25 minutes spoken, ~22 slides.

**Repository:** This deck reflects work in [github.com/<org>/enterprise-data-product-planner].
Anchor documents listed at the end of this brief.

**Tone:** Crisp, business-voice, low jargon. The deck is anchored on a domain
(specialty London Market insurance) and should feel like an authoritative
walkthrough of how a business question becomes a governed data product —
not a tour of dbt features.

---

## What this project does, in one sentence

We read a data warehouse, compare it to a curated state-of-the-art reference
for the domain, propose a ranked backlog of data products grounded in
research, and generate runnable SQL for the products the warehouse can
support today.

## What this deck must achieve

By the end, the audience should be able to say in their own words:

1. "They turn business questions into governed data products."
2. "They can show me a working example end-to-end in Snowflake."
3. "They can tell me which products the warehouse supports today and what's
   blocking the rest."

If the audience walks away with those three sentences, the deck has worked.

---

## Section-by-section structure

### Section A — Hook (3 slides)

**Slide 1 — Title.** Project name, presenter name, date. Nothing else.

**Slide 2 — The Monday-morning question.** A single line of business voice
in large type. *"Of my open renewal queue, which ten should I focus on this
week?"* A small caption: *"Every business asks dozens of questions like
this. Most never get answered cleanly."*

**Slide 3 — The answer, live.** Screenshot of a Snowflake worksheet showing
the priority queue query result (10 rows, with policyholder, section,
priority_score, priority_band columns visible). No SQL. No architecture.
Caption: *"This is the answer. Now we'll show how we got there."*

---

### Section B — The architecture as a four-stage story (5 slides)

Each stage gets its own slide. Each slide carries a "what this stage makes
possible" line at the bottom.

**Slide 4 — Stage 1: Read the warehouse.** Inputs: dbt docs, conformed
schemas, source manifests. Output: structured *canonical assets* — every
table, column, and relationship represented in a machine-readable form.
Caption: *"The warehouse describes itself in machine terms. We translate it."*

**Slide 5 — Stage 2: A state-of-the-art archetype.** A curated reference of
what a *good* warehouse looks like for this domain, anchored in industry
research (McKinsey, CAS, hyperexponential, LMA, Lloyd's standards). Caption:
*"Without an external reference, 'is the warehouse good' has no answer."*

**Slide 6 — Stage 3: Reconcile and map.** LLMs compare the canonical assets
against the archetype. Output: enablers (mature primitives the warehouse
supports) and gaps (signals it lacks). On the enabler side, we map
research-grounded initiatives — concrete data products literature names —
and rank by readiness. Caption: *"This is where the warehouse stops being a
passive asset and starts proposing what to build next."*

**Slide 7 — Stage 4: From specification to generated code.** A
`ready_now` initiative carries enough structured context — canonical asset
references, enabler list, business question, asker, cadence — for an LLM to
generate runnable dbt SQL against the warehouse it analysed. Caption: *"The
loop closes. Question becomes spec becomes code becomes data product."*

**Slide 8 — The architecture as one diagram.** Now the full pipeline as a
recap. By this point every box on the diagram is recognisable. Visual:
horizontal flow, four named stages, with the `ready_now` output arrow
landing in a "Snowflake mart" icon on the far right.

---

### Section C — The opportunity graph (3 slides)

**Slide 9 — Where is the warehouse strong?** Visual: 5–7 named *enabler*
nodes (pricing_decomposition, rate_change_monitoring, exposure_structure,
quote_lifecycle, layer_pricing, etc.) with arrows to the initiatives they
support. Demonstrates the warehouse's actual capability surface. Use
`ontology/initiative_research.yaml` as the source.

**Slide 10 — Where is it gap-blocked?** Same visual style, the other side:
missing primitives (claims_experience, plan reference, declared appetite,
bordereaux, CAT model output) with the initiatives blocked behind them.
Frames the next investment conversation.

**Slide 11 — The full initiative landscape.** A matrix view: x-axis =
readiness (ready_now / partial / aspirational / not feasible); y-axis =
business value (or implementation effort, your call). Every initiative is a
labelled dot. Demonstrates the catalogue is comprehensive (not cherry-picked)
and prioritisation is principled.

---

### Section D — The initiative catalogue (3 slides)

**Slide 12 — Readiness, defined.** Four panels in a 2×2 grid, one per
readiness state. Each panel: the state name, a one-line definition, an
example initiative name. States: `ready_now` / `partial` / `aspirational` /
`not_feasible`.

**Slide 13 — Ready-now initiatives (table).** A clean tabular slide, ~13
rows, columns: initiative name, business question (in user voice), asker,
cadence. Pull verbatim from `ontology/initiative_research.yaml` where each
initiative carries a `business_question`, `business_question_asker`, and
`business_question_cadence` field. This is the *deliverable surface*.

**Slide 14 — Aspirational and partial initiatives (table).** Same format,
~13 rows, with an extra column "what's blocking it" populated from the
`expected_signal` field. Shows what investment would unlock the next tier.

---

### Section E — From initiatives to data products (3 slides)

**Slide 15 — What makes this different.** A single slide that names the
moat. *"This is not a dashboard project. It is a planning system that
scaffolds dashboards. The catalogue is question-driven; the marts are
spec-generated; the loop is auditable."* Anchor for the next two slides.

**Slide 16 — 13 initiatives compress to 7 marts.** Visual: 13 question
icons on the left, arrows converging into 7 mart-icon clusters on the right.
The compression is the planner's *delivery decision* — one canonical mart
can fulfil multiple initiatives whose questions can be answered from a
single grain. Source: `ontology/mart_plan.yaml`.

**Slide 17 — Today's demo: 2 of 7 marts, materialised in Snowflake.** Use
the dbt lineage graph as the visual (export from `dbt docs generate`).
Five seeds → six staging views → two canonical marts → six question views.
80 tests. All running in a Snowflake schema. Caption: *"Each transformation
is tested. Each view is contracted to one business question."*

---

### Section F — The six business questions (6 slides)

One slide per question. Same template each time:

- **Top:** the business question in large type, with asker and cadence
- **Middle-left:** the SQL query (small font — visual texture, not for
  reading)
- **Middle-right:** the result (a Snowflake worksheet screenshot)
- **Bottom:** a one-line *"what this lets the [role] do that they couldn't
  do before"*

Source content for these slides is in `dbt_demo/docs/demo_runbook.md` —
each query already has its business context, query SQL, and "what to say
live" written down.

The six questions, in suggested presentation order:

1. **Slide 18:** "Of my open renewal queue, which renewals warrant active
   negotiation versus a light-touch pass?" (renewal_prioritisation)
2. **Slide 19:** "Where in my portfolio is sold premium drifting below
   technical price, and by how much?" (pricing_adequacy_monitoring)
3. **Slide 20:** "How much of this renewal's rate change came from
   deliberate pricing action versus structural changes in exposure, limit,
   breadth, or claims inflation?" (rate_change_attribution_analytics) —
   this is the cleanest "decomposition reconciles" demo beat
4. **Slide 21:** "Which layers in my specialty excess book are written
   below technical adequacy, and is the gap widening on renewal?"
   (layer_rate_adequacy_monitoring)
5. **Slide 22:** "How does our written rate compare to technical price
   across segments, brokers, and underwriters?"
   (technical_price_benchmarking)
6. **Slide 23:** "For this renewal, what is our pricing position versus
   technical, and how has the risk shape moved year-on-year?"
   (underwriting_decision_support)

---

### Section G — Future and close (2 slides)

**Slide 24 — From views to applications.** A natural extension: any of
these views can be turned into a working app via a tool like Google AI
Studio. Visual: a screenshot of a prompt in the AI tool + a screenshot (or
sketch) of the resulting app rendering one of the views. Caption: *"The
view is the contract. The app is downstream."*

**Slide 25 — What an executive walks away with.** Three sentences,
restated from the brief intro:

> *"We have the machinery to turn business questions into governed data
> products. Today's demo proved the loop closes. The next investment is
> whichever blocked initiative the business cares about most."*

---

## Visual / design preferences

- **No clipart.** No emoji. No shadows on text.
- **One key image per slide max.** Empty slides outperform busy ones.
- **Tabular slides (13, 14):** small but legible monospace for the
  business question text; sans-serif for headers.
- **Architecture diagrams:** boxes with rounded corners, single-stroke
  arrows, no gradients. The four-stage story (Slides 4–7) should share a
  consistent visual grammar so the recap (Slide 8) snaps together.
- **Snowflake screenshots:** darken / desaturate slightly so they read as
  texture, not as the focal point. The focal point is always the *business
  signal* in the result, not the SQL.
- **Speaker-note region:** every slide should carry brief speaker notes
  (the "what to say live" content from `demo_runbook.md` for Slides 18–23).

---

## Architecture diagram specification (Slides 4–8)

The four-stage architecture is the deck's spine. Slides 4–7 each feature
*one* stage in detail; Slide 8 shows all four together as a recap. The
visual grammar must be consistent across all five slides so Slide 8 reads
as "the four pieces we've already met, now together."

**Visual identity is left to Claude Design's design system** — colours,
typography, fills, stroke weights, and accent treatments should follow
the system's defaults. The specifications below cover *layout and
emphasis* only.

### Overall flow

Linear, left-to-right, four stages. The output of each stage is the input
of the next. The right-most arrow on Stage 4 lands in a distinct visual
artefact representing the materialised data product (a Snowflake-mart
icon, cylinder, or whatever the design system uses for "data warehouse
table").

### Per-stage block — anatomy

Each stage is one rectangular block. Within the block:

- **Top band** — stage number ("Stage 1") and stage name ("Read the
  warehouse").
- **Inputs region** — 2–4 short bullet items naming what the stage
  consumes.
- **Core verb** — the one-word action the stage performs ("Translate",
  "Curate", "Reconcile", "Generate"). This is the visual focus inside the
  block, larger than the input/output text.
- **Output region** — a single label naming what the stage produces,
  visually differentiated from inputs.

Between blocks: a single connector arrow with a one-word label naming the
artefact passed forward (e.g. "canonical assets", "archetype", "ready_now
spec").

### ASCII layout reference

```
┌────────────────────────┐    ┌────────────────────────┐    ┌────────────────────────┐    ┌────────────────────────┐
│ STAGE 1                │    │ STAGE 2                │    │ STAGE 3                │    │ STAGE 4                │
│ Read the warehouse     │    │ State-of-the-art       │    │ Reconcile and map      │    │ Spec → generated code  │
│                        │    │ archetype              │    │                        │    │                        │
│ INPUTS:                │    │ INPUTS:                │    │ INPUTS:                │    │ INPUTS:                │
│  • dbt docs            │    │  • industry research   │    │  • canonical assets    │    │  • ready_now spec      │
│  • conformed schemas   │    │  • reference frameworks│    │  • archetype           │    │  • canonical assets    │
│  • source manifests    │    │                        │    │                        │    │  • enabler list        │
│                        │    │                        │    │                        │    │                        │
│      TRANSLATE         │    │      CURATE            │    │      RECONCILE         │    │      GENERATE          │
│                        │    │                        │    │                        │    │                        │
│ OUTPUT:                │    │ OUTPUT:                │    │ OUTPUTS:               │    │ OUTPUT:                │
│  canonical assets      │    │  curated archetype     │    │  • enablers + gaps     │    │  runnable dbt SQL      │
│                        │    │                        │    │  • ranked initiatives  │    │                        │
└─────────┬──────────────┘    └─────────┬──────────────┘    └─────────┬──────────────┘    └─────────┬──────────────┘
          │                              │                              │                              │
          └──── canonical assets ────────┼──── archetype ───────────────┼──── ready_now spec ──────────┼──▶ data product
```

### Slides 4–7 — individual stage slides

For each stage slide, render *all four blocks* in the layout above, but
emphasis differs per slide:

- **Active block** (the slide's stage): full opacity, full input/core
  verb/output detail visible, slight scale-up.
- **Other three blocks**: significantly de-emphasised — visible enough
  that the audience sees where they are in the pipeline, but not
  competing for attention. Only the stage number and stage name visible;
  inputs/outputs hidden or compressed.
- **Connector arrows**: also de-emphasised except those entering or
  leaving the active block.

This treatment gives the audience a continuous spatial anchor: every
stage slide shows them where they are in the loop.

### Slide 8 — recap

All four blocks at full emphasis, all detail visible, all connector
arrows prominent, including the final arrow into the data-product
artefact. This is the moment the audience sees the whole loop in one
frame for the first time.

### What to *not* draw

- No cloud icons, AI sparkles, robot heads, or neural-net abstractions.
- No dotted lines — every connector is a deliberate flow.
- No nested boxes inside boxes (one level of containment per block).
- No legend; the labels carry the meaning.

---

## Anchor documents in the repository

These are the source-of-truth files. Claude Design should pull verbatim
content from them rather than rewriting:

| Document | Used on slides | Why it matters |
|---|---|---|
| `ontology/initiative_research.yaml` | 9, 10, 11, 13, 14 | All 26 initiatives with `business_question`, `business_question_asker`, `business_question_cadence`, status, primitives, blockers |
| `ontology/mart_plan.yaml` | 16 | The 13→7 compression mapping; canonical mart definitions; per-view fulfilment annotations |
| `dbt_demo/docs/demo_runbook.md` | 17, 18–23 | Full text for the six demo questions — business context, SQL, "how to read a row", "what to say live" speaker notes |
| `dbt_demo/docs/risks_pack_b_specs.md` | Backup | Risk findings; not in the deck but ready if challenged |
| `dbt_demo/README.md` | 17 | Source-seed inventory + Snowflake schema layout |
| `reference_frameworks/insurance_analytics_state_of_art.md` | 5 | The literature anchor for the state-of-the-art archetype |
| `dbt_demo/models/marts/views/*.sql` | 18–23 | SQL source for the screenshots on each business-question slide |

---

## Things to *not* do

- Don't open with "project phases" framing. Executives don't care how the
  work was organised.
- Don't show the demo runbook content before Section F. The audience needs
  the architecture story first to consume the queries.
- Don't make tests, lineage, or mart_plan into their own slides — they are
  proof points within other slides (16, 17), not headlines.
- Don't show dbt internals (refs, configs, jinja). The audience consumes
  the *output*, not the source.
- Don't use "AI" as a buzzword. The deck mentions LLMs once on Slide 6,
  with a specific role (reconciling canonical assets to the archetype).
  Specificity beats branding.

---

## One closing note

The strongest single moment in the deck is Slide 20 — the rate-change
waterfall. The decomposition reconciles to the headline figure by
construction; the audit column shows residuals within ±0.015 across all 290
renewal layers. *"Look at the residual column — every row is within a few
basis points of zero. The named drivers add up to the headline rate change.
That's not dbt cleverness — that's how a real rating engine produces the
decomposition, faithfully preserved through the warehouse."* Lean into it.
