# Phase 6 — Architecture Alignment Report

## Purpose
Phase 6 produces a structured alignment report that compares
two views of the same warehouse:

  - The architect's view: schema groups, entity definitions,
    and physical asset organisation as built
  - The analytics view: capability primitives required to
    deliver the ranked initiative portfolio from Phase 4

The delta between these views is the enablement backlog —
the set of schema changes, synonym registrations, and source
system integrations that would unlock the full initiative
portfolio.

## The pricing_decomposition case study

The pricing_decomposition primitive illustrates the core
problem Phase 6 addresses.

The pipeline identified five columns (commission, modtech_gnwp,
sold_gnwp, tech_elc, tech_gnwp) that collectively represent
a pricing decomposition capability — the ability to decompose
written premium into its technical, modified technical, and
sold components. This capability is referenced in industry
literature as a foundational analytics primitive for specialty
insurance.

These five columns exist in ll_quote_policy_detail and five
related assets, by exact name, with full descriptions. The
data is there. The pipeline correctly identifies the asset
as the physical implementation of this capability.

However the conformed schema — the architect's formal
classification of business concepts — does not define a
pricing_component group. The columns are classified under
rate_monitoring and coverage groups, which is correct from
an operational modelling perspective but prevents the pipeline
from formally binding the asset to the primitive.

Result: the graph shows pricing_decomposition as an inferred
primitive (amber hexagon) — detected analytically, not
confirmed by the schema. Seven initiatives depend on it.
The remediation is a schema registration, not an ETL change.
This is a one-line fix with high business value.

This pattern — capability detected, schema registration
absent — is the primary output of Phase 6. It tells the
data architecture team not what to build, but what to name
and register in their existing schema so that the analytical
layer can formally recognise what already exists.

## Output artifacts

1. docs/architecture_alignment_report.md
   Human-readable report for data architects and engineering leads.
   Sections: capability coverage matrix, schema recommendations
   (with effort/value), source system gap backlog.
   Generated deterministically from the graph state.

2. output/alignment_report.json
   Machine-readable version. Contains all computed data
   underlying the markdown report. Consumable by downstream
   tooling or future human review workflows.

3. (Optional) Stakeholder presentation
   PowerPoint/PDF for CTO or head of data architecture.
   Generated from alignment_report.json using the pptx skill.
   Three slides: coverage matrix, schema recommendations,
   source system backlog.

## Implementation notes

Phase 6 reads from:
  - output/graph/nodes.json (primitive and gap node states)
  - output/spec_log/index.json (initiative readiness)
  - output/bundle.json (business terms and schema groups)

No LLM call required for the JSON and markdown outputs —
all data is deterministic from the graph state. The LLM
is only needed if generating narrative text for the
stakeholder presentation.

Phase 6 does not modify any upstream phase outputs.
It is a read-only reporting layer over the completed graph.

## Status
Scoped. Not yet implemented.
Blocked on: nothing — all required graph data is available.
Estimated effort: 1-2 days for JSON + markdown generation.
