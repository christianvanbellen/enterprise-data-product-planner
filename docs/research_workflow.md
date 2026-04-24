<!-- Last updated: April 2026 -->
# Research workflow

LLM-assisted curation pipeline for the ontology YAMLs. Three independent
scripts, two cadences, one critical ordering constraint. **All scripts
produce markdown briefs only — nothing auto-mutates YAML.** The curator
reads the brief, decides what to accept, and hand-edits the YAML.

## The three research scripts

| Script | Curates | Cadence | Order |
|---|---|---|---|
| `scripts/research_domain_taxonomy.py` | `domain_keywords.yaml` | annual-ish (rarely changes) | independent |
| `scripts/research_initiatives.py` | `initiative_research.yaml` | semi-annual or when market research updates | **run BEFORE semantic_model** |
| `scripts/research_semantic_model.py` | `entity_bindings.yaml`, `primitives.yaml`, `metric_patterns.yaml` | warehouse-driven (every major dbt change) | **run AFTER curating initiative YAML** |

Briefs land in `ontology/research_log/<folder>/vN_<date>_<hash>.md` —
versioned, diffable, never overwritten.

## The dependency chain (critical)

```
initiative_research.yaml (curated)
            │
            ▼   frozen input
  research_semantic_model.py
            │
            ▼
  primitives / entities / metric_patterns
```

**Initiatives conceptually come first.** A business owner funds an
initiative because of its outcome; primitives and data signal follow from
what the initiative requires. The `semantic_model` pass reads the
**curated** `initiative_research.yaml` as a frozen input and derives
primitive / entity / metric_pattern recommendations grounded in what the
initiatives demand.

**Do NOT run them in parallel.** If you run `semantic_model` before
curating the initiative YAML, it reads the pre-curation state and the
aspirational-initiative linkage is lost. The correct sequence is:

1. Run `research_initiatives.py` → review brief → hand-edit
   `initiative_research.yaml`.
2. THEN run `research_semantic_model.py` → review brief → hand-edit the
   three semantic YAMLs.

The `domain_taxonomy` script is on its own cadence — run it when you
think domains have shifted, not on every cycle.

## Gap-aware philosophy (tri-state schema)

Every proposed entity, primitive, initiative, and metric_patterns key
carries a status:

| Status | Meaning |
|---|---|
| `grounded` | Full warehouse signal + all dependencies present. Ready to build today. |
| `partial` | Some signal; named pieces missing (listed explicitly). |
| `aspirational` | Zero warehouse signal today, BUT load-bearing because a registered initiative (any status) or the reference framework demands it. |

**Aspirational entries are first-class outputs, not noise.** The "empty
audit row" is the backlog signal that tells the data team which concepts
the pipeline is expected to recognise but can't see yet. The existing
`underwriter` entity in `entity_bindings.yaml` is the canonical example —
deliberately empty, clearly annotated.

Partial + aspirational entries must carry:
- `blocker_class` — `data_source_missing` / `schema_group_missing` /
  `tool_missing` / `governance_missing` / `primitive_missing`
- `expected_signal` — what columns / assets / primitives would need to
  land for this to move to `grounded`
- `source` — reference § or initiative_id justifying registration
- `rationale` — one sentence: why this is worth registering now

## Running the pipeline

### 1. (Optional) Domain taxonomy refresh

Only when domain labels may have shifted. Annual-ish cadence.

```bash
.venv/Scripts/python scripts/research_domain_taxonomy.py
# → ontology/research_log/domain_taxonomy/vN_<date>_<hash>.md
# → curate ontology/domain_keywords.yaml manually
```

### 2. Initiative research

```bash
# Default: uses static reference framework only
.venv/Scripts/python scripts/research_initiatives.py

# With web research for fresher citations (slower, more expensive)
.venv/Scripts/python scripts/research_initiatives.py --web-research
# → ontology/research_log/initiatives/vN_<date>_<hash>.md
```

Review Parts A–G of the brief (see "How to read a brief" below) and
hand-edit `ontology/initiative_research.yaml`:
- Add new grounded / partial / aspirational initiatives.
- Update `feasibility_against_warehouse` fields where status changed.
- Merge new bibliography sources (Part D) after validating them.
- Commit as a SEPARATE commit.

Then apply Part F ontology-vocabulary contributions as small follow-on
edits, each a separate commit:
- Add any new `gap_types` surfaced by Part F.1 to
  `ontology/gap_types.yaml` (reconcile naming drift between
  `blocker_class` and `gap_type` values while you're there).
- Add any new `archetype` delivery profiles surfaced by Part F.2 to
  `ontology/delivery_heuristics.yaml` — without these, Phase 5 spec
  rendering will silently fall back to defaults for new archetypes.

### 3. Semantic-model research (reads curated initiatives)

```bash
.venv/Scripts/python scripts/research_semantic_model.py
# → ontology/research_log/semantic_model/vN_<date>_<hash>.md
```

Review Parts A–F and hand-edit in order:
1. `ontology/entity_bindings.yaml` (entities first — signatures, aspirational labels, conformed-group proposals)
2. `ontology/metric_patterns.yaml` (grounded + aspirational keys, corrections)
3. `ontology/primitives.yaml` (primitives last, after entities and metric patterns are frozen — they reference both)
4. `ontology/gap_types.yaml` — apply any new gap_types surfaced by Part E (the semantic-model mirror of the initiative Part F.1). Delivery heuristics are NOT in scope here; that's owned by initiative research.

Each as a separate commit.

### 4. Rebuild affected phases

- Entity signature change → Phase 3 (`scripts/run_phase3.py`)
- metric_patterns change only → Phase 3
- Primitive change → Phase 4 (`scripts/run_phase4.py`)
- Initiative change → Phase 4
- Domain change → Phase 3

Phase 1 and 2 only rebuild when the underlying bundle changes.

## The `--web-research` flag

Off by default. When on:
- Claude's server-side `web_search` tool is enabled (max 8 uses per call)
- The prompt instructs the model to cite additional sources inline with URLs
- Every web-sourced citation appears in the brief's Part D bibliography
- The brief header logs `web_research: enabled` so you can audit which briefs were produced with fresher citations

Use cases:
- **Annual refresh** — pull in Q4-of-last-year reports, new LMA surveys, new case studies.
- **New business line** — the reference framework doesn't cover cyber / parametric / embedded; web research helps scope.
- **Aspirational initiative strengthening** — when an aspirational initiative's value case needs more citation weight.

Cost: ~3–5× a non-web run in runtime, somewhat more in tokens. Don't
enable it on every iteration — it defeats the static-reference
reproducibility.

## How to read a brief

### Initiative brief (`research_log/initiatives/vN_...md`)

| Part | Contents |
|---|---|
| A | Proposed initiative catalogue (table + per-initiative blocks with status, blocker_class, etc.) |
| B | Primitive requirements mapping per initiative (existing vs `NEW:` aspirational) |
| C | Domain coverage analysis (which domains have which initiatives) |
| D | Bibliography (sources cited, new additions flagged) |
| E | Diff vs current YAML (added / removed / modified / status changes) |
| F | Ontology vocabulary contributions — new gap_types (F.1) and archetypes with proposed delivery profiles (F.2). Feeds follow-on edits to `gap_types.yaml` and `delivery_heuristics.yaml`. |
| G | Open questions for the curator |

### Semantic-model brief (`research_log/semantic_model/vN_...md`)

| Part | Contents |
|---|---|
| A | Entity taxonomy (table + definitions + YAML-ready signatures grouped by status + conformed-schema proposals) |
| B | Primitive catalogue (proposed + aspirational pulled forward from the frozen initiative YAML + diff) |
| C | Metric pattern curation (grounded additions, aspirational additions, corrections, removals) |
| D | Cross-layer coherence — **most important section**: aspirational-by-design vs accidentally orphaned, initiative→primitive→entity/metric chain table, triangle coherence, coverage projection |
| E | Ontology vocabulary contributions — new gap_types surfaced by aspirational entities / primitives / metric_patterns. Mirror of the initiative Part F.1. Feeds follow-on edits to `gap_types.yaml`. |
| F | Open questions |

### Domain-taxonomy brief (`research_log/domain_taxonomy/vN_...md`)

See `docs/domain_taxonomy_workflow.md` for its section layout.

## Principles

- **Briefs are proposals, not decisions.** The YAML is the decision.
- **Never auto-mutate YAML from scripts.** Hand-curation is where the
  judgment lives; keep it there.
- **Versioned briefs are forever.** Never overwrite, never delete.
  `git log` over `research_log/` tells you why the YAMLs evolved.
- **Gap-awareness over purity.** An empty audit row is a backlog signal,
  not a defect to hide.
- **Freeze-and-forward.** Each script's inputs include the prior
  layer's *curated* YAML, not its raw brief. The curation step is where
  the human decision lives; downstream scripts must not second-guess it.
