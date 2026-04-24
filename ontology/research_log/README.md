# Research log

Versioned output from LLM-assisted research scripts. Each run produces a new
markdown file — never overwriting previous versions — so the evolution of a
taxonomy (or any other research artefact) is a `git log` away.

## Directories

- **`domain_taxonomy/`** — output of `scripts/research_domain_taxonomy.py`.
  Proposes a domain taxonomy and keyword corpus grounded in a reference
  framework (see `ontology/reference_frameworks/`) and the current warehouse
  signal. Independent cadence (annual-ish); domain labels change rarely.
- **`initiatives/`** — output of `scripts/research_initiatives.py`.
  Gap-aware initiative catalogue: which analytical capabilities are
  load-bearing for the business, what primitives each requires, and
  which gaps block them. TOP of the research dependency chain —
  initiatives drive primitives drive entities + metric patterns.
  Supports optional web research via `--web-research` flag for fresher
  citations. Curate `initiative_research.yaml` BEFORE running
  `semantic_model` research.
- **`semantic_model/`** — output of `scripts/research_semantic_model.py`.
  Three-layer brief covering entities (`entity_bindings.yaml`), metric
  patterns (`metric_patterns.yaml`), and primitives (`primitives.yaml`).
  Reads the curated `initiative_research.yaml` as a frozen input so
  aspirational initiatives propagate down into aspirational primitives /
  entities / metric_pattern keys. Was called `entity_model/` through v1 —
  renamed once metric-pattern curation was folded into the same pass.

## File naming

```
vN_YYYY-MM-DD_<hash>.md
```

- `vN` — monotonically increasing per subdirectory. The script auto-computes
  the next value by scanning existing files.
- `YYYY-MM-DD` — run date (UTC).
- `<hash>` — first 8 chars of a stable hash over the primary input
  (typically the bundle's asset set). Lets you tell at a glance whether a
  run was against the same warehouse shape as a previous one.

## Lifecycle

1. Script writes a versioned markdown doc here.
2. Human reads it, decides what (if anything) to change.
3. Human edits `ontology/domain_keywords.yaml` (or other config) directly.
4. The YAML header cites the research version that informed the change.

Research files in this log are **read-only inputs to human judgment**. Nothing
else in the pipeline reads them — they exist for audit, diffing between
versions, and supporting future curation.
