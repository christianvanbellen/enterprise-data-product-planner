# Research log

Versioned output from LLM-assisted research scripts. Each run produces a new
markdown file — never overwriting previous versions — so the evolution of a
taxonomy (or any other research artefact) is a `git log` away.

## Directories

- **`domain_taxonomy/`** — output of `scripts/research_domain_taxonomy.py`.
  Proposes a domain taxonomy and keyword corpus grounded in a reference
  framework (see `ontology/reference_frameworks/`) and the current warehouse
  signal.

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
