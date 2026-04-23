# Domain taxonomy workflow

How the five domains in `ontology/domain_keywords.yaml` are meant to be
maintained over time. The short version:

> **LLM calls generate artefacts. Humans decide which artefacts become configuration.**

Nothing in the Phase 1–5 pipeline touches the taxonomy automatically. It's a
deliberate, human-driven activity anchored to an external reference framework,
versioned on disk, and reviewed before any change lands in config.

---

## Roles of each artefact

| Artefact | Location | Who writes it | Read by |
|----------|----------|---------------|---------|
| Reference framework | `ontology/reference_frameworks/<name>.md` | Human (once; updates as source evolves) | Research script (as prompt grounding) |
| Research brief | `ontology/research_log/domain_taxonomy/vN_<date>_<hash>.md` | LLM, via `research_domain_taxonomy.py` | Human curator (review-only; never read by pipeline) |
| Working taxonomy | `ontology/domain_keywords.yaml` | Human (hand-edited based on research brief) | Phase 1 `DbtMetadataAdapter` |
| Taxonomy audit | stdout + optional `output/taxonomy_audit/*.md` | Deterministic; `taxonomy_audit.py` | Human (decides when to commission research) |

---

## Flow

```
  +------------------+     +------------------+     +-----------------------+
  | taxonomy_audit   |---->| decide: refresh  |<----| triggered by signal   |
  | (deterministic)  |     | the taxonomy?    |     | (drift, low coverage) |
  +------------------+     +------------------+     +-----------------------+
                                   |
                                   | yes
                                   v
  +-----------------------+     +--------------------------+
  | research_domain_      |---->| research_log/            |
  | taxonomy.py (LLM)     |     | domain_taxonomy/vN.md    |
  +-----------------------+     +--------------------------+
                                             |
                                             | human reviews
                                             v
  +----------------------------+     +--------------------------+
  | edit domain_keywords.yaml  |<----| curator decisions        |
  | (by hand, git-tracked)     |     | (accept / reject / edit) |
  +----------------------------+     +--------------------------+
                  |
                  | next Phase 1 run
                  v
  +---------------------------+
  | domain_candidates /       |
  | domain_scores on assets   |
  +---------------------------+
```

---

## Step 1 — Audit

Run whenever you want a health check on the current taxonomy:

```bash
python scripts/taxonomy_audit.py --bundle output/bundle.json
# optional: also write a markdown copy
python scripts/taxonomy_audit.py --bundle output/bundle.json --output output/taxonomy_audit/$(date -u +%Y-%m-%d).md
```

Read the audit for:

- **Unassigned rate** — assets with zero domain matches. High values (today: 17%) indicate either genuinely out-of-scope assets or missing domains.
- **Tied-primary rate** — assets whose top score is shared by 2+ domains. High values (today: 29%) indicate overlapping keyword corpora or genuinely overlapping business meaning.
- **Dead keywords** — keywords that match zero assets. Either noise to prune or evidence that the keyword set is out of sync with the warehouse vocabulary.
- **Top tokens in unassigned assets** — the strongest signal for missing keywords or domains.

Triggers for commissioning research:
- Unassigned rate > 25% after a major dbt refactor.
- Several "dead keywords" appearing simultaneously.
- Top-token report shows a business concept (e.g. `claims`, `reinsurance`) that has no corresponding domain.
- Qualitative: stakeholder asks "why is X classified as Y?" and you can't justify it.

---

## Step 2 — Research

Pick a reference framework. Add or update a file under `ontology/reference_frameworks/` with the authoritative definitions. **Do not skip this — grounding quality determines research quality.**

Run the research script:

```bash
python scripts/research_domain_taxonomy.py \
  --bundle output/bundle.json \
  --reference lloyds_mdc
```

Optional flags:
- `--model claude-opus-4-7` (default) — switch to `claude-sonnet-4-6` for a lighter-weight pass.
- `--output <path>` — override the default versioned path.
- `--dry-run` — assemble the prompt and print it without calling the API. Useful for calibrating prompt size and reviewing grounding before paying for a call.

The script writes `ontology/research_log/domain_taxonomy/vN_<date>_<hash>.md` — never overwrites. Version `N` auto-increments. The hash fingerprint lets you see at a glance whether a run was against the same warehouse shape as a previous one.

The brief contains:
- **Proposed taxonomy** — domain list with reference-framework justification.
- **Domain definitions** — scope, boundaries, typical warehouse artefacts.
- **Keyword corpus** — per-domain table: keyword, rationale, precision rating, overlap notes.
- **Coverage analysis** — estimate of how the proposal would map the current warehouse.
- **Diff vs current** — added / removed / renamed domains and keywords with rationale.
- **Open questions** — things the curator must decide.

---

## Step 3 — Curate

Read the brief critically. Accept what's defensible, reject what's hand-wavy. Edit `ontology/domain_keywords.yaml` by hand. Add a header comment citing the research version that informed the change:

```yaml
# Last reviewed against: research_log/domain_taxonomy/v3_2026-05-02_8fc04fbe.md
# Reference framework: lloyds_mdc.md
```

Then commit the YAML edit with a message that references the research file. This creates a full audit trail in `git log` of "why does the taxonomy look like this?"

---

## Step 4 — Re-run Phase 1 and verify

```bash
python scripts/run_phase1.py \
  --dbt-metadata data/dbt_metadata_enriched.json \
  --conformed-schema data/conformed_schema.json \
  --output output/bundle.json

python scripts/taxonomy_audit.py --bundle output/bundle.json
```

Expect: lower unassigned rate, fewer dead keywords, a shift in the per-domain counts matching what the research predicted.

Re-run Phase 2 and Phase 3 to propagate the new `domain_scores` and `BELONGS_TO_DOMAIN` edges through the graph.

---

## Why not an in-pipeline LLM call?

- **Determinism.** Phases 1–5 need to be reproducible from the same bundle + config. An LLM in the hot path breaks that.
- **Auditability.** Every taxonomy change should be a single YAML edit in `git log`. Silent drift between builds because the LLM output varied is exactly the class of problem this workflow prevents.
- **Cost control.** LLM calls happen when you commission them, not per build.
- **Decoupling.** You can rerun research without invalidating pipeline state; you can rerun the pipeline without touching the taxonomy.

The research script is intentionally *outside* the pipeline. Think of it like `ontology/initiative_research.yaml` — a hand-curated config file produced by a deliberate research activity, not a live computation.

---

## Related

- `ontology/domain_keywords.yaml` — the actual config consumed by Phase 1.
- `ingestion/adapters/dbt_metadata.py` — `_infer_domains` and `_DOMAIN_FIELD_WEIGHTS` (scoring formula lives here).
- `graph/semantic/domain_assigner.py` — `_confidence_from_score` (score → confidence mapping).
- `docs/phase3_design_brief.md` — Distribution domain gap analysis.
- `docs/inputs.md` — full editable configuration reference.
