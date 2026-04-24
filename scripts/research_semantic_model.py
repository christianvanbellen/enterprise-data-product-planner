#!/usr/bin/env python
"""One-shot, human-triggered entity + primitive + metric-pattern research via an LLM call.

Second pass in the research chain. Sibling to scripts/research_initiatives.py
(run FIRST) and scripts/research_domain_taxonomy.py (independent cadence).
Produces a three-layer brief covering the entity layer (business nouns), the
primitive layer (analytical capabilities), and the metric_patterns layer
(column-name → metric-concept mappings).

The dependency chain is:

  initiative (curated, frozen) → primitive → { entity, metric_pattern }

This script reads the curated initiative_research.yaml as a FROZEN input
and derives primitive / entity / metric_pattern recommendations grounded
in what the initiatives require. Gap-aware philosophy is applied uniformly:
every proposal carries a status (grounded / partial / aspirational), and
aspirational entries are first-class outputs (they form the backlog map).

Runs OUT OF the Phase 1-5 pipeline. Writes a versioned markdown brief to
ontology/research_log/semantic_model/ that the curator then reads and uses to
edit entity_bindings.yaml, primitives.yaml, and metric_patterns.yaml
manually. Nothing about this script mutates configuration directly.

Inputs:
  --bundle        CanonicalBundle from Phase 1 (warehouse signal).
  --reference     Filename (without .md) under ontology/reference_frameworks/.
  --audit-report  Optional path to a markdown audit (e.g. from
                  scripts/entity_audit.py --output ...) to include as
                  empirical grounding.
  --model         Anthropic model; defaults to Opus for deep research.
  --max-tokens    Output token cap (default 20000).
  --output        Override output path.
  --dry-run       Print the prompt without calling the API.

See docs/domain_taxonomy_workflow.md for the equivalent workflow on domains.
"""

from __future__ import annotations

import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

import typer
from rich.console import Console

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)
except ImportError:
    pass

from ingestion.normalisation.hashing import stable_hash

app = typer.Typer(help="Run LLM-assisted entity + primitive + metric-pattern research.")
console = Console()

_REPO_ROOT = Path(__file__).resolve().parent.parent
_REFERENCE_DIR = _REPO_ROOT / "ontology" / "reference_frameworks"
_LOG_DIR = _REPO_ROOT / "ontology" / "research_log" / "semantic_model"
_ENTITY_BINDINGS_YAML = _REPO_ROOT / "ontology" / "entity_bindings.yaml"
_ENTITY_GROUPS_YAML = _REPO_ROOT / "ontology" / "entity_groups.yaml"
_PRIMITIVES_YAML = _REPO_ROOT / "ontology" / "primitives.yaml"
_METRIC_PATTERNS_YAML = _REPO_ROOT / "ontology" / "metric_patterns.yaml"
# Frozen input from scripts/research_initiatives.py — the curated initiative
# catalogue drives what primitives / entities / metric patterns need to exist.
_INITIATIVE_YAML = _REPO_ROOT / "ontology" / "initiative_research.yaml"
# Adjacent vocabulary whose values are set by aspirational-primitive /
# aspirational-entity / aspirational-metric blocker reasons. Loaded as a
# frozen input so Part E can diff proposed blocker_class values against it.
# Delivery heuristics is NOT loaded here — archetypes are an initiative-layer
# concern, curated by research_initiatives.py.
_GAP_TYPES_YAML = _REPO_ROOT / "ontology" / "gap_types.yaml"

_DEFAULT_MODEL = "claude-opus-4-7"
_MAX_TOKENS = 20000

_SYSTEM_PROMPT = """You are an insurance data semantic-model architect running \
a combined entity, primitive, and metric-pattern design review. You have been given:

  1. A reference framework (authoritative external taxonomy).
  2. The current warehouse signal (asset names, tags, sample columns).
  3. **The curated initiative catalogue (initiative_research.yaml)** — FROZEN
     INPUT from the prior `research_initiatives.py` pass. Every initiative
     carries a status (grounded / partial / aspirational). The initiatives
     drive which primitives need to exist; the primitives drive which
     entities + metric concepts need to exist.
  4. The current entity config (entity_bindings.yaml, entity_groups.yaml) —
     what the pipeline recognises today.
  5. The current primitive catalogue (primitives.yaml) — what analytical
     capabilities are defined, and which entities each requires.
  6. The current metric_patterns.yaml — column-name patterns that map to
     metric concepts, consumed by primitives via required_columns and by
     the semantic compiler to create MEASURES edges.
  7. The current gap_types vocabulary (ontology/gap_types.yaml) — READ-ONLY.
     Controlled vocabulary for blocker_class / gap_type values on
     aspirational primitives / entities / metric_pattern keys. Part E
     will diff your proposed blocker_class values against this file.
  8. Optionally, an entity audit report showing unbound rate, dead signatures,
     and per-signal attribution against the current warehouse.

Your task is to propose a revised entity taxonomy, primitive-catalogue \
updates, AND metric_patterns.yaml refinements in a single coherent brief. \
The layers form a dependency chain:

  initiative (frozen) → primitive → { entity, metric_pattern }

Coherence rules:
  - Every primitive's required_entities MUST use labels in the entity
    whitelist (current or newly-proposed).
  - Every primitive's required_columns reference metric concepts that must
    be produced by at least one key in metric_patterns.yaml.
  - Every required_primitive named in the initiative catalogue must either
    exist in primitives.yaml OR be proposed in Part B (with status label).
  - An entity / metric_pattern key / primitive with no initiative or
    primitive that requires it is orphaned — flag it.

# Gap-aware philosophy (CRITICAL)

Every proposed entity, primitive, and metric_patterns key carries a status:

  status: grounded | partial | aspirational

    grounded     — full warehouse signal exists; ready to use today.
    partial      — some signal, listed missing pieces.
    aspirational — zero signal today, BUT required by a registered
                   initiative (any status) or explicitly demanded by the
                   reference framework. These are DELIBERATELY REGISTERED
                   GAPS — the "empty audit row" IS the backlog signal that
                   tells the data team which concepts the pipeline is
                   expected to recognise but can't see yet. Do NOT filter
                   out aspirational entries. Do NOT require every column
                   or pattern key to be real. Aspirational registrations
                   are a primary output of this research.

Partial + aspirational entries MUST carry:
  blocker_class:   data_source_missing | schema_group_missing |
                   tool_missing | governance_missing
  expected_signal: one paragraph — what columns / assets / primitives
                   would need to land for this to move to `grounded`.
  source:          reference § or initiative_id that justifies registering
                   this as load-bearing despite the gap.
  rationale:       one sentence — why this is worth registering now.

Example (the existing `underwriter` entity in entity_bindings.yaml is the
model — gap-aware, deliberately empty, clearly annotated).

This output will be reviewed by a human curator who will manually edit \
YAML based on your recommendations. Be concrete; prefer specific column \
names, primitive IDs, and pattern keys over hand-waving.

Output a single markdown document with the following sections, in order:

## Part A — Entity model

### Proposed entity taxonomy
One row per entity. Columns:
  - entity label
  - status (grounded / partial / aspirational)
  - reference framework § that justifies it
  - which registered initiative(s) from initiative_research.yaml require
    this entity (transitively, via required_primitives). Aspirational
    entities without an initiative citation are dubious — either find
    the initiative link or drop the entity.

Prefer business nouns that appear as asset-shaped concepts. Avoid \
classifications (e.g. product line, jurisdiction) that belong on tag \
dimensions, not on BusinessEntityNode.

### Entity definitions
For each proposed entity: scope, boundary with adjacent entities, typical \
warehouse artefacts (asset name patterns, column families) that belong. \
For aspirational entities, describe what the boundary WILL look like \
when data lands.

### Entity signatures (YAML-ready)
For each entity, propose the column signature (normalised column names) \
for Signal 2 matching. Columns should be grouped by status:

  - **grounded columns**: real columns observed in the warehouse signal.
    Flag columns you are uncertain about.
  - **aspirational columns** (optional, for gap-aware entities): the
    column names we'd expect to see when the data lands. Include them
    even though they match zero assets today — they make the expected
    schema explicit and give ingestion-team a target. Annotate with
    a YAML comment `# aspirational — <source>`.

The `underwriter` entity signature in entity_bindings.yaml is the \
canonical example of an aspirational signature. Copy that pattern.

### Conformed-schema proposals
Bullets: new conformed-schema entity groups the discovery layer is \
identifying as load-bearing enough to deserve governance. For each, list \
the group name, candidate child fields, and status. These are \
ingestion-layer proposals the curator can take to the data team; they \
do NOT go into entity_bindings.yaml directly.

## Part B — Primitive catalogue

### Proposed primitives
For each primitive (new or modified): id, status (grounded / partial / \
aspirational), required_entities, required_columns (or \
required_tag_dimension + required_tags), supporting_domains, description. \
Explicitly tie required_entities to the entity labels in Part A and \
(by initiative_id) to initiatives in initiative_research.yaml that \
require this primitive.

### Aspirational primitives
Every primitive named in `required_primitives` or `optional_primitives` \
of an initiative in initiative_research.yaml that is NOT in \
primitives.yaml must appear here as aspirational. Include the \
initiative_id(s) that need it under `source`. This is how aspirational \
initiatives propagate down into the primitive layer.

### Diff vs current primitives.yaml
Bullets per primitive: added, modified (with what changed), removed \
(with why). Status changes explicitly called out (e.g. grounded → \
aspirational because the entity it depends on was reclassified).

## Part C — Metric pattern curation

### Proposed metric_patterns.yaml additions — grounded
YAML-ready bullets of the form `<pattern_key>: <metric_concept>` for \
measure-shaped columns observed in the warehouse signal that have no key \
in the current metric_patterns.yaml. Prefer the longest discriminating \
token (e.g. `modtech_gnwp` over `gnwp`) where both would match; the \
compiler's longest-first tie-break relies on that.

### Proposed metric_patterns.yaml additions — aspirational
YAML-ready bullets for pattern keys whose mapped concept is required by \
a registered initiative or aspirational primitive, even though no \
warehouse column matches the key today. These are NO-OPs at compile time \
(they match nothing) but they pin the expected column name so that when \
the data lands the binding is already in place. Annotate each with a \
YAML comment `# aspirational — <initiative_id> / <source>`.

### Proposed metric_patterns.yaml corrections
Bullets of the form `<pattern_key>: <current_concept> → <proposed_concept>` \
with a one-line rationale. Flag any key whose mapped concept conflicts \
with the entity signature it co-appears under (e.g. a key on a \
pricing_component signature column mapping to a non-pricing concept).

### Proposed metric_patterns.yaml removals
Bullets for keys that match no warehouse column AND no aspirational \
primitive requires their concept. Conservative default: retain. Only \
recommend removal if the key is wrong (bad concept mapping), not if \
it's merely unused today.

## Part D — Cross-layer coherence

### Aspirational vs accidentally orphaned
This is the most important distinction in the brief. Separate:

  **Aspirational by design** — entity / primitive / metric_pattern key
    that is deliberately registered despite zero current signal because
    a registered initiative (any status) requires it. These are the
    backlog map. List them with the initiative_id(s) that justify each.

  **Accidentally orphaned** — entity / primitive / metric_pattern key
    that is present but no initiative or primitive references it. These
    are candidates for removal OR for flagging a missing initiative that
    should exist to justify them.

### Orphan analysis (traditional)
- Entities with no primitive requiring them.
- Primitives not required (or optional) by any initiative — these are
  orphan primitives; recommend removal OR linking to a new aspirational
  initiative.
- Metric concepts referenced by primitives' `required_columns` but not
  produced by any current or proposed metric_patterns key (dead-end
  primitives).
- metric_patterns keys whose mapped concept is not required by any
  primitive.

### Initiative → primitive → entity/metric chain
One row per initiative in initiative_research.yaml. Columns:
  - initiative_id
  - initiative_status (grounded / partial / aspirational from the frozen
    initiative YAML)
  - required_primitives and their proposed status (after this brief)
  - required_entities (from those primitives)
  - required metric concepts (from those primitives)
  - gap summary — what's still blocking this initiative after the
    proposed changes land
Flag any initiative whose status would IMPROVE after the proposed \
entity + primitive + metric_pattern changes (these are the high-leverage \
curations). Also flag initiatives whose status would NOT change despite \
proposed changes (nothing moves the needle for them — may need an \
upstream data-team conversation).

### Triangle coherence table (primitive-level)
One row per primitive that has `required_columns`. Columns:
  - primitive id and status
  - required metric concept(s)
  - metric_patterns.yaml key(s) that produce each concept
  - entity signature column(s) the matching key(s) would bind to
Flag rows where the concept has no pattern key, or where the matching \
column is not in any entity signature.

### Coverage projection
Estimate:
  - Unbound rate if the proposed entity taxonomy is adopted.
  - Number of primitives that would move from 'confirmed' to 'inferred'
    or vice versa given proposed entity + metric-pattern changes.
  - Count of initiatives whose status would improve.
  - Net direction of initiative-readiness movement (qualitative; the
    curator will measure exactly after re-running Phase 4).

## Part E — Ontology vocabulary contributions

Semantic-model curation decisions set downstream vocabulary for the \
gap_types controlled list. Surface those contributions explicitly so \
the curator can update `ontology/gap_types.yaml` in the same commit \
cycle.

### E.1 — Gap types used

Collect every `blocker_class` value used on aspirational / partial \
entities, primitives, and metric_pattern keys across Parts A-C. For \
each distinct value, state:
  - the value
  - the artefact(s) using it (entity_label / primitive_id / pattern_key)
  - whether it is already in `ontology/gap_types.yaml` (quote current
    entry if so)
  - if not, propose adding it with a one-line description suitable for
    the gap_types.yaml controlled vocabulary

ALSO flag alignment issues between `blocker_class` values you've used \
and existing `gap_type` entries. Different names for the same concept \
(e.g. `data_source_missing` vs `missing_source_system`) are a sign the \
two vocabularies have drifted. Propose reconciliation — prefer treating \
`gap_types.yaml` as canonical and aligning `blocker_class` values to \
match, unless a cleaner rename of gap_types makes more sense.

Archetypes and delivery profiles are NOT in scope for this pass — they \
are curated by `research_initiatives.py` against `delivery_heuristics.yaml`.

## Part F — Open questions
Things the human reviewer must decide that you cannot: ambiguous entity \
boundaries, missing reference content, warehouse artefacts you couldn't \
classify, primitives whose required_entities could plausibly be several \
alternative labels, columns whose metric concept is genuinely ambiguous, \
gap_type vocabulary additions from Part E that the curator may want to \
rename or merge before accepting.

Be honest about uncertainty. If the reference framework is thin (e.g. a \
placeholder), say so prominently and caveat recommendations."""


def _load_reference(name: str) -> str:
    path = _REFERENCE_DIR / f"{name}.md"
    if not path.exists():
        console.print(f"[red]Reference framework not found: {path}[/red]")
        console.print(
            f"[yellow]Available:[/yellow] "
            + ", ".join(p.stem for p in _REFERENCE_DIR.glob("*.md") if p.stem != "README")
        )
        raise typer.Exit(1)
    return path.read_text(encoding="utf-8")


def _safe_read(path: Path, label: str) -> str:
    if not path.exists():
        return f"(missing — {label})"
    return path.read_text(encoding="utf-8")


def _warehouse_signal(bundle_path: Path, max_assets: int = 250,
                     top_columns: int = 150) -> dict:
    """Condense the bundle into a compact signal for prompt grounding."""
    if not bundle_path.exists():
        console.print(f"[red]Bundle not found: {bundle_path}[/red]")
        raise typer.Exit(1)

    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    assets = bundle.get("assets", [])
    columns = bundle.get("columns", [])

    asset_names = sorted({a["name"] for a in assets})[:max_assets]
    all_tags: Counter = Counter()
    for a in assets:
        all_tags.update(a.get("tags") or [])

    col_freq: Counter = Counter()
    for c in columns:
        col_freq[c["name"].lower()] += 1
    most_common_columns = [name for name, _ in col_freq.most_common(top_columns)]

    sample_descriptions = [
        (a["name"], (a.get("description") or "").strip())
        for a in assets
        if (a.get("description") or "").strip()
    ][:30]

    return {
        "total_assets": len(assets),
        "total_columns": len(columns),
        "asset_names": asset_names,
        "tag_frequency": all_tags.most_common(40),
        "most_common_columns": most_common_columns,
        "sample_descriptions": sample_descriptions,
    }


def _format_signal(signal: dict) -> str:
    lines: List[str] = [
        f"Total assets: {signal['total_assets']}",
        f"Total columns: {signal['total_columns']}",
        "",
        "## Asset names (sample)",
    ]
    for name in signal["asset_names"]:
        lines.append(f"- {name}")

    lines += ["", "## Top dbt tags"]
    for tag, count in signal["tag_frequency"]:
        lines.append(f"- `{tag}` × {count}")

    lines += ["", "## Most common column names"]
    for col in signal["most_common_columns"]:
        lines.append(f"- {col}")

    if signal["sample_descriptions"]:
        lines += ["", "## Sample asset descriptions"]
        for name, desc in signal["sample_descriptions"]:
            lines.append(f"- **{name}**: {desc[:200]}")

    return "\n".join(lines)


def _build_user_message(
    reference: str,
    signal_str: str,
    initiative_yaml: str,
    entity_bindings_yaml: str,
    entity_groups_yaml: str,
    primitives_yaml: str,
    metric_patterns_yaml: str,
    gap_types_yaml: str,
    audit_report: Optional[str],
) -> str:
    parts = [
        "# Reference framework",
        "",
        reference,
        "",
        "---",
        "",
        "# Frozen initiative catalogue — initiative_research.yaml",
        "",
        "> This is the curated output of the prior `research_initiatives.py`",
        "> run. Treat it as a FROZEN INPUT — do not propose changes to",
        "> initiatives in this brief. Every proposal below must trace back",
        "> to an initiative in this file (grounded, partial, or aspirational).",
        "",
        "```yaml",
        initiative_yaml,
        "```",
        "",
        "---",
        "",
        "# Current entity config — entity_bindings.yaml",
        "",
        "```yaml",
        entity_bindings_yaml,
        "```",
        "",
        "# Current entity config — entity_groups.yaml",
        "",
        "```yaml",
        entity_groups_yaml,
        "```",
        "",
        "# Current primitive catalogue — primitives.yaml",
        "",
        "```yaml",
        primitives_yaml,
        "```",
        "",
        "# Current metric patterns — metric_patterns.yaml",
        "",
        "```yaml",
        metric_patterns_yaml,
        "```",
        "",
        "# Current gap_types vocabulary (READ-ONLY) — gap_types.yaml",
        "",
        "```yaml",
        gap_types_yaml,
        "```",
        "",
    ]
    if audit_report:
        parts += [
            "---",
            "",
            "# Entity audit report (empirical baseline)",
            "",
            audit_report,
            "",
        ]
    parts += [
        "---",
        "",
        "# Warehouse signal",
        "",
        signal_str,
    ]
    return "\n".join(parts)


def _next_version(log_dir: Path) -> int:
    if not log_dir.exists():
        return 1
    existing = [p.name for p in log_dir.glob("v*.md")]
    nums = []
    for name in existing:
        m = re.match(r"v(\d+)_", name)
        if m:
            nums.append(int(m.group(1)))
    return (max(nums) + 1) if nums else 1


def _default_output_path(signal: dict) -> Path:
    n = _next_version(_LOG_DIR)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    fingerprint = stable_hash(json.dumps(signal["asset_names"], sort_keys=True))[:8]
    return _LOG_DIR / f"v{n}_{today}_{fingerprint}.md"


def _call_anthropic(model: str, system: str, user: str, max_tokens: int) -> tuple[str, str]:
    try:
        import anthropic
    except ImportError:
        console.print("[red]anthropic package not installed. Install with `pip install anthropic`.[/red]")
        raise typer.Exit(1)

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        console.print("[red]ANTHROPIC_API_KEY not set (check .env or environment).[/red]")
        raise typer.Exit(1)

    client = anthropic.Anthropic(api_key=api_key)
    console.print(f"[cyan]Calling {model}[/cyan] — this may take 60–180 seconds…")
    message = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    text = message.content[0].text if message.content else ""
    return text, getattr(message, "stop_reason", "") or ""


@app.command()
def main(
    bundle: Path = typer.Option(
        Path("output/bundle.json"), "--bundle",
        help="Path to bundle.json from Phase 1",
    ),
    reference: str = typer.Option(
        "commercial_specialty_pc", "--reference",
        help="Reference framework filename (without .md) under ontology/reference_frameworks/",
    ),
    audit_report: Optional[Path] = typer.Option(
        None, "--audit-report",
        help="Optional path to a markdown audit report to include as empirical "
             "grounding (e.g. output of scripts/entity_audit.py --output ...)",
    ),
    model: str = typer.Option(
        _DEFAULT_MODEL, "--model",
        help="Anthropic model id",
    ),
    max_tokens: int = typer.Option(
        _MAX_TOKENS, "--max-tokens",
        help="Override output token cap (default 16000). Bump if briefs truncate.",
    ),
    output: Optional[Path] = typer.Option(
        None, "--output",
        help="Override output path (default: ontology/research_log/semantic_model/vN_<date>_<hash>.md)",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Print the assembled prompt without calling the API",
    ),
) -> None:
    reference_text = _load_reference(reference)
    signal = _warehouse_signal(bundle)
    signal_str = _format_signal(signal)
    initiatives = _safe_read(_INITIATIVE_YAML, "initiative_research.yaml")
    entity_bindings = _safe_read(_ENTITY_BINDINGS_YAML, "entity_bindings.yaml")
    entity_groups = _safe_read(_ENTITY_GROUPS_YAML, "entity_groups.yaml")
    primitives = _safe_read(_PRIMITIVES_YAML, "primitives.yaml")
    metric_patterns = _safe_read(_METRIC_PATTERNS_YAML, "metric_patterns.yaml")
    gap_types = _safe_read(_GAP_TYPES_YAML, "gap_types.yaml")
    audit_text = audit_report.read_text(encoding="utf-8") if audit_report else None

    user_msg = _build_user_message(
        reference_text, signal_str,
        initiatives,
        entity_bindings, entity_groups,
        primitives, metric_patterns, gap_types, audit_text,
    )

    if dry_run:
        console.print("[bold]System prompt (excerpt):[/bold]")
        console.print(_SYSTEM_PROMPT[:1500] + "…")
        console.print(f"\n[cyan]Prompt size:[/cyan] ~{len(user_msg)} chars "
                      f"(≈ {len(user_msg) // 4} tokens)")
        return

    response, stop_reason = _call_anthropic(model, _SYSTEM_PROMPT, user_msg, max_tokens)

    if stop_reason == "max_tokens":
        console.print(
            "[yellow]Warning:[/yellow] response hit the max-tokens ceiling "
            f"({max_tokens}). The brief is likely truncated. Rerun with "
            "`--max-tokens <higher>` to capture the full output."
        )

    header = [
        "---",
        f"reference_framework: {reference}",
        f"bundle: {bundle}",
        f"audit_report: {audit_report or '(none)'}",
        f"model: {model}",
        f"generated_utc: {datetime.now(timezone.utc).isoformat()}",
        f"asset_fingerprint: {stable_hash(json.dumps(signal['asset_names'], sort_keys=True))[:8]}",
        f"max_tokens: {max_tokens}",
        f"stop_reason: {stop_reason}",
        "---",
        "",
        "# Entity + primitive + metric-pattern research",
        "",
        "> LLM-generated research brief covering the entity layer, the "
        "metric_patterns layer, and the primitive catalogue. Curate entities "
        "first, metric patterns second, primitives third — each layer uses "
        "the previous one as a frozen input. See "
        "`docs/domain_taxonomy_workflow.md` for the analogous pattern on domains.",
        "",
    ]

    out_path = output or _default_output_path(signal)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(header) + response + "\n", encoding="utf-8")
    console.print(f"[green]Research brief written to:[/green] {out_path}")
    console.print(
        "[yellow]Next steps:[/yellow]\n"
        "  1. Review the brief.\n"
        "  2. Curate entity-layer YAML edits, regenerate Phase 1-3, run entity_audit.\n"
        "  3. Curate metric_patterns.yaml against frozen entities, "
        "regenerate Phase 3.\n"
        "  4. Curate primitive-layer YAML edits against frozen entities + "
        "metric patterns, regenerate Phase 4.\n"
        "  5. Apply Part E ontology-vocabulary contributions: add any new "
        "gap_types to ontology/gap_types.yaml.\n"
        "  6. Commit entity, metric-pattern, primitive, and vocabulary "
        "changes as separate commits."
    )


if __name__ == "__main__":
    app()
