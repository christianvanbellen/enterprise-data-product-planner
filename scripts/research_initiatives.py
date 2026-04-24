#!/usr/bin/env python
"""One-shot, human-triggered initiative research via an LLM call.

Sibling to scripts/research_domain_taxonomy.py and
scripts/research_semantic_model.py. This is the TOP of the research
dependency chain — initiatives conceptually precede primitives and
entities in the reasoning order (a business owner funds an initiative
because of its outcome; primitives and data signal follow from what the
initiative requires).

Runs OUT OF the Phase 1-5 pipeline. Writes a versioned markdown brief to
ontology/research_log/initiatives/ that the curator reads and uses to
hand-edit ontology/initiative_research.yaml. Nothing about this script
mutates configuration directly.

Inputs:
  --bundle          CanonicalBundle from Phase 1 (warehouse signal).
  --reference       Filename (without .md) under ontology/reference_frameworks/.
                    Default: insurance_analytics_state_of_art.
  --web-research    Enable Claude's server-side web_search tool (off by
                    default). When on, the model may cite sources beyond
                    the static reference; those citations are tagged in
                    the brief header.
  --model           Anthropic model; defaults to Opus for deep research.
  --max-tokens      Output token cap (default 20000).
  --output          Override output path.
  --dry-run         Print the prompt without calling the API.

Gap-aware philosophy: every proposed initiative carries a `status`
(grounded / partial / aspirational). Aspirational initiatives are
explicitly welcomed — they are the map that future warehouse investment
follows. Each aspirational item must carry blocker_class,
expected_signal, source, and rationale fields.

See README.md for the overall research pipeline story.
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

app = typer.Typer(help="Run LLM-assisted initiative research.")
console = Console()

_REPO_ROOT = Path(__file__).resolve().parent.parent
_REFERENCE_DIR = _REPO_ROOT / "ontology" / "reference_frameworks"
_LOG_DIR = _REPO_ROOT / "ontology" / "research_log" / "initiatives"
_INITIATIVE_YAML = _REPO_ROOT / "ontology" / "initiative_research.yaml"
_PRIMITIVES_YAML = _REPO_ROOT / "ontology" / "primitives.yaml"
_DOMAIN_KEYWORDS_YAML = _REPO_ROOT / "ontology" / "domain_keywords.yaml"
# Adjacent vocabularies whose values are set by initiative decisions:
#   - gap_types.yaml:          blocker_class / gap_type controlled vocabulary
#   - delivery_heuristics.yaml: archetype → delivery profile (refresh/sla/format)
# Loaded as frozen inputs so the brief can diff proposed values against them
# in Part F (Ontology vocabulary contributions).
_GAP_TYPES_YAML = _REPO_ROOT / "ontology" / "gap_types.yaml"
_DELIVERY_HEURISTICS_YAML = _REPO_ROOT / "ontology" / "delivery_heuristics.yaml"

_DEFAULT_MODEL = "claude-opus-4-7"
_DEFAULT_REFERENCE = "insurance_analytics_state_of_art"
_MAX_TOKENS = 20000
_WEB_SEARCH_MAX_USES = 8

_SYSTEM_PROMPT = """You are an insurance data product strategist running an \
initiative design review. Initiatives sit at the top of the reasoning \
chain — an initiative is a load-bearing business outcome that primitives \
and data signal exist to enable, not the other way around. You have been \
given:

  1. A state-of-art reference framework (capability areas, editorial bar,
     bibliography standard).
  2. The current warehouse signal (asset names, tags, sample columns).
  3. The current initiative catalogue (ontology/initiative_research.yaml)
     — what initiatives are registered today, their scoring fields, their
     feasibility labels.
  4. The current primitive catalogue (ontology/primitives.yaml) — READ-ONLY
     input. Primitives named in required_primitives must come from this
     list, OR be explicitly proposed as new aspirational primitives to be
     added in a later semantic-model research pass.
  5. The current domain taxonomy (ontology/domain_keywords.yaml) — READ-ONLY
     input. Use domain labels for the `category` field on each initiative.
  6. The current gap_types vocabulary (ontology/gap_types.yaml) — READ-ONLY.
     This is the controlled vocabulary for blocker_class / gap_type values.
     Part F will diff your proposed blocker_class values against this file.
  7. The current delivery_heuristics (ontology/delivery_heuristics.yaml) —
     READ-ONLY. Archetype → delivery profile (refresh / sla / format) used
     by Phase 5 spec rendering. Every archetype you use must be either in
     this file or proposed with a delivery profile in Part F.

Your task is to propose a revised initiative catalogue that implements a \
gap-aware philosophy — aspirational initiatives are first-class, not \
filtered out. The map of aspirational initiatives IS the backlog the \
data team uses to prioritise warehouse investment.

# Gap-aware schema (apply to EVERY initiative)

Every proposed initiative carries:

  status: grounded | partial | aspirational
    grounded     — required primitives + columns all present in the
                   current warehouse. Ready to build today.
    partial      — some required primitives present; named pieces missing
                   (list them).
    aspirational — the business outcome is load-bearing per the reference
                   framework + external sources, but the warehouse does
                   not yet support it. Registered so that when the data
                   lands, the framing is already in place.

For partial + aspirational initiatives, ALSO include:

  blocker_class: data_source_missing | schema_group_missing |
                 tool_missing | governance_missing | primitive_missing
  expected_signal: one-paragraph description of what data / assets /
                   columns / primitives would need to land for this to
                   move to `grounded`.
  source: reference-framework § + citation(s) from the bibliography pool
          (or newly-cited sources if web research is enabled).
  rationale: one sentence — why this initiative is load-bearing even
             though it's gap-blocked today.

Grounded initiatives may also carry `source` when the business case for
funding them is strengthened by citation.

# Output structure

Output a single markdown document with the following sections, in order:

## Part A — Proposed initiative catalogue

A table row per initiative. Columns:
  - id (snake_case)
  - status (grounded / partial / aspirational)
  - category (must match a domain label from ontology/domain_keywords.yaml,
    OR flag as a new domain proposal)
  - archetype (monitoring / decision_support / ai_agent / automation /
    prioritization / analytics_product — reuse values from the current
    catalogue where possible)
  - required_primitives (list; primitives from primitives.yaml, OR
    `NEW: <primitive_name>` for aspirational primitives)
  - business_value_score (0.0-1.0)
  - implementation_effort_score (0.0-1.0)
  - one-line business_objective

Below the table, for each initiative, write a short block (4-8 lines):

  ### <initiative_id>
  **Status:** <status>
  **Business objective:** <one sentence>
  **Output type:** <monitoring_dashboard / decision_support / ai_agent / ...>
  **Target users:** <list>
  **Sources:** <citation ids, with inline descriptions if new>
  **Literature quote:** <exact short quote from a source, if applicable>
  (If partial/aspirational:)
  **Blocker class:** <one of the valid values>
  **Expected signal:** <paragraph>
  **Rationale:** <one sentence>

## Part B — Primitive requirements mapping

For each initiative, list its required_primitives. Separate into:
  - primitives present in primitives.yaml (just cite id)
  - aspirational primitives (label `NEW:` and describe briefly — these
    will be formally added in the next semantic-model research pass)

Flag any initiative whose required_primitives set contains only
aspirational primitives (these are pure-gap initiatives, important but
the most fragile).

## Part C — Domain coverage analysis

For each domain in ontology/domain_keywords.yaml:
  - count of grounded initiatives
  - count of partial initiatives
  - count of aspirational initiatives

Flag domains with ZERO initiatives (any status) as a coverage gap — is \
the omission intentional (domain doesn't produce analytical initiatives \
in this business), or is it a backlog signal?

## Part D — Bibliography

All sources cited in Part A. Format per existing `sources` block in
initiative_research.yaml — id, title, publisher, year, url (if web-sourced
or publicly available). Flag newly-added sources (those NOT in the current
YAML's bibliography pool) so the curator can decide whether to merge them.

## Part E — Diff vs current catalogue

Bullets:
  - added initiatives (new rows not in current YAML)
  - removed initiatives (with rationale — prefer status change to
    removal; only remove if the initiative is genuinely superseded or
    conceptually wrong)
  - modified initiatives (fields changed, with old → new)
  - status changes (e.g. `grounded` → `aspirational` because a primitive
    the curator previously had is now flagged as absent)

## Part F — Ontology vocabulary contributions

Initiative curation decisions set downstream vocabulary. Surface those \
contributions explicitly so the curator can update adjacent YAMLs in the \
same commit cycle.

### F.1 — Gap types used

List every distinct `blocker_class` value used across the initiatives \
in Part A. For each, state:
  - the value
  - the initiative_id(s) that use it
  - whether it is already in `ontology/gap_types.yaml` (quote the current
    entry if so)
  - if not, propose adding it, with a one-line description suitable for
    the gap_types.yaml controlled vocabulary

ALSO flag alignment issues between `blocker_class` (used in initiatives) \
and `gap_type` (used in gap_types.yaml + the data_gaps sub-entries). \
Different names for the same concept (e.g. `data_source_missing` vs \
`missing_source_system`) are a sign the two vocabularies have drifted. \
Propose reconciliation — prefer treating `gap_types.yaml` as canonical \
and aligning `blocker_class` values to match, unless a cleaner rename \
of gap_types makes more sense.

### F.2 — Archetypes used

List every distinct `archetype` value used across the initiatives in \
Part A. For each, state:
  - the value
  - the initiative_id(s) that use it
  - whether a delivery profile exists in `ontology/delivery_heuristics.yaml`
  - if not, PROPOSE a delivery profile in YAML-ready form:

```yaml
<archetype_name>:
  refresh: "<e.g. Daily / Weekly / On-demand / Real-time>"
  sla:     "<e.g. T+1 from mart refresh / Real-time / Event-triggered>"
  format:  "<e.g. BI dashboard / API / Ranked list / Agentic workflow>"
```

Base the proposed profile on the archetype's semantics — e.g. \
`anomaly_detection` is typically monitoring-style (daily refresh, \
dashboard format); `analytics_product` is typically weekly batch. \
Avoid duplicating an existing archetype's profile under a new name — \
if two archetypes would have identical delivery profiles, they should \
probably be the same archetype.

### F.3 — Archetype consolidation check

If your Part A uses more than ~10 distinct archetypes, flag it — the \
archetype list should stay small (7-9 values). Propose collapsing \
archetypes that differ only in name, not in delivery shape.

## Part G — Open questions for the curator

Things requiring human judgment:
  - ambiguous categorisation
  - aspirational items whose value is strong but whose feasibility
    depends on decisions outside the data team (business strategy,
    regulatory, etc.)
  - proposed new domain areas (if any)
  - bibliography additions the curator should validate
  - gap_type / archetype vocabulary additions from Part F that the
    curator may want to rename or merge before accepting

---

Be honest about uncertainty. Prefer specific, citable initiatives over \
vague capability claims. An aspirational initiative without a source or \
blocker_class is worse than no initiative — it's noise. Your job is to \
surface real gaps with reasons attached, not to pad the catalogue."""


def _load_reference(name: str) -> str:
    path = _REFERENCE_DIR / f"{name}.md"
    if not path.exists():
        console.print(f"[red]Reference framework not found: {path}[/red]")
        available = [p.stem for p in _REFERENCE_DIR.glob("*.md") if p.stem != "README"]
        console.print(f"[yellow]Available:[/yellow] " + ", ".join(available))
        raise typer.Exit(1)
    return path.read_text(encoding="utf-8")


def _safe_read(path: Path, label: str) -> str:
    if not path.exists():
        return f"(missing — {label})"
    return path.read_text(encoding="utf-8")


def _warehouse_signal(bundle_path: Path, max_assets: int = 250,
                     top_columns: int = 100) -> dict:
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
    initiatives_yaml: str,
    primitives_yaml: str,
    domain_keywords_yaml: str,
    gap_types_yaml: str,
    delivery_heuristics_yaml: str,
    web_research_enabled: bool,
) -> str:
    parts = [
        "# Reference framework",
        "",
        reference,
        "",
        "---",
        "",
        "# Current initiative catalogue — initiative_research.yaml",
        "",
        "```yaml",
        initiatives_yaml,
        "```",
        "",
        "# Current primitive catalogue (READ-ONLY) — primitives.yaml",
        "",
        "```yaml",
        primitives_yaml,
        "```",
        "",
        "# Current domain taxonomy (READ-ONLY) — domain_keywords.yaml",
        "",
        "```yaml",
        domain_keywords_yaml,
        "```",
        "",
        "# Current gap_types vocabulary (READ-ONLY) — gap_types.yaml",
        "",
        "```yaml",
        gap_types_yaml,
        "```",
        "",
        "# Current delivery heuristics (READ-ONLY) — delivery_heuristics.yaml",
        "",
        "```yaml",
        delivery_heuristics_yaml,
        "```",
        "",
        "---",
        "",
        "# Warehouse signal",
        "",
        signal_str,
    ]
    if web_research_enabled:
        parts += [
            "",
            "---",
            "",
            "# Web research enabled",
            "",
            "You have access to a web_search tool. Use it judiciously to:",
            "  - Verify recent carrier case-studies or AI deployment figures.",
            "  - Find additional primary sources for aspirational initiatives",
            "    whose value case needs strengthening.",
            "  - Check that your recommendations reflect 2025-2026 state of art,",
            "    not pre-2024 practice.",
            "Cite every web-sourced claim inline with URL. Web citations are",
            "additive to the bibliography pool, not a replacement for it.",
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


def _call_anthropic(
    model: str,
    system: str,
    user: str,
    max_tokens: int,
    web_research: bool,
) -> tuple[str, str]:
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
    kwargs = dict(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    if web_research:
        kwargs["tools"] = [{
            "type": "web_search_20250305",
            "name": "web_search",
            "max_uses": _WEB_SEARCH_MAX_USES,
        }]
        console.print(f"[cyan]Calling {model} with web_search tool "
                      f"(max_uses={_WEB_SEARCH_MAX_USES})[/cyan] — this may "
                      "take 2-5 minutes…")
    else:
        console.print(f"[cyan]Calling {model}[/cyan] — this may take 60–180 seconds…")

    message = client.messages.create(**kwargs)

    # Concatenate all text blocks; web_search tool responses interleave with text.
    text_parts = [block.text for block in message.content if getattr(block, "type", None) == "text"]
    text = "\n".join(text_parts)
    return text, getattr(message, "stop_reason", "") or ""


@app.command()
def main(
    bundle: Path = typer.Option(
        Path("output/bundle.json"), "--bundle",
        help="Path to bundle.json from Phase 1",
    ),
    reference: str = typer.Option(
        _DEFAULT_REFERENCE, "--reference",
        help="Reference framework filename (without .md) under ontology/reference_frameworks/",
    ),
    web_research: bool = typer.Option(
        False, "--web-research",
        help="Enable Claude's server-side web_search tool for fresher citations",
    ),
    model: str = typer.Option(
        _DEFAULT_MODEL, "--model",
        help="Anthropic model id",
    ),
    max_tokens: int = typer.Option(
        _MAX_TOKENS, "--max-tokens",
        help="Override output token cap (default 20000). Bump if briefs truncate.",
    ),
    output: Optional[Path] = typer.Option(
        None, "--output",
        help="Override output path (default: ontology/research_log/initiatives/vN_<date>_<hash>.md)",
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
    primitives = _safe_read(_PRIMITIVES_YAML, "primitives.yaml")
    domain_keywords = _safe_read(_DOMAIN_KEYWORDS_YAML, "domain_keywords.yaml")
    gap_types = _safe_read(_GAP_TYPES_YAML, "gap_types.yaml")
    delivery_heuristics = _safe_read(_DELIVERY_HEURISTICS_YAML, "delivery_heuristics.yaml")

    user_msg = _build_user_message(
        reference_text, signal_str,
        initiatives, primitives, domain_keywords,
        gap_types, delivery_heuristics,
        web_research,
    )

    if dry_run:
        console.print("[bold]System prompt (excerpt):[/bold]")
        console.print(_SYSTEM_PROMPT[:1500] + "…")
        console.print(f"\n[cyan]Prompt size:[/cyan] ~{len(user_msg)} chars "
                      f"(≈ {len(user_msg) // 4} tokens)")
        console.print(f"[cyan]Web research:[/cyan] {'ENABLED' if web_research else 'disabled'}")
        return

    response, stop_reason = _call_anthropic(
        model, _SYSTEM_PROMPT, user_msg, max_tokens, web_research,
    )

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
        f"model: {model}",
        f"web_research: {'enabled' if web_research else 'disabled'}",
        f"generated_utc: {datetime.now(timezone.utc).isoformat()}",
        f"asset_fingerprint: {stable_hash(json.dumps(signal['asset_names'], sort_keys=True))[:8]}",
        f"max_tokens: {max_tokens}",
        f"stop_reason: {stop_reason}",
        "---",
        "",
        "# Initiative research",
        "",
        "> LLM-generated research brief proposing a gap-aware initiative "
        "catalogue. Initiatives sit at the top of the research chain — "
        "primitives, entities, and metric patterns follow from what the "
        "initiatives require. Curate this brief BEFORE running "
        "`research_semantic_model.py`, which reads the curated "
        "`initiative_research.yaml` as a frozen input.",
        "",
    ]

    out_path = output or _default_output_path(signal)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(header) + response + "\n", encoding="utf-8")
    console.print(f"[green]Research brief written to:[/green] {out_path}")
    console.print(
        "[yellow]Next steps:[/yellow]\n"
        "  1. Review the brief.\n"
        "  2. Hand-edit ontology/initiative_research.yaml (add aspirational "
        "initiatives, update status fields, merge new bibliography).\n"
        "  3. Apply Part F ontology-vocabulary contributions:\n"
        "       - add new gap_types to ontology/gap_types.yaml\n"
        "       - add new archetype delivery profiles to "
        "ontology/delivery_heuristics.yaml\n"
        "  4. Run scripts/research_semantic_model.py — it reads the "
        "curated initiative YAML as a frozen input and proposes primitive / "
        "entity / metric_pattern updates grounded in the initiative map.\n"
        "  5. Commit initiative changes + vocabulary updates as separate "
        "commits before curating the semantic-model layers."
    )


if __name__ == "__main__":
    app()
