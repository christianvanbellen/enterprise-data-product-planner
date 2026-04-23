#!/usr/bin/env python
"""One-shot, human-triggered entity + primitive research via an LLM call.

Sibling to scripts/research_domain_taxonomy.py. Produces a two-section brief
that covers both the entity layer (business nouns the semantic graph should
recognise) and the primitive layer (analytical capabilities those entities
unlock). Entity and primitive decisions are tightly coupled — an entity with
no primitive is orphaned, a primitive requiring an absent entity is a gap —
so the research is done in a single pass with cross-layer coherence analysis.

Runs OUT OF the Phase 1-5 pipeline. Writes a versioned markdown brief to
ontology/research_log/entity_model/ that the curator then reads and uses to
edit entity_bindings.yaml, insurance_entities.yaml, and primitives.yaml
manually. Nothing about this script mutates configuration directly.

Inputs:
  --bundle        CanonicalBundle from Phase 1 (warehouse signal).
  --reference     Filename (without .md) under ontology/reference_frameworks/.
  --audit-report  Optional path to a markdown audit (e.g. from
                  scripts/entity_audit.py --output ...) to include as
                  empirical grounding.
  --model         Anthropic model; defaults to Opus for deep research.
  --max-tokens    Output token cap (default 16000).
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

app = typer.Typer(help="Run LLM-assisted entity + primitive research.")
console = Console()

_REPO_ROOT = Path(__file__).resolve().parent.parent
_REFERENCE_DIR = _REPO_ROOT / "ontology" / "reference_frameworks"
_LOG_DIR = _REPO_ROOT / "ontology" / "research_log" / "entity_model"
_ENTITY_BINDINGS_YAML = _REPO_ROOT / "ontology" / "entity_bindings.yaml"
_INSURANCE_ENTITIES_YAML = _REPO_ROOT / "ontology" / "insurance_entities.yaml"
_ENTITY_GROUPS_YAML = _REPO_ROOT / "ontology" / "entity_groups.yaml"
_PRIMITIVES_YAML = _REPO_ROOT / "ontology" / "primitives.yaml"

_DEFAULT_MODEL = "claude-opus-4-7"
_MAX_TOKENS = 16000

_SYSTEM_PROMPT = """You are an insurance data semantic-model architect running \
a combined entity and primitive design review. You have been given:

  1. A reference framework (authoritative external taxonomy).
  2. The current warehouse signal (asset names, tags, sample columns).
  3. The current entity config (insurance_entities.yaml, entity_bindings.yaml,
     entity_groups.yaml) — what the pipeline recognises today.
  4. The current primitive catalogue (primitives.yaml) — what analytical
     capabilities are defined, and which entities each requires.
  5. Optionally, an entity audit report showing unbound rate, dead signatures,
     and per-signal attribution against the current warehouse.

Your task is to propose a revised entity taxonomy AND the corresponding \
primitive-catalogue updates in a single coherent brief. Entity and primitive \
decisions are tightly coupled:
  - A primitive's required_entities MUST use labels in the entity whitelist.
  - An entity with no primitive that requires it is orphaned (worth flagging).
  - A primitive requiring an entity the warehouse has no signal for is a gap.
This output will be reviewed by a human curator who will manually edit YAML \
based on your recommendations. Be concrete; prefer specific column names \
and primitive IDs over hand-waving.

Output a single markdown document with the following sections, in order:

## Part A — Entity model

### Proposed entity taxonomy
One row per entity, linked back to the reference framework section that \
justifies it. Flag departures from the reference. Prefer business nouns \
that appear as asset-shaped concepts in the warehouse; avoid classifications \
(e.g. product line, jurisdiction) that belong on tag dimensions, not on \
BusinessEntityNode.

### Entity definitions
For each proposed entity: scope, boundary with adjacent entities, typical \
warehouse artefacts (asset name patterns, column families) that belong.

### Entity signatures (YAML-ready)
For each entity, propose the column signature (normalised column names) \
for Signal 2 matching. Every column listed MUST be a real column that \
appears in the warehouse signal you were given — no aspirational columns. \
Flag columns you are uncertain about.

### Conformed-schema proposals
Bullets: new conformed-schema entity groups the discovery layer is \
identifying as load-bearing enough to deserve governance. For each, list \
the group name and candidate child fields. These are ingestion-layer \
proposals the curator can take to the data team; they do NOT go into \
entity_bindings.yaml directly.

## Part B — Primitive catalogue

### Proposed primitives
For each primitive (new or modified): id, required_entities, \
required_columns (or required_tag_dimension + required_tags), \
description. Explicitly tie required_entities to the entity labels \
proposed in Part A.

### Diff vs current primitives.yaml
Bullets per primitive: added, modified (with what changed), removed \
(with why). Each row includes a one-line rationale.

## Part C — Cross-layer coherence

### Orphan analysis
- Entities with no primitive requiring them (orphan entities).
- Primitives requiring entities the warehouse has no signal for \
  (orphan primitives — these are data gaps).
- Primitives whose current required_entities use labels the proposed \
  taxonomy renames or removes.

### Coverage projection
Estimate:
  - Unbound rate if the proposed entity taxonomy is adopted.
  - Number of primitives that would move from 'confirmed' to 'inferred' \
    or vice versa given proposed entity changes.
  - Net direction of initiative-readiness movement (qualitative; the \
    curator will measure exactly after re-running Phase 4).

## Part D — Open questions
Things the human reviewer must decide that you cannot: ambiguous entity \
boundaries, missing reference content, warehouse artefacts you couldn't \
classify, primitives whose required_entities could plausibly be several \
alternative labels.

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
    entity_bindings_yaml: str,
    insurance_entities_yaml: str,
    entity_groups_yaml: str,
    primitives_yaml: str,
    audit_report: Optional[str],
) -> str:
    parts = [
        "# Reference framework",
        "",
        reference,
        "",
        "---",
        "",
        "# Current entity config — insurance_entities.yaml",
        "",
        "```yaml",
        insurance_entities_yaml,
        "```",
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
        help="Override output path (default: ontology/research_log/entity_model/vN_<date>_<hash>.md)",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Print the assembled prompt without calling the API",
    ),
) -> None:
    reference_text = _load_reference(reference)
    signal = _warehouse_signal(bundle)
    signal_str = _format_signal(signal)
    entity_bindings = _safe_read(_ENTITY_BINDINGS_YAML, "entity_bindings.yaml")
    insurance_entities = _safe_read(_INSURANCE_ENTITIES_YAML, "insurance_entities.yaml")
    entity_groups = _safe_read(_ENTITY_GROUPS_YAML, "entity_groups.yaml")
    primitives = _safe_read(_PRIMITIVES_YAML, "primitives.yaml")
    audit_text = audit_report.read_text(encoding="utf-8") if audit_report else None

    user_msg = _build_user_message(
        reference_text, signal_str,
        entity_bindings, insurance_entities, entity_groups,
        primitives, audit_text,
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
        "# Entity + primitive research",
        "",
        "> LLM-generated research brief covering both the entity layer and the "
        "primitive catalogue. Curate entities first, primitives second — see "
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
        "  3. Curate primitive-layer YAML edits against frozen entities, "
        "regenerate Phase 4.\n"
        "  4. Commit entity and primitive changes as separate commits."
    )


if __name__ == "__main__":
    app()
