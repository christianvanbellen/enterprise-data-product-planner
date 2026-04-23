#!/usr/bin/env python
"""One-shot, human-triggered domain taxonomy research via an LLM call.

Runs OUT OF the Phase 1–5 pipeline — never invoked automatically. Writes a
versioned markdown brief to `ontology/research_log/domain_taxonomy/` that
the human then reads and uses to curate `ontology/domain_keywords.yaml` by
hand. Nothing about this script mutates configuration directly.

Inputs:
  --bundle      CanonicalBundle from Phase 1 (warehouse signal).
  --reference   Filename (without .md) under ontology/reference_frameworks/.
  --model       Anthropic model; defaults to Opus for deep research.
  --output      Optional override; defaults to the versioned log path.
  --dry-run     Print the prompt without calling the API.

See docs/domain_taxonomy_workflow.md for the full workflow.
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

# Ensure non-ASCII characters (em-dashes, arrows, etc.) in prompts and output
# render correctly on Windows cp1252 terminals.
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

app = typer.Typer(help="Run LLM-assisted domain taxonomy research.")
console = Console()

_REPO_ROOT = Path(__file__).resolve().parent.parent
_REFERENCE_DIR = _REPO_ROOT / "ontology" / "reference_frameworks"
_LOG_DIR = _REPO_ROOT / "ontology" / "research_log" / "domain_taxonomy"
_CURRENT_YAML = _REPO_ROOT / "ontology" / "domain_keywords.yaml"

_DEFAULT_MODEL = "claude-opus-4-7"
# Research briefs run long. Opus supports up to 32k output tokens; 16k is a safe
# default that leaves headroom for a full multi-section brief without routinely
# truncating the last section. Bump higher via --max-tokens if needed.
_MAX_TOKENS = 16000

_SYSTEM_PROMPT = """You are an insurance data domain architect running a \
taxonomy design review. You have been given:

  1. A reference framework (authoritative external taxonomy).
  2. The current warehouse signal (asset names, tags, sample columns).
  3. The current working taxonomy (keyword -> domain mappings).

Your task is to propose a revised domain taxonomy grounded in the reference,
validated against the warehouse signal, and with a concrete keyword corpus
per domain. This output will be reviewed by a human curator who will \
manually edit YAML based on your recommendations.

Output a single markdown document with the following sections, in order:

## Proposed taxonomy
One line per domain, linked back to the reference framework section that \
justifies it. Flag any domain that departs from the reference.

## Domain definitions
For each proposed domain: scope, boundary with adjacent domains, typical \
warehouse artefacts (asset name patterns, column families) that belong.

## Keyword corpus
For each domain, a markdown table: keyword, rationale, expected precision \
(high / medium / low), notes on overlap with other domains.

## Coverage analysis
How the proposed taxonomy would map the warehouse signal you were given. \
Estimate unassigned count, flag assets that would fit multiple domains, and \
identify systematic gaps.

## Diff vs current taxonomy
Bullets: added domains, removed domains, renamed domains, keywords added, \
keywords dropped, with a one-line rationale per change.

## Open questions for the curator
Things the human reviewer should decide that you cannot: ambiguous boundaries, \
missing reference content, warehouse artefacts you couldn't classify.

Be honest about uncertainty. If the reference framework is thin (e.g. the \
file is a placeholder), say so prominently and caveat the recommendations."""


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


def _load_current_yaml() -> str:
    if not _CURRENT_YAML.exists():
        return "(none — ontology/domain_keywords.yaml missing)"
    return _CURRENT_YAML.read_text(encoding="utf-8")


def _warehouse_signal(bundle_path: Path, max_assets: int = 250, top_columns: int = 100) -> dict:
    """Condense the bundle into a compact signal for prompt grounding.

    We deliberately cap sizes. The LLM doesn't need every column of every asset
    — it needs enough vocabulary to calibrate keyword proposals against.
    """
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

    # Samples of descriptions (the LLM benefits from reading a few actual ones)
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


def _build_user_message(reference: str, signal_str: str, current_yaml: str) -> str:
    return f"""# Reference framework

{reference}

---

# Current working taxonomy (ontology/domain_keywords.yaml)

```yaml
{current_yaml}
```

---

# Warehouse signal

{signal_str}
"""


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
    """Returns (text, stop_reason). stop_reason='max_tokens' means the output was truncated."""
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
    console.print(f"[cyan]Calling {model}[/cyan] — this may take 30–90 seconds…")
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
        "lloyds_mdc", "--reference",
        help="Reference framework filename (without .md) under ontology/reference_frameworks/",
    ),
    model: str = typer.Option(
        _DEFAULT_MODEL, "--model",
        help="Anthropic model id",
    ),
    output: Optional[Path] = typer.Option(
        None, "--output",
        help="Override output path (default: ontology/research_log/domain_taxonomy/vN_<date>_<hash>.md)",
    ),
    max_tokens: int = typer.Option(
        _MAX_TOKENS, "--max-tokens",
        help="Override output token cap (default 16000). Bump if briefs truncate.",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Print the assembled prompt without calling the API",
    ),
) -> None:
    reference_text = _load_reference(reference)
    signal = _warehouse_signal(bundle)
    signal_str = _format_signal(signal)
    current_yaml = _load_current_yaml()
    user_msg = _build_user_message(reference_text, signal_str, current_yaml)

    if dry_run:
        console.print("[bold]System prompt:[/bold]")
        console.print(_SYSTEM_PROMPT)
        console.print("\n[bold]User message:[/bold]")
        console.print(user_msg[:3000] + "\n...[truncated]" if len(user_msg) > 3000 else user_msg)
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
        f"model: {model}",
        f"generated_utc: {datetime.now(timezone.utc).isoformat()}",
        f"asset_fingerprint: {stable_hash(json.dumps(signal['asset_names'], sort_keys=True))[:8]}",
        f"max_tokens: {max_tokens}",
        f"stop_reason: {stop_reason}",
        "---",
        "",
        "# Domain taxonomy research",
        "",
        f"> LLM-generated research brief. Read critically, curate manually into "
        f"`ontology/domain_keywords.yaml`. See `docs/domain_taxonomy_workflow.md`.",
        "",
    ]

    out_path = output or _default_output_path(signal)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(header) + response + "\n", encoding="utf-8")
    console.print(f"[green]Research brief written to:[/green] {out_path}")
    console.print("[yellow]Next step:[/yellow] read the brief, then edit "
                  "`ontology/domain_keywords.yaml` by hand.")


if __name__ == "__main__":
    app()
