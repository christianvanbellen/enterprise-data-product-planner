#!/usr/bin/env python
"""Deterministic audit of the current domain taxonomy against a bundle.

Companion to the LLM-assisted `research_domain_taxonomy.py`. This script never
calls an LLM, never mutates configuration — it just reports how well the current
keyword corpus is fitting the warehouse. Run it periodically (or in CI) to
decide *when* to commission a research refresh.

Sections:
  - Coverage summary (assets with zero / one / many domains, tied-primary count)
  - Per-domain counts (candidacy, primary, mean confidence)
  - Keyword utilisation (dead keywords and over-broad keywords)
  - Top frequent tokens in unassigned-asset names/columns (missing-keyword signal)
  - List of unassigned and tied assets for eyeballing

Writes to stdout by default; `--output` writes a markdown copy as well.
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import typer
from rich.console import Console
from rich.table import Table

from ingestion.adapters.dbt_metadata import DOMAIN_KEYWORDS
from graph.semantic.domain_assigner import _confidence_from_score

app = typer.Typer(help="Audit the current domain taxonomy against a CanonicalBundle.")
console = Console()

# Tokens to exclude from the "missing keyword signal" report. Common stop-words
# plus short fragments that carry no domain signal.
_STOPWORDS: Set[str] = {
    "hx", "ll", "id", "cd", "ky", "dt", "ts", "num", "qty", "amt", "pct", "nm",
    "src", "tgt", "fk", "pk", "the", "and", "for", "with", "from", "into", "tbl",
    "view", "tmp", "stg", "agg", "ref", "val", "flag", "type", "code", "name",
    "key", "our", "share", "usd", "eur", "gbp", "base", "local", "src", "dim",
    "fact", "raw", "v1", "v2", "bkp", "arch", "fx", "x", "y", "z",
}
_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9]*")
_MIN_TOKEN_LEN = 4


def _tokenise(text: str) -> List[str]:
    """Lower-case word tokens ≥ 4 chars, with stop-words removed."""
    return [
        tok.lower()
        for tok in _TOKEN_RE.findall(text)
        if len(tok) >= _MIN_TOKEN_LEN and tok.lower() not in _STOPWORDS
    ]


def _load_bundle(path: Path) -> dict:
    if not path.exists():
        console.print(f"[red]Bundle not found: {path}[/red]")
        raise typer.Exit(1)
    return json.loads(path.read_text(encoding="utf-8"))


def _coverage_summary(assets: List[dict]) -> Dict[str, int]:
    total = len(assets)
    zero = sum(1 for a in assets if not a.get("domain_candidates"))
    single = sum(1 for a in assets if len(a.get("domain_candidates") or []) == 1)
    multi = sum(1 for a in assets if len(a.get("domain_candidates") or []) >= 2)
    tied = 0
    for a in assets:
        s = a.get("domain_scores") or {}
        if not s:
            continue
        top = max(s.values())
        if sum(1 for v in s.values() if v == top) > 1:
            tied += 1
    return {
        "total": total,
        "unassigned": zero,
        "single_domain": single,
        "multi_domain": multi,
        "tied_primary": tied,
    }


def _per_domain_stats(assets: List[dict]) -> List[Tuple[str, int, int, float]]:
    """Return [(domain, candidacy_count, primary_count, mean_confidence), ...]."""
    candidacy: Counter = Counter()
    primary: Counter = Counter()
    conf_totals: Dict[str, List[float]] = defaultdict(list)

    for a in assets:
        cands = a.get("domain_candidates") or []
        scores = a.get("domain_scores") or {}
        if not cands:
            continue
        primary[cands[0]] += 1
        for d in cands:
            candidacy[d] += 1
            if d in scores:
                conf_totals[d].append(_confidence_from_score(scores[d]))

    rows: List[Tuple[str, int, int, float]] = []
    for d in DOMAIN_KEYWORDS:
        confs = conf_totals.get(d, [])
        mean_conf = sum(confs) / len(confs) if confs else 0.0
        rows.append((d, candidacy.get(d, 0), primary.get(d, 0), mean_conf))
    return rows


def _keyword_utilisation(assets: List[dict]) -> Dict[str, Dict[str, int]]:
    """For each (domain, keyword), count how many assets' corpus contains the keyword.

    Scans the same corpus _infer_domains scans: name + description + tags + column names.
    Separate from the scoring logic so that "dead keywords" (defined but never match)
    are still visible even if their domain has other keywords that carry it.
    """
    counts: Dict[str, Dict[str, int]] = {
        d: {kw: 0 for kw in kws} for d, kws in DOMAIN_KEYWORDS.items()
    }

    for a in assets:
        cols_text = " ".join(
            # columns aren't in the asset JSON — reconstruct from candidates cache
            # Actually columns are stored separately; fall back to searching
            # asset name + description + tags since those dominate name-weighted signal
            [str(a.get("name", "")),
             str(a.get("description") or ""),
             " ".join(a.get("tags") or [])]
        ).lower()
        for d, kws in DOMAIN_KEYWORDS.items():
            for kw in kws:
                if kw in cols_text:
                    counts[d][kw] += 1
    return counts


def _unassigned_token_frequency(
    assets: List[dict], bundle: dict, top_n: int = 25
) -> List[Tuple[str, int]]:
    """Tokens most common in unassigned asset names + their columns.

    Intended as a signal of missing keywords — if a frequent token appears in
    many unassigned assets, it's a candidate for a new domain keyword.
    """
    unassigned_ids: Set[str] = {
        a["internal_id"] for a in assets if not a.get("domain_candidates")
    }
    if not unassigned_ids:
        return []

    token_counter: Counter = Counter()

    # Asset names + tags + descriptions for unassigned assets
    for a in assets:
        if a["internal_id"] not in unassigned_ids:
            continue
        token_counter.update(_tokenise(str(a.get("name", ""))))
        token_counter.update(_tokenise(str(a.get("description") or "")))
        for tag in a.get("tags") or []:
            token_counter.update(_tokenise(str(tag)))

    # Column names on unassigned assets
    for col in bundle.get("columns", []):
        if col.get("asset_internal_id") in unassigned_ids:
            token_counter.update(_tokenise(str(col.get("name", ""))))

    return token_counter.most_common(top_n)


def _unassigned_asset_list(assets: List[dict], limit: int = 20) -> List[str]:
    return sorted(
        a["name"] for a in assets if not a.get("domain_candidates")
    )[:limit]


def _tied_asset_list(assets: List[dict], limit: int = 20) -> List[Tuple[str, List[str], float]]:
    out: List[Tuple[str, List[str], float]] = []
    for a in assets:
        s = a.get("domain_scores") or {}
        if not s:
            continue
        top = max(s.values())
        tied = sorted(d for d, v in s.items() if v == top)
        if len(tied) >= 2:
            out.append((a["name"], tied, top))
    out.sort(key=lambda x: (-x[2], x[0]))
    return out[:limit]


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _print_report(bundle: dict, markdown_lines: List[str]) -> None:
    assets = bundle["assets"]

    # --- Coverage summary ---
    cov = _coverage_summary(assets)
    table = Table(title="Coverage summary", show_header=False)
    table.add_column("Metric", style="bold cyan")
    table.add_column("Value", style="green")
    for k, v in cov.items():
        pct = (v / cov["total"]) * 100 if cov["total"] else 0.0
        table.add_row(k.replace("_", " "), f"{v}  ({pct:.0f}%)" if k != "total" else str(v))
    console.print(table)

    markdown_lines += [
        "## Coverage summary",
        "",
        "| Metric | Value |",
        "|--------|-------|",
    ]
    for k, v in cov.items():
        pct = (v / cov["total"]) * 100 if cov["total"] else 0.0
        val = f"{v} ({pct:.0f}%)" if k != "total" else str(v)
        markdown_lines.append(f"| {k.replace('_', ' ')} | {val} |")
    markdown_lines.append("")

    # --- Per-domain stats ---
    rows = _per_domain_stats(assets)
    table = Table(title="Per-domain counts")
    table.add_column("domain", style="bold cyan")
    table.add_column("candidacy", justify="right")
    table.add_column("primary", justify="right")
    table.add_column("mean confidence", justify="right")
    for d, cand, prim, mconf in rows:
        table.add_row(d, str(cand), str(prim), f"{mconf:.2f}")
    console.print(table)

    markdown_lines += [
        "## Per-domain counts",
        "",
        "| Domain | Candidacy | Primary | Mean confidence |",
        "|--------|-----------|---------|-----------------|",
    ]
    for d, cand, prim, mconf in rows:
        markdown_lines.append(f"| {d} | {cand} | {prim} | {mconf:.2f} |")
    markdown_lines.append("")

    # --- Keyword utilisation ---
    util = _keyword_utilisation(assets)
    markdown_lines += [
        "## Keyword utilisation",
        "",
        "Asset count (of assets where the keyword appears in name/tags/description — "
        "column hits not counted here, since column corpus is broader and noisier). "
        "Zero-count keywords are candidates for removal or reformulation; disproportionately "
        "high counts flag over-broad keywords.",
        "",
    ]
    console.print("\n[bold]Keyword utilisation[/bold] (zero = dead keyword)")
    for d, kw_counts in util.items():
        console.print(f"  [cyan]{d}[/cyan]")
        for kw in sorted(kw_counts, key=lambda k: (-kw_counts[k], k)):
            flag = " [red](dead)[/red]" if kw_counts[kw] == 0 else ""
            console.print(f"    {kw:<30s} {kw_counts[kw]:>4d}{flag}")
        markdown_lines.append(f"### {d}")
        markdown_lines += [
            "",
            "| Keyword | Asset count |",
            "|---------|-------------|",
        ]
        for kw in sorted(kw_counts, key=lambda k: (-kw_counts[k], k)):
            flag = " _(dead)_" if kw_counts[kw] == 0 else ""
            markdown_lines.append(f"| `{kw}` | {kw_counts[kw]}{flag} |")
        markdown_lines.append("")

    # --- Token frequency in unassigned ---
    freq = _unassigned_token_frequency(assets, bundle)
    if freq:
        console.print("\n[bold]Top tokens in unassigned assets[/bold] "
                      "(name + columns; stopwords filtered)")
        for tok, n in freq[:15]:
            console.print(f"  {tok:<30s} {n:>4d}")
        markdown_lines += [
            "## Top tokens in unassigned assets",
            "",
            "Frequent tokens across names + columns of assets that matched no domain. "
            "Stopwords and very short tokens filtered. Tokens appearing frequently here "
            "are candidates for new domain keywords.",
            "",
            "| Token | Count |",
            "|-------|-------|",
        ]
        for tok, n in freq:
            markdown_lines.append(f"| `{tok}` | {n} |")
        markdown_lines.append("")

    # --- Unassigned assets ---
    unassigned = _unassigned_asset_list(assets, limit=50)
    if unassigned:
        console.print(f"\n[bold]Unassigned assets (first {len(unassigned)}):[/bold]")
        for name in unassigned[:15]:
            console.print(f"  {name}")
        markdown_lines += [
            "## Unassigned assets",
            "",
            f"Assets with zero domain matches (showing up to 50):",
            "",
        ]
        for name in unassigned:
            markdown_lines.append(f"- `{name}`")
        markdown_lines.append("")

    # --- Tied assets ---
    tied = _tied_asset_list(assets, limit=30)
    if tied:
        console.print(f"\n[bold]Tied-primary assets (top {len(tied)} by score):[/bold]")
        for name, tied_doms, top in tied[:10]:
            console.print(f"  {name:<55s} {tied_doms}  @ {top}")
        markdown_lines += [
            "## Tied-primary assets",
            "",
            "Assets whose top score is shared by 2+ domains. Primary is resolved by "
            "(hit-count desc, alphabetical). Many ties at score 3.0 suggests the "
            "keyword set doesn't discriminate at the name level for these assets.",
            "",
            "| Asset | Tied domains | Top score |",
            "|-------|--------------|-----------|",
        ]
        for name, tied_doms, top in tied:
            markdown_lines.append(
                f"| `{name}` | {', '.join(tied_doms)} | {top} |"
            )
        markdown_lines.append("")


@app.command()
def main(
    bundle: Path = typer.Option(
        Path("output/bundle.json"), "--bundle",
        help="Path to bundle.json from Phase 1",
    ),
    output: Path = typer.Option(
        None, "--output",
        help="Optional path to write a markdown copy of the report",
    ),
) -> None:
    bundle_data = _load_bundle(bundle)
    md_lines: List[str] = [
        "# Domain taxonomy audit",
        "",
        f"Bundle: `{bundle}`  ·  Assets: {len(bundle_data['assets'])}",
        "",
    ]
    _print_report(bundle_data, md_lines)

    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text("\n".join(md_lines), encoding="utf-8")
        console.print(f"\n[green]Markdown report written to:[/green] {output}")


if __name__ == "__main__":
    app()
