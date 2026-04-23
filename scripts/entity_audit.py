#!/usr/bin/env python
"""Deterministic audit of the current entity taxonomy against a bundle.

Companion to scripts/taxonomy_audit.py — same shape, but for the four-signal
EntityMapper logic configured in ontology/entity_bindings.yaml. Never calls
an LLM, never mutates configuration.

Sections:
  - Coverage summary (assets with zero / one / many entity bindings, tied-primary)
  - Per-entity counts (candidacy, primary, mean confidence)
  - Per-signal attribution (how many candidates each signal contributed to)
  - Signature coverage (dead signature columns — defined but never match any asset)
  - Top column tokens in unbound assets (signal for missing signatures)
  - Samples of unbound and tied-primary assets for eyeballing

Writes to stdout by default; `--output` writes a markdown copy.
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

from ingestion.contracts.bundle import CanonicalBundle
from graph.semantic.conformed_binder import ConformedFieldBinder
from graph.semantic.entity_mapper import (
    ASSET_NAME_PATTERNS,
    CONFORMED_GROUP_TO_ENTITY,
    EntityMapper,
    MIN_CONFIDENCE,
)
from graph.semantic.ontology_loader import SynonymRegistry

app = typer.Typer(help="Audit the current entity taxonomy against a CanonicalBundle.")
console = Console()

_STOPWORDS: Set[str] = {
    "hx", "ll", "id", "cd", "ky", "dt", "ts", "num", "qty", "amt", "pct", "nm",
    "src", "tgt", "fk", "pk", "the", "and", "for", "with", "from", "into", "tbl",
    "view", "tmp", "stg", "agg", "ref", "val", "flag", "type", "code", "name",
    "key", "our", "share", "usd", "eur", "gbp", "base", "local", "dim",
    "fact", "raw", "v1", "v2", "bkp", "arch", "fx", "x", "y", "z",
}
_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9]*")
_MIN_TOKEN_LEN = 4


def _tokenise(text: str) -> List[str]:
    return [
        tok.lower()
        for tok in _TOKEN_RE.findall(text)
        if len(tok) >= _MIN_TOKEN_LEN and tok.lower() not in _STOPWORDS
    ]


def _load_bundle(path: Path) -> CanonicalBundle:
    if not path.exists():
        console.print(f"[red]Bundle not found: {path}[/red]")
        raise typer.Exit(1)
    return CanonicalBundle.from_json(path)


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def _coverage_summary(asset_to_candidates: Dict[str, List]) -> Dict[str, int]:
    total = len(asset_to_candidates)
    zero = sum(1 for cands in asset_to_candidates.values() if not cands)
    single = sum(1 for cands in asset_to_candidates.values() if len(cands) == 1)
    multi = sum(1 for cands in asset_to_candidates.values() if len(cands) >= 2)

    tied = 0
    for cands in asset_to_candidates.values():
        if not cands:
            continue
        top = max(c.confidence for c in cands)
        if sum(1 for c in cands if c.confidence == top) > 1:
            tied += 1

    return {
        "total": total,
        "unbound": zero,
        "single_entity": single,
        "multi_entity": multi,
        "tied_primary": tied,
    }


def _per_entity_stats(asset_to_candidates: Dict[str, List]) -> Dict[str, Dict]:
    """{entity: {candidacy, primary, mean_confidence}}."""
    candidacy: Counter = Counter()
    primary: Counter = Counter()
    conf_sums: Dict[str, float] = defaultdict(float)

    for cands in asset_to_candidates.values():
        if not cands:
            continue
        ranked = sorted(cands, key=lambda c: -c.confidence)
        primary[ranked[0].entity_label] += 1
        for c in cands:
            candidacy[c.entity_label] += 1
            conf_sums[c.entity_label] += c.confidence

    allowed = SynonymRegistry.allowed_entities()
    rows = {}
    for entity in allowed:
        n = candidacy.get(entity, 0)
        rows[entity] = {
            "candidacy": n,
            "primary": primary.get(entity, 0),
            "mean_confidence": (conf_sums[entity] / n) if n else 0.0,
        }
    return rows


def _per_signal_attribution(candidates_list: List) -> Dict[str, Dict]:
    """{signal_source: {count, mean_confidence}}.

    Counts candidates whose signal_sources contains each source. A candidate
    merged from multiple signals is counted under each of them.
    """
    count: Counter = Counter()
    conf_sum: Dict[str, float] = defaultdict(float)

    for c in candidates_list:
        for source in c.signal_sources:
            # Normalise tag_<dim> → tag_binding for grouping
            normalized = source if not source.startswith("tag_") else "tag_binding"
            count[normalized] += 1
            conf_sum[normalized] += c.confidence

    rows = {}
    for source, n in sorted(count.items()):
        rows[source] = {
            "count": n,
            "mean_confidence": conf_sum[source] / n if n else 0.0,
        }
    return rows


def _dead_signature_columns(
    bundle: CanonicalBundle,
) -> Dict[str, List[str]]:
    """For each entity, list signature columns that appear in zero asset column sets."""
    all_cols: Set[str] = {c.normalized_name for c in bundle.columns}
    dead: Dict[str, List[str]] = {}
    for entity, sig in SynonymRegistry.ENTITY_SIGNATURE_COLUMNS.items():
        missing = sorted(sig - all_cols)
        if missing:
            dead[entity] = missing
    return dead


def _unbound_asset_tokens(
    bundle: CanonicalBundle,
    asset_to_candidates: Dict[str, List],
    top_n: int = 25,
) -> List[Tuple[str, int]]:
    unbound_ids = {aid for aid, cands in asset_to_candidates.items() if not cands}
    if not unbound_ids:
        return []

    tokens: Counter = Counter()
    for a in bundle.assets:
        if a.internal_id not in unbound_ids:
            continue
        tokens.update(_tokenise(a.name))
        tokens.update(_tokenise(a.description or ""))
        for tag in a.tags:
            tokens.update(_tokenise(tag))
    for col in bundle.columns:
        if col.asset_internal_id in unbound_ids:
            tokens.update(_tokenise(col.name))
    return tokens.most_common(top_n)


def _unbound_asset_list(
    bundle: CanonicalBundle,
    asset_to_candidates: Dict[str, List],
    limit: int = 30,
) -> List[str]:
    unbound_ids = {aid for aid, cands in asset_to_candidates.items() if not cands}
    return sorted(a.name for a in bundle.assets if a.internal_id in unbound_ids)[:limit]


def _tied_primary_list(
    bundle: CanonicalBundle,
    asset_to_candidates: Dict[str, List],
    limit: int = 30,
) -> List[Tuple[str, List[str], float]]:
    name_by_id = {a.internal_id: a.name for a in bundle.assets}
    out: List[Tuple[str, List[str], float]] = []
    for aid, cands in asset_to_candidates.items():
        if not cands:
            continue
        top = max(c.confidence for c in cands)
        tied = sorted({c.entity_label for c in cands if c.confidence == top})
        if len(tied) >= 2:
            out.append((name_by_id.get(aid, aid), tied, top))
    out.sort(key=lambda x: (-x[2], x[0]))
    return out[:limit]


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _print_report(bundle: CanonicalBundle, md: List[str]) -> None:
    # Run EntityMapper
    binder = ConformedFieldBinder()
    binder_results = binder.bind(bundle)
    mapper = EntityMapper()
    all_candidates = mapper.map(bundle, binder_results)

    # Index per-asset
    asset_to_candidates: Dict[str, List] = {a.internal_id: [] for a in bundle.assets}
    for c in all_candidates:
        asset_to_candidates.setdefault(c.asset_id, []).append(c)

    # --- Coverage ---
    cov = _coverage_summary(asset_to_candidates)
    table = Table(title="Coverage summary", show_header=False)
    table.add_column("Metric", style="bold cyan")
    table.add_column("Value", style="green")
    for k, v in cov.items():
        pct = (v / cov["total"]) * 100 if cov["total"] else 0.0
        table.add_row(k.replace("_", " "), f"{v}  ({pct:.0f}%)" if k != "total" else str(v))
    console.print(table)

    md += [
        "## Coverage summary",
        "",
        "| Metric | Value |",
        "|--------|-------|",
    ]
    for k, v in cov.items():
        pct = (v / cov["total"]) * 100 if cov["total"] else 0.0
        val = f"{v} ({pct:.0f}%)" if k != "total" else str(v)
        md.append(f"| {k.replace('_', ' ')} | {val} |")
    md.append("")

    # --- Per-entity stats ---
    entity_stats = _per_entity_stats(asset_to_candidates)
    table = Table(title="Per-entity counts")
    table.add_column("entity", style="bold cyan")
    table.add_column("candidacy", justify="right")
    table.add_column("primary", justify="right")
    table.add_column("mean confidence", justify="right")
    for entity, stats in entity_stats.items():
        table.add_row(
            entity, str(stats["candidacy"]), str(stats["primary"]),
            f"{stats['mean_confidence']:.2f}",
        )
    console.print(table)

    md += [
        "## Per-entity counts",
        "",
        "| Entity | Candidacy | Primary | Mean confidence |",
        "|--------|-----------|---------|-----------------|",
    ]
    for entity, stats in entity_stats.items():
        md.append(
            f"| {entity} | {stats['candidacy']} | {stats['primary']} "
            f"| {stats['mean_confidence']:.2f} |"
        )
    md.append("")

    # --- Per-signal attribution ---
    signal_stats = _per_signal_attribution(all_candidates)
    table = Table(title="Per-signal attribution")
    table.add_column("signal", style="bold cyan")
    table.add_column("candidates", justify="right")
    table.add_column("mean confidence", justify="right")
    for source, stats in signal_stats.items():
        table.add_row(source, str(stats["count"]), f"{stats['mean_confidence']:.2f}")
    console.print(table)

    md += [
        "## Per-signal attribution",
        "",
        "Count of surviving EntityCandidates whose evidence includes each signal. "
        "A candidate merged from multiple signals is counted against each.",
        "",
        "| Signal | Candidates | Mean confidence |",
        "|--------|------------|-----------------|",
    ]
    for source, stats in signal_stats.items():
        md.append(f"| {source} | {stats['count']} | {stats['mean_confidence']:.2f} |")
    md.append("")

    # --- Dead signature columns ---
    dead_sigs = _dead_signature_columns(bundle)
    if dead_sigs:
        console.print("\n[bold]Dead signature columns[/bold] "
                      "(defined but never appear in any asset's columns)")
        for entity, cols in dead_sigs.items():
            console.print(f"  [cyan]{entity}[/cyan]: {', '.join(cols)}")
        md += [
            "## Dead signature columns",
            "",
            "Columns listed under `entity_signatures` in `entity_bindings.yaml` "
            "that do not appear in any asset's column set. These contribute zero "
            "to Signal-2 scoring and to IDENTIFIES-edge emission; candidates for "
            "removal or review.",
            "",
        ]
        for entity, cols in dead_sigs.items():
            md.append(f"- **{entity}**: `" + "`, `".join(cols) + "`")
        md.append("")
    else:
        md += ["## Dead signature columns", "", "_None — every signature column appears in at least one asset._", ""]

    # --- Unbound tokens ---
    tokens = _unbound_asset_tokens(bundle, asset_to_candidates)
    if tokens:
        console.print("\n[bold]Top tokens in unbound assets[/bold] (stopwords filtered)")
        for tok, n in tokens[:15]:
            console.print(f"  {tok:<30s} {n:>4d}")
        md += [
            "## Top tokens in unbound assets",
            "",
            "Name/tag/description and column-name tokens from assets with zero "
            "entity candidates. High-frequency tokens here are candidates for new "
            "signature columns or asset-name patterns.",
            "",
            "| Token | Count |",
            "|-------|-------|",
        ]
        for tok, n in tokens:
            md.append(f"| `{tok}` | {n} |")
        md.append("")

    # --- Unbound assets ---
    unbound = _unbound_asset_list(bundle, asset_to_candidates)
    if unbound:
        console.print(f"\n[bold]Unbound assets (first {len(unbound)}):[/bold]")
        for name in unbound[:15]:
            console.print(f"  {name}")
        md += [
            "## Unbound assets",
            "",
            "Assets with zero entity candidates above "
            f"`MIN_CONFIDENCE={MIN_CONFIDENCE}` (showing up to 30):",
            "",
        ]
        for name in unbound:
            md.append(f"- `{name}`")
        md.append("")

    # --- Tied primary ---
    tied = _tied_primary_list(bundle, asset_to_candidates)
    if tied:
        console.print(f"\n[bold]Tied-primary assets (top {len(tied)}):[/bold]")
        for name, entities, conf in tied[:10]:
            console.print(f"  {name:<55s} {entities}  @ {conf:.2f}")
        md += [
            "## Tied-primary assets",
            "",
            "Assets whose top entity confidence is shared by 2+ entities. "
            "Many ties at the same confidence value indicate signals that can't "
            "discriminate between the tied entities.",
            "",
            "| Asset | Tied entities | Top confidence |",
            "|-------|---------------|----------------|",
        ]
        for name, entities, conf in tied:
            md.append(f"| `{name}` | {', '.join(entities)} | {conf:.2f} |")
        md.append("")


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
    md: List[str] = [
        "# Entity taxonomy audit",
        "",
        f"Bundle: `{bundle}`  ·  Assets: {len(bundle_data.assets)}  "
        f"·  MIN_CONFIDENCE: {MIN_CONFIDENCE}",
        "",
    ]
    _print_report(bundle_data, md)

    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text("\n".join(md), encoding="utf-8")
        console.print(f"\n[green]Markdown report written to:[/green] {output}")


if __name__ == "__main__":
    app()
