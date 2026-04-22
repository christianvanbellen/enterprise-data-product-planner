#!/usr/bin/env python
"""Phase 5 CLI: generate data-product specs for opportunity-layer initiatives."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import typer
from rich.console import Console
from rich.table import Table

from ingestion.contracts.bundle import CanonicalBundle
from graph.store.json_store import JsonGraphStore
from graph.opportunity.primitive_extractor import CapabilityPrimitiveExtractor
from graph.opportunity.archetype_library import InitiativeArchetypeLibrary, INITIATIVE_ARCHETYPES
from graph.opportunity.planner import OpportunityPlanner
from graph.spec.pipeline import SpecGenerationPipeline

app = typer.Typer(help="Run Phase 5: generate data-product specifications.")
console = Console()

_READINESS_STYLE = {
    "ready_now":               "green",
    "ready_with_enablement":   "yellow",
    "needs_foundational_work": "dim",
    "not_currently_feasible":  "red",
}


@app.command()
def main(
    bundle: Path = typer.Option(..., "--bundle", help="Path to bundle.json from Phase 1"),
    graph: Path  = typer.Option(..., "--graph",  help="Directory containing Phase 4 nodes.json/edges.json"),
    initiatives: str = typer.Option(
        "all",
        "--initiatives",
        help="Initiatives to process: 'all', 'ready_now', or comma-separated IDs",
    ),
    render: bool = typer.Option(True,  "--render/--no-render", help="Call LLM to render specs"),
    force_render: bool = typer.Option(False, "--force-render", help="Re-render even if cached"),
    log_dir: Path = typer.Option(
        Path("output/spec_log"), "--log-dir", help="Directory for spec log output"
    ),
) -> None:

    # ── Validate inputs ────────────────────────────────────────────────────
    if not bundle.exists():
        console.print(f"[red]Bundle not found: {bundle}[/red]")
        raise typer.Exit(1)
    if not (graph / "nodes.json").exists():
        console.print(f"[red]Graph nodes.json not found in: {graph}[/red]")
        console.print("[yellow]Run scripts/run_phase4.py first.[/yellow]")
        raise typer.Exit(1)

    # ── Load inputs ────────────────────────────────────────────────────────
    console.print("[bold cyan]Phase 5 — Spec Generator[/bold cyan]")
    console.print(f"  bundle:      {bundle}")
    console.print(f"  graph:       {graph}")
    console.print(f"  render:      {render}")
    console.print(f"  force:       {force_render}")
    console.print(f"  log_dir:     {log_dir}")

    canonical_bundle = CanonicalBundle.from_json(bundle)
    graph_store = JsonGraphStore.from_json(graph)

    primitives = CapabilityPrimitiveExtractor().extract(canonical_bundle, graph_store)
    library = InitiativeArchetypeLibrary()
    opps = OpportunityPlanner().plan(primitives, library)
    opp_by_id = {o.initiative_id: o for o in opps}

    # ── Resolve initiative list ────────────────────────────────────────────
    if initiatives == "all":
        initiative_ids = list(INITIATIVE_ARCHETYPES.keys())
    elif initiatives == "ready_now":
        initiative_ids = [o.initiative_id for o in opps if o.readiness == "ready_now"]
    else:
        initiative_ids = [i.strip() for i in initiatives.split(",") if i.strip()]
        unknown = [i for i in initiative_ids if i not in INITIATIVE_ARCHETYPES]
        if unknown:
            console.print(f"[red]Unknown initiative IDs: {unknown}[/red]")
            raise typer.Exit(1)

    console.print(f"  initiatives: {len(initiative_ids)} selected\n")

    # ── Progress callback ──────────────────────────────────────────────────
    _STATUS_STYLE = {"rendered": "green", "cached": "dim", "assembled": "yellow", "error": "red"}
    _STATUS_ICON  = {"rendered": "+", "cached": "~", "assembled": "-", "error": "!"}

    def _on_progress(idx: int, total: int, init_id: str, readiness: str, status: str) -> None:
        rs   = _READINESS_STYLE.get(readiness, "")
        ss   = _STATUS_STYLE.get(status, "")
        icon = _STATUS_ICON.get(status, "?")
        pad  = len(str(total))
        # Only wrap in markup when style is non-empty — empty tags crash Rich
        id_part = f"[{rs}]{init_id:<45}[/{rs}]" if rs else f"{init_id:<45}"
        st_part = f"[{ss}]{icon} {status}[/{ss}]" if ss else f"{icon} {status}"
        console.print(f"  [{idx:>{pad}}/{total}] {id_part} {st_part}")

    # ── Run pipeline ───────────────────────────────────────────────────────
    pipeline = SpecGenerationPipeline()
    report = pipeline.run(
        initiative_ids=initiative_ids,
        graph_store=graph_store,
        bundle=canonical_bundle,
        primitives=primitives,
        opportunity_results=opps,
        log_dir=log_dir,
        render=render,
        force_render=force_render,
        archetype_lib=library,
        on_progress=_on_progress,
    )

    # ── Summary table ──────────────────────────────────────────────────────
    summary = Table(title="Phase 5 complete", show_header=False)
    summary.add_column("Metric", style="bold cyan")
    summary.add_column("Value", style="green")
    summary.add_row("Initiatives processed", str(report.total))
    summary.add_row("Assembled",             str(report.assembled))
    summary.add_row("Rendered (new)",        str(report.rendered))
    summary.add_row("Cached (reused)",       str(report.cached))
    summary.add_row("Errors",               str(report.errors))
    summary.add_row("Log directory",         str(log_dir))
    console.print(summary)

    # ── Per-initiative table ───────────────────────────────────────────────
    detail = Table(title="Spec log", show_lines=False)
    detail.add_column("Initiative",  style="bold")
    detail.add_column("Readiness",   style="cyan")
    detail.add_column("Type",        style="dim")
    detail.add_column("Rendered",    style="green")
    detail.add_column("spec_id",     style="dim")

    for entry in sorted(report.entries, key=lambda e: e.initiative_id):
        opp = opp_by_id.get(entry.initiative_id)
        readiness_str = opp.readiness if opp else entry.readiness
        style = _READINESS_STYLE.get(readiness_str, "")
        rendered_str = "yes" if entry.rendered else "no"
        detail.add_row(
            f"[{style}]{entry.initiative_id}[/{style}]",
            f"[{style}]{readiness_str}[/{style}]",
            entry.spec_type,
            rendered_str,
            entry.spec_id,
        )
    console.print(detail)

    # ── Print full markdown for top-scoring processed initiative ─────────────
    processed_ids = {e.initiative_id for e in report.entries}
    processed_opps = [o for o in opps if o.initiative_id in processed_ids]
    if processed_opps and report.rendered_specs:
        top_opp = max(processed_opps, key=lambda o: o.composite_score)
        md = report.rendered_specs.get(top_opp.initiative_id)
        if not md:
            # Try loading from log
            from graph.spec.log import SpecLog
            from ingestion.normalisation.hashing import stable_hash
            from graph.spec.pipeline import _infer_build_id
            build_id = _infer_build_id(graph_store)
            spec_id = stable_hash(top_opp.initiative_id, build_id)
            log = SpecLog(log_dir)
            if log.has_spec(spec_id):
                _, md = log.load(spec_id)

        if md:
            console.print(
                f"\n[bold]Full spec — {top_opp.initiative_name} "
                f"(composite score {top_opp.composite_score:.3f}):[/bold]\n"
            )
            # Write raw UTF-8 to stdout — bypasses Rich's Windows cp1252 encoder
            # which cannot handle ✓/✗ and box-drawing characters in the spec.
            import sys
            sys.stdout.buffer.write(md.encode("utf-8"))
            sys.stdout.buffer.write(b"\n")


if __name__ == "__main__":
    app()
