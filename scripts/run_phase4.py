#!/usr/bin/env python
"""Phase 4 CLI: add opportunity layer to an existing semantic graph."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import typer
from rich.console import Console
from rich.table import Table

from ingestion.contracts.bundle import CanonicalBundle
from ingestion.normalisation.hashing import stable_hash, utc_now_iso
from graph.store.json_store import JsonGraphStore
from graph.opportunity.compiler import OpportunityGraphCompiler

app = typer.Typer(help="Run Phase 4: add opportunity layer to an existing semantic graph.")
console = Console()

READINESS_LABEL = {
    "ready_now":              "[green]ready now[/green]",
    "ready_with_enablement":  "[yellow]ready with enablement[/yellow]",
    "needs_foundational_work": "[dim]needs foundational work[/dim]",
    "not_currently_feasible": "[red]not feasible[/red]",
}


@app.command()
def main(
    bundle: Path = typer.Option(..., "--bundle", help="Path to bundle.json from Phase 1"),
    graph: Path  = typer.Option(..., "--graph",  help="Directory containing Phase 2/3 nodes.json/edges.json"),
    store: str   = typer.Option("json", "--store", help="Graph store type (only 'json' supported)"),
    output: Path = typer.Option(..., "--output", help="Output directory for enriched graph JSON"),
    build_id: str = typer.Option(None, "--build-id", help="Optional build ID (auto-generated if omitted)"),
) -> None:
    if not bundle.exists():
        console.print(f"[red]Bundle file not found: {bundle}[/red]")
        raise typer.Exit(1)

    nodes_file = graph / "nodes.json"
    if not nodes_file.exists():
        console.print(f"[red]Graph nodes.json not found in: {graph}[/red]")
        console.print("[yellow]Run scripts/run_phase3.py first.[/yellow]")
        raise typer.Exit(1)

    if store != "json":
        console.print("[red]Only 'json' store is supported for Phase 4.[/red]")
        raise typer.Exit(1)

    canonical_bundle = CanonicalBundle.from_json(bundle)
    graph_store = JsonGraphStore.from_json(graph)

    opp_build_id = build_id or f"opp_{stable_hash(utc_now_iso())}"

    compiler = OpportunityGraphCompiler()
    artifact = compiler.compile(canonical_bundle, graph_store, opp_build_id)

    graph_store.export_json(output)

    # ── Summary table ──────────────────────────────────────────────────
    table = Table(title="Opportunity graph complete", show_header=False)
    table.add_column("Metric", style="bold cyan")
    table.add_column("Value", style="green")
    table.add_row("build_id",              artifact.build_id)
    table.add_row("Capability primitives", str(artifact.primitive_count))
    table.add_row("Initiatives",           str(artifact.initiative_count))
    table.add_row("  ready now",           str(artifact.ready_now_count))
    table.add_row("  ready with enablement", str(artifact.ready_with_enablement_count))
    table.add_row("  needs foundational work", str(artifact.needs_foundational_work_count))
    table.add_row("  not feasible",        str(artifact.not_feasible_count))
    table.add_row("Gaps identified",       str(artifact.gap_count))
    console.print(table)

    # ── Top initiatives ────────────────────────────────────────────────
    if artifact.top_initiatives:
        console.print("\n[bold]Top 5 by composite score:[/bold]")

        # Re-load opportunities to show readiness alongside name
        from graph.opportunity.primitive_extractor import CapabilityPrimitiveExtractor
        from graph.opportunity.archetype_library import InitiativeArchetypeLibrary
        from graph.opportunity.planner import OpportunityPlanner
        fresh_graph = JsonGraphStore.from_json(output)
        primitives = CapabilityPrimitiveExtractor().extract(canonical_bundle, fresh_graph)
        opps = OpportunityPlanner().plan(primitives, InitiativeArchetypeLibrary())
        opp_by_id = {o.initiative_id: o for o in opps}

        for rank, iid in enumerate(artifact.top_initiatives, 1):
            opp = opp_by_id.get(iid)
            if opp:
                label = READINESS_LABEL.get(opp.readiness, opp.readiness)
                console.print(
                    f"  {rank}. {opp.initiative_name} "
                    f"({label}) score={opp.composite_score:.2f}"
                )

    # ── Highest leverage gaps ──────────────────────────────────────────
    if artifact.highest_leverage_gaps:
        console.print("\n[bold]Highest leverage gaps:[/bold]")
        from graph.opportunity.gap_analyser import GapAnalyser
        fresh_graph2 = JsonGraphStore.from_json(output)
        primitives2 = CapabilityPrimitiveExtractor().extract(canonical_bundle, fresh_graph2)
        opps2 = OpportunityPlanner().plan(primitives2, InitiativeArchetypeLibrary())
        gaps = GapAnalyser().analyse(primitives2, opps2)
        gap_by_prim = {g.primitive_id: g for g in gaps}

        for rank, pid in enumerate(artifact.highest_leverage_gaps, 1):
            g = gap_by_prim.get(pid)
            if g:
                console.print(
                    f"  {rank}. {pid} blocks {len(g.blocking_initiatives)} initiative"
                    f"{'s' if len(g.blocking_initiatives) != 1 else ''} "
                    f"(leverage={g.leverage_score:.2f})"
                )


if __name__ == "__main__":
    app()
