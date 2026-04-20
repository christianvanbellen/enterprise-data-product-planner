#!/usr/bin/env python
"""Phase 3 CLI: enrich a structural graph with a semantic layer."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import typer
from rich.console import Console
from rich.table import Table

from ingestion.contracts.bundle import CanonicalBundle
from ingestion.normalisation.hashing import stable_hash, utc_now_iso
from graph.store.json_store import JsonGraphStore
from graph.semantic.compiler import SemanticGraphCompiler

app = typer.Typer(help="Run Phase 3: add semantic layer to an existing structural graph.")
console = Console()


@app.command()
def main(
    bundle: Path = typer.Option(..., "--bundle", help="Path to bundle.json from Phase 1"),
    graph: Path = typer.Option(..., "--graph", help="Directory containing Phase 2 nodes.json/edges.json"),
    store: str = typer.Option("json", "--store", help="Graph store type (only 'json' supported)"),
    output: Path = typer.Option(..., "--output", help="Output directory for enriched graph JSON"),
    build_id: str = typer.Option(None, "--build-id", help="Optional build ID (auto-generated if omitted)"),
) -> None:
    if not bundle.exists():
        console.print(f"[red]Bundle file not found: {bundle}[/red]")
        raise typer.Exit(1)

    nodes_file = graph / "nodes.json"
    if not nodes_file.exists():
        console.print(f"[red]Graph nodes.json not found in: {graph}[/red]")
        console.print("[yellow]Run scripts/run_phase2.py first.[/yellow]")
        raise typer.Exit(1)

    if store != "json":
        console.print(f"[red]Only 'json' store is supported for Phase 3.[/red]")
        raise typer.Exit(1)

    canonical_bundle = CanonicalBundle.from_json(bundle)

    graph_store = JsonGraphStore.from_json(graph)

    sem_build_id = build_id or f"sem_{stable_hash(utc_now_iso())}"

    compiler = SemanticGraphCompiler()
    artifact = compiler.compile(canonical_bundle, graph_store, sem_build_id)

    graph_store.export_json(output)

    table = Table(title="Semantic build complete", show_header=False)
    table.add_column("Metric", style="bold cyan")
    table.add_column("Value", style="green")
    table.add_row("build_id", artifact.build_id)
    table.add_row("Entity nodes", str(artifact.entity_node_count))
    table.add_row("Domain nodes", str(artifact.domain_node_count))
    table.add_row("Metric nodes", str(artifact.metric_node_count))
    table.add_row("REPRESENTS edges", str(artifact.represents_edge_count))
    table.add_row("BELONGS_TO_DOMAIN edges", str(artifact.belongs_to_domain_edge_count))
    table.add_row("IDENTIFIES edges", str(artifact.identifies_edge_count))
    table.add_row("MEASURES edges", str(artifact.measures_edge_count))
    table.add_row("  — from semantic_candidates", str(artifact.measures_from_semantic_candidates))
    table.add_row("  — from name pattern", str(artifact.measures_from_name_pattern))
    table.add_row("METRIC_BELONGS_TO_ENTITY edges", str(artifact.metric_belongs_to_entity_edge_count))
    table.add_row("Low-confidence assets", str(len(artifact.low_confidence_assignments)))
    table.add_row("Unassigned assets", str(len(artifact.unassigned_assets)))
    console.print(table)

    if artifact.unassigned_assets:
        console.print(f"\n[dim]Unassigned ({len(artifact.unassigned_assets)}): "
                      f"{', '.join(artifact.unassigned_assets[:10])}"
                      f"{'...' if len(artifact.unassigned_assets) > 10 else ''}[/dim]")


if __name__ == "__main__":
    app()
