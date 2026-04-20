#!/usr/bin/env python
"""Phase 2 CLI: compile CanonicalBundle → structural graph → JSON or Neo4j."""

import sys
from pathlib import Path

# Ensure repo root is on sys.path when run directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import typer
from rich.console import Console
from rich.table import Table

from ingestion.contracts.bundle import CanonicalBundle
from graph.build import GraphBuild
from ingestion.normalisation.hashing import stable_hash, utc_now_iso

app = typer.Typer(help="Run Phase 2: compile a CanonicalBundle into a structural graph.")
console = Console()


@app.command()
def main(
    bundle: Path = typer.Option(..., "--bundle", help="Path to bundle.json from Phase 1"),
    store: str = typer.Option("json", "--store", help="Graph store type: 'json' or 'neo4j'"),
    output: Path = typer.Option(..., "--output", help="Output directory for graph JSON"),
    build_id: str = typer.Option(None, "--build-id", help="Optional build ID (auto-generated if omitted)"),
    phase3: bool = typer.Option(False, "--phase3", help="Chain into Phase 3 semantic compilation after Phase 2"),
) -> None:
    if not bundle.exists():
        console.print(f"[red]Bundle file not found: {bundle}[/red]")
        raise typer.Exit(1)

    canonical_bundle = CanonicalBundle.from_json(bundle)
    build = GraphBuild(store_type=store, output_dir=output)
    artifact = build.run(canonical_bundle, build_id=build_id or None)

    dep_count = artifact.edge_counts.get("DEPENDS_ON", 0)
    asset_count = artifact.node_counts.get("Asset", 0)
    col_count = artifact.node_counts.get("Column", 0)

    table = Table(title="Phase 2 Complete", show_header=False)
    table.add_column("Metric", style="bold cyan")
    table.add_column("Value", style="green")
    table.add_row("build_id", artifact.build_id)
    table.add_row("Asset nodes", str(asset_count))
    table.add_row("Column nodes", str(col_count))
    table.add_row("DEPENDS_ON edges", str(dep_count))
    table.add_row("Lineage coverage", f"{artifact.lineage_coverage_pct * 100:.1f}%")
    table.add_row("Unresolved refs", str(len(artifact.unresolved_lineage)))
    console.print(table)

    if phase3:
        from graph.store.json_store import JsonGraphStore
        from graph.semantic.compiler import SemanticGraphCompiler

        graph_store = JsonGraphStore.from_json(output)
        sem_build_id = f"sem_{stable_hash(utc_now_iso())}"
        sem_artifact = SemanticGraphCompiler().compile(canonical_bundle, graph_store, sem_build_id)
        graph_store.export_json(output)

        sem_table = Table(title="Semantic build complete", show_header=False)
        sem_table.add_column("Metric", style="bold cyan")
        sem_table.add_column("Value", style="green")
        sem_table.add_row("build_id", sem_artifact.build_id)
        sem_table.add_row("Entity nodes", str(sem_artifact.entity_node_count))
        sem_table.add_row("Domain nodes", str(sem_artifact.domain_node_count))
        sem_table.add_row("Metric nodes", str(sem_artifact.metric_node_count))
        sem_table.add_row("REPRESENTS edges", str(sem_artifact.represents_edge_count))
        sem_table.add_row("BELONGS_TO_DOMAIN edges", str(sem_artifact.belongs_to_domain_edge_count))
        sem_table.add_row("IDENTIFIES edges", str(sem_artifact.identifies_edge_count))
        sem_table.add_row("MEASURES edges", str(sem_artifact.measures_edge_count))
        sem_table.add_row("Unassigned assets", str(len(sem_artifact.unassigned_assets)))
        console.print(sem_table)


if __name__ == "__main__":
    app()
