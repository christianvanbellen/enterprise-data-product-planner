#!/usr/bin/env python
"""Phase 1 CLI: ingest source files → CanonicalBundle → JSON output."""

import sys
from pathlib import Path

# Ensure repo root is on sys.path when run directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import typer
from rich.console import Console
from rich.table import Table

from ingestion.pipeline import IngestionPipeline, PipelineConfig

app = typer.Typer(help="Run Phase 1: ingest metadata sources into a CanonicalBundle.")
console = Console()


@app.command()
def main(
    dbt_metadata: Path = typer.Option(None, "--dbt-metadata", help="Path to dbt enriched metadata JSON"),
    conformed_schema: Path = typer.Option(None, "--conformed-schema", help="Path to conformed schema JSON"),
    info_schema: Path = typer.Option(None, "--info-schema", help="Path to information schema JSON"),
    glossary: Path = typer.Option(None, "--glossary", help="Path to business glossary JSON"),
    erd: Path = typer.Option(None, "--erd", help="Path to ERD JSON"),
    output: Path = typer.Option(..., "--output", help="Output path for bundle.json"),
    source_system: str = typer.Option("default", "--source-system", help="Source system name"),
) -> None:
    config = PipelineConfig(
        dbt_metadata_path=dbt_metadata,
        conformed_schema_path=conformed_schema,
        info_schema_path=info_schema,
        glossary_path=glossary,
        erd_path=erd,
        source_system_name=source_system,
    )

    pipeline = IngestionPipeline(config)
    bundle = pipeline.run_and_save(output)

    table = Table(title="Phase 1 Complete", show_header=False)
    table.add_column("Metric", style="bold cyan")
    table.add_column("Value", style="green")
    table.add_row("CanonicalBundle written to", str(output))
    table.add_row("Assets", str(len(bundle.assets)))
    table.add_row("Columns", str(len(bundle.columns)))
    table.add_row("Lineage edges", str(len(bundle.lineage_edges)))
    table.add_row("Business terms", str(len(bundle.business_terms)))
    console.print(table)


if __name__ == "__main__":
    app()
