"""GraphBuild — orchestrates the structural compiler and a graph store."""

import logging
from pathlib import Path
from typing import Optional

from ingestion.contracts.bundle import CanonicalBundle
from graph.compiler.structural import GraphBuildArtifact, StructuralGraphCompiler
from graph.store import get_graph_store

logger = logging.getLogger(__name__)


class GraphBuild:
    """Compile a CanonicalBundle and write results to a graph store."""

    def __init__(self, store_type: str = "json", output_dir: Optional[Path] = None) -> None:
        self.store_type = store_type
        self.output_dir = output_dir
        self.compiler = StructuralGraphCompiler()

    def run(self, bundle: CanonicalBundle, build_id: Optional[str] = None) -> GraphBuildArtifact:
        store = get_graph_store(self.store_type, self.output_dir)
        nodes, edges, artifact = self.compiler.compile(bundle, build_id=build_id)

        store.upsert_nodes(nodes)
        store.upsert_edges(edges)
        store.tag_build(artifact.build_id, {
            "ingestion_run_id": artifact.ingestion_run_id,
            "compiler_version": artifact.compiler_version,
            "timestamp_utc": artifact.timestamp_utc,
        })

        if self.output_dir:
            store.export_json(self.output_dir)
            logger.info("Graph exported to %s", self.output_dir)

        logger.info(
            "GraphBuild complete: build_id=%s nodes=%s edges=%s coverage=%.1f%%",
            artifact.build_id,
            artifact.node_counts,
            artifact.edge_counts,
            artifact.lineage_coverage_pct * 100,
        )
        return artifact
