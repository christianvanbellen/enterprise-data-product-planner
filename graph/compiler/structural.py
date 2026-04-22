"""StructuralGraphCompiler — converts a CanonicalBundle into graph nodes and edges."""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from ingestion.contracts.asset import CanonicalAsset
from ingestion.contracts.bundle import CanonicalBundle
from ingestion.normalisation.hashing import stable_hash, utc_now_iso
from graph.compiler.evidence import (
    CONFIDENCE_DIRECT_COL,
    CONFIDENCE_EXPLICIT_DEP,
    EvidenceRecord,
)
from graph.schema.edges import EdgeType, GraphEdge
from graph.schema.nodes import GraphNode


@dataclass
class GraphBuildArtifact:
    build_id: str
    ingestion_run_id: str
    timestamp_utc: str
    node_counts: Dict[str, int]
    edge_counts: Dict[str, int]
    lineage_coverage_pct: float
    unresolved_lineage: List[str]
    compiler_version: str = "structural_compiler_v1"
    ontology_version: str = "none"


class StructuralGraphCompiler:
    """Compile a CanonicalBundle into graph nodes, edges, and a build artifact."""

    def compile(
        self,
        bundle: CanonicalBundle,
        build_id: Optional[str] = None,
    ) -> Tuple[List[GraphNode], List[GraphEdge], GraphBuildArtifact]:
        if build_id is None:
            build_id = f"build_{stable_hash(bundle.metadata.get('source_file', ''), utc_now_iso())}"

        ingestion_run_id = bundle.metadata.get("ingestion_run_id", "unknown")

        # Step 1: asset nodes
        asset_nodes = self._create_asset_nodes(bundle, build_id)

        # Step 2: column nodes + HAS_COLUMN edges
        column_nodes, has_column_edges = self._create_column_nodes(bundle, build_id)

        # Step 3: schema nodes + containment edges (Schema → Asset CONTAINS)
        schema_nodes, containment_edges = self._create_schema_nodes_and_edges(bundle, build_id)

        # Step 4: lineage edges (DEPENDS_ON, confidence 1.0)
        lineage_edges = self._create_lineage_edges(bundle, build_id)

        # Step 5: SQL column-level edges (gated)
        sql_col_edges = self._create_sql_column_edges(bundle, build_id)

        # Step 6: test nodes + TESTED_BY edges
        test_nodes, tested_by_edges = self._attach_test_nodes(bundle, asset_nodes + column_nodes, build_id)

        # Step 7: doc nodes + DOCUMENTED_BY edges
        doc_nodes, documented_by_edges = self._attach_doc_nodes(bundle, asset_nodes, build_id)

        all_nodes = asset_nodes + column_nodes + schema_nodes + test_nodes + doc_nodes
        all_edges = (
            has_column_edges
            + containment_edges
            + lineage_edges
            + sql_col_edges
            + tested_by_edges
            + documented_by_edges
        )

        # Compute artifact
        node_counts: Dict[str, int] = {}
        for n in all_nodes:
            node_counts[n.label] = node_counts.get(n.label, 0) + 1

        edge_counts: Dict[str, int] = {}
        for e in all_edges:
            key = e.edge_type.value
            edge_counts[key] = edge_counts.get(key, 0) + 1

        # lineage coverage
        asset_ids_with_dep = set()
        for e in lineage_edges:
            asset_ids_with_dep.add(e.source_node_id)
            asset_ids_with_dep.add(e.target_node_id)

        total_assets = len(bundle.assets)
        coverage = (
            len(asset_ids_with_dep & {a.internal_id for a in bundle.assets}) / total_assets
            if total_assets > 0
            else 0.0
        )

        # Unresolved lineage
        asset_node_ids = {n.node_id for n in asset_nodes}
        unresolved = []
        for e in bundle.lineage_edges:
            if e.source_asset_id not in asset_node_ids:
                unresolved.append(e.source_asset_id)
            if e.target_asset_id not in asset_node_ids:
                unresolved.append(e.target_asset_id)
        unresolved = list(dict.fromkeys(unresolved))  # deduplicate, preserve order

        artifact = GraphBuildArtifact(
            build_id=build_id,
            ingestion_run_id=ingestion_run_id,
            timestamp_utc=utc_now_iso(),
            node_counts=node_counts,
            edge_counts=edge_counts,
            lineage_coverage_pct=round(coverage, 4),
            unresolved_lineage=unresolved,
        )

        return all_nodes, all_edges, artifact

    # ------------------------------------------------------------------ #
    # Step 1                                                               #
    # ------------------------------------------------------------------ #
    def _create_asset_nodes(self, bundle: CanonicalBundle, build_id: str) -> List[GraphNode]:
        nodes = []
        for asset in bundle.assets:
            ev = EvidenceRecord.auto(
                rule_id="structural.asset_node",
                confidence=1.0,
                evidence_sources=[{"type": "canonical_asset", "value": asset.internal_id}],
                build_id=build_id,
            )
            nodes.append(GraphNode.from_asset(asset, build_id, ev.to_dict()))
        return nodes

    # ------------------------------------------------------------------ #
    # Step 2                                                               #
    # ------------------------------------------------------------------ #
    def _create_column_nodes(
        self, bundle: CanonicalBundle, build_id: str
    ) -> Tuple[List[GraphNode], List[GraphEdge]]:
        nodes = []
        edges = []
        for col in bundle.columns:
            ev = EvidenceRecord.auto(
                rule_id="structural.column_node",
                confidence=CONFIDENCE_DIRECT_COL,
                evidence_sources=[{"type": "canonical_column", "value": col.internal_id}],
                build_id=build_id,
            )
            nodes.append(GraphNode.from_column(col, build_id, ev.to_dict()))

            # HAS_COLUMN edge: Asset → Column
            edge_id = f"e_{stable_hash(col.asset_internal_id, col.internal_id, 'HAS_COLUMN')}"
            edge_ev = EvidenceRecord.auto(
                rule_id="structural.has_column",
                confidence=CONFIDENCE_DIRECT_COL,
                evidence_sources=[
                    {"type": "canonical_column", "value": col.internal_id},
                    {"type": "canonical_asset", "value": col.asset_internal_id},
                ],
                build_id=build_id,
            )
            edges.append(
                GraphEdge(
                    edge_id=edge_id,
                    edge_type=EdgeType.HAS_COLUMN,
                    source_node_id=col.asset_internal_id,
                    target_node_id=col.internal_id,
                    properties={"confidence": CONFIDENCE_DIRECT_COL},
                    evidence=edge_ev.to_dict(),
                    build_id=build_id,
                )
            )
        return nodes, edges

    # ------------------------------------------------------------------ #
    # Step 3                                                               #
    # ------------------------------------------------------------------ #
    def _create_schema_nodes_and_edges(
        self, bundle: CanonicalBundle, build_id: str
    ) -> Tuple[List[GraphNode], List[GraphEdge]]:
        """Emit one Schema node per unique (database, schema_name) pair and a
        CONTAINS edge from each Schema to every asset it holds.

        Assets with no schema_name are skipped — they produce no Schema node
        and no CONTAINS edge.
        """
        # Group assets by (database, schema_name)
        by_schema: Dict[tuple, List[CanonicalAsset]] = {}
        for asset in bundle.assets:
            if not asset.schema_name:
                continue
            key = (asset.database or "", asset.schema_name)
            by_schema.setdefault(key, []).append(asset)

        schema_nodes: List[GraphNode] = []
        edges: List[GraphEdge] = []

        for (database, schema_name), assets in by_schema.items():
            node_ev = EvidenceRecord.auto(
                rule_id="structural.schema_node",
                confidence=1.0,
                evidence_sources=[
                    {"type": "canonical_asset_schema", "value": schema_name},
                    {"type": "canonical_asset_database", "value": database},
                ],
                build_id=build_id,
            )
            schema_node = GraphNode.from_schema(
                schema_name=schema_name,
                database=database,
                asset_count=len(assets),
                build_id=build_id,
                evidence=node_ev.to_dict(),
            )
            schema_nodes.append(schema_node)

            for asset in assets:
                edge_id = f"e_{stable_hash(schema_node.node_id, asset.internal_id, 'CONTAINS')}"
                edge_ev = EvidenceRecord.auto(
                    rule_id="structural.containment",
                    confidence=1.0,
                    evidence_sources=[{"type": "canonical_asset_schema", "value": schema_name}],
                    build_id=build_id,
                )
                edges.append(
                    GraphEdge(
                        edge_id=edge_id,
                        edge_type=EdgeType.CONTAINS,
                        source_node_id=schema_node.node_id,
                        target_node_id=asset.internal_id,
                        properties={"confidence": 1.0},
                        evidence=edge_ev.to_dict(),
                        build_id=build_id,
                    )
                )

        return schema_nodes, edges

    # ------------------------------------------------------------------ #
    # Step 4                                                               #
    # ------------------------------------------------------------------ #
    def _create_lineage_edges(self, bundle: CanonicalBundle, build_id: str) -> List[GraphEdge]:
        edges = []
        for lin in bundle.lineage_edges:
            edge_id = f"e_{stable_hash(lin.source_asset_id, lin.target_asset_id, 'DEPENDS_ON')}"
            ev = EvidenceRecord.auto(
                rule_id="lineage.explicit_upstream",
                confidence=CONFIDENCE_EXPLICIT_DEP,
                evidence_sources=[
                    {"type": "canonical_lineage_edge", "value": lin.internal_id},
                    {"type": "derivation_method", "value": lin.derivation_method},
                ],
                build_id=build_id,
            )
            edges.append(
                GraphEdge(
                    edge_id=edge_id,
                    edge_type=EdgeType.DEPENDS_ON,
                    source_node_id=lin.source_asset_id,
                    target_node_id=lin.target_asset_id,
                    properties={"confidence": CONFIDENCE_EXPLICIT_DEP, "relation_type": lin.relation_type},
                    evidence=ev.to_dict(),
                    build_id=build_id,
                )
            )
        return edges

    # ------------------------------------------------------------------ #
    # Step 5 (gated)                                                       #
    # ------------------------------------------------------------------ #
    def _create_sql_column_edges(self, bundle: CanonicalBundle, build_id: str) -> List[GraphEdge]:
        enabled = os.environ.get("ENABLE_SQL_LINEAGE", "false").lower() == "true"
        if not enabled:
            return []
        # SQL lineage extraction is delegated to graph.compiler.sql_lineage
        from graph.compiler.sql_lineage import extract_column_lineage  # noqa: F401
        # Currently returns empty — will be wired up when SQL lineage is implemented
        return []

    # ------------------------------------------------------------------ #
    # Step 6                                                               #
    # ------------------------------------------------------------------ #
    def _attach_test_nodes(
        self, bundle: CanonicalBundle, existing_nodes: List[GraphNode], build_id: str
    ) -> Tuple[List[GraphNode], List[GraphEdge]]:
        test_nodes = []
        tested_by_edges = []
        for col in bundle.columns:
            for test_name in col.tests:
                test_id = f"test_{stable_hash(col.internal_id, test_name)}"
                ev = EvidenceRecord.auto(
                    rule_id="structural.test_node",
                    confidence=1.0,
                    evidence_sources=[
                        {"type": "canonical_column_test", "value": test_name},
                        {"type": "canonical_column", "value": col.internal_id},
                    ],
                    build_id=build_id,
                )
                test_nodes.append(
                    GraphNode(
                        node_id=test_id,
                        label="Test",
                        properties={
                            "test_id": test_id,
                            "test_type": test_name,
                            "column_name": col.name,
                            "status": "unknown",
                        },
                        evidence=ev.to_dict(),
                        build_id=build_id,
                    )
                )
                edge_id = f"e_{stable_hash(col.internal_id, test_id, 'TESTED_BY')}"
                edge_ev = EvidenceRecord.auto(
                    rule_id="structural.tested_by",
                    confidence=1.0,
                    evidence_sources=[{"type": "canonical_column_test", "value": test_name}],
                    build_id=build_id,
                )
                tested_by_edges.append(
                    GraphEdge(
                        edge_id=edge_id,
                        edge_type=EdgeType.TESTED_BY,
                        source_node_id=col.internal_id,
                        target_node_id=test_id,
                        properties={"confidence": 1.0},
                        evidence=edge_ev.to_dict(),
                        build_id=build_id,
                    )
                )
        return test_nodes, tested_by_edges

    # ------------------------------------------------------------------ #
    # Step 7                                                               #
    # ------------------------------------------------------------------ #
    def _attach_doc_nodes(
        self, bundle: CanonicalBundle, asset_nodes: List[GraphNode], build_id: str
    ) -> Tuple[List[GraphNode], List[GraphEdge]]:
        doc_nodes = []
        documented_by_edges = []
        for asset in bundle.assets:
            has_description = bool(asset.description)
            doc_id = f"doc_{stable_hash(asset.internal_id, 'description')}"
            ev = EvidenceRecord.auto(
                rule_id="structural.doc_node",
                confidence=1.0,
                evidence_sources=[{"type": "canonical_asset", "value": asset.internal_id}],
                build_id=build_id,
            )
            doc_nodes.append(
                GraphNode(
                    node_id=doc_id,
                    label="DocObject",
                    properties={
                        "doc_id": doc_id,
                        "has_description": has_description,
                        "asset_id": asset.internal_id,
                    },
                    evidence=ev.to_dict(),
                    build_id=build_id,
                )
            )
            edge_id = f"e_{stable_hash(asset.internal_id, doc_id, 'DOCUMENTED_BY')}"
            edge_ev = EvidenceRecord.auto(
                rule_id="structural.documented_by",
                confidence=1.0,
                evidence_sources=[{"type": "canonical_asset", "value": asset.internal_id}],
                build_id=build_id,
            )
            documented_by_edges.append(
                GraphEdge(
                    edge_id=edge_id,
                    edge_type=EdgeType.DOCUMENTED_BY,
                    source_node_id=asset.internal_id,
                    target_node_id=doc_id,
                    properties={"confidence": 1.0, "has_description": has_description},
                    evidence=edge_ev.to_dict(),
                    build_id=build_id,
                )
            )
        return doc_nodes, documented_by_edges
