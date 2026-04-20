"""Neo4jGraphStore — production graph store backed by the Neo4j driver."""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional


class Neo4jGraphStore:
    """Graph store backed by Neo4j 5.x. Reads credentials from environment."""

    def __init__(self) -> None:
        enable = os.environ.get("ENABLE_NEO4J", "false").lower()
        if enable != "true":
            raise RuntimeError(
                "Neo4jGraphStore requires ENABLE_NEO4J=true in your environment. "
                "Set ENABLE_NEO4J=true and configure NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD."
            )
        try:
            from neo4j import GraphDatabase
        except ImportError as exc:
            raise RuntimeError(
                "Could not import the 'neo4j' package. "
                "Install it with: uv add neo4j"
            ) from exc

        uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
        user = os.environ.get("NEO4J_USER", "neo4j")
        password = os.environ.get("NEO4J_PASSWORD", "password")
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def ping(self) -> bool:
        """Return True if the Neo4j instance is reachable."""
        try:
            self._driver.verify_connectivity()
            return True
        except Exception:
            return False

    def upsert_nodes(self, nodes: list) -> int:
        count = 0
        with self._driver.session() as session:
            for node in nodes:
                label = node.label
                props = {**node.properties, "build_id": node.build_id}
                evidence_json = str(node.evidence)
                cypher = (
                    f"MERGE (n:{label} {{node_id: $node_id}}) "
                    "SET n += $props "
                    "SET n.evidence = $evidence"
                )
                session.run(cypher, node_id=node.node_id, props=props, evidence=evidence_json)
                count += 1
        return count

    def upsert_edges(self, edges: list) -> int:
        count = 0
        with self._driver.session() as session:
            for edge in edges:
                et = edge.edge_type.value
                cypher = (
                    "MATCH (src {node_id: $src_id}) "
                    "MATCH (tgt {node_id: $tgt_id}) "
                    f"MERGE (src)-[r:{et} {{edge_id: $edge_id}}]->(tgt) "
                    "SET r += $props "
                    "SET r.build_id = $build_id"
                )
                session.run(
                    cypher,
                    src_id=edge.source_node_id,
                    tgt_id=edge.target_node_id,
                    edge_id=edge.edge_id,
                    props=edge.properties,
                    build_id=edge.build_id,
                )
                count += 1
        return count

    def tag_build(self, build_id: str, metadata: Dict[str, Any]) -> None:
        with self._driver.session() as session:
            session.run(
                "MERGE (b:_BuildMeta {build_id: $build_id}) SET b += $metadata",
                build_id=build_id,
                metadata=metadata,
            )

    def query_lineage(
        self, asset_id: str, direction: str = "downstream", depth: int = 5
    ) -> list:
        if direction == "downstream":
            cypher = (
                "MATCH path = (start:Asset {node_id: $id})-[:DEPENDS_ON*1..$depth]->(downstream) "
                "RETURN downstream"
            )
        else:
            cypher = (
                "MATCH path = (upstream)-[:DEPENDS_ON*1..$depth]->(start:Asset {node_id: $id}) "
                "RETURN upstream"
            )
        with self._driver.session() as session:
            result = session.run(cypher, id=asset_id, depth=depth)
            return [dict(record["downstream" if direction == "downstream" else "upstream"]) for record in result]

    def export_json(self, path: Path) -> None:
        raise NotImplementedError(
            "Neo4jGraphStore.export_json is not implemented. "
            "Use JsonGraphStore for local JSON export."
        )
