"""JsonGraphStore — in-memory graph store with JSON export. Zero external dependencies."""

import dataclasses
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from graph.schema.edges import GraphEdge
from graph.schema.nodes import GraphNode


def _node_to_dict(node: GraphNode) -> Dict[str, Any]:
    return {
        "node_id": node.node_id,
        "label": node.label,
        "properties": node.properties,
        "evidence": node.evidence,
        "build_id": node.build_id,
    }


def _edge_to_dict(edge: GraphEdge) -> Dict[str, Any]:
    return {
        "edge_id": edge.edge_id,
        "edge_type": edge.edge_type.value,
        "source_node_id": edge.source_node_id,
        "target_node_id": edge.target_node_id,
        "properties": edge.properties,
        "evidence": edge.evidence,
        "build_id": edge.build_id,
    }


class JsonGraphStore:
    """Local in-memory graph store. Persists to two JSON files via export_json()."""

    def __init__(self, output_dir: Optional[Path] = None) -> None:
        self._nodes: Dict[str, Dict[str, Any]] = {}
        self._edges: Dict[str, Dict[str, Any]] = {}
        self.output_dir = Path(output_dir) if output_dir else None

    def upsert_nodes(self, nodes: List[GraphNode]) -> int:
        count = 0
        for node in nodes:
            self._nodes[node.node_id] = _node_to_dict(node)
            count += 1
        return count

    def upsert_edges(self, edges: List[GraphEdge]) -> int:
        count = 0
        for edge in edges:
            self._edges[edge.edge_id] = _edge_to_dict(edge)
            count += 1
        return count

    def purge_layer(self, graph_layer: str) -> int:
        """Remove all nodes and edges whose properties.graph_layer matches.

        Returns the number of items removed. Used by phase compilers to
        replace stale opportunity-layer nodes/edges on re-run.
        """
        node_ids_to_remove = [
            nid for nid, n in self._nodes.items()
            if n.get("properties", {}).get("graph_layer") == graph_layer
        ]
        edge_ids_to_remove = [
            eid for eid, e in self._edges.items()
            if e.get("properties", {}).get("graph_layer") == graph_layer
        ]
        for nid in node_ids_to_remove:
            del self._nodes[nid]
        for eid in edge_ids_to_remove:
            del self._edges[eid]
        return len(node_ids_to_remove) + len(edge_ids_to_remove)

    def tag_build(self, build_id: str, metadata: Dict[str, Any]) -> None:
        """Store build metadata in a special node."""
        self._nodes[f"_build_{build_id}"] = {
            "node_id": f"_build_{build_id}",
            "label": "_BuildMeta",
            "properties": {"build_id": build_id, **metadata},
            "evidence": {},
            "build_id": build_id,
        }

    def export_json(self, path: Path) -> None:
        """Write nodes.json and edges.json to the given directory."""
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)

        with (path / "nodes.json").open("w", encoding="utf-8") as fh:
            json.dump(list(self._nodes.values()), fh, indent=2, sort_keys=True)

        with (path / "edges.json").open("w", encoding="utf-8") as fh:
            json.dump(list(self._edges.values()), fh, indent=2, sort_keys=True)

    @classmethod
    def from_json(cls, path: Path) -> "JsonGraphStore":
        """Load an existing nodes.json + edges.json into a new store instance."""
        path = Path(path)
        store = cls(output_dir=path)
        nodes_file = path / "nodes.json"
        edges_file = path / "edges.json"
        if nodes_file.exists():
            for n in json.loads(nodes_file.read_text(encoding="utf-8")):
                store._nodes[n["node_id"]] = n
        if edges_file.exists():
            for e in json.loads(edges_file.read_text(encoding="utf-8")):
                store._edges[e["edge_id"]] = e
        return store

    def query_lineage(
        self, asset_id: str, direction: str = "downstream", depth: int = 5
    ) -> List[Dict[str, Any]]:
        """Pure Python BFS/DFS traversal over in-memory edge dict.

        direction: "downstream" follows DEPENDS_ON edges away from asset_id.
                   "upstream" follows DEPENDS_ON edges toward asset_id.
        Returns list of node dicts reachable within `depth` hops.
        """
        visited: set[str] = set()
        frontier = [asset_id]
        result: List[Dict[str, Any]] = []

        for _ in range(depth):
            next_frontier = []
            for current_id in frontier:
                for edge in self._edges.values():
                    if edge.get("edge_type") != "DEPENDS_ON":
                        continue
                    if direction == "downstream" and edge["source_node_id"] == current_id:
                        target = edge["target_node_id"]
                        if target not in visited:
                            visited.add(target)
                            next_frontier.append(target)
                            if target in self._nodes:
                                result.append(self._nodes[target])
                    elif direction == "upstream" and edge["target_node_id"] == current_id:
                        source = edge["source_node_id"]
                        if source not in visited:
                            visited.add(source)
                            next_frontier.append(source)
                            if source in self._nodes:
                                result.append(self._nodes[source])
            frontier = next_frontier
            if not frontier:
                break

        return result
