from pathlib import Path
from typing import Optional


def get_graph_store(store_type: str = "json", output_dir: Optional[Path] = None):
    """Factory function for graph stores.

    Args:
        store_type: "json" (default) or "neo4j"
        output_dir: output directory for JsonGraphStore

    Returns:
        An instance of JsonGraphStore or Neo4jGraphStore.
    """
    if store_type == "neo4j":
        from graph.store.neo4j_store import Neo4jGraphStore
        return Neo4jGraphStore()
    from graph.store.json_store import JsonGraphStore
    return JsonGraphStore(output_dir=output_dir)
