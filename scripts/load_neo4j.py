"""load_neo4j.py — Import compiled graph into a running Neo4j instance.

Usage:
    python scripts/load_neo4j.py
    python scripts/load_neo4j.py --graph output/graph/

Reads NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD from .env (via python-dotenv).
Uses MERGE on node_id — safe to re-run (idempotent).

Quick start:
    docker run -d --name neo4j-dev -p 7474:7474 -p 7687:7687 \\
        -e NEO4J_AUTH=neo4j/password neo4j:5
    python scripts/load_neo4j.py
    # Open http://localhost:7474 in browser
"""

import argparse
import json
import os
import sys
from pathlib import Path

# ------------------------------------------------------------------ #
# Dependency check                                                     #
# ------------------------------------------------------------------ #

try:
    from dotenv import load_dotenv
except ImportError:
    print("ERROR: python-dotenv not installed. Run: pip install python-dotenv")
    sys.exit(1)

try:
    from neo4j import GraphDatabase
except ImportError:
    print("ERROR: neo4j driver not installed. Run: pip install neo4j")
    sys.exit(1)


# ------------------------------------------------------------------ #
# Cypher helpers                                                       #
# ------------------------------------------------------------------ #

CREATE_INDEXES = [
    "CREATE INDEX asset_id_idx IF NOT EXISTS FOR (n:Asset) ON (n.asset_id)",
    "CREATE INDEX asset_name_idx IF NOT EXISTS FOR (n:Asset) ON (n.name)",
    "CREATE INDEX col_id_idx IF NOT EXISTS FOR (n:Column) ON (n.col_id)",
]

MERGE_NODE = """
UNWIND $rows AS row
MERGE (n {node_id: row.node_id})
SET n += row.props
SET n:{label}
"""

MERGE_EDGE = """
UNWIND $rows AS row
MATCH (src {{node_id: row.source_node_id}})
MATCH (tgt {{node_id: row.target_node_id}})
MERGE (src)-[r:{edge_type} {{edge_id: row.edge_id}}]->(tgt)
SET r += row.props
SET r.build_id = row.build_id
SET r.confidence = row.confidence
"""

BATCH_SIZE = 500


def _flatten_props(node: dict) -> dict:
    """Flatten a node dict into a Neo4j-safe property map."""
    props = dict(node.get("properties", {}))
    props["node_id"] = node["node_id"]
    props["build_id"] = node.get("build_id", "")

    # Neo4j does not support nested objects — flatten evidence to strings
    ev = node.get("evidence", {})
    props["ev_rule_id"] = ev.get("rule_id", "")
    props["ev_confidence"] = ev.get("confidence", 1.0)
    props["ev_created_by"] = ev.get("created_by", "")
    props["ev_review_status"] = ev.get("review_status", "")

    # Lists of primitives are fine; convert nested lists to JSON strings
    for k, v in list(props.items()):
        if isinstance(v, list):
            if all(isinstance(x, (str, int, float, bool)) for x in v):
                pass  # keep as-is
            else:
                props[k] = json.dumps(v)
        elif isinstance(v, dict):
            props[k] = json.dumps(v)

    return props


def _edge_row(edge: dict) -> dict:
    props = dict(edge.get("properties", {}))
    ev = edge.get("evidence", {})
    return {
        "edge_id": edge["edge_id"],
        "source_node_id": edge["source_node_id"],
        "target_node_id": edge["target_node_id"],
        "build_id": edge.get("build_id", ""),
        "confidence": ev.get("confidence", props.get("confidence", 1.0)),
        "props": {k: v for k, v in props.items() if not isinstance(v, (dict, list))},
    }


def _batches(items, size=BATCH_SIZE):
    for i in range(0, len(items), size):
        yield items[i : i + size]


# ------------------------------------------------------------------ #
# Loader                                                               #
# ------------------------------------------------------------------ #

def load_graph(driver, nodes: list, edges: list) -> dict:
    counts = {}

    with driver.session() as session:
        # ---- Indexes ----
        print("Creating indexes...")
        for stmt in CREATE_INDEXES:
            session.run(stmt)

        # ---- Nodes by label ----
        label_order = ["Asset", "Column", "Test", "DocObject", "_BuildMeta"]
        by_label: dict[str, list] = {lbl: [] for lbl in label_order}
        other: list = []
        for node in nodes:
            lbl = node.get("label", "Unknown")
            if lbl in by_label:
                by_label[lbl].append(node)
            else:
                other.append(node)

        for lbl in label_order:
            group = by_label[lbl]
            if not group:
                continue
            print(f"Loading {len(group):,} {lbl} nodes...")
            rows = [{"node_id": n["node_id"], "props": _flatten_props(n)} for n in group]
            cypher = MERGE_NODE.format(label=lbl)
            for batch in _batches(rows):
                session.run(cypher, rows=batch)
            counts[f"nodes_{lbl}"] = len(group)

        if other:
            print(f"Loading {len(other):,} other nodes...")
            for node in other:
                lbl = node.get("label", "Unknown")
                rows = [{"node_id": node["node_id"], "props": _flatten_props(node)}]
                session.run(MERGE_NODE.format(label=lbl), rows=rows)
            counts["nodes_other"] = len(other)

        # ---- Edges by type ----
        edge_order = ["DEPENDS_ON", "HAS_COLUMN", "CONTAINS", "TESTED_BY", "DOCUMENTED_BY"]
        by_etype: dict[str, list] = {et: [] for et in edge_order}
        other_edges: list = []
        for edge in edges:
            et = edge.get("edge_type", "UNKNOWN")
            if et in by_etype:
                by_etype[et].append(edge)
            else:
                other_edges.append(edge)

        for et in edge_order:
            group = by_etype[et]
            if not group:
                continue
            print(f"Loading {len(group):,} {et} edges...")
            rows = [_edge_row(e) for e in group]
            cypher = MERGE_EDGE.format(edge_type=et)
            for batch in _batches(rows):
                session.run(cypher, rows=batch)
            counts[f"edges_{et}"] = len(group)

        if other_edges:
            print(f"Loading {len(other_edges):,} other edges...")
            for edge in other_edges:
                et = edge.get("edge_type", "UNKNOWN")
                rows = [_edge_row(edge)]
                session.run(MERGE_EDGE.format(edge_type=et), rows=rows)
            counts["edges_other"] = len(other_edges)

    return counts


# ------------------------------------------------------------------ #
# CLI                                                                  #
# ------------------------------------------------------------------ #

CYPHER_EXAMPLES = """
Ready-to-paste Cypher queries for Neo4j Browser:

  // 1. View the full lineage graph (limit 100)
  MATCH (a:Asset)-[r:DEPENDS_ON]->(b:Asset)
  RETURN a, r, b LIMIT 100

  // 2. View all columns of one asset (replace name)
  MATCH (a:Asset {name: 'hx_rate_monitoring'})-[:HAS_COLUMN]->(c:Column)
  RETURN a, c

  // 3. Full lineage from root
  MATCH path = (root:Asset {name: 'hx_raw'})-[:DEPENDS_ON*1..5]->(downstream)
  RETURN path
"""

QUICKSTART = """
Quick start (if Neo4j is not running yet):

  docker run -d --name neo4j-dev -p 7474:7474 -p 7687:7687 \\
      -e NEO4J_AUTH=neo4j/password neo4j:5
  python scripts/load_neo4j.py
  Open http://localhost:7474 in browser
"""


def main():
    parser = argparse.ArgumentParser(description="Load compiled graph into Neo4j.")
    parser.add_argument(
        "--graph",
        default="output/graph",
        help="Path to graph output dir (default: output/graph)",
    )
    args = parser.parse_args()

    graph_dir = Path(args.graph)
    if not (graph_dir / "nodes.json").exists():
        print(f"ERROR: {graph_dir}/nodes.json not found. Run scripts/run_phase2.py first.")
        sys.exit(1)

    # Load .env
    env_path = Path(".env")
    if env_path.exists():
        load_dotenv(env_path)
        print(f"Loaded .env from {env_path.resolve()}")
    else:
        load_dotenv()

    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")

    print(f"\nConnecting to Neo4j at {uri} as {user!r}...")

    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
        print("Connected.\n")
    except Exception as exc:
        print(f"ERROR: Could not connect to Neo4j: {exc}")
        print(QUICKSTART)
        sys.exit(1)

    nodes = json.loads((graph_dir / "nodes.json").read_text(encoding="utf-8"))
    edges = json.loads((graph_dir / "edges.json").read_text(encoding="utf-8"))
    print(f"Graph: {len(nodes):,} nodes, {len(edges):,} edges\n")

    counts = load_graph(driver, nodes, edges)
    driver.close()

    print("\n--- Import complete ---")
    total_nodes = sum(v for k, v in counts.items() if k.startswith("nodes_"))
    total_edges = sum(v for k, v in counts.items() if k.startswith("edges_"))
    for k, v in sorted(counts.items()):
        print(f"  {k:<30s} {v:,}")
    print(f"  {'TOTAL nodes':<30s} {total_nodes:,}")
    print(f"  {'TOTAL edges':<30s} {total_edges:,}")

    print(CYPHER_EXAMPLES)


if __name__ == "__main__":
    main()
