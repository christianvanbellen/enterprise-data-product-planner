"""query_graph.py — 4 analytical queries over the compiled graph.

Usage:
    python scripts/query_graph.py --graph output/graph/
"""

import argparse
import json
from collections import defaultdict
from itertools import combinations
from pathlib import Path


# ------------------------------------------------------------------ #
# BFS lineage traversal                                               #
# ------------------------------------------------------------------ #

def bfs_lineage(node_id: str, edges, nodes_by_id: dict, direction: str, depth: int):
    """Return list of (hop, node) reachable from node_id within depth hops.

    direction='upstream'   follows DEPENDS_ON edges: source → target
    direction='downstream' follows DEPENDS_ON edges: target → source
    """
    dep_edges = [e for e in edges if e["edge_type"] == "DEPENDS_ON"]
    visited = {node_id}
    frontier = [node_id]
    result = []

    for hop in range(1, depth + 1):
        next_frontier = []
        for current in frontier:
            for e in dep_edges:
                if direction == "upstream" and e["source_node_id"] == current:
                    nbr = e["target_node_id"]
                elif direction == "downstream" and e["target_node_id"] == current:
                    nbr = e["source_node_id"]
                else:
                    continue
                if nbr not in visited:
                    visited.add(nbr)
                    next_frontier.append(nbr)
                    if nbr in nodes_by_id:
                        result.append((hop, nodes_by_id[nbr]))
        frontier = next_frontier
        if not frontier:
            break

    return result


# ------------------------------------------------------------------ #
# Query 1: Upstream lineage of widest mart asset                      #
# ------------------------------------------------------------------ #

def query_upstream_lineage(nodes, edges):
    """Find the asset with the most columns and walk its upstream lineage."""
    has_col = [e for e in edges if e["edge_type"] == "HAS_COLUMN"]
    asset_nodes = [n for n in nodes if n.get("label") == "Asset"]
    nodes_by_id = {n["node_id"]: n for n in nodes}

    col_count = defaultdict(int)
    for e in has_col:
        col_count[e["source_node_id"]] += 1

    widest = max(asset_nodes, key=lambda n: col_count[n["node_id"]])
    name = widest["properties"]["name"]
    ncols = col_count[widest["node_id"]]

    hops = bfs_lineage(widest["node_id"], edges, nodes_by_id, direction="upstream", depth=10)
    hop_map = defaultdict(list)
    for hop, node in hops:
        hop_map[hop].append(node["properties"]["name"])

    print(f"\n=== Query 1: Upstream Lineage of Widest Asset ===")
    print(f"Asset: {name!r}  ({ncols} columns)")
    if not hop_map:
        print("  (no upstream dependencies)")
    else:
        for h in sorted(hop_map):
            deps = hop_map[h]
            preview = ", ".join(deps[:3])
            suffix = f" … +{len(deps)-3} more" if len(deps) > 3 else ""
            print(f"  hop {h}: {len(deps)} upstream  [{preview}{suffix}]")
    print(f"  Total reachable upstream: {len(hops)}")


# ------------------------------------------------------------------ #
# Query 2: Downstream fan-out from hx_raw by hop depth               #
# ------------------------------------------------------------------ #

def query_downstream_fanout(nodes, edges):
    """Walk downstream consumers of hx_raw, showing fan-out per hop depth."""
    nodes_by_id = {n["node_id"]: n for n in nodes}
    asset_nodes = [n for n in nodes if n.get("label") == "Asset"]

    # Find hx_raw
    hx_raw = next(
        (n for n in asset_nodes if n["properties"].get("name") == "hx_raw"),
        None,
    )
    if hx_raw is None:
        print("\n=== Query 2: Downstream Fan-out from hx_raw ===")
        print("  (asset 'hx_raw' not found — searching for closest match)")
        hx_raw = next(
            (n for n in asset_nodes if "hx_raw" in n["properties"].get("name", "")),
            None,
        )
        if hx_raw is None:
            print("  No hx_raw-like asset found.")
            return

    name = hx_raw["properties"]["name"]
    hops = bfs_lineage(hx_raw["node_id"], edges, nodes_by_id, direction="downstream", depth=10)
    hop_map = defaultdict(list)
    for hop, node in hops:
        hop_map[hop].append(node["properties"]["name"])

    print(f"\n=== Query 2: Downstream Fan-out from {name!r} ===")
    if not hop_map:
        print("  (no downstream consumers)")
    else:
        for h in sorted(hop_map):
            consumers = hop_map[h]
            preview = ", ".join(consumers[:4])
            suffix = f" … +{len(consumers)-4} more" if len(consumers) > 4 else ""
            print(f"  hop {h}: {len(consumers):3d} consumers  [{preview}{suffix}]")
    print(f"  Total reachable downstream: {len(hops)}")


# ------------------------------------------------------------------ #
# Query 3: Domain clustering — top 5 assets per domain by col count  #
# ------------------------------------------------------------------ #

def query_domain_clustering(nodes, edges):
    """Show top 5 assets per domain ranked by column count."""
    has_col = [e for e in edges if e["edge_type"] == "HAS_COLUMN"]
    asset_nodes = [n for n in nodes if n.get("label") == "Asset"]

    col_count = defaultdict(int)
    for e in has_col:
        col_count[e["source_node_id"]] += 1

    domain_assets = defaultdict(list)
    for n in asset_nodes:
        doms = n["properties"].get("domain_candidates", [])
        for d in doms:
            domain_assets[d].append((n["properties"]["name"], col_count[n["node_id"]]))

    print("\n=== Query 3: Domain Clustering (top 5 assets per domain by column count) ===")
    for domain in sorted(domain_assets):
        ranked = sorted(domain_assets[domain], key=lambda x: -x[1])[:5]
        print(f"\n  {domain!r} ({len(domain_assets[domain])} assets total):")
        for name, ncols in ranked:
            print(f"    {name:<55s} {ncols:4d} cols")


# ------------------------------------------------------------------ #
# Query 4: Grain key co-occurrence pairs                              #
# ------------------------------------------------------------------ #

def query_grain_key_cooccurrence(nodes, edges, min_shared: int = 2):
    """Find assets that share >= min_shared grain keys."""
    asset_nodes = [n for n in nodes if n.get("label") == "Asset"]

    grain_key_assets: dict[str, set] = defaultdict(set)
    asset_grain_keys: dict[str, list] = {}
    for n in asset_nodes:
        gks = n["properties"].get("grain_keys", [])
        if gks:
            asset_grain_keys[n["node_id"]] = gks
            for gk in gks:
                grain_key_assets[gk].add(n["node_id"])

    # Find pairs sharing >= min_shared grain keys
    # Build: asset_id → set of grain keys
    asset_gk_set = {aid: set(gks) for aid, gks in asset_grain_keys.items()}
    assets_with_gks = list(asset_gk_set.keys())
    nodes_by_id = {n["node_id"]: n for n in asset_nodes}

    pairs = []
    for a, b in combinations(assets_with_gks, 2):
        shared = asset_gk_set[a] & asset_gk_set[b]
        if len(shared) >= min_shared:
            pairs.append((
                nodes_by_id[a]["properties"]["name"],
                nodes_by_id[b]["properties"]["name"],
                sorted(shared),
            ))

    pairs.sort(key=lambda x: -len(x[2]))

    print(f"\n=== Query 4: Grain Key Co-occurrence Pairs (sharing >={min_shared} grain keys) ===")
    if not pairs:
        print(f"  No asset pairs share >={min_shared} grain keys.")
    else:
        print(f"  {len(pairs)} pairs found. Top 15:")
        for name_a, name_b, shared in pairs[:15]:
            shared_str = ", ".join(shared)
            print(f"  [{shared_str}]")
            print(f"    {name_a}")
            print(f"    {name_b}")

    # Summary: grain key distribution
    print(f"\n  Grain key coverage: {len(asset_grain_keys)} assets have at least one grain key")
    for gk, asset_set in sorted(grain_key_assets.items(), key=lambda x: -len(x[1])):
        print(f"    {gk:<25s} {len(asset_set):4d} assets")


# ------------------------------------------------------------------ #
# CLI                                                                  #
# ------------------------------------------------------------------ #

def main():
    parser = argparse.ArgumentParser(description="Analytical queries over the compiled graph.")
    parser.add_argument("--graph", required=True, help="Path to graph output dir (nodes.json + edges.json)")
    args = parser.parse_args()

    graph_dir = Path(args.graph)
    nodes = json.loads((graph_dir / "nodes.json").read_text(encoding="utf-8"))
    edges = json.loads((graph_dir / "edges.json").read_text(encoding="utf-8"))

    asset_nodes = [n for n in nodes if n.get("label") == "Asset"]
    dep_edges = [e for e in edges if e["edge_type"] == "DEPENDS_ON"]
    print(f"Graph loaded: {len(asset_nodes)} asset nodes, {len(dep_edges)} DEPENDS_ON edges")

    query_upstream_lineage(nodes, edges)
    query_downstream_fanout(nodes, edges)
    query_domain_clustering(nodes, edges)
    query_grain_key_cooccurrence(nodes, edges)


if __name__ == "__main__":
    main()
