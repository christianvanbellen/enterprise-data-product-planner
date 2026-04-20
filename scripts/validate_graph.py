"""validate_graph.py — 7 structural correctness checks on the compiled graph.

Usage:
    python scripts/validate_graph.py --graph output/graph/
    python scripts/validate_graph.py --graph output/graph/ --bundle output/bundle.json
"""

import argparse
import json
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #

def load_graph(graph_dir: Path):
    nodes = json.loads((graph_dir / "nodes.json").read_text(encoding="utf-8"))
    edges = json.loads((graph_dir / "edges.json").read_text(encoding="utf-8"))
    return nodes, edges


def load_bundle(bundle_path: Path):
    return json.loads(bundle_path.read_text(encoding="utf-8"))


def fmt_pass(label: str) -> str:
    return f"  [PASS] {label}"


def fmt_fail(label: str, detail: str) -> str:
    return f"  [FAIL] {label}\n         {detail}"


# ------------------------------------------------------------------ #
# Individual checks (return (passed: bool, detail: str))              #
# ------------------------------------------------------------------ #

def check_node_count_consistency(nodes, edges, bundle=None):
    """Check 1: assets with zero HAS_COLUMN edges ↔ zero-column assets in bundle."""
    asset_nodes = [n for n in nodes if n.get("label") == "Asset"]
    has_col_edges = [e for e in edges if e["edge_type"] == "HAS_COLUMN"]

    col_count = defaultdict(int)
    for e in has_col_edges:
        col_count[e["source_node_id"]] += 1

    zero_col_asset_ids = {n["node_id"] for n in asset_nodes if col_count[n["node_id"]] == 0}

    if bundle is not None:
        bundle_assets = bundle.get("assets", [])
        bundle_zero = {
            a["internal_id"] for a in bundle_assets
            if len(a.get("columns", [])) == 0
        }
        # Compare using bundle columns field — if missing, derive from column records
        bundle_col_assets = set()
        for col in bundle.get("columns", []):
            bundle_col_assets.add(col["asset_internal_id"])
        bundle_zero_derived = {
            a["internal_id"] for a in bundle_assets
            if a["internal_id"] not in bundle_col_assets
        }
        if zero_col_asset_ids != bundle_zero_derived:
            mismatch = len(zero_col_asset_ids.symmetric_difference(bundle_zero_derived))
            return False, (
                f"Graph has {len(zero_col_asset_ids)} zero-column assets; "
                f"bundle has {len(bundle_zero_derived)} zero-column assets; "
                f"{mismatch} differ"
            )
        return True, (
            f"{len(zero_col_asset_ids)} zero-column assets in graph match bundle"
        )
    else:
        return True, (
            f"{len(zero_col_asset_ids)} assets have zero HAS_COLUMN edges "
            f"(no bundle provided for cross-check)"
        )


def check_edge_referential_integrity(nodes, edges):
    """Check 2: no dangling source/target IDs (excluding CONTAINS schema refs)."""
    node_ids = {n["node_id"] for n in nodes}
    dangling = []
    for e in edges:
        if e["edge_type"] == "CONTAINS":
            # Schema nodes (schema_*) are virtual — not persisted as graph nodes
            continue
        src_ok = e["source_node_id"] in node_ids
        tgt_ok = e["target_node_id"] in node_ids
        if not src_ok or not tgt_ok:
            dangling.append(
                f"{e['edge_type']}:{e['edge_id']} "
                f"src={'OK' if src_ok else 'MISSING'} "
                f"tgt={'OK' if tgt_ok else 'MISSING'}"
            )
    if dangling:
        return False, f"{len(dangling)} dangling edges: {dangling[:3]}"
    return True, "All non-CONTAINS edges reference existing nodes"


def check_depends_on_confidence(nodes, edges):
    """Check 3: all DEPENDS_ON edges have confidence==1.0 and derivation_method==explicit_metadata."""
    dep_edges = [e for e in edges if e["edge_type"] == "DEPENDS_ON"]
    if not dep_edges:
        return False, "No DEPENDS_ON edges found"

    bad_confidence = [
        e["edge_id"] for e in dep_edges
        if e.get("properties", {}).get("confidence") != 1.0
    ]

    bad_derivation = []
    for e in dep_edges:
        sources = e.get("evidence", {}).get("evidence_sources", [])
        methods = [s["value"] for s in sources if s.get("type") == "derivation_method"]
        if "explicit_metadata" not in methods:
            bad_derivation.append(e["edge_id"])

    if bad_confidence or bad_derivation:
        parts = []
        if bad_confidence:
            parts.append(f"{len(bad_confidence)} edges with confidence≠1.0")
        if bad_derivation:
            parts.append(f"{len(bad_derivation)} edges missing explicit_metadata derivation")
        return False, "; ".join(parts)

    return True, (
        f"All {len(dep_edges)} DEPENDS_ON edges: confidence=1.0, "
        f"derivation_method=explicit_metadata"
    )


def check_evidence_completeness(nodes, edges):
    """Check 4: every node and edge has required evidence fields."""
    REQUIRED_NODE = {"created_by", "rule_id", "confidence", "review_status", "build_id"}
    REQUIRED_EDGE = {"created_by", "rule_id", "confidence", "review_status", "build_id"}

    node_violations = []
    for n in nodes:
        if n.get("label") == "_BuildMeta":
            continue  # internal tracking node
        ev = n.get("evidence", {})
        missing = REQUIRED_NODE - ev.keys()
        if missing:
            node_violations.append(f"{n['node_id']}: missing {missing}")

    edge_violations = []
    for e in edges:
        ev = e.get("evidence", {})
        missing = REQUIRED_EDGE - ev.keys()
        if missing:
            edge_violations.append(f"{e['edge_id']}: missing {missing}")

    total = len(node_violations) + len(edge_violations)
    if total:
        sample = (node_violations + edge_violations)[:3]
        return False, f"{total} evidence gaps: {sample}"

    return True, (
        f"All {len(nodes)} nodes and {len(edges)} edges have complete evidence"
    )


def check_build_id_consistency(nodes, edges):
    """Check 5: all nodes and edges share the same build_id."""
    build_ids = set()
    for n in nodes:
        if n.get("label") == "_BuildMeta":
            continue
        bid = n.get("build_id")
        if bid:
            build_ids.add(bid)
    for e in edges:
        bid = e.get("build_id")
        if bid:
            build_ids.add(bid)

    if len(build_ids) != 1:
        return False, f"Found {len(build_ids)} distinct build_ids: {build_ids}"

    build_id = next(iter(build_ids))
    return True, f"All nodes and edges share build_id={build_id!r}"


def check_determinism(bundle_path: Path, tmp_dir: Path):
    """Check 6: run pipeline twice, confirm identical node/edge IDs and version_hashes."""
    out1 = tmp_dir / "run1"
    out2 = tmp_dir / "run2"

    def run_phase2(out: Path):
        result = subprocess.run(
            [
                sys.executable, "scripts/run_phase2.py",
                "--bundle", str(bundle_path),
                "--store", "json",
                "--output", str(out),
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"run_phase2 failed:\n{result.stderr}")
        return load_graph(out)

    nodes1, edges1 = run_phase2(out1)
    nodes2, edges2 = run_phase2(out2)

    # Exclude _BuildMeta nodes — their IDs embed the per-run build_id by design
    ids1 = {n["node_id"] for n in nodes1 if n.get("label") != "_BuildMeta"}
    ids2 = {n["node_id"] for n in nodes2 if n.get("label") != "_BuildMeta"}
    edge_ids1 = {e["edge_id"] for e in edges1}
    edge_ids2 = {e["edge_id"] for e in edges2}

    vh1 = {n["node_id"]: n.get("properties", {}).get("version_hash") for n in nodes1 if n.get("label") != "_BuildMeta"}
    vh2 = {n["node_id"]: n.get("properties", {}).get("version_hash") for n in nodes2 if n.get("label") != "_BuildMeta"}

    if ids1 != ids2:
        diff = ids1.symmetric_difference(ids2)
        return False, f"Node IDs differ across runs: {len(diff)} differences"
    if edge_ids1 != edge_ids2:
        diff = edge_ids1.symmetric_difference(edge_ids2)
        return False, f"Edge IDs differ across runs: {len(diff)} differences"
    vh_mismatches = {k for k in vh1 if vh1[k] != vh2.get(k)}
    if vh_mismatches:
        return False, f"{len(vh_mismatches)} version_hash mismatches across runs"

    return True, (
        f"{len(ids1)} node IDs, {len(edge_ids1)} edge IDs, "
        f"version_hashes all stable across two runs"
    )


def check_lineage_completeness(nodes, edges):
    """Check 7: DEPENDS_ON edges reference valid nodes; count isolated assets."""
    asset_nodes = [n for n in nodes if n.get("label") == "Asset"]
    node_ids = {n["node_id"] for n in nodes}
    dep_edges = [e for e in edges if e["edge_type"] == "DEPENDS_ON"]

    dangling_dep = [
        e["edge_id"] for e in dep_edges
        if e["source_node_id"] not in node_ids or e["target_node_id"] not in node_ids
    ]

    has_upstream = {e["source_node_id"] for e in dep_edges}
    has_downstream = {e["target_node_id"] for e in dep_edges}
    isolated = [
        n for n in asset_nodes
        if n["node_id"] not in has_upstream and n["node_id"] not in has_downstream
    ]

    if dangling_dep:
        return False, (
            f"{len(dangling_dep)} DEPENDS_ON edges reference missing nodes"
        )

    return True, (
        f"{len(dep_edges)} DEPENDS_ON edges all reference valid nodes; "
        f"{len(isolated)} isolated assets (no upstream or downstream)"
    )


# ------------------------------------------------------------------ #
# Reusable summary helpers (also used in tests)                       #
# ------------------------------------------------------------------ #

def run_all_checks(nodes, edges, bundle=None, bundle_path=None, tmp_dir=None):
    """Run all 7 checks. Returns list of (name, passed, detail)."""
    import tempfile

    results = []

    r = check_node_count_consistency(nodes, edges, bundle)
    results.append(("1. Node count consistency", r[0], r[1]))

    r = check_edge_referential_integrity(nodes, edges)
    results.append(("2. Edge referential integrity", r[0], r[1]))

    r = check_depends_on_confidence(nodes, edges)
    results.append(("3. DEPENDS_ON confidence + derivation", r[0], r[1]))

    r = check_evidence_completeness(nodes, edges)
    results.append(("4. Evidence completeness", r[0], r[1]))

    r = check_build_id_consistency(nodes, edges)
    results.append(("5. Build ID consistency", r[0], r[1]))

    if bundle_path is not None:
        if tmp_dir is None:
            tmp_dir = Path(tempfile.mkdtemp())
        r = check_determinism(bundle_path, tmp_dir)
        results.append(("6. Determinism (two-run comparison)", r[0], r[1]))
    else:
        results.append(("6. Determinism (two-run comparison)", None, "skipped — no bundle path"))

    r = check_lineage_completeness(nodes, edges)
    results.append(("7. Lineage completeness", r[0], r[1]))

    return results


# ------------------------------------------------------------------ #
# CLI                                                                  #
# ------------------------------------------------------------------ #

def main():
    parser = argparse.ArgumentParser(description="Validate compiled graph correctness.")
    parser.add_argument("--graph", required=True, help="Path to graph output dir (nodes.json + edges.json)")
    parser.add_argument("--bundle", default=None, help="Path to bundle.json (enables cross-checks + determinism check)")
    args = parser.parse_args()

    graph_dir = Path(args.graph)
    bundle_path = Path(args.bundle) if args.bundle else None

    nodes, edges = load_graph(graph_dir)
    bundle = load_bundle(bundle_path) if bundle_path else None

    print(f"\nValidating graph: {graph_dir}")
    print(f"  {len(nodes)} nodes, {len(edges)} edges\n")

    results = run_all_checks(nodes, edges, bundle=bundle, bundle_path=bundle_path)

    passed = sum(1 for _, ok, _ in results if ok is True)
    failed = sum(1 for _, ok, _ in results if ok is False)
    skipped = sum(1 for _, ok, _ in results if ok is None)

    for name, ok, detail in results:
        if ok is True:
            print(fmt_pass(f"{name}: {detail}"))
        elif ok is False:
            print(fmt_fail(name, detail))
        else:
            print(f"  [SKIP] {name}: {detail}")

    print(f"\nChecks passed: {passed}/7  |  Checks failed: {failed}/7  |  Skipped: {skipped}/7")

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
