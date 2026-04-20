"""Integration tests that validate the persisted opportunity graph output.

These tests read output/graph/nodes.json and output/graph/edges.json to
verify that the compiler correctly wrote YAML-sourced gaps, BLOCKED_BY edges,
and optional primitive fields.  Run after scripts/run_phase4.py.
"""

import json
import pytest
from pathlib import Path

NODES_PATH = Path("output/graph/nodes.json")
EDGES_PATH = Path("output/graph/edges.json")


@pytest.fixture(scope="module")
def graph_output():
    if not NODES_PATH.exists() or not EDGES_PATH.exists():
        pytest.skip("output/graph not found — run Phase 4 first")
    nodes = json.loads(NODES_PATH.read_text())
    edges = json.loads(EDGES_PATH.read_text())
    return nodes, edges


@pytest.fixture(scope="module")
def opp_nodes(graph_output):
    nodes, _ = graph_output
    return [n for n in nodes if n.get("properties", {}).get("graph_layer") == "opportunity"]


@pytest.fixture(scope="module")
def opp_edges(graph_output):
    _, edges = graph_output
    opp_types = {"ENABLES", "REQUIRES", "BLOCKED_BY", "COMPOSES_WITH", "PRIMITIVE_COVERS"}
    return [e for e in edges if e.get("edge_type") in opp_types]


# ------------------------------------------------------------------ #
# BUG 2 — YAML-sourced GapNodes written to graph                       #
# ------------------------------------------------------------------ #

def test_yaml_sourced_gap_nodes_exist(opp_nodes):
    """Compiler must emit GapNodes with source='yaml_research'."""
    yaml_gaps = [n for n in opp_nodes
                 if n["label"] == "GapNode"
                 and n["properties"].get("source") == "yaml_research"]
    assert len(yaml_gaps) >= 8, (
        f"Expected >= 8 yaml_research GapNodes, got {len(yaml_gaps)}"
    )


def test_yaml_gap_nodes_have_readable_names(opp_nodes):
    """YAML-sourced GapNodes must have human-readable names, not hash-based IDs."""
    for n in opp_nodes:
        if n["label"] == "GapNode" and n["properties"].get("source") == "yaml_research":
            name = n["properties"].get("name", "")
            # Should NOT start with "yaml_gap_" (the internal primitive_id)
            assert not name.startswith("yaml_gap_"), (
                f"GapNode {n['node_id']} has unreadable name: {name}"
            )
            assert len(name) > 5, f"GapNode {n['node_id']} has empty name"


def test_blocked_by_edges_exist_for_infeasible_initiatives(opp_nodes, opp_edges):
    """Every not_currently_feasible initiative must have >= 1 BLOCKED_BY edge."""
    initiative_nodes = {n["node_id"]: n for n in opp_nodes if n["label"] == "InitiativeNode"}
    blocked_by = [e for e in opp_edges if e["edge_type"] == "BLOCKED_BY"]
    inits_with_blocked_by = {e["source_node_id"] for e in blocked_by}

    for nid, n in initiative_nodes.items():
        if n["properties"].get("readiness") == "not_currently_feasible":
            assert nid in inits_with_blocked_by, (
                f"not_currently_feasible initiative "
                f"'{n['properties'].get('initiative_name', nid)}' "
                f"has no BLOCKED_BY edges"
            )


def test_total_blocked_by_edge_count(opp_edges):
    """Should have at least 12 BLOCKED_BY edges (4 primitive-maturity + 8+ YAML)."""
    blocked_by = [e for e in opp_edges if e["edge_type"] == "BLOCKED_BY"]
    assert len(blocked_by) >= 12, (
        f"Expected >= 12 BLOCKED_BY edges, got {len(blocked_by)}"
    )


# ------------------------------------------------------------------ #
# BUG 3 — optional_primitives fields written to InitiativeNode          #
# ------------------------------------------------------------------ #

def test_optional_primitives_available_on_multi_primitive_ready_now(opp_nodes):
    """Multi-primitive ready_now initiatives must have non-empty
    optional_primitives_available (since all 9 primitives are always extracted)."""
    ready_now = [n for n in opp_nodes
                 if n["label"] == "InitiativeNode"
                 and n["properties"].get("readiness") == "ready_now"]

    found_with_opt = False
    for n in ready_now:
        opt_a = n["properties"].get("optional_primitives_available", [])
        if len(opt_a) >= 1:
            found_with_opt = True
            break

    assert found_with_opt, (
        "No ready_now InitiativeNode has optional_primitives_available populated"
    )


def test_optional_primitives_fields_present_on_all_initiative_nodes(opp_nodes):
    """All InitiativeNodes must have both optional_primitives_* fields."""
    for n in opp_nodes:
        if n["label"] == "InitiativeNode":
            props = n["properties"]
            assert "optional_primitives_available" in props, (
                f"InitiativeNode {n['node_id']} missing optional_primitives_available"
            )
            assert "optional_primitives_missing" in props, (
                f"InitiativeNode {n['node_id']} missing optional_primitives_missing"
            )


# ------------------------------------------------------------------ #
# initiative_key field for sidebar lookups                             #
# ------------------------------------------------------------------ #

def test_initiative_nodes_have_initiative_key(opp_nodes):
    """All InitiativeNodes must have initiative_key (original string ID) for
    sidebar blocking initiative lookups."""
    for n in opp_nodes:
        if n["label"] == "InitiativeNode":
            key = n["properties"].get("initiative_key")
            assert key and isinstance(key, str), (
                f"InitiativeNode {n['node_id']} missing or invalid initiative_key"
            )
            # initiative_key should be the snake_case ID, not a hash
            assert not key.startswith("initiative_"), (
                f"initiative_key looks like a hash: {key}"
            )
