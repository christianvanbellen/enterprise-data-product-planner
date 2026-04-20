"""Tests for validate_graph.py check functions."""

import sys
from pathlib import Path

import pytest

# Add scripts/ to path so we can import validate_graph
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))
from validate_graph import (
    check_build_id_consistency,
    check_depends_on_confidence,
    check_edge_referential_integrity,
    check_evidence_completeness,
    check_lineage_completeness,
    check_node_count_consistency,
)


# ------------------------------------------------------------------ #
# Minimal graph fixtures                                               #
# ------------------------------------------------------------------ #

EVIDENCE_OK = {
    "build_id": "build_abc",
    "confidence": 1.0,
    "created_by": "test",
    "review_status": "auto",
    "rule_id": "test.rule",
}


def _asset(node_id: str, build_id: str = "build_abc") -> dict:
    return {
        "node_id": node_id,
        "label": "Asset",
        "build_id": build_id,
        "evidence": {**EVIDENCE_OK, "build_id": build_id},
        "properties": {"name": node_id, "grain_keys": [], "domain_candidates": []},
    }


def _column(node_id: str, asset_id: str, build_id: str = "build_abc") -> dict:
    return {
        "node_id": node_id,
        "label": "Column",
        "build_id": build_id,
        "evidence": {**EVIDENCE_OK, "build_id": build_id},
        "properties": {},
    }


def _has_col(edge_id: str, src: str, tgt: str, build_id: str = "build_abc") -> dict:
    return {
        "edge_id": edge_id,
        "edge_type": "HAS_COLUMN",
        "source_node_id": src,
        "target_node_id": tgt,
        "build_id": build_id,
        "evidence": {**EVIDENCE_OK, "build_id": build_id},
        "properties": {},
    }


def _depends_on(edge_id: str, src: str, tgt: str, confidence: float = 1.0) -> dict:
    return {
        "edge_id": edge_id,
        "edge_type": "DEPENDS_ON",
        "source_node_id": src,
        "target_node_id": tgt,
        "build_id": "build_abc",
        "evidence": {
            **EVIDENCE_OK,
            "evidence_sources": [
                {"type": "derivation_method", "value": "explicit_metadata"}
            ],
        },
        "properties": {"confidence": confidence},
    }


# ------------------------------------------------------------------ #
# Check 1: node count consistency                                      #
# ------------------------------------------------------------------ #

def test_check1_pass_with_no_bundle():
    nodes = [_asset("asset_a"), _asset("asset_b")]
    edges = [_has_col("e1", "asset_a", "col_x")]
    passed, detail = check_node_count_consistency(nodes, edges, bundle=None)
    assert passed is True
    assert "1 assets have zero HAS_COLUMN edges" in detail


def test_check1_pass_with_matching_bundle():
    nodes = [_asset("asset_a"), _asset("asset_b")]
    edges = [_has_col("e1", "asset_a", "col_x")]
    bundle = {
        "assets": [
            {"internal_id": "asset_a"},
            {"internal_id": "asset_b"},
        ],
        "columns": [
            {"asset_internal_id": "asset_a", "internal_id": "col_x"},
        ],
    }
    passed, detail = check_node_count_consistency(nodes, edges, bundle=bundle)
    assert passed is True


def test_check1_fail_mismatch():
    nodes = [_asset("asset_a"), _asset("asset_b")]
    edges = [_has_col("e1", "asset_a", "col_x")]
    # Bundle says asset_b has a column, but graph has no HAS_COLUMN for it
    bundle = {
        "assets": [
            {"internal_id": "asset_a"},
            {"internal_id": "asset_b"},
        ],
        "columns": [
            {"asset_internal_id": "asset_a", "internal_id": "col_x"},
            {"asset_internal_id": "asset_b", "internal_id": "col_y"},
        ],
    }
    passed, detail = check_node_count_consistency(nodes, edges, bundle=bundle)
    assert passed is False


# ------------------------------------------------------------------ #
# Check 2: edge referential integrity                                  #
# ------------------------------------------------------------------ #

def test_check2_pass_all_valid():
    nodes = [_asset("asset_a"), _column("col_x", "asset_a")]
    edges = [_has_col("e1", "asset_a", "col_x")]
    passed, detail = check_edge_referential_integrity(nodes, edges)
    assert passed is True


def test_check2_fail_dangling_target():
    nodes = [_asset("asset_a")]
    edges = [_has_col("e1", "asset_a", "col_missing")]
    passed, detail = check_edge_referential_integrity(nodes, edges)
    assert passed is False
    assert "dangling" in detail


def test_check2_contains_edges_excluded():
    """CONTAINS edges referencing virtual schema nodes must be ignored."""
    nodes = [_asset("asset_a")]
    contains_edge = {
        "edge_id": "e_contains",
        "edge_type": "CONTAINS",
        "source_node_id": "schema_virtual",
        "target_node_id": "asset_a",
        "build_id": "build_abc",
        "evidence": EVIDENCE_OK,
        "properties": {},
    }
    passed, detail = check_edge_referential_integrity(nodes, [contains_edge])
    assert passed is True


# ------------------------------------------------------------------ #
# Check 3: DEPENDS_ON confidence + derivation                         #
# ------------------------------------------------------------------ #

def test_check3_pass():
    nodes = [_asset("asset_a"), _asset("asset_b")]
    edges = [_depends_on("e1", "asset_a", "asset_b", confidence=1.0)]
    passed, detail = check_depends_on_confidence(nodes, edges)
    assert passed is True
    assert "1 DEPENDS_ON edges" in detail


def test_check3_fail_confidence_not_1():
    nodes = [_asset("asset_a"), _asset("asset_b")]
    edge = _depends_on("e1", "asset_a", "asset_b", confidence=0.8)
    passed, detail = check_depends_on_confidence(nodes, [edge])
    assert passed is False
    assert "confidence" in detail


def test_check3_fail_missing_derivation():
    nodes = [_asset("asset_a"), _asset("asset_b")]
    edge = _depends_on("e1", "asset_a", "asset_b")
    edge["evidence"]["evidence_sources"] = []  # remove derivation_method
    passed, detail = check_depends_on_confidence(nodes, [edge])
    assert passed is False
    assert "explicit_metadata" in detail


def test_check3_no_depends_on_edges():
    nodes = [_asset("asset_a")]
    edges = []
    passed, detail = check_depends_on_confidence(nodes, edges)
    assert passed is False
    assert "No DEPENDS_ON edges" in detail


# ------------------------------------------------------------------ #
# Check 4: evidence completeness                                       #
# ------------------------------------------------------------------ #

def test_check4_pass():
    nodes = [_asset("asset_a")]
    edges = [_has_col("e1", "asset_a", "col_x")]
    passed, detail = check_evidence_completeness(nodes, edges)
    assert passed is True


def test_check4_fail_missing_field():
    node = _asset("asset_a")
    del node["evidence"]["rule_id"]
    passed, detail = check_evidence_completeness([node], [])
    assert passed is False
    assert "evidence gaps" in detail


def test_check4_build_meta_skipped():
    """_BuildMeta nodes must be skipped even if evidence is incomplete."""
    build_meta = {
        "node_id": "_build_xyz",
        "label": "_BuildMeta",
        "build_id": "build_abc",
        "evidence": {},
        "properties": {},
    }
    passed, detail = check_evidence_completeness([build_meta], [])
    assert passed is True


# ------------------------------------------------------------------ #
# Check 5: build ID consistency                                        #
# ------------------------------------------------------------------ #

def test_check5_pass_single_build_id():
    nodes = [_asset("asset_a"), _asset("asset_b")]
    edges = [_has_col("e1", "asset_a", "col_x")]
    passed, detail = check_build_id_consistency(nodes, edges)
    assert passed is True
    assert "build_abc" in detail


def test_check5_fail_two_build_ids():
    nodes = [_asset("asset_a", build_id="build_1"), _asset("asset_b", build_id="build_2")]
    edges = []
    passed, detail = check_build_id_consistency(nodes, edges)
    assert passed is False
    assert "2 distinct build_ids" in detail


# ------------------------------------------------------------------ #
# Check 7: lineage completeness                                        #
# ------------------------------------------------------------------ #

def test_check7_pass_no_dangling():
    nodes = [_asset("asset_a"), _asset("asset_b")]
    edges = [_depends_on("e1", "asset_a", "asset_b")]
    passed, detail = check_lineage_completeness(nodes, edges)
    assert passed is True
    assert "1 DEPENDS_ON edges" in detail


def test_check7_isolated_count():
    """Isolated assets (no upstream, no downstream) should be counted correctly."""
    nodes = [_asset("asset_a"), _asset("asset_b"), _asset("asset_c")]
    edges = [_depends_on("e1", "asset_a", "asset_b")]
    passed, detail = check_lineage_completeness(nodes, edges)
    assert passed is True
    assert "1 isolated" in detail  # asset_c is isolated


def test_check7_fail_dangling_depends_on():
    nodes = [_asset("asset_a")]
    edges = [_depends_on("e1", "asset_a", "asset_missing")]
    passed, detail = check_lineage_completeness(nodes, edges)
    assert passed is False
    assert "1 DEPENDS_ON edges reference missing nodes" in detail
