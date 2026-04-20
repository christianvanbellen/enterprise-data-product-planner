"""Tests for StructuralGraphCompiler."""

import pytest

from ingestion.contracts.asset import CanonicalAsset, CanonicalColumn, Provenance
from ingestion.contracts.bundle import CanonicalBundle
from ingestion.contracts.lineage import CanonicalLineageEdge
from ingestion.normalisation.hashing import stable_hash
from graph.compiler.structural import StructuralGraphCompiler
from graph.compiler.evidence import CONFIDENCE_EXPLICIT_DEP
from graph.schema.edges import EdgeType


def _prov() -> Provenance:
    return Provenance(source_system="test", source_type="Test")


def _asset(name: str) -> CanonicalAsset:
    return CanonicalAsset(
        internal_id=f"asset_{stable_hash('test', name)}",
        asset_type="dbt_model",
        name=name,
        normalized_name=name,
        schema_name="analytics",
        version_hash=stable_hash(name),
        provenance=_prov(),
    )


def _col(asset: CanonicalAsset, col_name: str, pos: int = 0) -> CanonicalColumn:
    return CanonicalColumn(
        internal_id=f"col_{stable_hash(asset.internal_id, col_name)}",
        asset_internal_id=asset.internal_id,
        name=col_name,
        normalized_name=col_name,
        data_type_family="string",
        column_role="attribute",
        ordinal_position=pos,
        version_hash=stable_hash(asset.internal_id, col_name),
        provenance=_prov(),
    )


def _edge(src: CanonicalAsset, tgt: CanonicalAsset) -> CanonicalLineageEdge:
    return CanonicalLineageEdge(
        internal_id=f"lin_{stable_hash(src.internal_id, tgt.internal_id)}",
        source_asset_id=src.internal_id,
        target_asset_id=tgt.internal_id,
        relation_type="depends_on",
        derivation_method="explicit_metadata",
        confidence=1.0,
        version_hash=stable_hash(src.internal_id, tgt.internal_id),
        provenance=_prov(),
    )


@pytest.fixture
def minimal_bundle():
    a1 = _asset("model_a")
    a2 = _asset("model_b")
    a3 = _asset("model_c")

    cols = [
        _col(a1, "id", 0), _col(a1, "name", 1),
        _col(a2, "id", 0), _col(a2, "value", 1),
        _col(a3, "id", 0), _col(a3, "desc", 1),
    ]
    edges = [
        _edge(a1, a2),
        _edge(a2, a3),
    ]
    return CanonicalBundle(
        assets=[a1, a2, a3],
        columns=cols,
        lineage_edges=edges,
        business_terms=[],
        metadata={"source": "test"},
    )


# ------------------------------------------------------------------ #
# Node counts                                                           #
# ------------------------------------------------------------------ #

def test_asset_node_count(minimal_bundle):
    compiler = StructuralGraphCompiler()
    nodes, edges, artifact = compiler.compile(minimal_bundle, build_id="test_build")
    assert artifact.node_counts.get("Asset", 0) == 3


def test_column_node_count(minimal_bundle):
    compiler = StructuralGraphCompiler()
    nodes, edges, artifact = compiler.compile(minimal_bundle, build_id="test_build")
    assert artifact.node_counts.get("Column", 0) == 6


def test_doc_node_count(minimal_bundle):
    compiler = StructuralGraphCompiler()
    nodes, edges, artifact = compiler.compile(minimal_bundle, build_id="test_build")
    assert artifact.node_counts.get("DocObject", 0) == 3


# ------------------------------------------------------------------ #
# DEPENDS_ON edge confidence                                            #
# ------------------------------------------------------------------ #

def test_depends_on_edges_have_correct_confidence(minimal_bundle):
    compiler = StructuralGraphCompiler()
    nodes, edges, artifact = compiler.compile(minimal_bundle, build_id="test_build")
    dep_edges = [e for e in edges if e.edge_type == EdgeType.DEPENDS_ON]
    assert len(dep_edges) == 2
    for e in dep_edges:
        assert e.properties["confidence"] == CONFIDENCE_EXPLICIT_DEP == 1.0


# ------------------------------------------------------------------ #
# Determinism                                                           #
# ------------------------------------------------------------------ #

def test_two_runs_produce_identical_ids(minimal_bundle):
    compiler = StructuralGraphCompiler()
    nodes1, edges1, artifact1 = compiler.compile(minimal_bundle, build_id="fixed_build")
    nodes2, edges2, artifact2 = compiler.compile(minimal_bundle, build_id="fixed_build")

    node_ids1 = {n.node_id for n in nodes1}
    node_ids2 = {n.node_id for n in nodes2}
    assert node_ids1 == node_ids2

    edge_ids1 = {e.edge_id for e in edges1}
    edge_ids2 = {e.edge_id for e in edges2}
    assert edge_ids1 == edge_ids2


# ------------------------------------------------------------------ #
# lineage_coverage_pct                                                  #
# ------------------------------------------------------------------ #

def test_lineage_coverage_correct(minimal_bundle):
    compiler = StructuralGraphCompiler()
    _, _, artifact = compiler.compile(minimal_bundle, build_id="test_build")
    # a1, a2, a3 all appear in lineage edges → coverage should be 1.0 (3/3)
    assert artifact.lineage_coverage_pct == 1.0


def test_lineage_coverage_with_isolated_asset():
    a1 = _asset("root")
    a2 = _asset("mid")
    a3 = _asset("isolated")  # not in any edge
    bundle = CanonicalBundle(
        assets=[a1, a2, a3],
        columns=[_col(a1, "id"), _col(a2, "id"), _col(a3, "id")],
        lineage_edges=[_edge(a1, a2)],
    )
    compiler = StructuralGraphCompiler()
    _, _, artifact = compiler.compile(bundle, build_id="test_build")
    # a1 and a2 are in edges (2 out of 3 assets)
    assert artifact.lineage_coverage_pct == pytest.approx(2 / 3, abs=0.01)


# ------------------------------------------------------------------ #
# All edges carry build_id                                              #
# ------------------------------------------------------------------ #

def test_all_edges_have_build_id(minimal_bundle):
    compiler = StructuralGraphCompiler()
    nodes, edges, artifact = compiler.compile(minimal_bundle, build_id="explicit_build")
    for e in edges:
        assert e.build_id == "explicit_build", f"Edge {e.edge_id} missing build_id"


def test_all_nodes_have_build_id(minimal_bundle):
    compiler = StructuralGraphCompiler()
    nodes, edges, artifact = compiler.compile(minimal_bundle, build_id="explicit_build")
    for n in nodes:
        assert n.build_id == "explicit_build", f"Node {n.node_id} missing build_id"
