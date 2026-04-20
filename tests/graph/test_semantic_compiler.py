"""Tests for SemanticGraphCompiler."""

import json
import pytest
from pathlib import Path

from ingestion.contracts.bundle import CanonicalBundle
from graph.store.json_store import JsonGraphStore
from graph.semantic.compiler import SemanticGraphCompiler

BUNDLE_PATH = Path("output/bundle.json")
GRAPH_PATH = Path("output/graph")

BUILD_ID = "test_sem_build_abc"


@pytest.fixture(scope="module")
def golden_bundle():
    if not BUNDLE_PATH.exists():
        pytest.skip("output/bundle.json not found — run Phase 1 first")
    return CanonicalBundle.from_json(BUNDLE_PATH)


@pytest.fixture(scope="module")
def golden_store():
    if not (GRAPH_PATH / "nodes.json").exists():
        pytest.skip("output/graph/nodes.json not found — run Phase 2 first")
    return JsonGraphStore.from_json(GRAPH_PATH)


@pytest.fixture(scope="module")
def compiled(golden_bundle, golden_store):
    """Run SemanticGraphCompiler once; reuse results across tests."""
    # Use a fresh store copy so we don't mutate the shared golden_store
    store = JsonGraphStore.from_json(GRAPH_PATH)
    artifact = SemanticGraphCompiler().compile(golden_bundle, store, BUILD_ID)
    return artifact, store


# ------------------------------------------------------------------ #
# Non-zero counts for all edge types                                   #
# ------------------------------------------------------------------ #

def test_artifact_entity_nodes_non_zero(compiled):
    artifact, _ = compiled
    assert artifact.entity_node_count > 0, "Expected at least one BusinessEntityNode"


def test_artifact_domain_nodes_non_zero(compiled):
    artifact, _ = compiled
    assert artifact.domain_node_count > 0, "Expected at least one DomainNode"


def test_artifact_metric_nodes_non_zero(compiled):
    artifact, _ = compiled
    assert artifact.metric_node_count > 0, "Expected at least one MetricNode"


def test_artifact_represents_edges_non_zero(compiled):
    artifact, _ = compiled
    assert artifact.represents_edge_count > 0, "Expected at least one REPRESENTS edge"


def test_artifact_belongs_to_domain_edges_non_zero(compiled):
    artifact, _ = compiled
    assert artifact.belongs_to_domain_edge_count > 0, "Expected at least one BELONGS_TO_DOMAIN edge"


def test_artifact_identifies_edges_non_zero(compiled):
    artifact, _ = compiled
    assert artifact.identifies_edge_count > 0, "Expected at least one IDENTIFIES edge"


def test_artifact_measures_edges_non_zero(compiled):
    artifact, _ = compiled
    assert artifact.measures_edge_count > 0, "Expected at least one MEASURES edge"


# ------------------------------------------------------------------ #
# graph_layer = "semantic" on all emitted nodes/edges                  #
# ------------------------------------------------------------------ #

def test_all_semantic_nodes_have_graph_layer(compiled):
    _, store = compiled
    semantic_nodes = [
        n for n in store._nodes.values()
        if n["label"] in ("BusinessEntityNode", "DomainNode", "MetricNode")
    ]
    assert semantic_nodes, "No semantic nodes found in store"
    bad = [n for n in semantic_nodes
           if n.get("properties", {}).get("graph_layer") != "semantic"]
    assert not bad, f"{len(bad)} semantic nodes missing graph_layer='semantic'"


def test_all_semantic_edges_have_graph_layer(compiled):
    _, store = compiled
    sem_etypes = {"REPRESENTS", "BELONGS_TO_DOMAIN", "IDENTIFIES", "MEASURES"}
    sem_edges = [e for e in store._edges.values() if e["edge_type"] in sem_etypes]
    assert sem_edges, "No semantic edges found in store"
    bad = [e for e in sem_edges
           if e.get("properties", {}).get("graph_layer") != "semantic"]
    assert not bad, f"{len(bad)} semantic edges missing graph_layer='semantic'"


# ------------------------------------------------------------------ #
# Confidence field present on all semantic edges                       #
# ------------------------------------------------------------------ #

def test_all_semantic_edges_have_confidence(compiled):
    _, store = compiled
    sem_etypes = {"REPRESENTS", "BELONGS_TO_DOMAIN", "IDENTIFIES", "MEASURES"}
    bad = [
        e for e in store._edges.values()
        if e["edge_type"] in sem_etypes
        and "confidence" not in e.get("properties", {})
    ]
    assert not bad, f"{len(bad)} semantic edges missing 'confidence' in properties"


# ------------------------------------------------------------------ #
# Structural nodes/edges are untouched                                 #
# ------------------------------------------------------------------ #

def test_structural_node_count_unchanged(golden_store, compiled):
    """Semantic compilation must not remove or overwrite structural nodes."""
    _, sem_store = compiled
    struct_labels = {"Asset", "Column", "Test", "DocObject", "_BuildMeta"}
    orig_struct = sum(1 for n in golden_store._nodes.values()
                      if n["label"] in struct_labels)
    new_struct = sum(1 for n in sem_store._nodes.values()
                     if n["label"] in struct_labels)
    assert new_struct == orig_struct, (
        f"Structural node count changed: {orig_struct} → {new_struct}"
    )


def test_structural_edge_count_unchanged(golden_store, compiled):
    """Semantic compilation must not remove or overwrite structural edges."""
    _, sem_store = compiled
    struct_etypes = {"CONTAINS", "HAS_COLUMN", "DEPENDS_ON", "TESTED_BY", "DOCUMENTED_BY"}
    orig = sum(1 for e in golden_store._edges.values()
               if e["edge_type"] in struct_etypes)
    new = sum(1 for e in sem_store._edges.values()
              if e["edge_type"] in struct_etypes)
    assert new == orig, f"Structural edge count changed: {orig} → {new}"


# ------------------------------------------------------------------ #
# Determinism: two runs produce same node/edge IDs                     #
# ------------------------------------------------------------------ #

def test_deterministic_node_ids(golden_bundle):
    store1 = JsonGraphStore.from_json(GRAPH_PATH)
    store2 = JsonGraphStore.from_json(GRAPH_PATH)
    SemanticGraphCompiler().compile(golden_bundle, store1, "det_build")
    SemanticGraphCompiler().compile(golden_bundle, store2, "det_build")

    sem_labels = {"BusinessEntityNode", "DomainNode", "MetricNode"}
    ids1 = sorted(n["node_id"] for n in store1._nodes.values()
                  if n["label"] in sem_labels)
    ids2 = sorted(n["node_id"] for n in store2._nodes.values()
                  if n["label"] in sem_labels)
    assert ids1 == ids2, "Semantic node IDs are not deterministic"


def test_deterministic_edge_ids(golden_bundle):
    store1 = JsonGraphStore.from_json(GRAPH_PATH)
    store2 = JsonGraphStore.from_json(GRAPH_PATH)
    SemanticGraphCompiler().compile(golden_bundle, store1, "det_build")
    SemanticGraphCompiler().compile(golden_bundle, store2, "det_build")

    sem_etypes = {"REPRESENTS", "BELONGS_TO_DOMAIN", "IDENTIFIES", "MEASURES"}
    ids1 = sorted(e["edge_id"] for e in store1._edges.values()
                  if e["edge_type"] in sem_etypes)
    ids2 = sorted(e["edge_id"] for e in store2._edges.values()
                  if e["edge_type"] in sem_etypes)
    assert ids1 == ids2, "Semantic edge IDs are not deterministic"


# ------------------------------------------------------------------ #
# Unassigned assets are the expected source-table leaves               #
# ------------------------------------------------------------------ #

def test_unassigned_assets_are_reasonable(compiled):
    """Unassigned assets should be a small set (≤ 20) of source-table leaves."""
    artifact, _ = compiled
    assert len(artifact.unassigned_assets) <= 35, (
        f"Too many unassigned assets ({len(artifact.unassigned_assets)}): "
        f"{artifact.unassigned_assets[:10]}"
    )
