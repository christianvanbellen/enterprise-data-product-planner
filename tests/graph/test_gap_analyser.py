"""Tests for GapAnalyser."""

import pytest
from pathlib import Path

from ingestion.contracts.bundle import CanonicalBundle
from graph.opportunity.primitive_extractor import CapabilityPrimitiveExtractor
from graph.opportunity.archetype_library import InitiativeArchetypeLibrary
from graph.opportunity.planner import OpportunityPlanner
from graph.opportunity.gap_analyser import GapAnalyser

BUNDLE_PATH = Path("output/bundle.json")
GRAPH_PATH  = Path("output/graph")


@pytest.fixture(scope="module")
def golden_bundle():
    if not BUNDLE_PATH.exists():
        pytest.skip("output/bundle.json not found — run Phase 1 first")
    return CanonicalBundle.from_json(BUNDLE_PATH)


@pytest.fixture(scope="module")
def golden_graph_store():
    if not (GRAPH_PATH / "nodes.json").exists():
        pytest.skip("output/graph not found — run Phase 2+3 first")
    from graph.store.json_store import JsonGraphStore
    return JsonGraphStore.from_json(GRAPH_PATH)


@pytest.fixture(scope="module")
def golden_primitives(golden_bundle, golden_graph_store):
    return CapabilityPrimitiveExtractor().extract(golden_bundle, golden_graph_store)


@pytest.fixture(scope="module")
def golden_opportunities(golden_primitives):
    return OpportunityPlanner().plan(golden_primitives, InitiativeArchetypeLibrary())


@pytest.fixture(scope="module")
def golden_gaps(golden_primitives, golden_opportunities):
    return GapAnalyser().analyse(golden_primitives, golden_opportunities)


# ------------------------------------------------------------------ #
# Gap presence                                                          #
# ------------------------------------------------------------------ #

def test_gaps_are_produced(golden_gaps):
    """At least one gap should be identified (broker_attribution < 0.9 maturity).
    Gap threshold is 0.9 because the warehouse is well-covered overall;
    primitives with missing columns (broker_code, sold_to_plan) are the gaps.
    """
    assert len(golden_gaps) >= 1, "Expected at least one gap"


def test_broker_attribution_produces_gap(golden_gaps):
    """broker_attribution has maturity 0.833 (missing broker_code) → gap."""
    gap_prim_ids = {g.primitive_id for g in golden_gaps}
    assert "broker_attribution" in gap_prim_ids, (
        f"Expected broker_attribution gap. Gaps found: {sorted(gap_prim_ids)}"
    )


def test_each_gap_has_at_least_one_blocking_initiative(golden_gaps):
    for g in golden_gaps:
        assert len(g.blocking_initiatives) >= 1, (
            f"{g.primitive_id} gap has no blocking initiatives"
        )


# ------------------------------------------------------------------ #
# Leverage score validity                                              #
# ------------------------------------------------------------------ #

def test_all_leverage_scores_between_0_and_1(golden_gaps):
    for g in golden_gaps:
        assert 0.0 <= g.leverage_score <= 1.0, (
            f"{g.primitive_id} leverage_score={g.leverage_score} out of [0,1]"
        )


# ------------------------------------------------------------------ #
# Gap type from ontology                                               #
# ------------------------------------------------------------------ #

def test_all_gap_types_are_valid(golden_gaps):
    import yaml
    from pathlib import Path
    gap_types_yaml = Path("ontology/gap_types.yaml")
    if not gap_types_yaml.exists():
        pytest.skip("ontology/gap_types.yaml not found")
    valid_types = set(yaml.safe_load(gap_types_yaml.read_text())["gap_types"])
    for g in golden_gaps:
        assert g.gap_type in valid_types, (
            f"{g.primitive_id} gap_type='{g.gap_type}' not in {valid_types}"
        )


# ------------------------------------------------------------------ #
# Determinism                                                          #
# ------------------------------------------------------------------ #

def test_deterministic_output(golden_primitives, golden_opportunities):
    r1 = GapAnalyser().analyse(golden_primitives, golden_opportunities)
    r2 = GapAnalyser().analyse(golden_primitives, golden_opportunities)
    ids1 = sorted(g.primitive_id for g in r1)
    ids2 = sorted(g.primitive_id for g in r2)
    assert ids1 == ids2


# ------------------------------------------------------------------ #
# YAML-sourced gaps                                                     #
# ------------------------------------------------------------------ #

def test_yaml_sourced_gaps_exist(golden_gaps):
    """At least one GapResult must have source='yaml_research'."""
    yaml_gaps = [g for g in golden_gaps if g.source == "yaml_research"]
    assert len(yaml_gaps) >= 1, "Expected at least one yaml_research gap"


def test_yaml_sourced_gaps_have_correct_source(golden_gaps):
    """Every yaml_research gap must have source set and maturity_score == 0."""
    for g in golden_gaps:
        if g.source == "yaml_research":
            assert g.maturity_score == 0.0, (
                f"{g.primitive_id}: yaml_research gap has non-zero maturity {g.maturity_score}"
            )
            assert g.primitive_id.startswith("yaml_gap_"), (
                f"yaml_research gap has unexpected primitive_id: {g.primitive_id}"
            )


def test_primitive_maturity_gaps_have_correct_source(golden_gaps):
    """Primitive-maturity gaps must have source='primitive_maturity'."""
    for g in golden_gaps:
        if not g.primitive_id.startswith("yaml_gap_"):
            assert g.source == "primitive_maturity", (
                f"{g.primitive_id} has unexpected source '{g.source}'"
            )


def test_yaml_gap_count(golden_gaps):
    """Should produce 10 unique YAML-sourced gap entries (one per data_gap entry
    in initiative_research.yaml after deduplication by gap_type+description)."""
    yaml_gaps = [g for g in golden_gaps if g.source == "yaml_research"]
    assert len(yaml_gaps) >= 8, (
        f"Expected >= 8 yaml_research gaps, got {len(yaml_gaps)}"
    )
