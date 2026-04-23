"""Tests for ConformedFieldBinder."""

import pytest
from pathlib import Path

from ingestion.contracts.bundle import CanonicalBundle
from graph.semantic.conformed_binder import ConformedFieldBinder, ENTITY_GROUPS
from graph.semantic.entity_mapper import CONFORMED_GROUP_TO_ENTITY

BUNDLE_PATH = Path("output/bundle.json")


@pytest.fixture(scope="module")
def golden_bundle():
    if not BUNDLE_PATH.exists():
        pytest.skip("output/bundle.json not found — run Phase 1 first")
    return CanonicalBundle.from_json(BUNDLE_PATH)


@pytest.fixture(scope="module")
def golden_results(golden_bundle):
    return ConformedFieldBinder().bind(golden_bundle)


# ------------------------------------------------------------------ #
# Count thresholds on real data                                        #
# ------------------------------------------------------------------ #

def test_policy_group_binds_at_least_5_assets(golden_results):
    bindings = golden_results.get("policy", [])
    assert len(bindings) >= 5, (
        f"Expected >= 5 policy bindings, got {len(bindings)}"
    )


def test_coverage_group_binds_at_least_3_assets(golden_results):
    bindings = golden_results.get("coverage", [])
    assert len(bindings) >= 3, (
        f"Expected >= 3 coverage bindings, got {len(bindings)}"
    )


def test_all_binding_scores_at_threshold(golden_results):
    """All returned bindings must have overlap_score >= 0.5."""
    for group, bindings in golden_results.items():
        for b in bindings:
            assert b.overlap_score >= 0.5, (
                f"Binding for {b.asset_id} / {group} has score {b.overlap_score} < 0.5"
            )


# ------------------------------------------------------------------ #
# Field completeness invariant                                         #
# ------------------------------------------------------------------ #

def test_matched_plus_missing_equals_all_group_fields(golden_bundle, golden_results):
    """matched_fields + missing_fields must equal the full group field set."""
    # Rebuild group_fields from the bundle (same logic as binder)
    group_term_ids = {}
    for term in golden_bundle.business_terms:
        if term.parent_term_id is None and term.name in ENTITY_GROUPS:
            group_term_ids[term.internal_id] = term.name

    group_fields = {name: set() for name in ENTITY_GROUPS}
    for term in golden_bundle.business_terms:
        if term.parent_term_id in group_term_ids:
            gname = group_term_ids[term.parent_term_id]
            group_fields[gname].add(term.name)

    for group, bindings in golden_results.items():
        expected = group_fields[group]
        for b in bindings:
            actual = set(b.matched_fields) | set(b.missing_fields)
            assert actual == expected, (
                f"Asset {b.asset_id} / {group}: "
                f"matched+missing={actual} != group_fields={expected}"
            )


# ------------------------------------------------------------------ #
# Confidence is binary 1.0 once the threshold is met                   #
# ------------------------------------------------------------------ #

def test_confidence_is_one_once_threshold_met(golden_results):
    """Signal 1 is binary: any asset admitted past OVERLAP_THRESHOLD gets 1.0.
    The graded overlap_score stays as evidence on the record but does not
    modulate confidence, so Signal 1 always outranks Signal 2/4 cleanly."""
    for group, bindings in golden_results.items():
        for b in bindings:
            assert b.confidence == 1.0, (
                f"binding {b.asset_id}/{group} has confidence {b.confidence} — "
                f"Signal 1 should be 1.0 once admitted"
            )


# ------------------------------------------------------------------ #
# No spurious bindings for empty asset                                 #
# ------------------------------------------------------------------ #

def test_asset_with_zero_matching_fields_produces_no_binding(golden_bundle):
    """An asset whose columns don't overlap with ANY group field gets no binding."""
    from ingestion.contracts.asset import CanonicalAsset, Provenance
    from ingestion.contracts.bundle import CanonicalBundle

    # Build a minimal bundle with one asset that has only non-overlapping columns
    dummy_asset = CanonicalAsset(
        internal_id="asset_test_zero",
        asset_type="dbt_model",
        name="zero_overlap_asset",
        normalized_name="zero_overlap_asset",
        domain_candidates=[],
        grain_keys=[],
        tags=[],
        is_enabled=True,
        version_hash="000",
        provenance=Provenance(source_system="test", source_type="test"),
    )
    # Keep all the real terms but replace assets/columns with our dummy
    mini_bundle = CanonicalBundle(
        assets=[dummy_asset],
        columns=[],
        lineage_edges=[],
        business_terms=golden_bundle.business_terms,
        metadata={},
    )
    results = ConformedFieldBinder().bind(mini_bundle)
    for group, bindings in results.items():
        assert bindings == [], (
            f"Expected no bindings for zero-overlap asset in group '{group}', "
            f"got {len(bindings)}"
        )


# ------------------------------------------------------------------ #
# CONFORMED_GROUP_TO_ENTITY mapping                                    #
# ------------------------------------------------------------------ #

def test_profitability_measures_maps_to_profitability_component():
    assert CONFORMED_GROUP_TO_ENTITY["profitability_measures"] == "profitability_component"


def test_rate_monitoring_maps_to_pricing_component():
    assert CONFORMED_GROUP_TO_ENTITY["rate_monitoring"] == "pricing_component"


def test_policy_totals_maps_to_policy():
    assert CONFORMED_GROUP_TO_ENTITY["policy_totals"] == "policy"


def test_coverage_maps_to_coverage():
    assert CONFORMED_GROUP_TO_ENTITY["coverage"] == "coverage"


def test_policy_maps_to_policy():
    assert CONFORMED_GROUP_TO_ENTITY["policy"] == "policy"


def test_entity_groups_contains_policy_totals():
    assert "policy_totals" in ENTITY_GROUPS


# ------------------------------------------------------------------ #
# Determinism                                                          #
# ------------------------------------------------------------------ #

def test_deterministic_output(golden_bundle):
    """Two runs on the same bundle must produce identical results."""
    r1 = ConformedFieldBinder().bind(golden_bundle)
    r2 = ConformedFieldBinder().bind(golden_bundle)
    for group in ENTITY_GROUPS:
        ids1 = sorted(b.asset_id for b in r1.get(group, []))
        ids2 = sorted(b.asset_id for b in r2.get(group, []))
        assert ids1 == ids2, f"Non-deterministic results for group '{group}'"
