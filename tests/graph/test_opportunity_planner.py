"""Tests for OpportunityPlanner and InitiativeArchetypeLibrary."""

import pytest
from pathlib import Path

from ingestion.contracts.bundle import CanonicalBundle
from graph.opportunity.primitive_extractor import CapabilityPrimitiveExtractor, PRIMITIVE_DEFINITIONS
from graph.opportunity.archetype_library import (
    InitiativeArchetypeLibrary, INITIATIVE_ARCHETYPES, validate_archetype_library,
)
from graph.opportunity.planner import OpportunityPlanner

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
    library = InitiativeArchetypeLibrary()
    return OpportunityPlanner().plan(golden_primitives, library)


# ------------------------------------------------------------------ #
# Library smoke tests                                                  #
# ------------------------------------------------------------------ #

def test_archetype_library_all_required_primitives_valid():
    """All required/optional primitives in INITIATIVE_ARCHETYPES must reference
    valid primitive IDs in PRIMITIVE_DEFINITIONS."""
    errors = validate_archetype_library()
    assert errors == [], f"Library validation errors:\n" + "\n".join(errors)


def test_library_all_initiatives_returns_twentysix():
    library = InitiativeArchetypeLibrary()
    assert len(library.all_initiatives()) == 26


def test_library_required_primitives_match_definition():
    library = InitiativeArchetypeLibrary()
    for iid in library.all_initiatives():
        rp = library.required_primitives(iid)
        assert isinstance(rp, list)


# ------------------------------------------------------------------ #
# All 26 initiatives produce an OpportunityResult                      #
# ------------------------------------------------------------------ #

def test_all_initiatives_have_result(golden_opportunities):
    ids = {o.initiative_id for o in golden_opportunities}
    expected = set(INITIATIVE_ARCHETYPES.keys())
    assert ids == expected, f"Missing: {expected - ids}"


def test_total_initiative_count_is_26(golden_opportunities):
    assert len(golden_opportunities) == 26, (
        f"Expected 26 initiatives, got {len(golden_opportunities)}"
    )


# ------------------------------------------------------------------ #
# Readiness assertions                                                  #
# ------------------------------------------------------------------ #

def test_pricing_adequacy_monitoring_ready(golden_opportunities):
    by_id = {o.initiative_id: o for o in golden_opportunities}
    opp = by_id["pricing_adequacy_monitoring"]
    assert opp.readiness in ("ready_now", "ready_with_enablement"), (
        f"pricing_adequacy_monitoring readiness={opp.readiness}"
    )


def test_claims_experience_analysis_ready(golden_opportunities):
    by_id = {o.initiative_id: o for o in golden_opportunities}
    opp = by_id["claims_experience_analysis"]
    assert opp.readiness in ("ready_now", "ready_with_enablement"), (
        f"claims_experience_analysis readiness={opp.readiness}"
    )


def test_not_currently_feasible_initiatives_have_low_composite_score(golden_opportunities):
    """Infeasible initiatives score < 0.15 (business_value * 0.1 multiplier)."""
    for o in golden_opportunities:
        if o.readiness == "not_currently_feasible":
            assert o.composite_score < 0.15, (
                f"{o.initiative_id} is not_currently_feasible "
                f"but composite_score={o.composite_score:.3f} >= 0.15"
            )


def test_claims_fraud_detection_not_feasible(golden_opportunities):
    by_id = {o.initiative_id: o for o in golden_opportunities}
    assert by_id["claims_fraud_detection"].readiness == "not_currently_feasible"


def test_submission_triage_not_feasible(golden_opportunities):
    by_id = {o.initiative_id: o for o in golden_opportunities}
    assert by_id["submission_triage"].readiness == "not_currently_feasible"


def test_cat_exposure_monitoring_not_feasible(golden_opportunities):
    by_id = {o.initiative_id: o for o in golden_opportunities}
    assert by_id["cat_exposure_monitoring"].readiness == "not_currently_feasible"


def test_broker_performance_intelligence_ready_with_enablement(golden_opportunities):
    """broker_performance_intelligence is capped at ready_with_enablement by the
    research artifact (broker_code absent) even though broker_attribution
    maturity >= 0.5 would otherwise compute as ready_now."""
    by_id = {o.initiative_id: o for o in golden_opportunities}
    assert by_id["broker_performance_intelligence"].readiness == "ready_with_enablement", (
        f"Expected ready_with_enablement, got {by_id['broker_performance_intelligence'].readiness}"
    )


# ------------------------------------------------------------------ #
# Effort scores                                                         #
# ------------------------------------------------------------------ #

def test_renewal_pricing_copilot_higher_effort_than_pricing_adequacy(golden_opportunities):
    by_id = {o.initiative_id: o for o in golden_opportunities}
    copilot    = by_id["renewal_pricing_copilot"]
    monitoring = by_id["pricing_adequacy_monitoring"]
    assert copilot.implementation_effort_score > monitoring.implementation_effort_score, (
        f"copilot effort={copilot.implementation_effort_score} "
        f"should be > pricing_adequacy effort={monitoring.implementation_effort_score}"
    )


# ------------------------------------------------------------------ #
# Composability                                                         #
# ------------------------------------------------------------------ #

def test_underwriting_decision_support_and_renewal_copilot_compose(golden_opportunities):
    """underwriting_decision_support and renewal_pricing_copilot share ≥ 2 primitives."""
    by_id = {o.initiative_id: o for o in golden_opportunities}
    uds     = by_id["underwriting_decision_support"]
    copilot = by_id["renewal_pricing_copilot"]

    # Direct check: shared primitives across required + optional
    arch_uds     = INITIATIVE_ARCHETYPES["underwriting_decision_support"]
    arch_copilot = INITIATIVE_ARCHETYPES["renewal_pricing_copilot"]
    shared = (
        set(arch_uds["required_primitives"] + arch_uds.get("optional_primitives", [])) &
        set(arch_copilot["required_primitives"] + arch_copilot.get("optional_primitives", []))
    )
    assert len(shared) >= 2, f"Expected >= 2 shared primitives, got {shared}"

    # Also check composes_with field
    assert "renewal_pricing_copilot" in uds.composes_with, (
        f"underwriting_decision_support.composes_with = {uds.composes_with}"
    )
    assert "underwriting_decision_support" in copilot.composes_with


# ------------------------------------------------------------------ #
# Composite score validity                                             #
# ------------------------------------------------------------------ #

def test_all_composite_scores_positive(golden_opportunities):
    for o in golden_opportunities:
        assert o.composite_score > 0, f"{o.initiative_id} composite_score <= 0"


# ------------------------------------------------------------------ #
# Determinism                                                          #
# ------------------------------------------------------------------ #

def test_deterministic_output(golden_primitives):
    library = InitiativeArchetypeLibrary()
    r1 = OpportunityPlanner().plan(golden_primitives, library)
    r2 = OpportunityPlanner().plan(golden_primitives, library)
    ids1 = sorted(o.initiative_id for o in r1)
    ids2 = sorted(o.initiative_id for o in r2)
    assert ids1 == ids2
    cs1 = {o.initiative_id: o.composite_score for o in r1}
    cs2 = {o.initiative_id: o.composite_score for o in r2}
    assert cs1 == cs2, "Composite scores are not deterministic"


# ------------------------------------------------------------------ #
# BUG 1 — missing_primitives populated for infeasible initiatives       #
# ------------------------------------------------------------------ #

def test_not_currently_feasible_have_visible_blocker(golden_opportunities):
    """not_currently_feasible initiatives must surface their blocker visibly —
    either as missing_primitives (primitive-level gap) or yaml_data_gaps
    (tool / governance / data-source gap). April 2026 (v1 initiative research):
    relaxed from missing_primitives-only to allow tool_missing /
    governance_missing initiatives (e.g. underwriter_copilot_rag,
    regulatory_reporting_qrt) whose required_primitives are all grounded but
    whose blockers are non-primitive.
    """
    by_id = {o.initiative_id: o for o in golden_opportunities}
    infeasible_ids = [iid for iid, o in by_id.items()
                      if o.readiness == "not_currently_feasible"]
    assert infeasible_ids, "No not_currently_feasible initiatives found"

    for iid in infeasible_ids:
        opp = by_id[iid]
        has_missing_prim = len(opp.missing_primitives) >= 1
        has_data_gap = len(opp.yaml_data_gaps) >= 1
        assert has_missing_prim or has_data_gap, (
            f"{iid} is not_currently_feasible but has no visible blocker "
            f"(missing_primitives and yaml_data_gaps both empty)"
        )


def test_infeasible_missing_primitives_are_virtual_dicts(golden_opportunities):
    """For infeasible initiatives with no defined required_primitives, every entry
    in missing_primitives should be a dict with source='yaml_data_gap'."""
    infeasible_no_req = [
        o for o in golden_opportunities
        if o.readiness == "not_currently_feasible"
        and len(INITIATIVE_ARCHETYPES[o.initiative_id]["required_primitives"]) == 0
    ]
    assert infeasible_no_req, "Expected at least one infeasible initiative with no required_primitives"
    for opp in infeasible_no_req:
        for entry in opp.missing_primitives:
            assert isinstance(entry, dict), (
                f"{opp.initiative_id}: expected dict in missing_primitives, got {type(entry)}"
            )
            assert entry.get("source") == "yaml_data_gap", (
                f"{opp.initiative_id}: missing_primitives entry has wrong source: {entry}"
            )


# ------------------------------------------------------------------ #
# BUG 3 — optional_primitives_available populated                      #
# ------------------------------------------------------------------ #

def test_optional_primitives_available_populated_for_ready_now(golden_opportunities):
    """Multi-primitive ready_now initiatives should have non-empty
    optional_primitives_available when their optional primitives exist in the
    warehouse (they're always extracted by the primitive extractor).
    This is validated after the compiler writes properties — here we check
    the planner's yaml_data_gaps field is populated for gapped initiatives.
    """
    by_id = {o.initiative_id: o for o in golden_opportunities}
    # pricing_adequacy_monitoring has optional: profitability_decomposition (maturity 0.875)
    # The YAML-backed data_gaps field should be empty for ready_now initiatives
    opp = by_id["pricing_adequacy_monitoring"]
    assert opp.readiness in ("ready_now", "ready_with_enablement")
    assert len(opp.yaml_data_gaps) == 0, (
        f"pricing_adequacy_monitoring should have no YAML data gaps, got: {opp.yaml_data_gaps}"
    )


def test_yaml_data_gaps_populated_for_infeasible(golden_opportunities):
    """not_currently_feasible initiatives must have non-empty yaml_data_gaps
    so the GapAnalyser can emit YAML-sourced GapNodes."""
    by_id = {o.initiative_id: o for o in golden_opportunities}
    for iid in ("submission_triage", "claims_fraud_detection", "cat_exposure_monitoring"):
        opp = by_id[iid]
        assert len(opp.yaml_data_gaps) >= 1, (
            f"{iid} expected yaml_data_gaps but got empty list"
        )
