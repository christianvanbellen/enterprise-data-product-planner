"""Tests for InitiativeArchetypeLibrary YAML loading and research artifact integrity."""

import pytest
from pathlib import Path

from graph.opportunity.archetype_library import (
    InitiativeArchetypeLibrary,
    INITIATIVE_ARCHETYPES,
    validate_archetype_library,
)


def test_library_loads_from_yaml_without_error():
    """Library initialisation must succeed and load all 26 initiatives."""
    library = InitiativeArchetypeLibrary()
    assert len(library.all_initiatives()) == 26


def test_all_initiatives_have_literature_sources():
    """Every initiative must trace to at least one source in initiative_research.yaml."""
    missing = [
        iid for iid, defn in INITIATIVE_ARCHETYPES.items()
        if not defn.get("literature_sources")
    ]
    assert missing == [], (
        f"Initiatives missing literature_sources: {missing}"
    )


def test_initiatives_by_feasibility_not_currently_feasible():
    """Research artifact identifies >= 4 initiatives as not currently feasible."""
    library = InitiativeArchetypeLibrary()
    infeasible = library.initiatives_by_feasibility("not_currently_feasible")
    assert len(infeasible) >= 4, (
        f"Expected >= 4 not_currently_feasible initiatives, got {len(infeasible)}: {infeasible}"
    )


def test_known_infeasible_initiatives_in_feasibility_query():
    library = InitiativeArchetypeLibrary()
    infeasible = set(library.initiatives_by_feasibility("not_currently_feasible"))
    # dynamic_pricing_model removed April 2026 (v1 initiative research) — was
    # documented as "not applicable to specialty London Market", not a gap.
    expected = {"submission_triage", "claims_fraud_detection",
                "claims_automation", "cat_exposure_monitoring"}
    assert expected <= infeasible, (
        f"Missing from infeasible set: {expected - infeasible}"
    )


def test_initiatives_by_feasibility_ready_now_includes_core():
    library = InitiativeArchetypeLibrary()
    ready = set(library.initiatives_by_feasibility("ready_now"))
    core = {"pricing_adequacy_monitoring", "claims_experience_analysis",
            "renewal_pricing_copilot", "underwriting_decision_support"}
    assert core <= ready, f"Missing from ready_now: {core - ready}"


def test_validate_archetype_library_clean():
    """No validation errors — all primitives referenced are valid IDs."""
    errors = validate_archetype_library()
    assert errors == [], "\n".join(errors)


def test_all_initiatives_have_feasibility_against_warehouse():
    """Every initiative must have a feasibility_against_warehouse value."""
    valid = {"ready_now", "ready_with_enablement",
             "needs_foundational_work", "not_currently_feasible"}
    bad = [
        iid for iid, defn in INITIATIVE_ARCHETYPES.items()
        if defn.get("feasibility_against_warehouse") not in valid
    ]
    assert bad == [], f"Missing/invalid feasibility_against_warehouse: {bad}"
