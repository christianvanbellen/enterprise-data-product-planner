"""Tests for DomainAssigner."""

import pytest
from pathlib import Path
from typing import Dict, List, Optional

from ingestion.contracts.asset import CanonicalAsset, Provenance
from ingestion.contracts.bundle import CanonicalBundle
from graph.semantic.domain_assigner import DomainAssigner, _confidence_from_score

BUNDLE_PATH = Path("output/bundle.json")


def _prov() -> Provenance:
    return Provenance(source_system="test", source_type="test")


def _asset(asset_id: str, name: str,
           domain_candidates: Optional[List[str]] = None,
           domain_scores: Optional[Dict[str, float]] = None) -> CanonicalAsset:
    # If only candidates are given, default each to a score that maps to the
    # old "primary" confidence (3.0 → 0.95) so existing fixtures stay meaningful.
    if domain_candidates and domain_scores is None:
        domain_scores = {d: 3.0 for d in domain_candidates}
    return CanonicalAsset(
        internal_id=asset_id,
        asset_type="dbt_model",
        name=name,
        normalized_name=name,
        tags=[],
        domain_candidates=domain_candidates or [],
        domain_scores=domain_scores or {},
        grain_keys=[],
        is_enabled=True,
        version_hash=asset_id,
        provenance=_prov(),
    )


def _dep(src: str, tgt: str) -> dict:
    """Minimal DEPENDS_ON edge dict (source=upstream, target=consumer)."""
    return {
        "edge_id": f"e_{src}_{tgt}",
        "edge_type": "DEPENDS_ON",
        "source_node_id": src,
        "target_node_id": tgt,
    }


# ------------------------------------------------------------------ #
# Phase 1 keyword signal                                               #
# ------------------------------------------------------------------ #

def test_assets_with_domain_candidates_get_assignment():
    asset = _asset("a1", "quote_model", domain_candidates=["underwriting"])
    bundle = CanonicalBundle(assets=[asset], columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    results = DomainAssigner().assign(bundle, [])
    assignments = [r for r in results if r.asset_id == "a1"]
    assert len(assignments) == 1
    assert assignments[0].domain == "underwriting"
    assert assignments[0].source == "phase1_keyword"


def test_confidence_formula():
    """_confidence_from_score is the single source of truth for Signal 1 → confidence.

    Anchored so the weakest possible candidate (column-only hit, score 0.5) maps to
    the 60% floor — weak evidence but not dismissible."""
    assert _confidence_from_score(0.5) == pytest.approx(0.60)  # column only
    assert _confidence_from_score(1.0) == pytest.approx(0.65)  # desc only
    assert _confidence_from_score(2.0) == pytest.approx(0.75)  # tag only
    assert _confidence_from_score(3.0) == pytest.approx(0.85)  # name only
    assert _confidence_from_score(3.5) == pytest.approx(0.90)  # name + col
    assert _confidence_from_score(4.0) == pytest.approx(0.95)  # name + desc (capped)
    assert _confidence_from_score(10.0) == pytest.approx(0.95)  # saturated


def test_confidence_reflects_match_strength():
    """A strong-evidence domain (name hit, score 3.0) must outrank a weak one
    (column-only hit, score 0.5) — not an arbitrary list-order split."""
    asset = _asset(
        "a2", "model",
        domain_candidates=["pricing", "underwriting"],
        domain_scores={"pricing": 3.0, "underwriting": 0.5},
    )
    bundle = CanonicalBundle(assets=[asset], columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    results = DomainAssigner().assign(bundle, [])
    by_domain = {r.domain: r.confidence for r in results if r.asset_id == "a2"}
    assert by_domain["pricing"] == pytest.approx(0.85)
    assert by_domain["underwriting"] == pytest.approx(0.60)


def test_multiple_candidates_each_get_own_confidence():
    asset = _asset(
        "a3", "model",
        domain_candidates=["underwriting", "pricing", "distribution"],
        domain_scores={"underwriting": 3.0, "pricing": 2.0, "distribution": 0.5},
    )
    bundle = CanonicalBundle(assets=[asset], columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    results = DomainAssigner().assign(bundle, [])
    by_domain = {r.domain: r.confidence for r in results if r.asset_id == "a3"}
    assert by_domain["underwriting"] == pytest.approx(0.85)
    assert by_domain["pricing"] == pytest.approx(0.75)
    assert by_domain["distribution"] == pytest.approx(0.60)


def test_missing_score_falls_back_to_baseline():
    """If domain_scores is missing a candidate (legacy bundle), the domain is
    still assigned at the baseline floor rather than being dropped."""
    asset = _asset(
        "a_legacy", "model",
        domain_candidates=["pricing"],
        domain_scores={},  # explicitly empty — legacy-shape bundle
    )
    bundle = CanonicalBundle(assets=[asset], columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    results = DomainAssigner().assign(bundle, [])
    assert len(results) == 1
    assert results[0].confidence == pytest.approx(0.55)


# ------------------------------------------------------------------ #
# Isolated asset → no assignment                                       #
# ------------------------------------------------------------------ #

def test_isolated_asset_no_domain_no_upstream_produces_no_assignment():
    asset = _asset("a_iso", "isolated_source")  # no domain_candidates
    bundle = CanonicalBundle(assets=[asset], columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    results = DomainAssigner().assign(bundle, [])
    assert not any(r.asset_id == "a_iso" for r in results)


# ------------------------------------------------------------------ #
# Lineage inheritance                                                  #
# ------------------------------------------------------------------ #

def test_lineage_inheritance_produces_confidence_05():
    """An asset with no domain_candidates whose upstream all share a domain
    should inherit that domain with confidence=0.5."""
    upstream = _asset("a_up", "upstream_model", domain_candidates=["underwriting"])
    downstream = _asset("a_down", "downstream_model")  # no domains
    bundle = CanonicalBundle(assets=[upstream, downstream],
                             columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    edges = [_dep("a_up", "a_down")]
    results = DomainAssigner().assign(bundle, edges)
    inherited = [r for r in results if r.asset_id == "a_down"]
    assert len(inherited) == 1
    assert inherited[0].domain == "underwriting"
    assert inherited[0].confidence == pytest.approx(0.5)
    assert inherited[0].source == "lineage_inheritance"


def test_lineage_inheritance_requires_shared_domain():
    """If upstream assets do NOT share a domain, inheritance does not fire."""
    up1 = _asset("up1", "a", domain_candidates=["pricing"])
    up2 = _asset("up2", "b", domain_candidates=["underwriting"])
    down = _asset("down1", "c")
    bundle = CanonicalBundle(assets=[up1, up2, down],
                             columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    edges = [_dep("up1", "down1"), _dep("up2", "down1")]
    results = DomainAssigner().assign(bundle, edges)
    assert not any(r.asset_id == "down1" for r in results)


def test_lineage_inheritance_shared_domain_fires():
    """If all upstream assets share a domain, that domain is inherited."""
    up1 = _asset("up1b", "a", domain_candidates=["pricing", "underwriting"])
    up2 = _asset("up2b", "b", domain_candidates=["pricing"])
    down = _asset("down2", "c")
    bundle = CanonicalBundle(assets=[up1, up2, down],
                             columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    edges = [_dep("up1b", "down2"), _dep("up2b", "down2")]
    results = DomainAssigner().assign(bundle, edges)
    inherited = [r for r in results if r.asset_id == "down2"]
    assert len(inherited) == 1
    assert inherited[0].domain == "pricing"


# ------------------------------------------------------------------ #
# Golden data smoke test                                               #
# ------------------------------------------------------------------ #

@pytest.fixture(scope="module")
def golden_bundle():
    if not BUNDLE_PATH.exists():
        pytest.skip("output/bundle.json not found — run Phase 1 first")
    return CanonicalBundle.from_json(BUNDLE_PATH)


def test_golden_domain_assignments_non_empty(golden_bundle):
    results = DomainAssigner().assign(golden_bundle, [])
    assert len(results) > 0


def test_golden_all_confidences_in_range(golden_bundle):
    results = DomainAssigner().assign(golden_bundle, [])
    for r in results:
        assert 0.0 <= r.confidence <= 1.0, (
            f"Domain assignment for {r.asset_id}/{r.domain} "
            f"has out-of-range confidence {r.confidence}"
        )
