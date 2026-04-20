"""Tests for DomainAssigner."""

import pytest
from pathlib import Path
from typing import List

from ingestion.contracts.asset import CanonicalAsset, Provenance
from ingestion.contracts.bundle import CanonicalBundle
from graph.semantic.domain_assigner import DomainAssigner

BUNDLE_PATH = Path("output/bundle.json")


def _prov() -> Provenance:
    return Provenance(source_system="test", source_type="test")


def _asset(asset_id: str, name: str,
           domain_candidates: List[str] = None) -> CanonicalAsset:
    return CanonicalAsset(
        internal_id=asset_id,
        asset_type="dbt_model",
        name=name,
        normalized_name=name,
        tags=[],
        domain_candidates=domain_candidates or [],
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


def test_primary_domain_confidence_085():
    asset = _asset("a2", "model", domain_candidates=["pricing", "underwriting"])
    bundle = CanonicalBundle(assets=[asset], columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    results = DomainAssigner().assign(bundle, [])
    primary = next(r for r in results if r.domain == "pricing")
    secondary = next(r for r in results if r.domain == "underwriting")
    assert primary.confidence == pytest.approx(0.85)
    assert secondary.confidence == pytest.approx(0.65)


def test_secondary_domain_confidence_065():
    asset = _asset("a3", "model", domain_candidates=["underwriting", "pricing", "distribution"])
    bundle = CanonicalBundle(assets=[asset], columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    results = DomainAssigner().assign(bundle, [])
    by_domain = {r.domain: r.confidence for r in results if r.asset_id == "a3"}
    assert by_domain["underwriting"] == pytest.approx(0.85)
    assert by_domain["pricing"] == pytest.approx(0.65)
    assert by_domain["distribution"] == pytest.approx(0.65)


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
