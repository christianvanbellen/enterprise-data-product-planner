"""Tests for EntityMapper."""

import pytest
from pathlib import Path
from typing import List

from ingestion.contracts.asset import CanonicalAsset, CanonicalColumn, Provenance
from ingestion.contracts.bundle import CanonicalBundle
from ingestion.contracts.business import CanonicalBusinessTerm
from graph.semantic.conformed_binder import ConformedFieldBinder, ConformedBindingResult
from graph.semantic.entity_mapper import EntityMapper

BUNDLE_PATH = Path("output/bundle.json")


def _prov() -> Provenance:
    return Provenance(source_system="test", source_type="test")


def _asset(asset_id: str, name: str, product_lines: List[str] = None,
           tags: List[str] = None, domain_candidates: List[str] = None) -> CanonicalAsset:
    tag_dims = {"product_line": list(product_lines)} if product_lines else {}
    return CanonicalAsset(
        internal_id=asset_id,
        asset_type="dbt_model",
        name=name,
        normalized_name=name,
        tags=tags or [],
        tag_dimensions=tag_dims,
        domain_candidates=domain_candidates or [],
        grain_keys=[],
        is_enabled=True,
        version_hash=asset_id,
        provenance=_prov(),
    )


def _col(col_id: str, asset_id: str, name: str,
         column_role: str = "attribute",
         semantic_candidates: List[str] = None) -> CanonicalColumn:
    return CanonicalColumn(
        internal_id=col_id,
        asset_internal_id=asset_id,
        name=name,
        normalized_name=name,
        data_type_family="string",
        column_role=column_role,
        tests=[],
        semantic_candidates=semantic_candidates or [],
        version_hash=col_id,
        provenance=_prov(),
    )


@pytest.fixture(scope="module")
def golden_bundle():
    if not BUNDLE_PATH.exists():
        pytest.skip("output/bundle.json not found — run Phase 1 first")
    return CanonicalBundle.from_json(BUNDLE_PATH)


@pytest.fixture(scope="module")
def golden_candidates(golden_bundle):
    binder_results = ConformedFieldBinder().bind(golden_bundle)
    return EntityMapper().map(golden_bundle, binder_results)


# ------------------------------------------------------------------ #
# Signal 3 (tag-dimension entity binding) was removed April 2026.      #
# Tags still populate asset.tag_dimensions for display/filter but no   #
# longer drive entity candidates. No regression test should assert     #
# that a tag alone produces an entity candidate.                       #
# ------------------------------------------------------------------ #

def test_tag_alone_produces_no_entity_candidate():
    """An asset with only tag data and no other signal must not produce any
    entity candidate — confirms Signal 3 is gone."""
    asset = _asset("asset_tag_only", "generic_name",
                   product_lines=["directors_and_officers"])
    bundle = CanonicalBundle(assets=[asset], columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    candidates = EntityMapper().map(bundle, {})
    assert not any(c.asset_id == "asset_tag_only" for c in candidates)


# ------------------------------------------------------------------ #
# Claim entity via signature columns                                   #
# ------------------------------------------------------------------ #

def test_asset_with_claim_columns_gets_claim_entity():
    """An asset with sufficient claim signature columns → entity 'claim'.

    claim sig has 16 fields; need ≥8 matches for score×0.8 ≥ MIN_CONFIDENCE(0.4).
    """
    asset = _asset("asset_clm", "claim_data_mart")
    cols = [
        _col("c1",  "asset_clm", "incurred"),
        _col("c2",  "asset_clm", "paid"),
        _col("c3",  "asset_clm", "claim_count"),
        _col("c4",  "asset_clm", "reserved"),
        _col("c5",  "asset_clm", "burn_rate_ulr"),
        _col("c6",  "asset_clm", "gg_ulr"),
        _col("c7",  "asset_clm", "gn_ulr"),
        _col("c8",  "asset_clm", "total_incurred"),
    ]
    bundle = CanonicalBundle(assets=[asset], columns=cols, lineage_edges=[],
                             business_terms=[], metadata={})
    candidates = EntityMapper().map(bundle, {})
    assert any(c.entity_label == "claim" and c.asset_id == "asset_clm"
               for c in candidates), "Expected claim candidate"


# ------------------------------------------------------------------ #
# Invalid entity labels are rejected                                   #
# ------------------------------------------------------------------ #

def test_invalid_entity_label_is_rejected():
    """An entity label not in allowed_entities() must never appear in output."""
    asset = _asset("asset_bad", "some_asset")
    bundle = CanonicalBundle(assets=[asset], columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    # Feed a binder result with a fake group mapped to an invalid label
    fake_binding = ConformedBindingResult(
        asset_id="asset_bad",
        entity_group="fake_group",
        overlap_score=0.9,
        matched_fields=["x"],
        missing_fields=[],
        confidence=0.9,
    )
    import warnings
    mapper = EntityMapper()
    # Directly call _add equivalent by feeding a fake binder result with unknown group
    # The group "fake_group" is not in CONFORMED_GROUP_TO_ENTITY, so it's simply ignored.
    # Test instead by patching: use a group mapped to a non-allowed entity.
    from graph.semantic.entity_mapper import CONFORMED_GROUP_TO_ENTITY
    import copy
    original = copy.copy(CONFORMED_GROUP_TO_ENTITY)
    try:
        CONFORMED_GROUP_TO_ENTITY["fake_group"] = "not_a_real_entity"
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            candidates = mapper.map(bundle, {"fake_group": [fake_binding]})
        assert not any(c.entity_label == "not_a_real_entity" for c in candidates)
    finally:
        CONFORMED_GROUP_TO_ENTITY.pop("fake_group", None)


# ------------------------------------------------------------------ #
# Conformed binding beats signature score                              #
# ------------------------------------------------------------------ #

def test_conformed_binding_raises_confidence_above_signature_alone():
    """High conformed binding score (0.8) should produce confidence >= 0.8."""
    asset = _asset("asset_c1", "policy_mart",
                   domain_candidates=["underwriting"])
    binding = ConformedBindingResult(
        asset_id="asset_c1",
        entity_group="policy",
        overlap_score=0.8,
        matched_fields=["policy_currency", "new_renewal"],
        missing_fields=[],
        confidence=0.8,
    )
    bundle = CanonicalBundle(assets=[asset], columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    candidates = EntityMapper().map(bundle, {"policy": [binding]})
    policy_c = next((c for c in candidates if c.entity_label == "policy"), None)
    assert policy_c is not None
    assert policy_c.confidence >= 0.8


# ------------------------------------------------------------------ #
# Signal 4: Asset name pattern                                         #
# ------------------------------------------------------------------ #

def test_asset_name_pattern_claim_entity():
    """Asset with 'claim' in normalized_name → entity 'claim' via asset_name_pattern."""
    asset = _asset("asset_clm_name", "fct_claim_summary")
    bundle = CanonicalBundle(assets=[asset], columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    candidates = EntityMapper().map(bundle, {})
    clm = next((c for c in candidates if c.entity_label == "claim"
                and c.asset_id == "asset_clm_name"), None)
    assert clm is not None, "Expected claim candidate from asset name pattern"
    assert clm.confidence == pytest.approx(0.6)
    assert "asset_name_pattern" in clm.signal_sources


def test_asset_name_pattern_broker_entity():
    """Asset with 'brokerage' in normalized_name → entity 'broker' via asset_name_pattern."""
    asset = _asset("asset_brok_name", "dim_brokerage_account")
    bundle = CanonicalBundle(assets=[asset], columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    candidates = EntityMapper().map(bundle, {})
    brk = next((c for c in candidates if c.entity_label == "broker"
                and c.asset_id == "asset_brok_name"), None)
    assert brk is not None, "Expected broker candidate from asset name pattern"
    assert "asset_name_pattern" in brk.signal_sources


# ------------------------------------------------------------------ #
# No signal → no candidate                                             #
# ------------------------------------------------------------------ #

def test_asset_with_no_signal_produces_no_candidate():
    asset = _asset("asset_none", "isolated_table")
    bundle = CanonicalBundle(assets=[asset], columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    candidates = EntityMapper().map(bundle, {})
    assert not any(c.asset_id == "asset_none" for c in candidates)


# ------------------------------------------------------------------ #
# Signal merging: same entity from two sources → max confidence        #
# ------------------------------------------------------------------ #

def test_conformed_binding_survives_with_tag_data_present():
    """A conformed binding should produce a coverage candidate regardless of whether
    the asset also carries product_line tags — tags no longer contribute to entity
    mapping (Signal 3 removed)."""
    asset = _asset("asset_merge", "coverage_mart",
                   product_lines=["general_aviation"])
    binding = ConformedBindingResult(
        asset_id="asset_merge",
        entity_group="coverage",
        overlap_score=0.8,
        matched_fields=["primary_coverage"],
        missing_fields=[],
        confidence=1.0,
    )
    bundle = CanonicalBundle(assets=[asset], columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    candidates = EntityMapper().map(bundle, {"coverage": [binding]})
    cov = next((c for c in candidates if c.entity_label == "coverage"), None)
    assert cov is not None
    assert cov.confidence == pytest.approx(1.0)
    assert "conformed_binding" in cov.signal_sources
    # Tag data does NOT add a candidate
    assert not any(c.entity_label == "line_of_business" for c in candidates)
    assert not any(s.startswith("tag_") for c in candidates for s in c.signal_sources)


# ------------------------------------------------------------------ #
# Signal merging: two different entities both >= 0.5 → both emitted   #
# ------------------------------------------------------------------ #

def test_two_strong_different_entities_both_emitted():
    """If conformed binding gives policy=0.7 and coverage=0.6, both >= 0.5 → emit both."""
    asset = _asset("asset_two", "dual_entity_table")
    policy_binding = ConformedBindingResult(
        asset_id="asset_two", entity_group="policy",
        overlap_score=0.7, matched_fields=["policy_currency"],
        missing_fields=[], confidence=0.7,
    )
    coverage_binding = ConformedBindingResult(
        asset_id="asset_two", entity_group="coverage",
        overlap_score=0.6, matched_fields=["primary_coverage"],
        missing_fields=[], confidence=0.6,
    )
    bundle = CanonicalBundle(assets=[asset], columns=[], lineage_edges=[],
                             business_terms=[], metadata={})
    candidates = EntityMapper().map(
        bundle,
        {"policy": [policy_binding], "coverage": [coverage_binding]},
    )
    asset_labels = {c.entity_label for c in candidates if c.asset_id == "asset_two"}
    assert "policy" in asset_labels
    assert "coverage" in asset_labels


# ------------------------------------------------------------------ #
# Determinism                                                          #
# ------------------------------------------------------------------ #

def test_deterministic_output(golden_bundle):
    binder_results = ConformedFieldBinder().bind(golden_bundle)
    c1 = EntityMapper().map(golden_bundle, binder_results)
    c2 = EntityMapper().map(golden_bundle, binder_results)
    ids1 = sorted((c.asset_id, c.entity_label) for c in c1)
    ids2 = sorted((c.asset_id, c.entity_label) for c in c2)
    assert ids1 == ids2, "EntityMapper output is not deterministic"
