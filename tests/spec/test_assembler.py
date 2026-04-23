"""Tests for SpecAssembler."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock

import inspect

from graph.spec.assembler import (
    SpecAssembler, SpecDocument, AssetDetail, ColumnDetail, JoinPath,
    OutputStructure, OutputColumn, TargetVariable,
    DataRequisite, DataRequisiteColumn, JoinAssessment,
    _count_upstream_dependents, _compute_grain_join_paths, _compute_grain_description,
    _collect_output_columns,
)
from graph.opportunity.planner import OpportunityResult
from graph.opportunity.primitive_extractor import CapabilityPrimitive
from ingestion.contracts.bundle import CanonicalBundle
from ingestion.contracts.asset import CanonicalAsset, CanonicalColumn, Provenance

BUNDLE_PATH = Path("output/bundle.json")
GRAPH_PATH  = Path("output/graph")


# ---------------------------------------------------------------------------
# Golden fixtures (require Phase 4 output)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def golden_bundle():
    if not BUNDLE_PATH.exists():
        pytest.skip("output/bundle.json not found — run Phase 1 first")
    return CanonicalBundle.from_json(BUNDLE_PATH)


@pytest.fixture(scope="module")
def golden_graph_store():
    if not (GRAPH_PATH / "nodes.json").exists():
        pytest.skip("output/graph not found — run Phase 4 first")
    from graph.store.json_store import JsonGraphStore
    return JsonGraphStore.from_json(GRAPH_PATH)


@pytest.fixture(scope="module")
def golden_primitives(golden_bundle, golden_graph_store):
    from graph.opportunity.primitive_extractor import CapabilityPrimitiveExtractor
    return CapabilityPrimitiveExtractor().extract(golden_bundle, golden_graph_store)


@pytest.fixture(scope="module")
def golden_opps(golden_primitives):
    from graph.opportunity.archetype_library import InitiativeArchetypeLibrary
    from graph.opportunity.planner import OpportunityPlanner
    return OpportunityPlanner().plan(golden_primitives, InitiativeArchetypeLibrary())


@pytest.fixture(scope="module")
def golden_spec_uds(golden_bundle, golden_graph_store, golden_primitives, golden_opps):
    opp_by_id = {o.initiative_id: o for o in golden_opps}
    opp = opp_by_id.get("underwriting_decision_support")
    if opp is None:
        pytest.skip("underwriting_decision_support not found")
    assembler = SpecAssembler()
    return assembler.assemble(
        opp=opp,
        primitives=golden_primitives,
        bundle=golden_bundle,
        graph_store=golden_graph_store,
        graph_build_id="test_build_001",
    )


# ---------------------------------------------------------------------------
# Unit tests (no golden files required)
# ---------------------------------------------------------------------------

def _make_prov():
    return Provenance(source_system="test", source_type="test")


def _make_opp(**kwargs):
    defaults = dict(
        initiative_id="test_initiative",
        initiative_name="Test Initiative",
        archetype="monitoring",
        readiness="ready_now",
        business_value_score=0.8,
        implementation_effort_score=0.4,
        composite_score=0.8,
        available_primitives=["prim_a"],
        missing_primitives=[],
        blocker_details=[],
        composes_with=[],
        target_users=["underwriters"],
        business_objective="Test objective",
        output_type="monitoring_dashboard",
        yaml_data_gaps=[],
    )
    defaults.update(kwargs)
    return OpportunityResult(**defaults)


def _make_primitive(primitive_id="prim_a", maturity=0.9, assets=None):
    return CapabilityPrimitive(
        primitive_id=primitive_id,
        primitive_name="Primitive A",
        description="Test primitive",
        maturity_score=maturity,
        entity_score=1.0,
        column_score=1.0,
        matched_entities=["exposure"],
        missing_entities=[],
        matched_columns=["col_a", "col_b"],
        missing_columns=["col_c"],
        supporting_asset_ids=assets or ["asset_001"],
    )


def _make_graph_store(asset_ids=None, depends_on_targets=None):
    """Return a mock graph store with Asset nodes and optional DEPENDS_ON edges."""
    store = MagicMock()
    nodes = {}
    for aid in (asset_ids or ["asset_001"]):
        nodes[aid] = {
            "node_id": aid,
            "label": "Asset",
            "build_id": "build_test",
            "properties": {
                "name": f"asset_{aid[-3:]}",
                "description": f"Description for {aid}",
                "domain_candidates": ["underwriting"],
                "grain_keys": ["quote_id"],
            },
        }
    edges = {}
    for i, target in enumerate(depends_on_targets or []):
        eid = f"edge_{i}"
        edges[eid] = {
            "edge_id": eid,
            "edge_type": "DEPENDS_ON",
            "source_node_id": f"downstream_{i}",
            "target_node_id": target,
        }
    store._nodes = nodes
    store._edges = edges
    return store


def _make_bundle(asset_ids=None, add_columns=True):
    assets = []
    columns = []
    for aid in (asset_ids or ["asset_001"]):
        assets.append(CanonicalAsset(
            internal_id=aid,
            asset_type="dbt_model",
            name=f"asset_{aid[-3:]}",
            normalized_name=f"asset_{aid[-3:]}",
            grain_keys=["quote_id"],
            domain_candidates=["underwriting"],
            version_hash="abc",
            provenance=_make_prov(),
        ))
        if add_columns:
            columns.append(CanonicalColumn(
                internal_id=f"col_{aid}_1",
                asset_internal_id=aid,
                name="quote_id",
                normalized_name="quote_id",
                data_type_family="string",
                column_role="grain_key",
                version_hash="abc",
                provenance=_make_prov(),
            ))
    return CanonicalBundle(assets=assets, columns=columns)


# ------------------------------------------------------------------ #
# spec_type logic                                                      #
# ------------------------------------------------------------------ #

@pytest.mark.parametrize("readiness,expected_type", [
    ("ready_now",               "full_spec"),
    ("ready_with_enablement",   "full_spec"),
    ("needs_foundational_work", "gap_brief"),
    ("not_currently_feasible",  "gap_brief"),
])
def test_spec_type_from_readiness(readiness, expected_type):
    opp = _make_opp(readiness=readiness)
    store = _make_graph_store()
    bundle = _make_bundle()
    prim = _make_primitive()
    spec = SpecAssembler().assemble(opp, [prim], bundle, store, "build_x")
    assert spec.spec_type == expected_type


# ------------------------------------------------------------------ #
# spec_id determinism                                                  #
# ------------------------------------------------------------------ #

def test_spec_id_is_deterministic():
    opp = _make_opp()
    store = _make_graph_store()
    bundle = _make_bundle()
    prim = _make_primitive()
    s1 = SpecAssembler().assemble(opp, [prim], bundle, store, "build_x")
    s2 = SpecAssembler().assemble(opp, [prim], bundle, store, "build_x")
    assert s1.spec_id == s2.spec_id


def test_spec_id_changes_with_build_id():
    opp = _make_opp()
    store = _make_graph_store()
    bundle = _make_bundle()
    prim = _make_primitive()
    s1 = SpecAssembler().assemble(opp, [prim], bundle, store, "build_a")
    s2 = SpecAssembler().assemble(opp, [prim], bundle, store, "build_b")
    assert s1.spec_id != s2.spec_id


# ------------------------------------------------------------------ #
# Primitive details                                                    #
# ------------------------------------------------------------------ #

def test_available_primitives_populated():
    opp = _make_opp(available_primitives=["prim_a"])
    store = _make_graph_store()
    bundle = _make_bundle()
    prim = _make_primitive("prim_a", assets=["asset_001"])
    spec = SpecAssembler().assemble(opp, [prim], bundle, store, "build_x")
    assert len(spec.available_primitives) == 1
    assert spec.available_primitives[0].primitive_id == "prim_a"


def test_primitive_without_graph_assets_still_included():
    """If supporting assets are not in graph, primitive still appears in summary."""
    opp = _make_opp(available_primitives=["prim_a"])
    store = _make_graph_store(asset_ids=[])
    bundle = _make_bundle(asset_ids=[])
    prim = _make_primitive("prim_a", assets=["nonexistent_asset"])
    spec = SpecAssembler().assemble(opp, [prim], bundle, store, "build_x")
    assert len(spec.available_primitives) == 1
    assert spec.available_primitives[0].primitive_id == "prim_a"
    assert 0.0 <= spec.available_primitives[0].maturity_score <= 1.0


# ------------------------------------------------------------------ #
# Column detail for top-5 assets                                      #
# ------------------------------------------------------------------ #

def test_top_asset_grain_key_in_data_requisite():
    """The most upstream-dependent supporting asset's grain key appears in DataRequisite."""
    opp = _make_opp(available_primitives=["prim_a"])
    store = _make_graph_store(
        asset_ids=["asset_001"],
        depends_on_targets=["asset_001", "asset_001", "asset_001"],
    )
    bundle = _make_bundle(asset_ids=["asset_001"])
    prim = _make_primitive("prim_a", assets=["asset_001"])
    spec = SpecAssembler().assemble(opp, [prim], bundle, store, "build_x")
    dr = spec.data_requisite
    assert dr is not None
    col_names = [c.column_name for c in dr.columns]
    assert "quote_id" in col_names


def test_described_column_flows_to_data_requisite():
    """A column with a description that matches a primitive should appear in DataRequisite."""
    prov = _make_prov()
    asset = CanonicalAsset(
        internal_id="asset_001",
        asset_type="dbt_model",
        name="rate_monitor",
        normalized_name="rate_monitor",
        grain_keys=["quote_id"],
        version_hash="abc",
        provenance=prov,
    )
    bundle = CanonicalBundle(
        assets=[asset],
        columns=[
            CanonicalColumn(
                internal_id="col_id",
                asset_internal_id="asset_001",
                name="quote_id",
                normalized_name="quote_id",
                data_type_family="string",
                column_role="identifier",
                version_hash="abc",
                provenance=prov,
            ),
            CanonicalColumn(
                internal_id="col_m",
                asset_internal_id="asset_001",
                name="gross_rarc",
                normalized_name="gross_rarc",
                description="Gross risk-adjusted rate change",
                data_type_family="float",
                column_role="numeric_attribute",
                version_hash="abc",
                provenance=prov,
            ),
        ],
    )
    opp = _make_opp(available_primitives=["prim_a"])
    prim = _make_primitive("prim_a", assets=["asset_001"])
    store = _make_graph_store(
        asset_ids=["asset_001"],
        depends_on_targets=["asset_001"],
    )
    spec = SpecAssembler().assemble(opp, [prim], bundle, store, "build_x")
    dr = spec.data_requisite
    assert dr is not None
    # gross_rarc has a description → positive-inclusion filter should admit it
    matched = [c for c in dr.columns if c.column_name == "gross_rarc"]
    assert matched, "Described measure column should appear in DataRequisite"
    assert matched[0].description == "Gross risk-adjusted rate change"


# ------------------------------------------------------------------ #
# Grain join paths                                                     #
# ------------------------------------------------------------------ #

def test_grain_join_paths_two_assets_same_grain():
    """Two assets sharing 2+ grain keys produce a JoinPath."""
    asset_map = {
        "a1": {"label": "Asset", "build_id": "b", "properties": {
            "name": "asset_alpha", "grain_keys": ["quote_id", "layer_id"],
            "domain_candidates": [], "description": None,
        }},
        "a2": {"label": "Asset", "build_id": "b", "properties": {
            "name": "asset_beta", "grain_keys": ["quote_id", "layer_id", "policy_id"],
            "domain_candidates": [], "description": None,
        }},
    }
    paths = _compute_grain_join_paths(["a1", "a2"], asset_map)
    assert len(paths) == 1
    assert set(paths[0].shared_grain_keys) == {"quote_id", "layer_id"}


def test_grain_join_paths_single_shared_key_excluded():
    """Only 1 shared key → no JoinPath emitted."""
    asset_map = {
        "a1": {"label": "Asset", "build_id": "b", "properties": {
            "name": "asset_alpha", "grain_keys": ["quote_id"],
            "domain_candidates": [], "description": None,
        }},
        "a2": {"label": "Asset", "build_id": "b", "properties": {
            "name": "asset_beta", "grain_keys": ["quote_id"],
            "domain_candidates": [], "description": None,
        }},
    }
    paths = _compute_grain_join_paths(["a1", "a2"], asset_map)
    assert len(paths) == 0


# ------------------------------------------------------------------ #
# Blocker details                                                      #
# ------------------------------------------------------------------ #

def test_yaml_blockers_present_for_gap_brief():
    data_gaps = [
        {"gap_type": "missing_source_system", "description": "No ML feature store"},
        {"gap_type": "missing_history", "description": "Claims history < 3 years"},
    ]
    opp = _make_opp(
        readiness="not_currently_feasible",
        yaml_data_gaps=data_gaps,
        available_primitives=[],
    )
    store = _make_graph_store(asset_ids=[])
    bundle = _make_bundle(asset_ids=[])
    spec = SpecAssembler().assemble(opp, [], bundle, store, "build_x")
    assert spec.spec_type == "gap_brief"
    assert len(spec.blockers) == 2
    sources = {b.source for b in spec.blockers}
    assert "yaml_research" in sources


# ------------------------------------------------------------------ #
# Golden integration tests                                             #
# ------------------------------------------------------------------ #

def test_golden_spec_uds_is_full_spec(golden_spec_uds):
    assert golden_spec_uds.spec_type == "full_spec"


def test_golden_spec_uds_has_primitives(golden_spec_uds):
    assert len(golden_spec_uds.available_primitives) >= 1


def test_golden_spec_uds_has_spec_id(golden_spec_uds):
    assert len(golden_spec_uds.spec_id) == 16
    assert all(c in "0123456789abcdef" for c in golden_spec_uds.spec_id)


def test_golden_spec_uds_primitives_have_valid_maturity(golden_spec_uds):
    """All PrimitiveSummary entries must have a maturity_score in [0, 1]."""
    for p in golden_spec_uds.available_primitives:
        assert 0.0 <= p.maturity_score <= 1.0, (
            f"Primitive {p.primitive_id} has out-of-range maturity_score {p.maturity_score}"
        )


def test_all_initiatives_assemble_without_error(
    golden_bundle, golden_graph_store, golden_primitives, golden_opps
):
    assembler = SpecAssembler()
    for opp in golden_opps:
        spec = assembler.assemble(
            opp=opp,
            primitives=golden_primitives,
            bundle=golden_bundle,
            graph_store=golden_graph_store,
            graph_build_id="test_build",
        )
        assert isinstance(spec, SpecDocument)
        assert spec.initiative_id == opp.initiative_id


# ------------------------------------------------------------------ #
# DataRequisite / output structure behaviour tests                     #
# (OutputStructure is now internal — assertions go via DataRequisite)  #
# ------------------------------------------------------------------ #

def test_data_requisite_not_none_for_full_spec_initiatives(
    golden_bundle, golden_graph_store, golden_primitives, golden_opps
):
    """Every full_spec initiative must produce a non-None data_requisite."""
    assembler = SpecAssembler()
    for opp in golden_opps:
        if opp.readiness not in ("ready_now", "ready_with_enablement"):
            continue
        spec = assembler.assemble(
            opp=opp, primitives=golden_primitives, bundle=golden_bundle,
            graph_store=golden_graph_store, graph_build_id="test_build",
        )
        assert spec.data_requisite is not None, (
            f"data_requisite is None for full_spec {opp.initiative_id}"
        )


def test_gap_brief_has_no_data_requisite():
    """gap_brief initiatives with no primitives must have data_requisite = None."""
    opp = _make_opp(
        readiness="not_currently_feasible",
        yaml_data_gaps=[{"gap_type": "missing_source_system", "description": "No data"}],
        available_primitives=[],
        output_type="monitoring_dashboard",
    )
    store = _make_graph_store(asset_ids=[])
    bundle = _make_bundle(asset_ids=[])
    spec = SpecAssembler().assemble(opp, [], bundle, store, "build_x")
    assert spec.spec_type == "gap_brief"
    assert spec.data_requisite is None


def test_monitoring_dashboard_data_requisite_has_measures(
    golden_bundle, golden_graph_store, golden_primitives, golden_opps
):
    """Monitoring dashboard full_specs must have at least one measure column."""
    assembler = SpecAssembler()
    opp_by_id = {o.initiative_id: o for o in golden_opps}
    for init_id in ("pricing_adequacy_monitoring", "portfolio_drift_monitoring"):
        opp = opp_by_id.get(init_id)
        if opp is None:
            continue
        spec = assembler.assemble(
            opp=opp, primitives=golden_primitives, bundle=golden_bundle,
            graph_store=golden_graph_store, graph_build_id="test_build",
        )
        dr = spec.data_requisite
        assert dr is not None
        measures = [c for c in dr.columns if c.role == "measure"]
        assert measures, f"No measure-role columns in DataRequisite for {init_id}"


def test_prediction_gap_brief_has_outcome_label_blocker(
    golden_bundle, golden_graph_store, golden_primitives, golden_opps
):
    """claims_severity_prediction must have an insufficient_outcome_labels blocker."""
    assembler = SpecAssembler()
    opp_by_id = {o.initiative_id: o for o in golden_opps}
    opp = opp_by_id.get("claims_severity_prediction")
    if opp is None:
        pytest.skip("claims_severity_prediction not found")
    spec = assembler.assemble(
        opp=opp, primitives=golden_primitives, bundle=golden_bundle,
        graph_store=golden_graph_store, graph_build_id="test_build",
    )
    assert spec.spec_type == "gap_brief"
    gap_types = {b.gap_type for b in spec.blockers}
    assert "insufficient_outcome_labels" in gap_types


def test_data_requisite_grain_keys_nonempty_for_full_specs(
    golden_bundle, golden_graph_store, golden_primitives, golden_opps
):
    """Full_spec initiatives with a primary source asset must have non-empty grain_keys."""
    assembler = SpecAssembler()
    for opp in golden_opps:
        if opp.readiness not in ("ready_now", "ready_with_enablement"):
            continue
        spec = assembler.assemble(
            opp=opp, primitives=golden_primitives, bundle=golden_bundle,
            graph_store=golden_graph_store, graph_build_id="test_build",
        )
        dr = spec.data_requisite
        if dr and dr.primary_source_asset:
            assert dr.grain_keys, f"Empty grain_keys for {opp.initiative_id}"


def test_data_requisite_grain_description_nonempty(
    golden_bundle, golden_graph_store, golden_primitives, golden_opps
):
    """Every full_spec data_requisite must have a non-empty grain_description."""
    assembler = SpecAssembler()
    for opp in golden_opps:
        if opp.readiness not in ("ready_now", "ready_with_enablement"):
            continue
        spec = assembler.assemble(
            opp=opp, primitives=golden_primitives, bundle=golden_bundle,
            graph_store=golden_graph_store, graph_build_id="test_build",
        )
        dr = spec.data_requisite
        if dr is not None:
            assert dr.grain_description, f"Empty grain_description for {opp.initiative_id}"


# ------------------------------------------------------------------ #
# _compute_grain_description unit tests                                #
# ------------------------------------------------------------------ #

@pytest.mark.parametrize("keys,expected", [
    (["quote_id"], "one row per quote"),
    (["quote_id", "layer_id"], "one row per layer per quote"),
    (["quote_id", "layer_id", "pas_id"], "one row per layer per quote per policy system record"),
    (["coverage_id", "layer_id", "quote_id", "pas_id"], "one row per coverage per layer per quote"),
    ([], "grain not determined"),
])
def test_compute_grain_description(keys, expected):
    assert _compute_grain_description(keys) == expected


# ------------------------------------------------------------------ #
# DataRequisite column content tests                                   #
# ------------------------------------------------------------------ #

def test_data_requisite_no_pdm_timestamp(
    golden_bundle, golden_graph_store, golden_primitives, golden_opps
):
    """_pdm_last_update_timestamp must never appear as a requisite column."""
    assembler = SpecAssembler()
    for opp in golden_opps:
        if opp.readiness not in ("ready_now", "ready_with_enablement"):
            continue
        spec = assembler.assemble(
            opp=opp, primitives=golden_primitives, bundle=golden_bundle,
            graph_store=golden_graph_store, graph_build_id="test_build",
        )
        dr = spec.data_requisite
        if dr is not None:
            names = [c.column_name for c in dr.columns]
            assert "_pdm_last_update_timestamp" not in names, (
                f"_pdm_last_update_timestamp found in DataRequisite for {opp.initiative_id}"
            )


def test_uds_data_requisite_has_time_columns(golden_spec_uds):
    """underwriting_decision_support must have at least one time-role column in its
    DataRequisite — business time columns exist even though the pipeline watermark
    (_pdm_last_update_timestamp) is correctly excluded."""
    dr = golden_spec_uds.data_requisite
    assert dr is not None
    time_cols = [c for c in dr.columns if c.role == "time"]
    assert time_cols, "Expected at least one time-role column in UDS DataRequisite"


def test_product_line_primary_source_not_rating_asset(
    golden_bundle, golden_graph_store, golden_primitives, golden_opps
):
    """product_line_performance_dashboard must not pick a deprioritised asset
    (one whose name contains _rating, _factor, _load, _war_, _ops_, _inputs, or _modifiers)."""
    assembler = SpecAssembler()
    opp_by_id = {o.initiative_id: o for o in golden_opps}
    opp = opp_by_id.get("product_line_performance_dashboard")
    if opp is None:
        pytest.skip("product_line_performance_dashboard not found")
    spec = assembler.assemble(
        opp=opp, primitives=golden_primitives, bundle=golden_bundle,
        graph_store=golden_graph_store, graph_build_id="test_build",
    )
    dr = spec.data_requisite
    assert dr is not None
    primary = dr.primary_source_asset or ""
    deprioritised = ("_rating", "_factor", "_load", "_war_", "_ops_", "_inputs", "_modifiers")
    for pattern in deprioritised:
        assert pattern not in primary, (
            f"primary_source_asset {primary!r} contains deprioritised pattern {pattern!r}"
        )


# ------------------------------------------------------------------ #
# FIX 3: positive inclusion filter tests                              #
# ------------------------------------------------------------------ #

def test_measures_positive_inclusion_excludes_no_signal_columns():
    """A numeric column with no description, no semantic_candidate, and not in
    primitive matched_columns must be excluded from measures."""
    asset = AssetDetail(
        asset_id="a1",
        name="test_asset",
        grain_keys=["quote_id"],
        upstream_dependents=5,
        columns=[
            ColumnDetail(name="junk_col",      data_type_family="float",
                         column_role="numeric_attribute",
                         description=None, semantic_candidates=[]),
            ColumnDetail(name="described_col", data_type_family="float",
                         column_role="numeric_attribute",
                         description="A useful measure", semantic_candidates=[]),
            ColumnDetail(name="prim_col",      data_type_family="float",
                         column_role="numeric_attribute",
                         description=None, semantic_candidates=[]),
        ],
    )
    result = _collect_output_columns(
        [asset],
        role_filter={"numeric_attribute"},
        limit=10,
        prefer_names={"prim_col"},
    )
    names = [c.name for c in result]
    assert "junk_col" not in names, "Column with no signal must be excluded"
    assert "described_col" in names, "Column with description must be included"
    assert "prim_col" in names, "Column in primitive matched_columns must be included"


def test_dimensions_positive_inclusion_excludes_no_signal_columns():
    """A categorical column with no description, no semantic_candidate, and not in
    primitive matched_columns must be excluded from dimensions."""
    asset = AssetDetail(
        asset_id="a1",
        name="test_asset",
        grain_keys=["quote_id"],
        upstream_dependents=5,
        columns=[
            ColumnDetail(name="mystery_flag", data_type_family="string",
                         column_role="categorical_attribute",
                         description=None, semantic_candidates=[]),
            ColumnDetail(name="new_renewal",  data_type_family="string",
                         column_role="categorical_attribute",
                         description="New or renewal indicator", semantic_candidates=[]),
        ],
    )
    result = _collect_output_columns(
        [asset],
        role_filter={"categorical_attribute", "attribute"},
        limit=10,
        prefer_names=set(),
    )
    names = [c.name for c in result]
    assert "mystery_flag" not in names, "Undescribed dimension with no signal must be excluded"
    assert "new_renewal" in names, "Dimension with description must be included"


# ------------------------------------------------------------------ #
# DataRequisite tests                                                  #
# ------------------------------------------------------------------ #

def _make_two_asset_setup():
    """Return (opp, primitives, bundle, store) for a primary + joinable supporting asset."""
    prov = _make_prov()
    opp = _make_opp(available_primitives=["prim_primary", "prim_support"])

    store = MagicMock()
    store._nodes = {
        "primary_001": {
            "node_id": "primary_001", "label": "Asset", "build_id": "b",
            "properties": {
                "name": "primary_policy",
                "description": "Primary policy asset",
                "domain_candidates": ["underwriting"],
                "grain_keys": ["quote_id"],
            },
        },
        "support_001": {
            "node_id": "support_001", "label": "Asset", "build_id": "b",
            "properties": {
                "name": "support_coverage",
                "description": "Supporting coverage asset",
                "domain_candidates": ["underwriting"],
                "grain_keys": ["quote_id", "section"],
            },
        },
    }
    store._edges = {
        "e1": {"edge_id": "e1", "edge_type": "DEPENDS_ON",
               "source_node_id": "d1", "target_node_id": "primary_001"},
        "e2": {"edge_id": "e2", "edge_type": "DEPENDS_ON",
               "source_node_id": "d2", "target_node_id": "primary_001"},
    }

    assets = [
        CanonicalAsset(internal_id="primary_001", asset_type="dbt_model",
                       name="primary_policy", normalized_name="primary_policy",
                       grain_keys=["quote_id"], version_hash="a", provenance=prov),
        CanonicalAsset(internal_id="support_001", asset_type="dbt_model",
                       name="support_coverage", normalized_name="support_coverage",
                       grain_keys=["quote_id", "section"], version_hash="a", provenance=prov),
    ]
    columns = [
        CanonicalColumn(internal_id="c1", asset_internal_id="primary_001",
                        name="quote_id", normalized_name="quote_id",
                        data_type_family="string", column_role="identifier",
                        description="Unique quote identifier",
                        version_hash="a", provenance=prov),
        CanonicalColumn(internal_id="c2", asset_internal_id="primary_001",
                        name="gross_premium", normalized_name="gross_premium",
                        data_type_family="float", column_role="measure",
                        description="Gross written premium",
                        version_hash="a", provenance=prov),
        CanonicalColumn(internal_id="c3", asset_internal_id="support_001",
                        name="quote_id", normalized_name="quote_id",
                        data_type_family="string", column_role="identifier",
                        description="Unique quote identifier",
                        version_hash="a", provenance=prov),
        CanonicalColumn(internal_id="c4", asset_internal_id="support_001",
                        name="section", normalized_name="section",
                        data_type_family="string", column_role="categorical_attribute",
                        description="Business section grouping",
                        version_hash="a", provenance=prov),
    ]
    bundle = CanonicalBundle(assets=assets, columns=columns)

    prim_primary = CapabilityPrimitive(
        primitive_id="prim_primary", primitive_name="Primary Prim", description="",
        maturity_score=1.0, entity_score=1.0, column_score=1.0,
        matched_entities=[], missing_entities=[],
        matched_columns=["gross_premium"], missing_columns=[],
        supporting_asset_ids=["primary_001"],
    )
    prim_support = CapabilityPrimitive(
        primitive_id="prim_support", primitive_name="Support Prim", description="",
        maturity_score=1.0, entity_score=1.0, column_score=1.0,
        matched_entities=[], missing_entities=[],
        matched_columns=["section"], missing_columns=[],
        supporting_asset_ids=["support_001"],
    )

    return opp, [prim_primary, prim_support], bundle, store


def test_data_requisite_grain_keys_are_identifiers():
    """All grain keys must appear as identifier-role columns in data_requisite.columns."""
    opp, prims, bundle, store = _make_two_asset_setup()
    spec = SpecAssembler().assemble(opp, prims, bundle, store, "build_x")
    dr = spec.data_requisite
    assert dr is not None
    id_cols = [c for c in dr.columns if c.role == "identifier"]
    id_names = [c.column_name for c in id_cols]
    for gk in dr.grain_keys:
        assert gk in id_names, f"Grain key '{gk}' missing from identifier columns"


def test_data_requisite_identifier_columns_first():
    """Identifier-role columns must appear before any dimension/measure/time columns."""
    opp, prims, bundle, store = _make_two_asset_setup()
    spec = SpecAssembler().assemble(opp, prims, bundle, store, "build_x")
    dr = spec.data_requisite
    assert dr is not None
    first_non_id = next(
        (i for i, c in enumerate(dr.columns) if c.role != "identifier"), None
    )
    last_id = max(
        (i for i, c in enumerate(dr.columns) if c.role == "identifier"),
        default=-1,
    )
    if first_non_id is not None and last_id >= 0:
        assert last_id < first_non_id, (
            "All identifier columns must come before dimension/measure/time columns"
        )


def test_data_requisite_no_duplicate_grain_keys_in_dimensions():
    """Grain key column names must not also appear as dimension-role columns."""
    opp, prims, bundle, store = _make_two_asset_setup()
    spec = SpecAssembler().assemble(opp, prims, bundle, store, "build_x")
    dr = spec.data_requisite
    assert dr is not None
    dim_names = {c.column_name for c in dr.columns if c.role == "dimension"}
    for gk in dr.grain_keys:
        assert gk not in dim_names, (
            f"Grain key '{gk}' must not appear as a dimension column"
        )


def test_data_requisite_joinable_asset_contributes_dimensions():
    """A supporting asset whose grain_keys include all primary grain keys should
    contribute its described categorical columns as join dimensions."""
    opp, prims, bundle, store = _make_two_asset_setup()
    spec = SpecAssembler().assemble(opp, prims, bundle, store, "build_x")
    dr = spec.data_requisite
    assert dr is not None
    dim_names = [c.column_name for c in dr.columns if c.role == "dimension"]
    assert "section" in dim_names, (
        "Categorical column from joinable supporting asset should appear as dimension"
    )
    section_col = next(c for c in dr.columns if c.column_name == "section")
    assert section_col.source_asset == "support_coverage"
    assert section_col.derivation == "join"


def test_data_requisite_build_complexity_single_table():
    """Single primary source with no joinable dims → build_complexity = single_table."""
    opp = _make_opp(available_primitives=["prim_a"])
    store = _make_graph_store()
    prov = _make_prov()
    bundle = CanonicalBundle(
        assets=[CanonicalAsset(internal_id="asset_001", asset_type="dbt_model",
                               name="asset_001", normalized_name="asset_001",
                               grain_keys=["quote_id"], version_hash="a", provenance=prov)],
        columns=[
            CanonicalColumn(internal_id="c1", asset_internal_id="asset_001",
                            name="quote_id", normalized_name="quote_id",
                            data_type_family="string", column_role="identifier",
                            version_hash="a", provenance=prov),
            CanonicalColumn(internal_id="c2", asset_internal_id="asset_001",
                            name="loss_ratio", normalized_name="loss_ratio",
                            data_type_family="float", column_role="measure",
                            description="Incurred loss ratio", version_hash="a", provenance=prov),
        ],
    )
    prim = _make_primitive("prim_a", assets=["asset_001"])
    spec = SpecAssembler().assemble(opp, [prim], bundle, store, "build_x")
    dr = spec.data_requisite
    assert dr is not None
    assert dr.build_complexity == "single_table"


def test_data_requisite_uds_has_identifiers(golden_spec_uds):
    """underwriting_decision_support data_requisite must contain identifier columns
    for each grain key."""
    dr = golden_spec_uds.data_requisite
    assert dr is not None, "data_requisite should be set for full_spec"
    id_names = {c.column_name for c in dr.columns if c.role == "identifier"}
    for gk in dr.grain_keys:
        assert gk in id_names, (
            f"Grain key '{gk}' not found as identifier in data_requisite.columns"
        )


# ------------------------------------------------------------------ #
# _infer_table_type tests                                              #
# ------------------------------------------------------------------ #

def _make_infer_asset(grain_keys=None, lineage_layers=None):
    """Minimal CanonicalAsset for _infer_table_type tests."""
    from ingestion.contracts.asset import CanonicalAsset, Provenance
    prov = Provenance(source_system="test", source_type="test")
    tag_dims = {"lineage_layer": list(lineage_layers)} if lineage_layers else {}
    return CanonicalAsset(
        internal_id="test_asset",
        asset_type="dbt_model",
        name="test_asset",
        normalized_name="test_asset",
        grain_keys=grain_keys or [],
        tag_dimensions=tag_dims,
        version_hash="abc",
        provenance=prov,
    )


def _make_infer_columns(n_fact=0, n_dim=0, n_id=0):
    """Build a synthetic CanonicalColumn list with the given role counts."""
    from ingestion.contracts.asset import CanonicalColumn, Provenance
    prov = Provenance(source_system="test", source_type="test")
    cols = []
    for i in range(n_fact):
        cols.append(CanonicalColumn(
            internal_id=f"f{i}", asset_internal_id="test_asset",
            name=f"fact_{i}", normalized_name=f"fact_{i}",
            data_type_family="float", column_role="measure",
            version_hash="abc", provenance=prov,
        ))
    for i in range(n_dim):
        cols.append(CanonicalColumn(
            internal_id=f"d{i}", asset_internal_id="test_asset",
            name=f"dim_{i}", normalized_name=f"dim_{i}",
            data_type_family="string", column_role="categorical_attribute",
            version_hash="abc", provenance=prov,
        ))
    for i in range(n_id):
        cols.append(CanonicalColumn(
            internal_id=f"i{i}", asset_internal_id="test_asset",
            name=f"id_{i}", normalized_name=f"id_{i}",
            data_type_family="string", column_role="identifier",
            version_hash="abc", provenance=prov,
        ))
    return cols


def test_infer_table_type_snapshot_from_lineage():
    """Assets with 'historic_exchange' in lineage_layers must return 'snapshot'."""
    assembler = SpecAssembler()
    store = _make_graph_store()
    asset = _make_infer_asset(grain_keys=["quote_id"], lineage_layers=["historic_exchange"])
    cols = _make_infer_columns(n_fact=8, n_dim=1, n_id=1)
    assert assembler._infer_table_type(asset, cols, store) == "snapshot"


def test_infer_table_type_source_from_lineage():
    """Assets with 'source_table' in lineage_layers must return 'source'."""
    assembler = SpecAssembler()
    store = _make_graph_store()
    asset = _make_infer_asset(grain_keys=["quote_id"], lineage_layers=["source_table"])
    cols = _make_infer_columns(n_fact=5, n_dim=5, n_id=1)
    assert assembler._infer_table_type(asset, cols, store) == "source"


def test_infer_table_type_secondary_layer_tag_matches():
    """Signal 1 must scan all layer tags — a secondary 'source_table' tag classifies
    as 'source' even when the primary tag (liberty_link) doesn't match _LAYER_TO_TYPE."""
    assembler = SpecAssembler()
    store = _make_graph_store()
    asset = _make_infer_asset(
        grain_keys=["quote_id"],
        lineage_layers=["liberty_link", "source_table"],
    )
    cols = _make_infer_columns(n_fact=5, n_dim=5, n_id=1)
    assert assembler._infer_table_type(asset, cols, store) == "source"


def test_infer_table_type_bridge_from_grain_count():
    """Assets with 4+ grain keys must return 'bridge' regardless of composition."""
    assembler = SpecAssembler()
    store = _make_graph_store()
    asset = _make_infer_asset(
        grain_keys=["coverage_id", "layer_id", "quote_id", "pas_id"],
        lineage_layers=["liberty_link"],
    )
    cols = _make_infer_columns(n_fact=4, n_dim=10, n_id=4)
    assert assembler._infer_table_type(asset, cols, store) == "bridge"


def test_infer_table_type_fact_from_composition():
    """Assets with predominantly numeric columns must return 'fact'."""
    assembler = SpecAssembler()
    store = _make_graph_store()
    asset = _make_infer_asset(grain_keys=["quote_id"], lineage_layers=[])
    cols = _make_infer_columns(n_fact=18, n_dim=2, n_id=1)   # 86% fact
    assert assembler._infer_table_type(asset, cols, store) == "fact"


def test_infer_table_type_dimension_from_composition():
    """Assets with predominantly categorical columns must return 'dimension'."""
    assembler = SpecAssembler()
    store = _make_graph_store()
    asset = _make_infer_asset(grain_keys=["quote_id"], lineage_layers=[])
    cols = _make_infer_columns(n_fact=0, n_dim=8, n_id=1)   # 89% dim
    assert assembler._infer_table_type(asset, cols, store) == "dimension"


def test_data_requisite_table_type_not_none_for_full_specs(
    golden_bundle, golden_graph_store, golden_primitives, golden_opps
):
    """Every full_spec data_requisite must have a non-None, non-empty table_type."""
    assembler = SpecAssembler()
    for opp in golden_opps:
        if opp.readiness not in ("ready_now", "ready_with_enablement"):
            continue
        spec = assembler.assemble(
            opp=opp, primitives=golden_primitives, bundle=golden_bundle,
            graph_store=golden_graph_store, graph_build_id="test_build",
        )
        dr = spec.data_requisite
        if dr is not None:
            assert dr.table_type, (
                f"data_requisite.table_type is empty for {opp.initiative_id}"
            )
            assert dr.table_type != "None", (
                f"data_requisite.table_type is 'None' string for {opp.initiative_id}"
            )


def test_infer_table_type_no_name_patterns():
    """_infer_table_type must not contain name-based string literals like
    '_setup' or '_monitoring' in its source — pure signal-based classification."""
    assembler = SpecAssembler()
    src = inspect.getsource(assembler._infer_table_type)
    forbidden = ["_setup", "_monitoring"]
    for pattern in forbidden:
        assert pattern not in src, (
            f"_infer_table_type contains name pattern {pattern!r} — "
            "classification must use only structural signals"
        )


# ------------------------------------------------------------------ #
# JoinAssessment tests                                                 #
# ------------------------------------------------------------------ #

def _make_fact_dim_setup():
    """Two-asset setup where primary is fact-typed and support is dimension-typed.
    Both share the same grain (quote_id), so join should be safe."""
    prov = _make_prov()
    opp = _make_opp(available_primitives=["prim_primary", "prim_support"])

    store = MagicMock()
    store._nodes = {
        "fact_001": {
            "node_id": "fact_001", "label": "Asset", "build_id": "b",
            "properties": {
                "name": "fact_measures",
                "description": "Fact table",
                "domain_candidates": [],
                "grain_keys": ["quote_id"],
            },
        },
        "dim_001": {
            "node_id": "dim_001", "label": "Asset", "build_id": "b",
            "properties": {
                "name": "dim_setup",
                "description": "Dimension table",
                "domain_candidates": [],
                "grain_keys": ["quote_id"],
            },
        },
    }
    store._edges = {
        "e1": {"edge_id": "e1", "edge_type": "DEPENDS_ON",
               "source_node_id": "x1", "target_node_id": "fact_001"},
        "e2": {"edge_id": "e2", "edge_type": "DEPENDS_ON",
               "source_node_id": "x2", "target_node_id": "fact_001"},
        "e3": {"edge_id": "e3", "edge_type": "DEPENDS_ON",
               "source_node_id": "x3", "target_node_id": "dim_001"},
        "e4": {"edge_id": "e4", "edge_type": "DEPENDS_ON",
               "source_node_id": "x4", "target_node_id": "dim_001"},
        "e5": {"edge_id": "e5", "edge_type": "DEPENDS_ON",
               "source_node_id": "x5", "target_node_id": "dim_001"},
    }

    # fact_measures: mostly numeric_attribute → fact
    # dim_setup: mostly categorical_attribute → dimension
    assets = [
        CanonicalAsset(internal_id="fact_001", asset_type="dbt_model",
                       name="fact_measures", normalized_name="fact_measures",
                       grain_keys=["quote_id"],
                       tag_dimensions={"lineage_layer": ["gen2_mart"]},
                       version_hash="a", provenance=prov),
        CanonicalAsset(internal_id="dim_001", asset_type="dbt_model",
                       name="dim_setup", normalized_name="dim_setup",
                       grain_keys=["quote_id"],
                       tag_dimensions={"lineage_layer": ["liberty_link"]},
                       version_hash="a", provenance=prov),
    ]
    columns = [
        CanonicalColumn(internal_id="c1", asset_internal_id="fact_001",
                        name="quote_id", normalized_name="quote_id",
                        data_type_family="string", column_role="identifier",
                        description="Quote identifier", version_hash="a", provenance=prov),
        # 12 numeric columns → fact (12/14 = 86%)
        *[
            CanonicalColumn(internal_id=f"fm{i}", asset_internal_id="fact_001",
                            name=f"metric_{i}", normalized_name=f"metric_{i}",
                            data_type_family="float", column_role="numeric_attribute",
                            description=f"Metric {i}", version_hash="a", provenance=prov)
            for i in range(12)
        ],
        CanonicalColumn(internal_id="c2", asset_internal_id="dim_001",
                        name="quote_id", normalized_name="quote_id",
                        data_type_family="string", column_role="identifier",
                        description="Quote identifier", version_hash="a", provenance=prov),
        # 8 categorical columns → dimension (8/9 = 89%)
        *[
            CanonicalColumn(internal_id=f"dd{i}", asset_internal_id="dim_001",
                            name=f"attr_{i}", normalized_name=f"attr_{i}",
                            data_type_family="string", column_role="categorical_attribute",
                            description=f"Attribute {i}", version_hash="a", provenance=prov)
            for i in range(8)
        ],
    ]
    bundle = CanonicalBundle(assets=assets, columns=columns)

    prim_fact = CapabilityPrimitive(
        primitive_id="prim_primary", primitive_name="Fact Prim", description="",
        maturity_score=1.0, entity_score=1.0, column_score=1.0,
        matched_entities=[], missing_entities=[],
        matched_columns=["metric_0"], missing_columns=[],
        supporting_asset_ids=["fact_001"],
    )
    prim_dim = CapabilityPrimitive(
        primitive_id="prim_support", primitive_name="Dim Prim", description="",
        maturity_score=1.0, entity_score=1.0, column_score=1.0,
        matched_entities=[], missing_entities=[],
        matched_columns=["attr_0"], missing_columns=[],
        supporting_asset_ids=["dim_001"],
    )
    return opp, [prim_fact, prim_dim], bundle, store


def test_join_assessments_nonempty_for_multi_source():
    """When build_complexity != 'single_table', join_assessments must be non-empty."""
    opp, prims, bundle, store = _make_fact_dim_setup()
    spec = SpecAssembler().assemble(opp, prims, bundle, store, "build_x")
    dr = spec.data_requisite
    assert dr is not None
    assert dr.build_complexity != "single_table"
    assert len(dr.join_assessments) > 0, (
        "join_assessments must be populated when multiple source assets are joined"
    )


def test_join_assessment_fact_to_dimension_safe():
    """fact_to_dimension join with identical grain → join_safety == 'safe'."""
    opp, prims, bundle, store = _make_fact_dim_setup()
    spec = SpecAssembler().assemble(opp, prims, bundle, store, "build_x")
    dr = spec.data_requisite
    assert dr is not None
    ja_list = [ja for ja in dr.join_assessments if ja.join_direction == "fact_to_dimension"]
    assert ja_list, "Expected at least one fact_to_dimension join assessment"
    ja = ja_list[0]
    assert ja.grain_match is True
    assert ja.join_safety == "safe"


def test_join_assessment_fact_to_fact_risky():
    """When both assets are inferred as 'fact', join_safety must be 'risky'."""
    prov = _make_prov()
    opp = _make_opp(available_primitives=["prim_a", "prim_b"])
    store = MagicMock()
    store._nodes = {
        "fa": {
            "node_id": "fa", "label": "Asset", "build_id": "b",
            "properties": {"name": "fact_a", "description": "", "domain_candidates": [],
                           "grain_keys": ["quote_id"]},
        },
        "fb": {
            "node_id": "fb", "label": "Asset", "build_id": "b",
            "properties": {"name": "fact_b", "description": "", "domain_candidates": [],
                           "grain_keys": ["quote_id"]},
        },
    }
    store._edges = {
        "e1": {"edge_id": "e1", "edge_type": "DEPENDS_ON",
               "source_node_id": "x1", "target_node_id": "fa"},
        "e2": {"edge_id": "e2", "edge_type": "DEPENDS_ON",
               "source_node_id": "x2", "target_node_id": "fa"},
    }
    # Both assets are >55% numeric → both "fact"
    def _fact_cols(asset_id, n=10):
        return [
            CanonicalColumn(
                internal_id=f"{asset_id}_m{i}", asset_internal_id=asset_id,
                name=f"measure_{asset_id}_{i}", normalized_name=f"measure_{asset_id}_{i}",
                data_type_family="float", column_role="measure",
                description=f"Measure {i}", version_hash="a", provenance=prov,
            )
            for i in range(n)
        ]
    assets = [
        CanonicalAsset(internal_id="fa", asset_type="dbt_model",
                       name="fact_a", normalized_name="fact_a",
                       grain_keys=["quote_id"], version_hash="a", provenance=prov),
        CanonicalAsset(internal_id="fb", asset_type="dbt_model",
                       name="fact_b", normalized_name="fact_b",
                       grain_keys=["quote_id"], version_hash="a", provenance=prov),
    ]
    columns = (
        [CanonicalColumn(internal_id="id_a", asset_internal_id="fa",
                         name="quote_id", normalized_name="quote_id",
                         data_type_family="string", column_role="identifier",
                         version_hash="a", provenance=prov)]
        + _fact_cols("fa")
        + [CanonicalColumn(internal_id="id_b", asset_internal_id="fb",
                           name="quote_id", normalized_name="quote_id",
                           data_type_family="string", column_role="identifier",
                           version_hash="a", provenance=prov)]
        + _fact_cols("fb")
    )
    bundle = CanonicalBundle(assets=assets, columns=columns)
    prim_a = CapabilityPrimitive(
        primitive_id="prim_a", primitive_name="Prim A", description="",
        maturity_score=1.0, entity_score=1.0, column_score=1.0,
        matched_entities=[], missing_entities=[],
        matched_columns=["measure_fa_0"], missing_columns=[],
        supporting_asset_ids=["fa"],
    )
    prim_b = CapabilityPrimitive(
        primitive_id="prim_b", primitive_name="Prim B", description="",
        maturity_score=1.0, entity_score=1.0, column_score=1.0,
        matched_entities=[], missing_entities=[],
        matched_columns=["measure_fb_0"], missing_columns=[],
        supporting_asset_ids=["fb"],
    )
    spec = SpecAssembler().assemble(opp, [prim_a, prim_b], bundle, store, "build_x")
    dr = spec.data_requisite
    assert dr is not None
    # Only risky joins expected (fact-to-fact)
    risky = [ja for ja in dr.join_assessments if ja.join_direction == "fact_to_fact"]
    if risky:
        assert all(ja.join_safety == "risky" for ja in risky)


def test_join_assessment_aggregation_needed_when_right_grain_finer():
    """aggregation_needed must be True when right asset has more grain keys than left."""
    opp, prims, bundle, store = _make_two_asset_setup()
    spec = SpecAssembler().assemble(opp, prims, bundle, store, "build_x")
    dr = spec.data_requisite
    assert dr is not None
    # support_coverage has grain [quote_id, section] — finer than primary [quote_id]
    finer = [ja for ja in dr.join_assessments if ja.right_asset == "support_coverage"]
    assert finer, "Expected join assessment for support_coverage"
    assert finer[0].aggregation_needed is True


def test_join_assessments_empty_for_single_table():
    """Single-source initiatives must produce an empty join_assessments list."""
    opp = _make_opp(available_primitives=["prim_a"])
    store = _make_graph_store()
    prov = _make_prov()
    bundle = CanonicalBundle(
        assets=[CanonicalAsset(internal_id="asset_001", asset_type="dbt_model",
                               name="solo_asset", normalized_name="solo_asset",
                               grain_keys=["quote_id"], version_hash="a", provenance=prov)],
        columns=[
            CanonicalColumn(internal_id="c1", asset_internal_id="asset_001",
                            name="quote_id", normalized_name="quote_id",
                            data_type_family="string", column_role="identifier",
                            version_hash="a", provenance=prov),
            CanonicalColumn(internal_id="c2", asset_internal_id="asset_001",
                            name="net_premium", normalized_name="net_premium",
                            data_type_family="float", column_role="measure",
                            description="Net written premium", version_hash="a", provenance=prov),
        ],
    )
    prim = _make_primitive("prim_a", assets=["asset_001"])
    spec = SpecAssembler().assemble(opp, [prim], bundle, store, "build_x")
    dr = spec.data_requisite
    assert dr is not None
    assert dr.build_complexity == "single_table"
    assert dr.join_assessments == [], (
        "join_assessments must be empty for single_table initiatives"
    )
