"""Unit tests for ingestion/contracts/ models."""

import pytest
from pathlib import Path

from pydantic import ValidationError

from ingestion.contracts.asset import CanonicalAsset, CanonicalColumn, Provenance
from ingestion.contracts.bundle import CanonicalBundle
from ingestion.contracts.business import CanonicalBusinessTerm
from ingestion.contracts.lineage import CanonicalLineageEdge
from ingestion.normalisation.hashing import stable_hash


def _make_provenance() -> Provenance:
    return Provenance(
        source_system="test",
        source_type="TestAdapter",
        source_native_id="native-1",
        raw_record_hash=stable_hash("test"),
    )


def _make_asset(suffix: str = "a") -> CanonicalAsset:
    return CanonicalAsset(
        internal_id=f"asset_{suffix}",
        asset_type="dbt_model",
        name=f"model_{suffix}",
        normalized_name=f"model_{suffix}",
        version_hash=stable_hash(f"asset_{suffix}"),
        provenance=_make_provenance(),
    )


def _make_column(asset_id: str, col_name: str = "col1") -> CanonicalColumn:
    return CanonicalColumn(
        internal_id=f"col_{stable_hash(asset_id, col_name)}",
        asset_internal_id=asset_id,
        name=col_name,
        normalized_name=col_name,
        data_type_family="string",
        column_role="attribute",
        version_hash=stable_hash(asset_id, col_name),
        provenance=_make_provenance(),
    )


def _make_edge(src: str, tgt: str) -> CanonicalLineageEdge:
    return CanonicalLineageEdge(
        internal_id=f"lin_{stable_hash(src, tgt)}",
        source_asset_id=src,
        target_asset_id=tgt,
        relation_type="depends_on",
        derivation_method="explicit_metadata",
        confidence=1.0,
        version_hash=stable_hash(src, tgt),
        provenance=_make_provenance(),
    )


def _make_term(name: str) -> CanonicalBusinessTerm:
    return CanonicalBusinessTerm(
        internal_id=f"term_{stable_hash('test', name)}",
        term_type="conformed_concept",
        name=name,
        normalized_name=name.lower(),
        version_hash=stable_hash(name),
        provenance=_make_provenance(),
    )


# ------------------------------------------------------------------ #
# Round-trip JSON serialisation                                         #
# ------------------------------------------------------------------ #

def test_bundle_json_round_trip(tmp_path: Path) -> None:
    asset = _make_asset("1")
    col = _make_column(asset.internal_id)
    edge = _make_edge("asset_x", "asset_1")
    term = _make_term("quote")

    bundle = CanonicalBundle(
        assets=[asset],
        columns=[col],
        lineage_edges=[edge],
        business_terms=[term],
        metadata={"source": "test"},
    )

    out = tmp_path / "bundle.json"
    bundle.to_json(out)

    loaded = CanonicalBundle.from_json(out)
    assert len(loaded.assets) == 1
    assert len(loaded.columns) == 1
    assert len(loaded.lineage_edges) == 1
    assert len(loaded.business_terms) == 1
    assert loaded.assets[0].internal_id == asset.internal_id
    assert loaded.columns[0].internal_id == col.internal_id
    assert loaded.lineage_edges[0].internal_id == edge.internal_id
    assert loaded.metadata["source"] == "test"


def test_bundle_round_trip_is_equal(tmp_path: Path) -> None:
    bundle = CanonicalBundle(
        assets=[_make_asset("a"), _make_asset("b")],
        columns=[_make_column("asset_a"), _make_column("asset_b")],
        lineage_edges=[_make_edge("asset_a", "asset_b")],
        business_terms=[_make_term("policy")],
        metadata={"run_id": "42"},
    )
    out = tmp_path / "b.json"
    bundle.to_json(out)
    loaded = CanonicalBundle.from_json(out)
    # Compare counts (extraction timestamps will differ)
    assert len(loaded.assets) == len(bundle.assets)
    assert len(loaded.columns) == len(bundle.columns)
    assert len(loaded.lineage_edges) == len(bundle.lineage_edges)
    assert len(loaded.business_terms) == len(bundle.business_terms)


# ------------------------------------------------------------------ #
# merge                                                                 #
# ------------------------------------------------------------------ #

def test_bundle_merge_sums_counts() -> None:
    b1 = CanonicalBundle(
        assets=[_make_asset("1"), _make_asset("2")],
        columns=[_make_column("asset_1")],
        lineage_edges=[],
        business_terms=[_make_term("quote")],
    )
    b2 = CanonicalBundle(
        assets=[_make_asset("3")],
        columns=[_make_column("asset_3"), _make_column("asset_3", "col2")],
        lineage_edges=[_make_edge("asset_1", "asset_3")],
        business_terms=[],
    )
    merged = b1.merge(b2)
    assert len(merged.assets) == 3
    assert len(merged.columns) == 3
    assert len(merged.lineage_edges) == 1
    assert len(merged.business_terms) == 1


def test_bundle_merge_merges_metadata() -> None:
    b1 = CanonicalBundle(metadata={"a": 1})
    b2 = CanonicalBundle(metadata={"b": 2})
    merged = b1.merge(b2)
    assert merged.metadata == {"a": 1, "b": 2}


# ------------------------------------------------------------------ #
# CanonicalLineageEdge confidence validator                             #
# ------------------------------------------------------------------ #

def test_lineage_edge_confidence_rejects_out_of_range() -> None:
    with pytest.raises(ValidationError):
        _make_edge_with_confidence(1.5)
    with pytest.raises(ValidationError):
        _make_edge_with_confidence(-0.1)


def test_lineage_edge_confidence_accepts_boundaries() -> None:
    assert _make_edge_with_confidence(0.0).confidence == 0.0
    assert _make_edge_with_confidence(1.0).confidence == 1.0


def _make_edge_with_confidence(conf: float) -> CanonicalLineageEdge:
    return CanonicalLineageEdge(
        internal_id="e1",
        source_asset_id="asset_a",
        target_asset_id="asset_b",
        relation_type="depends_on",
        derivation_method="explicit_metadata",
        confidence=conf,
        version_hash="vh",
        provenance=_make_provenance(),
    )


# ------------------------------------------------------------------ #
# Optional fields accept None                                          #
# ------------------------------------------------------------------ #

def test_canonical_asset_optional_fields_accept_none() -> None:
    asset = CanonicalAsset(
        internal_id="a1",
        asset_type="unknown",
        name="x",
        normalized_name="x",
        database=None,
        schema_name=None,
        path=None,
        description=None,
        materialization=None,
        row_count=None,
        size_mb=None,
        version_hash="vh",
        provenance=_make_provenance(),
    )
    assert asset.description is None
    assert asset.database is None


def test_canonical_column_optional_fields_accept_none() -> None:
    col = CanonicalColumn(
        internal_id="c1",
        asset_internal_id="a1",
        name="col",
        normalized_name="col",
        description=None,
        raw_data_type=None,
        data_type_family="unknown",
        column_role="unknown",
        ordinal_position=None,
        is_nullable=None,
        version_hash="vh",
        provenance=_make_provenance(),
    )
    assert col.description is None
    assert col.raw_data_type is None


def test_canonical_business_term_optional_parent() -> None:
    term = _make_term("broker")
    assert term.parent_term_id is None


def test_provenance_optional_fields() -> None:
    p = Provenance(
        source_system="s",
        source_type="t",
        source_native_id=None,
        raw_record_hash=None,
    )
    assert p.source_native_id is None
    assert p.raw_record_hash is None
