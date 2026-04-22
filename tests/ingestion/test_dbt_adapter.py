"""Golden and unit tests for DbtMetadataAdapter."""

import json
import pytest
from pathlib import Path

from ingestion.adapters.dbt_metadata import DbtMetadataAdapter, DOMAIN_KEYWORDS

GOLDEN_PATH = Path(__file__).parent.parent.parent / "data" / "dbt_metadata_enriched.json"
MINIMAL_PATH = Path(__file__).parent / "golden" / "minimal_dbt_sample.json"
ALLOWED_DOMAINS = set(DOMAIN_KEYWORDS.keys())

VALID_COLUMN_ROLES = {
    "identifier", "measure", "categorical_attribute", "timestamp",
    "boolean_flag", "numeric_attribute", "attribute", "semi_structured", "unknown",
}


# ------------------------------------------------------------------ #
# Minimal regression fixture (always runs, no real data needed)        #
# ------------------------------------------------------------------ #

@pytest.fixture(scope="module")
def minimal_bundle():
    adapter = DbtMetadataAdapter()
    return adapter.parse_file(MINIMAL_PATH)


def test_minimal_asset_count(minimal_bundle):
    assert len(minimal_bundle.assets) == 2, (
        f"Expected exactly 2 assets, got {len(minimal_bundle.assets)}"
    )


def test_minimal_column_count(minimal_bundle):
    assert len(minimal_bundle.columns) == 6, (
        f"Expected exactly 6 columns, got {len(minimal_bundle.columns)}"
    )


def test_minimal_lineage_edge_count(minimal_bundle):
    assert len(minimal_bundle.lineage_edges) == 1, (
        f"Expected exactly 1 lineage edge, got {len(minimal_bundle.lineage_edges)}"
    )


def test_minimal_ids_start_with_asset(minimal_bundle):
    for asset in minimal_bundle.assets:
        assert asset.internal_id.startswith("asset_"), (
            f"Asset ID {asset.internal_id!r} does not start with 'asset_'"
        )


def test_minimal_column_ids_start_with_col(minimal_bundle):
    for col in minimal_bundle.columns:
        assert col.internal_id.startswith("col_"), (
            f"Column ID {col.internal_id!r} does not start with 'col_'"
        )


def test_minimal_determinism():
    """Parsing the same file twice must produce identical IDs."""
    adapter = DbtMetadataAdapter()
    b1 = adapter.parse_file(MINIMAL_PATH)
    b2 = adapter.parse_file(MINIMAL_PATH)
    assert sorted(a.internal_id for a in b1.assets) == sorted(a.internal_id for a in b2.assets)
    assert sorted(c.internal_id for c in b1.columns) == sorted(c.internal_id for c in b2.columns)
    assert sorted(e.internal_id for e in b1.lineage_edges) == sorted(e.internal_id for e in b2.lineage_edges)


# ------------------------------------------------------------------ #
# Golden dataset fixture (skipped if real data absent)                 #
# ------------------------------------------------------------------ #

@pytest.fixture(scope="module")
def golden_bundle():
    if not GOLDEN_PATH.exists():
        pytest.skip("Golden data not present at data/dbt_metadata_enriched.json")
    adapter = DbtMetadataAdapter()
    return adapter.parse_file(GOLDEN_PATH)


# --- count bounds (reflect confirmed real counts) ---

def test_golden_at_least_207_assets(golden_bundle):
    assert len(golden_bundle.assets) >= 207, (
        f"Expected >= 207 assets, got {len(golden_bundle.assets)}"
    )


def test_golden_at_least_201_lineage_edges(golden_bundle):
    assert len(golden_bundle.lineage_edges) >= 201, (
        f"Expected >= 201 lineage edges, got {len(golden_bundle.lineage_edges)}"
    )


# --- ID stability ---

def test_golden_ids_are_stable(golden_bundle):
    adapter = DbtMetadataAdapter()
    bundle2 = adapter.parse_file(GOLDEN_PATH)
    ids1 = {a.internal_id for a in golden_bundle.assets}
    ids2 = {a.internal_id for a in bundle2.assets}
    assert ids1 == ids2, "Asset IDs are not stable across two parse calls"

    col_ids1 = {c.internal_id for c in golden_bundle.columns}
    col_ids2 = {c.internal_id for c in bundle2.columns}
    assert col_ids1 == col_ids2, "Column IDs are not stable across two parse calls"


# --- ID format ---

def test_golden_all_asset_ids_start_with_asset(golden_bundle):
    bad = [a.internal_id for a in golden_bundle.assets if not a.internal_id.startswith("asset_")]
    assert not bad, f"Assets with malformed IDs: {bad[:5]}"


def test_golden_all_column_ids_start_with_col(golden_bundle):
    bad = [c.internal_id for c in golden_bundle.columns if not c.internal_id.startswith("col_")]
    assert not bad, f"Columns with malformed IDs: {bad[:5]}"


# --- domain candidates ---

def test_golden_domain_candidates_are_from_allowed_set(golden_bundle):
    for asset in golden_bundle.assets:
        for domain in asset.domain_candidates:
            assert domain in ALLOWED_DOMAINS, (
                f"Asset {asset.name!r} has invalid domain {domain!r}. "
                f"Allowed: {ALLOWED_DOMAINS}"
            )


# --- column roles ---

def test_golden_all_columns_have_valid_role(golden_bundle):
    for col in golden_bundle.columns:
        assert col.column_role in VALID_COLUMN_ROLES, (
            f"Column {col.name!r} has invalid role {col.column_role!r}"
        )


# --- Fix 4 regression: SUPER/ARRAY must never be classified as string ---

def test_golden_super_and_array_not_classified_as_string(golden_bundle):
    """Regression for dtypes.py fix: semi_structured check must come before string."""
    offenders = [
        c for c in golden_bundle.columns
        if c.raw_data_type
        and any(kw in c.raw_data_type.upper() for kw in ("ARRAY", "SUPER", "JSON", "STRUCT"))
        and c.data_type_family == "string"
    ]
    assert not offenders, (
        f"{len(offenders)} column(s) with ARRAY/SUPER/JSON dtype misclassified as string: "
        + ", ".join(f"{c.name}:{c.raw_data_type}" for c in offenders[:5])
    )


# --- lineage confidence ---

def test_golden_all_lineage_confidence_is_one(golden_bundle):
    for edge in golden_bundle.lineage_edges:
        assert edge.confidence == 1.0, (
            f"Lineage edge {edge.internal_id} has confidence {edge.confidence}, expected 1.0"
        )


# ------------------------------------------------------------------ #
# detect() tests                                                       #
# ------------------------------------------------------------------ #

def test_detect_compatible_on_golden_file():
    """detect() must return compatible=True on the real golden file."""
    if not GOLDEN_PATH.exists():
        pytest.skip("Golden data not present at data/dbt_metadata_enriched.json")
    adapter = DbtMetadataAdapter()
    result = adapter.detect(GOLDEN_PATH)
    assert result["compatible"] is True, f"detect() returned incompatible: {result}"
    assert result["variant"] == "entities_dict"
    assert result["missing_fields"] == []
    assert result["entity_count"] == 207


def test_detect_incompatible_on_empty_dict(tmp_path):
    """detect() must return compatible=False for an empty dict {}."""
    bad_file = tmp_path / "empty.json"
    bad_file.write_text("{}", encoding="utf-8")
    adapter = DbtMetadataAdapter()
    result = adapter.detect(bad_file)
    assert result["compatible"] is False
    assert len(result["missing_fields"]) > 0


def test_detect_variant_root_list(tmp_path):
    """detect() must return variant='root_list' when the root is a JSON array."""
    root_list_file = tmp_path / "root_list.json"
    root_list_file.write_text(
        json.dumps([
            {
                "unique_id": "model.p.a",
                "name": "a",
                "resource_type": "model",
                "columns": [],
                "upstream_dependencies": [],
            }
        ]),
        encoding="utf-8",
    )
    adapter = DbtMetadataAdapter()
    result = adapter.detect(root_list_file)
    assert result["compatible"] is True
    assert result["variant"] == "root_list"
    assert result["entity_count"] == 1


def test_detect_variant_entities_dict(tmp_path):
    """detect() must return variant='entities_dict' for {"entities": [...]} shape."""
    entities_dict_file = tmp_path / "entities_dict.json"
    entities_dict_file.write_text(
        json.dumps({
            "entities": [
                {
                    "unique_id": "model.p.b",
                    "name": "b",
                    "resource_type": "model",
                    "columns": [],
                    "upstream_dependencies": [],
                }
            ]
        }),
        encoding="utf-8",
    )
    adapter = DbtMetadataAdapter()
    result = adapter.detect(entities_dict_file)
    assert result["compatible"] is True
    assert result["variant"] == "entities_dict"


def test_detect_missing_columns_field(tmp_path):
    """detect() must flag missing_fields when 'columns' is absent."""
    bad_file = tmp_path / "no_columns.json"
    bad_file.write_text(
        json.dumps({
            "entities": [
                {"unique_id": "model.p.c", "name": "c", "upstream_dependencies": []}
            ]
        }),
        encoding="utf-8",
    )
    adapter = DbtMetadataAdapter()
    result = adapter.detect(bad_file)
    assert result["compatible"] is False
    assert "columns" in result["missing_fields"]


def test_detect_missing_name_and_unique_id(tmp_path):
    """detect() must flag missing_fields when both name and unique_id are absent."""
    bad_file = tmp_path / "no_name.json"
    bad_file.write_text(
        json.dumps({
            "entities": [
                {"resource_type": "model", "columns": [], "upstream_dependencies": []}
            ]
        }),
        encoding="utf-8",
    )
    adapter = DbtMetadataAdapter()
    result = adapter.detect(bad_file)
    assert result["compatible"] is False
    assert any("name" in f or "unique_id" in f for f in result["missing_fields"])


def test_detect_parse_file_raises_on_incompatible(tmp_path):
    """parse_file() must raise ValueError when detect() returns compatible=False."""
    bad_file = tmp_path / "bad.json"
    bad_file.write_text("{}", encoding="utf-8")
    adapter = DbtMetadataAdapter()
    with pytest.raises(ValueError, match="not compatible"):
        adapter.parse_file(bad_file)


def test_detect_minimal_sample_is_compatible():
    """The minimal golden sample must be detected as compatible."""
    adapter = DbtMetadataAdapter()
    result = adapter.detect(MINIMAL_PATH)
    assert result["compatible"] is True
    assert result["entity_count"] == 2


# ------------------------------------------------------------------ #
# product_lines and lineage_layer fields                               #
# ------------------------------------------------------------------ #

def _make_entity_file(tmp_path, tags):
    """Create a minimal entities dict with the given tags on the first entity."""
    f = tmp_path / "entity.json"
    f.write_text(json.dumps({
        "entities": [{
            "unique_id": "model.p.x",
            "name": "x",
            "resource_type": "model",
            "tags": tags,
            "columns": [],
            "upstream_dependencies": [],
        }]
    }), encoding="utf-8")
    return f


def test_product_lines_eupi_tag(tmp_path):
    """Tag 'eupi' must map to product_line 'european_professional_indemnity'."""
    adapter = DbtMetadataAdapter()
    bundle = adapter.parse_file(_make_entity_file(tmp_path, ["eupi"]))
    assert bundle.assets[0].product_lines == ["european_professional_indemnity"]


def test_product_lines_do_tag(tmp_path):
    """Tag 'd_o' must map to product_line 'directors_and_officers'."""
    adapter = DbtMetadataAdapter()
    bundle = adapter.parse_file(_make_entity_file(tmp_path, ["d_o"]))
    assert bundle.assets[0].product_lines == ["directors_and_officers"]


def test_product_lines_no_match(tmp_path):
    """Tags with no mapping must produce empty product_lines."""
    adapter = DbtMetadataAdapter()
    bundle = adapter.parse_file(_make_entity_file(tmp_path, ["raw", "quotes"]))
    assert bundle.assets[0].product_lines == []


def test_lineage_layers_hx_tag(tmp_path):
    """Tag 'HX' must map to lineage_layers ['historic_exchange']."""
    adapter = DbtMetadataAdapter()
    bundle = adapter.parse_file(_make_entity_file(tmp_path, ["HX"]))
    assert bundle.assets[0].lineage_layers == ["historic_exchange"]


def test_lineage_layers_ll_tag(tmp_path):
    """Tag 'LL' must map to lineage_layers ['liberty_link']."""
    adapter = DbtMetadataAdapter()
    bundle = adapter.parse_file(_make_entity_file(tmp_path, ["LL"]))
    assert bundle.assets[0].lineage_layers == ["liberty_link"]


def test_lineage_layers_no_match(tmp_path):
    """Tags with no layer mapping must produce empty lineage_layers."""
    adapter = DbtMetadataAdapter()
    bundle = adapter.parse_file(_make_entity_file(tmp_path, ["eupi"]))
    assert bundle.assets[0].lineage_layers == []


def test_lineage_layers_preserves_order(tmp_path):
    """When multiple layer tags are present, all are recorded in tag order."""
    adapter = DbtMetadataAdapter()
    bundle = adapter.parse_file(_make_entity_file(tmp_path, ["HX", "LL"]))
    assert bundle.assets[0].lineage_layers == ["historic_exchange", "liberty_link"]


def test_lineage_layers_preserves_conformance_tags(tmp_path):
    """Secondary tags like 'bookends' and 'semi_conformed' are no longer lost."""
    adapter = DbtMetadataAdapter()
    bundle = adapter.parse_file(_make_entity_file(tmp_path, ["HX", "bookends"]))
    assert bundle.assets[0].lineage_layers == ["historic_exchange", "conformed_bookends"]


def test_lineage_layers_deduplicates(tmp_path):
    """Duplicate tags producing the same layer must be deduplicated."""
    adapter = DbtMetadataAdapter()
    bundle = adapter.parse_file(_make_entity_file(tmp_path, ["HX", "hx"]))
    assert bundle.assets[0].lineage_layers == ["historic_exchange"]


def test_golden_product_lines_non_empty(golden_bundle):
    """At least some golden assets must have product_lines populated."""
    assets_with_pl = [a for a in golden_bundle.assets if a.product_lines]
    assert assets_with_pl, "Expected at least one asset with product_lines from golden data"


def test_golden_lineage_layers_non_empty(golden_bundle):
    """At least some golden assets must have lineage_layers populated."""
    assets_with_ll = [a for a in golden_bundle.assets if a.lineage_layers]
    assert assets_with_ll, "Expected at least one asset with lineage_layers from golden data"


def test_golden_product_lines_values_are_strings(golden_bundle):
    """All product_lines values must be non-empty strings."""
    for asset in golden_bundle.assets:
        for pl in asset.product_lines:
            assert isinstance(pl, str) and pl, (
                f"Asset {asset.name!r} has invalid product_line value: {pl!r}"
            )


def test_golden_lineage_layers_values_are_strings(golden_bundle):
    """Every lineage_layers entry must be a non-empty string."""
    for asset in golden_bundle.assets:
        for layer in asset.lineage_layers:
            assert isinstance(layer, str) and layer, (
                f"Asset {asset.name!r} has invalid lineage_layers entry: {layer!r}"
            )
