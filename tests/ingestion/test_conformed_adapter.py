"""Tests for ConformedSchemaAdapter (JSON Schema draft-04 parser)."""

import json
import pytest
from pathlib import Path

from ingestion.adapters.conformed_schema import ConformedSchemaAdapter

GOLDEN_PATH = Path(__file__).parent.parent.parent / "data" / "conformed_schema.json"


# ------------------------------------------------------------------ #
# Minimal synthetic fixtures                                           #
# ------------------------------------------------------------------ #

MINIMAL_SCHEMA = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "conformed_data": {
                "type": "object",
                "properties": {
                    "coverage": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "quote_name": {"type": "string"},
                                "premium": {"type": "number"},
                            }
                        }
                    }
                }
            }
        }
    }
}

TOTALS_SCHEMA = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "conformed_data": {
                "type": "object",
                "properties": {
                    "policy_totals": {
                        "type": "object",
                        "properties": {
                            "100_percent_usd": {
                                "type": "object",
                                "properties": {
                                    "gross_premium": {"type": "number"},
                                    "net_premium": {"type": "number"},
                                }
                            },
                            "lsm_share_usd": {
                                "type": "object",
                                "properties": {
                                    "gross_premium": {"type": "number"},
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}


@pytest.fixture
def adapter():
    return ConformedSchemaAdapter()


@pytest.fixture
def minimal_bundle(adapter, tmp_path):
    f = tmp_path / "schema.json"
    f.write_text(json.dumps(MINIMAL_SCHEMA), encoding="utf-8")
    return adapter.parse_file(f)


@pytest.fixture
def totals_bundle(adapter, tmp_path):
    f = tmp_path / "totals.json"
    f.write_text(json.dumps(TOTALS_SCHEMA), encoding="utf-8")
    return adapter.parse_file(f)


# ------------------------------------------------------------------ #
# Basic term counts                                                    #
# ------------------------------------------------------------------ #

def test_minimal_emits_terms(minimal_bundle):
    """Array entity group should emit 1 group term + 2 field terms = 3 total."""
    assert len(minimal_bundle.business_terms) == 3


def test_minimal_no_assets(minimal_bundle):
    assert minimal_bundle.assets == []


def test_minimal_no_columns(minimal_bundle):
    assert minimal_bundle.columns == []


def test_minimal_group_term_has_no_parent(minimal_bundle):
    group_terms = [t for t in minimal_bundle.business_terms if t.parent_term_id is None]
    assert len(group_terms) == 1
    assert group_terms[0].name == "coverage"


def test_minimal_field_terms_have_group_parent(minimal_bundle):
    group_term = next(t for t in minimal_bundle.business_terms if t.parent_term_id is None)
    field_terms = [t for t in minimal_bundle.business_terms if t.parent_term_id == group_term.internal_id]
    assert len(field_terms) == 2
    field_names = {t.name for t in field_terms}
    assert field_names == {"quote_name", "premium"}


def test_minimal_term_ids_start_with_term(minimal_bundle):
    for term in minimal_bundle.business_terms:
        assert term.internal_id.startswith("term_"), (
            f"Term ID {term.internal_id!r} does not start with 'term_'"
        )


# ------------------------------------------------------------------ #
# Object-of-objects (totals) structure                                 #
# ------------------------------------------------------------------ #

def test_totals_top_level_group_emitted(totals_bundle):
    """policy_totals group-level term must be emitted with no parent."""
    top = [t for t in totals_bundle.business_terms if t.name == "policy_totals"]
    assert len(top) == 1
    assert top[0].parent_term_id is None


def test_totals_sub_groups_emitted(totals_bundle):
    """Each sub-group of policy_totals must have its own term."""
    top_id = next(t.internal_id for t in totals_bundle.business_terms if t.name == "policy_totals")
    sub_groups = [t for t in totals_bundle.business_terms if t.parent_term_id == top_id]
    sub_names = {t.name for t in sub_groups}
    assert sub_names == {"100_percent_usd", "lsm_share_usd"}


def test_totals_field_children_have_sub_group_parent(totals_bundle):
    """Fields inside a sub-group must have the sub-group term as their parent."""
    usd_group = next(t for t in totals_bundle.business_terms if t.name == "100_percent_usd")
    fields = [t for t in totals_bundle.business_terms if t.parent_term_id == usd_group.internal_id]
    assert len(fields) == 2
    field_names = {t.name for t in fields}
    assert field_names == {"gross_premium", "net_premium"}


def test_totals_total_term_count(totals_bundle):
    """1 top-level + 2 sub-groups + 3 fields (2+1) = 6 terms."""
    assert len(totals_bundle.business_terms) == 6


# ------------------------------------------------------------------ #
# Determinism                                                          #
# ------------------------------------------------------------------ #

def test_determinism(adapter, tmp_path):
    """Parsing the same file twice must produce identical IDs."""
    f = tmp_path / "schema.json"
    f.write_text(json.dumps(MINIMAL_SCHEMA), encoding="utf-8")
    b1 = adapter.parse_file(f)
    b2 = adapter.parse_file(f)
    ids1 = sorted(t.internal_id for t in b1.business_terms)
    ids2 = sorted(t.internal_id for t in b2.business_terms)
    assert ids1 == ids2


# ------------------------------------------------------------------ #
# detect() tests                                                       #
# ------------------------------------------------------------------ #

def test_detect_compatible_with_schema_key(adapter, tmp_path):
    f = tmp_path / "schema.json"
    f.write_text(json.dumps(MINIMAL_SCHEMA), encoding="utf-8")
    result = adapter.detect(f)
    assert result["compatible"] is True
    assert result["variant"] == "json_schema_draft04"


def test_detect_incompatible_without_schema_key(adapter, tmp_path):
    f = tmp_path / "no_schema.json"
    f.write_text(json.dumps({"type": "array", "items": {}}), encoding="utf-8")
    result = adapter.detect(f)
    assert result["compatible"] is False


def test_detect_entity_count(adapter, tmp_path):
    f = tmp_path / "schema.json"
    f.write_text(json.dumps(MINIMAL_SCHEMA), encoding="utf-8")
    result = adapter.detect(f)
    assert result["entity_count"] == 1  # one entity group: coverage


def test_detect_incompatible_on_non_json(adapter, tmp_path):
    f = tmp_path / "bad.txt"
    f.write_text("not json", encoding="utf-8")
    result = adapter.detect(f)
    assert result["compatible"] is False


# ------------------------------------------------------------------ #
# Golden dataset (skipped if real data absent)                         #
# ------------------------------------------------------------------ #

@pytest.fixture(scope="module")
def golden_bundle():
    if not GOLDEN_PATH.exists():
        pytest.skip("Golden data not present at data/conformed_schema.json")
    adapter = ConformedSchemaAdapter()
    return adapter.parse_file(GOLDEN_PATH)


def test_golden_business_terms_non_empty(golden_bundle):
    assert len(golden_bundle.business_terms) > 0


def test_golden_has_seven_entity_groups(golden_bundle):
    """Golden schema has 7 entity groups: coverage, policy, policy_totals, etc."""
    top_level = [t for t in golden_bundle.business_terms if t.parent_term_id is None]
    assert len(top_level) == 7


def test_golden_coverage_group_present(golden_bundle):
    names = {t.name for t in golden_bundle.business_terms}
    assert "coverage" in names


def test_golden_policy_totals_group_present(golden_bundle):
    names = {t.name for t in golden_bundle.business_terms}
    assert "policy_totals" in names


def test_golden_all_ids_start_with_term(golden_bundle):
    bad = [t.internal_id for t in golden_bundle.business_terms if not t.internal_id.startswith("term_")]
    assert not bad, f"Terms with malformed IDs: {bad[:5]}"


def test_golden_all_terms_have_name(golden_bundle):
    nameless = [t for t in golden_bundle.business_terms if not t.name]
    assert not nameless, f"{len(nameless)} terms have empty name"


def test_golden_field_terms_have_data_type_attribute(golden_bundle):
    """Field-level terms (those with a parent) must have a data_type attribute."""
    field_terms = [t for t in golden_bundle.business_terms if t.parent_term_id is not None]
    missing_dtype = [t for t in field_terms if "data_type" not in t.attributes]
    # Allow sub-group terms (object-of-objects midlevel) to lack data_type
    # Check that at least 90% of field terms have it
    pct = (len(field_terms) - len(missing_dtype)) / len(field_terms) if field_terms else 0
    assert pct >= 0.9, (
        f"Only {pct:.0%} of child terms have data_type attribute "
        f"({len(missing_dtype)} missing out of {len(field_terms)})"
    )
