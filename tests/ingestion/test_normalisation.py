"""Property and unit tests for ingestion/normalisation/ modules."""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ingestion.normalisation.dtypes import classify_data_type
from ingestion.normalisation.hashing import stable_hash, utc_now_iso
from ingestion.normalisation.names import normalize_name, normalize_tags, normalize_text
from ingestion.normalisation.roles import infer_column_role


# ------------------------------------------------------------------ #
# normalize_name                                                        #
# ------------------------------------------------------------------ #

@given(st.text())
@settings(max_examples=500)
def test_normalize_name_idempotent(s: str) -> None:
    assert normalize_name(normalize_name(s)) == normalize_name(s)


def test_normalize_name_spot_checks() -> None:
    assert normalize_name("Amount Insured") == "amount_insured"
    assert normalize_name("  foo-bar  ") == "foo_bar"
    assert normalize_name("__hello__") == "hello"
    assert normalize_name("CamelCase") == "camelcase"
    assert normalize_name("a  b  c") == "a_b_c"
    assert normalize_name("123_abc") == "123_abc"
    assert normalize_name("") == ""


def test_normalize_text_collapses_whitespace() -> None:
    assert normalize_text("  hello   world  ") == "hello world"
    assert normalize_text(None) is None
    assert normalize_text("   ") is None


def test_normalize_tags_deduplicates_and_orders() -> None:
    result = normalize_tags(["Alpha", "beta", "Alpha", "GAMMA"])
    assert result == ["alpha", "beta", "gamma"]


# ------------------------------------------------------------------ #
# stable_hash                                                           #
# ------------------------------------------------------------------ #

@given(st.text())
@settings(max_examples=500)
def test_stable_hash_deterministic(s: str) -> None:
    assert stable_hash(s) == stable_hash(s)


def test_stable_hash_none_treated_as_empty() -> None:
    assert stable_hash(None) == stable_hash("")


def test_stable_hash_length_parameter() -> None:
    assert len(stable_hash("x", length=8)) == 8
    assert len(stable_hash("x", length=32)) == 32


def test_stable_hash_different_inputs_differ() -> None:
    assert stable_hash("a") != stable_hash("b")


def test_utc_now_iso_is_string() -> None:
    ts = utc_now_iso()
    assert isinstance(ts, str)
    assert "T" in ts  # ISO 8601 format


# ------------------------------------------------------------------ #
# classify_data_type                                                    #
# ------------------------------------------------------------------ #

@given(st.one_of(st.none(), st.text()))
def test_classify_data_type_never_raises(value) -> None:
    result = classify_data_type(value)
    assert result in {"numeric", "boolean", "timestamp", "date", "string", "semi_structured", "unknown"}


def test_classify_data_type_spot_checks() -> None:
    assert classify_data_type("numeric(24,4)") == "numeric"
    assert classify_data_type("INTEGER") == "numeric"
    assert classify_data_type("FLOAT") == "numeric"
    assert classify_data_type("BOOLEAN") == "boolean"
    assert classify_data_type("TIMESTAMP") == "timestamp"
    assert classify_data_type("DATETIME") == "timestamp"
    assert classify_data_type("DATE") == "date"
    assert classify_data_type("VARCHAR(255)") == "string"
    assert classify_data_type("TEXT") == "string"
    assert classify_data_type("JSON") == "semi_structured"
    assert classify_data_type("ARRAY<STRING>") == "semi_structured"
    assert classify_data_type(None) == "unknown"
    assert classify_data_type("") == "unknown"
    assert classify_data_type("EXOTIC_TYPE_XYZ") == "unknown"


# ------------------------------------------------------------------ #
# infer_column_role                                                     #
# ------------------------------------------------------------------ #

@given(
    st.one_of(st.none(), st.text()),
    st.one_of(st.none(), st.text()),
    st.one_of(st.none(), st.text()),
)
def test_infer_column_role_never_raises(name, dtype, desc) -> None:
    if name is None:
        name = ""
    result = infer_column_role(name, dtype, desc)
    valid_roles = {
        "identifier", "measure", "categorical_attribute", "timestamp",
        "boolean_flag", "numeric_attribute", "attribute", "semi_structured", "unknown",
    }
    assert result in valid_roles


def test_infer_column_role_spot_checks() -> None:
    assert infer_column_role("quote_id", "varchar", None) == "identifier"
    assert infer_column_role("id", "integer", None) == "identifier"
    assert infer_column_role("created_at", "timestamp", None) == "timestamp"
    assert infer_column_role("is_active", "boolean", None) == "boolean_flag"
    assert infer_column_role("premium_amount", "numeric", None) == "measure"
    assert infer_column_role("some_number", "numeric", None) == "numeric_attribute"
    assert infer_column_role("status", "varchar", None) == "categorical_attribute"
    assert infer_column_role("notes", "text", None) == "attribute"
    assert infer_column_role("payload", "json", None) == "semi_structured"


# ------------------------------------------------------------------ #
# Fix 2 regression: timestamp boundary-safe detection                  #
# ------------------------------------------------------------------ #

@pytest.mark.parametrize("name,dtype,desc,expected", [
    # Should be timestamp — _date suffix with explicit date dtype
    ("expiry_date",     "date",      None, "timestamp"),
    # Should be timestamp — _date suffix, no dtype (name-based pattern)
    ("inception_date",  None,        None, "timestamp"),
    # Should be timestamp — _at suffix, explicit dtype
    ("created_at",      "timestamp", None, "timestamp"),
    # Should be timestamp — _datetime suffix
    ("closed_datetime", None,        None, "timestamp"),
    # Must NOT be timestamp — "date" is a substring of "updated" but not a boundary match
    ("updated_by",      "varchar",   None, "attribute"),
    # Must NOT be timestamp — "date" inside "candidate" is not a boundary match
    ("candidate",       "varchar",   None, "attribute"),
    # Must NOT be timestamp — generic string column
    ("last_update",     "varchar",   None, "attribute"),
])
def test_timestamp_boundary_detection(name, dtype, desc, expected) -> None:
    result = infer_column_role(name, dtype, desc)
    assert result == expected, (
        f"infer_column_role({name!r}, {dtype!r}, {desc!r}) = {result!r}, expected {expected!r}"
    )
