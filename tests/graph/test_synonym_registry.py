"""Tests for SynonymRegistry."""

import pytest
from graph.semantic.ontology_loader import SynonymRegistry


# ------------------------------------------------------------------ #
# allowed_entities()                                                   #
# ------------------------------------------------------------------ #

def test_allowed_entities_contains_all_current():
    """After the April 2026 entity review, line_of_business was removed when
    Signal 3 (tag-dimension entity binding) was dropped — product-line is a
    classification, not a business noun. The current whitelist is 9 entities."""
    entities = SynonymRegistry.allowed_entities()
    expected = {
        "policyholder", "broker", "claim",
        "coverage", "policy", "pricing_component", "profitability_component",
        "exposure", "underwriter",
    }
    assert expected == set(entities), (
        f"Missing: {expected - set(entities)}, extra: {set(entities) - expected}"
    )


def test_allowed_entities_contains_claim():
    assert "claim" in SynonymRegistry.allowed_entities()


def test_line_of_business_no_longer_an_allowed_entity():
    """Regression guard: line_of_business was removed with Signal 3."""
    assert "line_of_business" not in SynonymRegistry.allowed_entities()


def test_allowed_entities_does_not_contain_quote():
    assert "quote" not in SynonymRegistry.allowed_entities()


def test_allowed_entities_does_not_contain_jurisdiction():
    assert "jurisdiction" not in SynonymRegistry.allowed_entities()


# ------------------------------------------------------------------ #
# score_entity_signature — broker columns                             #
# ------------------------------------------------------------------ #

def test_score_entity_signature_broker_columns():
    broker_cols = {"broker_primary", "broker_code", "broker_group", "broker_pseudo_code"}
    scores = SynonymRegistry.score_entity_signature(broker_cols)
    assert "broker" in scores
    assert scores["broker"] >= 0.5, f"broker score={scores['broker']} expected >= 0.5"


def test_score_entity_signature_broker_single_column():
    scores = SynonymRegistry.score_entity_signature({"broker_primary", "policy_id"})
    assert "broker" in scores
    assert scores["broker"] > 0.0


# ------------------------------------------------------------------ #
# score_entity_signature — claim columns                              #
# ------------------------------------------------------------------ #

def test_score_entity_signature_claim_columns():
    claim_cols = {
        "incurred", "paid", "reserved", "claim_count",
        "burn_rate_ulr", "gg_ulr", "gn_ulr", "total_incurred",
        "gglr_incurred", "gnlr_incurred",
    }
    scores = SynonymRegistry.score_entity_signature(claim_cols)
    assert "claim" in scores
    assert scores["claim"] > 0.5, f"claim score={scores['claim']} expected > 0.5"


def test_score_entity_signature_claim_single_column():
    scores = SynonymRegistry.score_entity_signature({"incurred", "some_other_col"})
    assert "claim" in scores
    assert scores["claim"] > 0.0


# ------------------------------------------------------------------ #
# lookup_column_concept                                                #
# ------------------------------------------------------------------ #

def test_lookup_column_concept_broker_primary():
    assert SynonymRegistry.lookup_column_concept("broker_primary") == "broker"


def test_lookup_column_concept_broker_code():
    assert SynonymRegistry.lookup_column_concept("broker_code") == "broker"


def test_lookup_column_concept_incurred():
    assert SynonymRegistry.lookup_column_concept("incurred") == "claim"


def test_lookup_column_concept_total_incurred():
    assert SynonymRegistry.lookup_column_concept("total_incurred") == "claim"


def test_lookup_column_concept_policyholder_name():
    assert SynonymRegistry.lookup_column_concept("policyholder_name") == "policyholder"


def test_lookup_column_concept_entity():
    assert SynonymRegistry.lookup_column_concept("entity") == "policyholder"


def test_lookup_column_concept_unknown_returns_none():
    assert SynonymRegistry.lookup_column_concept("nonexistent_column_xyz") is None


def test_lookup_column_concept_burn_rate_ulr():
    assert SynonymRegistry.lookup_column_concept("burn_rate_ulr") == "claim"
