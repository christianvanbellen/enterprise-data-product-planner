"""OntologyLoader — loads ontology YAML files and provides registry lookups for Phase 3."""

from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import yaml  # type: ignore

ONTOLOGY_DIR = Path(__file__).parent.parent.parent / "ontology"


def _load_yaml(filename: str) -> dict:
    path = ONTOLOGY_DIR / filename
    return yaml.safe_load(path.read_text(encoding="utf-8"))


class SynonymRegistry:
    """Maps column names and asset properties to semantic entity labels.

    ENTITY_SIGNATURE_COLUMNS maps each entity label to the set of column
    normalized_names that are strong identifiers for that entity.
    score_entity_signature() returns a 0–1 score per entity based on column
    name overlap with its signature set.

    COLUMN_SYNONYM_MAP provides a direct concept lookup for individual
    column names that are unambiguous signals for a specific entity.
    """

    ENTITY_SIGNATURE_COLUMNS: Dict[str, Set[str]] = {
        "policy": {
            "policy_id", "policy_number", "policy_currency", "policy_term",
            "policy_reference",
        },
        "coverage": {
            "coverage_id", "primary_coverage", "coverage_type",
            "limit", "excess", "deductible_value", "section",
        },
        "policyholder": {
            "policyholder_name", "policyholder_id", "insured_name",
            "entity", "branch", "new_renewal", "inception_date", "expiry_date",
        },
        "broker": {
            "broker_id", "broker_name", "broker_primary", "brokerage_pct",
            "broker_code", "broker_group", "broker_pseudo_code", "coverholder",
        },
        "line_of_business": {
            "eupi", "d_o", "general_aviation", "contingency",
            "london_cash_in_transit_and_general_specie",
        },
        "claim": {
            "incurred", "paid", "reserved", "claims_as_at",
            "burn_rate_ulr", "gg_ulr", "gn_ulr",
            "gglr_incurred", "gnlr_incurred", "loss_cost_inflation",
            "claim_count", "fgu_incurred_loss", "total_incurred",
            "incurred_excl_specific", "paid_claims", "number_claims",
        },
        "underwriter": {
            "underwriter", "underwriter_id",
        },
        "pricing_component": {
            "premium", "rate", "elr", "tech_gnwp", "modtech_gnwp",
            "ggwp", "gnwp", "sold_ggwp", "sold_gnwp",
        },
        "profitability_component": {
            "commission", "profitability", "sold_to_tech", "sold_to_modtech",
            "change_in_sold_to_tech", "change_in_sold_to_modtech",
        },
        "exposure": {
            "exposure", "exposure_type", "expiring_exposure",
        },
    }

    COLUMN_SYNONYM_MAP: Dict[str, str] = {
        # Broker identifiers
        "broker_primary":     "broker",
        "broker_code":        "broker",
        "broker_group":       "broker",
        "broker_pseudo_code": "broker",
        "coverholder":        "broker",
        # Policyholder identifiers
        "policyholder_name":  "policyholder",
        "entity":             "policyholder",
        # Claim signals
        "incurred":           "claim",
        "paid":               "claim",
        "reserved":           "claim",
        "burn_rate_ulr":      "claim",
        "gg_ulr":             "claim",
        "gn_ulr":             "claim",
        "claim_count":        "claim",
        "total_incurred":     "claim",
    }

    @classmethod
    def score_entity_signature(cls, asset_col_names: Set[str]) -> Dict[str, float]:
        """Return entity_label → score (0.0–1.0) based on column name overlap."""
        scores: Dict[str, float] = {}
        for entity, sig_cols in cls.ENTITY_SIGNATURE_COLUMNS.items():
            matched = asset_col_names & sig_cols
            if matched:
                scores[entity] = len(matched) / len(sig_cols)
        return scores

    @classmethod
    def lookup_column_concept(cls, col_name: str) -> Optional[str]:
        """Return the entity concept for a column name, or None if not recognised."""
        return cls.COLUMN_SYNONYM_MAP.get(col_name)

    @classmethod
    def allowed_entities(cls) -> List[str]:
        return _load_yaml("insurance_entities.yaml").get("entities", [])

    @classmethod
    def allowed_domains(cls) -> List[str]:
        return _load_yaml("insurance_domains.yaml").get("domains", [])
