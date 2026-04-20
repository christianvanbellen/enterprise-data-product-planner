"""InitiativeArchetypeLibrary — loads from ontology/initiative_research.yaml.

Business value and implementation effort scores are editorial constants
kept here. All other fields (feasibility, literature sources, data gaps)
are loaded from the research artifact at ontology/initiative_research.yaml.
"""

from pathlib import Path
from typing import Any, Dict, List

import yaml

from graph.opportunity.primitive_extractor import PRIMITIVE_DEFINITIONS

# ---------------------------------------------------------------------------
# Hardcoded scoring and primitive wiring for each initiative
# ---------------------------------------------------------------------------
# These are editorial constants: business value, effort, primitives, and
# archetype classification. Feasibility (readiness ceiling) comes from YAML.

_INITIATIVE_SCORES: Dict[str, Dict[str, Any]] = {
    "pricing_adequacy_monitoring": {
        "archetype": "monitoring",
        "required_primitives": ["pricing_decomposition", "rate_change_monitoring"],
        "optional_primitives": ["profitability_decomposition"],
        "business_value_score": 0.85,
        "implementation_effort_score": 0.35,
        "target_users": ["pricing_team", "underwriting_leads"],
        "business_objective": "Detect portfolios drifting below technical price before they impact loss ratios",
        "output_type": "monitoring_dashboard",
    },
    "underwriting_decision_support": {
        "archetype": "decision_support",
        "required_primitives": ["quote_lifecycle", "pricing_decomposition", "exposure_structure"],
        "optional_primitives": ["claims_experience", "renewal_tracking"],
        "business_value_score": 0.88,
        "implementation_effort_score": 0.60,
        "target_users": ["underwriters"],
        "business_objective": "Provide data-driven context at point of underwriting decision",
        "output_type": "decision_support",
    },
    "submission_triage": {
        "archetype": "automation",
        "required_primitives": [],
        "optional_primitives": ["quote_lifecycle"],
        "business_value_score": 0.90,
        "implementation_effort_score": 0.80,
        "target_users": ["underwriters", "operations"],
        "business_objective": "Reduce quote turnaround time by automated triage of incoming submissions",
        "output_type": "ai_agent",
    },
    "renewal_prioritisation": {
        "archetype": "prioritization",
        "required_primitives": ["renewal_tracking", "pricing_decomposition"],
        "optional_primitives": ["profitability_decomposition", "claims_experience"],
        "business_value_score": 0.82,
        "implementation_effort_score": 0.40,
        "target_users": ["underwriters", "portfolio_managers"],
        "business_objective": "Focus underwriter time on highest-value renewals",
        "output_type": "decision_support",
    },
    "risk_appetite_monitoring": {
        "archetype": "monitoring",
        "required_primitives": ["exposure_structure", "product_line_segmentation"],
        "optional_primitives": ["quote_lifecycle"],
        "business_value_score": 0.75,
        "implementation_effort_score": 0.35,
        "target_users": ["exposure_management", "portfolio_managers"],
        "business_objective": "Monitor concentration risk across geography, industry, and peril",
        "output_type": "monitoring_dashboard",
    },
    "technical_price_benchmarking": {
        "archetype": "decision_support",
        "required_primitives": ["pricing_decomposition", "rate_change_monitoring"],
        "optional_primitives": ["claims_experience", "profitability_decomposition"],
        "business_value_score": 0.85,
        "implementation_effort_score": 0.50,
        "target_users": ["pricing_team", "actuaries"],
        "business_objective": "Benchmark written rates against technical price across market segments",
        "output_type": "analytics_product",
    },
    "dynamic_pricing_model": {
        "archetype": "automation",
        "required_primitives": [],
        "optional_primitives": [],
        "business_value_score": 0.65,
        "implementation_effort_score": 0.90,
        "target_users": ["pricing_team"],
        "business_objective": "Adjust premiums dynamically based on real-time risk signals",
        "output_type": "ai_agent",
    },
    "claims_experience_analysis": {
        "archetype": "decision_support",
        "required_primitives": ["claims_experience", "pricing_decomposition"],
        "optional_primitives": ["product_line_segmentation", "exposure_structure"],
        "business_value_score": 0.83,
        "implementation_effort_score": 0.35,
        "target_users": ["actuaries", "underwriters", "claims_team"],
        "business_objective": "Understand loss development patterns to improve pricing and reserving",
        "output_type": "analytics_product",
    },
    "claims_severity_prediction": {
        "archetype": "prediction",
        "required_primitives": ["claims_experience"],
        "optional_primitives": ["exposure_structure"],
        "business_value_score": 0.80,
        "implementation_effort_score": 0.65,
        "target_users": ["actuaries", "claims_team"],
        "business_objective": "Predict ultimate claims severity to improve reserving adequacy",
        "output_type": "analytics_product",
    },
    "claims_fraud_detection": {
        "archetype": "anomaly_detection",
        "required_primitives": [],
        "optional_primitives": ["claims_experience"],
        "business_value_score": 0.75,
        "implementation_effort_score": 0.70,
        "target_users": ["claims_team", "finance"],
        "business_objective": "Flag anomalous claims patterns for investigation",
        "output_type": "monitoring_dashboard",
    },
    "claims_automation": {
        "archetype": "automation",
        "required_primitives": [],
        "optional_primitives": ["claims_experience"],
        "business_value_score": 0.85,
        "implementation_effort_score": 0.85,
        "target_users": ["claims_team", "operations"],
        "business_objective": "Automate routine claims processing via end-to-end agentic AI",
        "output_type": "ai_agent",
    },
    "portfolio_drift_monitoring": {
        "archetype": "anomaly_detection",
        "required_primitives": ["rate_change_monitoring", "exposure_structure"],
        "optional_primitives": ["product_line_segmentation", "claims_experience"],
        "business_value_score": 0.80,
        "implementation_effort_score": 0.30,
        "target_users": ["portfolio_managers", "actuaries"],
        "business_objective": "Early warning of portfolio drift before it becomes a loss event",
        "output_type": "monitoring_dashboard",
    },
    "cat_exposure_monitoring": {
        "archetype": "monitoring",
        "required_primitives": [],
        "optional_primitives": ["exposure_structure"],
        "business_value_score": 0.90,
        "implementation_effort_score": 0.80,
        "target_users": ["exposure_management", "portfolio_managers"],
        "business_objective": "Stress-test portfolio performance across catastrophe scenarios",
        "output_type": "monitoring_dashboard",
    },
    "broker_performance_intelligence": {
        "archetype": "recommendation",
        "required_primitives": ["broker_attribution", "pricing_decomposition"],
        "optional_primitives": ["claims_experience", "profitability_decomposition"],
        "business_value_score": 0.78,
        "implementation_effort_score": 0.45,
        "target_users": ["distribution_team", "underwriting_leads"],
        "business_objective": "Identify highest-quality broker relationships and optimise channel mix",
        "output_type": "analytics_product",
    },
    "delegated_authority_monitoring": {
        "archetype": "monitoring",
        "required_primitives": ["broker_attribution"],
        "optional_primitives": ["claims_experience"],
        "business_value_score": 0.70,
        "implementation_effort_score": 0.60,
        "target_users": ["distribution_team", "portfolio_managers"],
        "business_objective": "Monitor coverholder and delegated authority portfolio performance",
        "output_type": "analytics_product",
    },
    "profitability_decomposition_assistant": {
        "archetype": "decision_support",
        "required_primitives": ["profitability_decomposition", "pricing_decomposition"],
        "optional_primitives": ["broker_attribution"],
        "business_value_score": 0.75,
        "implementation_effort_score": 0.25,
        "target_users": ["actuaries", "finance", "underwriting_leads"],
        "business_objective": "Understand drivers of profitability gap vs plan",
        "output_type": "analytics_product",
    },
    "loss_ratio_forecasting": {
        "archetype": "prediction",
        "required_primitives": ["claims_experience", "profitability_decomposition"],
        "optional_primitives": ["pricing_decomposition"],
        "business_value_score": 0.80,
        "implementation_effort_score": 0.55,
        "target_users": ["actuaries", "finance"],
        "business_objective": "Project loss ratios and actuarial reserving requirements",
        "output_type": "analytics_product",
    },
    "renewal_pricing_copilot": {
        "archetype": "copilot",
        "required_primitives": ["quote_lifecycle", "pricing_decomposition", "renewal_tracking"],
        "optional_primitives": ["claims_experience", "broker_attribution"],
        "business_value_score": 0.80,
        "implementation_effort_score": 0.70,
        "target_users": ["underwriters"],
        "business_objective": "Reduce time to gather context at renewal from hours to seconds",
        "output_type": "ai_agent",
    },
    "product_line_performance_dashboard": {
        "archetype": "monitoring",
        "required_primitives": ["product_line_segmentation", "pricing_decomposition"],
        "optional_primitives": ["claims_experience", "profitability_decomposition"],
        "business_value_score": 0.72,
        "implementation_effort_score": 0.30,
        "target_users": ["product_leads", "underwriting_management"],
        "business_objective": "Single view of portfolio health by product line",
        "output_type": "monitoring_dashboard",
    },
}

_RESEARCH_YAML_PATH = Path(__file__).parents[2] / "ontology" / "initiative_research.yaml"


def _load_research_yaml() -> Dict[str, Dict[str, Any]]:
    """Load initiative_research.yaml and return a dict keyed by initiative id."""
    raw = yaml.safe_load(_RESEARCH_YAML_PATH.read_text(encoding="utf-8"))
    by_id: Dict[str, Dict[str, Any]] = {}
    for entry in raw.get("initiative_taxonomy", []):
        by_id[entry["id"]] = entry
    return by_id


def _build_archetypes() -> Dict[str, Dict[str, Any]]:
    """Merge hardcoded scores with YAML research data at module load time."""
    yaml_data = _load_research_yaml()
    archetypes: Dict[str, Dict[str, Any]] = {}
    for iid, scores in _INITIATIVE_SCORES.items():
        entry = yaml_data.get(iid, {})
        archetype = dict(scores)
        archetype["literature_sources"] = entry.get("sources", [])
        archetype["feasibility_against_warehouse"] = entry.get(
            "feasibility_against_warehouse", "ready_now"
        )
        archetype["data_gaps"] = entry.get("data_gaps", [])
        archetype["literature_name"] = entry.get("literature_name", iid.replace("_", " ").title())
        archetype["literature_quote"] = entry.get("literature_quote", "")
        archetype["category"] = entry.get("category", "")
        archetypes[iid] = archetype
    return archetypes


# Module-level dict — built once at import, same structure callers expect.
INITIATIVE_ARCHETYPES: Dict[str, Dict[str, Any]] = _build_archetypes()


class InitiativeArchetypeLibrary:
    """Curated library of insurance analytics initiative archetypes.

    Loaded from ontology/initiative_research.yaml at init; scoring constants
    are hardcoded in _INITIATIVE_SCORES above.
    """

    def get_archetype(self, initiative_id: str) -> Dict[str, Any]:
        return INITIATIVE_ARCHETYPES[initiative_id]

    def all_initiatives(self) -> List[str]:
        return list(INITIATIVE_ARCHETYPES.keys())

    def initiatives_by_archetype(self, archetype: str) -> List[str]:
        return [k for k, v in INITIATIVE_ARCHETYPES.items() if v["archetype"] == archetype]

    def initiatives_by_feasibility(self, feasibility: str) -> List[str]:
        """Return initiative IDs where feasibility_against_warehouse matches."""
        return [
            k for k, v in INITIATIVE_ARCHETYPES.items()
            if v.get("feasibility_against_warehouse") == feasibility
        ]

    def required_primitives(self, initiative_id: str) -> List[str]:
        return INITIATIVE_ARCHETYPES[initiative_id]["required_primitives"]


def validate_archetype_library() -> List[str]:
    """Return list of validation errors — empty means the library is consistent."""
    valid_primitive_ids = set(PRIMITIVE_DEFINITIONS.keys())
    errors = []
    for init_id, defn in INITIATIVE_ARCHETYPES.items():
        for pid in defn.get("required_primitives", []):
            if pid not in valid_primitive_ids:
                errors.append(f"{init_id}: required_primitive '{pid}' not in PRIMITIVE_DEFINITIONS")
        for pid in defn.get("optional_primitives", []):
            if pid not in valid_primitive_ids:
                errors.append(f"{init_id}: optional_primitive '{pid}' not in PRIMITIVE_DEFINITIONS")
        if not defn.get("literature_sources"):
            errors.append(f"{init_id}: missing literature_sources (check initiative_research.yaml)")
    return errors
