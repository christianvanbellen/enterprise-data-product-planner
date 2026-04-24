"""InitiativeArchetypeLibrary — loads all initiative definitions from ontology/initiative_research.yaml.

All scoring constants (business_value_score, implementation_effort_score, required_primitives,
optional_primitives, archetype, target_users, business_objective, output_type) and research
fields (feasibility, literature, data_gaps) are now colocated in initiative_research.yaml.
"""

from pathlib import Path
from typing import Any, Dict, List

import yaml

from graph.opportunity.primitive_extractor import PRIMITIVE_DEFINITIONS

_RESEARCH_YAML_PATH = Path(__file__).parents[2] / "ontology" / "initiative_research.yaml"


def _load_research_yaml() -> Dict[str, Any]:
    """Load initiative_research.yaml and return the full parsed structure."""
    return yaml.safe_load(_RESEARCH_YAML_PATH.read_text(encoding="utf-8"))


def _build_archetypes() -> Dict[str, Dict[str, Any]]:
    """Build the INITIATIVE_ARCHETYPES dict from initiative_research.yaml."""
    raw = _load_research_yaml()
    archetypes: Dict[str, Dict[str, Any]] = {}
    for entry in raw.get("initiative_taxonomy", []):
        iid = entry["id"]
        archetypes[iid] = {
            # Scoring fields
            "archetype":                   entry.get("archetype", "monitoring"),
            "required_primitives":         entry.get("required_primitives", []),
            "optional_primitives":         entry.get("optional_primitives", []),
            "business_value_score":        float(entry.get("business_value_score", 0.5)),
            "implementation_effort_score": float(entry.get("implementation_effort_score", 0.5)),
            "target_users":                entry.get("target_users", []),
            "business_objective":          entry.get("business_objective", ""),
            "output_type":                 entry.get("output_type", "analytics_product"),
            # Research fields
            "category":                    entry.get("category", ""),
            "literature_name":             entry.get("literature_name", iid.replace("_", " ").title()),
            "literature_sources":          entry.get("sources", []),
            "literature_quote":            entry.get("literature_quote", ""),
            "feasibility_against_warehouse": entry.get("feasibility_against_warehouse", "ready_now"),
            "feasibility_rationale":       entry.get("feasibility_rationale", ""),
            "data_gaps":                   entry.get("data_gaps", []),
            # Tri-state intent from gap-aware curation (v3 semantic_model research).
            # Defaults to `grounded` for back-compat with pre-gap-aware entries.
            "status":                      entry.get("status", "grounded"),
        }
    return archetypes


# Module-level dict — built once at import.
INITIATIVE_ARCHETYPES: Dict[str, Dict[str, Any]] = _build_archetypes()


class InitiativeArchetypeLibrary:
    """Curated library of insurance analytics initiative archetypes.

    All definitions loaded from ontology/initiative_research.yaml at import time.
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
