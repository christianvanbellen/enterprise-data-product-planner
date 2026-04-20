"""ConformedFieldBinder — binds assets to conformed schema entity groups.

Highest-confidence inference path: uses explicit human-designed field
mappings from the conformed schema to find assets that are strong candidates
for a given entity group.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Set

from ingestion.contracts.bundle import CanonicalBundle

# Entity groups from the conformed schema that have semantic counterparts.
# Excludes *_totals groups (structural containers, not leaf entities).
ENTITY_GROUPS = ["coverage", "policy", "policy_totals", "profitability_measures", "rate_monitoring"]

OVERLAP_THRESHOLD = 0.5


@dataclass
class ConformedBindingResult:
    asset_id: str
    entity_group: str
    overlap_score: float        # 0.0–1.0
    matched_fields: List[str]   # fields that matched
    missing_fields: List[str]   # group fields not in asset
    confidence: float           # = overlap_score (direct, no adjustment)


class ConformedFieldBinder:
    """Match assets to conformed entity groups by column-name overlap."""

    def bind(self, bundle: CanonicalBundle) -> Dict[str, List[ConformedBindingResult]]:
        """Return {entity_group_name → [ConformedBindingResult, ...]} for all
        asset-entity pairs where overlap_score >= OVERLAP_THRESHOLD."""

        # Build group_fields: entity_group_name → set of field names
        # (fields are the children of the top-level group BusinessTerm)
        group_term_ids: Dict[str, str] = {}   # term_id → group_name
        for term in bundle.business_terms:
            if term.parent_term_id is None and term.name in ENTITY_GROUPS:
                group_term_ids[term.internal_id] = term.name

        group_fields: Dict[str, Set[str]] = {name: set() for name in ENTITY_GROUPS}
        for term in bundle.business_terms:
            if term.parent_term_id in group_term_ids:
                group_name = group_term_ids[term.parent_term_id]
                group_fields[group_name].add(term.name)

        # Build asset_cols: asset_id → set of column normalized_names
        asset_cols: Dict[str, Set[str]] = {}
        for col in bundle.columns:
            asset_cols.setdefault(col.asset_internal_id, set()).add(col.normalized_name)

        results: Dict[str, List[ConformedBindingResult]] = {name: [] for name in ENTITY_GROUPS}

        for asset in bundle.assets:
            a_cols = asset_cols.get(asset.internal_id, set())

            for group_name in ENTITY_GROUPS:
                g_fields = group_fields.get(group_name, set())
                if not g_fields:
                    continue

                matched = sorted(a_cols & g_fields)
                missing = sorted(g_fields - a_cols)
                overlap_score = len(matched) / len(g_fields)

                if overlap_score >= OVERLAP_THRESHOLD:
                    results[group_name].append(
                        ConformedBindingResult(
                            asset_id=asset.internal_id,
                            entity_group=group_name,
                            overlap_score=overlap_score,
                            matched_fields=matched,
                            missing_fields=missing,
                            confidence=overlap_score,
                        )
                    )

        return results
