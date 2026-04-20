"""EntityMapper — combines three signal sources to assign BusinessEntity candidates to assets."""

import warnings
from dataclasses import dataclass, field
from typing import Any, Dict, List, Set

from ingestion.contracts.bundle import CanonicalBundle
from graph.semantic.conformed_binder import ConformedBindingResult
from graph.semantic.ontology_loader import SynonymRegistry

# Maps conformed schema entity groups → ontology entity labels
CONFORMED_GROUP_TO_ENTITY: Dict[str, str] = {
    "coverage":               "coverage",
    "policy":                 "policy",
    "profitability_measures": "profitability_component",
    "rate_monitoring":        "pricing_component",
    "policy_totals":          "policy",
}

# Maps product_line values → ontology entity labels (signal 3)
# Kept for backward compatibility; TAG_TO_ENTITY is the canonical name.
TAG_TO_ENTITY: Dict[str, str] = {
    "european_professional_indemnity": "line_of_business",
    "directors_and_officers":          "line_of_business",
    "general_aviation":                "line_of_business",
    "cash_in_transit_and_specie":      "line_of_business",
    "contingency":                     "line_of_business",
    "digital_platform":                "line_of_business",
}

# Alias for backward compatibility with existing tests/code
PRODUCT_LINE_TO_ENTITY = TAG_TO_ENTITY

# Maps substrings in asset normalized_name → entity label (signal 4)
# Used when signature scoring can't reach threshold due to sparse column coverage.
ASSET_NAME_PATTERNS: Dict[str, str] = {
    "claim":     "claim",
    "brokerage": "broker",
}

MIN_CONFIDENCE = 0.4
CONFLICT_THRESHOLD = 0.5   # second entity must reach this to coexist


@dataclass
class EntityCandidate:
    asset_id: str
    entity_label: str
    confidence: float
    signal_sources: List[str]
    evidence: Dict[str, Any]


class EntityMapper:
    """Merge conformed binding, signature scoring, and tag signals into EntityCandidates."""

    def __init__(self) -> None:
        self._allowed: Set[str] = set(SynonymRegistry.allowed_entities())

    def map(
        self,
        bundle: CanonicalBundle,
        binder_results: Dict[str, List[ConformedBindingResult]],
    ) -> List[EntityCandidate]:
        """Return one EntityCandidate per (asset_id, entity_label) pair that survives
        the confidence threshold and conflict-resolution rules."""

        # Accumulator: asset_id → entity_label → {confidence, signal_sources, evidence}
        acc: Dict[str, Dict[str, Dict[str, Any]]] = {}

        def _add(asset_id: str, entity_label: str, confidence: float,
                 source: str, extra_evidence: Dict[str, Any]) -> None:
            if entity_label not in self._allowed:
                warnings.warn(
                    f"Entity label '{entity_label}' not in allowed entities — skipping.",
                    stacklevel=3,
                )
                return
            slot = acc.setdefault(asset_id, {}).setdefault(entity_label, {
                "confidence": 0.0,
                "signal_sources": [],
                "evidence": {},
            })
            if confidence > slot["confidence"]:
                slot["confidence"] = confidence
            if source not in slot["signal_sources"]:
                slot["signal_sources"].append(source)
            slot["evidence"].update(extra_evidence)

        # ---- Signal 1: Conformed field binding (highest trust) ----
        for group_name, bindings in binder_results.items():
            entity_label = CONFORMED_GROUP_TO_ENTITY.get(group_name)
            if not entity_label:
                continue
            for b in bindings:
                _add(b.asset_id, entity_label, b.confidence,
                     "conformed_binding",
                     {"matched_fields": b.matched_fields,
                      "overlap_score": b.overlap_score,
                      "entity_group": group_name})

        # ---- Signal 2: Entity signature scoring (discounted 0.8×) ----
        asset_cols: Dict[str, Set[str]] = {}
        for col in bundle.columns:
            asset_cols.setdefault(col.asset_internal_id, set()).add(col.normalized_name)

        for asset in bundle.assets:
            cols = asset_cols.get(asset.internal_id, set())
            scores = SynonymRegistry.score_entity_signature(cols)
            for entity_label, raw_score in scores.items():
                _add(asset.internal_id, entity_label, raw_score * 0.8,
                     "signature_score",
                     {"signature_score": raw_score})

        # ---- Signal 3: Tag-based product line (confidence 0.6) ----
        for asset in bundle.assets:
            for pl in asset.product_lines:
                entity_label = TAG_TO_ENTITY.get(pl)
                if entity_label:
                    _add(asset.internal_id, entity_label, 0.6,
                         "tag_product_line",
                         {"product_line": pl})

        # ---- Signal 4: Asset name pattern (confidence 0.6) ----
        for asset in bundle.assets:
            for pattern, entity_label in ASSET_NAME_PATTERNS.items():
                if pattern in asset.normalized_name:
                    _add(asset.internal_id, entity_label, 0.6,
                         "asset_name_pattern",
                         {"name_pattern": pattern})

        # ---- Merge and apply conflict resolution ----
        candidates: List[EntityCandidate] = []

        for asset_id, entity_map in acc.items():
            # Sort descending by confidence
            ranked = sorted(entity_map.items(), key=lambda x: -x[1]["confidence"])

            emitted: List[str] = []
            for entity_label, sig in ranked:
                conf = sig["confidence"]
                if conf < MIN_CONFIDENCE:
                    break  # rest are even lower; sorted so we can stop
                # If we already emitted at least one, only keep this if >= threshold
                if emitted and conf < CONFLICT_THRESHOLD:
                    continue
                candidates.append(EntityCandidate(
                    asset_id=asset_id,
                    entity_label=entity_label,
                    confidence=conf,
                    signal_sources=list(sig["signal_sources"]),
                    evidence=dict(sig["evidence"]),
                ))
                emitted.append(entity_label)

        return candidates
