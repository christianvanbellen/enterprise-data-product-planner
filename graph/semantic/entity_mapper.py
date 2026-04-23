"""EntityMapper — combines three signal sources to assign BusinessEntity candidates to assets.

Signals today:
    1. Conformed-schema binding (ConformedFieldBinder output).
    2. Column-signature scoring against SynonymRegistry.ENTITY_SIGNATURE_COLUMNS.
    4. Asset-name substring patterns.

Signal 3 (tag-dimension entity binding) was removed in April 2026 after the
entity-audit review: classifications like `product_line` are orthogonal to
business entities (line_of_business is a slice, not a noun), and treating
them as entity signals distorted the primary-count distribution. Tag data
remains on each asset as asset.tag_dimensions for filtering and display,
and tags continue to contribute to domain inference in Phase 1.
"""

import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Set

import yaml

from ingestion.contracts.bundle import CanonicalBundle
from graph.semantic.conformed_binder import ConformedBindingResult
from graph.semantic.ontology_loader import SynonymRegistry

_ONTOLOGY_DIR = Path(__file__).parent.parent.parent / "ontology"
_ENTITY_BINDINGS_PATH = _ONTOLOGY_DIR / "entity_bindings.yaml"


def _load_entity_bindings_config() -> Dict[str, Any]:
    """Load Phase 3 entity-mapping config from ontology/entity_bindings.yaml."""
    return yaml.safe_load(_ENTITY_BINDINGS_PATH.read_text(encoding="utf-8")) or {}


_ENTITY_BINDINGS_CONFIG = _load_entity_bindings_config()

# Loaded from ontology/entity_bindings.yaml — edit that file to change binding behaviour.
CONFORMED_GROUP_TO_ENTITY: Dict[str, str] = dict(
    _ENTITY_BINDINGS_CONFIG.get("conformed_group_to_entity") or {}
)

# Loaded from ontology/entity_bindings.yaml.
ASSET_NAME_PATTERNS: Dict[str, str] = dict(
    _ENTITY_BINDINGS_CONFIG.get("asset_name_patterns") or {}
)

_CONFIDENCE = _ENTITY_BINDINGS_CONFIG.get("confidence") or {}
MIN_CONFIDENCE: float = float(_CONFIDENCE.get("min_threshold", 0.4))
CONFLICT_THRESHOLD: float = float(_CONFIDENCE.get("conflict_threshold", 0.5))
_SIGNAL_2_SCALE: float = float(_CONFIDENCE.get("signal_2_scale", 0.8))
_SIGNAL_4_FLAT: float = float(_CONFIDENCE.get("signal_4_flat", 0.6))


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
                _add(asset.internal_id, entity_label, raw_score * _SIGNAL_2_SCALE,
                     "signature_score",
                     {"signature_score": raw_score})

        # ---- Signal 3: Tag-dimension entity bindings — removed April 2026 ----
        # Product-line / class-of-business classifications are orthogonal to
        # business entities. Tags remain available on each asset via
        # asset.tag_dimensions and drive domain inference in Phase 1.
        # See docs/phase3_design_brief.md and the entity-audit review.

        # ---- Signal 4: Asset name pattern (flat confidence) ----
        for asset in bundle.assets:
            for pattern, entity_label in ASSET_NAME_PATTERNS.items():
                if pattern in asset.normalized_name:
                    _add(asset.internal_id, entity_label, _SIGNAL_4_FLAT,
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
