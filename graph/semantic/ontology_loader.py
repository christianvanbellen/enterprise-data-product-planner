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
    name overlap with its signature set. Also used by SemanticGraphCompiler
    to decide which columns get IDENTIFIES edges to each entity.

    COLUMN_SYNONYM_MAP provides a direct concept lookup for individual
    column names. Currently referenced only by tests — retained for
    reference until the entity-model research pass decides whether to wire
    it into IDENTIFIES-edge creation or drop it.

    Both are loaded from ontology/entity_bindings.yaml at module import.
    """

    _BINDINGS: Dict[str, Any] = _load_yaml("entity_bindings.yaml") or {}

    ENTITY_SIGNATURE_COLUMNS: Dict[str, Set[str]] = {
        entity: set(cols or [])
        for entity, cols in (_BINDINGS.get("entity_signatures") or {}).items()
    }

    COLUMN_SYNONYM_MAP: Dict[str, str] = dict(
        _BINDINGS.get("column_synonyms") or {}
    )

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
        """Entity whitelist loaded from ontology/entity_bindings.yaml."""
        return list(cls._BINDINGS.get("entities") or [])
