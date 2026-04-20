"""ConformedSchemaAdapter — parses JSON Schema draft-04 conformed schema into CanonicalBusinessTerms.

Expected input structure:
    {
      "$schema": "http://json-schema.org/draft-04/schema#",
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "conformed_data": {
            "type": "object",
            "properties": {
              "<entity_group>": {
                "type": "array",            # array entity: fields in items.properties
                "items": {"properties": {...}}
              },
              "<entity_group_totals>": {
                "type": "object",           # object-of-objects: sub-groups in properties
                "properties": {
                  "<sub_group>": {
                    "type": "object",
                    "properties": {...}     # leaf fields here
                  }
                }
              }
            }
          }
        }
      }
    }

Each entity group → one group-level CanonicalBusinessTerm.
Each field within a group → one field-level CanonicalBusinessTerm (child of the group).
Object-of-objects (e.g. policy_totals): each sub-group → one sub-group term; each sub-field → child.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from ingestion.adapters.base import BaseAdapter
from ingestion.contracts.bundle import CanonicalBundle
from ingestion.contracts.business import CanonicalBusinessTerm
from ingestion.normalisation.hashing import stable_hash
from ingestion.normalisation.names import normalize_name

logger = logging.getLogger(__name__)

SOURCE_SYSTEM = "conformed_schema"
SOURCE_TYPE = "ConformedSchemaAdapter"


def _term_id(path_tokens: List[str]) -> str:
    return f"term_{stable_hash(SOURCE_SYSTEM, '::'.join(path_tokens))}"


def _version_hash(raw: Any) -> str:
    return stable_hash(json.dumps(raw, sort_keys=True, default=str))


class ConformedSchemaAdapter(BaseAdapter):
    """Parse a JSON Schema draft-04 conformed schema file into CanonicalBusinessTerms."""

    def detect(self, path: Path) -> Dict[str, Any]:
        """Return compatible=True if file has $schema key and properties somewhere."""
        try:
            raw = json.loads(Path(path).read_text(encoding="utf-8"))
        except Exception:
            return {"compatible": False, "variant": None, "warnings": ["Could not parse file as JSON"]}

        if not isinstance(raw, dict):
            return {"compatible": False, "variant": None, "warnings": ["Root is not an object"]}

        if "$schema" not in raw:
            return {"compatible": False, "variant": None, "warnings": ["No $schema key"]}

        raw_str = json.dumps(raw)
        if '"properties"' not in raw_str:
            return {"compatible": False, "variant": None, "warnings": ["No properties found"]}

        return {
            "compatible": True,
            "variant": "json_schema_draft04",
            "missing_fields": [],
            "warnings": [],
            "entity_count": len(self._extract_entity_groups(raw)),
        }

    def parse_file(self, path: Path) -> CanonicalBundle:
        path = Path(path)
        raw = json.loads(path.read_text(encoding="utf-8"))

        terms: List[CanonicalBusinessTerm] = []
        entity_groups = self._extract_entity_groups(raw)

        for group_name, group_schema in entity_groups.items():
            group_type = group_schema.get("type")

            if group_type == "array":
                # Array entity: fields live in items.properties
                field_props = group_schema.get("items", {}).get("properties", {})
                self._emit_group_and_fields(
                    group_name=group_name,
                    field_props=field_props,
                    group_schema=group_schema,
                    parent_term_id=None,
                    terms=terms,
                )

            elif group_type == "object":
                sub_props = group_schema.get("properties", {})
                # Check if this is object-of-objects (each value is itself an object with properties)
                # e.g. policy_totals: { "100_percent_original_ccy": { "type": "object", "properties": {...} } }
                if sub_props and all(
                    isinstance(v, dict) and v.get("type") == "object" and "properties" in v
                    for v in sub_props.values()
                ):
                    # Emit the top-level group term
                    group_tokens = [group_name]
                    group_term_id = _term_id(group_tokens)
                    terms.append(CanonicalBusinessTerm(
                        internal_id=group_term_id,
                        term_type="conformed_concept",
                        name=group_name,
                        normalized_name=normalize_name(group_name),
                        parent_term_id=None,
                        attributes={"schema_type": "object_of_objects"},
                        version_hash=_version_hash(group_schema),
                        provenance=self._provenance(SOURCE_SYSTEM, SOURCE_TYPE, group_name, group_schema),
                    ))
                    # Emit each sub-group and its fields
                    for sub_name, sub_schema in sub_props.items():
                        self._emit_group_and_fields(
                            group_name=sub_name,
                            field_props=sub_schema.get("properties", {}),
                            group_schema=sub_schema,
                            parent_term_id=group_term_id,
                            terms=terms,
                            path_prefix=[group_name],
                        )
                else:
                    # Plain object with fields directly in properties
                    self._emit_group_and_fields(
                        group_name=group_name,
                        field_props=sub_props,
                        group_schema=group_schema,
                        parent_term_id=None,
                        terms=terms,
                    )

        logger.info(
            "ConformedSchemaAdapter: %d business terms from %s",
            len(terms), path,
        )

        return CanonicalBundle(
            assets=[],
            columns=[],
            lineage_edges=[],
            business_terms=terms,
            metadata={"source_file": str(path), "adapter": SOURCE_TYPE},
        )

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    def _extract_entity_groups(self, raw: dict) -> Dict[str, Any]:
        """Navigate to conformed_data.properties and return the entity group map."""
        # Path: root.items.properties.conformed_data.properties
        try:
            return (
                raw
                .get("items", {})
                .get("properties", {})
                .get("conformed_data", {})
                .get("properties", {})
            )
        except AttributeError:
            return {}

    def _emit_group_and_fields(
        self,
        group_name: str,
        field_props: Dict[str, Any],
        group_schema: Any,
        parent_term_id: Optional[str],
        terms: List[CanonicalBusinessTerm],
        path_prefix: Optional[List[str]] = None,
    ) -> None:
        """Emit one group-level term and one child term per field."""
        path_tokens = (path_prefix or []) + [group_name]
        group_term_id = _term_id(path_tokens)

        terms.append(CanonicalBusinessTerm(
            internal_id=group_term_id,
            term_type="conformed_concept",
            name=group_name,
            normalized_name=normalize_name(group_name),
            parent_term_id=parent_term_id,
            attributes={"schema_type": group_schema.get("type", "unknown"), "field_count": len(field_props)},
            version_hash=_version_hash(group_schema),
            provenance=self._provenance(SOURCE_SYSTEM, SOURCE_TYPE, "::".join(path_tokens), group_schema),
        ))

        for field_name, field_schema in field_props.items():
            field_tokens = path_tokens + [field_name]
            field_term_id = _term_id(field_tokens)
            terms.append(CanonicalBusinessTerm(
                internal_id=field_term_id,
                term_type="conformed_concept",
                name=field_name,
                normalized_name=normalize_name(field_name),
                parent_term_id=group_term_id,
                attributes={
                    "data_type": field_schema.get("type", "unknown"),
                    **{k: v for k, v in field_schema.items() if k != "type" and not isinstance(v, dict)},
                },
                version_hash=_version_hash(field_schema),
                provenance=self._provenance(SOURCE_SYSTEM, SOURCE_TYPE, "::".join(field_tokens), field_schema),
            ))
