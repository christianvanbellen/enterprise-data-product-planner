"""CapabilityPrimitiveExtractor — identifies analytical building blocks from the semantic graph."""

from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Set

import yaml

from ingestion.contracts.bundle import CanonicalBundle

# ---------------------------------------------------------------------------
# Primitive definitions registry — loaded from ontology/primitives.yaml
# ---------------------------------------------------------------------------

_PRIMITIVES_YAML_PATH = Path(__file__).parents[2] / "ontology" / "primitives.yaml"


def _load_primitive_definitions() -> Dict[str, Dict[str, Any]]:
    """Load PRIMITIVE_DEFINITIONS from ontology/primitives.yaml.

    YAML stores required_columns and required_tags as lists; this function converts
    them to sets for fast intersection in CapabilityPrimitiveExtractor.extract().
    """
    raw = yaml.safe_load(_PRIMITIVES_YAML_PATH.read_text(encoding="utf-8"))
    result: Dict[str, Dict[str, Any]] = {}
    for entry in raw.get("primitives", []):
        pid = entry["id"]
        defn: Dict[str, Any] = {
            "required_entities":  list(entry.get("required_entities", [])),
            "required_columns":   set(entry.get("required_columns", [])),
            "supporting_domains": list(entry.get("supporting_domains", [])),
            "description":        entry.get("description", ""),
        }
        if "required_tags" in entry:
            defn["required_tags"] = set(entry["required_tags"])
            # required_tag_dimension names which asset.tag_dimensions key to intersect
            # against. Required whenever required_tags is present.
            defn["required_tag_dimension"] = entry.get("required_tag_dimension", "")
        result[pid] = defn
    return result


# Loaded from ontology/primitives.yaml — edit that file to change primitive definitions.
PRIMITIVE_DEFINITIONS: Dict[str, Dict[str, Any]] = _load_primitive_definitions()


# ---------------------------------------------------------------------------
# Data contract
# ---------------------------------------------------------------------------

@dataclass
class CapabilityPrimitive:
    primitive_id: str
    primitive_name: str          # human-readable title-cased name
    description: str
    entity_score: float
    column_score: float
    maturity_score: float
    matched_entities: List[str]
    missing_entities: List[str]
    matched_columns: List[str]
    missing_columns: List[str]
    supporting_asset_ids: List[str]
    graph_layer: str = "opportunity"


# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------

class CapabilityPrimitiveExtractor:
    """Extract CapabilityPrimitive records from the bundle + semantic graph state."""

    def extract(
        self,
        bundle: CanonicalBundle,
        graph_store: Any,           # JsonGraphStore (or compatible)
    ) -> List[CapabilityPrimitive]:

        # ── Build entity → asset_ids from REPRESENTS edges ────────────────
        entity_to_assets: Dict[str, Set[str]] = defaultdict(set)
        node_label_map: Dict[str, str] = {}   # node_id → label
        node_entity_label: Dict[str, str] = {}  # entity node_id → entity_label

        for node in graph_store._nodes.values():
            lbl = node.get("label", "")
            node_label_map[node["node_id"]] = lbl
            if lbl == "BusinessEntityNode":
                node_entity_label[node["node_id"]] = node["properties"]["entity_label"]

        for edge in graph_store._edges.values():
            if edge.get("edge_type") == "REPRESENTS":
                tgt = edge["target_node_id"]
                entity_label = node_entity_label.get(tgt)
                if entity_label:
                    entity_to_assets[entity_label].add(edge["source_node_id"])

        # ── Build domain → asset_ids from BELONGS_TO_DOMAIN edges ─────────
        domain_assets: Dict[str, Set[str]] = defaultdict(set)
        node_domain_label: Dict[str, str] = {}
        for node in graph_store._nodes.values():
            if node.get("label") == "DomainNode":
                node_domain_label[node["node_id"]] = node["properties"]["domain_name"]

        for edge in graph_store._edges.values():
            if edge.get("edge_type") == "BELONGS_TO_DOMAIN":
                tgt = edge["target_node_id"]
                domain_label = node_domain_label.get(tgt)
                if domain_label:
                    domain_assets[domain_label].add(edge["source_node_id"])

        # ── Global column name set and per-asset column map ───────────────
        all_col_names: Set[str] = set()
        asset_cols: Dict[str, Set[str]] = defaultdict(set)
        for col in bundle.columns:
            all_col_names.add(col.normalized_name)
            asset_cols[col.asset_internal_id].add(col.normalized_name)

        # ── Asset id → tag_dimensions for tag-based primitive matching ───
        # Lookup table: asset_id → dict of {dimension_name: set(values)}.
        # Used only when a primitive declares required_tags + required_tag_dimension.
        asset_tag_dims: Dict[str, Dict[str, Set[str]]] = {}
        for asset in bundle.assets:
            asset_tag_dims[asset.internal_id] = {
                dim: set(values) for dim, values in (asset.tag_dimensions or {}).items()
            }

        # ── Asset id → name for reference ────────────────────────────────
        asset_name: Dict[str, str] = {a.internal_id: a.name for a in bundle.assets}

        # ── Extract each primitive ────────────────────────────────────────
        results: List[CapabilityPrimitive] = []

        for prim_id, defn in PRIMITIVE_DEFINITIONS.items():
            required_entities: List[str] = defn["required_entities"]
            required_columns: Set[str]   = defn.get("required_columns", set())
            required_tags: Set[str]      = defn.get("required_tags", set())
            required_tag_dim: str        = defn.get("required_tag_dimension", "")
            supporting_domains: List[str] = defn["supporting_domains"]

            # Entity coverage
            matched_entities = [e for e in required_entities if entity_to_assets.get(e)]
            missing_entities = [e for e in required_entities if e not in matched_entities]
            entity_score = len(matched_entities) / len(required_entities) if required_entities else 1.0

            # Column coverage (global warehouse)
            if required_columns:
                matched_columns = sorted(required_columns & all_col_names)
                missing_columns = sorted(required_columns - all_col_names)
                column_score = len(matched_columns) / len(required_columns)
            else:
                matched_columns = []
                missing_columns = []
                column_score = 1.0

            maturity_score = round(entity_score * 0.5 + column_score * 0.5, 4)

            # Supporting assets: represent any required entity AND have ≥1 required column (or tag)
            candidate_assets: Set[str] = set()
            for e in matched_entities:
                candidate_assets |= entity_to_assets.get(e, set())

            if required_columns:
                supporting = sorted(
                    aid for aid in candidate_assets
                    if asset_cols.get(aid, set()) & required_columns
                )
            elif required_tags and required_tag_dim:
                supporting = sorted(
                    aid for aid in candidate_assets
                    if asset_tag_dims.get(aid, {}).get(required_tag_dim, set()) & required_tags
                )
            else:
                supporting = sorted(candidate_assets)

            primitive_name = prim_id.replace("_", " ").title()

            results.append(CapabilityPrimitive(
                primitive_id=prim_id,
                primitive_name=primitive_name,
                description=defn["description"],
                entity_score=entity_score,
                column_score=column_score,
                maturity_score=maturity_score,
                matched_entities=matched_entities,
                missing_entities=missing_entities,
                matched_columns=matched_columns,
                missing_columns=missing_columns,
                supporting_asset_ids=supporting,
            ))

        return results
