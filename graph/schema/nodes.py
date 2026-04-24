from dataclasses import dataclass, field
from typing import Any, Dict

from ingestion.contracts.asset import CanonicalAsset, CanonicalColumn
from ingestion.normalisation.hashing import stable_hash


@dataclass
class GraphNode:
    node_id: str
    label: str          # "Asset" | "Column" | "Schema" | "Test" | "DocObject"
    properties: Dict[str, Any]
    evidence: Dict[str, Any]
    build_id: str

    @classmethod
    def from_asset(cls, asset: CanonicalAsset, build_id: str, evidence: Dict[str, Any]) -> "GraphNode":
        props = {
            "asset_id": asset.internal_id,
            "name": asset.name,
            "normalized_name": asset.normalized_name,
            "asset_type": asset.asset_type,
            "database": asset.database,
            "schema_name": asset.schema_name,
            "path": asset.path,
            "description": asset.description,
            "tags": asset.tags,
            "materialization": asset.materialization,
            "row_count": asset.row_count,
            "grain_keys": asset.grain_keys,
            "domain_candidates": asset.domain_candidates,
            "domain_scores": asset.domain_scores,
            "tag_dimensions": asset.tag_dimensions,
            "is_enabled": asset.is_enabled,
            "version_hash": asset.version_hash,
        }
        return cls(
            node_id=asset.internal_id,
            label="Asset",
            properties=props,
            evidence=evidence,
            build_id=build_id,
        )

    @classmethod
    def from_schema(
        cls, schema_name: str, database: str, asset_count: int,
        build_id: str, evidence: Dict[str, Any],
    ) -> "GraphNode":
        """Create a Schema node. node_id matches the CONTAINS edge source format."""
        node_id = f"schema_{stable_hash(database or '', schema_name)}"
        props = {
            "schema_id": node_id,
            "name": schema_name,
            "database": database or None,
            "asset_count": asset_count,
        }
        return cls(
            node_id=node_id,
            label="Schema",
            properties=props,
            evidence=evidence,
            build_id=build_id,
        )

    @classmethod
    def from_business_entity(
        cls,
        entity_label: str,
        build_id: str,
        evidence: Dict[str, Any],
        candidacy_count: int = 0,
    ) -> "GraphNode":
        node_id = f"entity_{stable_hash('semantic', 'entity', entity_label)}"
        # status mirrors the entity_bindings gap-aware philosophy: a label with
        # zero candidate assets is a declared-but-empty aspirational entity —
        # the node exists so the backlog shows up in the graph, not only in
        # the entity_audit report.
        status = "grounded" if candidacy_count > 0 else "aspirational"
        props = {
            "entity_id": node_id,
            "entity_label": entity_label,
            "graph_layer": "semantic",
            "candidacy_count": candidacy_count,
            "status": status,
        }
        return cls(node_id=node_id, label="BusinessEntityNode", properties=props, evidence=evidence, build_id=build_id)

    @classmethod
    def from_domain(cls, domain_name: str, build_id: str, evidence: Dict[str, Any]) -> "GraphNode":
        node_id = f"domain_{stable_hash('semantic', 'domain', domain_name)}"
        props = {
            "domain_id": node_id,
            "domain_name": domain_name,
            "graph_layer": "semantic",
        }
        return cls(node_id=node_id, label="DomainNode", properties=props, evidence=evidence, build_id=build_id)

    @classmethod
    def from_metric(cls, metric_name: str, build_id: str, evidence: Dict[str, Any]) -> "GraphNode":
        node_id = f"metric_{stable_hash('semantic', 'metric', metric_name)}"
        props = {
            "metric_id": node_id,
            "metric_name": metric_name,
            "graph_layer": "semantic",
        }
        return cls(node_id=node_id, label="MetricNode", properties=props, evidence=evidence, build_id=build_id)

    @classmethod
    def from_capability_primitive(
        cls, primitive_id: str, primitive_name: str, description: str,
        maturity_score: float, supporting_asset_ids: list,
        build_id: str, evidence: Dict[str, Any],
        status: str = "grounded",
        blocker_class: Any = None,
        expected_signal: Any = None,
        source: Any = None,
        rationale: Any = None,
    ) -> "GraphNode":
        node_id = f"primitive_{stable_hash('opportunity', 'primitive', primitive_id)}"
        # Divergence between curatorial intent (status) and warehouse reality
        # (maturity_score) is a diagnostic signal: e.g. status=grounded with
        # maturity_score<1.0 means the curator expected full coverage the
        # warehouse doesn't have; status=aspirational with supporting assets
        # present means the primitive should be promoted.
        props = {
            "primitive_id": node_id,
            "primitive_name": primitive_name,
            "description": description,
            "maturity_score": maturity_score,
            "supporting_asset_ids": supporting_asset_ids,
            "graph_layer": "opportunity",
            "status": status,
            "blocker_class": blocker_class,
            "expected_signal": expected_signal,
            "source": source or [],
            "rationale": rationale,
        }
        return cls(node_id=node_id, label="CapabilityPrimitiveNode", properties=props, evidence=evidence, build_id=build_id)

    @classmethod
    def from_initiative(
        cls, initiative_id: str, initiative_name: str, archetype: str, readiness: str,
        business_value_score: float, implementation_effort_score: float,
        build_id: str, evidence: Dict[str, Any],
        status: str = "grounded",
    ) -> "GraphNode":
        node_id = f"initiative_{stable_hash('opportunity', 'initiative', initiative_id)}"
        # status is curatorial intent from initiative_research.yaml (grounded /
        # partial / aspirational). readiness is Phase 4's computed reality
        # (ready_now / ready_with_enablement / needs_foundational_work /
        # not_currently_feasible). Divergence is a diagnostic — e.g. an
        # aspirational initiative whose required primitives all land grounded
        # should be promoted.
        props = {
            "initiative_id": node_id,
            "initiative_key": initiative_id,    # original string ID — for sidebar lookups
            "initiative_name": initiative_name,
            "archetype": archetype,
            "readiness": readiness,
            "status": status,
            "business_value_score": business_value_score,
            "implementation_effort_score": implementation_effort_score,
            "graph_layer": "opportunity",
        }
        return cls(node_id=node_id, label="InitiativeNode", properties=props, evidence=evidence, build_id=build_id)

    @classmethod
    def from_gap(
        cls, primitive_id: str, gap_type: str, description: str,
        blocking_initiative_ids: list,
        build_id: str, evidence: Dict[str, Any],
    ) -> "GraphNode":
        node_id = f"gap_{stable_hash('opportunity', 'gap', primitive_id)}"
        props = {
            "gap_id": node_id,
            "name": f"{primitive_id} — {gap_type}",
            "primitive_id_ref": primitive_id,
            "gap_type": gap_type,
            "description": description,
            "blocking_initiative_ids": blocking_initiative_ids,
            "graph_layer": "opportunity",
        }
        return cls(node_id=node_id, label="GapNode", properties=props, evidence=evidence, build_id=build_id)

    @classmethod
    def from_column(cls, column: CanonicalColumn, build_id: str, evidence: Dict[str, Any]) -> "GraphNode":
        props = {
            "col_id": column.internal_id,
            "asset_internal_id": column.asset_internal_id,
            "name": column.name,
            "normalized_name": column.normalized_name,
            "description": column.description,
            "raw_data_type": column.raw_data_type,
            "data_type_family": column.data_type_family,
            "column_role": column.column_role,
            "ordinal_position": column.ordinal_position,
            "is_nullable": column.is_nullable,
            "tests": column.tests,
            "semantic_candidates": column.semantic_candidates,
            "version_hash": column.version_hash,
        }
        return cls(
            node_id=column.internal_id,
            label="Column",
            properties=props,
            evidence=evidence,
            build_id=build_id,
        )
