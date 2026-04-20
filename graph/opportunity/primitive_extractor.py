"""CapabilityPrimitiveExtractor — identifies analytical building blocks from the semantic graph."""

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Set

from ingestion.contracts.bundle import CanonicalBundle

# ---------------------------------------------------------------------------
# Primitive definitions registry
# ---------------------------------------------------------------------------

PRIMITIVE_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "quote_lifecycle": {
        "required_entities": ["policy", "coverage"],
        "required_columns": {"quote_id", "inception_date", "expiry_date",
                             "new_renewal", "policyholder_name"},
        "supporting_domains": ["underwriting"],
        "description": "End-to-end quote and policy lifecycle tracking",
    },
    "pricing_decomposition": {
        "required_entities": ["pricing_component"],
        "required_columns": {"tech_gnwp", "modtech_gnwp", "sold_gnwp",
                             "tech_elc", "commission"},
        "supporting_domains": ["pricing"],
        "description": "Technical vs modified technical vs sold premium breakdown",
    },
    "rate_change_monitoring": {
        "required_entities": ["pricing_component"],
        "required_columns": {"gross_rarc", "net_rarc", "claims_inflation",
                             "breadth_of_cover_change"},
        "supporting_domains": ["portfolio_monitoring", "pricing"],
        "description": "Risk-adjusted rate change tracking across renewals",
    },
    "claims_experience": {
        "required_entities": ["claim"],
        "required_columns": {"incurred", "paid", "burn_rate_ulr",
                             "gg_ulr", "gn_ulr"},
        "supporting_domains": ["underwriting"],
        "description": "Historical claims development and loss ratios",
    },
    "profitability_decomposition": {
        "required_entities": ["profitability_component"],
        "required_columns": {"sold_to_modtech", "modtech_to_tech",
                             "sold_to_plan", "target_to_plan"},
        "supporting_domains": ["profitability"],
        "description": "Multi-layer profitability variance analysis",
    },
    "broker_attribution": {
        "required_entities": ["broker", "policy"],
        "required_columns": {"broker_primary", "broker_code", "brokerage_pct"},
        "supporting_domains": ["distribution"],
        "description": "Broker and channel performance attribution",
    },
    "renewal_tracking": {
        "required_entities": ["policy", "policyholder"],
        "required_columns": {"new_renewal", "quote_id",
                             "inception_date", "expiry_date"},
        "supporting_domains": ["underwriting"],
        "description": "New business vs renewal tracking with policyholder linkage",
    },
    "product_line_segmentation": {
        "required_entities": ["line_of_business", "coverage"],
        "required_columns": set(),
        "required_tags": {"eupi", "d_o", "general_aviation", "contingency"},
        "supporting_domains": ["underwriting"],
        "description": "Product line and class of business segmentation",
    },
    "exposure_structure": {
        "required_entities": ["exposure", "coverage"],
        "required_columns": {"exposure", "limit_100", "deductible_value",
                             "excess", "policy_coverage_jurisdiction"},
        "supporting_domains": ["underwriting"],
        "description": "Exposure, limit, deductible and jurisdiction structure",
    },
}


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

        # ── Asset tags (product_lines) map ────────────────────────────────
        asset_tags: Dict[str, Set[str]] = {}
        for asset in bundle.assets:
            asset_tags[asset.internal_id] = set(asset.product_lines or [])

        # ── Asset id → name for reference ────────────────────────────────
        asset_name: Dict[str, str] = {a.internal_id: a.name for a in bundle.assets}

        # ── Extract each primitive ────────────────────────────────────────
        results: List[CapabilityPrimitive] = []

        for prim_id, defn in PRIMITIVE_DEFINITIONS.items():
            required_entities: List[str] = defn["required_entities"]
            required_columns: Set[str]   = defn.get("required_columns", set())
            required_tags: Set[str]      = defn.get("required_tags", set())
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
            elif required_tags:
                supporting = sorted(
                    aid for aid in candidate_assets
                    if asset_tags.get(aid, set()) & required_tags
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
