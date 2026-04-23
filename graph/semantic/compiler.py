"""SemanticGraphCompiler — Phase 3 orchestrator.

Adds a semantic layer ON TOP of the existing structural graph.
All new nodes and edges are additive; no structural elements are modified.
"""

from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import yaml

from ingestion.contracts.bundle import CanonicalBundle
from ingestion.normalisation.hashing import stable_hash, utc_now_iso
from graph.compiler.evidence import EvidenceRecord
from graph.schema.edges import EdgeType, GraphEdge
from graph.schema.nodes import GraphNode
from graph.semantic.conformed_binder import ConformedFieldBinder
from graph.semantic.domain_assigner import DomainAssigner
from graph.semantic.entity_mapper import EntityMapper
from graph.semantic.ontology_loader import SynonymRegistry

_ONTOLOGY_DIR = Path(__file__).parent.parent.parent / "ontology"


def _load_metric_patterns() -> Dict[str, str]:
    """Load column-name pattern → metric concept map from ontology YAML."""
    raw = yaml.safe_load(
        (_ONTOLOGY_DIR / "metric_patterns.yaml").read_text(encoding="utf-8")
    )
    return dict(raw.get("patterns") or {})


# Loaded from ontology/metric_patterns.yaml — edit that file to change metric inference.
# See docs/inputs.md for the matching-priority rules.
METRIC_NAME_PATTERNS: Dict[str, str] = _load_metric_patterns()


def _ev(rule_id: str, confidence: float, build_id: str,
        sources: List[Dict[str, str]] = None) -> Dict[str, Any]:
    return EvidenceRecord.semantic(
        rule_id=rule_id,
        confidence=confidence,
        evidence_sources=sources or [],
        build_id=build_id,
    ).to_dict()


@dataclass
class SemanticBuildArtifact:
    build_id: str
    timestamp_utc: str
    entity_node_count: int
    domain_node_count: int
    metric_node_count: int
    represents_edge_count: int
    belongs_to_domain_edge_count: int
    identifies_edge_count: int
    measures_edge_count: int
    metric_belongs_to_entity_edge_count: int = 0
    measures_from_semantic_candidates: int = 0
    measures_from_name_pattern: int = 0
    low_confidence_assignments: List[str] = field(default_factory=list)
    unassigned_assets: List[str] = field(default_factory=list)


class SemanticGraphCompiler:
    """Compile semantic layer nodes and edges into an existing graph store."""

    # ------------------------------------------------------------------ #
    # Metric name pattern lookup                                           #
    # Patterns loaded from ontology/metric_patterns.yaml at module import. #
    # ------------------------------------------------------------------ #

    METRIC_NAME_PATTERNS: Dict[str, str] = METRIC_NAME_PATTERNS

    # Pre-sort keys longest-first so more-specific patterns win
    _SORTED_PATTERN_KEYS: List[str] = sorted(
        METRIC_NAME_PATTERNS.keys(), key=len, reverse=True
    )

    def _infer_metric_concept(self, col_name: str) -> Optional[str]:
        """Return a metric concept name for col_name, or None if no pattern matches.

        Priority: exact match > suffix (_key) > prefix (key_) > substring (_key_).
        Longer keys are tested first to prefer more-specific patterns.
        """
        # 1. Exact match
        if col_name in self.METRIC_NAME_PATTERNS:
            return self.METRIC_NAME_PATTERNS[col_name]
        # 2 & 3. Word-boundary suffix, prefix, or interior (longest key first)
        for key in self._SORTED_PATTERN_KEYS:
            concept = self.METRIC_NAME_PATTERNS[key]
            if (col_name.endswith(f"_{key}")
                    or col_name.startswith(f"{key}_")
                    or f"_{key}_" in col_name):
                return concept
        return None

    # ------------------------------------------------------------------ #
    # Main compile method                                                  #
    # ------------------------------------------------------------------ #

    def compile(
        self,
        bundle: CanonicalBundle,
        graph_store: Any,           # JsonGraphStore (or compatible)
        build_id: str,
    ) -> SemanticBuildArtifact:

        # ── Step 1: Conformed field binding ──────────────────────────────
        binder = ConformedFieldBinder()
        binder_results = binder.bind(bundle)

        # ── Step 2: Entity mapping (all signals merged) ───────────────────
        mapper = EntityMapper()
        entity_candidates = mapper.map(bundle, binder_results)

        # ── Step 3: Domain assignment ─────────────────────────────────────
        depends_on_edges = [
            e for e in graph_store._edges.values()
            if e.get("edge_type") == "DEPENDS_ON"
        ]
        assigner = DomainAssigner()
        domain_assignments = assigner.assign(bundle, depends_on_edges)

        # ── Step 4: Emit DomainNode for each unique domain ────────────────
        domain_nodes: Dict[str, GraphNode] = {}
        for da in domain_assignments:
            if da.domain not in domain_nodes:
                node = GraphNode.from_domain(
                    da.domain, build_id,
                    _ev("semantic.domain_node", 1.0, build_id,
                        [{"type": "ontology", "value": da.domain}]),
                )
                domain_nodes[da.domain] = node
        graph_store.upsert_nodes(list(domain_nodes.values()))

        # ── Step 5: Emit BusinessEntityNode for each unique entity label ──
        entity_nodes: Dict[str, GraphNode] = {}
        for ec in entity_candidates:
            if ec.entity_label not in entity_nodes:
                node = GraphNode.from_business_entity(
                    ec.entity_label, build_id,
                    _ev("semantic.entity_node", 1.0, build_id,
                        [{"type": "ontology", "value": ec.entity_label}]),
                )
                entity_nodes[ec.entity_label] = node
        graph_store.upsert_nodes(list(entity_nodes.values()))

        # ── Step 6: BELONGS_TO_DOMAIN edges ──────────────────────────────
        btd_edges: List[GraphEdge] = []
        btd_seen: Set[str] = set()
        for da in domain_assignments:
            if da.domain not in domain_nodes:
                continue
            domain_nid = domain_nodes[da.domain].node_id
            eid = f"e_{stable_hash(da.asset_id, domain_nid, 'BELONGS_TO_DOMAIN')}"
            if eid in btd_seen:
                continue
            btd_seen.add(eid)
            btd_edges.append(GraphEdge(
                edge_id=eid,
                edge_type=EdgeType.BELONGS_TO_DOMAIN,
                source_node_id=da.asset_id,
                target_node_id=domain_nid,
                properties={"confidence": da.confidence, "graph_layer": "semantic",
                            "source": da.source},
                evidence=_ev("semantic.belongs_to_domain", da.confidence, build_id,
                             [{"type": "domain_assignment_source", "value": da.source}]),
                build_id=build_id,
            ))

        # ── Step 7: REPRESENTS edges ──────────────────────────────────────
        represents_edges: List[GraphEdge] = []
        rep_seen: Set[str] = set()
        for ec in entity_candidates:
            if ec.entity_label not in entity_nodes:
                continue
            entity_nid = entity_nodes[ec.entity_label].node_id
            eid = f"e_{stable_hash(ec.asset_id, entity_nid, 'REPRESENTS')}"
            if eid in rep_seen:
                continue
            rep_seen.add(eid)
            represents_edges.append(GraphEdge(
                edge_id=eid,
                edge_type=EdgeType.REPRESENTS,
                source_node_id=ec.asset_id,
                target_node_id=entity_nid,
                properties={"confidence": ec.confidence, "graph_layer": "semantic",
                            "signal_sources": ec.signal_sources},
                evidence=_ev("semantic.represents", ec.confidence, build_id,
                             [{"type": "signal_source", "value": s}
                              for s in ec.signal_sources]),
                build_id=build_id,
            ))

        # ── Step 8: IDENTIFIES edges ──────────────────────────────────────
        asset_to_entities: Dict[str, List[str]] = {}
        for ec in entity_candidates:
            asset_to_entities.setdefault(ec.asset_id, []).append(ec.entity_label)

        identifies_edges: List[GraphEdge] = []
        ids_seen: Set[str] = set()
        for col in bundle.columns:
            entity_labels = asset_to_entities.get(col.asset_internal_id, [])
            for entity_label in entity_labels:
                sig_cols = SynonymRegistry.ENTITY_SIGNATURE_COLUMNS.get(entity_label, set())
                if col.normalized_name not in sig_cols:
                    continue
                if entity_label not in entity_nodes:
                    continue
                entity_nid = entity_nodes[entity_label].node_id
                eid = f"e_{stable_hash(col.internal_id, entity_nid, 'IDENTIFIES')}"
                if eid in ids_seen:
                    continue
                ids_seen.add(eid)
                identifies_edges.append(GraphEdge(
                    edge_id=eid,
                    edge_type=EdgeType.IDENTIFIES,
                    source_node_id=col.internal_id,
                    target_node_id=entity_nid,
                    properties={"confidence": 0.9, "graph_layer": "semantic"},
                    evidence=_ev("semantic.identifies", 0.9, build_id,
                                 [{"type": "column_signature_match",
                                   "value": col.normalized_name}]),
                    build_id=build_id,
                ))

        # ── Step 9: MEASURES edges (expanded) ────────────────────────────
        # Uses semantic_candidates first; falls back to METRIC_NAME_PATTERNS.
        metric_nodes: Dict[str, GraphNode] = {}
        measures_edges: List[GraphEdge] = []
        meas_seen: Set[str] = set()
        measures_from_candidates = 0
        measures_from_pattern = 0

        for col in bundle.columns:
            if col.column_role != "measure":
                continue

            if col.semantic_candidates:
                metric_concept = col.semantic_candidates[0]
                confidence = 0.9
                measures_from_candidates += 1
            else:
                metric_concept = self._infer_metric_concept(col.normalized_name)
                if metric_concept is None:
                    continue
                confidence = 0.7
                measures_from_pattern += 1

            if metric_concept not in metric_nodes:
                node = GraphNode.from_metric(
                    metric_concept, build_id,
                    _ev("semantic.metric_node", 1.0, build_id,
                        [{"type": "metric_concept", "value": metric_concept}]),
                )
                metric_nodes[metric_concept] = node

            metric_nid = metric_nodes[metric_concept].node_id
            eid = f"e_{stable_hash(col.internal_id, metric_nid, 'MEASURES')}"
            if eid in meas_seen:
                continue
            meas_seen.add(eid)
            measures_edges.append(GraphEdge(
                edge_id=eid,
                edge_type=EdgeType.MEASURES,
                source_node_id=col.internal_id,
                target_node_id=metric_nid,
                properties={
                    "confidence": confidence,
                    "graph_layer": "semantic",
                    "col_name": col.normalized_name,
                    "match_source": "semantic_candidates" if col.semantic_candidates
                                    else "name_pattern",
                },
                evidence=_ev("semantic.measures", confidence, build_id,
                             [{"type": "column_role", "value": col.column_role},
                              {"type": "metric_concept", "value": metric_concept}]),
                build_id=build_id,
            ))

        if metric_nodes:
            graph_store.upsert_nodes(list(metric_nodes.values()))

        # ── Step 10: METRIC_BELONGS_TO_ENTITY edges ───────────────────────
        # Derived edge: MetricNode → BusinessEntityNode.
        # Links a metric to an entity when at least one asset REPRESENTS the
        # entity AND has a column that MEASURES this metric.

        # col_id → asset_id
        col_to_asset: Dict[str, str] = {
            c.internal_id: c.asset_internal_id for c in bundle.columns
        }

        # asset_id → set of entity node_ids  (from REPRESENTS edges)
        asset_to_entity_nids: Dict[str, Set[str]] = defaultdict(set)
        for e in represents_edges:
            asset_to_entity_nids[e.source_node_id].add(e.target_node_id)

        # entity_nid → set of asset_ids  (inverse, for evidence)
        entity_to_assets: Dict[str, Set[str]] = defaultdict(set)
        for e in represents_edges:
            entity_to_assets[e.target_node_id].add(e.source_node_id)

        # metric_nid → [(col_id, col_name, confidence)]
        metric_to_measures: Dict[str, List] = defaultdict(list)
        for e in measures_edges:
            metric_to_measures[e.target_node_id].append((
                e.source_node_id,
                e.properties.get("col_name", e.source_node_id),
                e.properties["confidence"],
            ))

        # asset_id → name  (for evidence strings)
        asset_name_map: Dict[str, str] = {a.internal_id: a.name for a in bundle.assets}

        mbe_edges: List[GraphEdge] = []
        mbe_seen: Set[str] = set()

        for metric_concept, metric_node in metric_nodes.items():
            metric_nid = metric_node.node_id

            # Accumulate (entity_nid → [confidences]) from all MEASURES edges
            entity_conf_accum: Dict[str, List[float]] = defaultdict(list)
            for col_id, _col_name, conf in metric_to_measures.get(metric_nid, []):
                asset_id = col_to_asset.get(col_id)
                if not asset_id:
                    continue
                for entity_nid in asset_to_entity_nids.get(asset_id, set()):
                    entity_conf_accum[entity_nid].append(conf)

            for entity_nid, confs in entity_conf_accum.items():
                eid = f"e_{stable_hash(metric_nid, entity_nid, 'METRIC_BELONGS_TO_ENTITY')}"
                if eid in mbe_seen:
                    continue
                mbe_seen.add(eid)

                avg_conf = round(sum(confs) / len(confs), 4)

                # Sample up to 5 contributing asset names as evidence
                contributing_assets = entity_to_assets[entity_nid] & {
                    col_to_asset[cid]
                    for cid, _, _ in metric_to_measures[metric_nid]
                    if cid in col_to_asset
                }
                sample_assets = sorted(
                    asset_name_map[aid] for aid in contributing_assets
                    if aid in asset_name_map
                )[:5]

                mbe_edges.append(GraphEdge(
                    edge_id=eid,
                    edge_type=EdgeType.METRIC_BELONGS_TO_ENTITY,
                    source_node_id=metric_nid,
                    target_node_id=entity_nid,
                    properties={
                        "confidence": avg_conf,
                        "graph_layer": "semantic",
                        "evidence_sources": sample_assets,
                    },
                    evidence=_ev("semantic.metric_belongs_to_entity", avg_conf, build_id,
                                 [{"type": "asset", "value": n} for n in sample_assets]),
                    build_id=build_id,
                ))

        # Batch-upsert all semantic edges
        all_edges = btd_edges + represents_edges + identifies_edges + measures_edges + mbe_edges
        graph_store.upsert_edges(all_edges)

        # ── Compute artifact metrics ──────────────────────────────────────
        assets_with_entity: Set[str] = {ec.asset_id for ec in entity_candidates}
        assets_with_domain: Set[str] = {da.asset_id for da in domain_assignments}

        asset_max_conf: Dict[str, float] = {}
        for ec in entity_candidates:
            asset_max_conf[ec.asset_id] = max(
                asset_max_conf.get(ec.asset_id, 0.0), ec.confidence
            )
        low_confidence = sorted(
            a.name for a in bundle.assets
            if a.internal_id in assets_with_entity
            and asset_max_conf.get(a.internal_id, 0.0) < 0.6
        )

        unassigned = sorted(
            a.name for a in bundle.assets
            if a.internal_id not in assets_with_entity
            and a.internal_id not in assets_with_domain
        )

        return SemanticBuildArtifact(
            build_id=build_id,
            timestamp_utc=utc_now_iso(),
            entity_node_count=len(entity_nodes),
            domain_node_count=len(domain_nodes),
            metric_node_count=len(metric_nodes),
            represents_edge_count=len(represents_edges),
            belongs_to_domain_edge_count=len(btd_edges),
            identifies_edge_count=len(identifies_edges),
            measures_edge_count=len(measures_edges),
            metric_belongs_to_entity_edge_count=len(mbe_edges),
            measures_from_semantic_candidates=measures_from_candidates,
            measures_from_name_pattern=measures_from_pattern,
            low_confidence_assignments=low_confidence,
            unassigned_assets=unassigned,
        )
