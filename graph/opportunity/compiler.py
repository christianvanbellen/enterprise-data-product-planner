"""OpportunityGraphCompiler — Phase 4 orchestrator.

Adds an opportunity layer ON TOP of the existing semantic graph.
All new nodes and edges are additive.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Set

from ingestion.contracts.bundle import CanonicalBundle
from ingestion.normalisation.hashing import stable_hash, utc_now_iso
from graph.compiler.evidence import EvidenceRecord
from graph.schema.edges import EdgeType, GraphEdge
from graph.schema.nodes import GraphNode
from graph.opportunity.primitive_extractor import CapabilityPrimitiveExtractor
from graph.opportunity.archetype_library import InitiativeArchetypeLibrary
from graph.opportunity.planner import OpportunityPlanner
from graph.opportunity.gap_analyser import GapAnalyser


def _ev(rule_id: str, confidence: float, build_id: str,
        sources: list = None) -> Dict[str, Any]:
    return EvidenceRecord.opportunity(
        rule_id=rule_id,
        confidence=confidence,
        evidence_sources=sources or [],
        build_id=build_id,
    ).to_dict()


def _prim_id_str(p: object) -> str:
    """Normalise a possibly-dict missing_primitives entry to its string primitive ID."""
    return p if isinstance(p, str) else p.get("primitive_id", "")  # type: ignore[union-attr]


@dataclass
class OpportunityBuildArtifact:
    build_id: str
    timestamp_utc: str
    primitive_count: int
    initiative_count: int
    gap_count: int
    ready_now_count: int
    ready_with_enablement_count: int
    needs_foundational_work_count: int
    not_feasible_count: int
    top_initiatives: List[str]          # top 5 by composite_score
    highest_leverage_gaps: List[str]    # top 3 by leverage_score


class OpportunityGraphCompiler:
    """Compile opportunity layer nodes and edges into an existing graph store."""

    def compile(
        self,
        bundle: CanonicalBundle,
        graph_store: Any,           # JsonGraphStore (or compatible)
        build_id: str,
        min_entity_confidence: float = 0.0,
    ) -> OpportunityBuildArtifact:

        # ── Step 0: Purge stale opportunity layer (idempotent re-run) ────
        graph_store.purge_layer("opportunity")

        # ── Step 1: Extract primitives ────────────────────────────────────
        extractor = CapabilityPrimitiveExtractor()
        primitives = extractor.extract(
            bundle, graph_store, min_entity_confidence=min_entity_confidence,
        )

        # ── Step 2: Plan initiatives ──────────────────────────────────────
        library = InitiativeArchetypeLibrary()
        planner = OpportunityPlanner()
        opportunities = planner.plan(primitives, library)

        # ── Step 3: Identify gaps ─────────────────────────────────────────
        analyser = GapAnalyser()
        gaps = analyser.analyse(primitives, opportunities)

        # ── Step 4: Emit CapabilityPrimitiveNode for each primitive ───────
        primitive_nodes: Dict[str, GraphNode] = {}
        for p in primitives:
            node = GraphNode.from_capability_primitive(
                primitive_id=p.primitive_id,
                primitive_name=p.primitive_name,
                description=p.description,
                maturity_score=p.maturity_score,
                supporting_asset_ids=p.supporting_asset_ids,
                build_id=build_id,
                evidence=_ev("opportunity.primitive_node", p.maturity_score, build_id,
                             [{"type": "primitive_id", "value": p.primitive_id}]),
                status=p.status,
                blocker_class=p.blocker_class,
                expected_signal=p.expected_signal,
                source=p.source,
                rationale=p.rationale,
            )
            primitive_nodes[p.primitive_id] = node

        graph_store.upsert_nodes(list(primitive_nodes.values()))

        # ── Step 5: Emit InitiativeNode for each opportunity ──────────────
        available_prim_ids_by_init: Dict[str, Set[str]] = {}
        initiative_nodes: Dict[str, GraphNode] = {}

        for opp in opportunities:
            node = GraphNode.from_initiative(
                initiative_id=opp.initiative_id,
                initiative_name=opp.initiative_name,
                archetype=opp.archetype,
                readiness=opp.readiness,
                status=opp.status,
                business_value_score=opp.business_value_score,
                implementation_effort_score=opp.implementation_effort_score,
                build_id=build_id,
                evidence=_ev("opportunity.initiative_node", opp.composite_score, build_id,
                             [{"type": "initiative_id", "value": opp.initiative_id},
                              {"type": "readiness", "value": opp.readiness}]),
            )
            initiative_nodes[opp.initiative_id] = node

            # Build optional primitive availability from archetype definition.
            # Use primitive_nodes (all extracted primitives) not just required available.
            available_prim_ids_by_init[opp.initiative_id] = set(opp.available_primitives)
            archetype_def = library.get_archetype(opp.initiative_id)
            opt_prims = archetype_def.get("optional_primitives", [])
            opt_available = [p for p in opt_prims if p in primitive_nodes]
            opt_missing   = [p for p in opt_prims if p not in primitive_nodes]

            # Normalise missing_primitives: store as string IDs for serialisation
            missing_prim_ids = [_prim_id_str(p) for p in opp.missing_primitives]

            # Enrich node properties with full metadata for explorer
            node.properties.update({
                "composite_score":               opp.composite_score,
                "available_primitives":          opp.available_primitives,
                "missing_primitives":            missing_prim_ids,
                "optional_primitives_available": opt_available,
                "optional_primitives_missing":   opt_missing,
                "composes_with":                 opp.composes_with,
                "target_users":                  opp.target_users,
                "business_objective":            opp.business_objective,
                "output_type":                   opp.output_type,
                "blocker_details":               opp.blocker_details,
            })

        graph_store.upsert_nodes(list(initiative_nodes.values()))

        # ── Step 6: Emit GapNode for each gap ────────────────────────────
        gap_nodes: Dict[str, GraphNode] = {}
        for g in gaps:
            node = GraphNode.from_gap(
                primitive_id=g.primitive_id,
                gap_type=g.gap_type,
                description=g.description,
                blocking_initiative_ids=g.blocking_initiatives,
                build_id=build_id,
                evidence=_ev("opportunity.gap_node", g.leverage_score, build_id,
                             [{"type": "primitive_id", "value": g.primitive_id},
                              {"type": "gap_type", "value": g.gap_type}]),
            )
            gap_nodes[g.primitive_id] = node

            # Override name for YAML-sourced gaps with human-readable label
            if g.source == "yaml_research":
                node.properties["name"] = (
                    f"{g.gap_type.replace('_', ' ')}: {g.description[:45]}"
                )

            # Enrich with gap details for explorer
            node.properties.update({
                "maturity_score":  g.maturity_score,
                "matched_columns": g.matched_columns,
                "missing_columns": g.missing_columns,
                "leverage_score":  g.leverage_score,
                "source":          g.source,
            })

        graph_store.upsert_nodes(list(gap_nodes.values()))

        # ── Step 7: PRIMITIVE_COVERS edges: Asset → CapabilityPrimitive ──
        covers_edges: List[GraphEdge] = []
        seen: Set[str] = set()
        for p in primitives:
            prim_node = primitive_nodes[p.primitive_id]
            for asset_id in p.supporting_asset_ids:
                eid = f"e_{stable_hash(asset_id, prim_node.node_id, 'PRIMITIVE_COVERS')}"
                if eid in seen:
                    continue
                seen.add(eid)
                covers_edges.append(GraphEdge(
                    edge_id=eid,
                    edge_type=EdgeType.PRIMITIVE_COVERS,
                    source_node_id=asset_id,
                    target_node_id=prim_node.node_id,
                    properties={"confidence": p.maturity_score, "graph_layer": "opportunity"},
                    evidence=_ev("opportunity.primitive_covers", p.maturity_score, build_id,
                                 [{"type": "asset_id", "value": asset_id}]),
                    build_id=build_id,
                ))

        # ── Step 8: ENABLES edges: CapabilityPrimitive → Initiative ──────
        enables_edges: List[GraphEdge] = []
        seen_en: Set[str] = set()
        prim_by_id = {p.primitive_id: p for p in primitives}
        for opp in opportunities:
            init_node = initiative_nodes[opp.initiative_id]
            for pid in opp.available_primitives:
                prim_node = primitive_nodes.get(pid)
                if not prim_node:
                    continue
                eid = f"e_{stable_hash(prim_node.node_id, init_node.node_id, 'ENABLES')}"
                if eid in seen_en:
                    continue
                seen_en.add(eid)
                p = prim_by_id.get(pid)
                conf = p.maturity_score if p else 0.5
                enables_edges.append(GraphEdge(
                    edge_id=eid,
                    edge_type=EdgeType.ENABLES,
                    source_node_id=prim_node.node_id,
                    target_node_id=init_node.node_id,
                    properties={"confidence": conf, "graph_layer": "opportunity"},
                    evidence=_ev("opportunity.enables", conf, build_id,
                                 [{"type": "primitive_id", "value": pid}]),
                    build_id=build_id,
                ))

        # ── Step 9: REQUIRES edges: Initiative → CapabilityPrimitive ─────
        # available_primitives are primitives that exist in the warehouse (any maturity).
        # missing_primitives may contain dict entries for virtual/YAML gaps — those are
        # skipped here (they have no CapabilityPrimitiveNode to wire to).
        requires_edges: List[GraphEdge] = []
        seen_req: Set[str] = set()
        for opp in opportunities:
            init_node = initiative_nodes[opp.initiative_id]
            # Normalise missing to string IDs; virtual entries won't match any primitive node
            all_req = opp.available_primitives + [_prim_id_str(p) for p in opp.missing_primitives]
            for pid in all_req:
                prim_node = primitive_nodes.get(pid)
                if not prim_node:
                    continue   # virtual or undefined primitive — no CapabilityPrimitiveNode
                eid = f"e_{stable_hash(init_node.node_id, prim_node.node_id, 'REQUIRES')}"
                if eid in seen_req:
                    continue
                seen_req.add(eid)
                requires_edges.append(GraphEdge(
                    edge_id=eid,
                    edge_type=EdgeType.REQUIRES,
                    source_node_id=init_node.node_id,
                    target_node_id=prim_node.node_id,
                    properties={"graph_layer": "opportunity"},
                    evidence=_ev("opportunity.requires", 1.0, build_id,
                                 [{"type": "initiative_id", "value": opp.initiative_id}]),
                    build_id=build_id,
                ))

        # ── Step 10: BLOCKED_BY edges: Initiative → Gap ───────────────────
        # Gap-centric loop: covers both primitive-maturity gaps and YAML-sourced gaps.
        # Each GapResult.blocking_initiatives contains the original initiative IDs.
        blocked_edges: List[GraphEdge] = []
        seen_bl: Set[str] = set()
        for g in gaps:
            gap_node = gap_nodes.get(g.primitive_id)
            if not gap_node:
                continue
            for init_id in g.blocking_initiatives:
                init_node = initiative_nodes.get(init_id)
                if not init_node:
                    continue
                eid = f"e_{stable_hash(init_node.node_id, gap_node.node_id, 'BLOCKED_BY')}"
                if eid in seen_bl:
                    continue
                seen_bl.add(eid)
                blocked_edges.append(GraphEdge(
                    edge_id=eid,
                    edge_type=EdgeType.BLOCKED_BY,
                    source_node_id=init_node.node_id,
                    target_node_id=gap_node.node_id,
                    properties={"graph_layer": "opportunity"},
                    evidence=_ev("opportunity.blocked_by", 1.0, build_id,
                                 [{"type": "initiative_id", "value": init_id}]),
                    build_id=build_id,
                ))

        # ── Step 11: COMPOSES_WITH edges: Initiative → Initiative ──────────
        composes_edges: List[GraphEdge] = []
        seen_cw: Set[str] = set()
        for opp in opportunities:
            init_node = initiative_nodes[opp.initiative_id]
            for other_id in opp.composes_with:
                other_node = initiative_nodes.get(other_id)
                if not other_node:
                    continue
                # Use canonical (sorted) pair to avoid duplicates
                pair = tuple(sorted([init_node.node_id, other_node.node_id]))
                eid = f"e_{stable_hash(*pair, 'COMPOSES_WITH')}"
                if eid in seen_cw:
                    continue
                seen_cw.add(eid)
                composes_edges.append(GraphEdge(
                    edge_id=eid,
                    edge_type=EdgeType.COMPOSES_WITH,
                    source_node_id=init_node.node_id,
                    target_node_id=other_node.node_id,
                    properties={"graph_layer": "opportunity"},
                    evidence=_ev("opportunity.composes_with", 1.0, build_id,
                                 [{"type": "initiative_id", "value": opp.initiative_id}]),
                    build_id=build_id,
                ))

        # ── Persist all edges ─────────────────────────────────────────────
        all_edges = (covers_edges + enables_edges + requires_edges +
                     blocked_edges + composes_edges)
        graph_store.upsert_edges(all_edges)

        # ── Build artifact ────────────────────────────────────────────────
        readiness_counts = {"ready_now": 0, "ready_with_enablement": 0,
                            "needs_foundational_work": 0, "not_currently_feasible": 0}
        for opp in opportunities:
            readiness_counts[opp.readiness] += 1

        top5 = sorted(opportunities, key=lambda o: -o.composite_score)[:5]
        top5_names = [o.initiative_id for o in top5]

        top3_gaps = sorted(gaps, key=lambda g: -g.leverage_score)[:3]
        top3_gap_ids = [g.primitive_id for g in top3_gaps]

        return OpportunityBuildArtifact(
            build_id=build_id,
            timestamp_utc=utc_now_iso(),
            primitive_count=len(primitives),
            initiative_count=len(opportunities),
            gap_count=len(gaps),
            ready_now_count=readiness_counts["ready_now"],
            ready_with_enablement_count=readiness_counts["ready_with_enablement"],
            needs_foundational_work_count=readiness_counts["needs_foundational_work"],
            not_feasible_count=readiness_counts["not_currently_feasible"],
            top_initiatives=top5_names,
            highest_leverage_gaps=top3_gap_ids,
        )
