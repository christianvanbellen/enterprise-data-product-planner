"""GapAnalyser — produces structured gap records for missing or weak primitives."""

import hashlib
from dataclasses import dataclass
from typing import Dict, List, Tuple

from graph.opportunity.primitive_extractor import CapabilityPrimitive
from graph.opportunity.planner import OpportunityResult

# Gap type heuristic: derived from ontology/gap_types.yaml
_HISTORY_COLUMNS = {"inception_date", "expiry_date", "claim_history", "event_timeline"}


def _infer_gap_type(primitive: CapabilityPrimitive) -> str:
    if primitive.entity_score < 1.0:
        return "missing_conformed_entity"
    missing_set = set(primitive.missing_columns)
    if missing_set & _HISTORY_COLUMNS:
        return "missing_history"
    return "incomplete_relationship"


def _yaml_gap_primitive_id(gap_type: str, description: str) -> str:
    """Stable, deterministic primitive_id for a YAML-sourced gap."""
    key = f"{gap_type}||{description[:50]}"
    return f"yaml_gap_{hashlib.sha256(key.encode()).hexdigest()[:12]}"


def _prim_id_str(p: object) -> str:
    """Normalise a possibly-dict missing_primitives entry to its string ID."""
    return p if isinstance(p, str) else p.get("primitive_id", "")  # type: ignore[union-attr]


@dataclass
class GapResult:
    primitive_id: str
    gap_type: str
    description: str
    maturity_score: float
    matched_columns: List[str]
    missing_columns: List[str]
    blocking_initiatives: List[str]
    leverage_score: float
    source: str = "primitive_maturity"   # or "yaml_research"


class GapAnalyser:
    """Analyse primitives for gaps and produce GapResult records.

    Two categories of gap are emitted:

    1. Primitive-maturity gaps — primitives with maturity_score < 0.9 that are
       required by at least one initiative.

    2. YAML-sourced gaps — data_gaps declared in initiative_research.yaml for
       initiatives marked not_currently_feasible or needs_foundational_work.
       These represent missing source systems, absent outcome labels, etc. that
       primitives cannot capture.  Deduplicated by (gap_type, description[:50]).
    """

    def analyse(
        self,
        primitives: List[CapabilityPrimitive],
        opportunities: List[OpportunityResult],
    ) -> List[GapResult]:

        total_initiatives = len(opportunities)

        results: List[GapResult] = []

        # ── 1. Primitive-maturity gaps ────────────────────────────────────
        for primitive in primitives:
            if primitive.maturity_score >= 0.9:
                continue

            # All initiatives that require this primitive (available or missing)
            # are at risk — include both categories.
            blocking = [
                o.initiative_id for o in opportunities
                if primitive.primitive_id in (
                    o.available_primitives +
                    [_prim_id_str(p) for p in o.missing_primitives]
                )
            ]

            leverage = round(len(blocking) / total_initiatives, 4) if total_initiatives else 0.0

            results.append(GapResult(
                primitive_id=primitive.primitive_id,
                gap_type=_infer_gap_type(primitive),
                description=primitive.description,
                maturity_score=primitive.maturity_score,
                matched_columns=primitive.matched_columns,
                missing_columns=primitive.missing_columns,
                blocking_initiatives=sorted(blocking),
                leverage_score=leverage,
                source="primitive_maturity",
            ))

        # ── 2. YAML-sourced gaps ──────────────────────────────────────────
        # Deduplication key: (gap_type, description[:50])
        yaml_gap_index: Dict[Tuple[str, str], int] = {}

        for opp in opportunities:
            for gap in opp.yaml_data_gaps:
                gap_type = gap.get("gap_type", "")
                description = gap.get("description", "")
                key: Tuple[str, str] = (gap_type, description[:50])

                if key in yaml_gap_index:
                    existing = results[yaml_gap_index[key]]
                    if opp.initiative_id not in existing.blocking_initiatives:
                        existing.blocking_initiatives.append(opp.initiative_id)
                        existing.blocking_initiatives.sort()
                        existing.leverage_score = round(
                            len(existing.blocking_initiatives) / total_initiatives, 4
                        ) if total_initiatives else 0.0
                else:
                    prim_id = _yaml_gap_primitive_id(gap_type, description)
                    gap_result = GapResult(
                        primitive_id=prim_id,
                        gap_type=gap_type,
                        description=description,
                        maturity_score=0.0,
                        matched_columns=[],
                        missing_columns=[],
                        blocking_initiatives=[opp.initiative_id],
                        leverage_score=round(1 / total_initiatives, 4) if total_initiatives else 0.0,
                        source="yaml_research",
                    )
                    yaml_gap_index[key] = len(results)
                    results.append(gap_result)

        return results
