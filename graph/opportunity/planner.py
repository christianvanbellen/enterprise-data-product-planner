"""OpportunityPlanner — maps available primitives to initiative archetypes."""

from dataclasses import dataclass, field
from typing import Any, Dict, List

from graph.opportunity.primitive_extractor import CapabilityPrimitive
from graph.opportunity.archetype_library import InitiativeArchetypeLibrary

_READINESS_MULTIPLIER: Dict[str, float] = {
    "ready_now":               1.0,
    "ready_with_enablement":   0.8,
    "needs_foundational_work": 0.4,
    "not_currently_feasible":  0.1,
}

# Lower index = less ready. Used to find the binding constraint.
_READINESS_RANK: Dict[str, int] = {
    "not_currently_feasible":  0,
    "needs_foundational_work": 1,
    "ready_with_enablement":   2,
    "ready_now":               3,
}


def _min_readiness(a: str, b: str) -> str:
    """Return the less-ready of two readiness values."""
    return a if _READINESS_RANK[a] <= _READINESS_RANK[b] else b


@dataclass
class OpportunityResult:
    initiative_id: str
    initiative_name: str
    archetype: str
    readiness: str
    business_value_score: float
    implementation_effort_score: float
    composite_score: float
    available_primitives: List[str]       # required primitives that exist at any maturity
    missing_primitives: List[Any]         # str IDs absent from warehouse; dicts for virtual gaps
    blocker_details: List[str]
    composes_with: List[str]              # other initiative IDs sharing >= 2 primitives
    target_users: List[str]
    business_objective: str
    output_type: str
    # Tri-state curatorial intent from initiative_research.yaml — distinct
    # from `readiness` (Phase 4's computed reality).
    status: str = "grounded"
    yaml_data_gaps: List[Dict[str, str]] = field(default_factory=list)


class OpportunityPlanner:
    """Match available primitives to initiative archetypes deterministically.

    Readiness is computed as the minimum of:
      - primitive-based readiness (what the warehouse can currently support)
      - feasibility_against_warehouse from the research artifact (literature ceiling)

    available_primitives contains every required primitive that exists in the
    warehouse model at any maturity (>= 0), so REQUIRES edges are always emitted
    for known primitives.  The readiness threshold (>= 0.5) is applied separately
    via avail_for_readiness and does not gate edge emission.

    For infeasible initiatives with no defined required_primitives, virtual
    missing_primitives entries are synthesised from YAML data_gaps so that the
    graph is honest about what's absent.
    """

    def plan(
        self,
        primitives: List[CapabilityPrimitive],
        library: InitiativeArchetypeLibrary,
    ) -> List[OpportunityResult]:

        prim_by_id: Dict[str, CapabilityPrimitive] = {p.primitive_id: p for p in primitives}

        # First pass: compute everything except composes_with
        raw: List[Dict[str, Any]] = []

        for initiative_id in library.all_initiatives():
            archetype_def = library.get_archetype(initiative_id)
            req_prims = archetype_def["required_primitives"]
            opt_prims = archetype_def.get("optional_primitives", [])
            data_gaps = archetype_def.get("data_gaps", [])

            # ── Primitive availability ─────────────────────────────────────
            # available: any defined primitive that was extracted (maturity >= 0)
            available = [pid for pid in req_prims if pid in prim_by_id]
            # missing (strings): defined in required_primitives but absent from warehouse
            missing_str = [pid for pid in req_prims if pid not in prim_by_id]

            # For initiatives with no defined required_primitives but YAML data_gaps,
            # synthesise virtual missing entries so the gap is visible in the graph.
            missing_virtual: List[Dict[str, Any]] = []
            if not req_prims and data_gaps:
                for gap in data_gaps:
                    missing_virtual.append({
                        "primitive_id": f"yaml_gap_{gap['gap_type']}",
                        "reason": "no_primitive_defined",
                        "source": "yaml_data_gap",
                        "gap_type": gap["gap_type"],
                        "description": gap["description"],
                    })

            missing: List[Any] = missing_str + missing_virtual

            # ── Readiness: keep 0.5 threshold for quality gate ─────────────
            avail_for_readiness = [pid for pid in req_prims
                                   if prim_by_id.get(pid) and prim_by_id[pid].maturity_score >= 0.5]
            missing_for_readiness = [pid for pid in req_prims if pid not in avail_for_readiness]

            opt_available = [pid for pid in opt_prims
                             if prim_by_id.get(pid) and prim_by_id[pid].maturity_score >= 0.5]

            if not missing_for_readiness:
                avg_mat = (sum(prim_by_id[pid].maturity_score for pid in req_prims) / len(req_prims)
                           if req_prims else 1.0)
                prim_readiness = "ready_now" if avg_mat >= 0.7 else "ready_with_enablement"
            else:
                max_mat = max(
                    (prim_by_id[pid].maturity_score for pid in req_prims if pid in prim_by_id),
                    default=0.0,
                )
                prim_readiness = "needs_foundational_work" if max_mat >= 0.3 else "not_currently_feasible"

            # Research artifact ceiling — take the less-ready of the two
            yaml_readiness = archetype_def.get("feasibility_against_warehouse", "ready_now")
            readiness = _min_readiness(yaml_readiness, prim_readiness)

            # Blocker details: YAML data gaps always included; add primitive gaps too
            blocker_details: List[str] = []
            for gap in data_gaps:
                blocker_details.append(f"{gap['gap_type']}: {gap['description']}")
            for pid in missing_for_readiness:
                p = prim_by_id.get(pid)
                mat_str = f"{p.maturity_score:.2f}" if p else "0.00"
                blocker_details.append(f"{pid} (maturity={mat_str})")

            # Composite score
            mult = _READINESS_MULTIPLIER[readiness]
            composite = round(
                archetype_def["business_value_score"] * mult * (1 + 0.1 * len(opt_available)),
                4,
            )

            # Display name from literature artifact if available, else title-case ID
            initiative_name = archetype_def.get("literature_name") or initiative_id.replace("_", " ").title()

            raw.append({
                "initiative_id":               initiative_id,
                "initiative_name":             initiative_name,
                "archetype":                   archetype_def["archetype"],
                "readiness":                   readiness,
                "status":                      archetype_def.get("status", "grounded"),
                "business_value_score":        archetype_def["business_value_score"],
                "implementation_effort_score": archetype_def["implementation_effort_score"],
                "composite_score":             composite,
                "available_primitives":        available,
                "missing_primitives":          missing,
                "blocker_details":             blocker_details,
                "yaml_data_gaps":              data_gaps,
                "all_primitives":              set(req_prims + opt_prims),
                "target_users":                archetype_def.get("target_users", []),
                "business_objective":          archetype_def.get("business_objective", ""),
                "output_type":                 archetype_def.get("output_type", ""),
            })

        # Second pass: compute composes_with (>= 2 shared primitives, either direction)
        results: List[OpportunityResult] = []
        for i, r in enumerate(raw):
            composes_with = []
            for j, other in enumerate(raw):
                if i == j:
                    continue
                shared = r["all_primitives"] & other["all_primitives"]
                if len(shared) >= 2:
                    composes_with.append(other["initiative_id"])
            results.append(OpportunityResult(
                initiative_id=r["initiative_id"],
                initiative_name=r["initiative_name"],
                archetype=r["archetype"],
                readiness=r["readiness"],
                status=r["status"],
                business_value_score=r["business_value_score"],
                implementation_effort_score=r["implementation_effort_score"],
                composite_score=r["composite_score"],
                available_primitives=r["available_primitives"],
                missing_primitives=r["missing_primitives"],
                blocker_details=r["blocker_details"],
                yaml_data_gaps=r["yaml_data_gaps"],
                composes_with=sorted(composes_with),
                target_users=r["target_users"],
                business_objective=r["business_objective"],
                output_type=r["output_type"],
            ))

        return results
