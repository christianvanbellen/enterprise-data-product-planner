"""DomainAssigner — assigns domain memberships to assets using Phase 1 signals
and lineage inheritance as a fallback.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Set

from ingestion.contracts.bundle import CanonicalBundle


@dataclass
class DomainAssignment:
    asset_id: str
    domain: str
    confidence: float
    source: str     # "phase1_keyword" | "lineage_inheritance"


class DomainAssigner:
    """Assign domain memberships.

    Signal 1 — Phase 1 domain_candidates (already on CanonicalAsset):
        primary   (index 0): confidence 0.85
        secondary (index 1+): confidence 0.65

    Signal 2 — Lineage inheritance:
        If an asset has no domain_candidates but ALL its direct upstream
        assets share at least one domain, inherit those shared domain(s).
        Confidence 0.5.
    """

    def assign(
        self,
        bundle: CanonicalBundle,
        depends_on_edges: List[Dict[str, Any]],
    ) -> List[DomainAssignment]:
        assignments: List[DomainAssignment] = []
        asset_id_set: Set[str] = {a.internal_id for a in bundle.assets}

        no_domain_assets: Set[str] = set()

        # ---- Signal 1: Phase 1 domain_candidates ----
        for asset in bundle.assets:
            if asset.domain_candidates:
                for i, domain in enumerate(asset.domain_candidates):
                    conf = 0.85 if i == 0 else 0.65
                    assignments.append(DomainAssignment(
                        asset_id=asset.internal_id,
                        domain=domain,
                        confidence=conf,
                        source="phase1_keyword",
                    ))
            else:
                no_domain_assets.add(asset.internal_id)

        if not no_domain_assets:
            return assignments

        # ---- Signal 2: Lineage inheritance ----
        # Build direct-upstream map: asset_id → list of upstream asset_ids
        # Edge convention: source_node_id = upstream dependency,
        #                  target_node_id = dependent (consumer)
        upstream_of: Dict[str, List[str]] = {a.internal_id: [] for a in bundle.assets}
        for edge in depends_on_edges:
            if edge.get("edge_type") != "DEPENDS_ON":
                continue
            src = edge.get("source_node_id", "")
            tgt = edge.get("target_node_id", "")
            if src in asset_id_set and tgt in asset_id_set:
                upstream_of[tgt].append(src)

        # Build known domains per asset from Signal 1
        known_domains: Dict[str, Set[str]] = {}
        for a in assignments:
            known_domains.setdefault(a.asset_id, set()).add(a.domain)

        for asset_id in no_domain_assets:
            upstream_ids = [u for u in upstream_of.get(asset_id, [])
                            if u in known_domains]
            if not upstream_ids:
                continue

            # Intersection: domains shared by ALL direct upstream assets
            shared = known_domains[upstream_ids[0]].copy()
            for uid in upstream_ids[1:]:
                shared &= known_domains[uid]

            for domain in shared:
                assignments.append(DomainAssignment(
                    asset_id=asset_id,
                    domain=domain,
                    confidence=0.5,
                    source="lineage_inheritance",
                ))

        return assignments
