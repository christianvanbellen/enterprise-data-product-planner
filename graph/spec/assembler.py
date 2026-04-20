"""SpecAssembler — pure-deterministic spec document builder.

Queries the graph store and bundle to assemble a SpecDocument for a single
initiative.  No LLM calls.  Produces byte-identical output for the same
(initiative_id, graph_build_id) pair.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from ingestion.contracts.asset import CanonicalAsset, CanonicalColumn
from ingestion.contracts.bundle import CanonicalBundle
from ingestion.normalisation.hashing import stable_hash, utc_now_iso
from graph.opportunity.planner import OpportunityResult
from graph.opportunity.primitive_extractor import CapabilityPrimitive


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

class ColumnDetail(BaseModel):
    name: str
    data_type_family: str
    column_role: str
    description: Optional[str] = None
    tests: List[str] = Field(default_factory=list)
    semantic_candidates: List[str] = Field(default_factory=list)


class AssetDetail(BaseModel):
    asset_id: str
    name: str
    description: Optional[str] = None
    domain_candidates: List[str] = Field(default_factory=list)
    grain_keys: List[str] = Field(default_factory=list)
    upstream_dependents: int = 0
    columns: List[ColumnDetail] = Field(default_factory=list)


class PrimitiveDetail(BaseModel):
    primitive_id: str
    primitive_name: str
    description: str
    maturity_score: float
    matched_columns: List[str] = Field(default_factory=list)
    missing_columns: List[str] = Field(default_factory=list)
    supporting_assets: List[AssetDetail] = Field(default_factory=list)


class BlockerDetail(BaseModel):
    gap_type: str
    description: str
    source: str   # "primitive_maturity" | "yaml_research"
    feasibility_rationale: Optional[str] = None


class JoinPath(BaseModel):
    left_asset: str   # asset name
    right_asset: str
    shared_grain_keys: List[str]


class OutputColumn(BaseModel):
    name: str
    asset: str
    role: str
    description: Optional[str] = None
    semantic_candidate: Optional[str] = None


class TargetVariable(BaseModel):
    available: bool
    column_name: Optional[str] = None
    asset: Optional[str] = None
    description: Optional[str] = None
    gap_reason: Optional[str] = None


class OutputStructure(BaseModel):
    structure_type: str
    primary_grain: List[str] = Field(default_factory=list)
    grain_description: str = ""
    primary_source_asset: str = ""
    summary_source_asset: Optional[str] = None
    dimensions: List[OutputColumn] = Field(default_factory=list)
    measures: List[OutputColumn] = Field(default_factory=list)
    time_columns: List[OutputColumn] = Field(default_factory=list)
    target_variable: Optional[TargetVariable] = None
    feature_columns: Optional[List[OutputColumn]] = None
    history_note: Optional[str] = None
    pipeline_timestamp: Optional[str] = None


class DataRequisiteColumn(BaseModel):
    column_name: str
    description: Optional[str]
    data_type: str          # "float" | "string" | "date" | "boolean"
    role: str               # "identifier" | "dimension" | "measure" | "time"
    source_asset: Optional[str]    # warehouse asset that provides it
    source_column: Optional[str]   # column name in source asset
    derivation: str                # "direct_read" | "join" | "derived" | "absent"
    join_key: Optional[str]        # if derivation == "join", the join column


class JoinAssessment(BaseModel):
    left_asset: str
    left_type: str        # fact, dimension, bridge, snapshot, source, unknown
    right_asset: str
    right_type: str
    join_key: str
    join_safety: str      # "safe" | "risky" | "aggregation_required"
    join_direction: str   # "fact_to_dimension" | "fact_to_fact" |
                          # "fact_to_snapshot" | "fact_to_bridge" | "other"
    grain_match: bool     # True if both assets share same grain keys
    aggregation_needed: bool
    aggregation_note: Optional[str]
    safety_note: str      # one-sentence explanation


class DataRequisite(BaseModel):
    initiative_id: str
    canonical_table_name: str    # snake_case name for the output table
    grain_description: str
    grain_keys: List[str]
    minimal_source_assets: List[str]   # minimum set needed for the output
    columns: List[DataRequisiteColumn]
    build_complexity: str    # "single_table" | "simple_join" | "complex_join"
    build_notes: str         # one sentence on how to build
    table_type: str = "unknown"  # "fact"|"dimension"|"bridge"|"snapshot"|"source"|"unknown"
    primary_source_asset: str = ""
    source_asset_types: Dict[str, str] = Field(default_factory=dict)
    source_asset_grains: Dict[str, List[str]] = Field(default_factory=dict)
    join_assessments: List["JoinAssessment"] = Field(default_factory=list)


class SpecDocument(BaseModel):
    spec_id: str
    spec_type: str          # "full_spec" | "gap_brief"
    initiative_id: str
    initiative_name: str
    archetype: str
    readiness: str
    composite_score: float
    business_value_score: float
    implementation_effort_score: float
    business_objective: str
    output_type: str
    target_users: List[str] = Field(default_factory=list)
    composes_with: List[str] = Field(default_factory=list)
    available_primitives: List[PrimitiveDetail] = Field(default_factory=list)
    missing_primitives: List[Any] = Field(default_factory=list)
    blockers: List[BlockerDetail] = Field(default_factory=list)
    grain_join_paths: List[JoinPath] = Field(default_factory=list)
    # ── CHANGE 1: primitive-to-asset mapping and bill of materials ────────
    primitive_to_assets: Dict[str, List[str]] = Field(default_factory=dict)
    all_supporting_asset_names: List[str] = Field(default_factory=list)
    # ── feasibility rationale (from YAML research artifact) ──────────────
    feasibility_rationale: Optional[str] = None
    # ── output data structure (pre-computed by _build_output_structure) ──
    output_structure: Optional[OutputStructure] = None
    # ── data requisite (pre-computed by _build_data_requisite) ───────────
    data_requisite: Optional[DataRequisite] = None
    graph_build_id: str
    assembled_at_utc: str


# ---------------------------------------------------------------------------
# Assembler
# ---------------------------------------------------------------------------

_FULL_SPEC_READINESS = {"ready_now", "ready_with_enablement"}


class SpecAssembler:
    """Assemble a SpecDocument from graph state and bundle.

    Deterministic: same inputs → same output every time.
    """

    def assemble(
        self,
        opp: OpportunityResult,
        primitives: List[CapabilityPrimitive],
        bundle: CanonicalBundle,
        graph_store: Any,                       # JsonGraphStore or compatible
        graph_build_id: str,
        archetype_def: Optional[Dict[str, Any]] = None,   # from InitiativeArchetypeLibrary
    ) -> SpecDocument:

        prim_by_id = {p.primitive_id: p for p in primitives}

        # ── Upstream-dependent counts ──────────────────────────────────────
        dep_counts = _count_upstream_dependents(graph_store)

        # ── Column lookup from bundle ──────────────────────────────────────
        cols_by_asset: Dict[str, list] = {}
        for col in bundle.columns:
            cols_by_asset.setdefault(col.asset_internal_id, []).append(col)

        # ── Asset lookup ───────────────────────────────────────────────────
        asset_map: Dict[str, Any] = {}
        for node in graph_store._nodes.values():  # type: ignore[union-attr]
            if node.get("label") == "Asset":
                asset_map[node["node_id"]] = node

        # ── Determine which asset IDs get full column detail ───────────────
        all_supporting_ids: List[str] = []
        for pid in opp.available_primitives:
            p = prim_by_id.get(pid)
            if p:
                all_supporting_ids.extend(p.supporting_asset_ids)

        # Sort by upstream_dependents desc, dedup, take top 5
        seen_ids: set[str] = set()
        ordered_ids: List[str] = []
        for aid in sorted(
            set(all_supporting_ids),
            key=lambda a: -dep_counts.get(a, 0),
        ):
            if aid not in seen_ids:
                seen_ids.add(aid)
                ordered_ids.append(aid)
        top5_ids = set(ordered_ids[:5])

        # ── Build PrimitiveDetail list ─────────────────────────────────────
        available_prim_details: List[PrimitiveDetail] = []
        for pid in opp.available_primitives:
            p = prim_by_id.get(pid)
            if not p:
                continue
            asset_details = _build_asset_details(
                p.supporting_asset_ids, asset_map, dep_counts,
                cols_by_asset, top5_ids,
            )
            available_prim_details.append(PrimitiveDetail(
                primitive_id=p.primitive_id,
                primitive_name=p.primitive_name,
                description=p.description,
                maturity_score=p.maturity_score,
                matched_columns=sorted(p.matched_columns),
                missing_columns=sorted(p.missing_columns),
                supporting_assets=asset_details,
            ))

        # ── CHANGE 1: primitive_to_assets and all_supporting_asset_names ───
        primitive_to_assets: Dict[str, List[str]] = {}
        for pd in available_prim_details:
            primitive_to_assets[pd.primitive_id] = [a.name for a in pd.supporting_assets]

        all_supporting_asset_names: List[str] = sorted(set(
            name
            for names in primitive_to_assets.values()
            for name in names
        ))

        # ── Build BlockerDetail list ───────────────────────────────────────
        blockers: List[BlockerDetail] = []
        for gap in opp.yaml_data_gaps:
            blockers.append(BlockerDetail(
                gap_type=gap.get("gap_type", ""),
                description=gap.get("description", ""),
                source="yaml_research",
                feasibility_rationale=gap.get("feasibility_rationale") or None,
            ))
        for pid in opp.missing_primitives:
            if isinstance(pid, str):
                p = prim_by_id.get(pid)
                if p and p.maturity_score < 0.5:
                    blockers.append(BlockerDetail(
                        gap_type="incomplete_primitive",
                        description=f"{pid} maturity={p.maturity_score:.2f} — {p.description}",
                        source="primitive_maturity",
                    ))

        # ── Grain join paths ───────────────────────────────────────────────
        grain_join_paths = _compute_grain_join_paths(all_supporting_ids, asset_map)

        # ── spec_type ──────────────────────────────────────────────────────
        spec_type = (
            "full_spec" if opp.readiness in _FULL_SPEC_READINESS else "gap_brief"
        )

        # ── feasibility_rationale from archetype def ───────────────────────
        feasibility_rationale: Optional[str] = None
        if archetype_def:
            feasibility_rationale = archetype_def.get("feasibility_rationale")

        # ── output data structure ─────────────────────────────────────────
        output_structure = self._build_output_structure(
            opp=opp,
            spec_type=spec_type,
            available_prim_details=available_prim_details,
            blockers=blockers,
        )

        # ── Bundle-wide asset type inference (for data requisite) ─────────
        # Build name → upstream_dependents count using the graph node IDs
        dep_counts_by_name: Dict[str, int] = {}
        for node_id, node in asset_map.items():
            name = node["properties"].get("name", "")
            if name:
                dep_counts_by_name[name] = dep_counts.get(node_id, 0)

        asset_types: Dict[str, str] = {}
        for ba in bundle.assets:
            ba_cols = cols_by_asset.get(ba.internal_id, [])
            asset_types[ba.name] = self._infer_table_type(ba, ba_cols, graph_store)

        # ── data requisite ────────────────────────────────────────────────
        data_requisite = self._build_data_requisite(
            opp=opp,
            output_structure=output_structure,
            all_supporting_asset_names=all_supporting_asset_names,
            available_prim_details=available_prim_details,
            asset_types=asset_types,
            dep_counts_by_name=dep_counts_by_name,
            bundle_assets=list(bundle.assets),
            bundle_cols_by_id=cols_by_asset,
        )

        spec_id = stable_hash(opp.initiative_id, graph_build_id)

        return SpecDocument(
            spec_id=spec_id,
            spec_type=spec_type,
            initiative_id=opp.initiative_id,
            initiative_name=opp.initiative_name,
            archetype=opp.archetype,
            readiness=opp.readiness,
            composite_score=opp.composite_score,
            business_value_score=opp.business_value_score,
            implementation_effort_score=opp.implementation_effort_score,
            business_objective=opp.business_objective,
            output_type=opp.output_type,
            target_users=list(opp.target_users),
            composes_with=list(opp.composes_with),
            available_primitives=available_prim_details,
            missing_primitives=list(opp.missing_primitives),
            blockers=blockers,
            grain_join_paths=grain_join_paths,
            primitive_to_assets=primitive_to_assets,
            all_supporting_asset_names=all_supporting_asset_names,
            feasibility_rationale=feasibility_rationale,
            output_structure=output_structure,
            data_requisite=data_requisite,
            graph_build_id=graph_build_id,
            assembled_at_utc=utc_now_iso(),
        )


    def _build_output_structure(
        self,
        opp: OpportunityResult,
        spec_type: str,
        available_prim_details: List[PrimitiveDetail],
        blockers: List[BlockerDetail],
    ) -> OutputStructure:
        """Deterministically build OutputStructure from assembled primitives."""
        # 1. structure_type
        _type_map: Dict[str, str] = {
            "monitoring_dashboard": "monitoring_dashboard",
            "decision_support":     "decision_support",
            "analytics_product":    "analytics_product",
            "ai_agent":             "ai_agent",
        }
        if spec_type == "gap_brief" and not available_prim_details:
            structure_type = "gap_brief"
        elif opp.archetype == "prediction":
            # ML prediction initiatives expose target_variable — treat as decision_support
            structure_type = "decision_support"
        else:
            structure_type = _type_map.get(opp.output_type, opp.output_type)

        # Deduplicate assets across primitives (keep highest upstream_dependents per name)
        assets_by_name: Dict[str, AssetDetail] = {}
        for prim in available_prim_details:
            for asset in prim.supporting_assets:
                existing = assets_by_name.get(asset.name)
                if existing is None or asset.upstream_dependents > existing.upstream_dependents:
                    assets_by_name[asset.name] = asset
        all_assets = list(assets_by_name.values())

        # All primitive matched column names across all primitives
        all_matched: set[str] = set()
        for prim in available_prim_details:
            all_matched.update(prim.matched_columns)

        # 2. primary_source_asset: two-stage ranking (preference tier first,
        #    upstream_dependents second) to avoid peripheral rating-model assets
        def _has_prim_col(asset: AssetDetail) -> bool:
            if asset.columns:
                return any(c.name in all_matched for c in asset.columns)
            # No column detail — accept if it serves a primitive that has matched_columns
            for prim in available_prim_details:
                if any(a.name == asset.name for a in prim.supporting_assets):
                    if prim.matched_columns:
                        return True
            return False

        candidates = sorted(
            all_assets,
            key=lambda a: (_asset_preference_tier(a.name), -a.upstream_dependents),
        )
        prim_candidates = [a for a in candidates if _has_prim_col(a)]
        primary_asset: Optional[AssetDetail] = (
            prim_candidates[0] if prim_candidates else (candidates[0] if candidates else None)
        )
        primary_source_asset = primary_asset.name if primary_asset else ""

        # 3. summary_source_asset — two-stage ranking within _totals/_total_our_share_usd names
        summary_candidates = [
            a for a in all_assets
            if a.name.endswith("_total_our_share_usd") or a.name.endswith("_totals")
        ]
        summary_candidates.sort(
            key=lambda a: (_asset_preference_tier(a.name), -a.upstream_dependents)
        )
        summary_asset = summary_candidates[0] if summary_candidates else None
        summary_source_asset = summary_asset.name if summary_asset else None

        # 4. primary_grain — try primary asset first, then any asset with grain_keys
        primary_grain: List[str] = (
            list(primary_asset.grain_keys) if primary_asset and primary_asset.grain_keys else []
        )
        if not primary_grain:
            for fallback in sorted(all_assets, key=lambda a: -a.upstream_dependents):
                if fallback.grain_keys:
                    primary_grain = list(fallback.grain_keys)
                    break

        # 5. grain_description
        grain_description = _compute_grain_description(primary_grain)

        # 6. dimensions — pass all_matched so Signal C applies here too
        dimensions = _collect_output_columns(
            all_assets,
            role_filter={"categorical_attribute", "attribute"},
            limit=6,
            prefer_names=all_matched,
            prefer_primary=primary_source_asset,
        )

        # 7. measures (prefer primitive matched columns)
        measures = _collect_output_columns(
            all_assets,
            role_filter={"measure", "numeric_attribute"},
            limit=8,
            prefer_names=all_matched,
            prefer_primary=primary_source_asset,
        )

        # 8. time_columns: scan ALL assets with column detail (top-5).
        #    _pdm_last_update_timestamp is separated as pipeline_timestamp.
        #    Deduplicate by column name, keeping the instance with a description.
        _time_seen: Dict[str, OutputColumn] = {}
        pipeline_timestamp: Optional[str] = None
        for _ta in all_assets:
            for col in _ta.columns:
                if col.column_role != "timestamp":
                    continue
                if col.name == "_pdm_last_update_timestamp":
                    pipeline_timestamp = "_pdm_last_update_timestamp"
                    continue
                existing = _time_seen.get(col.name)
                if existing is None or (not existing.description and col.description):
                    _time_seen[col.name] = OutputColumn(
                        name=col.name,
                        asset=_ta.name,
                        role=col.column_role,
                        description=col.description,
                        semantic_candidate=(
                            col.semantic_candidates[0] if col.semantic_candidates else None
                        ),
                    )
        time_columns = list(_time_seen.values())

        # 9. target_variable (decision_support structure only)
        target_variable: Optional[TargetVariable] = None
        if structure_type == "decision_support":
            outcome_blocker = next(
                (b for b in blockers if b.gap_type == "insufficient_outcome_labels"), None
            )
            if outcome_blocker:
                target_variable = TargetVariable(
                    available=False,
                    gap_reason=outcome_blocker.description,
                )
            else:
                # Look for a column suggesting a settled/final outcome
                found: Optional[tuple[ColumnDetail, str]] = None
                for asset in all_assets:
                    for col in asset.columns:
                        n = col.name.lower()
                        if (
                            any(kw in n for kw in ("ultimate", "settled", "final"))
                            and col.column_role in ("measure", "numeric_attribute")
                        ):
                            found = (col, asset.name)
                            break
                    if found:
                        break
                if found:
                    col, aname = found
                    target_variable = TargetVariable(
                        available=True,
                        column_name=col.name,
                        asset=aname,
                        description=col.description,
                    )

        # 10. feature_columns (prediction archetype only)
        feature_columns: Optional[List[OutputColumn]] = None
        if opp.archetype == "prediction":
            feature_columns = _collect_output_columns(
                all_assets,
                role_filter={"measure", "numeric_attribute"},
                limit=8,
                prefer_names=all_matched,
                prefer_primary=primary_source_asset,
            )

        # 11. history_note
        history_note: Optional[str] = None
        if any(a.name.startswith("hx_") for a in all_assets):
            history_note = "Historical data available via hx_ asset family"

        return OutputStructure(
            structure_type=structure_type,
            primary_grain=primary_grain,
            grain_description=grain_description,
            primary_source_asset=primary_source_asset,
            summary_source_asset=summary_source_asset,
            dimensions=dimensions,
            measures=measures,
            time_columns=time_columns,
            target_variable=target_variable,
            feature_columns=feature_columns,
            history_note=history_note,
            pipeline_timestamp=pipeline_timestamp,
        )


    def _infer_table_type(
        self,
        asset: CanonicalAsset,
        asset_columns: List[CanonicalColumn],
        graph_store: Any,
    ) -> str:
        """Infer table type using four graph signals — no name pattern matching.

        Signals (priority order):
          Signal 3 (bridge)  — grain_key_count >= 4
          Signal 1 (lineage) — direct lineage_layer mappings
          Signal 4 (refinement) — mart-layer composition lean
          Signal 2 (composition) — column role ratios
          Tiebreaker

        Returns one of: "fact" | "dimension" | "bridge" | "snapshot" | "source" | "unknown"
        """
        # Signal 1 — lineage layer direct mapping
        _LAYER_TO_TYPE: Dict[str, str] = {
            "source_table":      "source",
            "raw_layer":         "source",
            "historic_exchange": "snapshot",
        }
        signal1 = _LAYER_TO_TYPE.get(asset.lineage_layer or "")  # None for gen2_mart / liberty_link / etc.

        # Signal 2 — column composition ratios
        total = len(asset_columns)
        if total == 0:
            return "unknown"

        fact_cols = sum(
            1 for c in asset_columns
            if c.column_role in ("measure", "numeric_attribute")
        )
        dim_cols = sum(
            1 for c in asset_columns
            if c.column_role in ("categorical_attribute", "attribute")
        )
        fact_ratio = fact_cols / total
        dim_ratio  = dim_cols  / total

        if fact_ratio > 0.55:
            composition_signal: Optional[str] = "fact"
        elif dim_ratio > 0.45:
            composition_signal = "dimension"
        else:
            composition_signal = None   # ambiguous

        # Signal 3 — grain key count (definitive for bridge tables)
        grain_count = len(asset.grain_keys)
        if grain_count == 0:
            return "unknown"
        if grain_count >= 4:
            return "bridge"

        # Signal 1: lineage-layer mappings are definitive for source/snapshot
        if signal1 is not None:
            return signal1

        # Signal 4 — lineage-layer refinement for mart layers
        # gen2_mart tables lean toward "fact"; override only when dim_ratio is not
        # strongly in the dimension direction (< 0.6).
        if asset.lineage_layer == "gen2_mart":
            if composition_signal == "dimension" and dim_ratio < 0.6:
                composition_signal = "fact"

        # Apply composition signal
        if composition_signal is not None:
            return composition_signal

        # Tiebreaker
        if fact_ratio > dim_ratio:
            return "fact"
        if dim_ratio > fact_ratio:
            return "dimension"
        return "unknown"


    def _build_data_requisite(
        self,
        opp: OpportunityResult,
        output_structure: OutputStructure,
        all_supporting_asset_names: List[str],
        available_prim_details: List[PrimitiveDetail],
        asset_types: Optional[Dict[str, str]] = None,
        dep_counts_by_name: Optional[Dict[str, int]] = None,
        bundle_assets: Optional[List[CanonicalAsset]] = None,
        bundle_cols_by_id: Optional[Dict[str, list]] = None,
    ) -> Optional[DataRequisite]:
        """Build a DataRequisite describing how to construct the canonical output table."""
        os_ = output_structure
        if os_ is None or os_.structure_type == "gap_brief":
            return None

        _atype  = asset_types or {}
        _dcbn   = dep_counts_by_name or {}
        _bassets = bundle_assets or []
        _bcols  = bundle_cols_by_id or {}

        # 1. canonical_table_name
        suffix = "_dashboard" if os_.structure_type == "monitoring_dashboard" else "_mart"
        canonical_table_name = opp.initiative_id + suffix

        # 2. grain_keys
        grain_keys = list(os_.primary_grain)
        primary_grain_set = set(grain_keys)
        join_key = grain_keys[0] if len(grain_keys) == 1 else None

        # Build asset lookup (name → AssetDetail) from available_prim_details
        assets_by_name: Dict[str, AssetDetail] = {}
        for prim in available_prim_details:
            for asset in prim.supporting_assets:
                existing = assets_by_name.get(asset.name)
                if existing is None or asset.upstream_dependents > existing.upstream_dependents:
                    assets_by_name[asset.name] = asset

        supporting_set = set(all_supporting_asset_names)
        primary = os_.primary_source_asset

        _type_map: Dict[str, str] = {
            "measure":              "float",
            "numeric_attribute":    "float",
            "categorical_attribute":"string",
            "attribute":            "string",
            "identifier":           "string",
            "timestamp":            "date",
        }
        _role_map: Dict[str, str] = {
            "measure":              "measure",
            "numeric_attribute":    "measure",
            "categorical_attribute":"dimension",
            "attribute":            "dimension",
            "identifier":           "identifier",
            "timestamp":            "time",
        }

        # FIX 1: grain key identifier columns (first entries)
        # Look up descriptions from the primary asset's column details
        primary_asset_detail = assets_by_name.get(primary)
        grain_col_desc: Dict[str, Optional[str]] = {}
        if primary_asset_detail:
            for col in primary_asset_detail.columns:
                if col.name in primary_grain_set:
                    grain_col_desc[col.name] = col.description
        # Also try bundle columns for the primary asset
        primary_ba = next((ba for ba in _bassets if ba.name == primary), None)
        if primary_ba:
            for col in _bcols.get(primary_ba.internal_id, []):
                if col.name in primary_grain_set and col.name not in grain_col_desc:
                    grain_col_desc[col.name] = col.description

        id_columns: List[DataRequisiteColumn] = []
        for gk in grain_keys:
            id_columns.append(DataRequisiteColumn(
                column_name=gk,
                description=grain_col_desc.get(gk),
                data_type="string",
                role="identifier",
                source_asset=primary if primary else None,
                source_column=gk,
                derivation="direct_read",
                join_key=None,
            ))

        # Track which column names are already covered (dedup)
        seen_col_names: set[str] = set(grain_keys)

        # FIX 2: dimension columns from joinable supporting assets (within primitives).
        # A supporting asset qualifies if primary_grain_set ⊆ asset.grain_keys.
        joinable_assets = sorted(
            [
                a for name, a in assets_by_name.items()
                if name != primary
                and a.grain_keys
                and primary_grain_set <= set(a.grain_keys)
            ],
            key=lambda a: -a.upstream_dependents,
        )

        join_dim_columns: List[DataRequisiteColumn] = []
        for asset in joinable_assets:
            if len(join_dim_columns) >= 8:
                break
            shared_grain = sorted(primary_grain_set & set(asset.grain_keys))
            jk = shared_grain[0] if len(shared_grain) == 1 else None
            for col in asset.columns:
                if len(join_dim_columns) >= 8:
                    break
                if col.column_role not in ("categorical_attribute", "attribute"):
                    continue
                if col.name in seen_col_names:
                    continue
                has_desc = bool(col.description and col.description.strip())
                has_sem  = bool(col.semantic_candidates)
                if not (has_desc or has_sem):
                    continue
                seen_col_names.add(col.name)
                join_dim_columns.append(DataRequisiteColumn(
                    column_name=col.name,
                    description=col.description,
                    data_type=_type_map.get(col.column_role, "string"),
                    role="dimension",
                    source_asset=asset.name,
                    source_column=col.name,
                    derivation="join",
                    join_key=jk,
                ))

        # FIX B: cross-bundle dimension enrichment using inferred asset types.
        # Scans ALL bundle assets for "dimension"-typed assets that are:
        #   (a) not already the primary source
        #   (b) joinable: primary_grain_set ⊆ set(asset.grain_keys)
        #   (c) core dimension: upstream_dependents > 1 (DEPENDS_ON structural signal)
        # Sorted by (-upstream_dependents, len(grain_keys), name) for determinism.
        if _bassets and primary_grain_set:
            dim_candidates = sorted(
                [
                    ba for ba in _bassets
                    if ba.name != primary
                    and _atype.get(ba.name) == "dimension"
                    and ba.grain_keys
                    and primary_grain_set <= set(ba.grain_keys)
                    and _dcbn.get(ba.name, 0) > 1
                ],
                key=lambda ba: (-_dcbn.get(ba.name, 0), len(ba.grain_keys), ba.name),
            )
            for ba in dim_candidates:
                if len(join_dim_columns) >= 8:
                    break
                shared_grain = sorted(primary_grain_set & set(ba.grain_keys))
                jk = shared_grain[0] if len(shared_grain) == 1 else None
                for col in _bcols.get(ba.internal_id, []):
                    if len(join_dim_columns) >= 8:
                        break
                    if col.column_role not in ("categorical_attribute", "attribute"):
                        continue
                    if col.name in seen_col_names:
                        continue
                    has_desc = bool(col.description and col.description.strip())
                    has_sem  = bool(col.semantic_candidates)
                    if not (has_desc or has_sem):
                        continue
                    seen_col_names.add(col.name)
                    join_dim_columns.append(DataRequisiteColumn(
                        column_name=col.name,
                        description=col.description,
                        data_type=_type_map.get(col.column_role, "string"),
                        role="dimension",
                        source_asset=ba.name,
                        source_column=col.name,
                        derivation="join",
                        join_key=jk,
                    ))

        # 3. Build remaining columns from output_structure (dimensions / measures / time),
        #    deduped against identifiers and join dimensions already added.
        os_columns: List[DataRequisiteColumn] = []
        all_output_cols = list(os_.dimensions) + list(os_.measures) + list(os_.time_columns)
        for col in all_output_cols:
            if col.name in seen_col_names:
                continue
            seen_col_names.add(col.name)
            src = col.asset
            if src == primary:
                derivation = "direct_read"
            elif src in supporting_set:
                derivation = "join"
            else:
                derivation = "absent"
            os_columns.append(DataRequisiteColumn(
                column_name=col.name,
                description=col.description,
                data_type=_type_map.get(col.role, "string"),
                role=_role_map.get(col.role, "dimension"),
                source_asset=src if derivation != "absent" else None,
                source_column=col.name,
                derivation=derivation,
                join_key=join_key if derivation == "join" else None,
            ))

        # Assemble: identifiers first, then join dims, then output_structure columns
        dr_columns = id_columns + join_dim_columns + os_columns

        # FIX A: set table_type from inferred type of primary source asset
        table_type = _atype.get(primary, "unknown")

        # Recompute minimal_source_assets from ALL columns
        minimal_source_assets = sorted({
            c.source_asset for c in dr_columns
            if c.derivation != "absent" and c.source_asset
        })

        n = len(minimal_source_assets)
        if n <= 1:
            build_complexity = "single_table"
        elif n == 2:
            build_complexity = "simple_join"
        else:
            build_complexity = "complex_join"

        # build_notes
        if build_complexity == "single_table":
            src_name = minimal_source_assets[0] if minimal_source_assets else primary
            build_notes = f"Read directly from {src_name}. No joins required."
        elif build_complexity == "simple_join":
            a, b = minimal_source_assets[0], minimal_source_assets[1]
            first_jk = next((c.join_key for c in dr_columns if c.derivation == "join" and c.join_key), None)
            key_str = first_jk or " + ".join(grain_keys)
            build_notes = f"Join {a} to {b} on {key_str}."
        else:
            n_src = len(minimal_source_assets)
            build_notes = f"Multi-asset join across {n_src} sources — see join paths section."

        # Build per-source metadata (types and grains) for renderer
        source_asset_types: Dict[str, str] = {}
        source_asset_grains: Dict[str, List[str]] = {}
        for src_name in minimal_source_assets:
            source_asset_types[src_name] = _atype.get(src_name, "unknown")
            ba = next((a for a in _bassets if a.name == src_name), None)
            if ba and ba.grain_keys:
                source_asset_grains[src_name] = list(ba.grain_keys)
            elif src_name in assets_by_name and assets_by_name[src_name].grain_keys:
                source_asset_grains[src_name] = list(assets_by_name[src_name].grain_keys)
            else:
                source_asset_grains[src_name] = list(grain_keys)

        # Build join assessments — one per unique (primary, right_asset) pair
        left = primary
        left_type = table_type
        left_grain = source_asset_grains.get(left, grain_keys)
        join_assessments: List[JoinAssessment] = []
        seen_join_pairs: set[tuple] = set()

        for col in dr_columns:
            if col.derivation != "join" or col.source_asset is None:
                continue
            right = col.source_asset
            if right == left:
                continue
            pair = (left, right)
            if pair in seen_join_pairs:
                continue
            seen_join_pairs.add(pair)

            right_type = _atype.get(right, "unknown")
            right_grain = source_asset_grains.get(right, [])
            jk = col.join_key or (grain_keys[0] if len(grain_keys) == 1 else "")

            grain_match = bool(right_grain) and set(left_grain) == set(right_grain)
            aggregation_needed = bool(right_grain) and len(right_grain) > len(left_grain)

            if left_type == "fact" and right_type == "dimension":
                join_direction = "fact_to_dimension"
            elif left_type == "fact" and right_type == "fact":
                join_direction = "fact_to_fact"
            elif left_type == "fact" and right_type == "snapshot":
                join_direction = "fact_to_snapshot"
            elif left_type == "fact" and right_type == "bridge":
                join_direction = "fact_to_bridge"
            else:
                join_direction = "other"

            if join_direction == "fact_to_dimension" and grain_match:
                join_safety = "safe"
                safety_note = "Dimension join on shared grain — no row duplication risk."
            elif join_direction == "fact_to_dimension":
                join_safety = "risky"
                safety_note = (
                    "Dimension grain is finer than fact grain — join may produce multiple "
                    "rows per fact row. Apply GROUP BY or filter to a single dimension row "
                    "before joining."
                )
            elif join_direction == "fact_to_fact":
                join_safety = "risky"
                safety_note = (
                    "Fact-to-fact join — verify grain alignment and aggregation strategy "
                    "to avoid double-counting."
                )
            elif join_direction == "fact_to_snapshot":
                join_safety = "safe"
                safety_note = (
                    "Snapshot join adds historical versions — filter by pas_id or date "
                    "range to avoid row multiplication."
                )
            elif join_direction == "fact_to_bridge":
                join_safety = "risky"
                safety_note = (
                    "Bridge table join may produce multiple rows per fact row. "
                    "Apply coverage_id or other filter."
                )
            else:
                join_safety = "risky"
                safety_note = (
                    "Join type could not be fully assessed — verify grain alignment "
                    "before building ETL."
                )

            aggregation_note: Optional[str] = None
            if aggregation_needed:
                aggregation_note = f"Aggregate {right} to {left} grain before joining."

            join_assessments.append(JoinAssessment(
                left_asset=left,
                left_type=left_type,
                right_asset=right,
                right_type=right_type,
                join_key=jk,
                join_safety=join_safety,
                join_direction=join_direction,
                grain_match=grain_match,
                aggregation_needed=aggregation_needed,
                aggregation_note=aggregation_note,
                safety_note=safety_note,
            ))

        return DataRequisite(
            initiative_id=opp.initiative_id,
            canonical_table_name=canonical_table_name,
            grain_description=os_.grain_description,
            grain_keys=grain_keys,
            minimal_source_assets=minimal_source_assets,
            columns=dr_columns,
            build_complexity=build_complexity,
            build_notes=build_notes,
            table_type=table_type,
            primary_source_asset=primary,
            source_asset_types=source_asset_types,
            source_asset_grains=source_asset_grains,
            join_assessments=join_assessments,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _count_upstream_dependents(graph_store: Any) -> Dict[str, int]:
    """Count DEPENDS_ON edges pointing TO each asset — high count = semantic core."""
    counts: Dict[str, int] = {}
    for edge in graph_store._edges.values():  # type: ignore[union-attr]
        if edge.get("edge_type") == "DEPENDS_ON":
            target = edge["target_node_id"]
            counts[target] = counts.get(target, 0) + 1
    return counts


def _build_asset_details(
    asset_ids: List[str],
    asset_map: Dict[str, Any],
    dep_counts: Dict[str, int],
    cols_by_asset: Dict[str, list],
    top5_ids: set,
) -> List[AssetDetail]:
    details: List[AssetDetail] = []
    for aid in asset_ids:
        node = asset_map.get(aid)
        if not node:
            continue
        props = node.get("properties", {})
        ud = dep_counts.get(aid, 0)

        col_details: List[ColumnDetail] = []
        if aid in top5_ids:
            raw_cols = cols_by_asset.get(aid, [])
            raw_cols = sorted(raw_cols, key=lambda c: (c.ordinal_position or 9999, c.name))
            for col in raw_cols:
                col_details.append(ColumnDetail(
                    name=col.name,
                    data_type_family=col.data_type_family,
                    column_role=col.column_role,
                    description=col.description,
                    tests=list(col.tests),
                    semantic_candidates=list(col.semantic_candidates),
                ))

        details.append(AssetDetail(
            asset_id=aid,
            name=props.get("name", aid),
            description=props.get("description"),
            domain_candidates=list(props.get("domain_candidates", [])),
            grain_keys=list(props.get("grain_keys", [])),
            upstream_dependents=ud,
            columns=col_details,
        ))
    details.sort(key=lambda d: -d.upstream_dependents)
    return details


def _compute_grain_description(grain_keys: List[str]) -> str:
    """Map a set of grain keys to a human-readable row description."""
    normalized = sorted(k.lower() for k in grain_keys)
    if not normalized:
        return "grain not determined"
    if normalized == ["quote_id"]:
        return "one row per quote"
    if normalized == ["layer_id", "quote_id"]:
        return "one row per layer per quote"
    if normalized == ["layer_id", "pas_id", "quote_id"]:
        return "one row per layer per quote per policy system record"
    if set(normalized) == {"coverage_id", "layer_id", "pas_id", "quote_id"}:
        return "one row per coverage per layer per quote"
    return "one row per " + " + ".join(sorted(grain_keys))


# Asset name terms used for two-stage ranking in primary/summary source selection.
_PREFERRED_ASSET_TERMS = (
    "_quote", "_policy", "_detail", "_summary",
    "_experience", "_monitoring", "_performance", "_measures",
)
_DEPRIORITISED_ASSET_TERMS = (
    "_rating", "_factor", "_load", "_war_", "_ops_",
    "_inputs", "_modifiers",
)


def _asset_preference_tier(name: str) -> int:
    """0 = preferred, 1 = neutral, 2 = deprioritised."""
    n = name.lower()
    if any(t in n for t in _DEPRIORITISED_ASSET_TERMS):
        return 2
    if any(t in n for t in _PREFERRED_ASSET_TERMS):
        return 0
    return 1


def _collect_output_columns(
    assets: List[AssetDetail],
    role_filter: set,
    limit: int,
    prefer_names: Optional[set] = None,
    prefer_primary: str = "",
) -> List[OutputColumn]:
    """Collect OutputColumn objects filtered by column_role, deduped, prioritised.

    Positive-inclusion filter: a column is included ONLY if it satisfies at
    least one of:
      Signal A — non-empty description
      Signal B — has at least one semantic_candidate
      Signal C — name appears in prefer_names (primitive matched_columns)

    Priority within included columns:
      0 — Signal A AND Signal C (described + primitive-matched)
      1 — Signal A only (has description)
      2 — Signal B or Signal C without description (semantic or primitive match)
    """
    seen: set[str] = set()
    buckets: Dict[int, List[OutputColumn]] = {0: [], 1: [], 2: []}

    ordered = sorted(
        assets,
        key=lambda a: (0 if a.name == prefer_primary else 1, -a.upstream_dependents),
    )
    for asset in ordered:
        for col in asset.columns:
            if col.column_role not in role_filter:
                continue
            if col.name in seen:
                continue

            has_desc = bool(col.description and col.description.strip())
            has_sem  = bool(col.semantic_candidates)
            in_prim  = prefer_names is not None and col.name in prefer_names

            # Positive inclusion: skip if no signal at all
            if not (has_desc or has_sem or in_prim):
                continue

            seen.add(col.name)
            out_col = OutputColumn(
                name=col.name,
                asset=asset.name,
                role=col.column_role,
                description=col.description,
                semantic_candidate=(
                    col.semantic_candidates[0] if col.semantic_candidates else None
                ),
            )
            priority = (
                0 if (has_desc and in_prim)
                else 1 if has_desc
                else 2   # has_sem or in_prim but no description
            )
            buckets[priority].append(out_col)

    combined: List[OutputColumn] = []
    for p in sorted(buckets):
        combined.extend(buckets[p])
    return combined[:limit]


def _compute_grain_join_paths(
    asset_ids: List[str],
    asset_map: Dict[str, Any],
) -> List[JoinPath]:
    """Find pairs of supporting assets that share >= 2 grain keys."""
    grains: List[tuple[str, List[str]]] = []
    seen: set[str] = set()
    for aid in asset_ids:
        if aid in seen:
            continue
        seen.add(aid)
        node = asset_map.get(aid)
        if not node:
            continue
        props = node.get("properties", {})
        gk = list(props.get("grain_keys", []))
        if gk:
            grains.append((props.get("name", aid), gk))

    paths: List[JoinPath] = []
    seen_pairs: set[tuple[str, str]] = set()
    for i, (name_a, keys_a) in enumerate(grains):
        for j, (name_b, keys_b) in enumerate(grains):
            if i >= j:
                continue
            shared = sorted(set(keys_a) & set(keys_b))
            if len(shared) >= 2:
                pair = (min(name_a, name_b), max(name_a, name_b))
                if pair not in seen_pairs:
                    seen_pairs.add(pair)
                    paths.append(JoinPath(
                        left_asset=name_a,
                        right_asset=name_b,
                        shared_grain_keys=shared,
                    ))
    return paths
