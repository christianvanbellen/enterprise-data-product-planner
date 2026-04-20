"""SpecRenderer — bounded LLM call that converts a SpecDocument to markdown.

Uses claude-sonnet-4-6 (pinned).  Never raises: if the LLM call fails,
rendered is set to "" and render_error carries the exception message.

Five sections are pre-rendered deterministically in Python and injected via
sentinel tokens; the LLM is only responsible for prose/analytical sections:

  {{BOM}}              → ## Data assets — bill of materials  (full_spec only)
  {{OUTPUT_STRUCTURE}} → ## Output data structure            (full_spec only)
  {{DATA_REQUISITES}}  → ## Data requisites                  (full_spec only)
  {{JOIN_PATHS}}       → ## Join paths                       (full_spec only)
  {{PRERENDERED}}      → ## Readiness evidence (full_spec) or ## Gap chain (gap_brief)

This guarantees that numeric values, ✓/✗ column lists, grain keys, join
tables, and output schema blocks are exact — the LLM cannot misrender them
— while leaving the LLM ~900 tokens of analytical prose (Overview, schematic,
one-sentence output context, Business objective, Key measures, Delivery
guidance, Composability), well within the 2 500-token budget.

Requires the anthropic package and ANTHROPIC_API_KEY in the environment.
"""

from __future__ import annotations

import json
import os
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Load .env if present (project root)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env", override=False)
except ImportError:
    pass

from graph.spec.assembler import SpecDocument, PrimitiveDetail, OutputStructure

_MODEL = "claude-sonnet-4-6"
_MAX_TOKENS_FULL = 2500
_MAX_TOKENS_BRIEF = 800

_SENTINEL          = "{{PRERENDERED}}"
_SENTINEL_BOM      = "{{BOM}}"
_SENTINEL_JOIN     = "{{JOIN_PATHS}}"
_SENTINEL_OUTPUT   = "{{OUTPUT_STRUCTURE}}"
_SENTINEL_DATA_REQ = "{{DATA_REQUISITES}}"

# ---------------------------------------------------------------------------
# Pre-rendered section builders (Python only, no LLM)
# ---------------------------------------------------------------------------

def _render_readiness_evidence(spec: SpecDocument) -> str:
    """Fully deterministic ## Readiness evidence section for full_spec."""
    lines: List[str] = ["## Readiness evidence", ""]

    # ── Primitive coverage ─────────────────────────────────────────────────
    lines += ["### Primitive coverage", ""]
    for prim in spec.available_primitives:
        total = len(prim.matched_columns) + len(prim.missing_columns)
        matched = len(prim.matched_columns)
        pct = round(matched / total * 100) if total else 100
        lines.append(f"**{prim.primitive_name}** — {matched}/{total} columns ({pct}%)")

        ticks = [f"✓ {c}" for c in sorted(prim.matched_columns)]
        crosses = [f"✗ {c} (absent)" for c in sorted(prim.missing_columns)]
        lines.append("  " + "   ".join(ticks + crosses))

        asset_names = spec.primitive_to_assets.get(prim.primitive_id, [])
        lines.append(f"  Assets: {', '.join(asset_names) if asset_names else '(none)'}")
        lines.append("")

    # ── Data quality signals ───────────────────────────────────────────────
    lines += ["### Data quality signals", ""]
    lines.append("| Asset | Columns | Tested | Coverage | Descriptions |")
    lines.append("|-------|---------|--------|----------|--------------|")

    seen_names: set[str] = set()
    delivery_risks: List[str] = []
    for prim in spec.available_primitives:
        for asset in prim.supporting_assets:
            if asset.name in seen_names or not asset.columns:
                continue
            seen_names.add(asset.name)
            n = len(asset.columns)
            tested = sum(1 for c in asset.columns if c.tests)
            described = sum(1 for c in asset.columns if c.description)
            pct = round(tested / n * 100) if n else 0
            risk = " ⚠" if pct < 30 else ""
            lines.append(
                f"| `{asset.name}` | {n} | {tested} | {pct}%{risk} | {described}/{n} |"
            )
            if pct < 30:
                delivery_risks.append(asset.name)

    if delivery_risks:
        lines += [
            "",
            f"⚠ Delivery risk: {', '.join(delivery_risks)} — test coverage below 30%.",
        ]
    lines.append("")

    # ── Confidence summary ─────────────────────────────────────────────────
    lines += ["### Confidence summary", ""]

    prims = spec.available_primitives
    avg_mat = sum(p.maturity_score for p in prims) / len(prims) if prims else 0.0
    full_ev = [p for p in prims if p.maturity_score >= 1.0]
    partial = [p for p in prims if p.maturity_score < 1.0]

    # Columns with assigned role but no description (role inferred from name pattern)
    inferred: List[str] = []
    for prim in prims:
        for asset in prim.supporting_assets:
            for col in asset.columns:
                if col.description is None and col.column_role not in ("identifier", "timestamp"):
                    inferred.append(f"{col.name} ({asset.name})")

    # Single-point fragility
    if partial:
        weakest = min(partial, key=lambda p: p.maturity_score)
        if weakest.missing_columns:
            frag = (
                f"Adding {weakest.missing_columns[0]} to {weakest.primitive_id} would raise "
                f"maturity from {weakest.maturity_score:.2f} toward full coverage."
            )
        else:
            frag = f"{weakest.primitive_id} is the weakest primitive at {weakest.maturity_score:.2f}."
    else:
        frag = (
            "All primitives at full maturity; readiness degrades only if a "
            "supporting asset is removed from the warehouse."
        )

    conf_pct = round(avg_mat * 100)
    parts = [
        f"Overall readiness confidence: {conf_pct}% "
        f"(avg primitive maturity {avg_mat:.3f}).",
    ]
    if full_ev:
        parts.append(
            f"Fully evidenced: {', '.join(p.primitive_id for p in full_ev)}."
        )
    if partial:
        parts.append(
            "Partially evidenced: "
            + ", ".join(f"{p.primitive_id} ({p.maturity_score:.2f})" for p in partial)
            + "."
        )
    if inferred:
        sample = inferred[:5]
        etc = "…" if len(inferred) > 5 else ""
        parts.append(
            f"Column roles inferred from name pattern (no description): "
            f"{', '.join(sample)}{etc}."
        )
    else:
        parts.append("All assigned column roles have supporting descriptions.")
    parts.append(f"Fragility: {frag}")

    lines.append("  ".join(parts))

    return "\n".join(lines)


def _render_gap_chain(spec: SpecDocument) -> str:
    """Fully deterministic ## Gap chain section for gap_brief."""
    lines: List[str] = ["## Gap chain", ""]

    if not spec.blockers:
        lines.append("No blockers identified.")
        return "\n".join(lines)

    prim_by_id = {p.primitive_id: p for p in spec.available_primitives}

    for blocker in spec.blockers:
        lines += [
            f"**Gap: {blocker.gap_type}**",
            f"Description: {blocker.description}",
            f"Source: {blocker.source}",
            "",
        ]

        if blocker.source == "primitive_maturity":
            # Extract primitive_id from description ("prim_id maturity=X.XX — ...")
            pid = blocker.description.split(" maturity=")[0] if " maturity=" in blocker.description else ""
            prim = prim_by_id.get(pid)
            if prim:
                total = len(prim.matched_columns) + len(prim.missing_columns)
                effort_map = {1: "low", 2: "low", 3: "medium"}
                effort = effort_map.get(len(prim.missing_columns), "high")
                lines += [
                    f"Primitive: {prim.primitive_id}",
                    f"Current maturity: {prim.maturity_score:.3f} "
                    f"({len(prim.matched_columns)}/{total} columns)",
                    f"Missing columns: {', '.join(sorted(prim.missing_columns)) or '(none)'}",
                    f"Columns needed to reach 0.9 threshold: "
                    f"{', '.join(sorted(prim.missing_columns)) or '(none)'}",
                    f"Estimated effort: {effort}",
                ]

        elif blocker.source == "yaml_research":
            rationale = (
                blocker.feasibility_rationale
                or spec.feasibility_rationale
                or "(feasibility rationale not specified)"
            )
            # downstream unblock: initiatives that share the same gap_type via composes_with
            downstream = spec.composes_with[:3] if spec.composes_with else []
            downstream_str = (
                ", ".join(downstream) + ("…" if len(spec.composes_with) > 3 else "")
                if downstream else "(none identified)"
            )
            lines += [
                f"Missing artifact: {blocker.description}",
                f"Why it is absent: {rationale}",
                f"What it enables: resolves {blocker.gap_type} constraint; "
                f"would unlock a primitive covering this initiative",
                f"Downstream unblock: {downstream_str}",
            ]

        lines.append("")
        if spec.composes_with:
            lines += [
                "Composability impact:",
                f"  Fixing this gap also benefits: {', '.join(spec.composes_with)}",
            ]
        else:
            lines.append("Composability impact: none — no composable peers identified.")

        lines += ["", "---", ""]

    return "\n".join(lines)


def _render_output_structure(spec: SpecDocument) -> str:
    """Fully deterministic ## Output data structure section."""
    os_ = spec.output_structure
    if os_ is None:
        return ""

    lines: List[str] = ["## Output data structure", ""]

    if os_.structure_type == "gap_brief":
        lines += [
            "**Type:** gap brief — no output structure available",
            "",
            "No warehouse assets are currently mapped to the required primitives. "
            "See Gap analysis for what is needed.",
        ]
        return "\n".join(lines)

    if os_.structure_type == "ai_agent":
        prim_names = [p.primitive_name for p in spec.available_primitives]
        lines += [
            "**Type:** AI agent / copilot",
            f"**Input context:** {', '.join(prim_names) if prim_names else '(none identified)'}",
            f"**Output:** natural language responses grounded in structured data "
            f"from `{os_.primary_source_asset}`",
            f"**Grain of underlying data:** {os_.grain_description}",
        ]
        if os_.history_note:
            lines += ["", f"*{os_.history_note}*"]
        return "\n".join(lines)

    _type_labels: Dict[str, str] = {
        "monitoring_dashboard": "monitoring dashboard",
        "decision_support":     "decision support",
        "analytics_product":    "analytics product",
    }
    type_label = _type_labels.get(os_.structure_type, os_.structure_type)

    lines += [
        f"**Type:** {type_label}",
        f"**Grain:** {os_.grain_description}",
        f"**Primary source:** `{os_.primary_source_asset}`",
    ]
    if os_.summary_source_asset:
        lines.append(f"**Summary source:** `{os_.summary_source_asset}`")
    lines.append("")

    # Dimensions
    if os_.dimensions:
        lines += [
            "### Dimensions",
            "| Column | Asset | Description |",
            "|--------|-------|-------------|",
        ]
        for col in os_.dimensions:
            desc = col.description or "—"
            lines.append(f"| {col.name} | {col.asset} | {desc} |")
        lines.append("")

    # Measures / Features
    if os_.feature_columns is not None:
        section_label = "Features"
        cols_list = os_.feature_columns
    elif os_.structure_type == "decision_support":
        section_label = "Features"
        cols_list = os_.measures
    else:
        section_label = "Measures"
        cols_list = os_.measures

    if cols_list:
        lines += [
            f"### {section_label}",
            "| Column | Asset | Concept | Description |",
            "|--------|-------|---------|-------------|",
        ]
        for col in cols_list:
            concept = col.semantic_candidate or "—"
            desc = col.description or "—"
            lines.append(f"| {col.name} | {col.asset} | {concept} | {desc} |")
        lines.append("")

    # Target variable
    if os_.target_variable is not None:
        lines += ["### Target variable", ""]
        if os_.target_variable.available:
            lines.append(
                f"**Available:** yes — `{os_.target_variable.column_name}` "
                f"on `{os_.target_variable.asset}`"
            )
            if os_.target_variable.description:
                lines.append(os_.target_variable.description)
        else:
            lines += [
                "**Available:** no",
                f"**Gap:** {os_.target_variable.gap_reason or '(not specified)'}",
            ]
        lines.append("")

    # Time columns
    if os_.time_columns:
        lines += [
            "### Time",
            "| Column | Asset | Role |",
            "|--------|-------|------|",
        ]
        for col in os_.time_columns:
            role_desc = col.description if col.description else col.role
            lines.append(f"| {col.name} | {col.asset} | {role_desc} |")
        lines.append("")
        if os_.pipeline_timestamp:
            lines += [f"*Pipeline watermark: `{os_.pipeline_timestamp}`*", ""]
    elif os_.pipeline_timestamp:
        lines += [
            "### Time",
            "",
            f"*No business time columns identified. Pipeline watermark: `{os_.pipeline_timestamp}`*",
            "",
        ]

    # History note
    if os_.history_note:
        lines += [f"*{os_.history_note}*", ""]

    return "\n".join(lines)


def _fmt_source_label(name: str, typ: str, grains: List[str]) -> str:
    """Format `asset_name` (type table, grain: keys) omitting type if unknown."""
    grain_str = ", ".join(grains) if grains else "unknown"
    if typ and typ not in ("unknown", ""):
        return f"`{name}` ({typ} table, grain: {grain_str})"
    return f"`{name}` (grain: {grain_str})"


def _render_data_requisites(spec: SpecDocument) -> str:
    """Fully deterministic ## Data requisites section for full_spec."""
    dr = spec.data_requisite
    if dr is None:
        return ""

    lines: List[str] = ["## Data requisites", ""]

    # Source / build header with type labels (IMPROVEMENT 1)
    lines.append(f"**Canonical output table:** `{dr.canonical_table_name}`")

    sat = dr.source_asset_types    # Dict[name, type]
    sag = dr.source_asset_grains   # Dict[name, List[str]]

    primary = dr.primary_source_asset or (dr.minimal_source_assets[0] if dr.minimal_source_assets else "")
    joined = [s for s in dr.minimal_source_assets if s != primary]

    if dr.build_complexity == "single_table":
        src_name = primary or "—"
        label = _fmt_source_label(src_name, sat.get(src_name, "unknown"), sag.get(src_name, []))
        lines.append(f"**Source:** {label} — no joins required")
    elif dr.build_complexity == "simple_join":
        join_key = next((c.join_key for c in dr.columns if c.join_key), None) or "shared key"
        label_a = _fmt_source_label(primary, sat.get(primary, "unknown"), sag.get(primary, []))
        b = joined[0] if joined else dr.minimal_source_assets[-1]
        label_b = _fmt_source_label(b, sat.get(b, "unknown"), sag.get(b, []))
        lines.append(f"**Primary source:** {label_a}")
        lines.append(f"**Joined source:** {label_b} — joined on `{join_key}`")
    else:
        lines.append(
            f"**Sources:** {len(dr.minimal_source_assets)} assets — see join paths section"
        )
        for src_name in ([primary] + joined):
            label = _fmt_source_label(src_name, sat.get(src_name, "unknown"), sag.get(src_name, []))
            lines.append(f"  - {label}")
    lines.append(f"**Build note:** {dr.build_notes}")
    lines.append("")

    def _desc(col) -> str:
        raw = col.description or "—"
        return (raw[:57] + "...") if len(raw) > 60 else raw

    # Group columns by role
    identifiers = [c for c in dr.columns if c.role == "identifier"]
    dimensions  = [c for c in dr.columns if c.role == "dimension"]
    measures    = [c for c in dr.columns if c.role == "measure"]
    time_cols   = [c for c in dr.columns if c.role == "time"]
    absent_cols = [c for c in dr.columns if c.derivation == "absent"]

    # ### Identifiers (no Derivation column — always direct_read from primary)
    if identifiers:
        lines += [
            "### Identifiers",
            "| Column | Type | Description |",
            "|--------|------|-------------|",
        ]
        for col in identifiers:
            lines.append(f"| {col.column_name} | {col.data_type} | {_desc(col)} |")
        lines.append("")

    # ### Dimensions (include Derivation column only if any are joins)
    if dimensions:
        has_join = any(c.derivation == "join" for c in dimensions)
        if has_join:
            lines += [
                "### Dimensions",
                "| Column | Type | Source | Derivation | Description |",
                "|--------|------|--------|------------|-------------|",
            ]
            for col in dimensions:
                src = col.source_asset or "—"
                lines.append(
                    f"| {col.column_name} | {col.data_type} | {src} | {col.derivation} | {_desc(col)} |"
                )
        else:
            lines += [
                "### Dimensions",
                "| Column | Type | Description |",
                "|--------|------|-------------|",
            ]
            for col in dimensions:
                lines.append(f"| {col.column_name} | {col.data_type} | {_desc(col)} |")
        lines.append("")

    # ### Join assessment (IMPROVEMENT 3) — only if joins exist
    if dr.join_assessments:
        lines += [
            "### Join assessment",
            "",
            "| Join | Type | Grain match | Safety |",
            "|------|------|-------------|--------|",
        ]
        for ja in dr.join_assessments:
            type_str = f"{ja.left_type} → {ja.right_type}"
            if ja.grain_match:
                grain_cell = f"✓ {ja.join_key}" if ja.join_key else "✓"
            else:
                grain_cell = "✗ mismatch"
            safety_icon = "✅ safe" if ja.join_safety == "safe" else "⚠ risky"
            lines.append(
                f"| `{ja.left_asset}` → `{ja.right_asset}` "
                f"| {type_str} | {grain_cell} | {safety_icon} |"
            )
        lines.append("")

        # Safety notes
        for ja in dr.join_assessments:
            lines.append(f"- `{ja.left_asset}` → `{ja.right_asset}`: {ja.safety_note}")
        lines.append("")

        # Warning block for any risky joins
        risky = [ja for ja in dr.join_assessments if ja.join_safety != "safe"]
        if risky:
            lines.append("⚠ Review join strategy before building ETL:")
            for ja in risky:
                lines.append(f"  - `{ja.left_asset}` → `{ja.right_asset}`: {ja.safety_note}")
            lines.append("")

    # ### Measures (no Derivation column)
    if measures:
        lines += [
            "### Measures",
            "| Column | Type | Description |",
            "|--------|------|-------------|",
        ]
        for col in measures:
            lines.append(f"| {col.column_name} | {col.data_type} | {_desc(col)} |")
        lines.append("")

    # ### Time (no Derivation column)
    if time_cols:
        lines += [
            "### Time",
            "| Column | Type | Description |",
            "|--------|------|-------------|",
        ]
        for col in time_cols:
            lines.append(f"| {col.column_name} | {col.data_type} | {_desc(col)} |")
        lines.append("")

    # Absent-column warning
    if absent_cols:
        lines.append(
            "⚠ The following columns are not available in the current warehouse "
            "and must be sourced or derived:"
        )
        for col in absent_cols:
            col_desc = col.description or "(no description)"
            lines.append(f"- `{col.column_name}`: {col_desc}")
        lines.append("")

    return "\n".join(lines)


def _render_bill_of_materials(spec: SpecDocument) -> str:
    """Fully deterministic ## Data assets — bill of materials section."""
    lines: List[str] = ["## Data assets — bill of materials", ""]

    # Build name → AssetDetail so we can look up grain_keys
    asset_grain: Dict[str, List[str]] = {}
    for prim in spec.available_primitives:
        for asset in prim.supporting_assets:
            if asset.name not in asset_grain:
                asset_grain[asset.name] = list(asset.grain_keys)

    # Reverse primitive_to_assets: name → [primitive_id, ...]
    name_to_prims: Dict[str, List[str]] = {}
    for prim_id, asset_names in spec.primitive_to_assets.items():
        for name in asset_names:
            name_to_prims.setdefault(name, []).append(prim_id)

    for i, asset_name in enumerate(spec.all_supporting_asset_names, 1):
        grain_keys = asset_grain.get(asset_name, [])
        grain_str = ", ".join(grain_keys) if grain_keys else "(unknown)"
        prim_ids = sorted(name_to_prims.get(asset_name, []))
        prim_str = ", ".join(prim_ids) if prim_ids else "(none)"
        lines.append(f"{i}. `{asset_name}`")
        lines.append(f"   primitives: {prim_str}")
        lines.append(f"   grain: {grain_str}")
        lines.append("")

    return "\n".join(lines)


def _infer_join_purpose(left: str, right: str) -> str:
    """Heuristic join purpose from asset name patterns."""
    def tags(name: str) -> set:
        t: set = set()
        n = name.lower()
        if n.startswith("hx_") or "_hx_" in n:
            t.add("historical")
        if n.startswith("ll_") or "_ll_" in n:
            t.add("liberty")
        if "_coverage_" in n:
            t.add("coverage")
        if "_policy_" in n:
            t.add("policy")
        if "_total_" in n:
            t.add("rollup")
        return t

    lt, rt = tags(left), tags(right)
    both = lt | rt

    if "rollup" in both:
        return "Reconcile Liberty-share rollup against total-share rollup"
    if "historical" in both and "coverage" in both:
        return "Historical coverage to current-cycle coverage"
    if "historical" in lt and "coverage" in rt:
        return "Join current policy pricing to historical coverage terms"
    if "historical" in rt and "coverage" in lt:
        return "Historical coverage to Liberty-specific coverage detail"
    if "historical" in both:
        return "Attach historical quote versions to current-cycle policy"
    if "liberty" in lt and "coverage" in rt:
        return "Join Liberty-share policy metrics to coverage/exposure structure"
    if "liberty" in rt and "coverage" in lt:
        return "Base coverage to Liberty coverage reconciliation"
    if "liberty" in both:
        return "Align Liberty-share policy metrics with base policy record"
    if "coverage" in both:
        return "Base policy to base coverage join"
    if "coverage" in both and "policy" in both:
        return "Join policy pricing to coverage/exposure structure"
    return "Join on shared grain keys"


def _render_join_paths(spec: SpecDocument) -> str:
    """Fully deterministic ## Join paths section."""
    lines: List[str] = ["## Join paths", ""]

    if not spec.grain_join_paths:
        lines.append("No join paths identified.")
        return "\n".join(lines)

    # Primary key = the most common shared-key combination
    key_sets = [tuple(jp.shared_grain_keys) for jp in spec.grain_join_paths]
    most_common = Counter(key_sets).most_common(1)[0][0] if key_sets else ()
    if most_common:
        key_tick = " + ".join(f"`{k}`" for k in most_common)
        lines.append(
            f"The primary join key across policy-detail and coverage-detail assets is "
            f"the composite key {key_tick}; coverage-level assets additionally require "
            f"`coverage_id` when multiple coverages exist per layer."
        )
    lines.append("")
    lines.append("| Left asset | Right asset | Keys | Purpose |")
    lines.append("|---|---|---|---|")

    for jp in spec.grain_join_paths:
        keys_str = ", ".join(f"`{k}`" for k in jp.shared_grain_keys)
        purpose = _infer_join_purpose(jp.left_asset, jp.right_asset)
        lines.append(f"| `{jp.left_asset}` | `{jp.right_asset}` | {keys_str} | {purpose} |")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are a technical writer reviewing the output of an automated data-warehouse \
analysis for an insurance underwriting organisation.

You will receive a JSON object with these keys:
  "spec"                      — a SpecDocument containing all initiative and primitive data
  "prerendered_section"       — pre-formatted markdown (readiness evidence or gap chain)
  "bom_section"               — pre-formatted ## Data assets bill of materials (full_spec only)
  "output_structure_section"  — pre-formatted ## Output data structure block (full_spec only)
  "data_requisites_section"   — pre-formatted ## Data requisites block (full_spec only)
  "join_paths_section"        — pre-formatted ## Join paths table (full_spec only)

OUTPUT RULES
- Write in plain markdown. No preamble. No sign-off.
- Start with a level-2 heading (## <initiative_name>).
- Output sections in the exact order shown below for the spec_type.
- Where the order shows a sentinel token \
({{PRERENDERED}}, {{BOM}}, {{OUTPUT_STRUCTURE}}, {{JOIN_PATHS}}), \
output that token EXACTLY on its own line and nothing else on that line. \
Python will replace it with the pre-rendered content.
- All other sections: generate from spec data. Be specific. No marketing language.

════════════════════════════════════════════════════
FULL SPEC  (spec_type = "full_spec")
════════════════════════════════════════════════════

## <initiative_name>

## Overview
1–2 sentences. What decision does this product enable and for whom.

{{BOM}}

## Asset schematic
Plain-text ASCII box diagram. Asset names in boxes. Join keys on connecting lines.
Only assets in all_supporting_asset_names. ~20 lines max.
If > 6 assets, group by primitive and show intra-group joins only.
Permitted characters: ┌─┐└┘│├┤┬┴┼→▶

{{OUTPUT_STRUCTURE}}
After the {{OUTPUT_STRUCTURE}} block, write one sentence explaining what a business \
user would do with this output \
(e.g. "Use this table to filter renewals by RARC threshold and identify where \
rate adequacy is eroding."). Do not add any other content to this section.

{{DATA_REQUISITES}}
Do not add any content to the ## Data requisites section — it is pre-rendered. \
Output {{DATA_REQUISITES}} exactly as shown on its own line.

{{PRERENDERED}}

## Business objective
One paragraph. What business problem this solves, referencing business_objective \
and output_type.

## Key measures
Table of the 8–10 most analytically significant columns across all assets.
Columns: Measure | Asset | Definition | Role
Prioritise columns with non-null descriptions that are measures or \
numeric_attributes. Skip identifiers and timestamps.

{{JOIN_PATHS}}

## Delivery guidance
Refresh cadence, SLA, output format (table/dashboard/API), currency/FX notes, \
open risks from the readiness evidence.

## Composability
For each initiative in composes_with, one line noting the shared primitive(s).

════════════════════════════════════════════════════
GAP BRIEF  (spec_type = "gap_brief")
════════════════════════════════════════════════════

## <initiative_name>

## Overview
1–2 sentences. What this initiative would deliver if unblocked.

{{PRERENDERED}}

## Data assets
Short list of any available_primitives and their supporting assets.
Omit this section entirely if available_primitives is empty.

## Business objective
One sentence.

## Gap analysis
What data is missing and why that blocks the initiative. Concrete, not generic.

## Enablement path
3–5 numbered concrete action items to unblock this initiative.\
"""


# ---------------------------------------------------------------------------
# User message builder
# ---------------------------------------------------------------------------

def _user_message(spec: SpecDocument) -> str:
    """Serialise spec + all pre-rendered sections as the user message content."""
    prerendered = (
        _render_readiness_evidence(spec)
        if spec.spec_type == "full_spec"
        else _render_gap_chain(spec)
    )
    is_full = spec.spec_type == "full_spec"
    bom = _render_bill_of_materials(spec) if is_full else ""
    output_structure = _render_output_structure(spec) if is_full else ""
    data_requisites = _render_data_requisites(spec) if is_full else ""
    join_paths = _render_join_paths(spec) if is_full else ""

    payload = {
        "spec": spec.model_dump(mode="json"),
        "prerendered_section": prerendered,
        "bom_section": bom,
        "output_structure_section": output_structure,
        "data_requisites_section": data_requisites,
        "join_paths_section": join_paths,
    }
    return json.dumps(payload, indent=2, sort_keys=True)


# ---------------------------------------------------------------------------
# Renderer
# ---------------------------------------------------------------------------

class SpecRenderer:
    """Render a SpecDocument to markdown via a single LLM call."""

    def render(self, spec: SpecDocument) -> Tuple[str, Optional[str]]:
        """Return (rendered_markdown, error_message).

        rendered_markdown is "" on failure.
        error_message is None on success.

        The returned markdown has all sentinel tokens replaced with the
        deterministically pre-rendered structured sections.
        """
        try:
            import anthropic
        except ImportError:
            return "", "anthropic package not installed"

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return "", "ANTHROPIC_API_KEY not set"

        max_tokens = (
            _MAX_TOKENS_FULL if spec.spec_type == "full_spec" else _MAX_TOKENS_BRIEF
        )

        # Pre-render all structured sections for post-substitution
        is_full = spec.spec_type == "full_spec"
        prerendered = (
            _render_readiness_evidence(spec) if is_full else _render_gap_chain(spec)
        )
        bom = _render_bill_of_materials(spec) if is_full else ""
        output_structure = _render_output_structure(spec) if is_full else ""
        data_requisites = _render_data_requisites(spec) if is_full else ""
        join_paths = _render_join_paths(spec) if is_full else ""

        try:
            client = anthropic.Anthropic(api_key=api_key)
            message = client.messages.create(
                model=_MODEL,
                max_tokens=max_tokens,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": _user_message(spec)}],
            )
            llm_text = message.content[0].text if message.content else ""
            # Substitute all sentinels with pre-rendered content
            rendered = llm_text.replace(_SENTINEL, prerendered)
            rendered = rendered.replace(_SENTINEL_BOM, bom)
            rendered = rendered.replace(_SENTINEL_OUTPUT, output_structure)
            rendered = rendered.replace(_SENTINEL_DATA_REQ, data_requisites)
            rendered = rendered.replace(_SENTINEL_JOIN, join_paths)
            return rendered, None
        except Exception as exc:  # noqa: BLE001
            return "", str(exc)
