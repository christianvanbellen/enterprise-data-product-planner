"""SpecRenderer — bounded LLM call that converts a SpecDocument to markdown.

Sections are structured around the 5W2H framework:
  What / Why / Who / Where — LLM prose, grounded in structured spec data
  When                     — pre-rendered deterministically ({{WHEN}})
  How                      — pre-rendered deterministically ({{HOW}}, full_spec only)

The LLM writes the four prose sections only. When and How are computed in Python
and injected verbatim, guaranteeing that readiness evidence, column tables, join
safety classifications, and build contracts are exact across all renders.

Model: claude-sonnet-4-6 (pinned).
Token budgets: 1 200 full_spec · 600 gap_brief.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env", override=False)
except ImportError:
    pass

from graph.spec.assembler import SpecDocument

_MODEL = "claude-sonnet-4-6"
_MAX_TOKENS_FULL  = 1600
_MAX_TOKENS_BRIEF = 600

_DELIVERY_YAML_PATH = (
    Path(__file__).resolve().parent.parent.parent / "ontology" / "delivery_heuristics.yaml"
)


def _load_delivery_heuristics() -> Dict[str, tuple]:
    """Load per-archetype delivery defaults from ontology/delivery_heuristics.yaml."""
    raw = yaml.safe_load(_DELIVERY_YAML_PATH.read_text(encoding="utf-8"))
    return {
        archetype: (profile["refresh"], profile["sla"], profile["format"])
        for archetype, profile in raw.get("delivery", {}).items()
    }


# Loaded from ontology/delivery_heuristics.yaml — edit that file to change delivery defaults.
_DELIVERY: Dict[str, tuple] = _load_delivery_heuristics()

_SENTINEL_WHEN = "{{WHEN}}"
_SENTINEL_HOW  = "{{HOW}}"

# ---------------------------------------------------------------------------
# Pre-rendered section builders (Python only, no LLM)
# ---------------------------------------------------------------------------

_READINESS_LABELS = {
    "ready_now":               "Ready now",
    "ready_with_enablement":   "Ready with enablement",
    "needs_foundational_work": "Needs foundational work",
    "not_currently_feasible":  "Not currently feasible",
}


def _render_when(spec: SpecDocument) -> str:
    """Pre-rendered ## When section — readiness, primitives, data quality, delivery."""
    lines: List[str] = ["## When", ""]
    readiness_label = _READINESS_LABELS.get(spec.readiness, spec.readiness)

    if spec.spec_type == "full_spec":
        dr = spec.data_requisite
        complexity = dr.build_complexity if dr else "—"
        lines.append(
            f"**Readiness:** {readiness_label}  ·  "
            f"**Complexity:** {complexity}  ·  "
            f"**Score:** {spec.composite_score:.3f}"
        )
        lines.append("")

        # Primitive coverage
        if spec.available_primitives:
            lines += [
                "| Primitive | Maturity |",
                "|-----------|----------|",
            ]
            for p in spec.available_primitives:
                flag = " ⚠ partial" if p.maturity_score < 0.5 else ""
                lines.append(f"| {p.primitive_name} | {p.maturity_score:.2f}{flag} |")
            lines.append("")

        if spec.blockers:
            lines.append("**Blockers:**")
            for b in spec.blockers:
                lines.append(f"- `{b.gap_type}`: {b.description}")
            lines.append("")

        # Data quality
        if spec.data_quality_signals:
            lines += ["### Data quality", ""]
            lines += [
                "| Asset | Columns | Tests | Coverage | Descriptions |",
                "|-------|---------|-------|----------|--------------|",
            ]
            low_coverage: List[str] = []
            for s in spec.data_quality_signals:
                flag = " ⚠" if s.test_coverage_pct < 30 else ""
                lines.append(
                    f"| `{s.asset_name}` | {s.column_count} | {s.tested_count} "
                    f"| {s.test_coverage_pct}%{flag} | {s.described_count}/{s.column_count} |"
                )
                if s.test_coverage_pct < 30:
                    low_coverage.append(s.asset_name)
            lines.append("")
            if low_coverage:
                lines.append(
                    f"⚠ Test coverage below 30% on {', '.join(f'`{a}`' for a in low_coverage)} "
                    f"— data quality assertions cannot be made with confidence. "
                    f"Validate pipeline before production use."
                )
                lines.append("")

        # Undescribed columns risk
        if dr:
            undescribed = [
                c.column_name for c in dr.columns
                if c.role != "identifier" and not c.description
            ]
            if undescribed:
                sample = undescribed[:5]
                etc = f" + {len(undescribed) - 5} more" if len(undescribed) > 5 else ""
                lines.append(
                    f"⚠ {len(undescribed)} output column(s) have no dbt description — "
                    f"analytical intent inferred from name only: "
                    f"{', '.join(f'`{c}`' for c in sample)}{etc}."
                )
                lines.append("")

        # Delivery heuristics
        cadence, sla, fmt = _DELIVERY.get(
            spec.archetype, ("As required", "Depends on upstream refresh", "—")
        )
        lines += [
            "### Delivery",
            "",
            f"**Refresh:** {cadence}  ·  **SLA:** {sla}  ·  **Format:** {fmt}",
            "",
        ]

    else:  # gap_brief
        lines.append(f"**Readiness:** {readiness_label}")
        lines.append("")

        if spec.available_primitives:
            lines += [
                "**Available primitives:**",
                "",
                "| Primitive | Maturity |",
                "|-----------|----------|",
            ]
            for p in spec.available_primitives:
                lines.append(f"| {p.primitive_name} | {p.maturity_score:.2f} |")
            lines.append("")

        if spec.blockers:
            lines += [
                "**Blockers:**",
                "",
                "| Gap type | Description |",
                "|----------|-------------|",
            ]
            for b in spec.blockers:
                lines.append(f"| `{b.gap_type}` | {b.description} |")
            lines.append("")

        if spec.feasibility_rationale:
            lines += [f"*{spec.feasibility_rationale}*", ""]

    return "\n".join(lines)


def _render_how(spec: SpecDocument) -> str:
    """Pre-rendered ## How section — sources, columns, join safety."""
    dr = spec.data_requisite
    if dr is None:
        return ""

    lines: List[str] = []

    lines.append(f"**Output table:** `{dr.canonical_table_name}`")
    lines.append(f"**Grain:** {dr.grain_description}")
    lines.append(f"**Build:** {dr.build_complexity} — {dr.build_notes}")
    lines.append("")

    # Sources
    sat = dr.source_asset_types
    sag = dr.source_asset_grains
    primary = dr.primary_source_asset or ""

    lines += [
        "### Sources",
        "",
        "| Asset | Role | Type | Grain |",
        "|-------|------|------|-------|",
    ]
    # Primary first, then joined assets in original order
    ordered_sources = [primary] + [s for s in dr.minimal_source_assets if s != primary]
    for src in ordered_sources:
        role = "PRIMARY" if src == primary else "JOINED"
        typ = sat.get(src, "unknown")
        grain_str = ", ".join(sag.get(src, [])) or "—"
        lines.append(f"| `{src}` | {role} | {typ} | {grain_str} |")
    lines.append("")

    # Columns — grouped by role
    role_order = ["identifier", "dimension", "feature", "measure", "target", "time"]
    cols_by_role: dict = {r: [] for r in role_order}
    for col in dr.columns:
        cols_by_role.setdefault(col.role, []).append(col)

    lines += [
        "### Columns",
        "",
        "| Column | Role | Source | Description |",
        "|--------|------|--------|-------------|",
    ]
    for role in role_order:
        for col in cols_by_role.get(role, []):
            src_str = col.source_asset or "—"
            if col.derivation == "join" and col.join_key:
                src_str = f"{src_str} (join: {col.join_key})"
            raw_desc = col.description or "—"
            desc = (raw_desc[:55] + "...") if len(raw_desc) > 58 else raw_desc
            lines.append(f"| `{col.column_name}` | {col.role} | {src_str} | {desc} |")
    lines.append("")

    # Join safety
    if dr.join_assessments:
        lines += [
            "### Join safety",
            "",
            "| Join | Direction | Key | Safety |",
            "|------|-----------|-----|--------|",
        ]
        for ja in dr.join_assessments:
            direction = f"{ja.left_type} → {ja.right_type}"
            safety = "✅ safe" if ja.join_safety == "safe" else "⚠ risky"
            lines.append(
                f"| `{ja.left_asset}` → `{ja.right_asset}` "
                f"| {direction} | `{ja.join_key}` | {safety} |"
            )
        lines.append("")
        for ja in dr.join_assessments:
            lines.append(f"*{ja.left_asset} → {ja.right_asset}: {ja.safety_note}*")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are a technical writer producing data product specifications for a specialty \
insurance firm. Write for a senior data engineer or data architect audience.

You will receive a JSON object with:
  "spec"         — SpecDocument (initiative metadata, primitives, blockers)
  "when_section" — pre-rendered ## When block
  "how_section"  — pre-rendered ## How block (full_spec only, empty for gap_brief)

OUTPUT RULES
- Plain markdown. No preamble. No sign-off.
- Start with a level-2 heading: ## {initiative_name}
- Output sections in exactly the order shown for the spec_type.
- Where you see {{WHEN}} or {{HOW}}: output that token exactly on its own line. \
Python replaces it with pre-rendered content. Do not write those sections yourself.
- Use spec fields directly: initiative_name, archetype, readiness, output_type, \
business_objective, target_users, composes_with, literature_quote, \
literature_source_ids, available_primitives, blockers, feasibility_rationale.
- Be specific. No marketing language. No invented numbers or metrics.
- literature_quote: if non-null, reference it in ## What. Attribute to the \
organisation from literature_source_ids (e.g. mckinsey_pnc_underwriting_2021 \
→ "McKinsey, 2021"). If null, skip the citation.

════════════════════════════════════════════════════
FULL SPEC  (spec_type = "full_spec")
════════════════════════════════════════════════════

## {initiative_name}
*{archetype} · {readiness} · {output_type}*

## What
2–3 sentences. What decision or operational capability this product enables, \
and for whom. If literature_quote is present, open with it or close with it: \
e.g. "Industry research notes that [quote] (McKinsey, 2021) — this initiative \
operationalises that capability by...". Reference business_objective.

## Why
2–3 sentences. Why this matters for a specialty insurer. \
Reference composes_with peers where relevant (note what shared capability they \
unlock together). Do not state the composite_score number.

## Who
Three labelled bullets. Keep each to one sentence.
- **Builders:** [data engineering skill set implied by archetype and output_type]
- **Users:** [from target_users; what they do with this product specifically]
- **Maintainers:** [who owns refresh cadence and data quality for this output]

## Where
2 sentences. Where this product sits in the underwriting/pricing/claims workflow. \
What infrastructure it runs on (infer from output_type: monitoring_dashboard → \
BI/Streamlit, decision_support → internal API or embedded tool, ai_agent → \
LLM-backed agent layer).

{{WHEN}}

## How
Write 2 sections — no extra headings, no bullet points, plain prose:

Paragraph 1 — Product form (1–2 sentences):
  State exactly what this product is: a mart / dashboard / copilot / ML model / API.
  Name the output_type and archetype concretely (e.g. "a daily-refreshed Streamlit
  monitoring dashboard", "an on-demand decision-support mart queried at bind time",
  "a batch-scored ML model producing a severity estimate per claim").

Paragraph 2 — Information gain (3–5 sentences):
  Read through the how_section in the JSON payload: inspect the source assets, their
  roles (PRIMARY / JOINED), their types (fact / dimension / snapshot / bridge), and the
  specific column names and roles (identifier / dimension / measure / time / feature /
  target). Explain what analytical question each asset or column group enables.
  Crucially: explain what the *combination* makes possible that neither asset alone
  could answer. Reference specific column names. Be concrete — name the question
  ("which underwriters are writing below technical price?"), not just the capability
  ("enables pricing analysis"). Avoid generic phrases like "data-driven decisions".

{{HOW}}

════════════════════════════════════════════════════
GAP BRIEF  (spec_type = "gap_brief")
════════════════════════════════════════════════════

## {initiative_name}
*{archetype} · {readiness}*

## What
2 sentences. What this initiative would produce if blockers were resolved. \
If literature_quote is present, reference it briefly.

## Why
1–2 sentences. Business case — why the gap is worth resolving.

{{WHEN}}\
"""


# ---------------------------------------------------------------------------
# User message builder
# ---------------------------------------------------------------------------

def _user_message(spec: SpecDocument) -> str:
    """Serialise spec + pre-rendered sections as the user message content."""
    payload = {
        "spec": spec.model_dump(mode="json"),
        "when_section": _render_when(spec),
        "how_section": _render_how(spec) if spec.spec_type == "full_spec" else "",
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
        Sentinel tokens in LLM output are replaced with pre-rendered content.
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

        when_block = _render_when(spec)
        how_block  = _render_how(spec) if spec.spec_type == "full_spec" else ""

        try:
            client = anthropic.Anthropic(api_key=api_key)
            message = client.messages.create(
                model=_MODEL,
                max_tokens=max_tokens,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": _user_message(spec)}],
            )
            llm_text = message.content[0].text if message.content else ""
            rendered = llm_text.replace(_SENTINEL_WHEN, when_block)
            rendered = rendered.replace(_SENTINEL_HOW, how_block)
            return rendered, None
        except Exception as exc:  # noqa: BLE001
            return "", str(exc)
