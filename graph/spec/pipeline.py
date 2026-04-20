"""SpecGenerationPipeline — orchestrates assembler, renderer, and log.

Usage:
    pipeline = SpecGenerationPipeline()
    report = pipeline.run(
        initiative_ids=["underwriting_decision_support"],
        graph_store=store,
        bundle=bundle,
        primitives=primitives,
        opportunity_results=opps,
        log_dir=Path("output/spec_log"),
        render=True,
        force_render=False,
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from graph.opportunity.planner import OpportunityResult
from graph.opportunity.primitive_extractor import CapabilityPrimitive
from graph.spec.assembler import SpecAssembler, SpecDocument
from graph.spec.log import SpecLog, SpecLogEntry
from graph.spec.renderer import SpecRenderer


@dataclass
class SpecGenerationReport:
    total: int
    assembled: int
    rendered: int
    cached: int
    errors: int
    entries: List[SpecLogEntry] = field(default_factory=list)
    # Map initiative_id → rendered markdown (only populated entries)
    rendered_specs: Dict[str, str] = field(default_factory=dict)


class SpecGenerationPipeline:
    """Orchestrate spec assembly, rendering, and logging for a set of initiatives."""

    def run(
        self,
        initiative_ids: List[str],
        graph_store: Any,
        bundle: Any,
        primitives: List[CapabilityPrimitive],
        opportunity_results: List[OpportunityResult],
        log_dir: Path,
        render: bool = True,
        force_render: bool = False,
        archetype_lib: Any = None,   # InitiativeArchetypeLibrary, optional
    ) -> SpecGenerationReport:

        opp_by_id: Dict[str, OpportunityResult] = {
            o.initiative_id: o for o in opportunity_results
        }

        # Determine graph_build_id from any opportunity node in the graph store
        graph_build_id = _infer_build_id(graph_store)

        assembler = SpecAssembler()
        renderer = SpecRenderer()
        log = SpecLog(log_dir)

        report = SpecGenerationReport(
            total=len(initiative_ids),
            assembled=0,
            rendered=0,
            cached=0,
            errors=0,
        )

        for init_id in initiative_ids:
            opp = opp_by_id.get(init_id)
            if not opp:
                report.errors += 1
                continue

            # Assemble
            archetype_def = (
                archetype_lib.get_archetype(init_id)
                if archetype_lib else None
            )
            try:
                spec = assembler.assemble(
                    opp=opp,
                    primitives=primitives,
                    bundle=bundle,
                    graph_store=graph_store,
                    graph_build_id=graph_build_id,
                    archetype_def=archetype_def,
                )
                report.assembled += 1
            except Exception as exc:  # noqa: BLE001
                report.errors += 1
                continue

            # Check cache — only skip rendering if the cached entry was successfully rendered
            if render and not force_render and log.has_spec(spec.spec_id):
                entry = log.get_latest(init_id)
                if entry and entry.rendered:
                    report.cached += 1
                    report.entries.append(entry)
                    _, cached_md = log.load(spec.spec_id)
                    report.rendered_specs[init_id] = cached_md
                    continue

            # Render
            rendered_md = ""
            render_error: Optional[str] = None
            if render:
                rendered_md, render_error = renderer.render(spec)
                if render_error:
                    report.errors += 1
                else:
                    report.rendered += 1

            # Save
            spec_id = log.save(spec, rendered_md, render_error=render_error)
            entry = log.get_latest(init_id)
            if entry:
                report.entries.append(entry)
            if rendered_md:
                report.rendered_specs[init_id] = rendered_md

        return report


def _infer_build_id(graph_store: Any) -> str:
    """Extract the opportunity build_id from the graph store.

    Looks for the first InitiativeNode and returns its build_id.
    Falls back to "unknown" if none found.
    """
    for node in graph_store._nodes.values():  # type: ignore[union-attr]
        if node.get("label") == "InitiativeNode":
            return node.get("build_id", "unknown")
    return "unknown"
