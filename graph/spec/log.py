"""SpecLog — persistent, versioned spec storage.

Directory structure per initiative:
  {log_dir}/{initiative_id}/
    current.json             — copy of latest SpecDocument JSON
    current.md               — copy of latest rendered markdown
    v{N}_{YYYY-MM-DD}_{hash8}.json   — versioned history
    v{N}_{YYYY-MM-DD}_{hash8}.md

A root index.json maintains one entry per initiative (deduplicated by
initiative_id — no more growth on repeated runs with the same graph build).

spec_id = stable_hash(initiative_id, graph_build_id) — deterministic 16 hex chars.
Re-running Phase 5 with the same graph build_id overwrites the same files and
updates the index entry in place.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from graph.spec.assembler import SpecDocument

_INDEX_FILE = "index.json"


@dataclass
class SpecLogEntry:
    spec_id: str
    initiative_id: str
    spec_type: str
    readiness: str
    graph_build_id: str
    assembled_at_utc: str
    rendered: bool
    render_error: Optional[str] = None
    # Extended fields (April 2026)
    initiative_name: str = ""
    composite_score: float = 0.0
    current_version: int = 1
    rendered_at_utc: str = ""
    path: str = ""


class SpecLog:
    """Read and write specs from/to a log directory."""

    def __init__(self, log_dir: Path) -> None:
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    # ── Write ──────────────────────────────────────────────────────────────

    def save(
        self,
        spec: SpecDocument,
        rendered: str,
        render_error: Optional[str] = None,
    ) -> str:
        """Persist spec + rendered markdown.  Returns spec_id."""
        # 1. Create initiative directory
        initiative_dir = self.log_dir / spec.initiative_id
        initiative_dir.mkdir(parents=True, exist_ok=True)

        # 2. Determine version number
        existing_versions = list(initiative_dir.glob("v*.json"))
        new_version = len(existing_versions) + 1

        # 3. Write versioned files
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        hash8 = spec.spec_id[:8]
        versioned_stem = f"v{new_version}_{date_str}_{hash8}"

        (initiative_dir / f"{versioned_stem}.json").write_text(
            spec.model_dump_json(indent=2), encoding="utf-8"
        )
        (initiative_dir / f"{versioned_stem}.md").write_text(
            rendered, encoding="utf-8"
        )

        # 4. Write/overwrite current files
        (initiative_dir / "current.json").write_text(
            spec.model_dump_json(indent=2), encoding="utf-8"
        )
        (initiative_dir / "current.md").write_text(
            rendered, encoding="utf-8"
        )

        # 5. Update index (deduplicated by initiative_id)
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        entry = SpecLogEntry(
            spec_id=spec.spec_id,
            initiative_id=spec.initiative_id,
            initiative_name=spec.initiative_name,
            spec_type=spec.spec_type,
            readiness=spec.readiness,
            composite_score=spec.composite_score,
            graph_build_id=spec.graph_build_id,
            assembled_at_utc=spec.assembled_at_utc,
            rendered=bool(rendered),
            render_error=render_error,
            current_version=new_version,
            rendered_at_utc=now_utc,
            path=f"spec_log/{spec.initiative_id}/current.json",
        )
        self._upsert_index(entry)

        return spec.spec_id

    # ── Read ───────────────────────────────────────────────────────────────

    def load(self, spec_id: str) -> Tuple[SpecDocument, str]:
        """Return (SpecDocument, rendered_markdown).  Raises FileNotFoundError.

        Reads from {initiative_id}/current.json if the spec_id matches,
        otherwise falls back to the versioned file with matching hash8.
        """
        entries = self._load_index()
        entry = next((e for e in entries if e.spec_id == spec_id), None)

        if entry is None:
            raise FileNotFoundError(f"Spec not found: {spec_id}")

        initiative_dir = self.log_dir / entry.initiative_id

        # Try current.json first
        current_json = initiative_dir / "current.json"
        if current_json.exists():
            spec = SpecDocument.model_validate_json(
                current_json.read_text(encoding="utf-8")
            )
            if spec.spec_id == spec_id:
                current_md = initiative_dir / "current.md"
                rendered = (
                    current_md.read_text(encoding="utf-8")
                    if current_md.exists()
                    else ""
                )
                return spec, rendered

        # Fall back to versioned file matching hash8
        hash8 = spec_id[:8]
        for versioned in sorted(initiative_dir.glob(f"v*_{hash8}.json")):
            spec = SpecDocument.model_validate_json(
                versioned.read_text(encoding="utf-8")
            )
            md_path = versioned.with_suffix(".md")
            rendered = (
                md_path.read_text(encoding="utf-8") if md_path.exists() else ""
            )
            return spec, rendered

        raise FileNotFoundError(f"Spec not found: {spec_id}")

    def list_specs(
        self, initiative_id: Optional[str] = None
    ) -> List[SpecLogEntry]:
        """Return all log entries, optionally filtered by initiative_id."""
        entries = self._load_index()
        if initiative_id:
            entries = [e for e in entries if e.initiative_id == initiative_id]
        return sorted(entries, key=lambda e: e.assembled_at_utc)

    def get_latest(self, initiative_id: str) -> Optional[SpecLogEntry]:
        """Return the most recently assembled entry for an initiative."""
        entries = self.list_specs(initiative_id=initiative_id)
        return entries[-1] if entries else None

    def has_spec(self, spec_id: str) -> bool:
        """Return True if a spec with this spec_id exists in the log."""
        entries = self._load_index()
        entry = next((e for e in entries if e.spec_id == spec_id), None)
        if entry is None:
            return False
        return (self.log_dir / entry.initiative_id / "current.json").exists()

    # ── Index helpers ──────────────────────────────────────────────────────

    def _load_index(self) -> List[SpecLogEntry]:
        index_path = self.log_dir / _INDEX_FILE
        if not index_path.exists():
            return []
        raw = json.loads(index_path.read_text(encoding="utf-8"))
        entries = []
        for r in raw:
            # Tolerate old-format entries that lack the extended fields
            r.setdefault("initiative_name", "")
            r.setdefault("composite_score", 0.0)
            r.setdefault("current_version", 1)
            r.setdefault("rendered_at_utc", r.get("assembled_at_utc", ""))
            r.setdefault("path", f"spec_log/{r.get('initiative_id', '')}/current.json")
            entries.append(SpecLogEntry(**r))
        return entries

    def _upsert_index(self, entry: SpecLogEntry) -> None:
        entries = self._load_index()
        # Deduplicate by initiative_id — replace any existing entry for this initiative
        entries = [e for e in entries if e.initiative_id != entry.initiative_id]
        entries.append(entry)
        entries.sort(key=lambda e: e.initiative_id)
        index_path = self.log_dir / _INDEX_FILE
        index_path.write_text(
            json.dumps([asdict(e) for e in entries], indent=2, sort_keys=True),
            encoding="utf-8",
        )

    # ── Migration ──────────────────────────────────────────────────────────

    @classmethod
    def migrate_flat_to_versioned(cls, log_dir: Path) -> None:
        """Migrate a flat spec_log to the initiative-named folder structure.

        Reads the existing flat index.json, deduplicates by initiative_id
        (keeping the latest assembled_at_utc per initiative), writes each spec
        to the new structure, removes old flat files, and rewrites index.json.
        """
        log_dir = Path(log_dir)
        index_path = log_dir / _INDEX_FILE

        if not index_path.exists():
            return

        raw: List[dict] = json.loads(index_path.read_text(encoding="utf-8"))

        # Deduplicate by initiative_id, keeping latest assembled_at_utc
        by_initiative: Dict[str, dict] = {}
        for entry_dict in raw:
            iid = entry_dict.get("initiative_id", "")
            existing = by_initiative.get(iid)
            if existing is None or (
                entry_dict.get("assembled_at_utc", "")
                > existing.get("assembled_at_utc", "")
            ):
                by_initiative[iid] = entry_dict

        # Wipe the current index — migration will rebuild it
        index_path.unlink(missing_ok=True)

        new_log = cls(log_dir)
        migrated_spec_ids: set = set()

        for iid, entry_dict in sorted(by_initiative.items()):
            spec_id = entry_dict.get("spec_id", "")
            old_json = log_dir / f"{spec_id}.json"
            old_md = log_dir / f"{spec_id}.md"

            if not old_json.exists():
                continue

            spec = SpecDocument.model_validate_json(
                old_json.read_text(encoding="utf-8")
            )
            rendered = old_md.read_text(encoding="utf-8") if old_md.exists() else ""
            render_error = entry_dict.get("render_error")

            # Preserve original assembled_at date for versioned filename
            try:
                assembled_date = spec.assembled_at_utc[:10]  # YYYY-MM-DD
            except (AttributeError, IndexError):
                assembled_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            initiative_dir = log_dir / iid
            initiative_dir.mkdir(parents=True, exist_ok=True)

            hash8 = spec.spec_id[:8]
            versioned_stem = f"v1_{assembled_date}_{hash8}"

            (initiative_dir / f"{versioned_stem}.json").write_text(
                spec.model_dump_json(indent=2), encoding="utf-8"
            )
            (initiative_dir / f"{versioned_stem}.md").write_text(
                rendered, encoding="utf-8"
            )
            (initiative_dir / "current.json").write_text(
                spec.model_dump_json(indent=2), encoding="utf-8"
            )
            (initiative_dir / "current.md").write_text(
                rendered, encoding="utf-8"
            )

            now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
            index_entry = SpecLogEntry(
                spec_id=spec.spec_id,
                initiative_id=spec.initiative_id,
                initiative_name=spec.initiative_name,
                spec_type=spec.spec_type,
                readiness=spec.readiness,
                composite_score=spec.composite_score,
                graph_build_id=spec.graph_build_id,
                assembled_at_utc=spec.assembled_at_utc,
                rendered=bool(rendered),
                render_error=render_error,
                current_version=1,
                rendered_at_utc=now_utc,
                path=f"spec_log/{iid}/current.json",
            )
            new_log._upsert_index(index_entry)
            migrated_spec_ids.add(spec_id)

        # Remove old flat spec files
        for spec_id in migrated_spec_ids:
            (log_dir / f"{spec_id}.json").unlink(missing_ok=True)
            (log_dir / f"{spec_id}.md").unlink(missing_ok=True)

        # Remove any remaining flat spec files (orphaned duplicates: 16-char hex names)
        for old_json in list(log_dir.glob("*.json")):
            if old_json.name == _INDEX_FILE:
                continue
            stem = old_json.stem
            if len(stem) == 16 and all(c in "0123456789abcdef" for c in stem):
                old_json.unlink(missing_ok=True)
                old_md = old_json.with_suffix(".md")
                if old_md.exists():
                    old_md.unlink()
