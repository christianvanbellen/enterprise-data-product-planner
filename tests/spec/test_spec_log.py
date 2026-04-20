"""Tests for SpecLog."""

import json
import pytest
from pathlib import Path

from graph.spec.assembler import SpecDocument
from graph.spec.log import SpecLog, SpecLogEntry


def _make_spec(initiative_id="test_initiative", build_id="build_001", readiness="ready_now"):
    from ingestion.normalisation.hashing import stable_hash
    spec_id = stable_hash(initiative_id, build_id)
    return SpecDocument(
        spec_id=spec_id,
        spec_type="full_spec" if readiness in ("ready_now", "ready_with_enablement") else "gap_brief",
        initiative_id=initiative_id,
        initiative_name=initiative_id.replace("_", " ").title(),
        archetype="monitoring",
        readiness=readiness,
        composite_score=0.75,
        business_value_score=0.80,
        implementation_effort_score=0.40,
        business_objective="Test objective",
        output_type="monitoring_dashboard",
        target_users=[],
        composes_with=[],
        available_primitives=[],
        missing_primitives=[],
        blockers=[],
        grain_join_paths=[],
        graph_build_id=build_id,
        assembled_at_utc="2026-04-15T10:00:00+00:00",
    )


# ------------------------------------------------------------------ #
# Save / load round-trip                                              #
# ------------------------------------------------------------------ #

def test_save_and_load_round_trip(tmp_path):
    spec = _make_spec()
    log = SpecLog(tmp_path)
    rendered = "## Test Initiative\n\nSome spec text."
    spec_id = log.save(spec, rendered)

    loaded_spec, loaded_md = log.load(spec_id)
    assert loaded_spec.initiative_id == spec.initiative_id
    assert loaded_spec.spec_type == spec.spec_type
    assert loaded_md == rendered


def test_save_creates_initiative_directory(tmp_path):
    spec = _make_spec()
    log = SpecLog(tmp_path)
    log.save(spec, "Some text")

    assert (tmp_path / spec.initiative_id).is_dir()


def test_save_creates_current_files(tmp_path):
    spec = _make_spec()
    log = SpecLog(tmp_path)
    log.save(spec, "Some text")

    assert (tmp_path / spec.initiative_id / "current.json").exists()
    assert (tmp_path / spec.initiative_id / "current.md").exists()


def test_save_creates_versioned_files(tmp_path):
    spec = _make_spec()
    log = SpecLog(tmp_path)
    log.save(spec, "Some text")

    initiative_dir = tmp_path / spec.initiative_id
    versioned = list(initiative_dir.glob("v*.json"))
    assert len(versioned) == 1
    assert versioned[0].name.startswith("v1_")


def test_save_creates_index_json(tmp_path):
    spec = _make_spec()
    log = SpecLog(tmp_path)
    log.save(spec, "Some text")

    index_path = tmp_path / "index.json"
    assert index_path.exists()
    data = json.loads(index_path.read_text())
    assert len(data) == 1
    assert data[0]["initiative_id"] == spec.initiative_id


def test_load_raises_for_unknown_spec_id(tmp_path):
    log = SpecLog(tmp_path)
    with pytest.raises(FileNotFoundError):
        log.load("nonexistent0000ab")


def test_current_json_content_matches_latest_versioned(tmp_path):
    """current.json content must be identical to the latest versioned file."""
    spec = _make_spec()
    log = SpecLog(tmp_path)
    log.save(spec, "Some text")

    initiative_dir = tmp_path / spec.initiative_id
    current = json.loads((initiative_dir / "current.json").read_text())
    versioned = list(initiative_dir.glob("v*.json"))
    versioned_data = json.loads(versioned[0].read_text())

    assert current == versioned_data


# ------------------------------------------------------------------ #
# Index maintenance                                                    #
# ------------------------------------------------------------------ #

def test_upsert_index_replaces_same_spec_id(tmp_path):
    spec = _make_spec()
    log = SpecLog(tmp_path)
    log.save(spec, "First render")
    log.save(spec, "Second render")   # same spec_id, same initiative_id

    entries = log.list_specs()
    assert len(entries) == 1   # no duplicate


def test_index_no_duplicate_initiative_ids_after_multiple_writes(tmp_path):
    """Writing the same initiative twice must not create a duplicate index entry."""
    spec_v1 = _make_spec("my_initiative", "build_001")
    spec_v2 = _make_spec("my_initiative", "build_002")

    log = SpecLog(tmp_path)
    log.save(spec_v1, "Render v1")
    log.save(spec_v2, "Render v2")

    index = json.loads((tmp_path / "index.json").read_text())
    ids = [e["initiative_id"] for e in index]
    assert ids.count("my_initiative") == 1, "Duplicate initiative_id in index"


def test_second_write_increments_version(tmp_path):
    spec_v1 = _make_spec("my_initiative", "build_001")
    spec_v2 = _make_spec("my_initiative", "build_002")

    log = SpecLog(tmp_path)
    log.save(spec_v1, "Render v1")
    log.save(spec_v2, "Render v2")

    initiative_dir = tmp_path / "my_initiative"
    versioned = sorted(initiative_dir.glob("v*.json"))
    assert len(versioned) == 2
    assert versioned[0].name.startswith("v1_")
    assert versioned[1].name.startswith("v2_")


def test_second_write_updates_current_files(tmp_path):
    spec_v1 = _make_spec("my_initiative", "build_001")
    spec_v2 = _make_spec("my_initiative", "build_002")

    log = SpecLog(tmp_path)
    log.save(spec_v1, "Render v1")
    log.save(spec_v2, "Render v2")

    initiative_dir = tmp_path / "my_initiative"
    current = SpecDocument.model_validate_json(
        (initiative_dir / "current.json").read_text()
    )
    assert current.spec_id == spec_v2.spec_id


def test_list_specs_returns_all(tmp_path):
    log = SpecLog(tmp_path)
    log.save(_make_spec("initiative_a", "build_001"), "Render A")
    log.save(_make_spec("initiative_b", "build_001"), "Render B")

    entries = log.list_specs()
    assert len(entries) == 2


def test_list_specs_filter_by_initiative_id(tmp_path):
    log = SpecLog(tmp_path)
    log.save(_make_spec("initiative_a", "build_001"), "Render A")
    log.save(_make_spec("initiative_b", "build_001"), "Render B")

    entries = log.list_specs(initiative_id="initiative_a")
    assert len(entries) == 1
    assert entries[0].initiative_id == "initiative_a"


def test_get_latest_returns_most_recent(tmp_path):
    log = SpecLog(tmp_path)

    spec_v1 = _make_spec("my_initiative", "build_001")
    spec_v2 = _make_spec("my_initiative", "build_002")

    spec_v1 = spec_v1.model_copy(update={"assembled_at_utc": "2026-04-14T10:00:00+00:00"})
    spec_v2 = spec_v2.model_copy(update={"assembled_at_utc": "2026-04-15T12:00:00+00:00"})

    log.save(spec_v1, "Render v1")
    log.save(spec_v2, "Render v2")

    latest = log.get_latest("my_initiative")
    assert latest is not None
    assert latest.graph_build_id == "build_002"


def test_get_latest_returns_none_for_unknown(tmp_path):
    log = SpecLog(tmp_path)
    result = log.get_latest("does_not_exist")
    assert result is None


# ------------------------------------------------------------------ #
# has_spec                                                             #
# ------------------------------------------------------------------ #

def test_has_spec_true_after_save(tmp_path):
    spec = _make_spec()
    log = SpecLog(tmp_path)
    log.save(spec, "text")
    assert log.has_spec(spec.spec_id) is True


def test_has_spec_false_before_save(tmp_path):
    log = SpecLog(tmp_path)
    assert log.has_spec("00000000000000ab") is False


def test_has_spec_false_for_old_build_id(tmp_path):
    """has_spec must return False when a new build_id replaces an old one."""
    spec_v1 = _make_spec("my_initiative", "build_001")
    spec_v2 = _make_spec("my_initiative", "build_002")

    log = SpecLog(tmp_path)
    log.save(spec_v1, "Render v1")
    log.save(spec_v2, "Render v2")

    assert log.has_spec(spec_v1.spec_id) is False   # old build — no longer current
    assert log.has_spec(spec_v2.spec_id) is True    # current build


# ------------------------------------------------------------------ #
# render_error field                                                   #
# ------------------------------------------------------------------ #

def test_render_error_stored_in_index(tmp_path):
    spec = _make_spec()
    log = SpecLog(tmp_path)
    log.save(spec, "", render_error="Rate limit exceeded")

    entries = log.list_specs()
    assert entries[0].render_error == "Rate limit exceeded"
    assert entries[0].rendered is False


def test_rendered_flag_true_when_markdown_present(tmp_path):
    spec = _make_spec()
    log = SpecLog(tmp_path)
    log.save(spec, "## Some markdown content")

    entries = log.list_specs()
    assert entries[0].rendered is True


# ------------------------------------------------------------------ #
# Index entry extended fields                                          #
# ------------------------------------------------------------------ #

def test_index_entry_has_extended_fields(tmp_path):
    spec = _make_spec()
    log = SpecLog(tmp_path)
    log.save(spec, "Some text")

    index = json.loads((tmp_path / "index.json").read_text())
    entry = index[0]
    assert entry["initiative_name"] == spec.initiative_name
    assert entry["composite_score"] == spec.composite_score
    assert entry["current_version"] == 1
    assert "rendered_at_utc" in entry
    assert entry["path"] == f"spec_log/{spec.initiative_id}/current.json"


# ------------------------------------------------------------------ #
# migrate_flat_to_versioned                                            #
# ------------------------------------------------------------------ #

def _make_flat_log(flat_dir: Path) -> None:
    """Create a minimal flat spec_log fixture (old format) with two initiatives."""
    from ingestion.normalisation.hashing import stable_hash

    specs = [
        _make_spec("initiative_a", "build_001"),
        _make_spec("initiative_b", "build_001"),
        # Duplicate entry for initiative_a (simulates the dedup bug)
        _make_spec("initiative_a", "build_001"),
    ]

    index_entries = []
    seen_spec_ids = set()
    for spec in specs:
        (flat_dir / f"{spec.spec_id}.json").write_text(
            spec.model_dump_json(indent=2), encoding="utf-8"
        )
        (flat_dir / f"{spec.spec_id}.md").write_text(
            f"# {spec.initiative_id}", encoding="utf-8"
        )
        entry = {
            "spec_id": spec.spec_id,
            "initiative_id": spec.initiative_id,
            "spec_type": spec.spec_type,
            "readiness": spec.readiness,
            "graph_build_id": spec.graph_build_id,
            "assembled_at_utc": spec.assembled_at_utc,
            "rendered": True,
            "render_error": None,
        }
        index_entries.append(entry)

    (flat_dir / "index.json").write_text(
        json.dumps(index_entries, indent=2), encoding="utf-8"
    )


def test_migrate_creates_initiative_directories(tmp_path):
    _make_flat_log(tmp_path)
    SpecLog.migrate_flat_to_versioned(tmp_path)

    dirs = [d for d in tmp_path.iterdir() if d.is_dir()]
    dir_names = {d.name for d in dirs}
    assert "initiative_a" in dir_names
    assert "initiative_b" in dir_names


def test_migrate_creates_current_and_versioned_files(tmp_path):
    _make_flat_log(tmp_path)
    SpecLog.migrate_flat_to_versioned(tmp_path)

    for iid in ("initiative_a", "initiative_b"):
        d = tmp_path / iid
        assert (d / "current.json").exists()
        assert (d / "current.md").exists()
        versioned = list(d.glob("v*.json"))
        assert len(versioned) == 1
        assert versioned[0].name.startswith("v1_")


def test_migrate_deduplicates_index(tmp_path):
    """After migration, index.json must have exactly one entry per initiative."""
    _make_flat_log(tmp_path)
    SpecLog.migrate_flat_to_versioned(tmp_path)

    index = json.loads((tmp_path / "index.json").read_text())
    ids = [e["initiative_id"] for e in index]
    assert len(ids) == len(set(ids)), "Duplicate initiative_id entries after migration"
    assert set(ids) == {"initiative_a", "initiative_b"}


def test_migrate_removes_flat_files(tmp_path):
    """Old flat {spec_id}.json files must be removed after migration."""
    _make_flat_log(tmp_path)

    # Collect all 16-char hex json files before migration
    flat_files_before = [
        f for f in tmp_path.glob("*.json")
        if f.name != "index.json" and len(f.stem) == 16
    ]
    assert len(flat_files_before) > 0, "Fixture must have flat files"

    SpecLog.migrate_flat_to_versioned(tmp_path)

    flat_files_after = [
        f for f in tmp_path.glob("*.json")
        if f.name != "index.json" and len(f.stem) == 16
    ]
    assert len(flat_files_after) == 0, "Flat spec files not removed after migration"


def test_migrate_is_no_op_when_no_index(tmp_path):
    """migrate_flat_to_versioned must not raise if index.json is absent."""
    SpecLog.migrate_flat_to_versioned(tmp_path)  # should not raise


def test_migrate_produces_loadable_specs(tmp_path):
    """After migration, log.load() must return the correct spec for each initiative."""
    _make_flat_log(tmp_path)
    SpecLog.migrate_flat_to_versioned(tmp_path)

    log = SpecLog(tmp_path)
    for iid in ("initiative_a", "initiative_b"):
        entry = log.get_latest(iid)
        assert entry is not None
        spec, rendered = log.load(entry.spec_id)
        assert spec.initiative_id == iid
        assert rendered == f"# {iid}"
