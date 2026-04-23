"""DbtMetadataAdapter — converts dbt enriched metadata JSON into a CanonicalBundle."""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from ingestion.adapters.base import BaseAdapter
from ingestion.contracts.asset import CanonicalAsset, CanonicalColumn
from ingestion.contracts.bundle import CanonicalBundle
from ingestion.contracts.lineage import CanonicalLineageEdge
from ingestion.normalisation.dtypes import classify_data_type
from ingestion.normalisation.hashing import stable_hash
from ingestion.normalisation.names import normalize_name, normalize_tags, normalize_text
from ingestion.normalisation.roles import infer_column_role

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
# Tag mappings (loaded once at module level)                          #
# ------------------------------------------------------------------ #

_ONTOLOGY_DIR = Path(__file__).parent.parent.parent / "ontology"

# Dict of {dimension_name: {raw_tag: mapped_value}} loaded from tag_mappings.yaml.
# Example: {"lineage_layer": {"hx": "historic_exchange", ...}, "product_line": {...}}
_DIMENSION_TAG_MAPPINGS: Dict[str, Dict[str, str]] = {}

DOMAIN_KEYWORDS: Dict[str, List[str]] = {}
GRAIN_KEY_CANDIDATES: set = set()
SEMANTIC_MAP: Dict[str, str] = {}

try:
    import yaml  # type: ignore

    # Loaded from ontology/tag_mappings.yaml. Each dimension's tag_mappings becomes an
    # entry in _DIMENSION_TAG_MAPPINGS keyed by dimension name.
    _raw_mappings = yaml.safe_load((_ONTOLOGY_DIR / "tag_mappings.yaml").read_text(encoding="utf-8"))
    for _dim_name, _dim_spec in (_raw_mappings.get("dimensions") or {}).items():
        _DIMENSION_TAG_MAPPINGS[_dim_name] = dict((_dim_spec or {}).get("tag_mappings") or {})

    # Loaded from ontology/domain_keywords.yaml — edit that file to change domain assignment.
    DOMAIN_KEYWORDS = yaml.safe_load((_ONTOLOGY_DIR / "domain_keywords.yaml").read_text(encoding="utf-8")) or {}

    # Loaded from ontology/semantic_map.yaml — edit that file to change semantic candidates.
    SEMANTIC_MAP = yaml.safe_load((_ONTOLOGY_DIR / "semantic_map.yaml").read_text(encoding="utf-8")) or {}

    # Loaded from ontology/grain_keys.yaml — edit that file to change grain key recognition.
    _grain_raw = yaml.safe_load((_ONTOLOGY_DIR / "grain_keys.yaml").read_text(encoding="utf-8"))
    GRAIN_KEY_CANDIDATES = set(_grain_raw.get("candidates", []))

except Exception as _exc:
    logger.warning("Could not load ontology config (%s); domain/semantic/grain inference may be empty.", _exc)

SOURCE_SYSTEM = "dbt"
SOURCE_TYPE = "DbtMetadataAdapter"


def _infer_domains(
    model_name: str,
    description: Optional[str],
    tags: List[str],
    column_names: List[str],
) -> List[str]:
    corpus = " ".join(
        [model_name, description or ""] + tags + column_names
    ).lower()
    found = []
    for domain, keywords in DOMAIN_KEYWORDS.items():
        if any(kw in corpus for kw in keywords):
            found.append(domain)
    return found


def _infer_grain_keys(column_names: List[str]) -> List[str]:
    return [c for c in column_names if c.lower() in GRAIN_KEY_CANDIDATES]


def _infer_tag_dimensions(tags: List[str]) -> Dict[str, List[str]]:
    """Classify a flat dbt tag list across every registered dimension.

    For each dimension in _DIMENSION_TAG_MAPPINGS, iterates the asset's tags in order and
    collects any that map to a dimension value, deduplicating while preserving order.
    Dimensions with no matching tags are omitted from the result — the returned dict only
    contains dimensions that the asset actually participates in.

    Example — tags=['HX', 'bookends', 'd_o'] yields:
        {
          "lineage_layer": ["historic_exchange", "conformed_bookends"],
          "product_line":  ["directors_and_officers"],
        }
    """
    result: Dict[str, List[str]] = {}
    for dim_name, tag_map in _DIMENSION_TAG_MAPPINGS.items():
        seen: set = set()
        values: List[str] = []
        for tag in tags:
            mapped = tag_map.get(tag)
            if mapped and mapped not in seen:
                seen.add(mapped)
                values.append(mapped)
        if values:
            result[dim_name] = values
    return result


def _infer_semantic_candidates(col_name: str, description: Optional[str]) -> List[str]:
    candidates = []
    lower_name = col_name.lower()
    lower_desc = (description or "").lower()
    for key, semantic in SEMANTIC_MAP.items():
        if key in lower_name or key in lower_desc:
            if semantic not in candidates:
                candidates.append(semantic)
    return candidates


def _parse_nullable(value: Any) -> Optional[bool]:
    """Convert 'YES'/'NO'/bool/None to Optional[bool]."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    s = str(value).strip().upper()
    if s == "YES":
        return True
    if s == "NO":
        return False
    return None


class DbtMetadataAdapter(BaseAdapter):
    """Parse a dbt enriched metadata JSON file into a CanonicalBundle."""

    def detect(self, path: Path) -> dict:
        """Check whether the file at path looks like a supported dbt metadata artifact.

        Returns a dict with keys:
          - compatible: bool
          - variant: str | None  ("root_list" | "entities_dict" | "single_entity" | None)
          - missing_fields: list[str]  (required fields absent from first entity)
          - warnings: list[str]  (non-fatal issues found)
          - entity_count: int | None
        Does not raise. Never modifies state.
        """
        result: dict = {
            "compatible": False,
            "variant": None,
            "missing_fields": [],
            "warnings": [],
            "entity_count": None,
        }

        try:
            path = Path(path)
            with path.open("r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            result["warnings"].append(f"Could not load file: {exc}")
            return result

        # Determine variant and entity list
        if isinstance(raw, list):
            result["variant"] = "root_list"
            entities = raw
        elif isinstance(raw, dict):
            if "entities" in raw:
                result["variant"] = "entities_dict"
                entities = raw["entities"]
            else:
                result["variant"] = "single_entity"
                entities = [raw]
        else:
            result["warnings"].append(
                f"Root value is {type(raw).__name__}, expected list or dict."
            )
            return result

        result["entity_count"] = len(entities)

        if not entities:
            result["warnings"].append("entities array is empty.")
            result["compatible"] = True  # structurally valid but empty
            return result

        # Validate required fields on the first entity
        first = entities[0]
        if not isinstance(first, dict):
            result["missing_fields"].append("first entity is not a dict")
            return result

        # At least one of name / unique_id must be present
        if not first.get("name") and not first.get("unique_id"):
            result["missing_fields"].append("name or unique_id")

        # columns must be present (list or dict)
        if "columns" not in first:
            result["missing_fields"].append("columns")
        elif not isinstance(first["columns"], (list, dict)):
            result["missing_fields"].append(
                f"columns (expected list or dict, got {type(first['columns']).__name__})"
            )

        # Non-fatal warnings
        if not first.get("resource_type"):
            result["warnings"].append(
                "First entity has no resource_type field; will default to dbt_model."
            )
        if first.get("columns") is not None and len(first.get("columns") or []) == 0:
            result["warnings"].append(
                "First entity has an empty columns list."
            )
        if "upstream_dependencies" not in first and "depends_on" not in first:
            result["warnings"].append(
                "First entity has neither upstream_dependencies nor depends_on; "
                "no lineage edges will be produced."
            )

        result["compatible"] = len(result["missing_fields"]) == 0
        return result

    def parse_file(self, path: Path) -> CanonicalBundle:
        path = Path(path)
        detection = self.detect(path)
        if not detection["compatible"]:
            raise ValueError(
                f"DbtMetadataAdapter: file {path} is not compatible. "
                f"Missing required fields: {detection['missing_fields']}. "
                f"Warnings: {detection['warnings']}"
            )

        with path.open("r", encoding="utf-8") as fh:
            raw = json.load(fh)

        # Support top-level list, or object with 'entities' key
        if isinstance(raw, list):
            entities = raw
        elif isinstance(raw, dict):
            entities = raw.get("entities", []) if "entities" in raw else [raw]
        else:
            entities = []

        assets: List[CanonicalAsset] = []
        columns: List[CanonicalColumn] = []
        edges: List[CanonicalLineageEdge] = []

        for entity in entities:
            asset, asset_cols, asset_edges = self._parse_entity(entity)
            assets.append(asset)
            columns.extend(asset_cols)
            edges.extend(asset_edges)

        logger.info(
            "DbtMetadataAdapter: %d assets, %d columns, %d edges from %s",
            len(assets), len(columns), len(edges), path,
        )

        return CanonicalBundle(
            assets=assets,
            columns=columns,
            lineage_edges=edges,
            business_terms=[],
            metadata={"source_file": str(path), "adapter": SOURCE_TYPE},
        )

    def _parse_entity(
        self, entity: Dict[str, Any]
    ) -> tuple[CanonicalAsset, List[CanonicalColumn], List[CanonicalLineageEdge]]:
        unique_id: str = entity.get("unique_id") or entity.get("name", "")
        name: str = entity.get("name", unique_id)
        description = normalize_text(entity.get("description"))
        tags = normalize_tags(entity.get("tags") or [])
        schema_name = entity.get("schema") or entity.get("schema_name")
        database = entity.get("database")
        path_str = entity.get("path")

        # `materialized` is a top-level field in the enriched metadata format
        materialization = (
            entity.get("materialized")
            or entity.get("config", {}).get("materialized")
            or entity.get("materialization")
        )

        # `row_count` and `size_mb` are top-level fields
        row_count_raw = entity.get("row_count")
        row_count = int(row_count_raw) if row_count_raw is not None else None
        size_mb_raw = entity.get("size_mb")
        size_mb = float(size_mb_raw) if size_mb_raw is not None else None

        # columns is a list of dicts with a 'name' field
        raw_columns_list: List[Dict[str, Any]] = entity.get("columns") or []
        if isinstance(raw_columns_list, dict):
            # Fall back for dict-style columns (other dbt formats)
            raw_columns_list = [{"name": k, **v} for k, v in raw_columns_list.items()]

        column_names = [c.get("name", "").lower() for c in raw_columns_list]

        asset_id = f"asset_{stable_hash(SOURCE_SYSTEM, unique_id)}"
        version_hash = stable_hash(
            json.dumps(entity, sort_keys=True, default=str)
        )

        domain_candidates = _infer_domains(name, description, tags, column_names)
        grain_keys = _infer_grain_keys(column_names)
        tag_dimensions = _infer_tag_dimensions(tags)

        provenance = self._provenance(SOURCE_SYSTEM, SOURCE_TYPE, unique_id, entity)

        # Determine asset_type
        raw_type = (entity.get("resource_type") or "").lower()
        mat = (materialization or "").lower()
        if raw_type == "source":
            asset_type = "source_table"
        elif mat == "view":
            asset_type = "view"
        elif mat == "table":
            asset_type = "table"
        else:
            # incremental, ephemeral, or unknown → dbt_model
            asset_type = "dbt_model"

        asset = CanonicalAsset(
            internal_id=asset_id,
            asset_type=asset_type,
            name=name,
            normalized_name=normalize_name(name),
            database=database,
            schema_name=schema_name,
            path=path_str,
            description=description,
            tags=tags,
            materialization=materialization,
            row_count=row_count,
            size_mb=size_mb,
            grain_keys=grain_keys,
            domain_candidates=domain_candidates,
            tag_dimensions=tag_dimensions,
            is_enabled=entity.get("config", {}).get("enabled", True),
            version_hash=version_hash,
            provenance=provenance,
        )

        parsed_columns = self._parse_columns(asset_id, raw_columns_list, entity)
        parsed_edges = self._parse_lineage(asset_id, entity)

        return asset, parsed_columns, parsed_edges

    def _parse_columns(
        self,
        asset_id: str,
        raw_columns_list: List[Dict[str, Any]],
        entity: Dict[str, Any],
    ) -> List[CanonicalColumn]:
        columns = []
        for enumerate_pos, col_data in enumerate(raw_columns_list):
            if not isinstance(col_data, dict):
                continue
            col_name = col_data.get("name", "")
            if not col_name:
                continue

            raw_dtype = col_data.get("data_type") or col_data.get("type") or ""
            # Treat empty string data_type as None for classification
            raw_dtype_for_classify = raw_dtype if raw_dtype else None
            desc = normalize_text(col_data.get("description"))
            dtype_family = classify_data_type(raw_dtype_for_classify)
            role = infer_column_role(col_name, raw_dtype_for_classify, desc)
            semantic = _infer_semantic_candidates(col_name, desc)
            tests_raw = col_data.get("tests") or []
            tests = [str(t) if not isinstance(t, str) else t for t in tests_raw]

            # Use ordinal_position from data if present; fall back to enumerate index
            ordinal_pos = col_data.get("ordinal_position")
            if ordinal_pos is None:
                ordinal_pos = enumerate_pos

            col_id = f"col_{stable_hash(asset_id, col_name)}"
            col_version = stable_hash(
                json.dumps(col_data, sort_keys=True, default=str)
            )
            provenance = self._provenance(
                SOURCE_SYSTEM,
                SOURCE_TYPE,
                f"{entity.get('unique_id', '')}::{col_name}",
                col_data,
            )

            columns.append(
                CanonicalColumn(
                    internal_id=col_id,
                    asset_internal_id=asset_id,
                    name=col_name,
                    normalized_name=normalize_name(col_name),
                    description=desc,
                    raw_data_type=raw_dtype if raw_dtype else None,
                    data_type_family=dtype_family,
                    column_role=role,
                    ordinal_position=int(ordinal_pos) if ordinal_pos is not None else None,
                    is_nullable=_parse_nullable(col_data.get("is_nullable")),
                    tests=tests,
                    meta=col_data.get("meta") or {},
                    semantic_candidates=semantic,
                    version_hash=col_version,
                    provenance=provenance,
                )
            )
        return columns

    def _parse_lineage(
        self, asset_id: str, entity: Dict[str, Any]
    ) -> List[CanonicalLineageEdge]:
        edges = []
        deps = entity.get("upstream_dependencies") or entity.get("depends_on", {})
        if isinstance(deps, dict):
            dep_list = deps.get("nodes") or []
        elif isinstance(deps, list):
            dep_list = deps
        else:
            dep_list = []

        for dep in dep_list:
            source_asset_id = f"asset_{stable_hash(SOURCE_SYSTEM, dep)}"
            edge_id = f"lin_{stable_hash(source_asset_id, asset_id, 'depends_on')}"
            version_hash = stable_hash(source_asset_id, asset_id, "depends_on")
            provenance = self._provenance(
                SOURCE_SYSTEM,
                SOURCE_TYPE,
                f"{dep}→{entity.get('unique_id', '')}",
                {"source": dep, "target": entity.get("unique_id", "")},
            )
            edges.append(
                CanonicalLineageEdge(
                    internal_id=edge_id,
                    source_asset_id=source_asset_id,
                    target_asset_id=asset_id,
                    relation_type="depends_on",
                    derivation_method="explicit_metadata",
                    confidence=1.0,
                    version_hash=version_hash,
                    provenance=provenance,
                )
            )
        return edges
