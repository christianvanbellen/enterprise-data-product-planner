from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from ingestion.normalisation.hashing import utc_now_iso


class Provenance(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_system: str
    source_type: str
    source_native_id: Optional[str] = None
    extraction_timestamp_utc: str = Field(default_factory=utc_now_iso)
    raw_record_hash: Optional[str] = None


class CanonicalAsset(BaseModel):
    model_config = ConfigDict(frozen=True)

    internal_id: str
    asset_type: Literal[
        "dbt_model", "table", "view",
        "source_table", "conformed_concept_group", "unknown"
    ]
    name: str
    normalized_name: str
    database: Optional[str] = None
    schema_name: Optional[str] = None
    path: Optional[str] = None
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    materialization: Optional[str] = None
    row_count: Optional[int] = None
    size_mb: Optional[float] = None
    grain_keys: List[str] = Field(default_factory=list)
    domain_candidates: List[str] = Field(default_factory=list)
    product_lines: List[str] = Field(default_factory=list)
    # All lineage-layer values the asset's tags map to, in tag order, deduplicated.
    # Example: an asset tagged ['hx', 'bookends'] yields ['historic_exchange', 'conformed_bookends'].
    # Phase 5's _infer_table_type scans this list for any _LAYER_TO_TYPE match — so both
    # pipeline-stage tags (hx/ll/gen2/raw/source) and conformance-grade tags (bookends/semi_conformed)
    # contribute. See docs/inputs.md — tag_mappings.yaml section.
    lineage_layers: List[str] = Field(default_factory=list)
    is_enabled: bool = True
    version_hash: str
    provenance: Provenance


class CanonicalColumn(BaseModel):
    model_config = ConfigDict(frozen=True)

    internal_id: str
    asset_internal_id: str
    name: str
    normalized_name: str
    description: Optional[str] = None
    raw_data_type: Optional[str] = None
    data_type_family: str
    column_role: str
    ordinal_position: Optional[int] = None
    is_nullable: Optional[bool] = None
    tests: List[str] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)
    semantic_candidates: List[str] = Field(default_factory=list)
    version_hash: str
    provenance: Provenance
