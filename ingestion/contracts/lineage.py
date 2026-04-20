from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, field_validator

from ingestion.contracts.asset import Provenance


class CanonicalLineageEdge(BaseModel):
    model_config = ConfigDict(frozen=True)

    internal_id: str
    source_asset_id: str
    target_asset_id: str
    relation_type: Literal["depends_on", "downstream_of"]
    derivation_method: Literal["explicit_metadata", "reverse_index", "parsed_sql"]
    confidence: float
    version_hash: str
    provenance: Provenance

    @field_validator("confidence")
    @classmethod
    def confidence_in_range(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"confidence must be between 0.0 and 1.0, got {v}")
        return v
