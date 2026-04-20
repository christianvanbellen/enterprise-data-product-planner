from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict

from ingestion.contracts.asset import Provenance


class CanonicalBusinessTerm(BaseModel):
    model_config = ConfigDict(frozen=True)

    internal_id: str
    term_type: Literal["conformed_concept", "business_term"]
    name: str
    normalized_name: str
    parent_term_id: Optional[str] = None
    attributes: Dict[str, Any] = {}
    version_hash: str
    provenance: Provenance
