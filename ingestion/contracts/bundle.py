import json
from pathlib import Path
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from ingestion.contracts.asset import CanonicalAsset, CanonicalColumn
from ingestion.contracts.business import CanonicalBusinessTerm
from ingestion.contracts.lineage import CanonicalLineageEdge


class CanonicalBundle(BaseModel):
    assets: List[CanonicalAsset] = Field(default_factory=list)
    columns: List[CanonicalColumn] = Field(default_factory=list)
    lineage_edges: List[CanonicalLineageEdge] = Field(default_factory=list)
    business_terms: List[CanonicalBusinessTerm] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def merge(self, other: "CanonicalBundle") -> "CanonicalBundle":
        """Return a new bundle that concatenates all list fields and merges metadata."""
        merged_metadata = {**self.metadata, **other.metadata}
        return CanonicalBundle(
            assets=self.assets + other.assets,
            columns=self.columns + other.columns,
            lineage_edges=self.lineage_edges + other.lineage_edges,
            business_terms=self.business_terms + other.business_terms,
            metadata=merged_metadata,
        )

    def to_json(self, path: Path) -> None:
        """Write the bundle to an indented JSON file, creating parent dirs as needed."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            json.dump(self.model_dump(mode="json"), fh, indent=2, sort_keys=True)

    @classmethod
    def from_json(cls, path: Path) -> "CanonicalBundle":
        """Load a bundle from a JSON file."""
        path = Path(path)
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        return cls.model_validate(data)

    def summary(self) -> str:
        keys_str = ", ".join(sorted(self.metadata.keys())) if self.metadata else "(none)"
        return (
            "CanonicalBundle\n"
            f"  assets:          {len(self.assets)}\n"
            f"  columns:         {len(self.columns)}\n"
            f"  lineage_edges:   {len(self.lineage_edges)}\n"
            f"  business_terms:  {len(self.business_terms)}\n"
            f"  metadata keys:   {keys_str}"
        )
