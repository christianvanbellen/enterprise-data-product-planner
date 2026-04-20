from pathlib import Path

from ingestion.adapters.base import BaseAdapter
from ingestion.contracts.bundle import CanonicalBundle


class GlossaryAdapter(BaseAdapter):
    """Stub adapter for business glossary / catalogue exports. Not yet implemented."""

    def parse_file(self, path: Path) -> CanonicalBundle:
        raise NotImplementedError(
            "GlossaryAdapter.parse_file is not yet implemented. "
            "Implement this adapter to parse business glossary or catalogue files."
        )
