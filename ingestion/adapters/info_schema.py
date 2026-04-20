from pathlib import Path

from ingestion.adapters.base import BaseAdapter
from ingestion.contracts.bundle import CanonicalBundle


class InformationSchemaAdapter(BaseAdapter):
    """Stub adapter for information schema exports. Not yet implemented."""

    def parse_file(self, path: Path) -> CanonicalBundle:
        raise NotImplementedError(
            "InformationSchemaAdapter.parse_file is not yet implemented. "
            "Implement this adapter to parse information schema export files."
        )
