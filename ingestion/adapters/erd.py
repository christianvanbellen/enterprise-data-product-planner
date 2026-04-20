from pathlib import Path

from ingestion.adapters.base import BaseAdapter
from ingestion.contracts.bundle import CanonicalBundle


class ERDAdapter(BaseAdapter):
    """Stub adapter for ERD JSON exports. Not yet implemented."""

    def parse_file(self, path: Path) -> CanonicalBundle:
        raise NotImplementedError(
            "ERDAdapter.parse_file is not yet implemented. "
            "Implement this adapter to parse entity-relationship diagram JSON files."
        )
