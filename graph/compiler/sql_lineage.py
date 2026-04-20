"""SQL column-level lineage extraction using sqlglot.

This module is gated behind ENABLE_SQL_LINEAGE=true.
Currently a stub — full implementation is Phase 2 optional.
"""

from typing import List

from ingestion.contracts.lineage import CanonicalLineageEdge


def extract_column_lineage(sql: str, asset_id: str) -> List[CanonicalLineageEdge]:
    """Parse compiled SQL and emit DERIVES_FROM lineage edges.

    Uses sqlglot for multi-dialect parsing. Not yet fully implemented.
    """
    raise NotImplementedError(
        "extract_column_lineage is not yet implemented. "
        "Set ENABLE_SQL_LINEAGE=false (the default) to skip SQL-level lineage."
    )
