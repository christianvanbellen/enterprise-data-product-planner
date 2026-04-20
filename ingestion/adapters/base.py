import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict

from ingestion.contracts.asset import Provenance
from ingestion.contracts.bundle import CanonicalBundle
from ingestion.normalisation.hashing import stable_hash, utc_now_iso


class BaseAdapter(ABC):
    """Abstract base class for all ingestion adapters."""

    @abstractmethod
    def parse_file(self, path: Path) -> CanonicalBundle:
        """Parse a source file and return a CanonicalBundle."""
        ...

    def _provenance(
        self,
        source_system: str,
        source_type: str,
        source_native_id: str,
        raw_record: Any,
    ) -> Provenance:
        raw_json = json.dumps(raw_record, sort_keys=True, default=str)
        raw_record_hash = stable_hash(raw_json)
        return Provenance(
            source_system=source_system,
            source_type=source_type,
            source_native_id=source_native_id,
            extraction_timestamp_utc=utc_now_iso(),
            raw_record_hash=raw_record_hash,
        )
