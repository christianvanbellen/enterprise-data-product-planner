"""IngestionPipeline — runs configured adapters and merges their CanonicalBundles."""

import logging
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

from ingestion.contracts.bundle import CanonicalBundle

logger = logging.getLogger(__name__)


class PipelineConfig(BaseModel):
    dbt_metadata_path: Optional[Path] = None
    conformed_schema_path: Optional[Path] = None
    info_schema_path: Optional[Path] = None
    glossary_path: Optional[Path] = None
    erd_path: Optional[Path] = None
    source_system_name: str = "default"


class IngestionPipeline:
    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    def run(self) -> CanonicalBundle:
        """Run each configured adapter and merge all bundles into one."""
        # Import here to avoid circular imports at module level
        from ingestion.adapters.conformed_schema import ConformedSchemaAdapter
        from ingestion.adapters.dbt_metadata import DbtMetadataAdapter
        from ingestion.adapters.erd import ERDAdapter
        from ingestion.adapters.glossary import GlossaryAdapter
        from ingestion.adapters.info_schema import InformationSchemaAdapter

        adapter_specs = [
            (self.config.dbt_metadata_path, DbtMetadataAdapter, "DbtMetadataAdapter"),
            (self.config.conformed_schema_path, ConformedSchemaAdapter, "ConformedSchemaAdapter"),
            (self.config.info_schema_path, InformationSchemaAdapter, "InformationSchemaAdapter"),
            (self.config.glossary_path, GlossaryAdapter, "GlossaryAdapter"),
            (self.config.erd_path, ERDAdapter, "ERDAdapter"),
        ]

        merged = CanonicalBundle()

        for path, adapter_cls, adapter_name in adapter_specs:
            if path is None:
                logger.debug("Skipping %s: no path configured", adapter_name)
                continue
            path = Path(path)
            if not path.exists():
                logger.warning("Skipping %s: file not found at %s", adapter_name, path)
                continue
            logger.info("Running %s on %s", adapter_name, path)
            try:
                adapter = adapter_cls()

                # Run detect() before parse_file() for adapters that support it
                if hasattr(adapter, "detect"):
                    detection = adapter.detect(path)
                    logger.debug(
                        "%s detect() result: %s", adapter_name, detection
                    )
                    for warning in detection.get("warnings", []):
                        logger.warning("%s: %s", adapter_name, warning)

                bundle = adapter.parse_file(path)
                merged = merged.merge(bundle)
                logger.info(
                    "%s produced: %d assets, %d columns, %d edges, %d terms",
                    adapter_name,
                    len(bundle.assets),
                    len(bundle.columns),
                    len(bundle.lineage_edges),
                    len(bundle.business_terms),
                )
            except NotImplementedError as exc:
                logger.warning("Skipping %s: %s", adapter_name, exc)
            except Exception:
                logger.exception("Error running %s", adapter_name)
                raise

        return merged

    def run_and_save(self, output_path: Path) -> CanonicalBundle:
        bundle = self.run()
        bundle.to_json(output_path)
        return bundle
