from dataclasses import dataclass, field
from typing import Any, Dict, List

from ingestion.normalisation.hashing import utc_now_iso

# Confidence constants
CONFIDENCE_EXPLICIT_DEP   = 1.00
CONFIDENCE_DIRECT_COL     = 0.95
CONFIDENCE_EXPRESSION_COL = 0.75
CONFIDENCE_AMBIGUOUS      = 0.50


@dataclass
class EvidenceRecord:
    created_by: str
    rule_id: str
    evidence_sources: List[Dict[str, str]]
    confidence: float
    review_status: str      # "auto" | "confirmed" | "overridden" | "rejected"
    build_id: str
    timestamp_utc: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "created_by": self.created_by,
            "rule_id": self.rule_id,
            "evidence_sources": self.evidence_sources,
            "confidence": self.confidence,
            "review_status": self.review_status,
            "build_id": self.build_id,
            "timestamp_utc": self.timestamp_utc,
        }

    @classmethod
    def auto(
        cls,
        rule_id: str,
        confidence: float,
        evidence_sources: List[Dict[str, str]],
        build_id: str,
    ) -> "EvidenceRecord":
        return cls(
            created_by="structural_compiler_v1",
            rule_id=rule_id,
            evidence_sources=evidence_sources,
            confidence=confidence,
            review_status="auto",
            build_id=build_id,
        )

    @classmethod
    def semantic(
        cls,
        rule_id: str,
        confidence: float,
        evidence_sources: List[Dict[str, str]],
        build_id: str,
    ) -> "EvidenceRecord":
        return cls(
            created_by="semantic_compiler_v1",
            rule_id=rule_id,
            evidence_sources=evidence_sources,
            confidence=confidence,
            review_status="auto",
            build_id=build_id,
        )

    @classmethod
    def opportunity(
        cls,
        rule_id: str,
        confidence: float,
        evidence_sources: List[Dict[str, str]],
        build_id: str,
    ) -> "EvidenceRecord":
        return cls(
            created_by="opportunity_compiler_v1",
            rule_id=rule_id,
            evidence_sources=evidence_sources,
            confidence=confidence,
            review_status="auto",
            build_id=build_id,
        )
