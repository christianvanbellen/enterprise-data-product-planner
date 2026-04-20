from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict


class EdgeType(str, Enum):
    # Structural (Phase 2)
    CONTAINS      = "CONTAINS"
    HAS_COLUMN    = "HAS_COLUMN"
    DEPENDS_ON    = "DEPENDS_ON"
    DERIVES_FROM  = "DERIVES_FROM"
    TESTED_BY     = "TESTED_BY"
    DOCUMENTED_BY = "DOCUMENTED_BY"
    # Semantic (Phase 3)
    REPRESENTS                = "REPRESENTS"
    BELONGS_TO_DOMAIN         = "BELONGS_TO_DOMAIN"
    IDENTIFIES                = "IDENTIFIES"
    MEASURES                  = "MEASURES"
    METRIC_BELONGS_TO_ENTITY  = "METRIC_BELONGS_TO_ENTITY"
    # Opportunity (Phase 4)
    ENABLES          = "ENABLES"
    REQUIRES         = "REQUIRES"
    BLOCKED_BY       = "BLOCKED_BY"
    COMPOSES_WITH    = "COMPOSES_WITH"
    PRIMITIVE_COVERS = "PRIMITIVE_COVERS"


@dataclass
class GraphEdge:
    edge_id: str
    edge_type: EdgeType
    source_node_id: str
    target_node_id: str
    properties: Dict[str, Any]
    evidence: Dict[str, Any]
    build_id: str
