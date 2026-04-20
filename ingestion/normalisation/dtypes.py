from typing import Optional


def classify_data_type(data_type: Optional[str]) -> str:
    """Map a raw SQL data type string to a canonical type family.

    Returns one of: numeric | boolean | timestamp | date | string |
                    semi_structured | unknown
    """
    if not data_type:
        return "unknown"

    lower = data_type.lower()

    # semi_structured must be checked before string (ARRAY<STRING> contains "string")
    if any(k in lower for k in ("json", "struct", "array", "map", "super", "variant", "object")):
        return "semi_structured"
    if any(k in lower for k in ("int", "numeric", "decimal", "float", "double", "real", "number")):
        return "numeric"
    if "bool" in lower:
        return "boolean"
    if any(k in lower for k in ("timestamp", "datetime")):
        return "timestamp"
    if "date" in lower:
        return "date"
    if any(k in lower for k in ("char", "text", "varchar", "string", "clob", "nvarchar", "nchar")):
        return "string"

    return "unknown"
