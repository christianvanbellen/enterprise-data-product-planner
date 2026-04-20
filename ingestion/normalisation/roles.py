from typing import Optional

from ingestion.normalisation.dtypes import classify_data_type


def infer_column_role(
    column_name: str,
    data_type: Optional[str],
    description: Optional[str],
) -> str:
    """Infer a semantic role for a column from its name, type, and description.

    Priority order (first match wins):
      identifier         — name ends _id, starts id_, or equals id
      timestamp          — name ends _at, contains "timestamp", or dtype is timestamp/date
      boolean_flag       — dtype boolean
      measure            — dtype numeric + name contains measure keywords
      numeric_attribute  — dtype numeric (fallthrough)
      categorical_attribute — dtype string + name contains category keywords,
                              OR dtype string + description contains name/description/code
      attribute          — dtype string (fallthrough)
      semi_structured    — dtype semi_structured
      unknown            — everything else
    """
    name = (column_name or "").lower().strip()
    dtype_family = classify_data_type(data_type)
    desc = (description or "").lower()

    # identifier
    if name == "id" or name.endswith("_id") or name.startswith("id_"):
        return "identifier"

    # timestamp — name-based patterns or dtype is timestamp/date.
    # Use boundary-safe patterns to avoid matching "updated_by" via "date" in "updated".
    _DATE_PARTS = ("_at", "_date", "_datetime", "_timestamp")
    if (
        any(name.endswith(p) for p in _DATE_PARTS)
        or name.startswith("date_")
        or "timestamp" in name
        or dtype_family in ("timestamp", "date")
    ):
        return "timestamp"

    # boolean_flag
    if dtype_family == "boolean":
        return "boolean_flag"

    _MEASURE_KEYWORDS = (
        "pct", "percentage", "amount", "premium", "rate", "count",
        "sum", "total", "value",
    )

    # measure
    if dtype_family == "numeric":
        if any(k in name for k in _MEASURE_KEYWORDS):
            return "measure"
        return "numeric_attribute"

    # categorical_attribute / attribute
    if dtype_family == "string":
        cat_name_keywords = ("status", "type", "category", "segment", "flag", "indicator")
        if any(k in name for k in cat_name_keywords):
            return "categorical_attribute"
        cat_desc_keywords = ("name", "description", "code")
        if any(k in desc for k in cat_desc_keywords):
            return "categorical_attribute"
        return "attribute"

    # semi_structured
    if dtype_family == "semi_structured":
        return "semi_structured"

    # Name-based fallback for null/unknown dtype — avoids "unknown" when column name
    # or description provides clear evidence of a numeric measure.
    if any(k in name for k in _MEASURE_KEYWORDS):
        return "measure"
    if any(k in desc for k in ("ratio", "percentage", "percent", " %", "expressed as a")):
        return "measure"

    return "unknown"
