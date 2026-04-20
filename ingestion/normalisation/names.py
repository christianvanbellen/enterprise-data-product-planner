import re
from typing import Iterable, List, Optional


def normalize_name(value: str) -> str:
    """Normalise an identifier to snake_case lowercase.

    Pipeline:
      1. strip whitespace
      2. replace hyphens with underscores
      3. collapse runs of whitespace to underscore
      4. strip anything that is not alphanumeric or underscore
      5. deduplicate consecutive underscores
      6. lowercase
      7. strip leading/trailing underscores
    """
    s = value.strip()
    s = s.replace("-", "_")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-zA-Z0-9_]", "", s)
    s = re.sub(r"_+", "_", s)
    s = s.lower()
    s = s.strip("_")
    return s


def normalize_text(value: Optional[str]) -> Optional[str]:
    """Collapse whitespace, strip, and return None if the result is empty."""
    if value is None:
        return None
    collapsed = re.sub(r"\s+", " ", value).strip()
    return collapsed if collapsed else None


def normalize_tags(tags: Optional[Iterable[str]]) -> List[str]:
    """Normalize each tag, deduplicate while preserving first-seen order."""
    if tags is None:
        return []
    seen: set[str] = set()
    result: List[str] = []
    for tag in tags:
        normalized = normalize_name(tag)
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result
