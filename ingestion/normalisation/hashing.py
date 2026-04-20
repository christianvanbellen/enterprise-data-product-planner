import hashlib
from datetime import datetime, timezone
from typing import Any


def stable_hash(*parts: Any, length: int = 16) -> str:
    """Deterministic hex hash of the given parts, joined by '||'.

    None is treated as empty string. Returns first `length` hex characters of sha256.
    """
    joined = "||".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:length]


def utc_now_iso() -> str:
    """Return current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()
