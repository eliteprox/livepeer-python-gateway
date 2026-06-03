from __future__ import annotations

import base64
import json
import time
from typing import Any, Optional


def bearer_jwt_exp_unix(headers: Optional[dict[str, str]]) -> Optional[int]:
    """Return JWT ``exp`` (unix seconds) from an Authorization bearer header, if present."""
    if not headers:
        return None
    auth = headers.get("Authorization") or headers.get("authorization") or ""
    if not auth.lower().startswith("bearer "):
        return None
    token = auth[7:].strip()
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        payload_b64 = parts[1]
        pad = "=" * (-len(payload_b64) % 4)
        payload: dict[str, Any] = json.loads(
            base64.urlsafe_b64decode(payload_b64 + pad).decode("utf-8"),
        )
        exp = payload.get("exp")
        if exp is None:
            return None
        return int(exp)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def should_refresh_signer_bearer(
    headers: Optional[dict[str, str]],
    *,
    skew_seconds: int = 60,
    now: Optional[float] = None,
) -> bool:
    """True when the bearer JWT is missing, unparsable, or within ``skew_seconds`` of expiry."""
    exp = bearer_jwt_exp_unix(headers)
    if exp is None:
        return False
    current = time.time() if now is None else now
    return current >= exp - max(0, skew_seconds)
