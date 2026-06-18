from __future__ import annotations

import base64
import json
import time
from typing import Optional


def bearer_jwt_exp_unix(headers: Optional[dict[str, str]]) -> Optional[float]:
    """Best-effort JWT ``exp`` from Authorization bearer (no signature verify)."""
    if not headers:
        return None
    auth = headers.get("Authorization")
    if not isinstance(auth, str) or not auth.startswith("Bearer "):
        return None
    token = auth[7:].strip()
    if token.startswith("pmth_"):
        return None
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        payload = parts[1]
        padded = payload + "=" * (-len(payload) % 4)
        data = json.loads(base64.urlsafe_b64decode(padded))
        exp = data.get("exp")
        if isinstance(exp, (int, float)):
            return float(exp)
    except Exception:
        return None
    return None


def should_refresh_signer_bearer(
    headers: Optional[dict[str, str]],
    *,
    skew_seconds: int = 60,
    now: Optional[float] = None,
) -> bool:
    exp = bearer_jwt_exp_unix(headers)
    if exp is None:
        return False
    current = time.time() if now is None else now
    return exp - current <= skew_seconds
