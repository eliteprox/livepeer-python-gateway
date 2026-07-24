from __future__ import annotations

import base64
import json
from typing import Any

from comfypeer_mcp.auth import CachedSignerJwt, build_sdk_token_payload
from comfypeer_mcp.config import Settings


def create_signer_session_payload(
    settings: Settings,
    *,
    api_key: str,
    session: CachedSignerJwt,
) -> dict[str, Any]:
    signer_url = (session.signer_url or settings.signer_url or "").strip()
    discovery_url = (
        session.discovery_url
        or settings.resolved_default_discovery_url()
    )
    sdk = build_sdk_token_payload(
        api_key=api_key,
        signer_url=signer_url,
        discovery_url=discovery_url,
    )
    encoded = base64.b64encode(
        json.dumps(sdk, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")

    return {
        "access_token": session.jwt,
        "token_type": "Bearer",
        "expires_at": session.expires_at,
        "signer_url": signer_url or None,
        "discovery_url": discovery_url,
        "balanceUsdMicros": session.balance_usd_micros,
        "lifetimeGrantedUsdMicros": session.lifetime_granted_usd_micros,
        "sdk_token": encoded,
        "sdk_token_payload": sdk,
        "client_id": settings.pymthouse_public_client_id,
    }
