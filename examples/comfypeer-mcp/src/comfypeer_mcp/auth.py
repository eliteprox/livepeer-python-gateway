from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from typing import Any

import httpx

from comfypeer_mcp.config import Settings


class AuthError(Exception):
    def __init__(
        self,
        message: str,
        *,
        status: int = 401,
        code: str = "unauthorized",
    ) -> None:
        super().__init__(message)
        self.status = status
        self.code = code


@dataclass
class AuthenticatedPrincipal:
    """End-user credential presented to the MCP host."""

    bearer_token: str
    external_user_id: str | None = None
    client_id: str | None = None


@dataclass
class CachedSignerJwt:
    jwt: str
    expires_at: float
    signer_url: str | None
    discovery_url: str | None
    balance_usd_micros: str | None = None
    lifetime_granted_usd_micros: str | None = None


_signer_cache: dict[str, CachedSignerJwt] = {}


def extract_bearer(authorization: str | None) -> str:
    if not authorization or not authorization.strip():
        raise AuthError("Authorization Bearer token is required")
    value = authorization.strip()
    if value.lower().startswith("bearer "):
        value = value[7:].strip()
    if not value:
        raise AuthError("Authorization Bearer token is required")
    return value


def _basic_auth_header(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


async def exchange_api_key_for_signer_session(
    settings: Settings,
    api_key: str,
    *,
    force_refresh: bool = False,
) -> CachedSignerJwt:
    """RFC 8693 exchange at app-scoped oidc/token using subject_token=api_key."""
    settings.require_comfypeer_app()
    cache_key = f"{settings.pymthouse_public_client_id}:{api_key[:24]}"
    cached = _signer_cache.get(cache_key)
    now = time.time()
    if (
        not force_refresh
        and cached is not None
        and cached.expires_at > now + 30
    ):
        return cached

    client_id = settings.pymthouse_public_client_id.strip()
    url = f"{settings.base_url()}/api/v1/apps/{client_id}/oidc/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": _basic_auth_header(
            settings.pymthouse_m2m_client_id.strip(),
            settings.pymthouse_m2m_client_secret.strip(),
        ),
    }
    data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "subject_token": api_key,
        "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(url, data=data, headers=headers)
        if response.status_code >= 400:
            raise AuthError(
                f"Token exchange failed: {response.status_code} {response.text}",
                status=response.status_code,
                code="token_exchange_failed",
            )
        body: dict[str, Any] = response.json()

    access_token = body.get("access_token")
    if not isinstance(access_token, str) or not access_token:
        raise AuthError("Token exchange returned no access_token", code="invalid_session")

    expires_in = int(body.get("expires_in") or 300)
    signer_url = body.get("signer_url") or settings.signer_url or None
    discovery_url = body.get("discovery_url") or settings.resolved_default_discovery_url()

    cached = CachedSignerJwt(
        jwt=access_token,
        expires_at=now + expires_in,
        signer_url=signer_url if isinstance(signer_url, str) else None,
        discovery_url=discovery_url if isinstance(discovery_url, str) else None,
        balance_usd_micros=(
            str(body["balanceUsdMicros"])
            if body.get("balanceUsdMicros") is not None
            else None
        ),
        lifetime_granted_usd_micros=(
            str(body["lifetimeGrantedUsdMicros"])
            if body.get("lifetimeGrantedUsdMicros") is not None
            else None
        ),
    )
    _signer_cache[cache_key] = cached
    return cached


async def authenticate_request(
    settings: Settings,
    authorization: str | None,
) -> tuple[AuthenticatedPrincipal, CachedSignerJwt]:
    token = extract_bearer(authorization)
    session = await exchange_api_key_for_signer_session(settings, token)
    principal = AuthenticatedPrincipal(
        bearer_token=token,
        client_id=settings.pymthouse_public_client_id.strip(),
    )
    return principal, session


def build_sdk_token_payload(
    *,
    api_key: str,
    signer_url: str,
    discovery_url: str | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "signer": signer_url,
        "signer_headers": {
            "Authorization": f"Bearer {api_key}",
        },
    }
    if discovery_url:
        payload["discovery"] = discovery_url
    return payload


def clear_signer_cache() -> None:
    _signer_cache.clear()
