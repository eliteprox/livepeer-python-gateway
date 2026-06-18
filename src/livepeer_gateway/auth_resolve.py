from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlparse

from .errors import LivepeerGatewayError

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class SignerAuthRefreshContext:
    """Inputs to re-mint a signer JWT via API-key exchange."""

    billing_url: str
    signer_url: Optional[str] = None
    scope: Optional[str] = "sign:job"
    api_key: Optional[str] = None


def extract_pmth_api_key_from_signer_headers(
    signer_headers: Optional[dict[str, str]],
) -> Optional[str]:
    """
    Return a PymtHouse ``pmth_*`` API key when ``signer_headers`` carries one.

    Signer JWTs and other bearer types return ``None`` so headers pass through.
    """
    if not signer_headers:
        return None
    auth = signer_headers.get("Authorization")
    if not isinstance(auth, str) or not auth.startswith("Bearer "):
        return None
    token = auth[7:].strip()
    if token.startswith("pmth_"):
        return token
    return None


def billing_origin_from_discovery_url(discovery_url: Optional[str]) -> Optional[str]:
    """Infer Dashboard/NaaP billing origin from a same-host discovery URL."""
    if not discovery_url or not isinstance(discovery_url, str):
        return None
    parsed = urlparse(discovery_url.strip())
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return None


def _extract_signer_access_token(payload: dict[str, Any]) -> str:
    token_obj = payload.get("token")
    if isinstance(token_obj, dict):
        for key in ("accessToken", "access_token"):
            value = token_obj.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    for key in ("accessToken", "access_token"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    raise LivepeerGatewayError(
        "API key exchange response missing signer access token"
    )


def _extract_signer_url(payload: dict[str, Any]) -> Optional[str]:
    for key in ("signerUrl", "signer_url"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def exchange_api_key_via_billing_app(
    billing_url: str,
    api_key: str,
    *,
    scope: Optional[str] = None,
    timeout: float = 15.0,
) -> dict[str, Any]:
    """
    Exchange a PymtHouse ``pmth_*`` key for a signer JWT via the billing app BFF.

    Calls ``POST {billing_url}/api/pymthouse/keys/exchange``.
    """
    from .orchestrator import post_json

    key = api_key.strip()
    if not key:
        raise LivepeerGatewayError("API key exchange requires a non-empty API key")
    url = f"{billing_url.rstrip('/')}/api/pymthouse/keys/exchange"
    body: dict[str, Any] = {"apiKey": key}
    if scope:
        body["scope"] = scope

    data = post_json(url, body, timeout=timeout)
    _extract_signer_access_token(data)
    return data


def refresh_signer_credentials(ctx: SignerAuthRefreshContext) -> dict[str, str]:
    """Re-mint signer bearer via API-key exchange."""
    if not ctx.api_key:
        raise LivepeerGatewayError("Signer credential refresh requires api_key")
    _LOG.info("Refreshing signer credentials (method=api-key)")
    _, headers, _, _ = resolve_signer_auth(
        billing_url=ctx.billing_url,
        signer_url=ctx.signer_url,
        signer_headers=None,
        scope=ctx.scope,
        api_key=ctx.api_key,
    )
    if not headers or not headers.get("Authorization"):
        raise LivepeerGatewayError("Signer credential refresh did not return Authorization")
    return headers


def _discovery_headers_from_signer(
    signer_headers: dict[str, str],
    discovery_url: Optional[str],
    discovery_headers: Optional[dict[str, str]],
) -> Optional[dict[str, str]]:
    if discovery_headers is not None:
        return discovery_headers
    if not discovery_url:
        return None
    if not signer_headers.get("Authorization"):
        return None
    return dict(signer_headers)


def resolve_signer_auth(
    *,
    billing_url: Optional[str] = None,
    signer_url: Optional[str] = None,
    signer_headers: Optional[dict[str, str]] = None,
    discovery_url: Optional[str] = None,
    discovery_headers: Optional[dict[str, str]] = None,
    scope: Optional[str] = "sign:job",
    api_key: Optional[str] = None,
) -> tuple[
    Optional[str],
    Optional[dict[str, str]],
    Optional[str],
    Optional[dict[str, str]],
]:
    """
    Resolve signer and discovery credentials for token / CLI startup.

    When ``api_key`` and ``billing_url`` are set, exchanges via the billing app
    BFF. Otherwise explicit ``signer_headers`` (non-``pmth_*`` JWTs) pass through.
    """
    if signer_headers and signer_headers.get("Authorization"):
        return (
            signer_url,
            signer_headers,
            discovery_url,
            discovery_headers,
        )

    if not billing_url or not api_key:
        return signer_url, signer_headers, discovery_url, discovery_headers

    _LOG.info("API-key signer exchange at %s", billing_url)
    exchange = exchange_api_key_via_billing_app(
        billing_url,
        api_key,
        scope=scope,
    )
    signer_access = _extract_signer_access_token(exchange)
    resolved_signer_url = signer_url or _extract_signer_url(exchange)
    resolved_headers = {"Authorization": f"Bearer {signer_access}"}
    resolved_discovery_headers = _discovery_headers_from_signer(
        resolved_headers,
        discovery_url,
        discovery_headers,
    )
    return (
        resolved_signer_url,
        resolved_headers,
        discovery_url,
        resolved_discovery_headers,
    )
