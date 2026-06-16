from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional

from .errors import LivepeerGatewayError
from .http import post_json_sync

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class SignerAuthRefreshContext:
    """Inputs to re-mint a fresh signer JWT mid-stream via the configured login method."""

    billing_url: str
    issuer_url: Optional[str] = None
    signer_url: Optional[str] = None
    oidc_client_id: Optional[str] = None
    oidc_scopes: str = "openid profile sign:job"
    scope: Optional[str] = "sign:job"
    headless: bool = True
    refresh_skew_seconds: int = 60
    api_key: Optional[str] = None


def refresh_signer_credentials(ctx: SignerAuthRefreshContext) -> dict[str, str]:
    """
    Obtain a new signer bearer via the configured login method + Dashboard exchange.

    Uses the API-key exchange when ``ctx.api_key`` is set, otherwise OIDC
    refresh/login. Does not pass an existing Authorization header so
    ``resolve_signer_auth`` always re-mints credentials.
    """
    _LOG.info(
        "Refreshing signer credentials (method=%s)",
        "api-key" if ctx.api_key else "oidc",
    )
    _, headers, _, _ = resolve_signer_auth(
        billing_url=ctx.billing_url,
        issuer_url=ctx.issuer_url,
        signer_url=ctx.signer_url,
        signer_headers=None,
        oidc_client_id=ctx.oidc_client_id,
        oidc_scopes=ctx.oidc_scopes,
        scope=ctx.scope,
        headless=ctx.headless,
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
    """Reuse signer Authorization on explicit discovery URLs when none were provided."""
    if discovery_headers is not None:
        return discovery_headers
    if not discovery_url:
        return None
    if not signer_headers.get("Authorization"):
        return None
    return dict(signer_headers)


def resolve_issuer_url(
    billing_url: str | None,
    issuer_url: str | None = None,
) -> str | None:
    """
    Resolve OIDC issuer for PymtHouse facade auth.

    When ``issuer_url`` is omitted, infer ``{billing_url}/api/v1/oidc`` for
    dashboard origins (not Keycloak ``/realms/...`` URLs).
    """
    explicit = issuer_url.strip() if isinstance(issuer_url, str) else ""
    if explicit:
        return explicit
    billing = billing_url.strip().rstrip("/") if isinstance(billing_url, str) else ""
    if not billing or "/realms/" in billing:
        return None
    return f"{billing}/api/v1/oidc"


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
        "Dashboard device exchange response missing signer access token"
    )


def _extract_signer_url(payload: dict[str, Any]) -> Optional[str]:
    for key in ("signerUrl", "signer_url"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def exchange_api_key_via_dashboard(
    billing_url: str,
    api_key: str,
    *,
    scope: Optional[str] = None,
    client_id: Optional[str] = None,
    timeout: float = 15.0,
) -> dict[str, Any]:
    """
    Exchange a PymtHouse API key (``pmth_*``) for a signer JWT via the Dashboard BFF.

    Calls ``POST {billing_url}/api/pymthouse/keys/exchange``, which performs the full
    API-key -> user token -> signer JWT exchange server-side (keeping the M2M client
    secret off the client), and returns the full JSON body (signer access token,
    optional ``signerUrl``, balance fields) in the same shape as the device exchange.

    ``client_id`` is sent only when provided; the Dashboard validates it against its
    configured public client, so omit it to defer to the server's configuration.
    """
    key = api_key.strip()
    if not key:
        raise LivepeerGatewayError("API key exchange requires a non-empty API key")
    url = f"{billing_url.rstrip('/')}/api/pymthouse/keys/exchange"
    body: dict[str, Any] = {"apiKey": key}
    if scope:
        body["scope"] = scope
    if client_id:
        body["clientId"] = client_id

    data = post_json_sync(url, body, headers=None, timeout=timeout)
    if not isinstance(data, dict):
        raise LivepeerGatewayError(
            "Dashboard API key exchange returned non-object JSON response"
        )
    _extract_signer_access_token(data)
    return data


def exchange_device_token_via_dashboard(
    billing_url: str,
    device_token: str,
    *,
    scope: Optional[str] = None,
    client_id: Optional[str] = None,
    timeout: float = 15.0,
) -> dict[str, Any]:
    """
    Exchange an OIDC user/device access token for a signer JWT via the facade.

    Calls ``POST {billing_url}/api/signer/device/exchange`` and returns the full
    JSON body (access token, optional ``signerUrl``, balance fields).
    """
    url = f"{billing_url.rstrip('/')}/api/signer/device/exchange"
    body: dict[str, Any] = {"deviceToken": device_token}
    if scope:
        body["scope"] = scope
    if client_id:
        body["clientId"] = client_id

    data = post_json_sync(url, body, headers=None, timeout=timeout)
    if not isinstance(data, dict):
        raise LivepeerGatewayError(
            "Dashboard device exchange returned non-object JSON response"
        )
    _extract_signer_access_token(data)
    return data


def resolve_signer_auth(
    *,
    billing_url: Optional[str] = None,
    issuer_url: Optional[str] = None,
    signer_url: Optional[str] = None,
    signer_headers: Optional[dict[str, str]] = None,
    discovery_url: Optional[str] = None,
    discovery_headers: Optional[dict[str, str]] = None,
    oidc_client_id: Optional[str] = None,
    oidc_scopes: str = "openid profile sign:job",
    scope: Optional[str] = "sign:job",
    headless: bool = True,
    on_device_auth: Optional[Callable[[str, str, int], None]] = None,
    clear_token_cache: bool = False,
    api_key: Optional[str] = None,
) -> tuple[
    Optional[str],
    Optional[dict[str, str]],
    Optional[str],
    Optional[dict[str, str]],
]:
    """
  Resolve signer and discovery credentials.

  When ``billing_url`` is set and no signer bearer is supplied, obtains a signer JWT
  via the Dashboard facade using one of two login methods:

  - ``api_key`` (non-interactive): a single call to
    ``POST {billing_url}/api/pymthouse/keys/exchange`` performs the whole API-key ->
    user token -> signer JWT exchange server-side.
  - OIDC browser/device login (interactive, default): requires a resolvable
    ``issuer_url``, runs the RFC 8628 device flow / browser PKCE login, then
    exchanges the user token via ``POST {billing_url}/api/signer/device/exchange``.

  Both paths return the same response envelope (signer access token + optional
  ``signerUrl``). ``billing_url`` is the Dashboard origin that hosts both endpoints.

  Explicit ``signer_url`` / ``signer_headers`` take precedence and skip login.
  """
    if signer_headers and signer_headers.get("Authorization"):
        return (
            signer_url,
            signer_headers,
            discovery_url,
            _discovery_headers_from_signer(signer_headers, discovery_url, discovery_headers),
        )

    if not billing_url:
        return signer_url, signer_headers, discovery_url, discovery_headers

    if api_key:
        _LOG.info("API-key signer exchange at %s", billing_url)
        signer_exchange = exchange_api_key_via_dashboard(
            billing_url,
            api_key,
            scope=scope,
            client_id=oidc_client_id,
        )
    else:
        resolved_issuer_url = resolve_issuer_url(billing_url, issuer_url)
        if not resolved_issuer_url:
            return signer_url, signer_headers, discovery_url, discovery_headers

        from .oidc_auth import DEFAULT_CLIENT_ID, clear_cached_token, ensure_valid_token

        client_id = oidc_client_id or DEFAULT_CLIENT_ID
        if clear_token_cache:
            clear_cached_token(resolved_issuer_url, client_id=client_id, scopes=oidc_scopes)
            _LOG.info(
                "Cleared OIDC token cache for %s (client_id=%s)",
                resolved_issuer_url,
                client_id,
            )
        _LOG.info("OIDC device login at %s (client_id=%s)", resolved_issuer_url, client_id)
        user_tokens = ensure_valid_token(
            resolved_issuer_url,
            client_id=client_id,
            scopes=oidc_scopes,
            headless=headless,
            on_device_auth=on_device_auth,
        )
        device_access = user_tokens.get("access_token")
        if not isinstance(device_access, str) or not device_access.strip():
            raise LivepeerGatewayError("OIDC login did not return an access token")
        signer_exchange = exchange_device_token_via_dashboard(
            billing_url,
            device_access,
            scope=scope,
            client_id=client_id,
        )

    signer_access = _extract_signer_access_token(signer_exchange)

    resolved_signer_url = signer_url or _extract_signer_url(signer_exchange)
    resolved_headers = {"Authorization": f"Bearer {signer_access}"}
    resolved_discovery_url = discovery_url
    resolved_discovery_headers = _discovery_headers_from_signer(
        resolved_headers,
        resolved_discovery_url,
        discovery_headers,
    )

    return (
        resolved_signer_url,
        resolved_headers,
        resolved_discovery_url,
        resolved_discovery_headers,
    )
