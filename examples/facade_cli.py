"""Shared CLI helpers for Dashboard OIDC device login + signer exchange."""

from __future__ import annotations

import argparse
import os
from typing import Any
from urllib.parse import urlparse

from livepeer_gateway.discovery import DEFAULT_DISCOVERY_TIMEOUT

DEFAULT_BILLING_URL = "http://localhost:3001"
DEFAULT_ISSUER_URL = "http://127.0.0.1:8080/realms/clearinghouse"
DEFAULT_SIGNER_URL = "http://127.0.0.1:8080"
DEFAULT_CLIENT_ID = "app_demo"
DEFAULT_STREAM_DURATION_S = 120.0


def add_facade_args(parser: argparse.ArgumentParser, *, dev_defaults: bool = False) -> None:
    billing_default = os.environ.get("LIVEPEER_DASHBOARD_ORIGIN")
    issuer_default = os.environ.get("LIVEPEER_OIDC_ISSUER_URL")
    client_default = os.environ.get("LIVEPEER_OIDC_CLIENT_ID")
    if dev_defaults:
        billing_default = billing_default or DEFAULT_BILLING_URL
        issuer_default = issuer_default or DEFAULT_ISSUER_URL
        client_default = client_default or DEFAULT_CLIENT_ID

    parser.add_argument(
        "--billing-url",
        default=billing_default,
        help="Dashboard origin for POST /api/signer/device/exchange",
    )
    parser.add_argument(
        "--issuer-url",
        default=issuer_default,
        help="OIDC issuer (Keycloak realm or PymtHouse /api/v1/oidc)",
    )
    parser.add_argument(
        "--client-id",
        default=client_default,
        dest="client_id",
        help="Public OIDC app client id (app_*)",
    )
    parser.add_argument(
        "--discovery-url",
        default=None,
        help="Override discovery URL (default: {signer}/discover-orchestrators?cap=MODEL)",
    )
    parser.add_argument(
        "--discovery-timeout",
        type=float,
        default=float(os.environ.get("LIVEPEER_DISCOVERY_TIMEOUT", DEFAULT_DISCOVERY_TIMEOUT)),
        help="HTTP timeout for discovery (seconds)",
    )
    parser.add_argument(
        "--browser",
        action="store_true",
        help="Use browser PKCE login instead of RFC 8628 device flow",
    )
    parser.add_argument(
        "--clear-token-cache",
        action="store_true",
        help="Delete cached OIDC device-login tokens for this issuer/client before login",
    )


def signer_base_from_issuer(issuer_url: str) -> str:
    """DMZ origin from realm issuer URL (e.g. https://host:8080/realms/... -> https://host:8080)."""
    parsed = urlparse(issuer_url.strip())
    if not parsed.scheme or not parsed.netloc:
        return DEFAULT_SIGNER_URL
    return f"{parsed.scheme}://{parsed.netloc}"


def resolve_signer_url(args: argparse.Namespace, *, signer_attr: str = "signer") -> str | None:
    explicit = getattr(args, signer_attr, None)
    if explicit:
        return explicit
    signer_url = getattr(args, "signer_url", None)
    if signer_url:
        return signer_url
    return os.environ.get("LIVEPEER_SIGNER_BASE_URL")


def resolve_discovery_url(
    args: argparse.Namespace,
    model: str,
    *,
    signer_attr: str = "signer",
) -> str | None:
    if args.discovery_url:
        return args.discovery_url
    base = resolve_signer_url(args, signer_attr=signer_attr)
    if base:
        return f"{base.rstrip('/')}/discover-orchestrators?cap={model}"
    return None


def facade_start_lv2v_kwargs(
    args: argparse.Namespace,
    model: str,
    *,
    signer_attr: str = "signer",
) -> dict[str, Any]:
    """Keyword args for ``start_lv2v`` when Dashboard facade auth is configured."""
    if not args.billing_url or not args.issuer_url:
        return {}
    kwargs: dict[str, Any] = {
        "billing_url": args.billing_url,
        "issuer_url": args.issuer_url,
        "oidc_client_id": args.client_id,
        "discovery_timeout": args.discovery_timeout,
        "headless": not args.browser,
        "scope": "sign:job openid profile",
    }
    if getattr(args, "clear_token_cache", False):
        kwargs["clear_token_cache"] = True
    signer = resolve_signer_url(args, signer_attr=signer_attr)
    if signer:
        kwargs["signer_url"] = signer
        kwargs["discovery_url"] = resolve_discovery_url(args, model, signer_attr=signer_attr)
    elif args.discovery_url:
        kwargs["discovery_url"] = args.discovery_url
    return kwargs
