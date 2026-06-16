"""Shared CLI helpers for Dashboard OIDC device login + signer exchange."""

from __future__ import annotations

import argparse
import os
from typing import Any

from livepeer_gateway.auth_resolve import resolve_issuer_url
from livepeer_gateway.discovery import DEFAULT_DISCOVERY_TIMEOUT

DEFAULT_BILLING_URL = "http://localhost:3001"
DEFAULT_CLIENT_ID = "app_demo"
DEFAULT_STREAM_DURATION_S = 120.0


def add_facade_args(parser: argparse.ArgumentParser, *, dev_defaults: bool = False) -> None:
    billing_default = os.environ.get("LIVEPEER_DASHBOARD_ORIGIN")
    client_default = os.environ.get("LIVEPEER_OIDC_CLIENT_ID")
    if dev_defaults:
        billing_default = billing_default or DEFAULT_BILLING_URL
        client_default = client_default or DEFAULT_CLIENT_ID

    parser.add_argument(
        "--billing-url",
        default=billing_default,
        help=(
            "PymtHouse dashboard origin for OIDC + signer exchange "
            "(issuer defaults to {origin}/api/v1/oidc; override with LIVEPEER_OIDC_ISSUER_URL)"
        ),
    )
    parser.add_argument(
        "--client-id",
        default=client_default,
        dest="client_id",
        help="Public OIDC app client id (app_*)",
    )
    parser.add_argument(
        "--discovery",
        default=None,
        help="Explicit discovery endpoint URL (overrides signer discovery).",
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
        "--api-key",
        default=os.environ.get("LIVEPEER_API_KEY") or os.environ.get("PMTH_API_KEY"),
        dest="api_key",
        help=(
            "PymtHouse API key (pmth_*) for non-interactive bearer exchange instead "
            "of OIDC browser/device login (env: LIVEPEER_API_KEY / PMTH_API_KEY)"
        ),
    )
    parser.add_argument(
        "--clear-token-cache",
        action="store_true",
        help="Delete cached OIDC device-login tokens for this issuer/client before login",
    )


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
    if args.discovery:
        return args.discovery
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
    issuer = resolve_issuer_url(args.billing_url, os.environ.get("LIVEPEER_OIDC_ISSUER_URL"))
    if not args.billing_url or not issuer:
        return {}
    kwargs: dict[str, Any] = {
        "billing_url": args.billing_url,
        "issuer_url": issuer,
        "oidc_client_id": args.client_id,
        "discovery_timeout": args.discovery_timeout,
        "headless": not args.browser,
        "scope": "sign:job openid profile",
    }
    if getattr(args, "clear_token_cache", False):
        kwargs["clear_token_cache"] = True
    api_key = getattr(args, "api_key", None)
    if api_key:
        kwargs["api_key"] = api_key
    signer = resolve_signer_url(args, signer_attr=signer_attr)
    if signer:
        kwargs["signer_url"] = signer
        kwargs["discovery_url"] = resolve_discovery_url(args, model, signer_attr=signer_attr)
    elif args.discovery:
        kwargs["discovery_url"] = args.discovery
    return kwargs
