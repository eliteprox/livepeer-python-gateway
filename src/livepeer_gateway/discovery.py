from __future__ import annotations

import logging
import os
from typing import Any, Optional, Sequence
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

from . import lp_rpc_pb2
from .capabilities import capabilities_to_query
from .errors import LivepeerGatewayError
from .remote_signer import RemoteSignerError
from .http import _http_origin, _parse_http_url, get_json_sync

_LOG = logging.getLogger(__name__)

# NaaP / multi-tenant discovery can take 15–30s; keep above typical gateway defaults.
DEFAULT_DISCOVERY_TIMEOUT = 60.0
DISCOVERY_SERVICE_RAW_PATH = "/v1/discovery/raw"
DEFAULT_DISCOVERY_SERVICE_TYPE = "legacy"


def read_discovery_service_url() -> Optional[str]:
    """Base URL for the materialized Discovery Service (Railway / self-hosted)."""
    raw = os.environ.get("LIVEPEER_DISCOVERY_SERVICE_URL", "").strip()
    return raw or None


def discovery_service_type() -> str:
    return os.environ.get("LIVEPEER_DISCOVERY_SERVICE_TYPE", DEFAULT_DISCOVERY_SERVICE_TYPE).strip() or DEFAULT_DISCOVERY_SERVICE_TYPE


def discovery_service_capability_name(cap: str) -> str:
    """
    Map gateway ``pipeline/model`` caps to Discovery Service capability keys.

    The materialized dataset indexes models such as ``streamdiffusion-sdxl``,
    not ``live-video-to-video/streamdiffusion-sdxl``.
    """
    value = cap.strip()
    if not value:
        return value
    if "/" in value:
        return value.rsplit("/", 1)[-1]
    return value


def is_discovery_service_endpoint(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    if path.endswith(DISCOVERY_SERVICE_RAW_PATH):
        return True
    base = read_discovery_service_url()
    if base:
        base_parsed = urlparse(base.rstrip("/"))
        if parsed.netloc == base_parsed.netloc and parsed.scheme == base_parsed.scheme:
            return True
    return False


def normalize_discovery_service_url(
    url: str,
    *,
    service_type: Optional[str] = None,
) -> str:
    """
    Resolve a Discovery Service base or partial path to the webhook-compatible raw endpoint.

    See https://discovery-service-production-8955.up.railway.app/docs — ``GET /v1/discovery/raw``.
    """
    parsed = urlparse(url.strip())
    path = parsed.path.rstrip("/")
    if path.endswith(DISCOVERY_SERVICE_RAW_PATH):
        resolved_path = path
    elif path.endswith("/v1/discovery"):
        resolved_path = DISCOVERY_SERVICE_RAW_PATH
    elif not path:
        resolved_path = DISCOVERY_SERVICE_RAW_PATH
    else:
        return url

    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    service = service_type or discovery_service_type()
    if service and not any(key == "serviceType" for key, _ in query_pairs):
        query_pairs.append(("serviceType", service))
    query = urlencode(query_pairs, doseq=True, quote_via=quote, safe="/")
    return urlunparse(parsed._replace(path=resolved_path, query=query))


def resolve_discovery_endpoint(
    discovery_url: str,
    *,
    service_type: Optional[str] = None,
) -> tuple[str, bool]:
    """
    Return (endpoint_url, uses_discovery_service_api).

    Cloudspe / signer webhook URLs are returned unchanged.
    """
    if is_discovery_service_endpoint(discovery_url) or _looks_like_discovery_service_base(discovery_url):
        return (
            normalize_discovery_service_url(discovery_url, service_type=service_type),
            True,
        )
    return discovery_url, False


def _looks_like_discovery_service_base(url: str) -> bool:
    base = read_discovery_service_url()
    if base and url.rstrip("/").startswith(base.rstrip("/")):
        return True
    host = urlparse(url).hostname or ""
    return "discovery-service" in host


def _append_query_values(url: str, values: Sequence[tuple[str, str]]) -> str:
    if not values:
        return url

    parsed = urlparse(url)
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    query_pairs.extend(values)
    query = urlencode(query_pairs, doseq=True, quote_via=quote, safe="/")
    return urlunparse(parsed._replace(query=query))


def _append_caps(url: str, capabilities: Optional[lp_rpc_pb2.Capabilities]) -> str:
    """
    Append repeated `caps` query parameters to a URL.

    Existing query params are preserved. Capability values keep `/` unescaped.
    """
    if capabilities is None:
        return url
    return _append_cap_strings(url, capabilities_to_query(capabilities))


def _append_cap_strings(url: str, cap_values: Sequence[str], *, discovery_service: bool = False) -> str:
    if not cap_values:
        return url
    values = cap_values
    if discovery_service:
        values = [discovery_service_capability_name(cap) for cap in cap_values]
    return _append_query_values(url, [("caps", cap) for cap in values])


def discover_orchestrators(
    orchestrators: Optional[Sequence[str] | str] = None,
    *,
    signer_url: Optional[str] = None,
    signer_headers: Optional[dict[str, str]] = None,
    discovery_url: Optional[str] = None,
    discovery_headers: Optional[dict[str, str]] = None,
    capabilities: Optional[lp_rpc_pb2.Capabilities] = None,
    timeout: float = DEFAULT_DISCOVERY_TIMEOUT,
) -> list[str]:
    """
    Discover orchestrators and return a list of addresses.

    This discovery can happen via the following parameters in priority order (highest first):
    - orchestrators: list or comma-delimited string
      (empty/whitespace-only input falls through)
    - discovery_url: use this discovery endpoint
    - signer_url: use signer-provided discovery service
    """
    if orchestrators is not None:
        if isinstance(orchestrators, str):
            orch_list = [orch.strip() for orch in orchestrators.split(",")]
        else:
            try:
                orch_list = list(orchestrators)
            except TypeError as e:
                raise LivepeerGatewayError(
                    "discover_orchestrators requires a list of orchestrator URLs or a comma-delimited string"
                ) from e
        orch_list = [orch.strip() for orch in orch_list if isinstance(orch, str) and orch.strip()]
        if orch_list:
            return orch_list

    if discovery_url:
        discovery_endpoint, uses_discovery_service = resolve_discovery_endpoint(discovery_url)
        discovery_endpoint = _parse_http_url(discovery_endpoint).geturl()
        request_headers = discovery_headers if discovery_headers is not None else signer_headers
    elif read_discovery_service_url():
        discovery_endpoint, uses_discovery_service = resolve_discovery_endpoint(
            read_discovery_service_url() or "",
        )
        discovery_endpoint = _parse_http_url(discovery_endpoint).geturl()
        request_headers = discovery_headers if discovery_headers is not None else signer_headers
    elif signer_url:
        discovery_endpoint = f"{_http_origin(signer_url)}/discover-orchestrators"
        uses_discovery_service = False
        request_headers = signer_headers
    else:
        _LOG.debug("discover_orchestrators failed: no discovery inputs")
        raise LivepeerGatewayError(
            "discover_orchestrators requires discovery_url, LIVEPEER_DISCOVERY_SERVICE_URL, or signer_url",
        )

    if capabilities is not None:
        cap_values = capabilities_to_query(capabilities)
        discovery_endpoint = _append_cap_strings(
            discovery_endpoint,
            cap_values,
            discovery_service=uses_discovery_service,
        )

    try:
        _LOG.debug(
            "discover_orchestrators running discovery: %s (timeout=%ss)",
            discovery_endpoint,
            timeout,
        )
        data = get_json_sync(discovery_endpoint, headers=request_headers, timeout=timeout)
    except LivepeerGatewayError as e:
        _LOG.debug("discover_orchestrators discovery failed: %s", e)
        raise RemoteSignerError(
            discovery_endpoint,
            str(e),
            cause=e.__cause__ or e,
        ) from None

    if not isinstance(data, list):
        _LOG.debug(
            "discover_orchestrators discovery response not list: type=%s",
            type(data).__name__,
        )
        raise RemoteSignerError(
            discovery_endpoint,
            f"Discovery response must be a JSON list, got {type(data).__name__}",
            cause=None,
        ) from None

    _LOG.debug("discover_orchestrators discovery response: %s", data)

    orch_list = []
    for item in data:
        if not isinstance(item, dict):
            continue
        address = item.get("address")
        if isinstance(address, str) and address.strip():
            orch_list.append(address.strip())
    _LOG.debug("discover_orchestrators discovered %d orchestrators", len(orch_list))

    return orch_list
