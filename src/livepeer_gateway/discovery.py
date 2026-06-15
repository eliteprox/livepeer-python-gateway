from __future__ import annotations

import logging
from typing import Optional, Sequence
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

from . import lp_rpc_pb2
from .capabilities import capabilities_to_query
from .errors import LivepeerGatewayError
from .remote_signer import RemoteSignerError
from .http import _http_origin, _parse_http_url, get_json_sync

_LOG = logging.getLogger(__name__)

# NaaP / multi-tenant discovery can take 15–30s; keep above typical gateway defaults.
DEFAULT_DISCOVERY_TIMEOUT = 60.0


def _append_caps(url: str, capabilities: Optional[lp_rpc_pb2.Capabilities]) -> str:
    """
    Append repeated `caps` query parameters to a URL.

    Existing query params are preserved. Capability values keep `/` unescaped.
    Caps are sent in the gateway-native ``pipeline/model`` form
    (e.g. ``live-video-to-video/streamdiffusion-sdxl``); discovery backends are
    responsible for matching their own materialized capability names.
    """
    if capabilities is None:
        return url
    cap_values = capabilities_to_query(capabilities)
    if not cap_values:
        return url

    parsed = urlparse(url)
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    query_pairs.extend(("caps", cap) for cap in cap_values)
    query = urlencode(query_pairs, doseq=True, quote_via=quote, safe="/")
    return urlunparse(parsed._replace(query=query))


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
        discovery_endpoint = _parse_http_url(discovery_url).geturl()
        request_headers = discovery_headers if discovery_headers is not None else signer_headers
    elif signer_url:
        discovery_endpoint = f"{_http_origin(signer_url)}/discover-orchestrators"
        request_headers = signer_headers
    else:
        _LOG.debug("discover_orchestrators failed: no discovery inputs")
        raise LivepeerGatewayError(
            "discover_orchestrators requires discovery_url or signer_url",
        )

    if capabilities is not None:
        discovery_endpoint = _append_caps(discovery_endpoint, capabilities)

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
