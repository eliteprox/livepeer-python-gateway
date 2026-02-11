from __future__ import annotations

import base64
import json
import logging
import ssl
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Optional, Sequence, Tuple
from urllib.parse import ParseResult, urlparse
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

from . import lp_rpc_pb2

from .capabilities import (
    CAPABILITY_BYOC,
    CAPABILITY_LIVE_VIDEO_TO_VIDEO,
    build_capabilities,
)
from .errors import (
    LivepeerGatewayError,
    NoOrchestratorAvailableError,
    SignerRefreshRequired,
    SkipPaymentCycle,
)
from .orch_info import get_orch_info, create_orchestrator_stub, call_get_orchestrator
from .remote_signer import RemoteSignerError, get_orch_info_sig

_LOG = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HTTP helpers (from main's refactor)
# ---------------------------------------------------------------------------

def _truncate(s: str, max_len: int = 2000) -> str:
    if len(s) <= max_len:
        return s
    return s[:max_len] + f"...(+{len(s) - max_len} chars)"

def _http_error_body(e: HTTPError) -> str:
    """
    Best-effort read of an HTTPError response body for debugging.
    """
    try:
        b = e.read()
        if not b:
            return ""
        if isinstance(b, bytes):
            return b.decode("utf-8", errors="replace")
        return str(b)
    except Exception:
        return ""

def _extract_error_message(e: HTTPError) -> str:
    """
    Best-effort extraction of a useful error message from an HTTPError body.

    If the body is JSON and matches {"error": {"message": "..."}}, return that message.
    Otherwise return the full body.

    Always truncates the returned value for readability.
    """
    body = _http_error_body(e)
    s = body.strip()
    if not s:
        return ""

    try:
        data = json.loads(s)
    except Exception:
        return _truncate(body)

    if isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, dict):
            msg = err.get("message")
            if isinstance(msg, str) and msg:
                return _truncate(msg)

    return _truncate(body)


def request_json(
    url: str,
    *,
    method: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
    headers: Optional[dict[str, str]] = None,
    timeout: float = 5.0,
) -> Any:
    """
    Make a JSON HTTP request and parse the JSON response.

    If method is None, defaults to POST when payload is provided, otherwise GET.

    Raises LivepeerGatewayError on HTTP/network/JSON parsing errors.
    """
    req_headers: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": "livepeer-python-gateway/0.1",
    }
    body: Optional[bytes] = None
    if payload is not None:
        req_headers["Content-Type"] = "application/json"
        body = json.dumps(payload).encode("utf-8")
    if headers:
        req_headers.update(headers)

    resolved_method = method.upper() if method else ("POST" if payload is not None else "GET")
    req = Request(url, data=body, headers=req_headers, method=resolved_method)

    # Always ignore HTTPS certificate validation (matches our gRPC behavior).
    ssl_ctx = ssl._create_unverified_context()

    try:
        with urlopen(req, timeout=timeout, context=ssl_ctx) as resp:
            raw = resp.read().decode("utf-8")
        data: Any = json.loads(raw)
    except HTTPError as e:
        body = _extract_error_message(e)
        body_part = f"; body={body!r}" if body else ""
        if e.code == 480:
            raise SignerRefreshRequired(
                f"Signer returned HTTP 480 (refresh session required) (url={url}){body_part}"
            ) from e
        if e.code == 482:
            raise SkipPaymentCycle(
                f"Signer returned HTTP 482 (skip payment cycle) (url={url}){body_part}"
            ) from e
        raise LivepeerGatewayError(
            f"HTTP JSON error: HTTP {e.code} from endpoint (url={url}){body_part}"
        ) from e
    except ConnectionRefusedError as e:
        raise LivepeerGatewayError(
            f"HTTP JSON error: connection refused (is the server running? is the host/port correct?) (url={url})"
        ) from e
    except URLError as e:
        raise LivepeerGatewayError(
            f"HTTP JSON error: failed to reach endpoint: {getattr(e, 'reason', e)} (url={url})"
        ) from e
    except json.JSONDecodeError as e:
        raise LivepeerGatewayError(f"HTTP JSON error: endpoint did not return valid JSON: {e} (url={url})") from e
    except Exception as e:
        raise LivepeerGatewayError(
            f"HTTP JSON error: unexpected error: {e.__class__.__name__}: {e} (url={url})"
        ) from e

    return data


def post_json(
    url: str,
    payload: dict[str, Any],
    *,
    headers: Optional[dict[str, str]] = None,
    timeout: float = 5.0,
) -> dict[str, Any]:
    """
    POST JSON to `url` and parse a JSON object response.
    """
    data = request_json(
        url,
        payload=payload,
        headers=headers,
        timeout=timeout,
    )
    if not isinstance(data, dict):
        raise LivepeerGatewayError(
            f"HTTP JSON error: expected JSON object, got {type(data).__name__} (url={url})"
        )
    return data


def get_json(
    url: str,
    *,
    headers: Optional[dict[str, str]] = None,
    timeout: float = 5.0,
) -> Any:
    """
    GET JSON from `url` and parse the response.
    """
    return request_json(url, headers=headers, timeout=timeout)


# ---------------------------------------------------------------------------
# URL normalization helpers
# ---------------------------------------------------------------------------

def _parse_http_url(url: str, *, context: str = "URL") -> ParseResult:
    """
    Normalize a URL for HTTP(S) endpoints.

    Accepts:
    - "host:port" (implicitly https://host:port)
    - "http://host:port[/...]"
    - "https://host:port[/...]"
    """
    url = url.strip()
    normalized = url if "://" in url else f"https://{url}"
    parsed = urlparse(normalized)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Only http:// or https:// {context}s are supported (got {parsed.scheme!r})")
    if not parsed.netloc:
        raise ValueError(f"Invalid {context}: {url!r}")
    return parsed


def _http_origin(url: str) -> str:
    """
    Normalize a URL (possibly with a path) into a scheme:// origin (scheme + host:port).

    Accepts:
    - "host:port" (implicitly https://host:port)
    - "http://host:port[/...]" (path/query/fragment are ignored)
    - "https://host:port[/...]" (path/query/fragment are ignored)
    """
    parsed = _parse_http_url(url)
    return f"{parsed.scheme}://{parsed.netloc}"


# Feature branch compatibility aliases
_normalize_https_base_url = _http_origin
_normalize_https_origin = _http_origin


# ---------------------------------------------------------------------------
# Orchestrator discovery and selection (from main's refactor)
# ---------------------------------------------------------------------------

def DiscoverOrchestrators(
    orchestrators: Optional[Sequence[str] | str] = None,
    *,
    signer_url: Optional[str] = None,
    discovery_url: Optional[str] = None,
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
                    "DiscoverOrchestrators requires a list of orchestrator URLs or a comma-delimited string"
                ) from e
        orch_list = [orch.strip() for orch in orch_list if isinstance(orch, str) and orch.strip()]
        if orch_list:
            return orch_list

    if discovery_url:
        discovery_endpoint = _parse_http_url(discovery_url).geturl()
    elif signer_url:
        discovery_endpoint = f"{_http_origin(signer_url)}/discover-orchestrators"
    else:
        _LOG.debug("DiscoverOrchestrators failed: no discovery inputs")
        raise LivepeerGatewayError("DiscoverOrchestrators requires discovery_url or signer_url")

    try:
        _LOG.debug("DiscoverOrchestrators running discovery: %s", discovery_endpoint)
        data = get_json(discovery_endpoint)
    except LivepeerGatewayError as e:
        _LOG.debug("DiscoverOrchestrators discovery failed: %s", e)
        raise RemoteSignerError(
            discovery_endpoint,
            str(e),
            cause=e.__cause__ or e,
        ) from None

    if not isinstance(data, list):
        _LOG.debug(
            "DiscoverOrchestrators discovery response not list: type=%s",
            type(data).__name__,
        )
        raise RemoteSignerError(
            discovery_endpoint,
            f"Discovery response must be a JSON list, got {type(data).__name__}",
            cause=None,
        ) from None

    _LOG.debug("DiscoverOrchestrators discovery response: %s", data)

    orch_list = []
    for item in data:
        if not isinstance(item, dict):
            continue
        address = item.get("address")
        if isinstance(address, str) and address.strip():
            orch_list.append(address.strip())
    _LOG.debug("DiscoverOrchestrators discovered %d orchestrators", len(orch_list))

    return orch_list


def SelectOrchestrator(
    orchestrators: Optional[Sequence[str] | str] = None,
    *,
    signer_url: Optional[str] = None,
    discovery_url: Optional[str] = None,
    capabilities: Optional[lp_rpc_pb2.Capabilities] = None,
) -> Tuple[str, lp_rpc_pb2.OrchestratorInfo]:
    """
    Select an orchestrator by trying up to ~5 candidates in parallel.

    If orchestrators is empty/None, a discovery endpoint is used:
    - discovery_url, if provided
    - otherwise {signer_url}/discover-orchestrators
    """
    orch_list = DiscoverOrchestrators(
        orchestrators,
        signer_url=signer_url,
        discovery_url=discovery_url,
    )

    if not orch_list:
        _LOG.debug("SelectOrchestrator failed: empty orchestrator list")
        raise NoOrchestratorAvailableError("No orchestrators available to select")

    candidates = orch_list[:5]

    _LOG.debug("SelectOrchestrator trying candidates: %s", candidates)
    with ThreadPoolExecutor(max_workers=len(candidates)) as executor:
        futures = {
            executor.submit(
                get_orch_info,
                url,
                signer_url=signer_url,
                capabilities=capabilities,
            ): url
            for url in candidates
        }

        for future in as_completed(futures):
            url = futures[future]
            try:
                info = future.result()
            except LivepeerGatewayError as e:
                _LOG.debug("SelectOrchestrator candidate failed: %s (%s)", url, e)
                continue
            _LOG.debug("SelectOrchestrator selected: %s", url)
            return url, info

    _LOG.debug("SelectOrchestrator failed: all candidates errored")
    raise NoOrchestratorAvailableError("All orchestrators failed")


# ---------------------------------------------------------------------------
# BYOC payment types (from feature branch)
# ---------------------------------------------------------------------------

@dataclass
class PaymentState:
    """
    Opaque state blob returned by the remote signer that must be sent with
    subsequent payment requests to ensure unique ticket nonces.
    """
    state: Optional[bytes] = None
    sig: Optional[bytes] = None

    def to_dict(self) -> dict[str, Any]:
        if self.state is None and self.sig is None:
            return {}
        result = {}
        if self.state is not None:
            result["State"] = base64.b64encode(self.state).decode("ascii")
        if self.sig is not None:
            result["Sig"] = base64.b64encode(self.sig).decode("ascii")
        return result

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "PaymentState":
        state_b64 = data.get("State")
        sig_b64 = data.get("Sig")
        return PaymentState(
            state=base64.b64decode(state_b64) if state_b64 else None,
            sig=base64.b64decode(sig_b64) if sig_b64 else None,
        )


@dataclass(frozen=True)
class GetPaymentResponse:
    payment: str
    seg_creds: Optional[str] = None
    state: Optional[PaymentState] = None


# ---------------------------------------------------------------------------
# Pricing selection (from feature branch, supports LV2V + BYOC)
# ---------------------------------------------------------------------------

def _select_price_info(
    info: lp_rpc_pb2.OrchestratorInfo,
    *,
    typ: str,
    model_id: Optional[str],
    capability: Optional[str] = None,
) -> lp_rpc_pb2.PriceInfo:
    """
    Choose the price info to use for a payment request.

    For LV2V, prefer a capability-scoped price matching the requested model ID (cap 35).
    For BYOC, look up byoc pricing matching the capability name (cap 37).
    Fallback to the general price_info only if no matching capability price exists.
    """
    if typ == "lv2v":
        if not model_id:
            raise LivepeerGatewayError("GetPayment requires model_id for LV2V pricing.")
        for pi in info.capabilities_prices:
            if (
                pi.capability == CAPABILITY_LIVE_VIDEO_TO_VIDEO
                and pi.pricePerUnit > 0
                and pi.pixelsPerUnit > 0
                and pi.constraint == model_id
            ):
                return pi
        if info.HasField("price_info") and info.price_info.pricePerUnit > 0 and info.price_info.pixelsPerUnit > 0:
            return info.price_info
        raise LivepeerGatewayError(
            f"No capability price found for LV2V model_id={model_id}; orchestrator did not return usable pricing."
        )

    if typ == "byoc":
        if not capability:
            raise LivepeerGatewayError("GetPayment requires capability for BYOC pricing.")
        for pi in info.capabilities_prices:
            if (
                pi.capability == CAPABILITY_BYOC
                and pi.pricePerUnit > 0
                and pi.pixelsPerUnit > 0
                and pi.constraint == capability
            ):
                return pi
        raise LivepeerGatewayError(
            f"No capability price found for BYOC capability={capability}; "
            "orchestrator did not return usable pricing."
        )

    if info.HasField("price_info") and info.price_info.pricePerUnit > 0 and info.price_info.pixelsPerUnit > 0:
        return info.price_info
    for pi in info.capabilities_prices:
        if pi.pricePerUnit > 0 and pi.pixelsPerUnit > 0:
            return pi
    raise LivepeerGatewayError("Orchestrator did not return usable pricing information.")


def _apply_ticket_params(
    info: lp_rpc_pb2.OrchestratorInfo,
    ticket_params: dict[str, Any],
) -> None:
    """Apply ticket params from a JobToken response to OrchestratorInfo."""
    if not ticket_params:
        return

    def decode_b64(val: Optional[str]) -> bytes:
        if val is None:
            return b""
        return base64.b64decode(val)

    tp = info.ticket_params

    if "recipient" in ticket_params:
        tp.recipient = decode_b64(ticket_params["recipient"])
    if "face_value" in ticket_params:
        tp.face_value = decode_b64(ticket_params["face_value"])
    if "win_prob" in ticket_params:
        tp.win_prob = decode_b64(ticket_params["win_prob"])
    if "recipient_rand_hash" in ticket_params:
        tp.recipient_rand_hash = decode_b64(ticket_params["recipient_rand_hash"])
    if "seed" in ticket_params:
        tp.seed = decode_b64(ticket_params["seed"])
    if "expiration_block" in ticket_params:
        tp.expiration_block = decode_b64(ticket_params["expiration_block"])

    if "expiration_params" in ticket_params and ticket_params["expiration_params"]:
        exp = ticket_params["expiration_params"]
        if "creation_round" in exp:
            tp.expiration_params.creation_round = exp["creation_round"]
        if "creation_round_block_hash" in exp:
            tp.expiration_params.creation_round_block_hash = decode_b64(
                exp["creation_round_block_hash"]
            )
        if "creation_round_initialized" in exp:
            tp.expiration_params.creation_round_initialized = exp[
                "creation_round_initialized"
            ]


# ---------------------------------------------------------------------------
# GetPayment (from feature branch, supports LV2V + BYOC)
# ---------------------------------------------------------------------------

def GetPayment(
    signer_base_url: str,
    info: lp_rpc_pb2.OrchestratorInfo,
    *,
    typ: str = "lv2v",
    model_id: Optional[str] = None,
    capability: Optional[str] = None,
    state: Optional[PaymentState] = None,
    manifest_id: Optional[str] = None,
    price_per_unit: Optional[int] = None,
    pixels_per_unit: Optional[int] = None,
    ticket_params: Optional[dict[str, Any]] = None,
) -> GetPaymentResponse:
    """
    Call the remote signer to generate an automatic payment for a job.
    """
    if typ == "lv2v" and not model_id:
        raise LivepeerGatewayError(
            "GetPayment requires model_id when requesting LV2V payments."
        )
    if typ == "byoc" and not capability:
        raise LivepeerGatewayError(
            "GetPayment requires capability when requesting BYOC payments."
        )

    if not signer_base_url:
        seg = lp_rpc_pb2.SegData()
        if not info.HasField("auth_token"):
            raise LivepeerGatewayError(
                "Orchestrator did not provide an auth token."
            )
        seg.auth_token.CopyFrom(info.auth_token)
        seg = base64.b64encode(seg.SerializeToString()).decode("ascii")
        return GetPaymentResponse(seg_creds=seg, payment="")

    if price_per_unit is not None and pixels_per_unit is not None:
        price_info = lp_rpc_pb2.PriceInfo(
            pricePerUnit=price_per_unit,
            pixelsPerUnit=pixels_per_unit,
        )
    else:
        price_info = _select_price_info(info, typ=typ, model_id=model_id, capability=capability)

    if price_info.pricePerUnit <= 0 or price_info.pixelsPerUnit <= 0:
        raise LivepeerGatewayError(
            f"Selected price_info has zero values: pricePerUnit={price_info.pricePerUnit}, "
            f"pixelsPerUnit={price_info.pixelsPerUnit}, model_id={model_id}"
        )

    info_for_payment = lp_rpc_pb2.OrchestratorInfo()
    info_for_payment.CopyFrom(info)
    info_for_payment.price_info.pricePerUnit = price_info.pricePerUnit
    info_for_payment.price_info.pixelsPerUnit = price_info.pixelsPerUnit
    info_for_payment.price_info.capability = price_info.capability
    info_for_payment.price_info.constraint = price_info.constraint

    if ticket_params is not None:
        _apply_ticket_params(info_for_payment, ticket_params)

    if info_for_payment.price_info.pricePerUnit <= 0 or info_for_payment.price_info.pixelsPerUnit <= 0:
        raise LivepeerGatewayError(
            f"Failed to set price_info on OrchestratorInfo: pricePerUnit={info_for_payment.price_info.pricePerUnit}, "
            f"pixelsPerUnit={info_for_payment.price_info.pixelsPerUnit}"
        )

    base = _http_origin(signer_base_url)
    url = f"{base}/generate-live-payment"

    pb = info_for_payment.SerializeToString()
    orch_b64 = base64.b64encode(pb).decode("ascii")

    cap_id = price_info.capability
    if typ == "lv2v" and cap_id == 0:
        cap_id = CAPABILITY_LIVE_VIDEO_TO_VIDEO
    constraint = price_info.constraint or (model_id or "")

    payload: dict[str, Any] = {
        "orchestrator": orch_b64,
        "priceInfo": {
            "pricePerUnit": price_info.pricePerUnit,
            "pixelsPerUnit": price_info.pixelsPerUnit,
            "capability": cap_id,
            "constraint": constraint,
        },
        "type": typ,
    }

    if typ == "byoc" and capability:
        payload["capability"] = capability

    if state is not None:
        state_dict = state.to_dict()
        if state_dict:
            payload["state"] = state_dict

    if manifest_id:
        payload["manifestId"] = manifest_id

    data = post_json(url, payload)

    payment = data.get("payment")
    if not isinstance(payment, str) or not payment:
        raise LivepeerGatewayError(f"GetPayment error: missing/invalid 'payment' in response (url={url})")

    seg_creds = data.get("segCreds")
    if seg_creds is not None and not isinstance(seg_creds, str):
        raise LivepeerGatewayError(f"GetPayment error: invalid 'segCreds' in response (url={url})")

    new_state = None
    state_data = data.get("state")
    if isinstance(state_data, dict):
        new_state = PaymentState.from_dict(state_data)

    return GetPaymentResponse(payment=payment, seg_creds=seg_creds, state=new_state)


# ---------------------------------------------------------------------------
# Job helpers (from feature branch)
# ---------------------------------------------------------------------------

def _start_job_with_headers(
    info: lp_rpc_pb2.OrchestratorInfo,
    req,
    headers: dict[str, Optional[str]],
):
    """Internal helper to start a job with pre-built headers."""
    from .lv2v import LiveVideoToVideo

    base = _http_origin(info.transcoder)
    url = f"{base}/live-video-to-video"
    data = post_json(url, req.to_json(), headers=headers)
    return LiveVideoToVideo.from_json(data)


def StartJob(
    info: lp_rpc_pb2.OrchestratorInfo,
    req,
    *,
    signer_base_url: Optional[str] = None,
    typ: str = "lv2v",
):
    """
    Start a live video-to-video job.
    """
    if not req.model_id:
        raise LivepeerGatewayError("StartJob requires model_id")

    p = GetPayment(signer_base_url, info, model_id=req.model_id)
    headers: dict[str, Optional[str]] = {
        "Livepeer-Payment": p.payment,
        "Livepeer-Segment": p.seg_creds,
    }

    return _start_job_with_headers(info, req, headers)


# ---------------------------------------------------------------------------
# OrchestratorClient (from feature branch)
# ---------------------------------------------------------------------------

class OrchestratorClient:
    def __init__(
        self,
        orch_url: str,
        *,
        signer_url: Optional[str] = None,
    ) -> None:
        self.orch_url = orch_url
        self.signer_url = signer_url

        _, stub = create_orchestrator_stub(orch_url)
        self._stub = stub

    def GetOrchestratorInfo(
        self,
        *,
        caps: Optional[lp_rpc_pb2.Capabilities] = None,
    ) -> lp_rpc_pb2.OrchestratorInfo:
        from .orch_info import OrchestratorRpcError

        try:
            signer = get_orch_info_sig(self.signer_url)
        except Exception as e:
            raise OrchestratorRpcError(
                self.orch_url,
                f"{e.__class__.__name__}: {e}",
                cause=e,
            ) from None

        request = lp_rpc_pb2.OrchestratorRequest(
            address=signer.address,
            sig=signer.sig,
            capabilities=caps,
            ignoreCapacityCheck=True,
        )

        return call_get_orchestrator(self._stub, request, self.orch_url)


def GetOrchestratorInfo(
    orch_url: str,
    *,
    signer_url: Optional[str] = None,
    caps: Optional[lp_rpc_pb2.Capabilities] = None,
    typ: str = "lv2v",
    model_id: Optional[str] = None,
) -> lp_rpc_pb2.OrchestratorInfo:
    """
    Public functional API for fetching orchestrator info.
    """
    effective_caps = caps
    if typ == "lv2v" and model_id:
        effective_caps = build_capabilities(CAPABILITY_LIVE_VIDEO_TO_VIDEO, model_id)
    return OrchestratorClient(orch_url, signer_url=signer_url).GetOrchestratorInfo(caps=effective_caps)
