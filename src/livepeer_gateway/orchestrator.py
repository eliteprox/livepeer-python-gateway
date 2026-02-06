from __future__ import annotations

import asyncio
import json
import logging
import ssl
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Optional, Sequence, Tuple
from urllib.parse import urlparse
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

from . import lp_rpc_pb2

from .capabilities import CapabilityId, build_capabilities
from .control import Control
from .events import Events
from .media_publish import MediaPublish, MediaPublishConfig
from .media_output import MediaOutput
from .errors import LivepeerGatewayError, NoOrchestratorAvailableError, SignerRefreshRequired
from .orch_info import get_orch_info
from .payments import PaymentSession, RemoteSignerError

_LOG = logging.getLogger(__name__)

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


@dataclass(frozen=True)
class StartJobRequest:
    # The ID of the Gateway request (for logging purposes).
    request_id: Optional[str] = None
    # ModelId Name of the pipeline to run in the live video to video job.
    model_id: Optional[str] = None
    # Params Initial parameters for the pipeline.
    params: Optional[dict[str, Any]] = None
    # StreamId The Stream ID (for logging purposes).
    stream_id: Optional[str] = None

    def to_json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.request_id is not None:
            payload["gateway_request_id"] = self.request_id
        if self.model_id is not None:
            payload["model_id"] = self.model_id
        if self.params is not None:
            payload["params"] = self.params
        if self.stream_id is not None:
            payload["stream_id"] = self.stream_id
        return payload


@dataclass(frozen=True)
class LiveVideoToVideo:
    raw: dict[str, Any]
    manifest_id: Optional[str] = None
    publish_url: Optional[str] = None
    subscribe_url: Optional[str] = None
    control_url: Optional[str] = None
    events_url: Optional[str] = None
    orchestrator_info: Optional[lp_rpc_pb2.OrchestratorInfo] = None
    control: Optional[Control] = None
    events: Optional[Events] = None
    _media: Optional[MediaPublish] = field(default=None, repr=False, compare=False)
    _payment_session: Optional["PaymentSession"] = field(default=None, repr=False, compare=False)

    @staticmethod
    def from_json(
        data: dict[str, Any],
        *,
        orchestrator_info: Optional[lp_rpc_pb2.OrchestratorInfo] = None,
        payment_session: Optional["PaymentSession"] = None,
    ) -> "LiveVideoToVideo":
        control_url = data.get("control_url") if isinstance(data.get("control_url"), str) else None
        control = Control(control_url) if control_url else None
        publish_url = data.get("publish_url") if isinstance(data.get("publish_url"), str) else None
        events_url = data.get("events_url") if isinstance(data.get("events_url"), str) else None
        events = Events(events_url) if events_url else None
        return LiveVideoToVideo(
            raw=data,
            control_url=control_url,
            events_url=events_url,
            manifest_id=data.get("manifest_id") if isinstance(data.get("manifest_id"), str) else None,
            publish_url=publish_url,
            subscribe_url=data.get("subscribe_url") if isinstance(data.get("subscribe_url"), str) else None,
            orchestrator_info=orchestrator_info,
            control=control,
            events=events,
            _payment_session=payment_session,
        )

    def start_media(self, config: MediaPublishConfig) -> MediaPublish:
        """
        Instantiate and return a MediaPublish helper for this job.
        """
        if not self.publish_url:
            raise LivepeerGatewayError("No publish_url present on this LiveVideoToVideo job")
        if self._media is None:
            media = MediaPublish(
                self.publish_url,
                mime_type=config.mime_type,
                keyframe_interval_s=config.keyframe_interval_s,
                fps=config.fps,
            )
            object.__setattr__(self, "_media", media)
        return self._media

    def media_output(
        self,
        *,
        start_seq: int = -2,
        max_retries: int = 5,
        max_segment_bytes: Optional[int] = None,
        connection_close: bool = False,
        chunk_size: int = 64 * 1024,
    ) -> MediaOutput:
        """
        Convenience helper to create a `MediaOutput` for this job.

        This uses `subscribe_url` from the job response and raises if missing.
        Subscription tuning is configured once here (and stored on the returned `MediaOutput`).
        """
        if not self.subscribe_url:
            raise LivepeerGatewayError("No subscribe_url present on this LiveVideoToVideo job")
        return MediaOutput(
            self.subscribe_url,
            start_seq=start_seq,
            max_retries=max_retries,
            max_segment_bytes=max_segment_bytes,
            connection_close=connection_close,
            chunk_size=chunk_size,
        )

    @property
    def payment_session(self) -> Optional["PaymentSession"]:
        """
        Access the PaymentSession for this job, if available.
        """
        return self._payment_session


    async def close(self) -> None:
        """
        Close any nested helpers (control, media, etc) best-effort.
        """
        tasks = []
        if self.control is not None:
            tasks.append(self.control.close_control())
        if self._media is not None:
            tasks.append(self._media.close())
        if not tasks:
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result

def start_lv2v(
    orch_url: Optional[Sequence[str] | str],
    req: StartJobRequest,
    *,
    signer_url: Optional[str] = None,
    discovery_url: Optional[str] = None,
) -> LiveVideoToVideo:
    """
    Start a live video-to-video job.

    Selects an orchestrator with LV2V capability and calls
    POST {info.transcoder}/live-video-to-video with JSON body.
    """
    if not req.model_id:
        raise LivepeerGatewayError("start_lv2v requires model_id")

    capabilities = build_capabilities(CapabilityId.LIVE_VIDEO_TO_VIDEO, req.model_id)
    _, info = SelectOrchestrator(
        orch_url,
        signer_url=signer_url,
        discovery_url=discovery_url,
        capabilities=capabilities,
    )

    session = PaymentSession(
        signer_url,
        info,
        capabilities=capabilities,
    )
    p = session.get_payment()
    headers: dict[str, str] = {
        "Livepeer-Payment": p.payment,
        "Livepeer-Segment": p.seg_creds,
    }

    base = _http_origin(info.transcoder)
    url = f"{base}/live-video-to-video"
    data = post_json(url, req.to_json(), headers=headers)
    return LiveVideoToVideo.from_json(
        data,
        orchestrator_info=info,
        payment_session=session,
    )

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


