from __future__ import annotations

import json
from contextvars import ContextVar
from typing import Any

from mcp.server.fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from comfypeer_mcp import __version__
from comfypeer_mcp.auth import AuthError, authenticate_request
from comfypeer_mcp.byoc_tools import byoc_submit, training_status, training_submit
from comfypeer_mcp.config import get_settings
from comfypeer_mcp.discovery import (
    discovery_freshness,
    list_capabilities as discovery_list_capabilities,
    query_orchestrators as discovery_query_orchestrators,
)
from comfypeer_mcp.live_runner import ExecutionError, live_runner_call
from comfypeer_mcp.lv2v_tools import lv2v_close, lv2v_start, lv2v_write_control
from comfypeer_mcp.rate_limit import RateLimitExceeded, RateLimiter
from comfypeer_mcp.session import create_signer_session_payload

_authorization: ContextVar[str | None] = ContextVar("authorization", default=None)

mcp = FastMCP(
    "Livepeer MCP",
    instructions=(
        "Livepeer Network MCP. Authenticate with Authorization: Bearer "
        "<PymtHouse user API key>. Use list_capabilities / query_orchestrators for catalog; "
        "create_signer_session for local gateway tokens; run_capability (BYOC), "
        "start_stream / stop_stream (LV2V), call_live_runner (sessionful HTTP apps) for execution. "
        "Loopback discovery/orch URLs require ALLOW_LOOPBACK_DISCOVERY=1."
    ),
)


def _auth_header() -> str | None:
    return _authorization.get()


async def _require_session():
    settings = get_settings()
    limiter = RateLimiter(settings.rate_limit_max, settings.rate_limit_window_seconds)
    try:
        principal, session = await authenticate_request(settings, _auth_header())
    except AuthError as exc:
        raise ValueError(f"{exc.code}: {exc}") from exc
    limiter.check(principal.bearer_token[:32])
    return settings, principal, session


@mcp.tool()
async def list_capabilities(service_type: str | None = None) -> str:
    """List capabilities from discovery-service (Storyboard-aligned catalog tool).

    service_type: live-video-to-video | live-runner | modules | batch (omit for default mix).
    """
    settings, _, _ = await _require_session()
    data = await discovery_list_capabilities(settings, service_type=service_type)
    return json.dumps(data, indent=2)


@mcp.tool()
async def query_orchestrators(
    capabilities: list[str],
    service_types: list[str] | None = None,
    top_n: int = 50,
) -> str:
    """Query ranked orchestrators for the given capability names."""
    settings, _, _ = await _require_session()
    data = await discovery_query_orchestrators(
        settings,
        capabilities=capabilities,
        service_types=service_types,
        top_n=top_n,
    )
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_discovery_freshness() -> str:
    """Return discovery-service dataset freshness stats."""
    settings, _, _ = await _require_session()
    data = await discovery_freshness(settings)
    return json.dumps(data, indent=2)


@mcp.tool()
async def create_signer_session(force_refresh: bool = False) -> str:
    """Mint/exchange a SignerSession + base64 livepeer-python-gateway --token payload.

    Uses the Bearer API key from the MCP Authorization header.
    """
    settings = get_settings()
    from comfypeer_mcp.auth import exchange_api_key_for_signer_session, extract_bearer

    try:
        api_key = extract_bearer(_auth_header())
        session = await exchange_api_key_for_signer_session(
            settings,
            api_key,
            force_refresh=force_refresh,
        )
    except AuthError as exc:
        raise ValueError(f"{exc.code}: {exc}") from exc
    payload = create_signer_session_payload(settings, api_key=api_key, session=session)
    return json.dumps(payload, indent=2)


@mcp.tool()
async def call_live_runner(
    app: str,
    path: str,
    payload: dict[str, Any],
    discovery_url: str | None = None,
    signer_url: str | None = None,
) -> str:
    """Call a live-runner HTTP app: reserve session → POST path → stop.

    Prefer production discovery by default. Pass discovery_url only for reachable hosts;
    localhost requires ALLOW_LOOPBACK_DISCOVERY=1.
    """
    settings, _, session = await _require_session()
    try:
        result = await live_runner_call(
            settings,
            app=app,
            path=path,
            payload=payload,
            signer_url=signer_url or session.signer_url,
            signer_jwt=session.jwt,
            discovery_url=discovery_url,
        )
    except (ExecutionError, RateLimitExceeded) as exc:
        raise ValueError(str(exc)) from exc
    return json.dumps(result, indent=2)


@mcp.tool()
async def run_capability(
    capability: str,
    payload: dict[str, Any],
    orch_url: str | None = None,
    discovery_url: str | None = None,
    timeout_seconds: int = 300,
) -> str:
    """Run a BYOC network capability (sync /inference). Storyboard analogue: create_media."""
    settings, _, session = await _require_session()
    try:
        result = byoc_submit(
            settings,
            capability=capability,
            payload=payload,
            signer_jwt=session.jwt,
            signer_url=session.signer_url,
            orch_url=orch_url,
            discovery_url=discovery_url,
            timeout_seconds=timeout_seconds,
        )
    except ExecutionError as exc:
        raise ValueError(str(exc)) from exc
    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def submit_training(
    capability: str,
    model_id: str,
    params: dict[str, Any],
    orch_url: str,
    callback_url: str | None = None,
) -> str:
    """Submit an async BYOC training job."""
    settings, _, session = await _require_session()
    try:
        result = training_submit(
            settings,
            capability=capability,
            model_id=model_id,
            params=params,
            signer_jwt=session.jwt,
            signer_url=session.signer_url,
            orch_url=orch_url,
            callback_url=callback_url,
        )
    except ExecutionError as exc:
        raise ValueError(str(exc)) from exc
    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def get_job_status(job_id: str, orch_url: str) -> str:
    """Poll an async training / job status by id."""
    await _require_session()
    try:
        result = training_status(job_id=job_id, orch_url=orch_url)
    except ExecutionError as exc:
        raise ValueError(str(exc)) from exc
    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def start_stream(
    model_id: str,
    params: dict[str, Any] | None = None,
    orch_url: str | None = None,
    discovery_url: str | None = None,
    request_id: str | None = None,
    stream_id: str | None = None,
) -> str:
    """Start a live-video-to-video stream; returns media/control URLs (no pixel streaming)."""
    settings, _, session = await _require_session()
    try:
        result = await lv2v_start(
            settings,
            model_id=model_id,
            params=params,
            signer_jwt=session.jwt,
            signer_url=session.signer_url,
            orch_url=orch_url,
            discovery_url=discovery_url,
            request_id=request_id,
            stream_id=stream_id,
        )
    except ExecutionError as exc:
        raise ValueError(str(exc)) from exc
    return json.dumps(result, indent=2)


@mcp.tool()
async def write_stream_control(job_id: str, message: dict[str, Any]) -> str:
    """Write a control message to an active live stream (LV2V)."""
    await _require_session()
    try:
        result = await lv2v_write_control(job_id=job_id, message=message)
    except ExecutionError as exc:
        raise ValueError(str(exc)) from exc
    return json.dumps(result, indent=2)


@mcp.tool()
async def stop_stream(job_id: str) -> str:
    """Stop / close an active live stream handle."""
    await _require_session()
    try:
        result = await lv2v_close(job_id=job_id)
    except ExecutionError as exc:
        raise ValueError(str(exc)) from exc
    return json.dumps(result, indent=2)


@mcp.tool()
async def livepeer_mcp_info() -> str:
    """Return MCP host metadata (no secrets)."""
    settings = get_settings()
    return json.dumps(
        {
            "name": "Livepeer MCP",
            "version": __version__,
            "mcp_public_url": settings.mcp_public_url,
            "discovery_service_url": settings.discovery_service_url,
            "public_client_id": settings.pymthouse_public_client_id or None,
            "issuer_url": settings.pymthouse_issuer_url,
        },
        indent=2,
    )


class AuthHeaderMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        if request.url.path in {"/healthz", "/health"}:
            return JSONResponse({"status": "ok", "service": "livepeer-mcp"})
        token = request.headers.get("authorization")
        _authorization.set(token)
        try:
            return await call_next(request)
        except RateLimitExceeded as exc:
            return JSONResponse({"error": str(exc)}, status_code=429)
        finally:
            _authorization.set(None)


def create_http_app():
    """Starlette app with streamable HTTP MCP + auth header middleware."""
    app = mcp.streamable_http_app()
    app.add_middleware(AuthHeaderMiddleware)
    return app
