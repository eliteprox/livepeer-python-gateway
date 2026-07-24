from __future__ import annotations

from typing import Any

from livepeer_gateway.byoc import (
    ByocJobRequest,
    get_training_status,
    submit_byoc_job,
)
from livepeer_gateway.errors import LivepeerGatewayError

from comfypeer_mcp.config import Settings, is_loopback_url
from comfypeer_mcp.live_runner import ExecutionError, _map_gateway_error


def _assert_orch_allowed(settings: Settings, orch_url: str | None) -> None:
    if orch_url and is_loopback_url(orch_url) and not settings.allow_loopback_discovery:
        raise ExecutionError(
            "Loopback orch_url is not allowed on the hosted MCP.",
            code="loopback_forbidden",
            status=400,
        )


def byoc_submit(
    settings: Settings,
    *,
    capability: str,
    payload: dict[str, Any],
    signer_jwt: str,
    signer_url: str | None = None,
    orch_url: str | None = None,
    discovery_url: str | None = None,
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    _assert_orch_allowed(settings, orch_url)
    if discovery_url and is_loopback_url(discovery_url) and not settings.allow_loopback_discovery:
        raise ExecutionError(
            "Loopback discovery_url is not allowed on the hosted MCP.",
            code="loopback_forbidden",
            status=400,
        )

    signer = (signer_url or settings.signer_url or "").strip() or None
    signer_headers = {"Authorization": f"Bearer {signer_jwt}"}
    req = ByocJobRequest(
        capability=capability,
        payload=payload,
        timeout_seconds=timeout_seconds,
    )
    try:
        kwargs: dict[str, Any] = {
            "req": req,
            "signer_url": signer,
            "signer_headers": signer_headers,
        }
        if orch_url:
            kwargs["orch_url"] = orch_url
        if discovery_url:
            kwargs["discovery_url"] = discovery_url
        result = submit_byoc_job(**kwargs)
        return {
            "status_code": result.status_code,
            "data": result.data,
            "orchestrator_url": result.orchestrator_url,
            "headers": dict(result.headers),
        }
    except LivepeerGatewayError as exc:
        raise _map_gateway_error(exc) from exc


def training_submit(
    settings: Settings,
    *,
    capability: str,
    model_id: str,
    params: dict[str, Any],
    signer_jwt: str,
    signer_url: str | None = None,
    orch_url: str | None = None,
    callback_url: str | None = None,
) -> dict[str, Any]:
    _assert_orch_allowed(settings, orch_url)
    if not orch_url:
        raise ExecutionError(
            "orch_url is required for training_submit",
            code="missing_orch_url",
            status=400,
        )

    # Import lazily — training helpers differ across SDK branches.
    try:
        from livepeer_gateway.byoc import ByocTrainingRequest, submit_training_job
    except ImportError as exc:
        raise ExecutionError(
            "Training API not available in installed livepeer-gateway",
            code="training_unsupported",
        ) from exc

    signer = (signer_url or settings.signer_url or "").strip() or None
    signer_headers = {"Authorization": f"Bearer {signer_jwt}"}
    req = ByocTrainingRequest(
        capability=capability,
        model_id=model_id,
        params=params,
        callback_url=callback_url,
    )
    try:
        result = submit_training_job(
            req=req,
            orch_url=orch_url,
            signer_url=signer,
            signer_headers=signer_headers,
        )
        if isinstance(result, dict):
            return result
        return {
            "job_id": getattr(result, "job_id", None),
            "data": getattr(result, "data", result),
        }
    except LivepeerGatewayError as exc:
        raise _map_gateway_error(exc) from exc


def training_status(*, job_id: str, orch_url: str) -> dict[str, Any]:
    try:
        status = get_training_status(job_id, orch_url)
        if isinstance(status, dict):
            return status
        return {"data": status}
    except LivepeerGatewayError as exc:
        raise _map_gateway_error(exc) from exc
