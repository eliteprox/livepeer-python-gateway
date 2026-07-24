from __future__ import annotations

from typing import Any

from livepeer_gateway.errors import LivepeerGatewayError
from livepeer_gateway.lv2v import StartJobRequest, start_lv2v

from comfypeer_mcp.config import Settings, is_loopback_url
from comfypeer_mcp.live_runner import ExecutionError, _map_gateway_error

_lv2v_jobs: dict[str, Any] = {}


async def lv2v_start(
    settings: Settings,
    *,
    model_id: str,
    params: dict[str, Any] | None,
    signer_jwt: str,
    signer_url: str | None = None,
    orch_url: str | None = None,
    discovery_url: str | None = None,
    request_id: str | None = None,
    stream_id: str | None = None,
) -> dict[str, Any]:
    if orch_url and is_loopback_url(orch_url) and not settings.allow_loopback_discovery:
        raise ExecutionError(
            "Loopback orch_url is not allowed on the hosted MCP.",
            code="loopback_forbidden",
            status=400,
        )
    if discovery_url and is_loopback_url(discovery_url) and not settings.allow_loopback_discovery:
        raise ExecutionError(
            "Loopback discovery_url is not allowed on the hosted MCP.",
            code="loopback_forbidden",
            status=400,
        )
    if not orch_url and not discovery_url:
        raise ExecutionError(
            "orch_url or discovery_url is required for lv2v_start",
            code="missing_orch_url",
            status=400,
        )

    signer = (signer_url or settings.signer_url or "").strip() or None
    signer_headers = {"Authorization": f"Bearer {signer_jwt}"}
    req = StartJobRequest(
        request_id=request_id,
        model_id=model_id,
        params=params,
        stream_id=stream_id,
    )
    try:
        job = start_lv2v(
            orch_url,
            req,
            signer_url=signer,
            signer_headers=signer_headers,
            discovery_url=discovery_url,
        )
        manifest_id = getattr(job, "manifest_id", None)
        if not manifest_id and getattr(job, "raw", None):
            manifest_id = job.raw.get("manifest_id")
        job_key = str(manifest_id or id(job))
        _lv2v_jobs[job_key] = job
        return {
            "job_id": job_key,
            "manifest_id": manifest_id,
            "publish_url": getattr(job, "publish_url", None),
            "subscribe_url": getattr(job, "subscribe_url", None),
            "control_url": getattr(job, "control_url", None),
            "events_url": getattr(job, "events_url", None),
        }
    except LivepeerGatewayError as exc:
        raise _map_gateway_error(exc) from exc


async def lv2v_close(*, job_id: str) -> dict[str, Any]:
    job = _lv2v_jobs.pop(job_id, None)
    if job is None:
        raise ExecutionError(f"Unknown LV2V job_id: {job_id}", code="unknown_job", status=404)
    close = getattr(job, "close", None)
    if close is not None:
        result = close()
        if hasattr(result, "__await__"):
            await result
    return {"job_id": job_id, "closed": True}


async def lv2v_write_control(*, job_id: str, message: dict[str, Any] | str) -> dict[str, Any]:
    job = _lv2v_jobs.get(job_id)
    if job is None:
        raise ExecutionError(f"Unknown LV2V job_id: {job_id}", code="unknown_job", status=404)
    control = getattr(job, "control", None)
    if control is None:
        raise ExecutionError("Job has no control channel", code="no_control", status=400)
    write = getattr(control, "write", None)
    if write is None:
        raise ExecutionError("Control channel is not writable", code="no_control", status=400)
    result = write(message)
    if hasattr(result, "__await__"):
        await result
    return {"job_id": job_id, "ok": True}
