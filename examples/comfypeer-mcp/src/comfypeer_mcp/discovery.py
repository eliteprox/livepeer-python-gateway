from __future__ import annotations

from typing import Any

import httpx

from comfypeer_mcp.config import Settings


async def list_capabilities(
    settings: Settings,
    *,
    service_type: str | None = None,
) -> dict[str, Any]:
    params: dict[str, str] = {}
    if service_type:
        params["serviceType"] = service_type
    url = f"{settings.discovery_base()}/v1/discovery/capabilities"
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()


async def query_orchestrators(
    settings: Settings,
    *,
    capabilities: list[str],
    service_types: list[str] | None = None,
    top_n: int = 50,
    sort_by: str = "avail",
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "capabilities": capabilities,
        "topN": top_n,
        "sortBy": sort_by,
    }
    if service_types:
        body["serviceTypes"] = service_types
    else:
        body["serviceTypes"] = ["live-video-to-video", "live-runner"]

    url = f"{settings.discovery_base()}/v1/discovery/query"
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(url, json=body)
        response.raise_for_status()
        return response.json()


async def discovery_freshness(settings: Settings) -> dict[str, Any]:
    url = f"{settings.discovery_base()}/v1/discovery/freshness"
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()
