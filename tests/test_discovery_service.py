from __future__ import annotations

import os

from livepeer_gateway.discovery import (
    discovery_service_capability_name,
    is_discovery_service_endpoint,
    normalize_discovery_service_url,
    resolve_discovery_endpoint,
)


def test_discovery_service_capability_name_strips_pipeline_prefix() -> None:
    assert discovery_service_capability_name("live-video-to-video/streamdiffusion-sdxl") == (
        "streamdiffusion-sdxl"
    )
    assert discovery_service_capability_name("streamdiffusion-sdxl") == "streamdiffusion-sdxl"


def test_normalize_discovery_service_base_url() -> None:
    url = normalize_discovery_service_url(
        "https://discovery-service-production-8955.up.railway.app",
        service_type="legacy",
    )
    assert url == (
        "https://discovery-service-production-8955.up.railway.app/v1/discovery/raw?serviceType=legacy"
    )


def test_resolve_discovery_endpoint_leaves_cloudspe_unchanged() -> None:
    cloudspe = "https://naap-api.cloudspe.com/v1/discover/orchestrators"
    endpoint, uses_service = resolve_discovery_endpoint(cloudspe)
    assert endpoint == cloudspe
    assert uses_service is False


def test_is_discovery_service_endpoint_for_raw_path() -> None:
    assert is_discovery_service_endpoint(
        "https://discovery.example.com/v1/discovery/raw?serviceType=legacy",
    )


def test_read_discovery_service_url_from_env(monkeypatch) -> None:
    monkeypatch.setenv(
        "LIVEPEER_DISCOVERY_SERVICE_URL",
        "https://discovery-service-production-8955.up.railway.app",
    )
    endpoint, uses_service = resolve_discovery_endpoint(
        "https://discovery-service-production-8955.up.railway.app",
    )
    assert uses_service is True
    assert "/v1/discovery/raw" in endpoint
    monkeypatch.delenv("LIVEPEER_DISCOVERY_SERVICE_URL", raising=False)
