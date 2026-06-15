from __future__ import annotations

from urllib.parse import parse_qs, urlparse

from livepeer_gateway import discovery
from livepeer_gateway.capabilities import CapabilityId, build_capabilities


def test_append_caps_sends_full_pipeline_model_form() -> None:
    caps = build_capabilities(CapabilityId.LIVE_VIDEO_TO_VIDEO, "streamdiffusion-sdxl")
    url = discovery._append_caps(
        "https://discovery.example.com/v1/discovery/raw?serviceType=legacy",
        caps,
    )
    query = parse_qs(urlparse(url).query)
    assert query["serviceType"] == ["legacy"]
    assert query["caps"] == ["live-video-to-video/streamdiffusion-sdxl"]


def test_discover_orchestrators_parses_addresses(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def fake_get_json_sync(url, *, headers=None, timeout=None):
        captured["url"] = url
        return [
            {"address": "https://orch-a.example:8935", "capabilities": ["streamdiffusion-sdxl"]},
            {"address": "https://orch-b.example:8935"},
            {"not_address": "ignored"},
        ]

    monkeypatch.setattr(discovery, "get_json_sync", fake_get_json_sync)

    caps = build_capabilities(CapabilityId.LIVE_VIDEO_TO_VIDEO, "streamdiffusion-sdxl")
    result = discovery.discover_orchestrators(
        discovery_url="https://discovery.example.com/v1/discovery/raw?serviceType=legacy",
        capabilities=caps,
    )

    assert result == ["https://orch-a.example:8935", "https://orch-b.example:8935"]
    assert "caps=live-video-to-video/streamdiffusion-sdxl" in captured["url"]
