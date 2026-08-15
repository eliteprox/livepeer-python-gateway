from __future__ import annotations

import base64
import inspect
import json
from urllib.parse import parse_qs, urlparse

import pytest

from livepeer_gateway.discovery import (
    _append_cap_strings,
    _entry_matches_caps,
    _filter_runner_discovery_entries,
    discover_orchestrator_runners,
)
from livepeer_gateway.errors import LivepeerGatewayError
from livepeer_gateway.selection import reserve_session, runner_selector
from livepeer_gateway.token import parse_token


def _encode(payload: dict) -> str:
    return base64.b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")


def test_parse_token_includes_caps() -> None:
    token = _encode(
        {
            "signer": "https://signer.example",
            "discovery": "https://signer.example/discover-orchestrators",
            "caps": [
                " live-video-to-video/streamdiffusion ",
                "text-to-image/flux",
                "live-video-to-video/streamdiffusion",
            ],
            "signer_headers": {"Authorization": "Bearer x"},
        }
    )
    parsed = parse_token(token)
    assert parsed["caps"] == [
        "live-video-to-video/streamdiffusion",
        "text-to-image/flux",
    ]
    assert parsed["discovery"] == "https://signer.example/discover-orchestrators"


def test_parse_token_rejects_invalid_caps() -> None:
    token = _encode({"caps": [""]})
    with pytest.raises(LivepeerGatewayError, match="caps must contain only non-empty"):
        parse_token(token)


def test_append_cap_strings_preserves_existing_query() -> None:
    url = _append_cap_strings(
        "https://signer.example/discover-orchestrators?caps=a",
        ["b", " c "],
    )
    assert "caps=a" in url
    assert "caps=b" in url
    assert "caps=c" in url


def test_append_cap_strings_uses_plural_caps_param() -> None:
    """
    The remote signer reads only `caps`; a singular `cap` is silently ignored
    and it returns every orchestrator. Pin the literal name so the fail-open
    cannot be reintroduced by a typo.
    """
    url = _append_cap_strings(
        "https://signer.example/discover-orchestrators",
        ["livepeer-example/hello-world"],
    )
    query = parse_qs(urlparse(url).query)
    assert list(query) == ["caps"]
    assert query["caps"] == ["livepeer-example/hello-world"]
    assert "cap=" not in url


def test_entry_matches_caps_rejects_non_advertising_orchestrator() -> None:
    entry = {
        "address": "https://orch.example",
        "capabilities": ["live-video-to-video/streamdiffusion-sdxl"],
    }
    assert _entry_matches_caps(entry, ()) is True
    assert _entry_matches_caps(entry, ["live-video-to-video/streamdiffusion-sdxl"]) is True
    # The signer ignored the filter and returned an unrelated orchestrator.
    assert _entry_matches_caps(entry, ["livepeer-example/hello-world"]) is False
    # OR semantics across requested caps.
    assert (
        _entry_matches_caps(
            entry,
            ["livepeer-example/hello-world", "live-video-to-video/streamdiffusion-sdxl"],
        )
        is True
    )
    # A missing/!list capabilities field cannot satisfy an explicit filter.
    assert _entry_matches_caps({"address": "https://orch.example"}, ["x"]) is False


def test_filter_runner_discovery_entries_applies_caps_locally() -> None:
    """A signer that ignores `caps` must not leak non-matching orchestrators."""
    unfiltered = [
        {
            "address": "https://streamdiffusion.example",
            "capabilities": ["live-video-to-video/streamdiffusion-sdxl"],
            "runners": [
                {"url": "https://streamdiffusion.example/apps/r1/session", "app": "sd"}
            ],
        },
        {
            "address": "https://hello.example",
            "capabilities": ["livepeer-example/hello-world"],
            "runners": [
                {
                    "url": "https://hello.example/apps/r2/session",
                    "app": "livepeer-example/hello-world",
                }
            ],
        },
    ]
    entries = _filter_runner_discovery_entries(
        unfiltered,
        app_filters=(),
        gpu_filters=(),
        caps_filters=["livepeer-example/hello-world"],
    )
    assert [e["address"] for e in entries] == ["https://hello.example"]

    # No caps filter keeps the existing (unfiltered) behavior.
    entries = _filter_runner_discovery_entries(
        unfiltered, app_filters=(), gpu_filters=(), caps_filters=()
    )
    assert len(entries) == 2


def test_reserve_session_and_orchestrator_runners_accept_caps() -> None:
    """`caps` must not be silently dropped on the orchestrators-list path."""
    assert "caps" in inspect.signature(discover_orchestrator_runners).parameters
    assert "caps" in inspect.signature(runner_selector).parameters
    assert "caps" in inspect.signature(reserve_session).parameters
