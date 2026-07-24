import json
from unittest.mock import AsyncMock, patch

import pytest

from comfypeer_mcp.auth import CachedSignerJwt, build_sdk_token_payload
from comfypeer_mcp.config import Settings
from comfypeer_mcp.live_runner import ExecutionError, live_runner_call
from comfypeer_mcp.session import create_signer_session_payload


def test_sdk_token_payload_includes_signer_headers():
    payload = build_sdk_token_payload(
        api_key="pmth_test",
        signer_url="https://signer.example",
        discovery_url="https://discovery.example/raw",
    )
    assert payload["signer"] == "https://signer.example"
    assert payload["discovery"] == "https://discovery.example/raw"
    assert payload["signer_headers"]["Authorization"] == "Bearer pmth_test"


def test_create_signer_session_payload_encodes_sdk_token():
    settings = Settings(
        PYMTHOUSE_PUBLIC_CLIENT_ID="app_test",
        SIGNER_URL="https://signer.example",
    )
    session = CachedSignerJwt(
        jwt="jwt-value",
        expires_at=9999999999,
        signer_url="https://signer.example",
        discovery_url="https://discovery.example/raw",
        balance_usd_micros="1000",
    )
    out = create_signer_session_payload(
        settings,
        api_key="pmth_test",
        session=session,
    )
    assert out["access_token"] == "jwt-value"
    assert out["sdk_token"]
    assert out["client_id"] == "app_test"


@pytest.mark.asyncio
async def test_live_runner_rejects_loopback_on_hosted():
    settings = Settings(ALLOW_LOOPBACK_DISCOVERY="0")
    with pytest.raises(ExecutionError) as exc:
        await live_runner_call(
            settings,
            app="livepeer-example/hello-world",
            path="/hello",
            payload={"name": "x"},
            signer_url="https://signer.example",
            signer_jwt="jwt",
            discovery_url="http://localhost:8935/discovery",
        )
    assert exc.value.code == "loopback_forbidden"


@pytest.mark.asyncio
async def test_live_runner_happy_path_mocked():
    settings = Settings(ALLOW_LOOPBACK_DISCOVERY="1")

    class FakeSession:
        session_id = "sess-1"
        app_url = "https://orch.example/apps/r1/session/sess-1/app"

    class FakeResult:
        data = {"message": "Hello, x!"}

    with (
        patch(
            "comfypeer_mcp.live_runner.reserve_session",
            new=AsyncMock(return_value=FakeSession()),
        ),
        patch(
            "comfypeer_mcp.live_runner.call_runner",
            new=AsyncMock(return_value=FakeResult()),
        ),
        patch(
            "comfypeer_mcp.live_runner.stop_runner_session",
            new=AsyncMock(),
        ),
    ):
        result = await live_runner_call(
            settings,
            app="livepeer-example/hello-world",
            path="/hello",
            payload={"name": "x"},
            signer_url="https://signer.example",
            signer_jwt="jwt",
            discovery_url="http://localhost:8935/discovery",
        )
    assert result["data"]["message"] == "Hello, x!"
    assert "sess-1" in result["session_id"]
