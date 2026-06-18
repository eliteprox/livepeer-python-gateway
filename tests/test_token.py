from __future__ import annotations

import base64
import json

import pytest

from livepeer_gateway.errors import LivepeerGatewayError
from livepeer_gateway.token import parse_token


def _encode(payload: dict) -> str:
    return base64.b64encode(json.dumps(payload).encode()).decode()


def test_parse_token_minimal():
    token = _encode(
        {
            "signer": "https://signer.example",
            "discovery": "http://localhost:3000/api/discovery",
            "signer_headers": {"Authorization": "Bearer pmth_abc"},
            "discovery_headers": {"Authorization": "Bearer gw_xyz"},
        }
    )
    data = parse_token(token)
    assert data["signer"] == "https://signer.example"
    assert data["discovery"] == "http://localhost:3000/api/discovery"
    assert data["signer_headers"]["Authorization"] == "Bearer pmth_abc"
    assert data["discovery_headers"]["Authorization"] == "Bearer gw_xyz"
    assert data["billing"] is None
    assert data["api_key"] is None


def test_parse_token_billing_and_api_key():
    token = _encode(
        {
            "billing": "http://localhost:3000",
            "api_key": "pmth_key",
            "discovery": "http://localhost:3000/api/discovery",
        }
    )
    data = parse_token(token)
    assert data["billing"] == "http://localhost:3000"
    assert data["api_key"] == "pmth_key"


def test_parse_token_invalid_base64():
    with pytest.raises(LivepeerGatewayError, match="base64"):
        parse_token("not-valid-base64!!!")


def test_parse_token_invalid_orchestrators():
    token = _encode({"orchestrators": "not-a-list"})
    with pytest.raises(LivepeerGatewayError, match="orchestrators must be an array"):
        parse_token(token)
