from __future__ import annotations

import base64
import json

import pytest

from livepeer_gateway.errors import LivepeerGatewayError
from livepeer_gateway.token import parse_token


def _encode(payload: dict) -> str:
    return base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")


def test_parse_token_billing_and_issuer_fields() -> None:
    token = _encode(
        {
            "billing": "http://localhost:3001",
            "issuer": "http://127.0.0.1:8080/realms/clearinghouse",
            "oidc_client_id": "app_demo",
            "oidc_scopes": "openid profile sign:job",
            "signer": "http://127.0.0.1:8080",
        }
    )
    data = parse_token(token)
    assert data["billing"] == "http://localhost:3001"
    assert data["issuer"] == "http://127.0.0.1:8080/realms/clearinghouse"
    assert data["oidc_client_id"] == "app_demo"
    assert data["oidc_scopes"] == "openid profile sign:job"
    assert data["signer"] == "http://127.0.0.1:8080"


def test_parse_token_rejects_non_string_billing() -> None:
    token = _encode({"billing": 123})
    with pytest.raises(LivepeerGatewayError, match="billing must be a string"):
        parse_token(token)
