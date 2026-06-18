from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from livepeer_gateway.auth_resolve import (
    SignerAuthRefreshContext,
    billing_origin_from_discovery_url,
    exchange_api_key_via_billing_app,
    extract_pmth_api_key_from_signer_headers,
    refresh_signer_credentials,
    resolve_signer_auth,
)
from livepeer_gateway.errors import LivepeerGatewayError


def test_extract_pmth_api_key_from_signer_headers():
    assert extract_pmth_api_key_from_signer_headers(
        {"Authorization": "Bearer pmth_abc123"}
    ) == "pmth_abc123"
    assert extract_pmth_api_key_from_signer_headers(
        {"Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjE3MDAwMDAwMDB9.sig"}
    ) is None
    assert extract_pmth_api_key_from_signer_headers(None) is None


def test_billing_origin_from_discovery_url():
    assert billing_origin_from_discovery_url(
        "http://localhost:3000/api/v1/orchestrator-leaderboard/python-gateway"
    ) == "http://localhost:3000"
    assert billing_origin_from_discovery_url(None) is None
    assert billing_origin_from_discovery_url("") is None


@patch("livepeer_gateway.orchestrator.post_json")
def test_exchange_api_key_via_billing_app(mock_post_json):
    mock_post_json.return_value = {
        "token": {"accessToken": "signer-jwt-xyz"},
        "signerUrl": "https://signer.example",
    }
    result = exchange_api_key_via_billing_app(
        "http://localhost:3000",
        "pmth_testkey",
        scope="sign:job",
    )
    assert result["token"]["accessToken"] == "signer-jwt-xyz"
    mock_post_json.assert_called_once_with(
        "http://localhost:3000/api/pymthouse/keys/exchange",
        {"apiKey": "pmth_testkey", "scope": "sign:job"},
        timeout=15.0,
    )


@patch("livepeer_gateway.orchestrator.post_json")
def test_resolve_signer_auth_exchanges_api_key(mock_post_json):
    mock_post_json.return_value = {
        "token": {"accessToken": "jwt-access"},
        "signerUrl": "https://signer.example",
    }
    signer_url, headers, discovery_url, discovery_headers = resolve_signer_auth(
        billing_url="http://localhost:3000",
        signer_url=None,
        signer_headers=None,
        discovery_url="http://localhost:3000/api/discovery",
        discovery_headers=None,
        api_key="pmth_key",
    )
    assert signer_url == "https://signer.example"
    assert headers == {"Authorization": "Bearer jwt-access"}
    assert discovery_url == "http://localhost:3000/api/discovery"
    assert discovery_headers == {"Authorization": "Bearer jwt-access"}


def test_resolve_signer_auth_passes_through_jwt_headers():
    jwt_headers = {"Authorization": "Bearer eyJ.test.sig"}
    signer_url, headers, _, _ = resolve_signer_auth(
        billing_url="http://localhost:3000",
        signer_url="https://signer.example",
        signer_headers=jwt_headers,
        api_key="pmth_should_be_ignored",
    )
    assert signer_url == "https://signer.example"
    assert headers == jwt_headers


@patch("livepeer_gateway.orchestrator.post_json")
def test_refresh_signer_credentials(mock_post_json):
    mock_post_json.return_value = {
        "token": {"accessToken": "refreshed-jwt"},
        "signerUrl": "https://signer.example",
    }
    ctx = SignerAuthRefreshContext(
        billing_url="http://localhost:3000",
        signer_url="https://signer.example",
        api_key="pmth_key",
    )
    headers = refresh_signer_credentials(ctx)
    assert headers == {"Authorization": "Bearer refreshed-jwt"}


@patch("livepeer_gateway.orchestrator.post_json")
def test_exchange_missing_token_raises(mock_post_json):
    mock_post_json.return_value = {"unexpected": True}
    with pytest.raises(LivepeerGatewayError, match="missing signer access token"):
        exchange_api_key_via_billing_app("http://localhost:3000", "pmth_x")
