from __future__ import annotations

from unittest.mock import patch

import pytest

from livepeer_gateway.auth_exchange import (
    SignerTokenProvider,
    exchange_api_key_for_signer,
)
from livepeer_gateway.errors import LivepeerGatewayError


def test_exchange_api_key_for_signer_returns_signer_url_and_bearer() -> None:
    payload = {
        "token": {"accessToken": "jwt"},
        "signerUrl": "https://signer.example",
    }
    with patch("livepeer_gateway.auth_exchange.post_json", return_value=payload) as post_json:
        signer_url, headers = exchange_api_key_for_signer(
            "https://dashboard.example.com",
            "pmth_test",
            client_id="app_test",
        )

    assert signer_url == "https://signer.example"
    assert headers == {"Authorization": "Bearer jwt"}
    post_json.assert_called_once_with(
        "https://dashboard.example.com/api/pymthouse/keys/exchange",
        {
            "apiKey": "pmth_test",
            "clientId": "app_test",
            "scope": "sign:job",
        },
        timeout=15.0,
    )


def test_exchange_api_key_for_signer_rejects_empty_api_key() -> None:
    with pytest.raises(LivepeerGatewayError, match="non-empty API key"):
        exchange_api_key_for_signer("https://dashboard.example.com", "  ")


def test_exchange_api_key_for_signer_requires_access_token() -> None:
    with patch("livepeer_gateway.auth_exchange.post_json", return_value={}):
        with pytest.raises(LivepeerGatewayError, match="missing signer access token"):
            exchange_api_key_for_signer("https://dashboard.example.com", "pmth_test")


def test_signer_token_provider_refresh_remints_headers() -> None:
    payloads = [
        {"token": {"accessToken": "jwt1"}, "signerUrl": "https://signer.example"},
        {"token": {"accessToken": "jwt2"}, "signerUrl": "https://signer.example"},
    ]
    with patch("livepeer_gateway.auth_exchange.post_json", side_effect=payloads):
        provider = SignerTokenProvider(
            "https://dashboard.example.com",
            "pmth_test",
            client_id="app_test",
        )
        first = provider.refresh()
        assert first == {"Authorization": "Bearer jwt1"}
        assert provider.signer_url == "https://signer.example"
        assert provider.headers == first

        second = provider.refresh()
        assert second == {"Authorization": "Bearer jwt2"}
        assert provider.headers == second
