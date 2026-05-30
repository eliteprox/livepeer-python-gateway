from __future__ import annotations

from unittest.mock import MagicMock, patch

from livepeer_gateway.oidc_auth import _pkce_challenge_pair, device_login


def test_pkce_challenge_pair_s256_no_padding() -> None:
    verifier, challenge = _pkce_challenge_pair()
    assert len(verifier) >= 43
    assert "=" not in challenge


def test_device_login_sends_pkce_on_authorization_request() -> None:
    config = MagicMock(
        issuer="http://issuer/realms/test",
        device_authorization_endpoint="http://issuer/device",
        token_endpoint="http://issuer/token",
    )
    device_resp = MagicMock(status_code=200)
    device_resp.json.return_value = {
        "device_code": "dev",
        "user_code": "ABCD",
        "verification_uri": "http://issuer/verify",
        "expires_in": 600,
        "interval": 5,
    }
    token_resp = MagicMock(status_code=200)
    token_resp.json.return_value = {"access_token": "tok", "token_type": "Bearer"}

    mock_client = MagicMock()
    mock_client.request.side_effect = [device_resp, token_resp]
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)

    with (
        patch("livepeer_gateway.oidc_auth.discover", return_value=config),
        patch("livepeer_gateway.oidc_auth._build_oauth2_client", return_value=mock_client),
        patch("livepeer_gateway.oidc_auth.time.sleep"),
    ):
        tokens = device_login("http://issuer/realms/test", client_id="app_demo")

    assert tokens["access_token"] == "tok"
    device_call = mock_client.request.call_args_list[0]
    device_data = device_call.kwargs.get("data") or device_call[1].get("data")
    assert device_data["code_challenge_method"] == "S256"
    assert "code_challenge" in device_data

    token_call = mock_client.request.call_args_list[1]
    token_data = token_call.kwargs.get("data") or token_call[1].get("data")
    assert "code_verifier" in token_data
