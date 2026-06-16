from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from livepeer_gateway.auth_resolve import (
    SignerAuthRefreshContext,
    _extract_signer_access_token,
    exchange_api_key_via_dashboard,
    exchange_device_token_via_dashboard,
    refresh_signer_credentials,
    resolve_issuer_url,
    resolve_signer_auth,
)
from livepeer_gateway.errors import LivepeerGatewayError


def test_resolve_issuer_url_from_billing() -> None:
    assert (
        resolve_issuer_url("https://pymthouse-staging.vercel.app", None)
        == "https://pymthouse-staging.vercel.app/api/v1/oidc"
    )
    assert resolve_issuer_url("http://localhost:3001", None) == (
        "http://localhost:3001/api/v1/oidc"
    )


def test_resolve_issuer_url_explicit_wins() -> None:
    assert (
        resolve_issuer_url(
            "http://localhost:3001",
            "https://localhost:8080/realms/clearinghouse",
        )
        == "https://localhost:8080/realms/clearinghouse"
    )


def test_resolve_issuer_url_skips_keycloak_billing() -> None:
    assert resolve_issuer_url("https://host/realms/foo", None) is None


def test_exchange_device_token_via_dashboard_camel_case() -> None:
    with patch("livepeer_gateway.auth_resolve.post_json_sync") as post_json:
        post_json.return_value = {
            "token": {"accessToken": "signer-jwt", "tokenType": "Bearer"},
            "signerUrl": "http://127.0.0.1:8080",
        }
        payload = exchange_device_token_via_dashboard(
            "http://localhost:3001",
            "user-device-token",
            scope="sign:job",
        )
    assert _extract_signer_access_token(payload) == "signer-jwt"
    assert payload["signerUrl"] == "http://127.0.0.1:8080"
    post_json.assert_called_once()
    url = post_json.call_args[0][0]
    body = post_json.call_args[0][1]
    assert url == "http://localhost:3001/api/signer/device/exchange"
    assert body["deviceToken"] == "user-device-token"
    assert body["scope"] == "sign:job"


def test_exchange_device_token_missing_token_raises() -> None:
    with patch("livepeer_gateway.auth_resolve.post_json_sync") as post_json:
        post_json.return_value = {"identity": {}}
        with pytest.raises(LivepeerGatewayError, match="missing signer access token"):
            exchange_device_token_via_dashboard("http://localhost:3001", "x")


def test_exchange_api_key_via_dashboard() -> None:
    with patch("livepeer_gateway.auth_resolve.post_json_sync") as post_json:
        post_json.return_value = {
            "access_token": "signer-jwt",
            "token_type": "Bearer",
            "signerUrl": "http://127.0.0.1:8080",
        }
        payload = exchange_api_key_via_dashboard(
            "http://localhost:3001",
            "pmth_secret",
            scope="sign:job",
            client_id="app_demo",
        )
    assert _extract_signer_access_token(payload) == "signer-jwt"
    assert payload["signerUrl"] == "http://127.0.0.1:8080"
    post_json.assert_called_once()
    url = post_json.call_args[0][0]
    body = post_json.call_args[0][1]
    assert url == "http://localhost:3001/api/pymthouse/keys/exchange"
    assert body == {"apiKey": "pmth_secret", "scope": "sign:job", "clientId": "app_demo"}


def test_exchange_api_key_omits_client_id_when_absent() -> None:
    with patch("livepeer_gateway.auth_resolve.post_json_sync") as post_json:
        post_json.return_value = {"access_token": "signer-jwt"}
        exchange_api_key_via_dashboard("http://localhost:3001", "pmth_secret")
    body = post_json.call_args[0][1]
    assert body == {"apiKey": "pmth_secret"}


def test_exchange_api_key_missing_token_raises() -> None:
    with patch("livepeer_gateway.auth_resolve.post_json_sync") as post_json:
        post_json.return_value = {"error": "nope"}
        with pytest.raises(LivepeerGatewayError, match="missing signer access token"):
            exchange_api_key_via_dashboard("http://localhost:3001", "pmth_secret")


def test_resolve_signer_auth_uses_api_key_exchange() -> None:
    with (
        patch(
            "livepeer_gateway.auth_resolve.exchange_api_key_via_dashboard",
            return_value={
                "access_token": "signer-access",
                "signerUrl": "http://127.0.0.1:8080",
            },
        ) as api_exchange,
        patch("livepeer_gateway.oidc_auth.ensure_valid_token") as ensure_valid,
        patch(
            "livepeer_gateway.auth_resolve.exchange_device_token_via_dashboard",
        ) as device_exchange,
    ):
        signer_url, signer_headers, _, _ = resolve_signer_auth(
            billing_url="http://localhost:3001",
            oidc_client_id="app_demo",
            api_key="pmth_secret",
        )

    ensure_valid.assert_not_called()
    device_exchange.assert_not_called()
    api_exchange.assert_called_once_with(
        "http://localhost:3001",
        "pmth_secret",
        scope="sign:job",
        client_id="app_demo",
    )
    assert signer_url == "http://127.0.0.1:8080"
    assert signer_headers == {"Authorization": "Bearer signer-access"}


def test_refresh_signer_credentials_uses_api_key() -> None:
    ctx = SignerAuthRefreshContext(
        billing_url="http://localhost:3001",
        signer_url="http://127.0.0.1:8080",
        oidc_client_id="app_demo",
        api_key="pmth_secret",
    )
    with patch(
        "livepeer_gateway.auth_resolve.resolve_signer_auth",
        return_value=("http://127.0.0.1:8080", {"Authorization": "Bearer new"}, None, None),
    ) as resolve:
        headers = refresh_signer_credentials(ctx)
    assert headers["Authorization"] == "Bearer new"
    assert resolve.call_args.kwargs["api_key"] == "pmth_secret"
    assert resolve.call_args.kwargs["signer_headers"] is None


def test_resolve_signer_auth_skips_when_bearer_present() -> None:
    headers = {"Authorization": "Bearer existing"}
    result = resolve_signer_auth(
        billing_url="http://localhost:3001",
        issuer_url="http://issuer",
        signer_url="http://127.0.0.1:8080",
        signer_headers=headers,
    )
    assert result == ("http://127.0.0.1:8080", headers, None, None)


def test_resolve_signer_auth_copies_bearer_to_explicit_discovery_url() -> None:
    headers = {"Authorization": "Bearer existing"}
    _, _, discovery_url, discovery_headers = resolve_signer_auth(
        billing_url="http://localhost:3001",
        issuer_url="http://issuer",
        signer_url="http://127.0.0.1:8080",
        signer_headers=headers,
        discovery_url="https://localhost:8080/discover-orchestrators?cap=model",
    )
    assert discovery_url == "https://localhost:8080/discover-orchestrators?cap=model"
    assert discovery_headers == headers


def test_resolve_signer_auth_runs_oidc_and_exchange() -> None:
    user_tokens = MagicMock()
    user_tokens.get.return_value = "user-access"
    with (
        patch(
            "livepeer_gateway.oidc_auth.ensure_valid_token",
            return_value=user_tokens,
        ) as ensure_valid,
        patch(
            "livepeer_gateway.auth_resolve.exchange_device_token_via_dashboard",
            return_value={
                "access_token": "signer-access",
                "signerUrl": "http://127.0.0.1:8080",
            },
        ) as exchange,
    ):
        signer_url, signer_headers, discovery_url, discovery_headers = resolve_signer_auth(
            billing_url="http://localhost:3001",
            issuer_url="http://localhost:3001/api/v1/oidc",
            oidc_client_id="app_demo",
        )

    ensure_valid.assert_called_once()
    exchange.assert_called_once_with(
        "http://localhost:3001",
        "user-access",
        scope="sign:job",
        client_id="app_demo",
    )
    assert signer_url == "http://127.0.0.1:8080"
    assert signer_headers == {"Authorization": "Bearer signer-access"}
    assert discovery_url is None
    assert discovery_headers is None


def test_resolve_signer_auth_clears_cache_when_requested() -> None:
    user_tokens = MagicMock()
    user_tokens.get.return_value = "user-access"
    with (
        patch(
            "livepeer_gateway.oidc_auth.clear_cached_token",
        ) as clear_cache,
        patch(
            "livepeer_gateway.oidc_auth.ensure_valid_token",
            return_value=user_tokens,
        ),
        patch(
            "livepeer_gateway.auth_resolve.exchange_device_token_via_dashboard",
            return_value={"access_token": "signer-access"},
        ),
    ):
        resolve_signer_auth(
            billing_url="http://localhost:3001",
            issuer_url="http://localhost:3001/api/v1/oidc",
            oidc_client_id="app_demo",
            clear_token_cache=True,
        )

    clear_cache.assert_called_once_with(
        "http://localhost:3001/api/v1/oidc",
        client_id="app_demo",
        scopes="openid profile sign:job",
    )


def test_refresh_signer_credentials_re_runs_oidc_exchange() -> None:
    ctx = SignerAuthRefreshContext(
        billing_url="http://localhost:3001",
        issuer_url="http://localhost:3001/api/v1/oidc",
        signer_url="http://127.0.0.1:8080",
        oidc_client_id="app_demo",
    )
    with patch(
        "livepeer_gateway.auth_resolve.resolve_signer_auth",
        return_value=("http://127.0.0.1:8080", {"Authorization": "Bearer new"}, None, None),
    ) as resolve:
        headers = refresh_signer_credentials(ctx)
    assert headers["Authorization"] == "Bearer new"
    resolve.assert_called_once()
    assert resolve.call_args.kwargs["signer_headers"] is None


def test_start_lv2v_explicit_signer_headers_win_over_billing() -> None:
    from livepeer_gateway.lv2v import StartJobRequest, start_lv2v

    explicit_headers = {"Authorization": "Bearer explicit"}
    with patch("livepeer_gateway.lv2v.resolve_signer_auth") as resolve:
        resolve.return_value = (
            "http://signer",
            explicit_headers,
            None,
            None,
        )
        with patch("livepeer_gateway.lv2v.orchestrator_selector") as selector:
            cursor = MagicMock()
            selector.return_value = cursor
            cursor.next.side_effect = LivepeerGatewayError("stop test early")

            with pytest.raises(LivepeerGatewayError):
                start_lv2v(
                    None,
                    StartJobRequest(model_id="test-model"),
                    billing_url="http://localhost:3001",
                    issuer_url="http://issuer",
                    signer_headers=explicit_headers,
                )

    resolve.assert_called_once()
    call_kwargs = resolve.call_args.kwargs
    assert call_kwargs["signer_headers"] == explicit_headers
