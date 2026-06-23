"""
Unit tests for PaymentSession signer-token refresh on expiry.

A long-running stream outlives the short-lived sign:job JWT. When the signer
rejects an expired token, PaymentSession must call refresh_signer_headers to
re-mint the bearer and retry, rather than failing the payment cycle.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

import pytest

from livepeer_gateway.errors import LivepeerGatewayError
from livepeer_gateway.remote_signer import PaymentSession, _is_signer_auth_error


def _stub_orch_info():
    info = MagicMock()
    info.transcoder = "https://orch.test:8935"
    info.SerializeToString = lambda: b"stub-orch-info-protobuf"
    return info


class _MockResponse:
    def __init__(self, body: bytes):
        self._body = body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


EXPIRED_BODY = (
    b'unexpected JWT "exp" (expiration time) claim value, '
    b"expiration is past current timestamp"
)


def test_is_signer_auth_error_detects_expired_jwt():
    assert _is_signer_auth_error(Exception(EXPIRED_BODY.decode()))
    cause = HTTPError("https://signer.test", 401, "unauthorized", {}, None)
    wrapped = Exception("boom")
    wrapped.__cause__ = cause
    assert _is_signer_auth_error(wrapped)
    assert not _is_signer_auth_error(Exception("HTTP 500 internal error"))


def test_payment_session_refreshes_expired_token_then_succeeds():
    calls = {"signer": 0}
    seen_auth: list[str] = []

    def _fake_urlopen(req, *args, **kwargs):
        url = req.full_url
        hdrs = {k.lower(): v for k, v in req.header_items()}
        seen_auth.append(hdrs.get("authorization", ""))
        calls["signer"] += 1
        if calls["signer"] == 1:
            # First attempt: expired token → signer 502 with JWT exp body.
            err = HTTPError(url, 502, "bad gateway", {}, None)
            err.read = lambda: EXPIRED_BODY
            raise err
        return _MockResponse(
            json.dumps(
                {"payment": "PAY", "segCreds": "SEG", "state": {"k": "v"}}
            ).encode()
        )

    refreshed = {"n": 0}

    def _refresh():
        refreshed["n"] += 1
        return {"Authorization": "Bearer jwt_fresh"}

    session = PaymentSession(
        "https://signer.test",
        _stub_orch_info(),
        signer_headers={"Authorization": "Bearer jwt_expired"},
        refresh_signer_headers=_refresh,
        type="lv2v",
    )

    with patch("livepeer_gateway.orchestrator.urlopen", side_effect=_fake_urlopen):
        result = session.get_payment()

    assert result.payment == "PAY"
    assert refreshed["n"] == 1, "refresh callback should fire exactly once"
    assert seen_auth[0] == "Bearer jwt_expired"
    assert seen_auth[1] == "Bearer jwt_fresh", "retry must use re-minted token"


def test_payment_session_without_provider_raises_on_expiry():
    def _fake_urlopen(req, *args, **kwargs):
        err = HTTPError(req.full_url, 502, "bad gateway", {}, None)
        err.read = lambda: EXPIRED_BODY
        raise err

    session = PaymentSession(
        "https://signer.test",
        _stub_orch_info(),
        signer_headers={"Authorization": "Bearer jwt_expired"},
        type="lv2v",
    )

    with patch("livepeer_gateway.orchestrator.urlopen", side_effect=_fake_urlopen):
        with pytest.raises(LivepeerGatewayError):
            session.get_payment()
