from __future__ import annotations

import base64
import json
import time

from livepeer_gateway.signer_bearer import bearer_jwt_exp_unix, should_refresh_signer_bearer


def _jwt_with_exp(exp: int) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').decode().rstrip("=")
    payload = base64.urlsafe_b64encode(
        json.dumps({"exp": exp}).encode()
    ).decode().rstrip("=")
    return f"{header}.{payload}.sig"


def test_bearer_jwt_exp_unix():
    exp = int(time.time()) + 3600
    headers = {"Authorization": f"Bearer {_jwt_with_exp(exp)}"}
    assert bearer_jwt_exp_unix(headers) == float(exp)
    assert bearer_jwt_exp_unix({"Authorization": "Bearer pmth_abc"}) is None


def test_should_refresh_signer_bearer():
    soon = int(time.time()) + 30
    headers = {"Authorization": f"Bearer {_jwt_with_exp(soon)}"}
    assert should_refresh_signer_bearer(headers, skew_seconds=60, now=time.time()) is True
    later = int(time.time()) + 3600
    headers_later = {"Authorization": f"Bearer {_jwt_with_exp(later)}"}
    assert should_refresh_signer_bearer(headers_later, skew_seconds=60, now=time.time()) is False
