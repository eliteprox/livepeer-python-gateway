from __future__ import annotations

import base64
import json
import time

from livepeer_gateway.signer_bearer import bearer_jwt_exp_unix, should_refresh_signer_bearer


def _jwt_with_exp(exp: int) -> str:
    header = base64.urlsafe_b64encode(b"{}").decode("ascii").rstrip("=")
    payload = base64.urlsafe_b64encode(
        json.dumps({"exp": exp}).encode("utf-8"),
    ).decode("ascii").rstrip("=")
    return f"header.{payload}.sig"


def test_bearer_jwt_exp_unix_reads_exp() -> None:
    exp = int(time.time()) + 600
    headers = {"Authorization": f"Bearer {_jwt_with_exp(exp)}"}
    assert bearer_jwt_exp_unix(headers) == exp


def test_should_refresh_signer_bearer_within_skew() -> None:
    exp = int(time.time()) + 30
    headers = {"Authorization": f"Bearer {_jwt_with_exp(exp)}"}
    assert should_refresh_signer_bearer(headers, skew_seconds=60, now=time.time())


def test_should_refresh_signer_bearer_false_when_fresh() -> None:
    exp = int(time.time()) + 600
    headers = {"Authorization": f"Bearer {_jwt_with_exp(exp)}"}
    assert not should_refresh_signer_bearer(headers, skew_seconds=60, now=time.time())
