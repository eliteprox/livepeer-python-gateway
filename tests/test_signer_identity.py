from __future__ import annotations

import base64
import json

from livepeer_gateway.signer_identity import (
    enrich_signer_payment_request,
    identity_from_bearer_token,
    livepeer_identity_headers,
)


def _jwt(payload: dict[str, object]) -> str:
    header = base64.urlsafe_b64encode(
        json.dumps({"alg": "RS256", "typ": "JWT"}).encode("utf-8"),
    ).decode("ascii").rstrip("=")
    body = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode(
        "ascii",
    ).rstrip("=")
    return f"{header}.{body}.sig"


def test_identity_from_bearer_token_reads_device_exchange_claims() -> None:
    token = _jwt(
        {
            "iss": "http://localhost:3001/api/v1/oidc",
            "client_id": "app_demo",
            "external_user_id": "user:abc123",
            "scope": "sign:job",
        },
    )
    identity = identity_from_bearer_token(token)
    assert identity == {
        "issuer": "http://localhost:3001/api/v1/oidc",
        "client_id": "app_demo",
        "usage_subject": "user:abc123",
        "usage_subject_type": "external_user_id",
    }
    assert livepeer_identity_headers(identity) == {
        "X-Livepeer-Usage-Issuer": "http://localhost:3001/api/v1/oidc",
        "X-Livepeer-Client-ID": "app_demo",
        "X-Livepeer-Usage-Subject": "user:abc123",
        "X-Livepeer-Usage-Subject-Type": "external_user_id",
    }


def test_enrich_signer_payment_request_adds_headers_and_body() -> None:
    token = _jwt(
        {
            "iss": "http://localhost:3001/api/v1/oidc",
            "client_id": "app_demo",
            "external_user_id": "user:abc123",
        },
    )
    headers, payload = enrich_signer_payment_request(
        {"Authorization": f"Bearer {token}"},
        {"type": "lv2v"},
    )
    assert headers["X-Livepeer-Client-ID"] == "app_demo"
    assert payload["identity"]["usage_subject"] == "user:abc123"


def test_enrich_signer_payment_request_noop_without_bearer() -> None:
    headers, payload = enrich_signer_payment_request(None, {"type": "lv2v"})
    assert headers == {}
    assert payload == {"type": "lv2v"}
