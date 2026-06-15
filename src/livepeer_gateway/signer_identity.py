from __future__ import annotations

import base64
import json
from typing import Any, Optional


def _decode_jwt_payload(jwt: str) -> dict[str, Any]:
    parts = jwt.strip().split(".")
    if len(parts) < 2:
        raise ValueError("invalid JWT shape")
    payload_b64 = parts[1].replace("-", "+").replace("_", "/")
    payload_b64 += "=" * (-len(payload_b64) % 4)
    payload = json.loads(base64.b64decode(payload_b64).decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("JWT payload must be an object")
    return payload


def _read_claim(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def identity_from_bearer_token(token: str) -> Optional[dict[str, str]]:
    """
    Build go-livepeer remote payment identity from a signer JWT payload.

    Matches builder-sdk ``identityFromJwtPayload`` / Apache DMZ trusted headers.
    """
    try:
        payload = _decode_jwt_payload(token)
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
        return None

    issuer = _read_claim(payload, "iss")
    client_id = _read_claim(payload, "client_id")
    usage_subject = _read_claim(payload, "external_user_id", "usage_subject", "sub")
    usage_subject_type = (
        _read_claim(payload, "external_user_id_type", "usage_subject_type")
        or "external_user_id"
    )
    if not issuer or not client_id or not usage_subject:
        return None

    return {
        "issuer": issuer,
        "client_id": client_id,
        "usage_subject": usage_subject,
        "usage_subject_type": usage_subject_type,
    }


def livepeer_identity_headers(identity: dict[str, str]) -> dict[str, str]:
    return {
        "X-Livepeer-Usage-Issuer": identity["issuer"],
        "X-Livepeer-Client-ID": identity["client_id"],
        "X-Livepeer-Usage-Subject": identity["usage_subject"],
        "X-Livepeer-Usage-Subject-Type": identity["usage_subject_type"],
    }


def enrich_signer_payment_request(
    headers: Optional[dict[str, str]],
    payload: dict[str, Any],
) -> tuple[dict[str, str], dict[str, Any]]:
    """
    Attach trusted-header identity for ``remoteSignerUsageIdentityMode=trusted_headers``.

    Apache DMZ validates the Bearer JWT but mod_authnz_jwt does not export custom
    claims as ``AUTHJWT_CLAIM_*`` env vars, so clients must send identity explicitly.
    """
    out_headers = dict(headers) if headers else {}
    auth = out_headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return out_headers, payload

    identity = identity_from_bearer_token(auth[7:].strip())
    if identity is None:
        return out_headers, payload

    out_headers.update(livepeer_identity_headers(identity))
    out_payload = dict(payload)
    out_payload["identity"] = identity
    return out_headers, out_payload
