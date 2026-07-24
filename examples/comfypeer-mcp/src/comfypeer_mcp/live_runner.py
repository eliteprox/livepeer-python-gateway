from __future__ import annotations

from contextlib import suppress
from typing import Any
from urllib.parse import urlparse

from livepeer_gateway.errors import (
    LivepeerGatewayError,
    PaymentError,
    SignerRefreshRequired,
    SkipPaymentCycle,
)
from livepeer_gateway.live_runner import call_runner, stop_runner_session
from livepeer_gateway.selection import reserve_session

from comfypeer_mcp.config import Settings, is_loopback_url


class ExecutionError(Exception):
    def __init__(
        self,
        message: str,
        *,
        code: str = "execution_error",
        status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


def _assert_discovery_allowed(settings: Settings, discovery_url: str) -> None:
    if is_loopback_url(discovery_url) and not settings.allow_loopback_discovery:
        raise ExecutionError(
            "Loopback discovery_url is not allowed on the hosted MCP. "
            "Run a local gateway/MCP with ALLOW_LOOPBACK_DISCOVERY=1, "
            "or omit discovery_url to use production discovery-service.",
            code="loopback_forbidden",
            status=400,
        )


def _map_gateway_error(exc: Exception) -> ExecutionError:
    if isinstance(exc, PaymentError):
        return ExecutionError(
            str(exc) or "Payment / balance error",
            code="insufficient_balance",
            status=483,
        )
    if isinstance(exc, SignerRefreshRequired):
        return ExecutionError(
            str(exc) or "Signer session refresh required",
            code="session_refresh",
            status=480,
        )
    if isinstance(exc, SkipPaymentCycle):
        return ExecutionError(
            str(exc) or "Skip payment cycle",
            code="skip_payment_cycle",
            status=482,
        )
    if isinstance(exc, LivepeerGatewayError):
        msg = str(exc)
        if "480" in msg or "refresh" in msg.lower():
            return ExecutionError(msg, code="session_refresh", status=480)
        if "483" in msg or "balance" in msg.lower():
            return ExecutionError(msg, code="insufficient_balance", status=483)
        return ExecutionError(msg, code="livepeer_gateway_error")
    return ExecutionError(str(exc))


async def live_runner_call(
    settings: Settings,
    *,
    app: str,
    path: str,
    payload: dict[str, Any],
    signer_url: str | None,
    signer_jwt: str,
    discovery_url: str | None = None,
) -> dict[str, Any]:
    discovery = (discovery_url or settings.resolved_default_discovery_url()).strip()
    _assert_discovery_allowed(settings, discovery)

    signer = (signer_url or settings.signer_url or "").strip() or None
    signer_headers = {"Authorization": f"Bearer {signer_jwt}"}

    session = None
    try:
        session = await reserve_session(
            discovery_url=discovery,
            app=app,
            signer_url=signer,
            signer_headers=signer_headers,
        )
        runner_path = path if path.startswith("/") else f"/{path}"
        runner_url = session.app_url.rstrip("/") + runner_path
        result = await call_runner(
            runner_url=runner_url,
            payload=payload,
            signer_url=signer,
            signer_headers=signer_headers,
        )
        return {
            "session_id": session.session_id,
            "app_url": session.app_url,
            "runner_url": runner_url,
            "data": result.data,
        }
    except ExecutionError:
        raise
    except Exception as exc:
        raise _map_gateway_error(exc) from exc
    finally:
        if session is not None:
            with suppress(Exception):
                await stop_runner_session(session)


def orch_origin_from_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return url
    return f"{parsed.scheme}://{parsed.netloc}"
