"""Opt-in TLS certificate verification.

The tests serve a real aiohttp application over a self-signed certificate
(``tests/fixtures/selfsigned.*``, valid until 2126). By default the SDK accepts
it, as it accepts the self-signed certificates orchestrators serve; with
``VERIFY_TLS`` on, every HTTP path refuses it before sending a request.
"""

from __future__ import annotations

import asyncio
import contextlib
import pathlib
import ssl
from collections.abc import AsyncIterator, Iterator

import pytest
from aiohttp import web

from livepeer_gateway import http
from livepeer_gateway.discovery import discover_runners
from livepeer_gateway.errors import LivepeerGatewayError
from livepeer_gateway.live_runner import call_runner
from livepeer_gateway.remote_signer import RemoteSignerError, get_signer_info

_FIXTURES = pathlib.Path(__file__).parent / "fixtures"


class TestVerifyTlsSetting:
    @pytest.mark.parametrize("value", ["1", "true", "yes", " TRUE ", "Yes"])
    def test_env_turns_verification_on(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv(http.VERIFY_TLS_ENV, value)
        assert http._verify_tls_from_env() is True

    @pytest.mark.parametrize("value", [None, "", "0", "false", "no", "off"])
    def test_anything_else_leaves_it_off(
        self, monkeypatch: pytest.MonkeyPatch, value: str | None
    ) -> None:
        if value is None:
            monkeypatch.delenv(http.VERIFY_TLS_ENV, raising=False)
        else:
            monkeypatch.setenv(http.VERIFY_TLS_ENV, value)
        assert http._verify_tls_from_env() is False

    def test_context_follows_the_setting_at_call_time(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(http, "VERIFY_TLS", False)
        assert http._ssl_context().verify_mode == ssl.CERT_NONE
        assert http._ssl_context().check_hostname is False
        monkeypatch.setattr(http, "VERIFY_TLS", True)
        assert http._ssl_context().verify_mode == ssl.CERT_REQUIRED
        assert http._ssl_context().check_hostname is True


@pytest.fixture
def verify_off(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(http, "VERIFY_TLS", False)
    get_signer_info.cache_clear()  # type: ignore[attr-defined]
    yield
    get_signer_info.cache_clear()  # type: ignore[attr-defined]


@pytest.fixture
def verify_on(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(http, "VERIFY_TLS", True)
    get_signer_info.cache_clear()  # type: ignore[attr-defined]
    yield
    get_signer_info.cache_clear()  # type: ignore[attr-defined]


@contextlib.asynccontextmanager
async def _serve_tls(app: web.Application) -> AsyncIterator[str]:
    """Serve ``app`` over the self-signed certificate on an ephemeral port."""
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_ctx.load_cert_chain(_FIXTURES / "selfsigned.crt", _FIXTURES / "selfsigned.key")
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=ssl_ctx)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    try:
        yield f"https://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


def _app(calls: list[str]) -> web.Application:
    """A runner, signer and discovery endpoint in one."""

    async def call(request: web.Request) -> web.Response:
        calls.append(request.path)
        return web.json_response({"text": "hello"})

    async def sse(request: web.Request) -> web.StreamResponse:
        calls.append(request.path)
        resp = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await resp.prepare(request)
        await resp.write(b"data: one\n\n")
        await resp.write_eof()
        return resp

    async def sign(request: web.Request) -> web.Response:
        calls.append(request.path)
        return web.json_response({"address": "0xpayer", "signature": "0xsig"})

    async def discover(request: web.Request) -> web.Response:
        calls.append(request.path)
        return web.json_response(
            [{"address": "https://orch.example.com", "runners": [{"url": "https://orch.example.com/r", "app": "a"}]}]
        )

    app = web.Application()
    app.router.add_post("/call", call)
    app.router.add_post("/sse", sse)
    app.router.add_post("/sign-orchestrator-info", sign)
    app.router.add_get("/discover-orchestrators", discover)
    return app


class TestDefaultAcceptsSelfSigned:
    async def test_runner_signer_and_discovery(self, verify_off: None) -> None:
        calls: list[str] = []
        async with _serve_tls(_app(calls)) as base:
            result = await call_runner(f"{base}/call", payload={"x": 1})
            async with await call_runner(f"{base}/sse", payload={"x": 1}, stream=True) as stream:
                lines = [line async for line in stream.aiter_lines() if line]
            signer = await get_signer_info(base)
            entries = await discover_runners(signer_url=base)
        assert result.data == {"text": "hello"}
        assert lines == ["data: one"]
        assert signer.address == "0xpayer"
        assert entries[0]["runners"][0]["app"] == "a"
        assert calls == ["/call", "/sse", "/sign-orchestrator-info", "/discover-orchestrators"]


class TestOptInRejectsSelfSigned:
    async def test_call_runner(self, verify_on: None) -> None:
        calls: list[str] = []
        async with _serve_tls(_app(calls)) as base:
            with pytest.raises(LivepeerGatewayError, match="certificate"):
                await call_runner(f"{base}/call", payload={"x": 1})
            with pytest.raises(LivepeerGatewayError, match="certificate"):
                await call_runner(f"{base}/sse", payload={"x": 1}, stream=True)
        assert calls == []

    async def test_signer(self, verify_on: None) -> None:
        calls: list[str] = []
        async with _serve_tls(_app(calls)) as base:
            with pytest.raises(LivepeerGatewayError, match="certificate"):
                await get_signer_info(base)
        assert calls == []

    async def test_discovery(self, verify_on: None) -> None:
        calls: list[str] = []
        async with _serve_tls(_app(calls)) as base:
            with pytest.raises(RemoteSignerError, match="certificate"):
                await discover_runners(signer_url=base)
        assert calls == []

    async def test_sync_request(self, verify_on: None) -> None:
        calls: list[str] = []

        def probe(base: str) -> str:
            try:
                http.post_json_sync(f"{base}/sign-orchestrator-info", {})
            except LivepeerGatewayError as e:
                return str(e)
            return ""

        async with _serve_tls(_app(calls)) as base:
            message = await asyncio.to_thread(probe, base)
        assert "certificate" in message
        assert calls == []
