import argparse
import json
import logging
import ssl
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional
from urllib.parse import urlparse
from urllib.request import Request, urlopen

_LOG = logging.getLogger(__name__)

_DEFAULT_AI_SERVICE_REGISTRY_ADDR = "0x04C0b249740175999E5BF5c9ac1dA92431EF34C5"
# Arbitrum AI subnet BondingManager (hardcoded per request)
_DEFAULT_BONDING_MANAGER_ADDR = "0x35Bcf3c30594191d53231E4FF333E8A770453e40"

# From go-livepeer generated bindings:
# - BondingManager.getFirstTranscoderInPool() method selector: 0x88a6c749
# - BondingManager.getNextTranscoderInPool(address) method selector: 0x235c9603
# - ServiceRegistry.getServiceURI(address) method selector: 0x214c2a4b
_SELECTOR_GET_FIRST_TRANSCODER = "0x88a6c749"
_SELECTOR_GET_NEXT_TRANSCODER = "0x235c9603"
_SELECTOR_GET_SERVICE_URI = "0x214c2a4b"


@dataclass(frozen=True)
class CacheSnapshot:
    entries: list[dict[str, Any]]
    last_refresh_unix: float
    refresh_duration_ms: int
    discovery_duration_ms: int
    refresh_in_progress: bool
    discovered_count: int
    ok_count: int
    last_error: Optional[str]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run an orchestrator discovery HTTP service backed by the Livepeer on-chain ServiceRegistry.",
        epilog=(
            "Examples:\n"
            "  python examples/capability_discovery_service.py --ai-service-registry --eth-rpc https://rpc.example\n"
            "  python examples/capability_discovery_service.py --service-registry-addr 0x... --eth-rpc https://rpc.example\n"
            "  python examples/capability_discovery_service.py --ai-service-registry --eth-rpc https://rpc.example --bind 0.0.0.0 --port 9876\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--ai-service-registry",
        action="store_true",
        help=(
            "Enable on-chain discovery via the Livepeer AI ServiceRegistry contract address "
            f"({_DEFAULT_AI_SERVICE_REGISTRY_ADDR}). Requires --eth-rpc."
        ),
    )
    p.add_argument(
        "--service-registry-addr",
        default=None,
        help="ServiceRegistry contract address (overrides --ai-service-registry default). Requires --eth-rpc.",
    )
    p.add_argument(
        "--eth-rpc",
        default=None,
        help="Ethereum JSON-RPC endpoint URL used for on-chain discovery (eth_call).",
    )
    p.add_argument(
        "--bonding-manager-addr",
        default=_DEFAULT_BONDING_MANAGER_ADDR,
        help="BondingManager contract address used to enumerate the transcoder pool.",
    )
    p.add_argument(
        "--max-onchain-orchestrators",
        type=int,
        default=2000,
        help="Safety cap on number of transcoders to enumerate on-chain.",
    )
    p.add_argument(
        "--bind",
        default="127.0.0.1",
        help="Bind address for the HTTP server.",
    )
    p.add_argument(
        "--port",
        type=int,
        default=9876,
        help="Port for the HTTP server.",
    )
    p.add_argument(
        "--refresh-interval",
        type=float,
        default=30.0,
        help="Refresh interval in seconds.",
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging.",
    )
    return p.parse_args()


def _rpc_call(rpc_url: str, method: str, params: list[Any]) -> Any:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    body = json.dumps(payload).encode("utf-8")
    req = Request(
        rpc_url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "livepeer-python-gateway/example-capability-discovery",
        },
        method="POST",
    )
    ssl_ctx = ssl._create_unverified_context()
    with urlopen(req, timeout=20.0, context=ssl_ctx) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    data = json.loads(raw)
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(f"eth rpc error: {data['error']}")
    if not isinstance(data, dict) or "result" not in data:
        raise RuntimeError("eth rpc error: missing result")
    return data["result"]


def _eth_call(rpc_url: str, *, to: str, data: str) -> str:
    to_s = str(to).strip()
    data_s = str(data).strip()
    if not (to_s.startswith("0x") and len(to_s) == 42):
        raise ValueError(f"Invalid contract address: {to_s!r}")
    if not data_s.startswith("0x"):
        raise ValueError(f"Invalid eth_call data (missing 0x): {data_s!r}")
    res = _rpc_call(rpc_url, "eth_call", [{"to": to_s, "data": data_s}, "latest"])
    return str(res or "0x")


def _abi_decode_string(data_hex: str) -> str:
    """
    Decode ABI-encoded `string` for a call with a single non-indexed string return value.
    Layout: offset (32) | ... | length (32) | bytes...
    """
    h = str(data_hex)
    if h.startswith("0x") or h.startswith("0X"):
        h = h[2:]
    b = bytes.fromhex(h) if h else b""
    if len(b) < 64:
        return ""
    offset = int.from_bytes(b[0:32], "big", signed=False)
    if offset + 32 > len(b):
        return ""
    strlen = int.from_bytes(b[offset : offset + 32], "big", signed=False)
    start = offset + 32
    end = min(len(b), start + strlen)
    if start > end:
        return ""
    return b[start:end].decode("utf-8", errors="replace")


def _abi_encode_address_arg(addr: str) -> str:
    """ABI-encode an address as a 32-byte hex string (no 0x prefix)."""
    a = str(addr).strip()
    if a.startswith("0x") or a.startswith("0X"):
        a = a[2:]
    if len(a) != 40:
        raise ValueError(f"Invalid address (expected 20 bytes): {addr!r}")
    int(a, 16)  # validate hex
    return ("0" * 24) + a.lower()


def _abi_decode_address(result_hex: str) -> str:
    """Decode a single address return value from eth_call (32-byte word)."""
    h = str(result_hex or "")
    if h.startswith("0x") or h.startswith("0X"):
        h = h[2:]
    h = h.rjust(64, "0")
    if len(h) < 64:
        return "0x" + ("0" * 40)
    return "0x" + h[-40:]


def _bonding_manager_transcoder_addrs(
    *,
    rpc_url: str,
    bonding_manager_addr: str,
    max_orchestrators: int,
) -> list[str]:
    max_n = max(1, int(max_orchestrators))
    bm = str(bonding_manager_addr).strip()
    if not (bm.startswith("0x") and len(bm) == 42):
        raise ValueError(f"Invalid BondingManager address: {bm!r}")

    first = _abi_decode_address(_eth_call(rpc_url, to=bm, data=_SELECTOR_GET_FIRST_TRANSCODER))
    zero = "0x" + ("0" * 40)
    if first.lower() == zero:
        return []

    out: list[str] = []
    seen: set[str] = set()
    curr = first
    for _ in range(max_n):
        c = curr.lower()
        if c in seen:
            break
        seen.add(c)
        out.append(curr)

        data = _SELECTOR_GET_NEXT_TRANSCODER + _abi_encode_address_arg(curr)
        nxt = _abi_decode_address(_eth_call(rpc_url, to=bm, data=data))
        if nxt.lower() == zero:
            break
        curr = nxt

    return out


def _service_registry_get_service_uri(
    *,
    rpc_url: str,
    service_registry_addr: str,
    eth_addr: str,
) -> str:
    sr = str(service_registry_addr).strip()
    if not (sr.startswith("0x") and len(sr) == 42):
        raise ValueError(f"Invalid ServiceRegistry address: {sr!r}")
    data = _SELECTOR_GET_SERVICE_URI + _abi_encode_address_arg(eth_addr)
    return _abi_decode_string(_eth_call(rpc_url, to=sr, data=data))


def _normalize_orch_address(addr: str) -> Optional[str]:
    """
    Normalize a serviceURI into a gRPC-compatible orchestrator URL (https://host:port).
    """
    s = str(addr).strip()
    if not s:
        return None
    parsed = urlparse(s if "://" in s else f"https://{s}")
    if not parsed.netloc:
        if "://" not in s and ":" in s:
            return s
        return None
    scheme = parsed.scheme or "https"
    if scheme not in ("http", "https"):
        return None
    if any(ch.isspace() for ch in parsed.netloc):
        return None
    return f"https://{parsed.netloc}"


def refresh_once(args: argparse.Namespace) -> CacheSnapshot:
    start = time.time()

    if not args.eth_rpc:
        raise RuntimeError("--eth-rpc is required for on-chain discovery")

    registry_addr = str(args.service_registry_addr or _DEFAULT_AI_SERVICE_REGISTRY_ADDR).strip()
    if not (registry_addr.startswith("0x") and len(registry_addr) == 42):
        raise RuntimeError(f"Invalid ServiceRegistry address: {registry_addr!r}")

    try:
        eth_addrs = _bonding_manager_transcoder_addrs(
            rpc_url=str(args.eth_rpc),
            bonding_manager_addr=str(args.bonding_manager_addr),
            max_orchestrators=int(args.max_onchain_orchestrators),
        )
    except Exception as e:
        raise RuntimeError(f"BondingManager enumeration failed: {e.__class__.__name__}: {e}") from None

    discovered_count = len(eth_addrs)
    ok_count = 0
    last_error: Optional[str] = None
    uris: list[str] = []

    if eth_addrs:
        max_workers = min(32, max(1, len(eth_addrs)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _service_registry_get_service_uri,
                    rpc_url=str(args.eth_rpc),
                    service_registry_addr=registry_addr,
                    eth_addr=a,
                ): a
                for a in eth_addrs
            }
            for future in as_completed(futures):
                try:
                    uri = str(future.result() or "").strip()
                except Exception as e:
                    _LOG.debug("getServiceURI failed addr=%s err=%s", futures[future], e)
                    last_error = f"{e.__class__.__name__}: {e}"
                    continue
                if uri:
                    uris.append(uri)
                    ok_count += 1

    discovery_end = time.time()

    orch_list = sorted({o for o in (_normalize_orch_address(u) for u in uris) if o})
    entries = [{"address": url} for url in orch_list]

    end = time.time()
    return CacheSnapshot(
        entries=entries,
        last_refresh_unix=end,
        refresh_duration_ms=int((end - start) * 1000),
        discovery_duration_ms=int((discovery_end - start) * 1000),
        refresh_in_progress=False,
        discovered_count=discovered_count,
        ok_count=ok_count,
        last_error=last_error,
    )


def refresh_loop(
    args: argparse.Namespace,
    *,
    server: "DiscoveryHTTPServer",
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        with server.cache_lock:
            if server.cache.last_refresh_unix == 0.0 and not server.cache.refresh_in_progress:
                server.cache = CacheSnapshot(
                    entries=server.cache.entries,
                    last_refresh_unix=server.cache.last_refresh_unix,
                    refresh_duration_ms=server.cache.refresh_duration_ms,
                    discovery_duration_ms=server.cache.discovery_duration_ms,
                    refresh_in_progress=True,
                    discovered_count=server.cache.discovered_count,
                    ok_count=server.cache.ok_count,
                    last_error=server.cache.last_error,
                )
        try:
            snapshot = refresh_once(args)
            with server.cache_lock:
                server.cache = snapshot
        except Exception as e:
            _LOG.warning("Refresh failed: %s", e)
            with server.cache_lock:
                server.cache = CacheSnapshot(
                    entries=server.cache.entries,
                    last_refresh_unix=server.cache.last_refresh_unix,
                    refresh_duration_ms=0,
                    discovery_duration_ms=server.cache.discovery_duration_ms,
                    refresh_in_progress=False,
                    discovered_count=server.cache.discovered_count,
                    ok_count=server.cache.ok_count,
                    last_error=f"{e.__class__.__name__}: {e}",
                )

        stop_event.wait(timeout=max(0.1, float(args.refresh_interval)))


class DiscoveryHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        RequestHandlerClass: type[BaseHTTPRequestHandler],
        *,
        cache_lock: threading.Lock,
        cache: CacheSnapshot,
    ) -> None:
        super().__init__(server_address, RequestHandlerClass)
        self.cache_lock = cache_lock
        self.cache = cache


class DiscoveryHandler(BaseHTTPRequestHandler):
    server: DiscoveryHTTPServer  # type: ignore[assignment]

    def log_message(self, format: str, *args: Any) -> None:
        _LOG.info("%s - %s", self.address_string(), format % args)

    def _send_json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path

        if path == "/healthz":
            with self.server.cache_lock:
                snap = self.server.cache
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "last_refresh_unix": snap.last_refresh_unix,
                    "refresh_duration_ms": snap.refresh_duration_ms,
                    "discovery_duration_ms": snap.discovery_duration_ms,
                    "refresh_in_progress": snap.refresh_in_progress,
                    "discovered_count": snap.discovered_count,
                    "ok_count": snap.ok_count,
                    "last_error": snap.last_error,
                },
            )
            return

        if path != "/discover-orchestrators":
            self._send_json(HTTPStatus.NOT_FOUND, {"error": {"message": "not found"}})
            return

        with self.server.cache_lock:
            entries = list(self.server.cache.entries)

        self._send_json(HTTPStatus.OK, entries)


def main() -> None:
    args = _parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)

    cache_lock = threading.Lock()
    initial_cache = CacheSnapshot(
        entries=[],
        last_refresh_unix=0.0,
        refresh_duration_ms=0,
        discovery_duration_ms=0,
        refresh_in_progress=False,
        discovered_count=0,
        ok_count=0,
        last_error=None,
    )

    server = DiscoveryHTTPServer(
        (str(args.bind), int(args.port)),
        DiscoveryHandler,
        cache_lock=cache_lock,
        cache=initial_cache,
    )

    stop_event = threading.Event()
    t = threading.Thread(
        target=refresh_loop,
        args=(args,),
        kwargs={"server": server, "stop_event": stop_event},
        daemon=True,
        name="discovery-refresh",
    )
    t.start()

    _LOG.info("Discovery service listening on http://%s:%d", args.bind, args.port)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
