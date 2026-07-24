# Local ComfyPeer MCP client

Run this example next to `livepeer-python-gateway` when you want Cursor (or any MCP client) to call Livepeer tools **on your machine** — discovery, signer session mint via PymtHouse, and `reserve_session` → `call_runner` → `stop` (same loop as `examples/echo` / the vllm gateway pattern).

For **hosted** Livepeer MCP (catalog + session mint, no local orch execution), use PymtHouse `GET/POST /api/v1/mcp` — see PymtHouse `docs/livepeer-mcp.md`.

## Setup

```bash
# from livepeer-python-gateway repo root
cd examples/comfypeer-mcp
cp .env.example .env   # fill PYMTHOUSE_* and SIGNER_URL
uv sync
uv run comfypeer-mcp
```

Cursor:

```json
{
  "mcpServers": {
    "livepeer-mcp-local": {
      "url": "http://127.0.0.1:8090/mcp",
      "headers": {
        "Authorization": "Bearer <pymthouse-user-api-key>"
      }
    }
  }
}
```

## Tools

| Tool | Purpose |
| --- | --- |
| `list_network_capabilities` | discovery-service catalog |
| `query_network_orchestrators` | ranked orch query |
| `create_signer_session` | SignerSession + base64 `--token` for other SDK scripts |
| `live_runner_call_tool` | reserve → call → stop |
| `byoc_*` / `lv2v_*` | batch and live-video-to-video |

`ALLOW_LOOPBACK_DISCOVERY=1` by default so `http://localhost:8935/discovery` works.

## Tests

```bash
uv run pytest -q
```
