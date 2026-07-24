# Local Livepeer MCP client

Run this example next to `livepeer-python-gateway` when you want Cursor (or any MCP client) to call Livepeer network tools **on your machine** — discovery, signer session mint via PymtHouse, and execution (`run_capability`, `start_stream`, `call_live_runner`).

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

Storyboard-aligned network verbs (product tools like `generate_project` stay in Storyboard):

| Tool | Purpose |
| --- | --- |
| `list_capabilities` | discovery-service catalog |
| `query_orchestrators` | ranked orch query |
| `create_signer_session` | SignerSession + base64 `--token` for other SDK scripts |
| `run_capability` | BYOC `/inference` (Storyboard analogue: `create_media`) |
| `submit_training` / `get_job_status` | async BYOC training |
| `start_stream` / `write_stream_control` / `stop_stream` | live-video-to-video |
| `call_live_runner` | reserve → HTTP app call → stop |
| `livepeer_mcp_info` | host metadata |

`ALLOW_LOOPBACK_DISCOVERY=1` by default so `http://localhost:8935/discovery` works.

## Tests

```bash
uv run pytest -q
```
