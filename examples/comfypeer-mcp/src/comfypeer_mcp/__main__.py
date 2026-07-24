from __future__ import annotations

import uvicorn

from comfypeer_mcp.config import get_settings
from comfypeer_mcp.server import create_http_app


def main() -> None:
    settings = get_settings()
    app = create_http_app()
    uvicorn.run(
        app,
        host=settings.mcp_host,
        port=settings.mcp_port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
