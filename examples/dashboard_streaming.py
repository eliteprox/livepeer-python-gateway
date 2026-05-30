"""
Live streaming via the Dashboard facade (OIDC device login + signer exchange).

Requires:
  uv sync
  uv sync --extra examples   # for camera_capture-style media publishing

Environment defaults match local signer/docker-compose:
  Dashboard:      http://localhost:3001
  OIDC issuer:    http://127.0.0.1:8080/realms/clearinghouse
  Signer (DMZ):   http://127.0.0.1:8080
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from livepeer_gateway.lv2v import StartJobRequest, start_lv2v
from livepeer_gateway.media_publish import MediaPublishConfig, VideoOutputConfig

DEFAULT_BILLING_URL = "http://localhost:3001"
DEFAULT_ISSUER_URL = "http://127.0.0.1:8080/realms/clearinghouse"
DEFAULT_SIGNER_URL = "http://127.0.0.1:8080"
DEFAULT_CLIENT_ID = "app_demo"
DEFAULT_MODEL_ID = "streamdiffusion-sdxl-v2v"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Start a live-video job using Dashboard OIDC device exchange.",
    )
    p.add_argument(
        "--billing-url",
        default=DEFAULT_BILLING_URL,
        help=f"Dashboard origin for device exchange (default: {DEFAULT_BILLING_URL})",
    )
    p.add_argument(
        "--issuer-url",
        default=DEFAULT_ISSUER_URL,
        help=f"Clearinghouse OIDC issuer URL (default: {DEFAULT_ISSUER_URL})",
    )
    p.add_argument(
        "--signer-url",
        default=DEFAULT_SIGNER_URL,
        help=f"Remote signer / DMZ base URL (default: {DEFAULT_SIGNER_URL})",
    )
    p.add_argument(
        "--client-id",
        default=DEFAULT_CLIENT_ID,
        dest="client_id",
        help=f"Public OIDC app client id (default: {DEFAULT_CLIENT_ID})",
    )
    p.add_argument(
        "--model",
        default=DEFAULT_MODEL_ID,
        help=f"Pipeline model id (default: {DEFAULT_MODEL_ID})",
    )
    p.add_argument(
        "--browser",
        action="store_true",
        help="Use browser PKCE login instead of RFC 8628 device flow",
    )
    return p.parse_args()


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    discovery_url = (
        f"{args.signer_url.rstrip('/')}/discover-orchestrators"
        f"?cap={args.model}"
    )

    job = start_lv2v(
        orch_url=None,
        req=StartJobRequest(model_id=args.model),
        billing_url=args.billing_url,
        issuer_url=args.issuer_url,
        signer_url=args.signer_url,
        discovery_url=discovery_url,
        oidc_client_id=args.client_id,
        headless=not args.browser,
    )

    print("publish_url:", job.publish_url)
    print("subscribe_url:", job.subscribe_url)

    media = job.start_media(MediaPublishConfig(tracks=[VideoOutputConfig(fps=30.0)]))
    print("Media publish ready (feed frames with media.write_frame). Press Ctrl+C to stop.")
    try:
        while True:
            await asyncio.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        await job.close()


if __name__ == "__main__":
    asyncio.run(main())
