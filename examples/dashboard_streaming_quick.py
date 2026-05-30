"""
Quick smoke test: OIDC device login → Dashboard device exchange → start_lv2v.

Run from the repo root:
  uv run examples/dashboard_streaming_quick.py

Or use a VS Code launch config (local facade or pymthouse.com).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os

from facade_cli import DEFAULT_STREAM_DURATION_S, add_facade_args, facade_start_lv2v_kwargs
from livepeer_gateway.lv2v import StartJobRequest, start_lv2v
from livepeer_gateway.media_publish import MediaPublishConfig, VideoOutputConfig

DEFAULT_MODEL_ID = "streamdiffusion"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Dashboard facade streaming smoke test (OIDC device + exchange).",
    )
    add_facade_args(p, dev_defaults=True)
    p.add_argument(
        "--signer-url",
        default=os.environ.get("LIVEPEER_SIGNER_BASE_URL"),
        dest="signer",
        help="Clearinghouse DMZ / remote signer base URL",
    )
    p.add_argument(
        "--model",
        default=os.environ.get("LIVEPEER_MODEL_ID", DEFAULT_MODEL_ID),
        help="Pipeline model / discovery capability id",
    )
    p.add_argument(
        "--duration",
        type=float,
        default=float(
            os.environ.get("LIVEPEER_STREAM_DURATION", DEFAULT_STREAM_DURATION_S),
        ),
        help="Keep the job open this many seconds before closing (default: 120)",
    )
    return p.parse_args()


async def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    args = _parse_args()

    job = start_lv2v(
        orch_url=None,
        req=StartJobRequest(model_id=args.model),
        **facade_start_lv2v_kwargs(args, args.model),
    )

    print("publish_url:", job.publish_url)
    print("subscribe_url:", job.subscribe_url)

    job.start_payment_sender()
    job.start_media(MediaPublishConfig(tracks=[VideoOutputConfig(fps=30.0)]))
    print(f"Streaming for {args.duration:.0f}s (Ctrl+C to stop early)...")
    try:
        await asyncio.sleep(args.duration)
    except KeyboardInterrupt:
        print("Interrupted")
    finally:
        await job.close()


if __name__ == "__main__":
    asyncio.run(main())
