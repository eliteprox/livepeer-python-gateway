import argparse
import asyncio
import logging
import os
import sys
from fractions import Fraction

import av

from facade_cli import add_facade_args, facade_start_lv2v_kwargs, resolve_discovery_url, resolve_signer_url

from livepeer_gateway.errors import LivepeerGatewayError, NoOrchestratorAvailableError
from livepeer_gateway.lv2v import StartJobRequest, start_lv2v
from livepeer_gateway.media_publish import MediaPublishConfig, VideoOutputConfig

DEFAULT_MODEL_ID = "streamdiffusion-sdxl"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Start an LV2V job and publish raw frames via publish_url.")
    p.add_argument(
        "orchestrator",
        nargs="?",
        default=None,
        help="Orchestrator (host:port). If omitted, discovery is used.",
    )
    p.add_argument(
        "--signer",
        default=os.environ.get("LIVEPEER_SIGNER_BASE_URL"),
        help="Remote signer URL (no path). If omitted, runs in offchain mode unless facade auth is used.",
    )
    p.add_argument(
        "--token",
        default=None,
        help="Base64-encoded gateway token (signer, discovery, headers); overrides missing signer/orchestrator.",
    )
    add_facade_args(p)
    p.add_argument(
        "--model",
        default=os.environ.get("LIVEPEER_MODEL_ID", DEFAULT_MODEL_ID),
        help=f"Pipeline model to start via /live-video-to-video. Default: {DEFAULT_MODEL_ID}",
    )
    p.add_argument("--width", type=int, default=320, help="Frame width (default: 320).")
    p.add_argument("--height", type=int, default=180, help="Frame height (default: 180).")
    p.add_argument("--fps", type=float, default=30.0, help="Frames per second (default: 30).")
    p.add_argument("--count", type=int, default=90, help="Number of frames to send (default: 90).")
    p.add_argument(
        "--debug",
        action="store_true",
        help="Enable DEBUG logging (or set LOG_LEVEL=DEBUG).",
    )
    return p.parse_args()


def _configure_logging(args: argparse.Namespace) -> None:
    level = logging.DEBUG if args.debug else os.environ.get("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=getattr(logging, str(level).upper(), logging.INFO),
        format="%(levelname)s:%(name)s:%(message)s",
    )


def _solid_rgb_frame(width: int, height: int, rgb: tuple[int, int, int]) -> av.VideoFrame:
    frame = av.VideoFrame(width, height, "rgb24")
    r, g, b = rgb
    frame.planes[0].update(bytes([r, g, b]) * (width * height))
    return frame


async def main() -> None:
    args = _parse_args()
    _configure_logging(args)
    frame_interval = 1.0 / max(1e-6, args.fps)

    job = None
    try:
        lv2v_kwargs = facade_start_lv2v_kwargs(args, args.model)
        if "signer_url" not in lv2v_kwargs:
            lv2v_kwargs["signer_url"] = resolve_signer_url(args)
        if "discovery_url" not in lv2v_kwargs:
            lv2v_kwargs["discovery_url"] = resolve_discovery_url(args, args.model)

        job = start_lv2v(
            args.orchestrator,
            StartJobRequest(model_id=args.model),
            token=args.token,
            **lv2v_kwargs,
        )

        print("=== LiveVideoToVideo ===")
        print("publish_url:", job.publish_url)
        print("subscribe_url:", job.subscribe_url)
        print()

        job.start_payment_sender()
        media = job.start_media(
            MediaPublishConfig(
                tracks=[VideoOutputConfig(fps=args.fps)],
            )
        )

        time_base = Fraction(1, int(round(args.fps)))
        for i in range(max(0, args.count)):
            color = (i * 5) % 255
            frame = _solid_rgb_frame(args.width, args.height, (color, 0, 255 - color))
            frame.pts = i
            frame.time_base = time_base
            await media.write_frame(frame)
            await asyncio.sleep(frame_interval)
    except LivepeerGatewayError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        if isinstance(e, NoOrchestratorAvailableError) and e.rejections:
            print(
                f"({len(e.rejections)} orchestrators tried; see list above or re-run with --debug)",
                file=sys.stderr,
            )
    finally:
        if job is not None:
            await job.close()


if __name__ == "__main__":
    asyncio.run(main())
