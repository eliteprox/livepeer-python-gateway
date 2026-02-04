import argparse
import asyncio
from fractions import Fraction

import av

from livepeer_gateway import (
    BYOCTokenRefreshConfig,
    BYOCTokenRefresher,
    GetBYOCJobToken,
    GetOrchestratorInfo,
    LivePaymentConfig,
    LivepeerGatewayError,
    MediaPublishConfig,
    OrchestratorSession,
    StartBYOCStreamWithRetry,
    StartJobRequest,
    StopBYOCStream,
    get_external_capabilities,
)

DEFAULT_ORCH = "localhost:8935"
DEFAULT_MODEL_ID = "noop"
DEFAULT_PAYMENT_INTERVAL = 5.0  # For LV2V live payments
DEFAULT_TOKEN_REFRESH_INTERVAL = 60.0  # For BYOC job token refresh


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Start an LV2V or BYOC streaming job and publish raw frames via publish_url."
    )
    p.add_argument(
        "orchestrator",
        nargs="?",
        default=DEFAULT_ORCH,
        help=f"Orchestrator gRPC target (host:port). Default: {DEFAULT_ORCH}",
    )
    p.add_argument(
        "--signer",
        default=None,
        help="Remote signer base URL (no path). If omitted, runs in offchain mode.",
    )

    # Mode selection: either LV2V (--model-id) or BYOC (--capability)
    mode_group = p.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--model-id",
        default=None,
        help=f"Pipeline model_id for LV2V mode via /live-video-to-video. Default: {DEFAULT_MODEL_ID}",
    )
    mode_group.add_argument(
        "--capability",
        default=None,
        help="External/BYOC capability name (e.g., 'comfystream'). Uses BYOC streaming mode.",
    )

    p.add_argument("--width", type=int, default=320, help="Frame width (default: 320).")
    p.add_argument("--height", type=int, default=180, help="Frame height (default: 180).")
    p.add_argument("--fps", type=float, default=30.0, help="Frames per second (default: 30).")
    p.add_argument("--count", type=int, default=90, help="Number of frames to send (default: 90).")
    p.add_argument(
        "--payment-interval",
        type=float,
        default=DEFAULT_PAYMENT_INTERVAL,
        help=f"Live payment interval in seconds for LV2V mode (default: {DEFAULT_PAYMENT_INTERVAL}).",
    )
    p.add_argument(
        "--token-refresh-interval",
        type=float,
        default=DEFAULT_TOKEN_REFRESH_INTERVAL,
        help=f"Job token refresh interval in seconds for BYOC mode (default: {DEFAULT_TOKEN_REFRESH_INTERVAL}).",
    )
    return p.parse_args()


def _solid_rgb_frame(width: int, height: int, rgb: tuple[int, int, int]) -> av.VideoFrame:
    frame = av.VideoFrame(width, height, "rgb24")
    r, g, b = rgb
    frame.planes[0].update(bytes([r, g, b]) * (width * height))
    return frame


async def run_lv2v_mode(args: argparse.Namespace) -> None:
    """Run in Live Video to Video (LV2V) mode."""
    frame_interval = 1.0 / max(1e-6, args.fps)
    model_id = args.model_id or DEFAULT_MODEL_ID

    # Configure live payments if signer is provided
    live_payment_config = None
    if args.signer:
        live_payment_config = LivePaymentConfig(
            interval_s=args.payment_interval,
            width=args.width,
            height=args.height,
            fps=args.fps,
        )

    session = OrchestratorSession(
        args.orchestrator,
        signer_url=args.signer,
        live_payment_config=live_payment_config,
    )

    try:
        job = session.start_job(StartJobRequest(model_id=model_id))

        print("=== LiveVideoToVideo ===")
        print("model_id:", model_id)
        print("publish_url:", job.publish_url)
        print("subscribe_url:", job.subscribe_url)
        if args.signer and live_payment_config:
            print(f"live payments: enabled (interval={args.payment_interval}s)")
        print()

        media = job.start_media(MediaPublishConfig(fps=args.fps))

        time_base = Fraction(1, int(round(args.fps)))
        for i in range(max(0, args.count)):
            color = (i * 5) % 255
            frame = _solid_rgb_frame(args.width, args.height, (color, 0, 255 - color))
            frame.pts = i
            frame.time_base = time_base
            await media.write_frame(frame)
            await asyncio.sleep(frame_interval)

        print(f"Sent {args.count} frames successfully")
    finally:
        await session.aclose()


async def run_byoc_mode(args: argparse.Namespace) -> None:
    """Run in BYOC (External Capability) mode."""
    frame_interval = 1.0 / max(1e-6, args.fps)

    print(f"Connecting to orchestrator: {args.orchestrator}")

    # Get orchestrator info
    info = GetOrchestratorInfo(args.orchestrator, signer_url=args.signer)

    # Verify the capability exists
    ext_caps = get_external_capabilities(info)
    cap_names = [c.name for c in ext_caps]
    if args.capability not in cap_names:
        available = ", ".join(cap_names) if cap_names else "none"
        raise LivepeerGatewayError(
            f"Capability '{args.capability}' not found. Available: {available}"
        )

    # Find the capability info for display
    cap_info = next((c for c in ext_caps if c.name == args.capability), None)

    # Start BYOC stream (uses retry logic for transient errors)
    params = {}  # Add any workflow-specific params here
    stream_job = StartBYOCStreamWithRetry(
        info,
        args.capability,
        params,
        signer_base_url=args.signer,
        enable_video_ingress=True,
        enable_video_egress=True,
        max_retries=2,
    )

    print("=== BYOC Stream ===")
    print("capability:", args.capability)
    if cap_info:
        print(f"  description: {cap_info.description or 'N/A'}")
    # Fetch job token to display accurate pricing for this sender
    if args.signer:
        try:
            job_token = GetBYOCJobToken(info, args.capability, args.signer)
            print(f"  price: {job_token.price_per_unit} wei per {job_token.pixels_per_unit} unit(s)")
            print(f"  balance: {job_token.balance}")
            print(f"  available_capacity: {job_token.available_capacity}")
        except Exception as e:
            print(f"  price: (could not fetch job token: {e})")
    print("stream_id:", stream_job.stream_id)
    print("publish_url:", stream_job.publish_url)
    print("subscribe_url:", stream_job.subscribe_url)
    print("control_url:", stream_job.control_url)
    print("events_url:", stream_job.events_url)

    # Start token refresher if signer is provided
    token_refresher = None
    if args.signer:
        print(f"job token refresh: enabled (interval={args.token_refresh_interval}s)")
        token_refresher = BYOCTokenRefresher(
            args.signer,
            info,
            args.capability,
            stream_job.stream_id,
            config=BYOCTokenRefreshConfig(interval_s=args.token_refresh_interval),
        )
        token_refresher.start()
    print()

    try:
        # Start media publishing
        if not stream_job.publish_url:
            raise LivepeerGatewayError("No publish_url returned - video ingress not enabled")

        media = stream_job.start_media_publish(MediaPublishConfig(fps=args.fps))

        time_base = Fraction(1, int(round(args.fps)))
        for i in range(max(0, args.count)):
            color = (i * 5) % 255
            frame = _solid_rgb_frame(args.width, args.height, (color, 0, 255 - color))
            frame.pts = i
            frame.time_base = time_base
            await media.write_frame(frame)
            await asyncio.sleep(frame_interval)

        print(f"Sent {args.count} frames successfully")
    finally:
        # Stop token refresher
        if token_refresher:
            await token_refresher.stop()

        # Stop the BYOC stream
        print(f"Stopping stream: {stream_job.stream_id}")
        try:
            await StopBYOCStream(
                info,
                stream_job.stream_id,
                args.capability,
                signer_base_url=args.signer,
            )
        except Exception as e:
            print(f"Warning: Error stopping stream: {e}")


async def main() -> None:
    args = _parse_args()

    try:
        if args.capability:
            # BYOC mode
            await run_byoc_mode(args)
        else:
            # LV2V mode (default)
            await run_lv2v_mode(args)
    except LivepeerGatewayError as e:
        print(f"ERROR ({args.orchestrator}): {e}")


if __name__ == "__main__":
    asyncio.run(main())

