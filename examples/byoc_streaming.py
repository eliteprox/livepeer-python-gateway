"""
Example demonstrating streaming BYOC job execution.

This example shows how to:
1. Connect to an orchestrator and discover BYOC capabilities
2. Start a streaming BYOC job (like comfystream)
3. Handle job token refresh during the stream (~1 minute interval)
4. Publish media frames and receive processed output
5. Stop the stream gracefully

Usage:
    python byoc_streaming.py localhost:8935 --capability comfystream
    python byoc_streaming.py localhost:8935 --capability comfystream --signer http://localhost:8937
    python byoc_streaming.py localhost:8935 --capability comfystream --input /path/to/video.mp4
"""
import argparse
import asyncio
import logging
import signal
import sys

# Configure logging to show INFO level messages
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

from livepeer_gateway import (
    BYOCTokenRefreshConfig,
    BYOCTokenRefresher,
    GetOrchestratorInfo,
    LivepeerGatewayError,
    MediaPublishConfig,
    StartBYOCStream,
    StopBYOCStream,
    fetch_external_capabilities,
)

DEFAULT_ORCH = "localhost:8935"
DEFAULT_TOKEN_REFRESH_INTERVAL = 60.0  # BYOC uses job token refresh (~1 minute)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Start a streaming BYOC job.")
    p.add_argument(
        "orchestrator",
        nargs="?",
        default=DEFAULT_ORCH,
        help=f"Orchestrator gRPC target (host:port). Default: {DEFAULT_ORCH}",
    )
    p.add_argument(
        "--capability",
        required=True,
        help="Name of the BYOC capability to use (e.g., 'comfystream')",
    )
    p.add_argument(
        "--signer",
        default=None,
        help="Remote signer base URL for payments. If omitted, runs in offchain mode.",
    )
    p.add_argument(
        "--input",
        default=None,
        help="Input video file path (optional)",
    )
    p.add_argument(
        "--output",
        default=None,
        help="Output directory for processed frames (optional)",
    )
    p.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Stream duration in seconds (default: 30)",
    )
    p.add_argument(
        "--no-video-ingress",
        action="store_true",
        help="Disable video input channel",
    )
    p.add_argument(
        "--no-video-egress",
        action="store_true",
        help="Disable video output channel",
    )
    p.add_argument(
        "--enable-data-output",
        action="store_true",
        help="Enable data output channel",
    )
    return p.parse_args()


async def run_stream(args: argparse.Namespace) -> None:
    """Main async function to run the streaming BYOC job."""
    print(f"Connecting to orchestrator: {args.orchestrator}")
    print()

    # Get orchestrator info
    info = GetOrchestratorInfo(args.orchestrator, signer_url=args.signer)
    print(f"Orchestrator: {info.transcoder}")
    print(f"ETH Address: {info.address.hex()}")
    print()

    # List external capabilities (from HTTP endpoint)
    ext_caps = fetch_external_capabilities(args.orchestrator)
    if not ext_caps:
        print("No external/BYOC capabilities available on this orchestrator.")
        sys.exit(1)

    print("Available BYOC capabilities:")
    for cap in ext_caps:
        available = cap.capacity_available
        print(f"  - {cap.name}: capacity={cap.capacity}, available={available}")
    print()

    # Check if requested capability exists
    cap_names = [c.name for c in ext_caps]
    if args.capability not in cap_names:
        print(f"Error: Capability '{args.capability}' not found on orchestrator.")
        print(f"Available capabilities: {', '.join(cap_names)}")
        sys.exit(1)

    # Start the streaming BYOC job
    print(f"Starting streaming BYOC job: capability={args.capability}")
    enable_video_ingress = not args.no_video_ingress
    enable_video_egress = not args.no_video_egress
    print(f"  Video ingress: {enable_video_ingress}")
    print(f"  Video egress: {enable_video_egress}")
    print(f"  Data output: {args.enable_data_output}")
    print()

    # Stream parameters (customize based on your BYOC worker)
    params = {
        "workflow": "default",  # Example parameter
    }

    stream_job = StartBYOCStream(
        info,
        args.capability,
        params,
        signer_base_url=args.signer,
        enable_video_ingress=enable_video_ingress,
        enable_video_egress=enable_video_egress,
        enable_data_output=args.enable_data_output,
    )

    print(f"Stream started: stream_id={stream_job.stream_id}")
    print(f"  Publish URL: {stream_job.publish_url or 'N/A'}")
    print(f"  Subscribe URL: {stream_job.subscribe_url or 'N/A'}")
    print(f"  Control URL: {stream_job.control_url or 'N/A'}")
    print(f"  Events URL: {stream_job.events_url or 'N/A'}")
    print(f"  Data URL: {stream_job.data_url or 'N/A'}")
    print()

    # Start token refresher if signer is provided (BYOC uses job token refresh, not live payments)
    token_refresher = None
    if args.signer and stream_job.signed_job_request:
        print(f"Starting job token refresher (interval={DEFAULT_TOKEN_REFRESH_INTERVAL}s)...")

        # Create refresh callback for handling HTTP 480 (session refresh required)
        def refresh_orch_info():
            return GetOrchestratorInfo(args.orchestrator, args.signer)

        token_refresher = BYOCTokenRefresher(
            args.signer,
            info,
            args.capability,
            stream_job.stream_id,
            stream_job.signed_job_request,
            config=BYOCTokenRefreshConfig(interval_s=DEFAULT_TOKEN_REFRESH_INTERVAL),
            refresh_info_callback=refresh_orch_info,
        )
        token_refresher.start()

    # Setup signal handler for graceful shutdown
    stop_event = asyncio.Event()

    def signal_handler(sig, frame):
        print("\nReceived interrupt signal, stopping stream...")
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # If input file is provided, start media publishing
        media_publisher = None
        if args.input and stream_job.publish_url:
            print(f"Starting media publish from: {args.input}")
            config = MediaPublishConfig(
                fps=30.0,
                width=1280,
                height=720,
            )
            media_publisher = stream_job.start_media_publish(config)
            # Note: You would typically start the publisher in a separate task
            # For this example, we'll just show how to set it up

        # If output directory is provided, start media output
        media_output = None
        if args.output and stream_job.subscribe_url:
            print(f"Starting media output to: {args.output}")
            media_output = stream_job.media_output(
                output_path=args.output,
                max_frames=-1,  # Unlimited
            )
            # Note: You would typically run the output subscriber in a separate task

        # Run for the specified duration or until interrupted
        print(f"Streaming for {args.duration} seconds...")
        print("Press Ctrl+C to stop early.")
        print()

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=args.duration)
        except asyncio.TimeoutError:
            print("Stream duration completed.")

    finally:
        # Stop token refresher
        if token_refresher:
            print("Stopping token refresher...")
            await token_refresher.stop()

        # Stop the stream
        print(f"Stopping stream: {stream_job.stream_id}")
        try:
            await StopBYOCStream(
                info,
                stream_job.stream_id,
                args.capability,
                signer_base_url=args.signer,
            )
            print("Stream stopped successfully.")
        except Exception as e:
            print(f"Warning: Error stopping stream: {e}")


def main() -> None:
    args = _parse_args()

    try:
        asyncio.run(run_stream(args))
    except LivepeerGatewayError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
