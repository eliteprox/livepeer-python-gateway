"""
Stream a video file through a BYOC capability (e.g. comfystream) and optionally
record the processed output to a local file.

This example shows how to:
1. Open a local video file and decode frames
2. Start a streaming BYOC job (e.g. comfystream)
3. Publish decoded frames at the source frame rate
4. Subscribe to the processed output and record to an MP4 file
5. Handle job token refresh and graceful shutdown

Usage:
    # Basic: stream a video file through comfystream (no recording)
    python stream_video_file.py localhost:8935 --capability comfystream --input video.mp4

    # With a ComfyUI API-format workflow file
    python stream_video_file.py localhost:8935 --capability comfystream --input video.mp4 \\
        --workflow workflow-api.json

    # Record the processed output to a file
    python stream_video_file.py localhost:8935 --capability comfystream --input video.mp4 --record output.mp4

    # Loop the video continuously until Ctrl+C
    python stream_video_file.py localhost:8935 --capability comfystream --input video.mp4 --loop

    # With payments via remote signer and workflow
    python stream_video_file.py localhost:8935 --capability comfystream --input video.mp4 \\
        --signer http://localhost:8937 --workflow workflow-api.json --record output.mp4
"""

import argparse
import asyncio
import json
import logging
import signal
import time
from contextlib import suppress
from dataclasses import dataclass
from fractions import Fraction
from typing import Optional

import av

from livepeer_gateway import (
    BYOCStreamRequest,
    BYOCTokenRefreshConfig,
    BYOCTokenRefresher,
    GetBYOCJobToken,
    GetOrchestratorInfo,
    LivepeerGatewayError,
    MediaOutput,
    MediaPublishConfig,
    StartBYOCStreamWithRetry,
    StopBYOCStream,
    TrickleSubscriber,
    fetch_external_capabilities,
)

# Configure logging to show INFO level messages
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

DEFAULT_ORCH = "localhost:8935"
DEFAULT_TOKEN_REFRESH_INTERVAL = 60.0
_TIME_BASE = 90_000


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Stream a video file through a BYOC capability and optionally record output."
    )
    p.add_argument(
        "orchestrator",
        nargs="?",
        default=DEFAULT_ORCH,
        help=f"Orchestrator gRPC target (host:port). Default: {DEFAULT_ORCH}",
    )
    p.add_argument(
        "--capability",
        default="comfystream",
        help="BYOC capability name (default: comfystream).",
    )
    p.add_argument(
        "--signer",
        default=None,
        help="Remote signer base URL (no path). If omitted, runs in offchain mode.",
    )
    p.add_argument(
        "--input",
        required=True,
        dest="input_file",
        help="Path to the input video file.",
    )
    p.add_argument(
        "--workflow",
        default=None,
        metavar="JSON_FILE",
        help="Path to a ComfyUI API-format workflow JSON file. "
        "The workflow is sent as the 'prompts' key in the stream start params.",
    )
    p.add_argument(
        "--params",
        default=None,
        metavar="JSON",
        help="Additional stream params as a JSON string (merged into params dict). "
        "Example: '{\"width\": 512, \"height\": 512}'",
    )
    p.add_argument(
        "--record",
        default=None,
        metavar="OUTPUT_FILE",
        help="Record the processed output to a file (e.g. output.mp4).",
    )
    p.add_argument(
        "--loop",
        action="store_true",
        help="Loop the input video continuously until stopped.",
    )
    p.add_argument(
        "--fps",
        type=float,
        default=None,
        help="Override the source frame rate. If omitted, uses the video's native FPS.",
    )
    p.add_argument(
        "--enable-data-output",
        action="store_true",
        help="Enable data output channel (e.g. for text/transcription output).",
    )
    p.add_argument(
        "--token-refresh-interval",
        type=float,
        default=DEFAULT_TOKEN_REFRESH_INTERVAL,
        help=f"Job token refresh interval in seconds for BYOC mode (default: {DEFAULT_TOKEN_REFRESH_INTERVAL}).",
    )
    return p.parse_args()


@dataclass
class _MediaInfo:
    fps: float
    width: int
    height: int
    has_audio: bool
    audio_sample_rate: Optional[int]
    audio_layout: Optional[str]


def _probe_media(path: str) -> _MediaInfo:
    """Open the media file briefly to read video FPS/resolution and audio info."""
    container = av.open(path)
    stream = container.streams.video[0]
    fps = float(stream.average_rate or stream.guessed_rate or 30)
    width = stream.codec_context.width
    height = stream.codec_context.height

    has_audio = bool(container.streams.audio)
    audio_sample_rate: Optional[int] = None
    audio_layout: Optional[str] = None
    if has_audio:
        audio_stream = container.streams.audio[0]
        audio_sample_rate = audio_stream.codec_context.sample_rate
        audio_layout = audio_stream.layout.name if audio_stream.layout else "stereo"

    container.close()
    return _MediaInfo(
        fps=fps,
        width=width,
        height=height,
        has_audio=has_audio,
        audio_sample_rate=audio_sample_rate,
        audio_layout=audio_layout,
    )


def _iter_media_frames(path: str, loop: bool = False, include_audio: bool = False):
    """Yield decoded video and audio frames from a file. Optionally loop forever."""
    while True:
        container = av.open(path)
        streams = [container.streams.video[0]]
        if include_audio and container.streams.audio:
            streams.append(container.streams.audio[0])

        for packet in container.demux(*streams):
            for frame in packet.decode():
                yield frame

        container.close()
        if not loop:
            break


async def _record_output(
    subscribe_url: str,
    output_path: str,
    fps: float,
    width: int,
    height: int,
    stop_event: asyncio.Event,
    audio_sample_rate: Optional[int] = None,
    audio_layout: Optional[str] = None,
) -> None:
    """Subscribe to the processed output stream and record frames to a video file."""
    media_out = MediaOutput(subscribe_url)
    output_container = None
    video_stream = None
    audio_stream = None
    audio_resampler = None
    video_frame_count = 0
    audio_frame_count = 0
    audio_samples_written = 0

    try:
        async for decoded in media_out.frames():
            if stop_event.is_set():
                break

            frame = decoded.frame

            if decoded.kind == "video":
                # Lazy-init the output container on first video frame so we match
                # the actual output resolution (which may differ from the input).
                if output_container is None:
                    output_container = av.open(output_path, mode="w")
                    video_stream = output_container.add_stream("libx264", rate=int(fps))
                    video_stream.width = frame.width
                    video_stream.height = frame.height
                    video_stream.pix_fmt = "yuv420p"

                    # Add audio output stream if audio is expected
                    if audio_sample_rate is not None:
                        audio_stream = output_container.add_stream(
                            "aac",
                            rate=audio_sample_rate,
                            layout=audio_layout or "stereo",
                        )

                    audio_tag = "+audio" if audio_stream else ""
                    print(f"Recording to {output_path} ({frame.width}x{frame.height} @ {int(fps)} fps{audio_tag})")

                # Re-encode the video frame into the output container
                frame_for_encode = frame.reformat(format="yuv420p")
                frame_for_encode.pts = video_frame_count
                frame_for_encode.time_base = Fraction(1, int(fps))
                for packet in video_stream.encode(frame_for_encode):
                    output_container.mux(packet)

                video_frame_count += 1
                if video_frame_count % 100 == 0:
                    msg = f"  recorded {video_frame_count} video"
                    if audio_frame_count > 0:
                        msg += f" + {audio_frame_count} audio"
                    msg += " output frames"
                    print(msg)

            elif decoded.kind == "audio" and audio_stream is not None:
                # Lazy-init audio resampler for format conversion
                if audio_resampler is None:
                    codec_ctx = audio_stream.codec_context
                    audio_resampler = av.AudioResampler(
                        format=codec_ctx.format,
                        layout=codec_ctx.layout,
                        rate=codec_ctx.sample_rate,
                    )

                resampled = audio_resampler.resample(frame)
                for rf in resampled:
                    rf.pts = audio_samples_written
                    audio_samples_written += rf.samples
                    for packet in audio_stream.encode(rf):
                        output_container.mux(packet)

                audio_frame_count += 1

    except LivepeerGatewayError as e:
        logging.warning("Output subscribe error: %s", e)
    except asyncio.CancelledError:
        pass
    finally:
        if output_container is not None:
            # Flush video encoder
            if video_stream is not None:
                for packet in video_stream.encode():
                    output_container.mux(packet)
            # Flush audio resampler + encoder
            if audio_stream is not None:
                if audio_resampler is not None:
                    for rf in audio_resampler.resample(None):
                        rf.pts = audio_samples_written
                        audio_samples_written += rf.samples
                        for packet in audio_stream.encode(rf):
                            output_container.mux(packet)
                for packet in audio_stream.encode():
                    output_container.mux(packet)
            output_container.close()

        msg = f"Recording finished: {video_frame_count} video"
        if audio_frame_count > 0:
            msg += f" + {audio_frame_count} audio"
        msg += f" frames written to {output_path}"
        print(msg)


async def _subscribe_data_channel(
    data_url: str,
    stop_event: asyncio.Event,
) -> None:
    """Subscribe to the data output channel and print text segments as they arrive."""
    sub = TrickleSubscriber(data_url)
    segment_count = 0

    try:
        while not stop_event.is_set():
            segment = await sub.next()
            if segment is None:
                break

            # Read all chunks for this segment
            data = b""
            while True:
                chunk = await segment.read()
                if chunk is None:
                    break
                data += chunk

            if data:
                segment_count += 1
                text = data.decode("utf-8", errors="replace")
                print(f"  [data #{segment_count}] {text}")

    except asyncio.CancelledError:
        pass
    except Exception as e:
        logging.warning("Data channel error: %s", e)
    finally:
        await sub.close()
        print(f"Data channel finished: {segment_count} segments received")


async def run(args: argparse.Namespace) -> None:
    print(f"Input file  : {args.input_file}")
    print(f"Capability  : {args.capability}")
    print(f"Orchestrator: {args.orchestrator}")
    print()

    # Probe the media file to get native FPS, resolution, and audio info
    media_info = _probe_media(args.input_file)
    fps = args.fps if args.fps else media_info.fps
    width, height = media_info.width, media_info.height
    frame_interval = 1.0 / max(1e-6, fps)
    print(f"Video: {width}x{height} @ {fps:.2f} fps (native {media_info.fps:.2f})")
    if media_info.has_audio:
        print(f"Audio: {media_info.audio_sample_rate} Hz, layout={media_info.audio_layout}")
    else:
        print("Audio: none")
    print()

    # --- Connect to orchestrator and verify capability ---
    print(f"Connecting to orchestrator: {args.orchestrator}")
    info = GetOrchestratorInfo(args.orchestrator, signer_url=args.signer)

    ext_caps = fetch_external_capabilities(args.orchestrator)
    cap_names = [c.name for c in ext_caps]
    if args.capability not in cap_names:
        available = ", ".join(cap_names) if cap_names else "none"
        raise LivepeerGatewayError(
            f"Capability '{args.capability}' not found. Available: {available}"
        )

    cap_info = next((c for c in ext_caps if c.name == args.capability), None)

    # --- Build stream params ---
    stream_params: dict = {}

    # Load workflow from JSON file if provided
    if args.workflow:
        with open(args.workflow) as f:
            workflow = json.load(f)
        stream_params["prompts"] = workflow
        print(f"Workflow    : {args.workflow} ({len(workflow)} nodes)")

    # Set width/height from video probe (can be overridden by --params)
    stream_params["width"] = width
    stream_params["height"] = height

    # Merge any additional params from --params
    if args.params:
        extra = json.loads(args.params)
        stream_params.update(extra)

    # --- Start BYOC stream ---
    stream_req = BYOCStreamRequest(
        capability=args.capability,
        params=stream_params,
        enable_video_ingress=True,
        enable_video_egress=True,
        enable_data_output=args.enable_data_output,
    )
    stream_job = StartBYOCStreamWithRetry(
        info,
        stream_req,
        signer_base_url=args.signer,
        max_retries=2,
    )

    print("=== BYOC Stream ===")
    print("capability  :", args.capability)
    if cap_info:
        print(f"  description: {cap_info.description or 'N/A'}")
    if args.signer:
        try:
            job_token = GetBYOCJobToken(info, args.capability, args.signer)
            print(f"  price: {job_token.price_per_unit} wei per {job_token.pixels_per_unit} unit(s)")
            print(f"  balance: {job_token.balance}")
        except Exception as e:
            print(f"  price: (could not fetch job token: {e})")
    print("stream_id   :", stream_job.stream_id)
    print("publish_url :", stream_job.publish_url)
    print("subscribe_url:", stream_job.subscribe_url)
    print("control_url :", stream_job.control_url)
    print("events_url  :", stream_job.events_url)
    print("data_url    :", stream_job.data_url or "N/A")

    # --- Token refresher (payments) ---
    token_refresher = None
    if args.signer and stream_job.signed_job_request:
        print(f"job token refresh: enabled (interval={args.token_refresh_interval}s)")

        def refresh_orch_info():
            return GetOrchestratorInfo(args.orchestrator, args.signer)

        token_refresher = BYOCTokenRefresher(
            args.signer,
            info,
            args.capability,
            stream_job.stream_id,
            stream_job.signed_job_request,
            config=BYOCTokenRefreshConfig(interval_s=args.token_refresh_interval),
            refresh_info_callback=refresh_orch_info,
        )
        token_refresher.start()
    print()

    # --- Publish + optional recording ---
    stop_event = asyncio.Event()

    def _on_signal(sig, _frame):
        print("\nStopping...")
        stop_event.set()

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    record_task: Optional[asyncio.Task] = None
    data_task: Optional[asyncio.Task] = None

    try:
        if not stream_job.publish_url:
            raise LivepeerGatewayError("No publish_url returned - video ingress not enabled")

        publish_config = MediaPublishConfig(
            fps=fps,
            audio_sample_rate=media_info.audio_sample_rate if media_info.has_audio else None,
            audio_layout=media_info.audio_layout or "stereo",
        )
        media = stream_job.start_media_publish(publish_config)

        # Start recording task if requested
        if args.record and stream_job.subscribe_url:
            record_task = asyncio.create_task(
                _record_output(
                    stream_job.subscribe_url,
                    args.record,
                    fps,
                    width,
                    height,
                    stop_event,
                    audio_sample_rate=media_info.audio_sample_rate,
                    audio_layout=media_info.audio_layout,
                )
            )

        # Start data channel subscriber if data output is enabled
        if stream_job.data_url:
            print(f"Subscribing to data channel: {stream_job.data_url}")
            data_task = asyncio.create_task(
                _subscribe_data_channel(stream_job.data_url, stop_event)
            )

        # Read and publish frames from the media file (video + audio)
        time_base = Fraction(1, _TIME_BASE)
        pts = 0
        video_frame_count = 0
        audio_frame_count = 0
        last_wall = time.monotonic()

        audio_tag = "+audio" if media_info.has_audio else ""
        print(f"Publishing frames{audio_tag} {'(looping)' if args.loop else ''}...")
        for frame in _iter_media_frames(
            args.input_file,
            loop=args.loop,
            include_audio=media_info.has_audio,
        ):
            if stop_event.is_set():
                break

            if isinstance(frame, av.VideoFrame):
                now = time.monotonic()
                if video_frame_count > 0:
                    pts += int((now - last_wall) * _TIME_BASE)
                last_wall = now

                frame.pts = pts
                frame.time_base = time_base

                await media.write_frame(frame)
                video_frame_count += 1

                if video_frame_count % 100 == 0:
                    msg = f"  published {video_frame_count} video"
                    if audio_frame_count > 0:
                        msg += f" + {audio_frame_count} audio"
                    msg += " frames"
                    print(msg)

                # Pace to the target FPS
                await asyncio.sleep(frame_interval)

            elif isinstance(frame, av.AudioFrame):
                # Audio frames are published as decoded (no pacing needed —
                # they naturally interleave between paced video frames).
                await media.write_audio_frame(frame)
                audio_frame_count += 1

        msg = f"Published {video_frame_count} video"
        if audio_frame_count > 0:
            msg += f" + {audio_frame_count} audio"
        msg += " frames total"
        print(msg)

    finally:
        # Cancel recording task
        if record_task is not None:
            stop_event.set()
            record_task.cancel()
            with suppress(asyncio.CancelledError):
                await record_task

        # Cancel data channel task
        if data_task is not None:
            stop_event.set()
            data_task.cancel()
            with suppress(asyncio.CancelledError):
                await data_task

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
        await run(args)
    except LivepeerGatewayError as e:
        print(f"ERROR ({args.orchestrator}): {e}")


if __name__ == "__main__":
    asyncio.run(main())
