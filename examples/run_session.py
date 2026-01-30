import argparse
import asyncio
import io
import logging
import os
import time
from pathlib import Path
from typing import Optional

import av

from livepeer_gateway.live_payment import LivePaymentConfig
from livepeer_gateway.media_publish import MediaPublishConfig
from livepeer_gateway.orchestrator import LivepeerGatewayError, StartJobRequest
from livepeer_gateway.orchestrator_session import OrchestratorSession
from livepeer_gateway.trickle_subscriber import SegmentReader, TrickleSubscriber

DEFAULT_ORCH = "https://localhost:8935"
DEFAULT_MODEL_ID = "noop"

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch orchestrator info, fetch job token, start a job, and publish frames from a file."
    )
    p.add_argument(
        "orchestrator",
        nargs="?",
        default=DEFAULT_ORCH,
        help="Orchestrator gRPC target (host:port).",
    )
    p.add_argument(
        "--signer",
        required=True,
        help="Remote signer base URL (no path).",
    )
    p.add_argument(
        "--model-id",
        default=DEFAULT_MODEL_ID,
        help="Pipeline model_id to start via /live-video-to-video.",
    )
    p.add_argument(
        "--input",
        required=True,
        help="Path to input video file to publish.",
    )
    p.add_argument(
        "--fps",
        type=float,
        default=None,
        help="Optional FPS override for publishing (defaults to source FPS if available).",
    )
    p.add_argument(
        "--keyframe-interval",
        type=float,
        default=2.0,
        help="Keyframe interval seconds for publishing (default: 2.0).",
    )
    p.add_argument(
        "--max-age-s",
        type=float,
        default=None,
        help="Optional TTL (seconds) for cached OrchestratorInfo.",
    )
    p.add_argument(
        "--max-frames",
        type=int,
        default=None,
        help="Optional maximum number of frames to send from the input.",
    )
    p.add_argument(
        "--stream-id",
        default=None,
        help="Optional stream ID for logging.",
    )
    p.add_argument(
        "--request-id",
        default=None,
        help="Optional gateway request ID for logging.",
    )
    p.add_argument(
        "--record-output",
        default=None,
        help="Optional path to record downstream trickle MPEG-TS segments (e.g. recording.ts).",
    )
    p.add_argument(
        "--record-start-seq",
        type=int,
        default=-1,
        help="Starting sequence number for recording (default: -1, follow latest).",
    )
    p.add_argument(
        "--record-max-segments",
        type=int,
        default=None,
        help="Optional cap on recorded segments (useful for quick checks).",
    )
    p.add_argument(
        "--payment-interval",
        type=float,
        default=5.0,
        help="Interval in seconds between payment submissions (default: 5.0). Set to 0 to disable live payments.",
    )
    return p.parse_args()


def _decode_frame_count(segment_bytes: bytes) -> int:
    frames = 0
    container = av.open(io.BytesIO(segment_bytes), format="mpegts")
    try:
        for packet in container.demux(video=0):
            for frame in packet.decode():
                if isinstance(frame, av.VideoFrame):
                    frames += 1
    finally:
        container.close()
    return frames


async def _read_all(segment: SegmentReader, *, chunk_size: int = 256 * 1024) -> bytes:
    parts = []
    try:
        while True:
            chunk = await segment.read(chunk_size=chunk_size)
            if not chunk:
                break
            parts.append(chunk)
    finally:
        await segment.close()
    return b"".join(parts)


async def _publish_file(
    path: str,
    media,
    *,
    fps_override: Optional[float],
    max_frames: Optional[int],
) -> None:
    if not os.path.isfile(path):
        raise LivepeerGatewayError(f"Input file not found: {path}")

    container = av.open(path)
    stream = next((s for s in container.streams if s.type == "video"), None)
    if stream is None:
        raise LivepeerGatewayError("Input file has no video stream")

    start_wallclock = time.perf_counter()
    sent = 0

    async for frame in _frame_iter(container, stream):
        if max_frames is not None and sent >= max_frames:
            break

        frame_time = 0.0
        if frame.pts is not None and frame.time_base is not None:
            frame_time = float(frame.pts * frame.time_base)
        elif fps_override:
            frame_time = sent / fps_override

        elapsed = time.perf_counter() - start_wallclock
        delay = frame_time - elapsed
        if delay > 0:
            await asyncio.sleep(delay)

        await media.write_frame(frame)
        sent += 1


async def _frame_iter(container: av.container.InputContainer, stream: av.video.stream.VideoStream):
    for packet in container.demux(stream):
        for frame in packet.decode():
            if isinstance(frame, av.VideoFrame):
                yield frame


async def _record_trickle(
    publish_url: str,
    output: Path,
    *,
    start_seq: int,
    max_segments: Optional[int],
    stop_event: asyncio.Event,
) -> None:
    """
    Record trickle segments to disk while optionally decoding frames for progress.
    """
    segments = 0
    frames = 0

    async with TrickleSubscriber(publish_url, start_seq=start_seq) as subscriber:
        with output.open("ab") as outfile:
            while True:
                segment = await subscriber.next()
                if segment is None:
                    break
                seq = segment.seq()
                data = await _read_all(segment)
                await asyncio.to_thread(outfile.write, data)
                frames += _decode_frame_count(data)
                segments += 1
                print(f"[record] seq={seq} bytes={len(data)} frames_total={frames}")
                if max_segments is not None and segments >= max_segments:
                    break
                if stop_event.is_set():
                    break


async def main() -> None:
    args = _parse_args()

    # Configure logging so we can see payment activity
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    # Configure live payments if interval > 0
    live_payment_config = None
    if args.payment_interval > 0:
        live_payment_config = LivePaymentConfig(
            interval_s=args.payment_interval,
        )
        print(f"[payment] Live payments enabled, interval={args.payment_interval}s")

    session = OrchestratorSession(
        args.orchestrator,
        signer_url=args.signer,
        max_age_s=args.max_age_s,
        live_payment_config=live_payment_config,
    )

    job = None
    try:
        info = session.ensure_info()
        print("=== OrchestratorInfo ===")
        print("Orchestrator:", args.orchestrator)
        print("Transcoder URI:", info.transcoder)
        print("ETH Address:", info.address.hex())
        print()

        job = session.start_job(
            StartJobRequest(
                model_id=args.model_id,
                request_id=args.request_id,
                stream_id=args.stream_id,
            )
        )
        print("=== LiveVideoToVideo ===")
        print("publish_url:", job.publish_url)
        print()

        record_task = None
        if args.record_output:
            output_path = Path(args.record_output).expanduser()
            print(f"[record] writing trickle segments to {output_path}")
            record_stop = asyncio.Event()
            record_task = asyncio.create_task(
                _record_trickle(
                    job.publish_url,
                    output_path,
                    start_seq=args.record_start_seq,
                    max_segments=args.record_max_segments,
                    stop_event=record_stop,
                )
            )

        media = job.start_media(
            MediaPublishConfig(
                fps=args.fps,
                keyframe_interval_s=args.keyframe_interval,
            )
        )

        await _publish_file(
            args.input,
            media,
            fps_override=args.fps,
            max_frames=args.max_frames,
        )

        if record_task:
            record_stop.set()
            try:
                await asyncio.wait_for(record_task, timeout=5)
            except asyncio.TimeoutError:
                record_task.cancel()
                try:
                    await record_task
                except asyncio.CancelledError:
                    pass

    except LivepeerGatewayError as e:
        print(f"ERROR ({args.orchestrator}): {e}")
    finally:
        # Close session (stops payment senders and job resources)
        await session.aclose()


if __name__ == "__main__":
    asyncio.run(main())
