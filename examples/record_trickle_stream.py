from __future__ import annotations

import argparse
import asyncio
import io
import logging
from pathlib import Path
from typing import Optional

import av

from livepeer_gateway.trickle_subscriber import SegmentReader, TrickleSubscriber


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


def _decode_frame_count(segment_bytes: bytes) -> int:
    """
    Decode MPEG-TS bytes to count frames for logging progress.
    """
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


async def record_stream(
    publish_url: str,
    output: Path,
    *,
    start_seq: int = -1,
    max_segments: Optional[int] = None,
) -> None:
    """
    Subscribe to a trickle publish URL and write MPEG-TS segments contiguously to `output`.
    Frames are decoded for simple progress logging.
    """
    frames_total = 0
    segments_total = 0

    async with TrickleSubscriber(publish_url, start_seq=start_seq) as subscriber:
        with output.open("ab") as outfile:
            while True:
                segment = await subscriber.next()
                if segment is None:
                    break

                seq = segment.seq()
                data = await _read_all(segment)
                await asyncio.to_thread(outfile.write, data)

                frames = _decode_frame_count(data)
                frames_total += frames
                segments_total += 1

                logging.info(
                    "seq=%s wrote=%s bytes frames=%s total_frames=%s",
                    seq,
                    len(data),
                    frames,
                    frames_total,
                )

                if max_segments is not None and segments_total >= max_segments:
                    break

    logging.info("Done. Wrote %s segments and %s frames to %s", segments_total, frames_total, output)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Record trickle video segments contiguously to a file. "
            "Playback with: ffplay -autoexit -i <output>"
        )
    )
    parser.add_argument(
        "--publish-url",
        required=True,
        help="Base trickle publish URL (no sequence), e.g. http://localhost:2939/my-stream",
    )
    parser.add_argument(
        "--output",
        default="recording.ts",
        help="Output MPEG-TS path (default: recording.ts)",
    )
    parser.add_argument(
        "--start-seq",
        type=int,
        default=-1,
        help="Starting sequence number (-1 to follow the latest, default)",
    )
    parser.add_argument(
        "--max-segments",
        type=int,
        default=None,
        help="Stop after this many segments (optional, for quick tests)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    output_path = Path(args.output).expanduser()
    asyncio.run(
        record_stream(
            args.publish_url,
            output_path,
            start_seq=args.start_seq,
            max_segments=args.max_segments,
        )
    )


if __name__ == "__main__":
    main()
