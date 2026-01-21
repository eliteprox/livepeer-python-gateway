from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
from typing import Optional

from livepeer_gateway.trickle_subscriber import TrickleFrameSubscriber


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

    subscriber = TrickleFrameSubscriber(publish_url, start_seq=start_seq)
    async with subscriber:
        with output.open("ab") as outfile:
            async for seq, data in subscriber.iter_segments():
                await asyncio.to_thread(outfile.write, data)

                frames = subscriber.decode_frames(data)
                frames_total += len(frames)
                segments_total += 1

                logging.info(
                    "seq=%s wrote=%s bytes frames=%s total_frames=%s",
                    seq,
                    len(data),
                    len(frames),
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
