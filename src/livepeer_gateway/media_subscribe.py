from __future__ import annotations

from typing import AsyncIterator, Optional

from .trickle_subscriber import SegmentReader, TrickleSubscriber


class MediaSubscribe:
    """
    Helper to subscribe to trickle media output for a LiveVideoToVideo job.
    Provides segment-level iteration as well as a continuous byte stream.
    """

    def __init__(self, subscribe_url: str) -> None:
        self.subscribe_url = subscribe_url

    def segments(
        self,
        *,
        start_seq: int = -2,
        max_retries: int = 5,
        max_segment_bytes: Optional[int] = None,
        connection_close: bool = False,
    ) -> AsyncIterator[SegmentReader]:
        """
        Yield SegmentReader objects for each trickle segment.
        """

        async def _iter() -> AsyncIterator[SegmentReader]:
            async with TrickleSubscriber(
                self.subscribe_url,
                start_seq=start_seq,
                max_retries=max_retries,
                max_bytes=max_segment_bytes,
                connection_close=connection_close,
            ) as subscriber:
                while True:
                    segment = await subscriber.next()
                    if segment is None:
                        break
                    yield segment

        return _iter()

    def bytes(
        self,
        *,
        start_seq: int = -2,
        max_retries: int = 5,
        max_segment_bytes: Optional[int] = None,
        connection_close: bool = False,
        chunk_size: int = 64 * 1024,
    ) -> AsyncIterator[bytes]:
        """
        Yield raw bytes across all trickle segments as a continuous stream.
        """

        async def _iter() -> AsyncIterator[bytes]:
            async for segment in self.segments(
                start_seq=start_seq,
                max_retries=max_retries,
                max_segment_bytes=max_segment_bytes,
                connection_close=connection_close,
            ):
                while True:
                    chunk = await segment.read(chunk_size=chunk_size)
                    if not chunk:
                        break
                    yield chunk

        return _iter()
