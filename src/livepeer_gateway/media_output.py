from __future__ import annotations

"""
Helpers for consuming trickle media outputs as segments, bytes, or frames.
"""

import asyncio
import logging
from enum import Enum
from contextlib import suppress
from typing import AsyncIterator, Optional

from .errors import LivepeerGatewayError
from .media_decode import (
    AudioDecodedMediaFrame,
    DecodedMediaFrame,
    MpegTsDecoder,
    VideoDecodedMediaFrame,
    decoder_error,
    is_decoder_end,
)

from .segment_reader import SegmentReader
from .trickle_subscriber import TrickleSubscriber

_LOG = logging.getLogger(__name__)

class LagPolicy(Enum):
    """
    Policy for handling consumers that fall behind the segment window.
    """
    FAIL = "fail"
    LATEST = "latest"
    EARLIEST = "earliest"


class MediaOutput:
    """
    Access a trickle media output

    Exposes:
      - per-segment iteration (SegmentReader objects)
      - continuous byte stream (bytes chunks)
      - individual audio and video frames

    Segments are sourced from a single shared subscriber so that multiple
    iterators can consume the same output concurrently without duplicate
    network requests.

    Attributes:
        subscribe_url: Trickle subscribe URL for this output.
        start_seq: Initial server sequence when subscribing.
        max_retries: Max retries for segment fetches.
        max_segment_bytes: Safety bound for a single segment size.
        connection_close: Whether to close connections after each segment.
        chunk_size: Byte chunk size yielded by bytes()/frames().
        max_segments: Max number of segments retained in memory.
        on_lag: Behavior when a consumer falls behind the segment window.
            - LagPolicy.FAIL: raise LivepeerGatewayError.
            - LagPolicy.LATEST: skip to the newest available segment.
            - LagPolicy.EARLIEST: retry from the oldest available segment.
        _sub: Shared trickle subscriber.
        _segments: In-memory window of SegmentReader objects.
        _lock: Coroutine-level lock for segment fetching/eviction.
        _eos: End-of-stream indicator.
        _next_local_seq: Local sequence counter for fetched segments.
        _base_seq: Local sequence of _segments[0].
    """

    def __init__(
        self,
        subscribe_url: str,
        *,
        start_seq: int = -2,
        max_retries: int = 5,
        max_segment_bytes: Optional[int] = None,
        connection_close: bool = False,
        chunk_size: int = 64 * 1024,
        max_segments: int = 5,
        on_lag: LagPolicy = LagPolicy.LATEST,
    ) -> None:
        if max_segments < 1:
            raise ValueError("max_segments must be >= 1")
        self.subscribe_url = subscribe_url
        self.start_seq = start_seq
        self.max_retries = max_retries
        self.max_segment_bytes = max_segment_bytes
        self.connection_close = connection_close
        self.chunk_size = chunk_size
        self.max_segments = max_segments
        self.on_lag = on_lag

        self._sub: Optional[TrickleSubscriber] = None
        self._segments: list[SegmentReader] = []
        self._lock = asyncio.Lock()
        self._eos = False
        self._next_local_seq = 0
        self._base_seq = 0

    def segments(
        self,
    ) -> AsyncIterator[SegmentReader]:
        """
        Read the trickle media channel and yield SegmentReader objects.

        Segments are shared across iterators.
        """
        async def _iter() -> AsyncIterator[SegmentReader]:
            seq = 0
            segment = await self._next_segment(seq)
            while segment is not None:
                yield segment
                # Use the returned segment's local seq in case we skipped ahead.
                seq = segment._local_seq + 1
                segment = await self._next_segment(seq)

        return _iter()

    def bytes(
        self,
    ) -> AsyncIterator[bytes]:
        """
        Read the trickle media channel and yield a continuous byte stream.
        """

        async def _iter() -> AsyncIterator[bytes]:
            async for chunk in self._iter_bytes():
                yield chunk

        return _iter()

    def frames(
        self,
    ) -> AsyncIterator[AudioDecodedMediaFrame | VideoDecodedMediaFrame]:
        """
        Read the trickle media channel, decode MPEG-TS, and yield raw frames.
        """

        async def _iter() -> AsyncIterator[AudioDecodedMediaFrame | VideoDecodedMediaFrame]:
            decoder = MpegTsDecoder()
            output = decoder.output_queue()
            decoder.start()

            async def _feed() -> None:
                async for chunk in self._iter_bytes():
                    decoder.feed(chunk)
                decoder.close()

            producer_task = asyncio.create_task(_feed())
            try:
                while True:
                    item = await asyncio.to_thread(output.get)
                    err = decoder_error(item)
                    if err is not None:
                        raise LivepeerGatewayError(
                            f"Media decode error: {err.__class__.__name__}: {err}"
                        ) from err
                    if is_decoder_end(item):
                        if producer_task.done():
                            exc = producer_task.exception()
                            if exc:
                                raise exc
                        break
                    if isinstance(item, DecodedMediaFrame):
                        yield item
            finally:
                decoder.stop()
                if not producer_task.done():
                    producer_task.cancel()
                with suppress(asyncio.CancelledError):
                    await producer_task
                await asyncio.to_thread(decoder.join)

        return _iter()

    async def _iter_bytes(
        self,
    ) -> AsyncIterator[bytes]:
        checked_content_type = False
        seq = 0
        segment = await self._next_segment(seq)
        while segment is not None:
            if not checked_content_type:
                _require_mpegts_content_type(segment.headers().get("Content-Type"))
                checked_content_type = True
            reader = segment.make_reader()
            while True:
                chunk = await reader.read(chunk_size=self.chunk_size)
                if not chunk:
                    break
                yield chunk
            # Use the returned segment's local seq in case we skipped ahead.
            seq = segment._local_seq + 1
            segment = await self._next_segment(seq)

    async def _next_segment(
        self,
        seq: int,
    ) -> Optional[SegmentReader]:
        """
        Return the segment at seq, lazily advancing the subscriber if needed.
        """
        # Safe lock-free read: asyncio only context-switches on awaits, and this
        # block has no awaits. That means _segments/_base_seq cannot change
        # until we return or enter the locked slow path below.
        relative = seq - self._base_seq
        if 0 <= relative < len(self._segments):
            return self._segments[relative]

        async with self._lock:
            relative = seq - self._base_seq
            if relative < 0:
                if self.on_lag is LagPolicy.FAIL:
                    raise LivepeerGatewayError(
                        "consumer fell behind segment window"
                    )
                if self._segments:
                    if self.on_lag is LagPolicy.EARLIEST:
                        _LOG.warning(
                            "MediaOutput consumer fell behind segment window; "
                            "retrying from earliest"
                        )
                        return self._segments[0]
                    _LOG.warning(
                        "MediaOutput consumer fell behind segment window; "
                        "skipping to latest"
                    )
                    return self._segments[-1]
            elif relative < len(self._segments):
                return self._segments[relative]

            while (seq - self._base_seq) >= len(self._segments):
                if self._eos:
                    return None
                if self._sub is None:
                    self._sub = TrickleSubscriber(
                        self.subscribe_url,
                        start_seq=self.start_seq,
                        max_retries=self.max_retries,
                        max_bytes=self.max_segment_bytes,
                        connection_close=self.connection_close,
                    )
                segment = await self._sub.next()
                if segment is None:
                    self._eos = True
                    return None
                segment._local_seq = self._next_local_seq
                self._next_local_seq += 1
                self._segments.append(segment)

                prev = len(self._segments) - 2
                if prev >= 0:
                    await self._segments[prev].close()

                while len(self._segments) > self.max_segments:
                    self._segments.pop(0)
                    self._base_seq += 1

            relative = seq - self._base_seq
            if 0 <= relative < len(self._segments):
                return self._segments[relative]
            return None

    async def close(self) -> None:
        for segment in self._segments:
            await segment.close()
        if self._sub is not None:
            await self._sub.close()

    async def __aenter__(self) -> "MediaOutput":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()


def _normalize_content_type(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return value.split(";", 1)[0].strip().lower()


def _require_mpegts_content_type(value: Optional[str]) -> None:
    normalized = _normalize_content_type(value)
    if normalized != "video/mp2t":
        raise LivepeerGatewayError(
            f"Expected MPEG-TS Content-Type 'video/mp2t', got {value!r}"
        )

