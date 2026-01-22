from __future__ import annotations

import asyncio
import logging
from typing import Optional, AsyncIterator

import aiohttp


class TricklePublisher:
    """
    Trickle publisher that streams bytes to a sequence of HTTP POST endpoints:
      - Create stream: POST {base_url}
      - Write segment: POST {base_url}/{seq} (streaming body)
      - Close stream: DELETE {base_url}

    The API matches the usage pattern:
        async with TricklePublisher(url, "application/json") as pub:
            async with await pub.next() as seg:
                await seg.write(b"...")
    """

    def __init__(self, url: str, mime_type: str, *, connection_close: bool = False):
        self.url = url.rstrip("/")
        self.mime_type = mime_type
        self.connection_close = connection_close
        self.seq = 0
        self._error: Optional[BaseException] = None

        # Lazily initialized async runtime bits (safe to construct in sync code).
        self._lock: Optional[asyncio.Lock] = None
        self._session: Optional[aiohttp.ClientSession] = None

        # Preconnected writer queue for the next segment.
        self._next_writer: Optional[asyncio.Queue[Optional[bytes]]] = None
        self._next_seq: int = None

    async def __aenter__(self) -> "TricklePublisher":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()

    async def _ensure_runtime(self) -> None:
        if self._lock is None:
            self._lock = asyncio.Lock()
        if self._session is None:
            # Ignore TLS validation (matches the rest of this repo).
            connector = aiohttp.TCPConnector(ssl=False)
            self._session = aiohttp.ClientSession(connector=connector)

    def _stream_url(self, seq: int) -> str:
        return f"{self.url}/{seq}"

    async def preconnect(self, seq: int) -> asyncio.Queue[Optional[bytes]]:
        """
        Start the POST for `seq` in the background and return a queue that feeds the request body.
        """
        await self._ensure_runtime()
        assert self._session is not None

        url = self._stream_url(seq)
        logging.debug("Trickle preconnect: %s", url)

        queue: asyncio.Queue[Optional[bytes]] = asyncio.Queue(maxsize=1)
        asyncio.create_task(self._run_post(url, queue))
        return queue

    async def _run_post(self, url: str, queue: asyncio.Queue[Optional[bytes]]) -> None:
        await self._ensure_runtime()
        assert self._session is not None

        try:
            headers = {"Content-Type": self.mime_type}
            if self.connection_close:
                headers["Connection"] = "close"
            resp = await self._session.post(
                url,
                headers=headers,
                data=self._stream_data(queue),
            )
            if resp.status != 200:
                body = await resp.text()
                logging.error("Trickle POST failed url=%s status=%s body=%r", url, resp.status, body)
                self._error = RuntimeError(f"Trickle POST failed: status={resp.status} body={body!r}")
            await resp.release()
        except Exception:
            logging.error("Trickle POST exception url=%s", url, exc_info=True)
            self._error = RuntimeError("Trickle POST exception")

    async def _run_delete(self) -> None:
        await self._ensure_runtime()
        assert self._session is not None

        try:
            resp = await self._session.delete(self.url)
            await resp.release()
        except Exception:
            logging.error("Trickle DELETE exception url=%s", self.url, exc_info=True)

    async def _stream_data(self, queue: asyncio.Queue[Optional[bytes]]) -> AsyncIterator[bytes]:
        while True:
            chunk = await queue.get()
            if chunk is None:
                break
            yield chunk

    async def create(self) -> None:
        await self._ensure_runtime()
        assert self._session is not None

        resp = await self._session.post(
            self.url,
            headers={"Expect-Content": self.mime_type},
            data={},
        )
        if resp.status != 200:
            body = await resp.text()
            await resp.release()
            raise ValueError(f"Trickle create failed: status={resp.status} body={body!r}")
        await resp.release()

    async def next(self) -> "SegmentWriter":
        await self._ensure_runtime()
        assert self._lock is not None

        async with self._lock:
            if self._next_writer is None or self._next_seq != self.seq:
                # don't have queue, or a queue for the wrong seq
                self._next_writer = await self.preconnect(self.seq)
                self._next_seq = self.seq

            seq = self.seq
            queue = self._next_writer
            self._next_writer = None
            self._next_seq = None

            # Preconnect the next segment in the background.
            self.seq += 1
            asyncio.create_task(self._preconnect_task(self.seq))

        return SegmentWriter(queue, seq)

    async def _preconnect_task(self, seq: int) -> None:
        await self._ensure_runtime()
        assert self._lock is not None

        async with self._lock:
            if self._next_writer is not None:
                return
            if self.seq != seq:
                # seq is stale
                return
            self._next_writer = await self.preconnect(seq)
            self._next_seq = seq

    async def close(self) -> None:
        # If the publisher was never used, avoid creating a session just to close it.
        if self._session is None and self._lock is None and self._next_writer is None:
            return

        await self._ensure_runtime()
        assert self._lock is not None

        logging.info("Trickle close: %s", self.url)
        async with self._lock:
            if self._next_writer is not None:
                await SegmentWriter(self._next_writer).close()
                self._next_writer = None

            if self._session is not None:
                try:
                    await self._run_delete()
                finally:
                    await self._session.close()
                    self._session = None

    @property
    def error(self) -> Optional[BaseException]:
        return self._error


class SegmentWriter:
    def __init__(self, queue: asyncio.Queue[Optional[bytes]], seq: int = -99):
        self.queue = queue
        self._seq = seq

    async def write(self, data: bytes) -> None:
        await self.queue.put(data)

    async def close(self) -> None:
        await self.queue.put(None)

    async def __aenter__(self) -> "SegmentWriter":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()

    def seq(self) -> int:
        return self._seq


