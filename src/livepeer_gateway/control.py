from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

from .trickle_publisher import SegmentWriter, TricklePublisher


_LOG = logging.getLogger(__name__)


class ControlMode(str, Enum):
    MESSAGE = "message"
    TIME = "time"


@dataclass(frozen=True)
class ControlConfig:
    mode: ControlMode = ControlMode.MESSAGE
    segment_interval: float = 10.0


class Control:
    def __init__(self, control_url: str, mime_type: str = "application/json") -> None:
        self.control_url = control_url
        self._publisher = TricklePublisher(control_url, mime_type)

    async def write_control(self, msg: dict[str, Any]) -> None:
        """
        Publish an unstructured JSON message onto the trickle control channel.

        One `write_control()` call sends one message per trickle segment.
        """
        if not isinstance(msg, dict):
            raise TypeError(f"write_control expects dict, got {type(msg).__name__}")

        payload = json.dumps(msg).encode("utf-8")
        async with await self._publisher.next() as segment:
            await segment.write(payload)

    async def close_control(self) -> None:
        """
        Close the control-channel publisher (best-effort).
        """
        await self._publisher.close()


class TimeControl:
    def __init__(
        self,
        control_url: str,
        mime_type: str = "application/jsonl",
        *,
        segment_interval: float = 10.0,
    ) -> None:
        if segment_interval <= 0:
            raise ValueError("segment_interval must be > 0")

        self.control_url = control_url
        self._publisher = TricklePublisher(control_url, mime_type)
        self._segment_interval = segment_interval
        self._writer: Optional[SegmentWriter] = None
        self._lock = asyncio.Lock()
        self._rotation_task: Optional[asyncio.Task[None]] = None
        self.start_rotation()

    def start_rotation(self) -> Optional[asyncio.Task[None]]:
        """
        Start periodic segment rotation if running in an event loop.

        If no loop is running, log a warning and return None; callers can
        invoke this method later from async code.
        """
        if self._rotation_task is not None and not self._rotation_task.done():
            return self._rotation_task
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            _LOG.warning(
                "No running event loop; time-based control rotation not started. "
                "Call control.start_rotation() from async code to enable."
            )
            return None
        self._rotation_task = loop.create_task(self._rotation_loop())
        return self._rotation_task

    async def _rotation_loop(self) -> None:
        while True:
            await asyncio.sleep(self._segment_interval)
            async with self._lock:
                if self._writer is not None:
                    await self._writer.close()
                    self._writer = None

    async def write_control(self, msg: dict[str, Any]) -> None:
        """
        Publish an unstructured JSON message as JSONL in a time-windowed segment.
        """
        if not isinstance(msg, dict):
            raise TypeError(f"write_control expects dict, got {type(msg).__name__}")

        payload = json.dumps(msg).encode("utf-8") + b"\n"
        async with self._lock:
            if self._writer is None:
                self._writer = await self._publisher.next()
            await self._writer.write(payload)

    async def close_control(self) -> None:
        """
        Close the time-based control publisher (best-effort).
        """
        task = self._rotation_task
        self._rotation_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                _LOG.exception("TimeControl rotation task failed during shutdown")

        async with self._lock:
            if self._writer is not None:
                await self._writer.close()
                self._writer = None

        await self._publisher.close()

