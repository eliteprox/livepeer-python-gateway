from __future__ import annotations

import asyncio
import logging
import math
import os
import queue
import threading
import time
from dataclasses import dataclass
from fractions import Fraction
from typing import Optional, Set

import av
from av.video.frame import PictureType

from .errors import LivepeerGatewayError
from .trickle_publisher import TricklePublisher

_OUT_TIME_BASE = Fraction(1, 90_000)
_READ_CHUNK = 64 * 1024
_STOP = object()


def _fraction_from_time_base(time_base: Fraction) -> Fraction:
    if isinstance(time_base, Fraction):
        return time_base
    numerator = getattr(time_base, "numerator", None)
    denominator = getattr(time_base, "denominator", None)
    if numerator is not None and denominator is not None:
        return Fraction(int(numerator), int(denominator))
    return Fraction(time_base)


def _rescale_pts(pts: int, src_tb: Fraction, dst_tb: Fraction) -> int:
    if src_tb == dst_tb:
        return int(pts)
    return int(round(float((Fraction(pts) * src_tb) / dst_tb)))


def _normalize_fps(fps: Optional[float], *, time_base: Optional[Fraction]) -> int:
    if fps is None and time_base is not None:
        try:
            tb = _fraction_from_time_base(time_base)
            if float(tb) > 0:
                fps = 1.0 / float(tb)
        except Exception:
            fps = None
    if fps is None or not math.isfinite(fps) or fps <= 0:
        fps = 30.0
    return max(1, int(round(fps)))


@dataclass(frozen=True)
class MediaPublishConfig:
    fps: Optional[float] = None
    mime_type: str = "video/mp2t"
    keyframe_interval_s: float = 2.0
    audio_sample_rate: Optional[int] = None
    audio_layout: str = "stereo"


class MediaPublish:
    def __init__(
        self,
        publish_url: str,
        *,
        mime_type: str = "video/mp2t",
        keyframe_interval_s: float = 2.0,
        fps: Optional[float] = None,
        audio_sample_rate: Optional[int] = None,
        audio_layout: str = "stereo",
    ) -> None:
        self.publish_url = publish_url
        self._publisher = TricklePublisher(publish_url, mime_type)
        self._keyframe_interval_s = float(keyframe_interval_s)
        self._fps_hint = fps

        self._queue: queue.Queue[object] = queue.Queue()
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._segment_tasks: Set[asyncio.Task[None]] = set()
        self._start_lock = threading.Lock()

        self._closed = False
        self._error: Optional[BaseException] = None

        # Video encoder state (owned by the encoder thread).
        self._container: Optional[av.container.OutputContainer] = None
        self._video_stream: Optional[av.video.stream.VideoStream] = None
        self._wallclock_start: Optional[float] = None
        self._last_keyframe_time: Optional[float] = None
        self._last_out_pts: Optional[int] = None

        # Audio encoder state (owned by the encoder thread).
        self._audio_sample_rate: Optional[int] = audio_sample_rate
        self._audio_layout: str = audio_layout
        self._audio_stream: Optional[av.audio.stream.AudioStream] = None
        self._audio_resampler: Optional[av.AudioResampler] = None
        self._audio_samples_encoded: int = 0
        self._audio_buffer: list[av.AudioFrame] = []

    async def write_frame(self, frame: av.VideoFrame) -> None:
        if self._closed:
            raise LivepeerGatewayError("MediaPublish is closed")
        if not isinstance(frame, av.VideoFrame):
            raise TypeError(f"write_frame expects av.VideoFrame, got {type(frame).__name__}")
        if self._error:
            raise LivepeerGatewayError(f"MediaPublish encoder failed: {self._error}") from self._error

        if self._loop is None:
            self._loop = asyncio.get_running_loop()

        self._ensure_thread()
        await asyncio.to_thread(self._queue.put, frame)

    async def write_audio_frame(self, frame: av.AudioFrame) -> None:
        if self._closed:
            raise LivepeerGatewayError("MediaPublish is closed")
        if not isinstance(frame, av.AudioFrame):
            raise TypeError(f"write_audio_frame expects av.AudioFrame, got {type(frame).__name__}")
        if self._audio_sample_rate is None:
            raise LivepeerGatewayError("Audio not configured (audio_sample_rate is None)")
        if self._error:
            raise LivepeerGatewayError(f"MediaPublish encoder failed: {self._error}") from self._error

        if self._loop is None:
            self._loop = asyncio.get_running_loop()

        self._ensure_thread()
        await asyncio.to_thread(self._queue.put, frame)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True

        if self._thread is not None:
            await asyncio.to_thread(self._queue.put, _STOP)
            await asyncio.to_thread(self._thread.join)

        if self._segment_tasks:
            await asyncio.gather(*list(self._segment_tasks), return_exceptions=True)

        await self._publisher.close()

        if self._error:
            raise LivepeerGatewayError("MediaPublish encoder failed") from self._error

    def _ensure_thread(self) -> None:
        with self._start_lock:
            if self._thread is not None:
                return
            self._thread = threading.Thread(
                target=self._run_encoder,
                name="MediaPublishEncoder",
                daemon=True,
            )
            self._thread.start()

    def _run_encoder(self) -> None:
        try:
            while True:
                item = self._queue.get()
                if item is _STOP:
                    break

                if isinstance(item, av.VideoFrame):
                    if self._container is None:
                        self._open_container(video_frame=item)
                    self._encode_video_frame(item)
                    # Flush any audio frames that arrived before the container was opened
                    if self._audio_buffer:
                        for buffered in self._audio_buffer:
                            self._encode_audio_frame(buffered)
                        self._audio_buffer.clear()

                elif isinstance(item, av.AudioFrame):
                    if self._container is None:
                        if self._fps_hint is None:
                            # No video expected -- open audio-only container
                            logging.info("MediaPublish: opening audio-only container (sample_rate=%s)", self._audio_sample_rate)
                            self._open_container(video_frame=None)
                            logging.info("MediaPublish: audio-only container opened, audio_stream=%s", self._audio_stream is not None)
                        else:
                            # Buffer audio until the first video frame opens the container
                            self._audio_buffer.append(item)
                            continue
                    self._encode_audio_frame(item)

            self._flush_encoder()
        except Exception as e:
            self._error = e
            logging.error("MediaPublish encoder error", exc_info=True)
        finally:
            if self._container is not None:
                try:
                    self._container.close()
                except Exception:
                    logging.exception("MediaPublish failed to close container")
            self._container = None
            self._video_stream = None
            self._audio_stream = None
            self._audio_resampler = None

    def _open_container(self, video_frame: Optional[av.VideoFrame] = None) -> None:
        if self._loop is None:
            raise RuntimeError("MediaPublish loop is not set")

        prev_write_file = [None]  # mutable ref for closure

        def custom_io_open(url: str, flags: int, options: dict) -> object:
            # Close the previous segment's write pipe so the reader gets EOF
            if prev_write_file[0] is not None:
                try:
                    prev_write_file[0].close()
                except Exception:
                    pass
            read_fd, write_fd = os.pipe()
            read_file = os.fdopen(read_fd, "rb", buffering=0)
            write_file = os.fdopen(write_fd, "wb", buffering=0)
            self._schedule_pipe_reader(read_file)
            prev_write_file[0] = write_file
            return write_file

        segment_options = {
            "segment_time": str(self._keyframe_interval_s),
            "segment_format": "mpegts",
        }

        self._container = av.open(
            "%d.ts",
            format="segment",
            mode="w",
            io_open=custom_io_open,
            options=segment_options,
        )

        # Add video stream if a video frame is provided
        if video_frame is not None:
            video_opts = {
                "bf": "0",
                "preset": "superfast",
                "tune": "zerolatency",
                "forced-idr": "1",
            }
            video_kwargs = {
                "time_base": _OUT_TIME_BASE,
                "width": video_frame.width,
                "height": video_frame.height,
                "pix_fmt": "yuv420p",
            }

            rounded_fps = _normalize_fps(self._fps_hint, time_base=video_frame.time_base)
            self._video_stream = self._container.add_stream("libx264", rate=rounded_fps, options=video_opts, **video_kwargs)

        # Add audio stream if configured
        if self._audio_sample_rate is not None:
            self._audio_stream = self._container.add_stream(
                "aac",
                rate=self._audio_sample_rate,
                layout=self._audio_layout,
            )

    def _encode_video_frame(self, frame: av.VideoFrame) -> None:
        if self._video_stream is None or self._container is None:
            raise RuntimeError("MediaPublish encoder is not initialized")

        source_pts = frame.pts
        source_tb = frame.time_base

        if frame.format.name != "yuv420p":
            frame = frame.reformat(format="yuv420p")

        current_time_s, out_pts = self._compute_pts(source_pts, source_tb)
        if self._last_out_pts is not None and out_pts <= self._last_out_pts:
            # timestamp would overlap with previous frame, so drop
            # happens if frames come in faster than the encode rate
            return
        self._last_out_pts = out_pts
        frame.pts = out_pts
        frame.time_base = _OUT_TIME_BASE

        if (
            self._last_keyframe_time is None
            or current_time_s - self._last_keyframe_time >= self._keyframe_interval_s
        ):
            frame.pict_type = PictureType.I
            self._last_keyframe_time = current_time_s
        else:
            frame.pict_type = PictureType.NONE

        packets = self._video_stream.encode(frame)
        for packet in packets:
            self._container.mux(packet)

    def _encode_audio_frame(self, frame: av.AudioFrame) -> None:
        if self._audio_stream is None or self._container is None:
            return

        # Lazy-init resampler for format/layout conversion to match AAC encoder
        if self._audio_resampler is None:
            codec_ctx = self._audio_stream.codec_context
            self._audio_resampler = av.AudioResampler(
                format=codec_ctx.format,
                layout=codec_ctx.layout,
                rate=codec_ctx.sample_rate,
            )

        resampled = self._audio_resampler.resample(frame)
        for rf in resampled:
            # Assign monotonic PTS based on total samples encoded so far.
            # This keeps audio PTS independent of source timestamps and
            # handles looping correctly (PTS never decreases).
            rf.pts = self._audio_samples_encoded
            self._audio_samples_encoded += rf.samples

            packets = self._audio_stream.encode(rf)
            for packet in packets:
                self._container.mux(packet)

    def _flush_encoder(self) -> None:
        if self._container is None:
            return

        # Flush video encoder
        if self._video_stream is not None:
            packets = self._video_stream.encode(None)
            for packet in packets:
                self._container.mux(packet)

        # Flush audio resampler then encoder
        if self._audio_stream is not None:
            if self._audio_resampler is not None:
                flushed = self._audio_resampler.resample(None)
                for rf in flushed:
                    rf.pts = self._audio_samples_encoded
                    self._audio_samples_encoded += rf.samples
                    packets = self._audio_stream.encode(rf)
                    for packet in packets:
                        self._container.mux(packet)

            packets = self._audio_stream.encode(None)
            for packet in packets:
                self._container.mux(packet)

    def _compute_pts(self, pts: Optional[int], time_base: Optional[Fraction]) -> tuple[float, int]:
        if pts is not None and time_base is not None:
            tb = _fraction_from_time_base(time_base)
            current_time_s = float(Fraction(pts) * tb)
            out_pts = _rescale_pts(pts, tb, _OUT_TIME_BASE)
            return current_time_s, out_pts

        now = time.time()
        if self._wallclock_start is None:
            self._wallclock_start = now
        current_time_s = now - self._wallclock_start
        return current_time_s, int(current_time_s * _OUT_TIME_BASE.denominator)

    def _schedule_pipe_reader(self, read_file: object) -> None:
        def _start() -> None:
            task = self._loop.create_task(self._stream_pipe_to_trickle(read_file))
            self._segment_tasks.add(task)
            task.add_done_callback(self._segment_tasks.discard)

        self._loop.call_soon_threadsafe(_start)

    async def _stream_pipe_to_trickle(self, read_file: object) -> None:
        total_bytes = 0
        try:
            async with await self._publisher.next() as segment:
                seq = segment.seq()
                logging.debug("MediaPublish segment %d: started", seq)
                while True:
                    chunk = await asyncio.to_thread(read_file.read, _READ_CHUNK)
                    if not chunk:
                        break
                    await segment.write(chunk)
                    total_bytes += len(chunk)
                logging.info("MediaPublish segment %d: published %d bytes", seq, total_bytes)
        except Exception:
            logging.error("MediaPublish pipe stream failed", exc_info=True)
        finally:
            try:
                read_file.close()
            except Exception:
                pass

