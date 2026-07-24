from __future__ import annotations

import time
from collections import defaultdict
from threading import Lock


class RateLimiter:
    """Simple fixed-window rate limiter keyed by credential fingerprint."""

    def __init__(
        self,
        max_requests: int,
        window_seconds: int,
    ) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._hits: dict[str, list[float]] = defaultdict(list)
        self._lock = Lock()

    def check(self, key: str) -> None:
        now = time.time()
        window_start = now - self.window_seconds
        with self._lock:
            bucket = self._hits[key]
            self._hits[key] = [t for t in bucket if t >= window_start]
            if len(self._hits[key]) >= self.max_requests:
                raise RateLimitExceeded(
                    f"Rate limit exceeded ({self.max_requests}/{self.window_seconds}s)"
                )
            self._hits[key].append(now)


class RateLimitExceeded(Exception):
    pass
