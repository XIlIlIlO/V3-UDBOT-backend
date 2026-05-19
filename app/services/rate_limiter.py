from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Mapping, Optional


@dataclass
class RateLimitSnapshot:
    limit: Optional[int] = None
    remain: Optional[int] = None
    reset_timestamp: Optional[float] = None
    last_status_code: Optional[int] = None
    last_wait_seconds: float = 0.0


class AsyncPacedRateLimiter:
    def __init__(self, requests_per_second: float, burst: int = 1):
        self.requests_per_second = max(0.1, float(requests_per_second))
        self.interval = 1.0 / self.requests_per_second
        self.burst = max(1, int(burst))

        self._lock = asyncio.Lock()
        self._next_allowed_at = 0.0
        self.snapshot = RateLimitSnapshot()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._next_allowed_at - now)

            if wait > 0:
                await asyncio.sleep(wait)

            now = time.monotonic()
            self._next_allowed_at = max(now, self._next_allowed_at) + self.interval
            self.snapshot.last_wait_seconds = wait

    async def backoff(self, seconds: float) -> None:
        seconds = max(0.0, float(seconds))
        async with self._lock:
            self._next_allowed_at = max(self._next_allowed_at, time.monotonic() + seconds)
            self.snapshot.last_wait_seconds = seconds

    def observe_response_headers(self, headers: Mapping[str, str], status_code: int) -> None:
        self.snapshot.last_status_code = status_code

        limit = _parse_int(headers.get("X-Gate-RateLimit-Limit"))
        remain = _parse_int(headers.get("X-Gate-RateLimit-Requests-Remain"))
        reset_ts = _parse_float(headers.get("X-Gate-RateLimit-Reset-Timestamp"))

        self.snapshot.limit = limit
        self.snapshot.remain = remain
        self.snapshot.reset_timestamp = reset_ts

    def seconds_until_reset(self) -> float:
        ts = self.snapshot.reset_timestamp
        if ts is None:
            return 0.0

        # 일부 환경에서 ms timestamp가 올 가능성까지 방어
        if ts > 10_000_000_000:
            ts = ts / 1000.0

        return max(0.0, ts - time.time())


def _parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def _parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None
