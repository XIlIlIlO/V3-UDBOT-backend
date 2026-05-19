"""Aggregate 1-minute candles into larger timeframes — with incremental cache."""
from __future__ import annotations

from typing import Dict, List, Optional

from app.models import Candle

TIMEFRAME_SECONDS: Dict[str, int] = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600,
}

DERIVED_TIMEFRAMES = ["3m", "5m", "15m", "30m", "1h"]


def aggregate_candles(candles_1m: List[Candle], period_seconds: int) -> List[Candle]:
    """Full aggregation (used on first call or when cache is invalid)."""
    if not candles_1m:
        return []

    buckets: Dict[int, List[Candle]] = {}
    for c in candles_1m:
        bucket_start = (c.time // period_seconds) * period_seconds
        buckets.setdefault(bucket_start, []).append(c)

    result: List[Candle] = []
    for bucket_start in sorted(buckets):
        group = buckets[bucket_start]
        result.append(_build_candle(bucket_start, group))

    return result


def aggregate_candles_incremental(
    prev_result: List[Candle],
    candles_1m: List[Candle],
    period_seconds: int,
    prev_1m_count: int,
) -> List[Candle]:
    """
    Incremental aggregation: only rebuild buckets that have new 1m candles.
    If candles_1m grew, only the tail changed — re-aggregate affected buckets.
    """
    if not candles_1m:
        return []

    if not prev_result or prev_1m_count == 0:
        return aggregate_candles(candles_1m, period_seconds)

    n = len(candles_1m)

    # Find which 1m candles are new/changed (from prev_1m_count - 1 onward to catch updates)
    start_idx = max(0, prev_1m_count - 1)
    affected_buckets: set = set()
    for i in range(start_idx, n):
        affected_buckets.add((candles_1m[i].time // period_seconds) * period_seconds)

    if not affected_buckets:
        return prev_result

    # Rebuild only affected buckets from full 1m data
    new_bucket_data: Dict[int, List[Candle]] = {}
    for c in candles_1m:
        bs = (c.time // period_seconds) * period_seconds
        if bs in affected_buckets:
            new_bucket_data.setdefault(bs, []).append(c)

    # Merge: keep old buckets, replace affected ones
    result_map: Dict[int, Candle] = {c.time: c for c in prev_result}
    for bs, group in new_bucket_data.items():
        result_map[bs] = _build_candle(bs, group)

    return [result_map[t] for t in sorted(result_map)]


def _build_candle(bucket_start: int, group: List[Candle]) -> Candle:
    """Build a single aggregated candle from a group of 1m candles."""
    # Find first candle with actual trades for open price
    first_with_volume: Optional[Candle] = None
    for c in group:
        if c.volume > 0 or c.contract_volume > 0:
            first_with_volume = c
            break

    open_price = first_with_volume.open if first_with_volume else group[0].open

    traded = [c for c in group if c.volume > 0 or c.contract_volume > 0]
    if traded:
        high = max(c.high for c in traded)
        low = min(c.low for c in traded)
    else:
        high = open_price
        low = open_price

    # Use last candle (by time) for close — group is unsorted, find max time
    last = max(group, key=lambda c: c.time)
    close = last.close

    return Candle(
        time=bucket_start,
        open=open_price,
        high=high,
        low=low,
        close=close,
        volume=sum(c.volume for c in group),
        contract_volume=sum(c.contract_volume for c in group),
    )
