from __future__ import annotations

from typing import List, Optional


def ema(values: List[float], period: int) -> List[Optional[float]]:
    if not values:
        return []

    result: List[Optional[float]] = [None] * len(values)
    if len(values) < period:
        return result

    seed = sum(values[:period]) / period
    result[period - 1] = seed

    k = 2 / (period + 1)
    prev = seed
    for i in range(period, len(values)):
        current = values[i] * k + prev * (1 - k)
        result[i] = current
        prev = current

    return result


def sma(values: List[float], period: int) -> List[Optional[float]]:
    result: List[Optional[float]] = [None] * len(values)
    if len(values) < period:
        return result

    window_sum = sum(values[:period])
    result[period - 1] = window_sum / period

    for i in range(period, len(values)):
        window_sum += values[i]
        window_sum -= values[i - period]
        result[i] = window_sum / period

    return result


def rsi(values: List[float], period: int = 14) -> List[Optional[float]]:
    result: List[Optional[float]] = [None] * len(values)
    if len(values) <= period:
        return result

    gains = []
    losses = []

    for i in range(1, period + 1):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0))
        losses.append(abs(min(diff, 0)))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    result[period] = 100.0 if avg_loss == 0 else 100 - (100 / (1 + avg_gain / avg_loss))

    for i in range(period + 1, len(values)):
        diff = values[i] - values[i - 1]
        gain = max(diff, 0)
        loss = abs(min(diff, 0))

        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period

        result[i] = 100.0 if avg_loss == 0 else 100 - (100 / (1 + avg_gain / avg_loss))

    return result


def true_range(highs: List[float], lows: List[float], closes: List[float]) -> List[float]:
    """Calculate True Range for each bar (index 0 has no previous close, uses high-low)."""
    result: List[float] = []
    for i in range(len(highs)):
        if i == 0:
            result.append(highs[i] - lows[i])
        else:
            prev_close = closes[i - 1]
            result.append(max(
                highs[i] - lows[i],
                abs(highs[i] - prev_close),
                abs(lows[i] - prev_close),
            ))
    return result


def atr(highs: List[float], lows: List[float], closes: List[float], period: int = 10) -> List[Optional[float]]:
    """
    Average True Range using RMA (Wilder's smoothing), matching TradingView's ta.atr().
    RMA = (prev_rma * (period - 1) + current_value) / period
    """
    tr = true_range(highs, lows, closes)

    result: List[Optional[float]] = [None] * len(tr)
    if len(tr) < period:
        return result

    # Seed with SMA of first `period` TR values
    seed = sum(tr[:period]) / period
    result[period - 1] = seed

    prev = seed
    for i in range(period, len(tr)):
        current = (prev * (period - 1) + tr[i]) / period
        result[i] = current
        prev = current

    return result
