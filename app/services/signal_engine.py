"""
UT Bot Alerts — Python port of the TradingView Pine Script.

Two computation paths:

1) `calculate_signals(...)` — full O(n) recompute over the whole candle list.
   Used by bootstrap and integrity sweep (re-seeds incremental state from scratch).

2) `incremental_update(ctx, ...)` — O(k) where k = new bars since last update
   (typically 1, sometimes a few). Used by the WS-driven hot path.

The incremental path maintains an `IncrementalContext` per (symbol, timeframe)
holding the UT Bot state at the last *finalized* bar plus tracking info for the
currently-live bar's signal (which can flip BUY/SELL within the bar — TV faithful).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from app.models import Candle, Signal
from app.services.indicators import atr


# ── Incremental state ─────────────────────────────────────────────


@dataclass
class UTBotState:
    """UT Bot trail-stop snapshot at a finalized bar."""
    bar_time: int
    close: float
    atr: float
    trail_stop: float
    pos: int  # -1 (short) or +1 (long)


@dataclass
class IncrementalContext:
    """Per (symbol, timeframe) state for incremental UT Bot updates."""
    prev: Optional[UTBotState] = None  # state at the last finalized bar
    live_bar_time: int = 0
    live_signal_type: str = ""  # '', 'BUY', 'SELL' — current signal on the live bar
    live_signal_id: str = ""
    live_score: int = 0
    live_price: float = 0.0


# ── Event tuples returned by incremental_update ───────────────────
# ("upsert", signal_id, Signal)  — new or in-place updated signal
# ("remove", signal_id, None)    — live-bar signal disappeared / un-flipped
EngineEvent = Tuple[str, str, Optional[Signal]]


class SignalEngine:
    def __init__(
        self,
        min_score: int = 70,
        key_value: float = 1.0,
        atr_period: int = 10,
        ema_period: int = 1,
    ):
        self.min_score = min_score
        self.key_value = key_value
        self.atr_period = atr_period
        self.ema_period = ema_period

    # ── Full compute (bootstrap / integrity) ──────────────────────

    def calculate_signals(
        self, symbol: str, timeframe: str, candles: List[Candle]
    ) -> List[Signal]:
        n = len(candles)
        if n < self.atr_period + 2:
            return []

        closes = [c.close for c in candles]
        highs = [c.high for c in candles]
        lows = [c.low for c in candles]

        xATR = atr(highs, lows, closes, self.atr_period)

        trail_stop: List[Optional[float]] = [None] * n
        pos: List[int] = [0] * n

        for i in range(1, n):
            if xATR[i] is None:
                continue

            nLoss = self.key_value * xATR[i]
            prev_stop = trail_stop[i - 1]

            if prev_stop is None:
                trail_stop[i] = closes[i] - nLoss
                pos[i] = 1 if closes[i] > trail_stop[i] else -1
                continue

            prev_close = closes[i - 1]

            if closes[i] > prev_stop and prev_close > prev_stop:
                trail_stop[i] = max(prev_stop, closes[i] - nLoss)
            elif closes[i] < prev_stop and prev_close < prev_stop:
                trail_stop[i] = min(prev_stop, closes[i] + nLoss)
            elif closes[i] > prev_stop:
                trail_stop[i] = closes[i] - nLoss
            else:
                trail_stop[i] = closes[i] + nLoss

            if prev_close < prev_stop and closes[i] > trail_stop[i]:
                pos[i] = 1
            elif prev_close > prev_stop and closes[i] < trail_stop[i]:
                pos[i] = -1
            else:
                pos[i] = pos[i - 1]

        signals: List[Signal] = []
        for i in range(1, n):
            if trail_stop[i] is None or xATR[i] is None:
                continue
            prev_pos = pos[i - 1]
            c = candles[i]

            if pos[i] == 1 and prev_pos != 1:
                score = self._score(xATR[i], closes[i])
                if score >= self.min_score:
                    signals.append(self._build_signal(
                        symbol, timeframe, c, xATR[i], trail_stop[i], "BUY", score
                    ))
            elif pos[i] == -1 and prev_pos != -1:
                score = self._score(xATR[i], closes[i])
                if score >= self.min_score:
                    signals.append(self._build_signal(
                        symbol, timeframe, c, xATR[i], trail_stop[i], "SELL", score
                    ))

        return signals

    # ── Incremental seeding ───────────────────────────────────────

    def seed_context(
        self, symbol: str, timeframe: str, candles: List[Candle], live_cutoff: int
    ) -> Optional[IncrementalContext]:
        """
        Build an IncrementalContext from a full candle history.

        live_cutoff: time-bucket start for the live bar of this TF. Any bar with
                     time >= live_cutoff is treated as the live bar.

        Returns None if there aren't enough bars to compute ATR.
        """
        if not candles or len(candles) < self.atr_period + 2:
            return None

        # Partition into finalized + optional live
        last = candles[-1]
        if last.time >= live_cutoff:
            finalized = candles[:-1]
            live_bar = last
        else:
            finalized = candles
            live_bar = None

        if len(finalized) < self.atr_period + 2:
            return None

        # Run full compute over finalized to extract state at the last finalized bar
        prev_state = self._state_at_last(finalized)
        if prev_state is None:
            return None

        ctx = IncrementalContext(prev=prev_state)

        if live_bar is not None:
            new_state = self._step_forward(prev_state, live_bar)
            ctx.live_bar_time = live_bar.time
            sig_type = self._transition_type(prev_state.pos, new_state.pos)
            if sig_type:
                score = self._score(new_state.atr, live_bar.close)
                if score >= self.min_score:
                    ctx.live_signal_type = sig_type
                    ctx.live_signal_id = f"{symbol}:{timeframe}:{live_bar.time}"
                    ctx.live_score = score
                    ctx.live_price = live_bar.close

        return ctx

    def _state_at_last(self, candles: List[Candle]) -> Optional[UTBotState]:
        """Compute UT Bot state through `candles`, return state at the final bar."""
        n = len(candles)
        if n < self.atr_period + 2:
            return None

        closes = [c.close for c in candles]
        highs = [c.high for c in candles]
        lows = [c.low for c in candles]
        xATR = atr(highs, lows, closes, self.atr_period)

        trail_stop: List[Optional[float]] = [None] * n
        pos: List[int] = [0] * n

        for i in range(1, n):
            if xATR[i] is None:
                continue
            nLoss = self.key_value * xATR[i]
            prev_stop = trail_stop[i - 1]
            if prev_stop is None:
                trail_stop[i] = closes[i] - nLoss
                pos[i] = 1 if closes[i] > trail_stop[i] else -1
                continue
            prev_close = closes[i - 1]
            if closes[i] > prev_stop and prev_close > prev_stop:
                trail_stop[i] = max(prev_stop, closes[i] - nLoss)
            elif closes[i] < prev_stop and prev_close < prev_stop:
                trail_stop[i] = min(prev_stop, closes[i] + nLoss)
            elif closes[i] > prev_stop:
                trail_stop[i] = closes[i] - nLoss
            else:
                trail_stop[i] = closes[i] + nLoss
            if prev_close < prev_stop and closes[i] > trail_stop[i]:
                pos[i] = 1
            elif prev_close > prev_stop and closes[i] < trail_stop[i]:
                pos[i] = -1
            else:
                pos[i] = pos[i - 1]

        i = n - 1
        if trail_stop[i] is None or xATR[i] is None or pos[i] == 0:
            return None

        return UTBotState(
            bar_time=candles[i].time,
            close=closes[i],
            atr=xATR[i],
            trail_stop=trail_stop[i],
            pos=pos[i],
        )

    # ── Incremental step (single bar) ─────────────────────────────

    def _step_forward(self, prev: UTBotState, bar: Candle) -> UTBotState:
        """Advance one bar from `prev` UT-Bot state. Returns new state at `bar`."""
        # ATR (Wilder RMA)
        prev_close = prev.close
        tr = max(
            bar.high - bar.low,
            abs(bar.high - prev_close),
            abs(bar.low - prev_close),
        )
        new_atr = (prev.atr * (self.atr_period - 1) + tr) / self.atr_period

        # Trail stop
        nLoss = self.key_value * new_atr
        prev_stop = prev.trail_stop
        if bar.close > prev_stop and prev_close > prev_stop:
            new_trail = max(prev_stop, bar.close - nLoss)
        elif bar.close < prev_stop and prev_close < prev_stop:
            new_trail = min(prev_stop, bar.close + nLoss)
        elif bar.close > prev_stop:
            new_trail = bar.close - nLoss
        else:
            new_trail = bar.close + nLoss

        # Position
        if prev_close < prev_stop and bar.close > new_trail:
            new_pos = 1
        elif prev_close > prev_stop and bar.close < new_trail:
            new_pos = -1
        else:
            new_pos = prev.pos

        return UTBotState(
            bar_time=bar.time,
            close=bar.close,
            atr=new_atr,
            trail_stop=new_trail,
            pos=new_pos,
        )

    # ── Incremental update (hot path) ─────────────────────────────

    def incremental_update(
        self,
        ctx: IncrementalContext,
        symbol: str,
        timeframe: str,
        candles: List[Candle],
        live_cutoff: int,
    ) -> List[EngineEvent]:
        """
        Process candles forward from ctx.prev. Returns events for upsert/remove.

        `candles` is the FULL candle list (sorted). We walk forward from
        ctx.prev.bar_time. All bars except the last (or all bars if last is
        finalized) are treated as finalized — their signal is emitted if a
        transition occurred. The LAST bar (if time >= live_cutoff) is treated
        as the live bar, whose signal can flip and emit upsert/remove.
        """
        if not candles or ctx.prev is None:
            return []

        events: List[EngineEvent] = []
        prev_state = ctx.prev

        # Determine if last bar is live
        last = candles[-1]
        last_is_live = last.time >= live_cutoff

        # Bars strictly after prev.bar_time
        new_bars = [c for c in candles if c.time > prev_state.bar_time]

        if not new_bars:
            # Possibly only the live bar changed in-place
            if last_is_live and ctx.live_bar_time == last.time:
                return self._reevaluate_live(ctx, symbol, timeframe, last)
            return []

        # Walk forward. Distinguish finalized vs live.
        finalized_bars: List[Candle]
        live_bar: Optional[Candle]
        if last_is_live and new_bars[-1] is last:
            finalized_bars = new_bars[:-1]
            live_bar = last
        else:
            finalized_bars = new_bars
            live_bar = None

        # ── Finalize each completed bar ──
        # If the bar we're finalizing equals ctx.live_bar_time, the previously-
        # emitted live signal needs to be confirmed or removed based on final state.
        for c in finalized_bars:
            new_state = self._step_forward(prev_state, c)
            sig_type = self._transition_type(prev_state.pos, new_state.pos)

            if c.time == ctx.live_bar_time:
                # This bar was the live bar; reconcile its emitted signal with final state
                if sig_type and ctx.live_signal_type == sig_type:
                    # Confirm — score may have shifted slightly, emit final upsert
                    score = self._score(new_state.atr, c.close)
                    if score >= self.min_score:
                        sig = self._build_signal(symbol, timeframe, c, new_state.atr, new_state.trail_stop, sig_type, score)
                        events.append(("upsert", sig.id, sig))
                elif sig_type and ctx.live_signal_type != sig_type:
                    # Different final type from what was live — remove old, emit new
                    if ctx.live_signal_id:
                        events.append(("remove", ctx.live_signal_id, None))
                    score = self._score(new_state.atr, c.close)
                    if score >= self.min_score:
                        sig = self._build_signal(symbol, timeframe, c, new_state.atr, new_state.trail_stop, sig_type, score)
                        events.append(("upsert", sig.id, sig))
                elif not sig_type and ctx.live_signal_type:
                    # Was a live signal, but on finalize no transition — remove
                    events.append(("remove", ctx.live_signal_id, None))
                # Reset live tracking
                ctx.live_bar_time = 0
                ctx.live_signal_type = ""
                ctx.live_signal_id = ""
                ctx.live_score = 0
                ctx.live_price = 0.0
            else:
                # Past bar we missed (gap recovery)
                if sig_type:
                    score = self._score(new_state.atr, c.close)
                    if score >= self.min_score:
                        sig = self._build_signal(symbol, timeframe, c, new_state.atr, new_state.trail_stop, sig_type, score)
                        events.append(("upsert", sig.id, sig))

            prev_state = new_state

        ctx.prev = prev_state

        # ── Process live bar (if present) ──
        if live_bar is not None:
            events.extend(self._process_live(ctx, symbol, timeframe, live_bar))

        return events

    def _reevaluate_live(
        self, ctx: IncrementalContext, symbol: str, timeframe: str, live_bar: Candle
    ) -> List[EngineEvent]:
        """Live bar updated in-place (same time, new OHLC). Recompute and diff."""
        if ctx.prev is None:
            return []
        return self._process_live(ctx, symbol, timeframe, live_bar)

    def _process_live(
        self, ctx: IncrementalContext, symbol: str, timeframe: str, live_bar: Candle
    ) -> List[EngineEvent]:
        prev_state = ctx.prev
        if prev_state is None:
            return []
        new_state = self._step_forward(prev_state, live_bar)
        sig_type = self._transition_type(prev_state.pos, new_state.pos)

        events: List[EngineEvent] = []
        new_sid = f"{symbol}:{timeframe}:{live_bar.time}"

        if sig_type:
            score = self._score(new_state.atr, live_bar.close)
            if score >= self.min_score:
                # Signal exists on this live bar
                if ctx.live_signal_type == sig_type and ctx.live_signal_id == new_sid:
                    # Same signal — emit upsert if score/price changed materially
                    if score != ctx.live_score or abs(live_bar.close - ctx.live_price) > 1e-12:
                        sig = self._build_signal(symbol, timeframe, live_bar, new_state.atr, new_state.trail_stop, sig_type, score)
                        events.append(("upsert", sig.id, sig))
                        ctx.live_score = score
                        ctx.live_price = live_bar.close
                else:
                    # Different signal (type changed or new bar) — remove old, emit new
                    if ctx.live_signal_id and ctx.live_signal_id != new_sid:
                        events.append(("remove", ctx.live_signal_id, None))
                    sig = self._build_signal(symbol, timeframe, live_bar, new_state.atr, new_state.trail_stop, sig_type, score)
                    events.append(("upsert", sig.id, sig))
                    ctx.live_bar_time = live_bar.time
                    ctx.live_signal_type = sig_type
                    ctx.live_signal_id = new_sid
                    ctx.live_score = score
                    ctx.live_price = live_bar.close
            else:
                # Score below threshold — treat as no signal
                if ctx.live_signal_id:
                    events.append(("remove", ctx.live_signal_id, None))
                    ctx.live_signal_type = ""
                    ctx.live_signal_id = ""
                    ctx.live_score = 0
                ctx.live_bar_time = live_bar.time
        else:
            # No transition on this live bar
            if ctx.live_signal_id and ctx.live_bar_time == live_bar.time:
                # Previously had a live signal, but it un-flipped — remove
                events.append(("remove", ctx.live_signal_id, None))
                ctx.live_signal_type = ""
                ctx.live_signal_id = ""
                ctx.live_score = 0
            ctx.live_bar_time = live_bar.time

        return events

    # ── Helpers ───────────────────────────────────────────────────

    @staticmethod
    def _transition_type(prev_pos: int, new_pos: int) -> str:
        if new_pos == 1 and prev_pos != 1:
            return "BUY"
        if new_pos == -1 and prev_pos != -1:
            return "SELL"
        return ""

    def _build_signal(
        self,
        symbol: str,
        timeframe: str,
        bar: Candle,
        atr_val: float,
        trail_stop: float,
        sig_type: str,
        score: int,
    ) -> Signal:
        return Signal(
            id=f"{symbol}:{timeframe}:{bar.time}",
            symbol=symbol,
            timeframe=timeframe,
            time=bar.time,
            price=bar.close,
            type=sig_type,  # type: ignore[arg-type]
            score=score,
            reason=f"{sig_type}: UT Bot · ATR {atr_val:.6g} · Stop {trail_stop:.6g}",
            marker_position="belowBar" if sig_type == "BUY" else "aboveBar",
            marker_shape="arrowUp" if sig_type == "BUY" else "arrowDown",
        )

    def _score(self, atr_val: float, close: float) -> int:
        if close == 0 or atr_val == 0:
            return 70
        atr_pct = (atr_val / close) * 100
        score = 70
        if atr_pct > 0.5: score += 5
        if atr_pct > 1.0: score += 5
        if atr_pct > 2.0: score += 5
        if atr_pct > 3.0: score += 5
        nLoss_pct = (self.key_value * atr_val / close) * 100
        if nLoss_pct > 1.0: score += 5
        if nLoss_pct > 2.0: score += 5
        return min(100, score)


# Avoid unused-import warning for `field` (kept for future dataclass expansion)
_ = field
