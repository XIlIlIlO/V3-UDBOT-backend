from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from typing import Deque, Dict, List, Literal, Optional, Tuple

from app.config import Settings
from app.models import Candle, Signal


class MarketState:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.lock = asyncio.Lock()

        self.symbols: List[str] = []

        # candles[symbol][timeframe] = [Candle, ...] (sorted by time)
        self.candles: Dict[str, Dict[str, List[Candle]]] = defaultdict(dict)

        # signals_by_symbol_tf[(symbol, timeframe)] = [Signal, ...]
        self.signals_by_symbol_tf: Dict[Tuple[str, str], List[Signal]] = defaultdict(list)

        # Recent signals: dict for O(1) upsert + ordered deque for newest-first traversal.
        # Order deque can contain stale IDs (lazy cleanup).
        self._recent_by_id: Dict[str, Signal] = {}
        self._recent_order: Deque[str] = deque()

        # Per-symbol last WS message arrival (for watchdog)
        self.last_ws_msg_at: Dict[str, float] = {}

        self.last_scan_started_at: Optional[int] = None
        self.last_scan_finished_at: Optional[int] = None
        self.scanner_running: bool = False
        self.current_phase: str = ""
        self.errors: Deque[str] = deque(maxlen=50)

    async def set_symbols(self, symbols: List[str]) -> None:
        async with self.lock:
            self.symbols = symbols

    async def set_candles(self, symbol: str, timeframe: str, candles: List[Candle]) -> None:
        """Set candles directly (already sorted)."""
        limit = self.settings.candle_limit_for(timeframe)
        async with self.lock:
            self.candles[symbol][timeframe] = candles[-limit:]

    async def merge_candles(self, symbol: str, timeframe: str, new_candles: List[Candle]) -> List[Candle]:
        """Merge new candles into existing (both assumed sorted by time)."""
        limit = self.settings.candle_limit_for(timeframe)

        async with self.lock:
            existing = self.candles.get(symbol, {}).get(timeframe, [])
            if not existing:
                merged = new_candles[-limit:]
                self.candles[symbol][timeframe] = merged
                return list(merged)

            by_time = {c.time: c for c in existing}
            for c in new_candles:
                by_time[c.time] = c

            merged = sorted(by_time.values(), key=lambda x: x.time)[-limit:]
            self.candles[symbol][timeframe] = merged
            return list(merged)

    async def get_candles_count(self, symbol: str, timeframe: str) -> int:
        async with self.lock:
            data = self.candles.get(symbol, {}).get(timeframe, [])
            return len(data)

    async def set_signals_for_symbol_tf(self, symbol: str, timeframe: str, signals: List[Signal]) -> None:
        async with self.lock:
            self.signals_by_symbol_tf[(symbol, timeframe)] = signals

    async def upsert_recent_signal(self, signal: Signal) -> Tuple[Literal["added", "updated", "unchanged"], Optional[Signal]]:
        """
        Insert or update a signal in recent_signals.
        Returns ('added', None) for new, ('updated', prev) if same ID existed with different content,
        ('unchanged', None) if same ID + same type/score (no-op).
        """
        async with self.lock:
            prev = self._recent_by_id.get(signal.id)
            if prev is not None:
                if prev.type == signal.type and prev.score == signal.score and prev.price == signal.price:
                    return ("unchanged", None)
                self._recent_by_id[signal.id] = signal
                return ("updated", prev)

            self._recent_by_id[signal.id] = signal
            self._recent_order.appendleft(signal.id)
            self._evict_oldest_locked()
            return ("added", None)

    async def upsert_recent_signals_batch(self, signals: List[Signal]) -> List[Tuple[str, Signal, Literal["added", "updated", "unchanged"], Optional[Signal]]]:
        """
        Batch upsert. Returns list of (id, new_signal, status, prev_signal_or_None).
        """
        results: List[Tuple[str, Signal, Literal["added", "updated", "unchanged"], Optional[Signal]]] = []
        async with self.lock:
            for s in signals:
                prev = self._recent_by_id.get(s.id)
                if prev is not None:
                    if prev.type == s.type and prev.score == s.score and prev.price == s.price:
                        results.append((s.id, s, "unchanged", None))
                        continue
                    self._recent_by_id[s.id] = s
                    results.append((s.id, s, "updated", prev))
                else:
                    self._recent_by_id[s.id] = s
                    self._recent_order.appendleft(s.id)
                    results.append((s.id, s, "added", None))
            self._evict_oldest_locked()
        return results

    async def remove_recent_signal(self, signal_id: str) -> Optional[Signal]:
        async with self.lock:
            prev = self._recent_by_id.pop(signal_id, None)
            return prev

    async def remove_recent_signals(self, ids_to_remove: set) -> List[Signal]:
        if not ids_to_remove:
            return []
        async with self.lock:
            removed: List[Signal] = []
            for sid in ids_to_remove:
                prev = self._recent_by_id.pop(sid, None)
                if prev is not None:
                    removed.append(prev)
            return removed

    def _evict_oldest_locked(self) -> None:
        """Evict oldest entries beyond max_recent_signals. Caller must hold lock."""
        max_keep = self.settings.max_recent_signals
        while len(self._recent_by_id) > max_keep and self._recent_order:
            old_id = self._recent_order.pop()
            self._recent_by_id.pop(old_id, None)

        # Also compact the order deque if it grew too large with stale entries
        if len(self._recent_order) > max_keep * 4:
            self._recent_order = deque(
                sid for sid in self._recent_order if sid in self._recent_by_id
            )

    async def batch_update_symbol(
        self,
        symbol: str,
        updates: List[Tuple[str, List[Candle], List[Signal]]],
    ) -> None:
        """
        Batch update multiple timeframes for one symbol in a single lock.
        updates: list of (timeframe, candles, signals)
        """
        async with self.lock:
            for timeframe, candles, signals in updates:
                limit = self.settings.candle_limit_for(timeframe)
                self.candles[symbol][timeframe] = candles[-limit:]
                self.signals_by_symbol_tf[(symbol, timeframe)] = signals

    async def get_candles(self, symbol: str, timeframe: str, limit: int) -> List[Candle]:
        async with self.lock:
            data = self.candles.get(symbol, {}).get(timeframe, [])
            return data[-limit:]

    async def get_signals(self, symbol: str, timeframe: str, limit: int = 200) -> List[Signal]:
        async with self.lock:
            data = self.signals_by_symbol_tf.get((symbol, timeframe), [])
            return data[-limit:]

    async def get_recent_signals(self, timeframe: str = "all", limit: int = 100) -> List[Signal]:
        async with self.lock:
            out: List[Signal] = []
            for sid in self._recent_order:
                s = self._recent_by_id.get(sid)
                if s is None:
                    continue
                if timeframe != "all" and s.timeframe != timeframe:
                    continue
                out.append(s)
                if len(out) >= limit:
                    break
            return out

    async def add_error(self, msg: str) -> None:
        async with self.lock:
            self.errors.appendleft(msg)

    async def set_phase(self, phase: str) -> None:
        async with self.lock:
            self.current_phase = phase

    def mark_ws_message(self, symbol: str) -> None:
        """Record arrival of a WS message for this symbol (no lock — single writer expected)."""
        self.last_ws_msg_at[symbol] = time.time()

    async def snapshot_status(self) -> dict:
        async with self.lock:
            n = len(self.symbols)
            scan_interval = self.settings.scan_interval_seconds
            estimated_rps = n / max(1, scan_interval)
            recent_count = len(self._recent_by_id)

            return {
                "ok": True,
                "market": "gate_usdt_perpetual_futures",
                "settle": self.settings.gate_settle,
                "symbols_loaded": n,
                "timeframes": self.settings.timeframe_list,
                "recent_signals": recent_count,
                "public_rps_limit": self.settings.public_rps_limit,
                "estimated_request_rate_per_sec": round(estimated_rps, 4),
                "estimated_request_rate_per_10s": round(estimated_rps * 10, 2),
                "last_scan_started_at": self.last_scan_started_at,
                "last_scan_finished_at": self.last_scan_finished_at,
                "scanner_running": self.scanner_running,
                "current_phase": self.current_phase,
                "errors": list(self.errors),
            }
