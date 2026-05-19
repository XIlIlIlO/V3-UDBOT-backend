"""
Market scanner — WS-driven real-time signal pipeline.

Architecture:
- Bootstrap runs symbol-by-symbol (REST 5rps). As each symbol completes its
  initial 2000-candle fetch, it's marked READY and starts streaming WS ticks
  through the fast incremental path.
- Processor / watchdog / integrity tasks start immediately (before bootstrap
  finishes), but only act on symbols in `_ready_symbols`.
- Steady state: WS subscriber pushes 1m candle ticks. Each tick →
  `_incremental_tick`:
    1) merge 1m candle into state
    2) broadcast 1m candle immediately (decoupled from signal compute)
    3) aggregate derived TFs (incremental)
    4) broadcast aggregated last-bar candle for each TF
    5) run incremental UT Bot updates (O(1) per TF)
    6) emit upsert/remove signal events
- Watchdog & integrity sweep fall back to FULL recompute path
  (`_recompute_all_tfs_locked`), which re-seeds the IncrementalContext.

REST budget (5 rps cap, ~50/10s) is shared across bootstrap, gap-fill,
integrity sweep, and on-demand /api/candles.
"""
from __future__ import annotations

import asyncio
import time
from typing import Dict, List, Optional, Set, Tuple

from app.config import Settings
from app.models import Candle, Signal
from app.services.aggregator import (
    DERIVED_TIMEFRAMES, TIMEFRAME_SECONDS,
    aggregate_candles_incremental,
)
from app.services.futures_client import GateFuturesClient
from app.services.signal_engine import IncrementalContext, SignalEngine
from app.services.webhook import WebhookSender
from app.state import MarketState
from app.ws_manager import WebSocketManager


def _current_period_start(timeframe: str) -> int:
    now = int(time.time())
    period = TIMEFRAME_SECONDS.get(timeframe, 60)
    return (now // period) * period


def _parse_ws_candle(item: dict, multiplier: float) -> Optional[Candle]:
    try:
        t = int(item["t"])
        o = float(item["o"])
        h = float(item["h"])
        l = float(item["l"])
        c = float(item["c"])
        v_contracts = float(item.get("v", 0) or 0)
        coin_volume = v_contracts * multiplier
        quote_volume = c * coin_volume
        return Candle(
            time=t, open=o, high=h, low=l, close=c,
            volume=quote_volume, contract_volume=coin_volume,
        )
    except Exception:
        return None


class MarketScanner:
    def __init__(
        self,
        settings: Settings,
        futures_client: GateFuturesClient,
        state: MarketState,
        ws_manager: WebSocketManager,
        webhook_sender: WebhookSender,
    ):
        self.settings = settings
        self.futures_client = futures_client
        self.state = state
        self.ws_manager = ws_manager
        self.webhook_sender = webhook_sender
        self.engine = SignalEngine(min_score=settings.signal_min_score)

        self._stop_event = asyncio.Event()
        self._main_task: Optional[asyncio.Task] = None
        self._processor_task: Optional[asyncio.Task] = None
        self._watchdog_task: Optional[asyncio.Task] = None
        self._integrity_task: Optional[asyncio.Task] = None

        # Incremental aggregation cache: (symbol, tf) -> (prev_result, prev_1m_count)
        self._agg_cache: Dict[Tuple[str, str], Tuple[List[Candle], int]] = {}

        # Per (symbol, tf) UT Bot incremental context
        self._engine_ctx: Dict[Tuple[str, str], IncrementalContext] = {}

        # Per-symbol async lock (prevents concurrent recompute for same symbol)
        self._sym_locks: Dict[str, asyncio.Lock] = {}

        # WS-driven recompute throttle
        self._dirty: Set[str] = set()
        self._latest_ws_candle: Dict[str, Candle] = {}
        self._last_recompute_at: Dict[str, float] = {}
        self._symbol_set: Set[str] = set()

        # Symbols that have completed bootstrap and are eligible for WS hot path
        self._ready_symbols: Set[str] = set()

        # Integrity sweep cursor
        self._sweep_idx: int = 0

    # ── Lifecycle ──────────────────────────────────────────────────

    def start(self) -> None:
        if self._main_task is None or self._main_task.done():
            self._main_task = asyncio.create_task(self._main())

    async def stop(self) -> None:
        self._stop_event.set()
        for t in (self._main_task, self._processor_task, self._watchdog_task, self._integrity_task):
            if t:
                t.cancel()
        for t in (self._main_task, self._processor_task, self._watchdog_task, self._integrity_task):
            if t:
                try:
                    await t
                except BaseException:
                    pass

    async def _main(self) -> None:
        try:
            await self.state.set_phase("loading_symbols")
            symbols = await self.futures_client.list_symbols()
            await self.state.set_symbols(symbols)
            self._symbol_set = set(symbols)

            # Start background tasks IMMEDIATELY — they gate on _ready_symbols
            self._processor_task = asyncio.create_task(self._processor_loop())
            self._watchdog_task = asyncio.create_task(self._watchdog_loop())
            self._integrity_task = asyncio.create_task(self._integrity_sweep_loop())

            if self.settings.bootstrap_on_start:
                await self._bootstrap()

            await self.state.set_phase("ws_streaming")
            await self._stop_event.wait()
        except asyncio.CancelledError:
            return
        except Exception as e:
            await self.state.add_error(f"scanner fatal: {type(e).__name__}: {e}")

    # ── Bootstrap ──────────────────────────────────────────────────

    async def _bootstrap(self) -> None:
        await self.state.set_phase("bootstrap")
        symbols = list(self.state.symbols)
        bootstrap_limit = self.settings.candle_limit_for("1m")

        for idx, symbol in enumerate(symbols, start=1):
            if self._stop_event.is_set():
                break
            try:
                candles = await self.futures_client.fetch_candles(symbol, "1m", bootstrap_limit)
                if candles:
                    await self._apply_full_1m(symbol, candles)
                    # Mark ready — WS tick processor will now pick this symbol up.
                    self._ready_symbols.add(symbol)
                    # If a WS tick already arrived during bootstrap, queue it.
                    if symbol in self._latest_ws_candle:
                        self._dirty.add(symbol)
            except Exception as e:
                await self.state.add_error(f"bootstrap {symbol}: {type(e).__name__}: {e}")
            if idx % 25 == 0:
                await self.state.set_phase(f"bootstrap_{idx}/{len(symbols)}")

    # ── WS event handlers (called from ws_subscriber) ──────────────

    async def on_ws_candle(self, symbol: str, raw_item: dict) -> None:
        if symbol not in self._symbol_set:
            return
        multiplier = self.futures_client.multipliers.get(symbol, 1.0)
        candle = _parse_ws_candle(raw_item, multiplier)
        if candle is None:
            return

        self.state.mark_ws_message(symbol)
        self._latest_ws_candle[symbol] = candle
        if symbol in self._ready_symbols:
            self._dirty.add(symbol)

    async def on_ws_reconnect(self, symbols: List[str]) -> None:
        """On shard reconnect — gap-fill via REST for ready symbols only."""
        for sym in symbols:
            if self._stop_event.is_set():
                break
            if sym not in self._ready_symbols:
                continue
            try:
                fill = await self.futures_client.fetch_candles(sym, "1m", 30)
                if fill:
                    await self._apply_rest_completed(sym, fill)
            except Exception as e:
                await self.state.add_error(f"reconnect-fill {sym}: {type(e).__name__}: {e}")

    # ── Background loops ───────────────────────────────────────────

    async def _processor_loop(self) -> None:
        tick = self.settings.processor_tick_ms / 1000.0
        min_interval = self.settings.recompute_min_interval_ms / 1000.0
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(tick)
                if not self._dirty:
                    continue
                now = time.monotonic()
                ready: List[str] = []
                for sym in list(self._dirty):
                    if sym not in self._ready_symbols:
                        # Not bootstrapped yet — skip (keep in dirty so we retry)
                        continue
                    last = self._last_recompute_at.get(sym, 0.0)
                    if now - last >= min_interval:
                        ready.append(sym)
                for sym in ready:
                    self._dirty.discard(sym)
                    self._last_recompute_at[sym] = time.monotonic()
                    candle = self._latest_ws_candle.pop(sym, None)
                    if candle is None:
                        continue
                    try:
                        await self._incremental_tick(sym, candle)
                    except Exception as e:
                        await self.state.add_error(f"incremental {sym}: {type(e).__name__}: {e}")
            except asyncio.CancelledError:
                return
            except Exception as e:
                await self.state.add_error(f"processor loop: {type(e).__name__}: {e}")

    async def _watchdog_loop(self) -> None:
        interval = self.settings.watchdog_interval_sec
        stale_threshold = self.settings.watchdog_stale_sec
        max_per_round = self.settings.watchdog_max_per_round
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(interval)
                now = time.time()
                stale: List[Tuple[str, float]] = []
                for sym in self._ready_symbols:
                    last = self.state.last_ws_msg_at.get(sym, 0.0)
                    age = now - last if last > 0 else 1e9
                    if age > stale_threshold:
                        stale.append((sym, age))
                if not stale:
                    continue
                stale.sort(key=lambda x: x[1], reverse=True)
                for sym, _age in stale[:max_per_round]:
                    if self._stop_event.is_set():
                        break
                    try:
                        fill = await self.futures_client.fetch_candles(sym, "1m", 30)
                        if fill:
                            await self._apply_rest_completed(sym, fill)
                    except Exception as e:
                        await self.state.add_error(f"watchdog {sym}: {type(e).__name__}: {e}")
            except asyncio.CancelledError:
                return
            except Exception as e:
                await self.state.add_error(f"watchdog loop: {type(e).__name__}: {e}")

    async def _integrity_sweep_loop(self) -> None:
        interval = self.settings.integrity_sweep_interval_sec
        batch = self.settings.integrity_sweep_batch_size
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(interval)
                # Only iterate ready symbols (bootstrap might still be filling others)
                ready_list = sorted(self._ready_symbols)
                if not ready_list:
                    continue
                pulled: List[str] = []
                for _ in range(batch):
                    if self._sweep_idx >= len(ready_list):
                        self._sweep_idx = 0
                    pulled.append(ready_list[self._sweep_idx])
                    self._sweep_idx += 1
                for sym in pulled:
                    if self._stop_event.is_set():
                        break
                    try:
                        full = await self.futures_client.fetch_candles(
                            sym, "1m", self.settings.candle_limit_for("1m")
                        )
                        if full:
                            await self._integrity_check(sym, full)
                    except Exception as e:
                        await self.state.add_error(f"integrity {sym}: {type(e).__name__}: {e}")
            except asyncio.CancelledError:
                return
            except Exception as e:
                await self.state.add_error(f"integrity loop: {type(e).__name__}: {e}")

    # ── Locks ──────────────────────────────────────────────────────

    def _get_sym_lock(self, symbol: str) -> asyncio.Lock:
        lock = self._sym_locks.get(symbol)
        if lock is None:
            lock = asyncio.Lock()
            self._sym_locks[symbol] = lock
        return lock

    # ── Fast path (WS tick) ────────────────────────────────────────

    async def _incremental_tick(self, symbol: str, ws_candle: Candle) -> None:
        """
        Hot path for a single WS candle tick.

        Strategy:
          - All state/ctx mutation happens under the per-symbol lock.
          - WS broadcasts happen AFTER lock release to avoid blocking other
            ticks behind broadcast I/O.
          - Order of broadcasts is 1m candle → derived candles → signal events,
            so the chart updates with minimal latency.
        """
        candle_broadcasts: List[Tuple[str, Candle]] = []  # (tf, last_bar)
        emit_events: List[Tuple[str, str, str, Optional[Signal]]] = []  # (tf, kind, id, sig)

        async with self._get_sym_lock(symbol):
            merged_1m = await self.state.merge_candles(symbol, "1m", [ws_candle])
            if not merged_1m:
                return
            candle_broadcasts.append(("1m", merged_1m[-1]))

            # Aggregate derived TFs (incremental); persist to state
            derived_aggs: Dict[str, List[Candle]] = {}
            for tf in DERIVED_TIMEFRAMES:
                period = TIMEFRAME_SECONDS[tf]
                cache_key = (symbol, tf)
                prev_agg, prev_count = self._agg_cache.get(cache_key, ([], 0))
                aggregated = aggregate_candles_incremental(
                    prev_agg, merged_1m, period, prev_count,
                )
                self._agg_cache[cache_key] = (aggregated, len(merged_1m))
                derived_aggs[tf] = aggregated
                if aggregated:
                    await self.state.set_candles(symbol, tf, aggregated)
                    candle_broadcasts.append((tf, aggregated[-1]))

            # Run incremental signal engine for all TFs (mutates ctx under lock)
            tf_candle_pairs: List[Tuple[str, List[Candle]]] = [("1m", merged_1m)]
            for tf in DERIVED_TIMEFRAMES:
                tf_candle_pairs.append((tf, derived_aggs[tf]))

            for tf, candles in tf_candle_pairs:
                ctx = self._engine_ctx.get((symbol, tf))
                if ctx is None or not candles:
                    continue
                live_cutoff = _current_period_start(tf)
                events = self.engine.incremental_update(ctx, symbol, tf, candles, live_cutoff)
                if not events:
                    continue
                # Apply to state.signals_by_symbol_tf for REST visibility
                await self._apply_signal_events_to_state(symbol, tf, events)
                for kind, sid, sig in events:
                    emit_events.append((tf, kind, sid, sig))

        # ── Outside the lock: broadcast candles first, then signal events ──
        for tf, last_bar in candle_broadcasts:
            await self.ws_manager.broadcast_candle(symbol, tf, last_bar)

        for tf, kind, sid, sig in emit_events:
            if kind == "upsert" and sig is not None:
                status, _ = await self.state.upsert_recent_signal(sig)
                if status in ("added", "updated"):
                    await self.ws_manager.broadcast_signal_upsert(sig, status=status)
                    if status == "added":
                        await self.webhook_sender.send_signal(sig)
            elif kind == "remove":
                await self.state.remove_recent_signal(sid)
                await self.ws_manager.broadcast_signal_remove(sid, symbol, tf)

    async def _apply_signal_events_to_state(
        self,
        symbol: str,
        timeframe: str,
        events: List[Tuple[str, str, Optional[Signal]]],
    ) -> None:
        """Update signals_by_symbol_tf based on incremental events."""
        if not events:
            return
        async with self.state.lock:
            current = list(self.state.signals_by_symbol_tf.get((symbol, timeframe), []))
            by_id = {s.id: s for s in current}
            for kind, sid, sig in events:
                if kind == "upsert" and sig is not None:
                    by_id[sid] = sig
                elif kind == "remove":
                    by_id.pop(sid, None)
            new_list = sorted(by_id.values(), key=lambda s: s.time)
            # Cap at a reasonable length to prevent unbounded growth
            self.state.signals_by_symbol_tf[(symbol, timeframe)] = new_list[-2000:]

    # ── Full path (bootstrap / watchdog / integrity / on-demand) ──

    async def _apply_full_1m(self, symbol: str, candles: List[Candle]) -> None:
        """Bootstrap and on-demand fetch path. Does NOT broadcast."""
        async with self._get_sym_lock(symbol):
            await self.state.set_candles(symbol, "1m", candles)
            await self._full_recompute_locked(
                symbol, candles, broadcast_candles=False, broadcast_signals=False,
            )

    async def _apply_rest_completed(self, symbol: str, candles: List[Candle]) -> None:
        """REST gap-fill (watchdog / reconnect). Skips the live bar."""
        cutoff = _current_period_start("1m")
        completed = [c for c in candles if c.time < cutoff]
        if not completed:
            return
        async with self._get_sym_lock(symbol):
            merged = await self.state.merge_candles(symbol, "1m", completed)
            await self._full_recompute_locked(symbol, merged, broadcast_candles=True)

    async def _integrity_check(self, symbol: str, fresh_full: List[Candle]) -> None:
        cutoff = _current_period_start("1m")
        fresh_completed = [c for c in fresh_full if c.time < cutoff]
        if not fresh_completed:
            return

        async with self._get_sym_lock(symbol):
            state_1m = await self.state.get_candles(symbol, "1m", limit=2000)
            state_completed_map = {c.time: c for c in state_1m if c.time < cutoff}
            fresh_map = {c.time: c for c in fresh_completed}

            diff = False
            if len(state_completed_map) != len(fresh_map):
                diff = True
            else:
                for t, fc in fresh_map.items():
                    sc = state_completed_map.get(t)
                    if sc is None or sc.open != fc.open or sc.high != fc.high \
                            or sc.low != fc.low or sc.close != fc.close:
                        diff = True
                        break
            if not diff:
                return

            live_bars = [c for c in state_1m if c.time >= cutoff]
            combined = sorted(
                {c.time: c for c in (fresh_completed + live_bars)}.values(),
                key=lambda c: c.time,
            )
            await self.state.set_candles(symbol, "1m", combined)
            for tf in DERIVED_TIMEFRAMES:
                self._agg_cache.pop((symbol, tf), None)
            await self._full_recompute_locked(symbol, combined, broadcast_candles=True)

    async def _full_recompute_locked(
        self,
        symbol: str,
        merged_1m: List[Candle],
        broadcast_candles: bool = True,
        broadcast_signals: bool = True,
    ) -> None:
        """
        Full UT Bot recompute over all 6 TFs. Replaces signals_by_symbol_tf,
        broadcasts candle deltas, diffs vs previous signals to emit events,
        and re-seeds the IncrementalContext per TF for the hot path.

        Caller must hold self._get_sym_lock(symbol).
        """
        if not merged_1m:
            return

        prev_signals: Dict[str, List[Signal]] = {}
        prev_last_candles: Dict[str, Optional[Candle]] = {}
        for tf in ["1m"] + list(DERIVED_TIMEFRAMES):
            prev_signals[tf] = await self.state.get_signals(symbol, tf, limit=10000)
            if broadcast_candles:
                cur = await self.state.get_candles(symbol, tf, limit=1)
                prev_last_candles[tf] = cur[-1] if cur else None

        # 1m signals + ctx seed
        signals_1m = self.engine.calculate_signals(symbol, "1m", merged_1m)
        ctx_1m = self.engine.seed_context(symbol, "1m", merged_1m, _current_period_start("1m"))
        if ctx_1m is not None:
            self._engine_ctx[(symbol, "1m")] = ctx_1m

        derived_updates: List[Tuple[str, List[Candle], List[Signal]]] = []
        broadcast_candle_list: List[Tuple[str, Candle]] = []
        new_signals_by_tf: Dict[str, List[Signal]] = {"1m": signals_1m}

        for tf in DERIVED_TIMEFRAMES:
            period = TIMEFRAME_SECONDS[tf]
            cache_key = (symbol, tf)
            prev_agg, prev_count = self._agg_cache.get(cache_key, ([], 0))
            aggregated = aggregate_candles_incremental(prev_agg, merged_1m, period, prev_count)
            self._agg_cache[cache_key] = (aggregated, len(merged_1m))

            signals_tf = self.engine.calculate_signals(symbol, tf, aggregated)
            derived_updates.append((tf, aggregated, signals_tf))
            new_signals_by_tf[tf] = signals_tf

            # Seed incremental ctx for this TF
            ctx_tf = self.engine.seed_context(symbol, tf, aggregated, _current_period_start(tf))
            if ctx_tf is not None:
                self._engine_ctx[(symbol, tf)] = ctx_tf

            if aggregated:
                broadcast_candle_list.append((tf, aggregated[-1]))

        all_updates: List[Tuple[str, List[Candle], List[Signal]]] = [
            ("1m", merged_1m, signals_1m)
        ] + derived_updates
        await self.state.batch_update_symbol(symbol, all_updates)

        if broadcast_candles:
            if merged_1m and self._candle_changed(prev_last_candles.get("1m"), merged_1m[-1]):
                await self.ws_manager.broadcast_candle(symbol, "1m", merged_1m[-1])
            for tf, last in broadcast_candle_list:
                if self._candle_changed(prev_last_candles.get(tf), last):
                    await self.ws_manager.broadcast_candle(symbol, tf, last)

        for tf, new_sigs in new_signals_by_tf.items():
            await self._diff_and_emit_signals(symbol, tf, prev_signals[tf], new_sigs, emit=broadcast_signals)

    async def _diff_and_emit_signals(
        self,
        symbol: str,
        timeframe: str,
        prev_list: List[Signal],
        new_list: List[Signal],
        emit: bool = True,
    ) -> None:
        prev_map = {s.id: s for s in prev_list}
        new_map = {s.id: s for s in new_list}

        for sid in prev_map.keys() - new_map.keys():
            await self.state.remove_recent_signal(sid)
            if emit:
                await self.ws_manager.broadcast_signal_remove(sid, symbol, timeframe)

        for sid, sig in new_map.items():
            prev = prev_map.get(sid)
            if prev is None:
                status, _ = await self.state.upsert_recent_signal(sig)
                if status in ("added", "updated") and emit:
                    await self.ws_manager.broadcast_signal_upsert(sig, status=status)
                    if status == "added":
                        await self.webhook_sender.send_signal(sig)
            else:
                if prev.type != sig.type or prev.score != sig.score or prev.price != sig.price:
                    status, _ = await self.state.upsert_recent_signal(sig)
                    if status in ("added", "updated") and emit:
                        await self.ws_manager.broadcast_signal_upsert(sig, status=status)
                        if status == "added":
                            await self.webhook_sender.send_signal(sig)

    # ── Public helpers (REST endpoints) ────────────────────────────

    async def fetch_symbol_timeframe(
        self, symbol: str, _timeframe: str, force_limit: Optional[int] = None
    ) -> None:
        limit = force_limit or self.settings.candle_limit_for("1m")
        try:
            candles = await self.futures_client.fetch_candles(symbol.upper(), "1m", limit)
            if candles:
                await self._apply_full_1m(symbol.upper(), candles)
                self._ready_symbols.add(symbol.upper())
        except Exception as e:
            await self.state.add_error(f"fetch {symbol}: {type(e).__name__}: {e}")

    async def scan_once(self) -> None:
        symbols = list(self.state.symbols)
        for sym in symbols:
            if self._stop_event.is_set():
                break
            try:
                full = await self.futures_client.fetch_candles(
                    sym, "1m", self.settings.candle_limit_for("1m")
                )
                if full:
                    await self._integrity_check(sym, full)
            except Exception as e:
                await self.state.add_error(f"scan_once {sym}: {type(e).__name__}: {e}")

    async def scan_timeframe(self, timeframe: str, full_bootstrap: bool = False) -> None:
        await self.state.set_phase(f"manual_scan:{timeframe}")
        if full_bootstrap:
            await self._bootstrap()
        else:
            await self.scan_once()
        await self.state.set_phase("ws_streaming")

    def _candle_changed(self, prev: Optional[Candle], current: Candle) -> bool:
        if prev is None:
            return True
        return (
            prev.time != current.time
            or prev.close != current.close
            or prev.high != current.high
            or prev.low != current.low
            or prev.volume != current.volume
        )

    # ── Diagnostics ───────────────────────────────────────────────

    def ready_status(self) -> dict:
        return {
            "ready_count": len(self._ready_symbols),
            "total_symbols": len(self._symbol_set),
            "dirty_count": len(self._dirty),
        }
