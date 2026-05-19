"""
Volatility / turnover top-100 ranking per timeframe.

Computed from `state.candles[sym][tf]`'s latest bar. A background ticker
recomputes every `ranking_tick_seconds`, compares with the previous snapshot,
and broadcasts only on diff via `ws_manager.broadcast_ranking_update`.

  - volatility: ((high - low) / low) * 100, descending
  - turnover : Candle.volume (quote USDT), descending
"""
from __future__ import annotations

import asyncio
from typing import Dict, List, Optional, Tuple

from app.config import Settings
from app.models import Candle
from app.state import MarketState
from app.ws_manager import WebSocketManager


RankItem = dict  # {rank, symbol, value}


class RankingService:
    KINDS = ("volatility", "turnover")
    TOP_N = 100

    def __init__(self, settings: Settings, state: MarketState, ws_manager: WebSocketManager):
        self.settings = settings
        self.state = state
        self.ws_manager = ws_manager

        # snapshots[(kind, tf)] = [{rank, symbol, value}, ...]
        self._snapshots: Dict[Tuple[str, str], List[RankItem]] = {}
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except BaseException:
                pass

    async def _loop(self) -> None:
        tick = max(0.2, float(self.settings.ranking_tick_seconds))
        while not self._stop.is_set():
            try:
                await asyncio.sleep(tick)
                await self._tick()
            except asyncio.CancelledError:
                return
            except Exception as e:
                await self.state.add_error(f"ranking loop: {type(e).__name__}: {e}")

    async def _tick(self) -> None:
        for tf in self.settings.timeframe_list:
            latest = await self._collect_latest(tf)
            if not latest:
                continue

            new_vol = self._compute_volatility(latest)
            new_tov = self._compute_turnover(latest)

            if self._snapshots.get(("volatility", tf)) != new_vol:
                self._snapshots[("volatility", tf)] = new_vol
                await self.ws_manager.broadcast_ranking_update("volatility", tf, new_vol)

            if self._snapshots.get(("turnover", tf)) != new_tov:
                self._snapshots[("turnover", tf)] = new_tov
                await self.ws_manager.broadcast_ranking_update("turnover", tf, new_tov)

    async def _collect_latest(self, tf: str) -> List[Tuple[str, Candle]]:
        async with self.state.lock:
            out: List[Tuple[str, Candle]] = []
            for sym, by_tf in self.state.candles.items():
                data = by_tf.get(tf)
                if data:
                    out.append((sym, data[-1]))
            return out

    @classmethod
    def _compute_volatility(cls, latest: List[Tuple[str, Candle]]) -> List[RankItem]:
        items: List[Tuple[str, float]] = []
        for sym, c in latest:
            if c.low <= 0 or c.high <= 0:
                continue
            v = ((c.high - c.low) / c.low) * 100.0
            if v <= 0:
                continue
            items.append((sym, v))
        items.sort(key=lambda x: x[1], reverse=True)
        return [
            {"rank": i + 1, "symbol": sym, "value": round(val, 4)}
            for i, (sym, val) in enumerate(items[: cls.TOP_N])
        ]

    @classmethod
    def _compute_turnover(cls, latest: List[Tuple[str, Candle]]) -> List[RankItem]:
        items: List[Tuple[str, float]] = []
        for sym, c in latest:
            if c.volume > 0:
                items.append((sym, c.volume))
        items.sort(key=lambda x: x[1], reverse=True)
        return [
            {"rank": i + 1, "symbol": sym, "value": round(val, 4)}
            for i, (sym, val) in enumerate(items[: cls.TOP_N])
        ]

    def get_snapshot(self, kind: str, tf: str) -> List[RankItem]:
        return list(self._snapshots.get((kind, tf), []))

    def all_snapshots(self) -> Dict[str, Dict[str, List[RankItem]]]:
        out: Dict[str, Dict[str, List[RankItem]]] = {}
        for (kind, tf), items in self._snapshots.items():
            out.setdefault(kind, {})[tf] = list(items)
        return out
