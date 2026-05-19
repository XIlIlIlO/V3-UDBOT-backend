from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Dict, List, Set, Tuple

from fastapi import WebSocket

from app.models import Candle, Signal


class WebSocketManager:
    def __init__(self):
        self._lock = asyncio.Lock()
        self.signal_clients: Set[WebSocket] = set()
        self.candle_clients: Dict[Tuple[str, str], Set[WebSocket]] = defaultdict(set)
        self._dead: List[WebSocket] = []

    async def add_signal_client(self, websocket: WebSocket) -> None:
        async with self._lock:
            self.signal_clients.add(websocket)

    async def remove_signal_client(self, websocket: WebSocket) -> None:
        async with self._lock:
            self.signal_clients.discard(websocket)

    async def add_candle_client(self, symbol: str, timeframe: str, websocket: WebSocket) -> None:
        async with self._lock:
            self.candle_clients[(symbol, timeframe)].add(websocket)

    async def remove_candle_client(self, symbol: str, timeframe: str, websocket: WebSocket) -> None:
        async with self._lock:
            clients = self.candle_clients.get((symbol, timeframe))
            if clients:
                clients.discard(websocket)

    async def broadcast_signal_upsert(self, signal: Signal, status: str = "added") -> None:
        """
        status: 'added' (new) | 'updated' (same id, type/score changed)
        Frontend treats both as upsert by id.
        """
        payload = {
            "event": "signal_upsert",
            "status": status,
            "data": signal.model_dump(),
        }
        async with self._lock:
            clients = list(self.signal_clients)
            candle_watchers = list(
                self.candle_clients.get((signal.symbol, signal.timeframe), set())
            )

        self._send_all(clients, payload)
        self._send_all(candle_watchers, payload)
        await self._flush_dead()

    async def broadcast_signal_remove(self, signal_id: str, symbol: str, timeframe: str) -> None:
        payload = {
            "event": "signal_remove",
            "id": signal_id,
            "symbol": symbol,
            "timeframe": timeframe,
        }
        async with self._lock:
            clients = list(self.signal_clients)
            candle_watchers = list(
                self.candle_clients.get((symbol, timeframe), set())
            )

        self._send_all(clients, payload)
        self._send_all(candle_watchers, payload)
        await self._flush_dead()

    async def broadcast_candle(self, symbol: str, timeframe: str, candle: Candle) -> None:
        payload = {
            "event": "candle_update",
            "symbol": symbol,
            "timeframe": timeframe,
            "data": candle.model_dump(),
        }
        async with self._lock:
            clients = list(self.candle_clients.get((symbol, timeframe), set()))

        self._send_all(clients, payload)
        await self._flush_dead()

    async def broadcast_ranking_update(self, kind: str, timeframe: str, items: List[dict]) -> None:
        """Pushed to /ws/signals subscribers only (global feed clients)."""
        payload = {
            "event": "ranking_update",
            "kind": kind,
            "timeframe": timeframe,
            "items": items,
        }
        async with self._lock:
            clients = list(self.signal_clients)

        self._send_all(clients, payload)
        await self._flush_dead()

    def _send_all(self, clients: List[WebSocket], payload: dict) -> None:
        for ws in clients:
            try:
                asyncio.ensure_future(ws.send_json(payload))
            except Exception:
                self._dead.append(ws)

    async def _flush_dead(self) -> None:
        if not self._dead:
            return
        dead = self._dead
        self._dead = []
        async with self._lock:
            for ws in dead:
                self.signal_clients.discard(ws)
                for group in self.candle_clients.values():
                    group.discard(ws)
