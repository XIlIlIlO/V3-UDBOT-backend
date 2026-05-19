"""
Gate.io Futures WebSocket subscriber.

Connects to wss://fx-ws.gateio.ws/v4/ws/usdt and subscribes to
futures.candlesticks channel for 1m interval, sharded across multiple
connections to stay under per-connection subscription soft limits.

Higher timeframes (3m/5m/.../1h) are derived from 1m by the scanner,
so we only need to subscribe to 1m here.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from typing import Awaitable, Callable, Deque, List, Optional

import websockets

# Callback signatures
OnCandleHandler = Callable[[str, dict], Awaitable[None]]
OnReconnectHandler = Callable[[List[str]], Awaitable[None]]


class GateFuturesWsShard:
    """One persistent WS connection handling a slice of symbols."""

    def __init__(
        self,
        shard_id: int,
        url: str,
        symbols: List[str],
        on_candle: OnCandleHandler,
        on_reconnect: OnReconnectHandler,
        subscribe_chunk_delay_ms: int = 0,
    ):
        self.shard_id = shard_id
        self.url = url
        self.symbols = list(symbols)
        self.on_candle = on_candle
        self.on_reconnect = on_reconnect
        self.subscribe_chunk_delay_ms = max(0, int(subscribe_chunk_delay_ms))

        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

        # Diagnostics
        self.connected: bool = False
        self.last_msg_at: float = 0.0
        self.connect_count: int = 0
        self.last_error: str = ""
        self.msg_count: int = 0
        self.update_count: int = 0
        self.subscribe_ack_count: int = 0
        self.subscribe_error_count: int = 0
        self.last_subscribe_error: str = ""
        self.last_raw_sample: str = ""  # Truncated sample of most recent message
        self.recent_errors: Deque[str] = deque(maxlen=10)
        self.pong_count: int = 0
        self.last_pong_at: float = 0.0

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except BaseException:
                pass

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            heartbeat_task: Optional[asyncio.Task] = None
            try:
                # Disable RFC 6455 ping — Gate ignores it. We send Gate-format
                # application ping ourselves (channel="futures.ping").
                async with websockets.connect(
                    self.url,
                    ping_interval=None,
                    close_timeout=5,
                    max_size=2**22,
                ) as ws:
                    self.connected = True
                    self.connect_count += 1
                    backoff = 1.0
                    await self._subscribe_all(ws)
                    heartbeat_task = asyncio.create_task(self._heartbeat(ws))
                    try:
                        await self.on_reconnect(self.symbols)
                    except Exception as e:
                        msg = f"on_reconnect: {type(e).__name__}: {e}"
                        self.last_error = msg
                        self.recent_errors.append(msg)
                    await self._reader_loop(ws)
            except asyncio.CancelledError:
                if heartbeat_task:
                    heartbeat_task.cancel()
                return
            except Exception as e:
                msg = f"connect/loop: {type(e).__name__}: {e}"
                self.last_error = msg
                self.recent_errors.append(msg)
            finally:
                if heartbeat_task and not heartbeat_task.done():
                    heartbeat_task.cancel()
                self.connected = False

            if self._stop.is_set():
                return
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)

    async def _heartbeat(self, ws) -> None:
        """Send Gate application-level ping every 15s to keep the connection alive."""
        try:
            while True:
                await asyncio.sleep(15)
                msg = {
                    "time": int(time.time()),
                    "channel": "futures.ping",
                }
                await ws.send(json.dumps(msg))
        except asyncio.CancelledError:
            return
        except Exception as e:
            err = f"heartbeat: {type(e).__name__}: {e}"
            self.last_error = err
            self.recent_errors.append(err)

    async def _subscribe_all(self, ws) -> None:
        for i, sym in enumerate(self.symbols):
            msg = {
                "time": int(time.time()),
                "channel": "futures.candlesticks",
                "event": "subscribe",
                "payload": ["1m", sym],
            }
            await ws.send(json.dumps(msg))
            if self.subscribe_chunk_delay_ms > 0 and (i + 1) % 25 == 0:
                await asyncio.sleep(self.subscribe_chunk_delay_ms / 1000.0)

    async def _reader_loop(self, ws) -> None:
        async for raw in ws:
            self.last_msg_at = time.time()
            self.msg_count += 1
            # Keep a small sample for diagnostics
            try:
                sample = raw if isinstance(raw, str) else raw.decode("utf-8", "replace")
            except Exception:
                sample = "<binary>"
            self.last_raw_sample = sample[:300]

            try:
                data = json.loads(raw)
            except Exception:
                continue

            channel = data.get("channel")
            event = data.get("event")
            err = data.get("error")
            if isinstance(err, dict) and err:
                # Gate signals errors via {"error": {"code": .., "message": ..}}
                err_msg = f"{channel}/{event}: code={err.get('code')} msg={err.get('message')}"
                self.subscribe_error_count += 1
                self.last_subscribe_error = err_msg
                self.recent_errors.append(err_msg)
                continue

            # Gate application-level pong response
            if channel == "futures.pong" or (channel == "futures.ping" and event != "subscribe"):
                self.pong_count += 1
                self.last_pong_at = time.time()
                continue

            if channel != "futures.candlesticks":
                continue

            if event == "subscribe":
                # Subscribe ack — might include result.status
                self.subscribe_ack_count += 1
                result = data.get("result") or {}
                status = result.get("status") if isinstance(result, dict) else None
                if status and status != "success":
                    err_msg = f"sub status={status} result={result}"
                    self.subscribe_error_count += 1
                    self.last_subscribe_error = err_msg
                    self.recent_errors.append(err_msg)
                continue

            if event != "update":
                continue

            self.update_count += 1
            result = data.get("result")
            if not isinstance(result, list):
                continue

            for item in result:
                n = item.get("n", "")
                if not isinstance(n, str) or "_" not in n:
                    continue
                interval, _, symbol = n.partition("_")
                if interval != "1m" or not symbol:
                    continue
                try:
                    await self.on_candle(symbol, item)
                except Exception as e:
                    err_msg = f"on_candle({symbol}): {type(e).__name__}: {e}"
                    self.last_error = err_msg
                    self.recent_errors.append(err_msg)

    def status(self) -> dict:
        now = time.time()
        return {
            "id": self.shard_id,
            "symbols": len(self.symbols),
            "connected": self.connected,
            "connect_count": self.connect_count,
            "msg_count": self.msg_count,
            "update_count": self.update_count,
            "subscribe_ack_count": self.subscribe_ack_count,
            "subscribe_error_count": self.subscribe_error_count,
            "last_subscribe_error": self.last_subscribe_error,
            "pong_count": self.pong_count,
            "last_pong_at": self.last_pong_at,
            "seconds_since_last_pong": (now - self.last_pong_at) if self.last_pong_at > 0 else None,
            "last_msg_at": self.last_msg_at,
            "seconds_since_last_msg": (now - self.last_msg_at) if self.last_msg_at > 0 else None,
            "last_error": self.last_error,
            "recent_errors": list(self.recent_errors),
            "last_raw_sample": self.last_raw_sample,
            "first_symbol": self.symbols[0] if self.symbols else None,
            "last_symbol": self.symbols[-1] if self.symbols else None,
        }


class GateFuturesWsSubscriber:
    """Manages multiple shard connections covering the full symbol list."""

    def __init__(
        self,
        url: str,
        symbols_per_shard: int,
        on_candle: OnCandleHandler,
        on_reconnect: OnReconnectHandler,
        subscribe_chunk_delay_ms: int = 0,
    ):
        self.url = url
        self.symbols_per_shard = max(20, int(symbols_per_shard))
        self.on_candle = on_candle
        self.on_reconnect = on_reconnect
        self.subscribe_chunk_delay_ms = subscribe_chunk_delay_ms
        self.shards: List[GateFuturesWsShard] = []
        self.started_at: float = 0.0

    def start(self, symbols: List[str]) -> None:
        chunks: List[List[str]] = []
        for i in range(0, len(symbols), self.symbols_per_shard):
            chunks.append(symbols[i : i + self.symbols_per_shard])

        for i, chunk in enumerate(chunks):
            shard = GateFuturesWsShard(
                shard_id=i,
                url=self.url,
                symbols=chunk,
                on_candle=self.on_candle,
                on_reconnect=self.on_reconnect,
                subscribe_chunk_delay_ms=self.subscribe_chunk_delay_ms,
            )
            shard.start()
            self.shards.append(shard)
        self.started_at = time.time()

    async def stop(self) -> None:
        if not self.shards:
            return
        await asyncio.gather(
            *(s.stop() for s in self.shards), return_exceptions=True
        )
        self.shards.clear()

    def status_snapshot(self) -> dict:
        now = time.time()
        return {
            "url": self.url,
            "symbols_per_shard": self.symbols_per_shard,
            "shard_count": len(self.shards),
            "started_at": self.started_at,
            "uptime_seconds": (now - self.started_at) if self.started_at > 0 else 0,
            "totals": {
                "connected": sum(1 for s in self.shards if s.connected),
                "msg_count": sum(s.msg_count for s in self.shards),
                "update_count": sum(s.update_count for s in self.shards),
                "subscribe_ack_count": sum(s.subscribe_ack_count for s in self.shards),
                "subscribe_error_count": sum(s.subscribe_error_count for s in self.shards),
                "connect_count": sum(s.connect_count for s in self.shards),
                "pong_count": sum(s.pong_count for s in self.shards),
            },
            "shards": [s.status() for s in self.shards],
        }
