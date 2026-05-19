from __future__ import annotations

import httpx

from app.models import Signal


class WebhookSender:
    def __init__(self, webhook_url: str = ""):
        self.webhook_url = webhook_url.strip()
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0))

    async def close(self) -> None:
        await self.client.aclose()

    async def send_signal(self, signal: Signal) -> None:
        if not self.webhook_url:
            return

        payload = {
            "event": "signal",
            "market": "gate_usdt_perpetual_futures",
            "symbol": signal.symbol,
            "timeframe": signal.timeframe,
            "type": signal.type,
            "price": signal.price,
            "score": signal.score,
            "time": signal.time,
            "reason": signal.reason,
        }

        try:
            await self.client.post(self.webhook_url, json=payload)
        except Exception:
            return
