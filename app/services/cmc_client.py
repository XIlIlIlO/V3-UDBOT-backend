"""CoinMarketCap top-100 client with on-disk-less in-memory caching."""
from __future__ import annotations

import asyncio
import time
from typing import Iterable, List, Optional

import httpx


class CmcClient:
    BASE_URL = "https://pro-api.coinmarketcap.com"

    def __init__(self, api_key: str = "", refresh_seconds: int = 3600):
        self.api_key = (api_key or "").strip()
        self.refresh_seconds = max(60, int(refresh_seconds))
        self._client = httpx.AsyncClient(
            base_url=self.BASE_URL,
            timeout=httpx.Timeout(15.0, connect=10.0),
            headers={
                "Accept": "application/json",
                "X-CMC_PRO_API_KEY": self.api_key,
            },
        )
        # Cached top-100 raw items, sorted by CMC rank.
        self._cache: List[dict] = []
        self._cache_at: float = 0.0
        self._last_error: str = ""
        self._lock = asyncio.Lock()

    async def close(self) -> None:
        await self._client.aclose()

    async def get_top_100(self, gate_symbols: Iterable[str]) -> List[dict]:
        """Top 100 by market cap, annotated with Gate USDT-perp availability.

        Item shape: {rank, cmc_symbol, name, gate_symbol|null, gate_listed}
        """
        gate_set = set(gate_symbols)
        async with self._lock:
            now = time.time()
            stale = (now - self._cache_at) >= self.refresh_seconds
            if (not self._cache or stale) and self.api_key:
                await self._refresh_locked(now)
            return self._annotate(self._cache, gate_set)

    async def _refresh_locked(self, now: float) -> None:
        try:
            res = await self._client.get(
                "/v1/cryptocurrency/listings/latest",
                params={
                    "limit": 100,
                    "convert": "USD",
                    "sort": "market_cap",
                    "sort_dir": "desc",
                },
            )
            res.raise_for_status()
            payload = res.json() or {}
            data = payload.get("data") or []
            self._cache = [
                {
                    "rank": i + 1,
                    "cmc_symbol": str(item.get("symbol", "")).upper(),
                    "name": item.get("name", ""),
                }
                for i, item in enumerate(data[:100])
                if item.get("symbol")
            ]
            self._cache_at = now
            self._last_error = ""
        except Exception as e:
            self._last_error = f"{type(e).__name__}: {e}"
            # Keep stale cache on failure so frontend still has something to show.

    @staticmethod
    def _annotate(cache: List[dict], gate_set: set) -> List[dict]:
        out: List[dict] = []
        for item in cache:
            gate = f"{item['cmc_symbol']}_USDT"
            listed = gate in gate_set
            out.append({
                "rank": item["rank"],
                "cmc_symbol": item["cmc_symbol"],
                "name": item["name"],
                "gate_symbol": gate if listed else None,
                "gate_listed": listed,
            })
        return out

    def status(self) -> dict:
        return {
            "cached_count": len(self._cache),
            "cache_age_seconds": (time.time() - self._cache_at) if self._cache_at > 0 else None,
            "refresh_seconds": self.refresh_seconds,
            "last_error": self._last_error,
            "has_api_key": bool(self.api_key),
        }
