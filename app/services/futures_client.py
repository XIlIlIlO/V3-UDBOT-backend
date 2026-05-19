from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import httpx

from app.config import Settings
from app.models import Candle
from app.services.rate_limiter import AsyncPacedRateLimiter


class GateFuturesClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.rate_limiter = AsyncPacedRateLimiter(
            requests_per_second=settings.public_rps_limit,
            burst=settings.public_burst,
        )
        self.client = httpx.AsyncClient(
            base_url=settings.gate_base_url.rstrip("/"),
            timeout=httpx.Timeout(float(settings.request_timeout_seconds), connect=10.0),
            headers={"Accept": "application/json"},
        )
        # symbol → quanto_multiplier (filled on list_symbols)
        self.multipliers: Dict[str, float] = {}

    async def close(self) -> None:
        await self.client.aclose()

    async def list_symbols(self) -> List[str]:
        contracts = await self._list_all_contracts()
        symbols: List[str] = []

        for c in contracts:
            name = c.get("name") or c.get("contract")
            status = str(c.get("status", "")).lower()
            in_delisting = bool(c.get("in_delisting", False))

            if not name:
                continue
            if not name.endswith("_" + self.settings.market_quote):
                continue
            if status and status != "trading":
                continue
            if self.settings.exclude_delisting and in_delisting:
                continue

            symbols.append(name)

            # Store quanto_multiplier for volume conversion
            try:
                self.multipliers[name] = float(c.get("quanto_multiplier", 1))
            except (ValueError, TypeError):
                self.multipliers[name] = 1.0

        symbols = sorted(set(symbols))

        if self.settings.use_top_volume:
            symbols = await self._sort_by_quote_volume(symbols)

        if self.settings.symbol_limit and self.settings.symbol_limit > 0:
            symbols = symbols[: self.settings.symbol_limit]

        return symbols

    async def fetch_candles(self, symbol: str, timeframe: str, limit: int) -> List[Candle]:
        safe_limit = max(1, min(int(limit), 2000))

        raw = await self._get_json(
            f"/futures/{self.settings.gate_settle}/candlesticks",
            params={
                "contract": symbol,
                "interval": timeframe,
                "limit": safe_limit,
            },
        )

        multiplier = self.multipliers.get(symbol, 1.0)

        candles: List[Candle] = []
        for item in raw:
            candle = self._parse_futures_candle(item, multiplier)
            if candle:
                candles.append(candle)

        candles.sort(key=lambda x: x.time)
        return candles[-safe_limit:]

    async def _list_all_contracts(self) -> List[Dict[str, Any]]:
        all_contracts: List[Dict[str, Any]] = []
        offset = 0
        limit = 100

        while True:
            page = await self._get_json(
                f"/futures/{self.settings.gate_settle}/contracts",
                params={"limit": limit, "offset": offset},
            )

            if not isinstance(page, list) or not page:
                break

            all_contracts.extend(page)

            if len(page) < limit:
                break

            offset += limit

        return all_contracts

    async def _sort_by_quote_volume(self, symbols: List[str]) -> List[str]:
        try:
            tickers = await self._get_json(f"/futures/{self.settings.gate_settle}/tickers")
        except Exception:
            return symbols

        volume_map: Dict[str, float] = {}

        for t in tickers:
            contract = t.get("contract")
            if not contract:
                continue

            raw_volume = (
                t.get("volume_24h_quote")
                or t.get("volume_24h_settle")
                or t.get("volume_24h_usd")
                or t.get("volume_24h_base")
                or t.get("volume_24h")
                or 0
            )
            try:
                volume_map[contract] = float(raw_volume)
            except Exception:
                volume_map[contract] = 0.0

        return sorted(symbols, key=lambda s: volume_map.get(s, 0.0), reverse=True)

    async def _get_json(self, path: str, params: Optional[dict] = None) -> Any:
        last_error: Optional[Exception] = None

        for attempt in range(self.settings.max_retries + 1):
            try:
                await self.rate_limiter.acquire()
                res = await self.client.get(path, params=params)
                self.rate_limiter.observe_response_headers(res.headers, res.status_code)

                if res.status_code == 429:
                    wait = self.rate_limiter.seconds_until_reset()
                    if wait <= 0:
                        wait = self.settings.rate_limit_backoff_seconds * (attempt + 1)
                    await self.rate_limiter.backoff(wait + 0.25)
                    await asyncio.sleep(wait + 0.25)
                    continue

                if 500 <= res.status_code < 600:
                    wait = self.settings.rate_limit_backoff_seconds * (attempt + 1)
                    await asyncio.sleep(wait)
                    continue

                res.raise_for_status()
                return res.json()

            except Exception as e:
                last_error = e
                if attempt >= self.settings.max_retries:
                    break
                wait = self.settings.rate_limit_backoff_seconds * (attempt + 1)
                await asyncio.sleep(wait)

        raise last_error or RuntimeError("Gate request failed")

    def _parse_futures_candle(self, item: Any, multiplier: float = 1.0) -> Optional[Candle]:
        try:
            if isinstance(item, dict):
                quote_volume = item.get("sum", None)
                contract_volume = item.get("v", 0)

                if quote_volume is None:
                    quote_volume = contract_volume

                # contract_volume × multiplier = coin quantity
                coin_volume = float(contract_volume or 0) * multiplier

                return Candle(
                    time=int(float(item["t"])),
                    open=float(item["o"]),
                    high=float(item["h"]),
                    low=float(item["l"]),
                    close=float(item["c"]),
                    volume=float(quote_volume or 0),
                    contract_volume=coin_volume,
                )

            if isinstance(item, list) and len(item) >= 6:
                coin_volume = float(item[1] or 0) * multiplier
                return Candle(
                    time=int(float(item[0])),
                    volume=float(item[1] or 0),
                    close=float(item[2]),
                    high=float(item[3]),
                    low=float(item[4]),
                    open=float(item[5]),
                    contract_volume=coin_volume,
                )

        except Exception:
            return None

        return None
