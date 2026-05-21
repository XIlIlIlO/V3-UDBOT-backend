from __future__ import annotations

import json
from functools import lru_cache
from typing import Dict, List

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    gate_base_url: str = "https://api.gateio.ws/api/v4"
    gate_ws_url: str = "wss://fx-ws.gateio.ws/v4/ws/usdt"
    gate_settle: str = "usdt"
    market_quote: str = "USDT"

    symbol_limit: int = 0
    use_top_volume: bool = True
    exclude_delisting: bool = True
    # Gate.io USDT-perp 중 stocks/indices/metals/commodities/forex 같은 비-크립토
    # contract_type을 제외할지 여부. True면 contract_type == "" 만 포함.
    exclude_non_crypto: bool = True

    # All timeframes served (1m fetched from API/WS, rest derived)
    timeframes: str = "1m,3m,5m,15m,30m,1h"
    candle_limits_json: str = '{"1m":2000,"3m":666,"5m":400,"15m":133,"30m":66,"1h":33}'
    incremental_candle_limit: int = 5

    # Legacy field — kept for the /api/status estimated_request_rate calc.
    # Not used as a polling interval anymore (WS drives steady state).
    scan_interval_seconds: int = 60

    # REST limiter — kept conservative (5 rps = 50 req/10s, vs Gate's 200/10s cap).
    public_rps_limit: float = 5.0
    public_burst: int = 3

    max_retries: int = 3
    request_timeout_seconds: int = 20
    rate_limit_backoff_seconds: int = 2

    bootstrap_on_start: bool = True

    max_recent_signals: int = 5000
    cors_origins: str = "*"

    webhook_url: str = ""
    signal_min_score: int = 70

    # ── WS subscriber ──
    ws_symbols_per_shard: int = 150
    ws_subscribe_chunk_delay_ms: int = 50

    # ── Realtime processor (WS tick → recompute) ──
    processor_tick_ms: int = 100
    recompute_min_interval_ms: int = 500

    # ── Watchdog (REST gap-fill on stale WS) ──
    watchdog_interval_sec: int = 5
    watchdog_stale_sec: int = 30
    watchdog_max_per_round: int = 50

    # ── Integrity sweep (full 2000-candle reverification) ──
    integrity_sweep_interval_sec: int = 10
    integrity_sweep_batch_size: int = 20

    # ── CoinMarketCap (시총 top 100) ──
    cmc_api_key: str = ""
    cmc_refresh_seconds: int = 3600  # 1시간. free trial 월 크레딧 안전선.

    # ── Ranking service (변동성/거래대금 top 100, WS 푸시) ──
    ranking_tick_seconds: float = 1.0

    # ── Telegram heartbeat (alive 주기 알림) ──
    # 인스턴스 식별자 — 메시지에 표시됨 (예: office, railway)
    instance_name: str = "unknown"
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    # heartbeat 간격(초). 0 또는 음수면 비활성화. 기본 300초 = 5분.
    telegram_heartbeat_interval_sec: int = 300

    @property
    def timeframe_list(self) -> List[str]:
        return [x.strip() for x in self.timeframes.split(",") if x.strip()]

    @property
    def candle_limits(self) -> Dict[str, int]:
        try:
            data = json.loads(self.candle_limits_json)
            return {str(k): min(int(v), 2000) for k, v in data.items()}
        except Exception:
            return {"1m": 2000, "3m": 666, "5m": 400, "15m": 133, "30m": 66, "1h": 33}

    @property
    def cors_origin_list(self) -> List[str]:
        raw = self.cors_origins.strip()
        if raw == "*":
            return ["*"]
        return [x.strip() for x in raw.split(",") if x.strip()]

    def candle_limit_for(self, timeframe: str) -> int:
        return int(self.candle_limits.get(timeframe, 100))


@lru_cache
def get_settings() -> Settings:
    return Settings()
