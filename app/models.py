from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class Candle(BaseModel):
    time: int
    open: float
    high: float
    low: float
    close: float

    # futures에서는 sum이 quote volume, v가 contract size volume인 경우가 있음.
    # 차트/지표에는 quote volume 우선 사용.
    volume: float = 0.0
    contract_volume: float = 0.0


class Signal(BaseModel):
    id: str
    symbol: str
    timeframe: str
    time: int
    price: float
    type: Literal["BUY", "SELL"]
    score: int = Field(ge=0, le=100)
    reason: str = ""
    marker_position: Literal["aboveBar", "belowBar"] = "belowBar"
    marker_shape: Literal["arrowUp", "arrowDown"] = "arrowUp"


class Status(BaseModel):
    ok: bool
    market: str
    settle: str
    symbols_loaded: int
    timeframes: list[str]
    recent_signals: int
    public_rps_limit: float
    estimated_request_rate_per_sec: float
    estimated_request_rate_per_10s: float
    last_scan_started_at: Optional[int] = None
    last_scan_finished_at: Optional[int] = None
    scanner_running: bool = False
    current_phase: str = ""
    errors: list[str] = []
