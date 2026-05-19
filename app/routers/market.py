from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from app.models import Status

router = APIRouter(prefix="/api", tags=["market"])


def normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace("-", "_").replace("/", "_")


@router.get("/status", response_model=Status)
async def get_status(request: Request):
    state = request.app.state.market_state
    return await state.snapshot_status()


@router.get("/rate-limit")
async def get_rate_limit(request: Request):
    limiter = request.app.state.futures_client.rate_limiter
    s = limiter.snapshot
    return {
        "configured_public_rps_limit": request.app.state.settings.public_rps_limit,
        "last_status_code": s.last_status_code,
        "header_limit": s.limit,
        "header_remaining": s.remain,
        "header_reset_timestamp": s.reset_timestamp,
        "seconds_until_reset": round(limiter.seconds_until_reset(), 3),
        "last_wait_seconds": round(s.last_wait_seconds, 4),
    }


@router.get("/ws-status")
async def get_ws_status(request: Request):
    """Diagnostic snapshot of the Gate WS subscriber and per-shard health."""
    sub = getattr(request.app.state, "ws_subscriber", None)
    scanner = getattr(request.app.state, "scanner", None)
    if sub is None:
        return {"ok": False, "error": "ws_subscriber not initialized"}
    out = {"ok": True, **sub.status_snapshot()}
    if scanner is not None and hasattr(scanner, "ready_status"):
        out["scanner"] = scanner.ready_status()
    return out


@router.get("/symbols")
async def get_symbols(request: Request):
    state = request.app.state.market_state
    async with state.lock:
        return {
            "market": "gate_usdt_perpetual_futures",
            "settle": request.app.state.settings.gate_settle,
            "count": len(state.symbols),
            "symbols": state.symbols,
        }


@router.get("/signals/recent")
async def get_recent_signals(
    request: Request,
    timeframe: str = Query("all"),
    limit: int = Query(100, ge=1, le=1000),
):
    state = request.app.state.market_state
    if timeframe != "all" and timeframe not in request.app.state.settings.timeframe_list:
        raise HTTPException(status_code=400, detail="invalid timeframe")

    signals = await state.get_recent_signals(timeframe=timeframe, limit=limit)
    return {"count": len(signals), "signals": [s.model_dump() for s in signals]}


@router.get("/signals/by-symbol")
async def get_signals_by_symbol(
    request: Request,
    symbol: str = Query(...),
    timeframe: str = Query("1m"),
    limit: int = Query(100, ge=1, le=1000),
):
    symbol = normalize_symbol(symbol)
    settings = request.app.state.settings
    if timeframe not in settings.timeframe_list:
        raise HTTPException(status_code=400, detail="invalid timeframe")

    scanner = request.app.state.scanner
    state = request.app.state.market_state

    candles = await state.get_candles(symbol, timeframe, limit=1)
    if not candles:
        await scanner.fetch_symbol_timeframe(symbol, timeframe, force_limit=settings.candle_limit_for(timeframe))

    signals = await state.get_signals(symbol, timeframe, limit=limit)
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "count": len(signals),
        "signals": [s.model_dump() for s in signals],
    }


@router.get("/candles")
async def get_candles(
    request: Request,
    symbol: str = Query(...),
    timeframe: str = Query("1m"),
    limit: int = Query(300, ge=1, le=2000),
):
    symbol = normalize_symbol(symbol)
    settings = request.app.state.settings
    if timeframe not in settings.timeframe_list:
        raise HTTPException(status_code=400, detail="invalid timeframe")

    scanner = request.app.state.scanner
    state = request.app.state.market_state

    candles = await state.get_candles(symbol, timeframe, limit=limit)
    if len(candles) < min(limit, settings.candle_limit_for(timeframe)):
        fetch_limit = min(max(limit, settings.candle_limit_for(timeframe)), 2000)
        await scanner.fetch_symbol_timeframe(symbol, timeframe, force_limit=fetch_limit)
        candles = await state.get_candles(symbol, timeframe, limit=limit)

    if not candles:
        raise HTTPException(status_code=404, detail="candles not found")

    signals = await state.get_signals(symbol, timeframe, limit=1000)
    start_time = candles[0].time
    end_time = candles[-1].time
    signals = [s for s in signals if start_time <= s.time <= end_time]

    return {
        "market": "gate_usdt_perpetual_futures",
        "symbol": symbol,
        "timeframe": timeframe,
        "candles": [c.model_dump() for c in candles],
        "signals": [s.model_dump() for s in signals],
    }


@router.get("/rankings/market-cap")
async def get_market_cap_ranking(
    request: Request,
    limit: int = Query(100, ge=1, le=100),
):
    state = request.app.state.market_state
    async with state.lock:
        gate_symbols = list(state.symbols)
    cmc = request.app.state.cmc_client
    items = await cmc.get_top_100(gate_symbols)
    return {"kind": "market_cap", "count": len(items), "items": items[:limit]}


@router.get("/rankings/volatility")
async def get_volatility_ranking(
    request: Request,
    timeframe: str = Query(...),
    limit: int = Query(100, ge=1, le=100),
):
    settings = request.app.state.settings
    if timeframe not in settings.timeframe_list:
        raise HTTPException(status_code=400, detail="invalid timeframe")
    rs = request.app.state.ranking_service
    items = rs.get_snapshot("volatility", timeframe)
    return {"kind": "volatility", "timeframe": timeframe, "count": len(items), "items": items[:limit]}


@router.get("/rankings/turnover")
async def get_turnover_ranking(
    request: Request,
    timeframe: str = Query(...),
    limit: int = Query(100, ge=1, le=100),
):
    settings = request.app.state.settings
    if timeframe not in settings.timeframe_list:
        raise HTTPException(status_code=400, detail="invalid timeframe")
    rs = request.app.state.ranking_service
    items = rs.get_snapshot("turnover", timeframe)
    return {"kind": "turnover", "timeframe": timeframe, "count": len(items), "items": items[:limit]}


@router.post("/scan/once")
async def scan_once(request: Request):
    scanner = request.app.state.scanner
    await scanner.scan_once()
    return {"ok": True}


@router.post("/scan/timeframe")
async def scan_timeframe(request: Request, timeframe: str = Query(...)):
    settings = request.app.state.settings
    if timeframe not in settings.timeframe_list:
        raise HTTPException(status_code=400, detail="invalid timeframe")

    scanner = request.app.state.scanner
    await scanner.scan_timeframe(timeframe, full_bootstrap=False)
    return {"ok": True, "timeframe": timeframe}
