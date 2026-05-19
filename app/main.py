from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.routers.market import router as market_router
from app.routers.ws import router as ws_router
from app.services.cmc_client import CmcClient
from app.services.futures_client import GateFuturesClient
from app.services.rankings import RankingService
from app.services.scanner import MarketScanner
from app.services.webhook import WebhookSender
from app.services.ws_subscriber import GateFuturesWsSubscriber
from app.state import MarketState
from app.ws_manager import WebSocketManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()

    futures_client = GateFuturesClient(settings)
    market_state = MarketState(settings)
    ws_manager = WebSocketManager()
    webhook_sender = WebhookSender(settings.webhook_url)
    cmc_client = CmcClient(
        api_key=settings.cmc_api_key,
        refresh_seconds=settings.cmc_refresh_seconds,
    )
    ranking_service = RankingService(
        settings=settings,
        state=market_state,
        ws_manager=ws_manager,
    )

    scanner = MarketScanner(
        settings=settings,
        futures_client=futures_client,
        state=market_state,
        ws_manager=ws_manager,
        webhook_sender=webhook_sender,
    )

    ws_subscriber = GateFuturesWsSubscriber(
        url=settings.gate_ws_url,
        symbols_per_shard=settings.ws_symbols_per_shard,
        on_candle=scanner.on_ws_candle,
        on_reconnect=scanner.on_ws_reconnect,
        subscribe_chunk_delay_ms=settings.ws_subscribe_chunk_delay_ms,
    )

    app.state.settings = settings
    app.state.futures_client = futures_client
    app.state.market_state = market_state
    app.state.ws_manager = ws_manager
    app.state.webhook_sender = webhook_sender
    app.state.scanner = scanner
    app.state.ws_subscriber = ws_subscriber
    app.state.cmc_client = cmc_client
    app.state.ranking_service = ranking_service

    scanner.start()
    ranking_service.start()

    # Start WS subscriber after bootstrap completes — wait for symbols to load
    async def _start_ws_when_ready():
        # Poll until scanner has symbols loaded; cap wait
        for _ in range(600):  # up to 10 min
            if market_state.symbols:
                phase = market_state.current_phase
                # Wait until bootstrap is past initial-loading phase
                if phase in ("ws_streaming", "") or phase.startswith("scheduled"):
                    break
                if phase.startswith("bootstrap_") or phase == "bootstrap":
                    # Bootstrap is in progress; we can launch WS in parallel — it's safe.
                    break
            await asyncio.sleep(1)
        if market_state.symbols:
            ws_subscriber.start(list(market_state.symbols))

    ws_starter_task = asyncio.create_task(_start_ws_when_ready())

    yield

    ws_starter_task.cancel()
    try:
        await ws_starter_task
    except BaseException:
        pass
    await ws_subscriber.stop()
    await ranking_service.stop()
    await scanner.stop()
    await futures_client.close()
    await webhook_sender.close()
    await cmc_client.close()


app = FastAPI(
    title="Gate USDT Perpetual Futures Signal Backend",
    version="0.3.0",
    lifespan=lifespan,
)

settings = get_settings()
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(market_router)
app.include_router(ws_router)


@app.get("/")
async def root():
    return {
        "ok": True,
        "name": "Gate USDT Perpetual Futures Signal Backend",
        "market": "gate_usdt_perpetual_futures",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/health")
async def health():
    return {"ok": True}
