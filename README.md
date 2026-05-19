# Gate USDT Perpetual Futures Signal Backend

Gate.io **USDT Perpetual Futures** 전체 계약을 대상으로 1m/5m/15m 캔들을 수집하고, 시그널을 계산해 REST API와 WebSocket으로 프론트에 전달하는 FastAPI 백엔드입니다.

## 핵심 변경점

이 버전은 Spot이 아니라 Futures endpoint를 씁니다.

```txt
GET /futures/{settle}/contracts
GET /futures/{settle}/tickers
GET /futures/{settle}/candlesticks
```

기본값은 `settle=usdt`라서 USDT perpetual futures를 대상으로 합니다.

## API 제한 회피 구조

Gate public endpoint 제한은 `200 requests / 10 seconds / endpoint`입니다.  
이 코드는 내부적으로 `PUBLIC_RPS_LIMIT=12`를 적용해서 약 `120 requests / 10 seconds` 이하로 요청을 제한합니다.

또한 매번 모든 분봉을 1분마다 다시 받지 않습니다.

```txt
1m  : 60초마다 전체 심볼 업데이트
5m  : 300초마다 전체 심볼 업데이트
15m : 900초마다 전체 심볼 업데이트
```

부트스트랩 때만 필요한 캔들 수를 크게 가져오고, 이후에는 `INCREMENTAL_CANDLE_LIMIT=5`만 가져와서 기존 캐시에 병합합니다.

## 구조

```txt
gate-futures-signal-backend/
  app/
    main.py
    config.py
    models.py
    state.py
    ws_manager.py
    routers/
      market.py
      ws.py
    services/
      futures_client.py
      indicators.py
      rate_limiter.py
      signal_engine.py
      scanner.py
      webhook.py
  requirements.txt
  railway.json
  .env.example
  .gitignore
  README.md
```

## 로컬 실행

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt

# Windows
copy .env.example .env

# macOS/Linux
cp .env.example .env

uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## Railway 배포

1. 이 폴더 전체를 GitHub repo에 push
2. Railway → New Project → Deploy from GitHub Repo
3. Variables에 `.env.example` 값 참고해서 환경변수 등록
4. Settings → Networking → Generate Domain

## 주요 API

```http
GET /health
GET /api/status
GET /api/symbols
GET /api/signals/recent?timeframe=all&limit=100
GET /api/signals/recent?timeframe=1m&limit=100
GET /api/candles?symbol=BTC_USDT&timeframe=1m&limit=300
GET /api/signals/by-symbol?symbol=BTC_USDT&timeframe=1m&limit=100
POST /api/scan/once
```

## WebSocket

```txt
/ws/signals
/ws/candles?symbol=BTC_USDT&timeframe=1m
```

## 프론트 연결 방식

1. 대시보드 진입 시 `/ws/signals` 연결
2. 최근 시그널 리스트 표시
3. 사용자가 코인 클릭 시 `/api/candles?symbol=...&timeframe=...` 요청
4. TradingView Lightweight Charts에 candles는 setData, signals는 markers로 표시
5. 차트가 열린 동안 `/ws/candles?symbol=...&timeframe=...` 연결

## 스캔 가능 심볼 수 계산

요청량은 대략 아래와 같습니다.

```txt
심볼 수 × (1/60 + 1/300 + 1/900) requests/sec
= 심볼 수 × 0.02111 requests/sec
```

`PUBLIC_RPS_LIMIT=12` 기준 이론상 약 568개 심볼까지 1m/5m/15m 전체 스캔이 가능합니다.

```txt
12 / 0.02111 ≈ 568
```

심볼 수가 이보다 많으면 방법은 세 가지입니다.

```txt
1. PUBLIC_RPS_LIMIT를 15 정도로 올림
2. SYMBOL_LIMIT로 거래량 상위 n개만 사용
3. 5m/15m 스캔 주기를 더 길게 늘림
```

공식 제한이 20r/s 수준이더라도 실운영은 60~75% 정도만 쓰는 게 안전합니다.

## 주의

- 이 코드는 MVP용 인메모리 캐시입니다.
- Railway가 재시작되면 캐시는 초기화되고 다시 부트스트랩합니다.
- 실제 자동매매 주문 기능은 포함하지 않았습니다.
- 지금 구조는 데이터 수집/시그널/차트용입니다.
