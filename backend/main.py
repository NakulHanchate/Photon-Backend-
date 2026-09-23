import asyncio
import json
import random
import os
import time
from typing import Dict, List, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import websockets
import httpx

load_dotenv()

DATA_SOURCE = os.getenv("DATA_SOURCE", "mock").lower()
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")

app = FastAPI(title="Photon Stock Ticker")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

STOCKS = {
    "BINANCE:BTCUSDT": 60000.0,
    "AAPL": 150.0,
    "GOOGL": 2800.0,
    "TSLA": 700.0,
    "AMZN": 3400.0,
    "MSFT": 300.0,
    "META": 330.0,
    "NFLX": 500.0,
    "NVDA": 220.0
}

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[WebSocket, Set[str]] = {}
        self.global_subscriptions: Set[str] = set()
        self.finnhub_ws = None

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[websocket] = set()

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            del self.active_connections[websocket]

    async def set_subscriptions(self, websocket: WebSocket, tickers: Set[str]):
        if websocket in self.active_connections:
            self.active_connections[websocket] = tickers
            
            if DATA_SOURCE == "finnhub" and self.finnhub_ws:
                new_tickers = tickers - self.global_subscriptions
                for ticker in new_tickers:
                    self.global_subscriptions.add(ticker)
                    await self.finnhub_ws.send(json.dumps({"type": "subscribe", "symbol": ticker}))

    async def broadcast(self, ticker: str, price: float):
        message = json.dumps({"type": "tick", "ticker": ticker, "price": price})
        for connection, subscriptions in list(self.active_connections.items()):
            if not subscriptions or ticker in subscriptions:
                try:
                    await connection.send_text(message)
                except Exception:
                    pass

manager = ConnectionManager()

async def mock_data_streamer():
    print("Starting Mock Data Streamer...")
    while True:
        await asyncio.sleep(1)
        num_to_update = random.randint(1, len(STOCKS))
        tickers_to_update = random.sample(list(STOCKS.keys()), num_to_update)
        for ticker in tickers_to_update:
            current_price = STOCKS.get(ticker, 100.0)
            change_percent = random.uniform(-0.01, 0.01)
            new_price = round(current_price * (1 + change_percent), 2)
            STOCKS[ticker] = new_price
            await manager.broadcast(ticker, new_price)

async def finnhub_data_streamer():
    if not FINNHUB_API_KEY:
        print("ERROR: FINNHUB_API_KEY not set. Falling back to mock data.")
        asyncio.create_task(mock_data_streamer())
        return

    print("Starting Finnhub Data Streamer...")
    uri = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
    
    while True:
        try:
            async with websockets.connect(uri) as ws:
                manager.finnhub_ws = ws
                
                # Resubscribe to globally tracked tickers on reconnect
                for ticker in manager.global_subscriptions:
                    await ws.send(json.dumps({"type": "subscribe", "symbol": ticker}))
                
                for ticker in STOCKS.keys():
                    if ticker not in manager.global_subscriptions:
                        manager.global_subscriptions.add(ticker)
                        await ws.send(json.dumps({"type": "subscribe", "symbol": ticker}))

                async for message in ws:
                    data = json.loads(message)
                    if data.get("type") == "trade":
                        for trade in data.get("data", []):
                            ticker = trade.get("s")
                            price = trade.get("p")
                            if ticker and price:
                                STOCKS[ticker] = price
                                await manager.broadcast(ticker, price)
        except Exception as e:
            print(f"Finnhub websocket error: {e}. Reconnecting in 5 seconds...")
            manager.finnhub_ws = None
            await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    if DATA_SOURCE == "finnhub":
        asyncio.create_task(finnhub_data_streamer())
    else:
        asyncio.create_task(mock_data_streamer())

@app.get("/")
async def root():
    return {"message": "Photon Stock Ticker backend is running. Connect to /ws for live data."}

@app.get("/api/history/{ticker}")
async def get_history(ticker: str):
    if DATA_SOURCE == "finnhub" and FINNHUB_API_KEY:
        to_time = int(time.time())
        from_time = to_time - (60 * 60 * 24)
        url = f"https://finnhub.io/api/v1/stock/candle?symbol={ticker}&resolution=1&from={from_time}&to={to_time}&token={FINNHUB_API_KEY}"
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(url)
                data = resp.json()
                if data.get("s") == "ok":
                    candles = []
                    for i in range(len(data['t'])):
                        candles.append({
                            "time": data['t'][i],
                            "open": data['o'][i],
                            "high": data['h'][i],
                            "low": data['l'][i],
                            "close": data['c'][i],
                        })
                    return candles
            except Exception as e:
                print(f"Error fetching history: {e}")
    
    candles = []
    base_price = STOCKS.get(ticker, 150.0)
    current_time = int(time.time())
    for i in range(60):
        candle_time = current_time - (60 - i) * 60
        close = base_price * (1 + random.uniform(-0.02, 0.02))
        high = close * (1 + random.uniform(0, 0.01))
        low = close * (1 - random.uniform(0, 0.01))
        open_price = low + random.uniform(0, high - low)
        candles.append({
            "time": candle_time,
            "open": round(open_price, 2),
            "high": round(high, 2),
            "low": round(low, 2),
            "close": round(close, 2)
        })
        base_price = close
    return candles

@app.get("/api/search")
async def search_ticker(q: str):
    if DATA_SOURCE == "finnhub" and FINNHUB_API_KEY:
        url = f"https://finnhub.io/api/v1/search?q={q}&token={FINNHUB_API_KEY}"
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(url)
                data = resp.json()
                results = [{"symbol": item.get("symbol"), "description": item.get("description")} for item in data.get("result", [])]
                return results[:5]
            except Exception as e:
                print(f"Error fetching search results: {e}")
                return []
    
    q_lower = q.lower()
    results = []
    for ticker in STOCKS.keys():
        if q_lower in ticker.lower():
            results.append({"symbol": ticker, "description": "Mock Data Stock"})
    return results

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        for ticker, price in STOCKS.items():
            await websocket.send_text(json.dumps({"type": "tick", "ticker": ticker, "price": price}))
            
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                if message.get("action") == "subscribe":
                    tickers = message.get("tickers", [])
                    ticker_set = {t.strip().upper() for t in tickers if t.strip()}
                    await manager.set_subscriptions(websocket, ticker_set)
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        manager.disconnect(websocket)
