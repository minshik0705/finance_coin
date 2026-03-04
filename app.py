# app.py
from __future__ import annotations

import sys
import json
import asyncio
from pathlib import Path
from datetime import datetime, timezone, timedelta

import psycopg2
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request

from config import (
    TIMESCALEDB_HOST, TIMESCALEDB_PORT,
    TIMESCALEDB_USER, TIMESCALEDB_PASSWORD, TIMESCALEDB_DB,
)

app = FastAPI()

# static/, templates/ 폴더 연결
app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")


# ────────────────────────────────────────
# DB
# ────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=TIMESCALEDB_HOST,
        port=TIMESCALEDB_PORT,
        user=TIMESCALEDB_USER,
        password=TIMESCALEDB_PASSWORD,
        dbname=TIMESCALEDB_DB
    )


def fetch_ohlcv(symbol: str, exchange: str = "binance", hours: int = 3) -> list[dict]:
    """최근 N시간 1분봉 조회."""
    since = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    conn  = get_conn()
    try:
        with conn.cursor() as cur:
            #원래는 SELECT time, open, high, low, close, volume -> 중복 timestamp발생 app.py 오류 발생
            cur.execute("""
                SELECT DISTINCT ON (time) time, open, high, low, close, volume
                FROM ohlcv_1m
                WHERE symbol   = %s
                  AND exchange = %s
                  AND time     >= %s
                ORDER BY time ASC;
            """, (symbol, exchange, since))
            rows = cur.fetchall()
    finally:
        conn.close()

    return [
        {
            "time":   int(row[0].timestamp()),  # Unix timestamp (Chart.js용)
            "open":   float(row[1]),
            "high":   float(row[2]),
            "low":    float(row[3]),
            "close":  float(row[4]),
            "volume": float(row[5]),
        }
        for row in rows
    ]


def fetch_anomalies(symbol: str, exchange: str = "binance", hours: int = 3) -> list[dict]:
    """최근 N시간 이상탐지 결과 조회."""
    since = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    conn  = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT time, anomaly_score, severity, reason, ohlcv_time
                FROM anomaly_results
                WHERE symbol   = %s
                  AND exchange = %s
                  AND time     >= %s
                ORDER BY time DESC
                LIMIT 50;
            """, (symbol, exchange, since))
            rows = cur.fetchall()
    finally:
        conn.close()

    return [
        {
            "time":          row[0].strftime("%H:%M"),
            "anomaly_score": round(float(row[1]), 4),
            "severity":      row[2],
            "reason":        row[3],
            "ohlcv_time":    int(row[4].timestamp()),
        }
        for row in rows
    ]


def fetch_symbols(exchange: str = "binance") -> list[str]:
    """DB에 있는 심볼 목록 조회."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT symbol
                FROM ohlcv_1m
                WHERE exchange = %s
                ORDER BY symbol;
            """, (exchange,))
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


# ────────────────────────────────────────
# HTTP 엔드포인트
# ────────────────────────────────────────

@app.get("/")
async def index(request: Request):
    symbols = fetch_symbols()
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "symbols": symbols}
    )


@app.get("/api/ohlcv/{symbol}")
async def api_ohlcv(symbol: str, hours: int = 3):
    """차트 초기 데이터 로드용 REST API."""
    return fetch_ohlcv(symbol.upper(), hours=hours)


@app.get("/api/anomalies/{symbol}")
async def api_anomalies(symbol: str, hours: int = 3):
    """이상탐지 결과 로드용 REST API."""
    return fetch_anomalies(symbol.upper(), hours=hours)


# ────────────────────────────────────────
# WebSocket (실시간 업데이트)
# ────────────────────────────────────────

@app.websocket("/ws/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):
    """
    클라이언트가 연결하면 1분마다 최신 1분봉을 push.
    차트가 자동으로 업데이트됨.
    """
    await websocket.accept()
    symbol = symbol.upper()
    print(f"[WS] 연결: {symbol}")

    try:
        while True:
            # 최신 1분봉 1개 조회
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT time, open, high, low, close, volume
                        FROM ohlcv_1m
                        WHERE symbol   = %s
                          AND exchange = 'binance'
                        ORDER BY time DESC
                        LIMIT 1;
                    """, (symbol,))
                    row = cur.fetchone()
            finally:
                conn.close()

            if row:
                await websocket.send_json({
                    "type":   "ohlcv",
                    "time":   int(row[0].timestamp()),
                    "open":   float(row[1]),
                    "high":   float(row[2]),
                    "low":    float(row[3]),
                    "close":  float(row[4]),
                    "volume": float(row[5]),
                })

            # 최신 이상탐지 결과 조회
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT time, anomaly_score, severity, reason
                        FROM anomaly_results
                        WHERE symbol   = %s
                          AND exchange = 'binance'
                        ORDER BY time DESC
                        LIMIT 1;
                    """, (symbol,))
                    anom = cur.fetchone()
            finally:
                conn.close()

            if anom:
                await websocket.send_json({
                    "type":          "anomaly",
                    "time":          anom[0].strftime("%H:%M"),
                    "anomaly_score": round(float(anom[1]), 4),
                    "severity":      anom[2],
                    "reason":        anom[3],
                })

            await asyncio.sleep(60)  # 1분마다 push

    except WebSocketDisconnect:
        print(f"[WS] 연결 종료: {symbol}")


# ────────────────────────────────────────
# 실행
# ────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)