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
    since = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    conn  = get_conn()
    try:
        with conn.cursor() as cur:
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
            "time":   int(row[0].timestamp()),
            "open":   float(row[1]),
            "high":   float(row[2]),
            "low":    float(row[3]),
            "close":  float(row[4]),
            "volume": float(row[5]),
        }
        for row in rows
    ]


def fetch_anomalies(symbol: str, exchange: str = "all", hours: int = 3) -> list[dict]:
    since = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    conn  = get_conn()
    try:
        with conn.cursor() as cur:
            if exchange == "all":
                cur.execute("""
                    SELECT time, exchange, anomaly_score, severity, reason, ohlcv_time
                    FROM anomaly_results
                    WHERE symbol = %s
                      AND time   >= %s
                    ORDER BY time DESC
                    LIMIT 50;
                """, (symbol, since))
            else:
                cur.execute("""
                    SELECT time, exchange, anomaly_score, severity, reason, ohlcv_time
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
            "exchange":      row[1],
            "anomaly_score": round(float(row[2]), 4),
            "severity":      row[3],
            "reason":        row[4],
            "ohlcv_time":    int(row[5].timestamp()),
        }
        for row in rows
    ]


def fetch_global_anomalies(hours: int = 3) -> list[dict]:
    """같은 심볼이 2개 이상 거래소에서 동시에 이상 탐지된 경우."""
    since = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    conn  = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    DATE_TRUNC('minute', time) AS minute_bucket,
                    symbol,
                    COUNT(DISTINCT exchange)                        AS exchange_count,
                    ARRAY_AGG(DISTINCT exchange ORDER BY exchange)  AS exchanges,
                    AVG(anomaly_score)                              AS avg_score,
                    MAX(severity)                                   AS max_severity
                FROM anomaly_results
                WHERE is_anomaly = true
                  AND time >= %s
                GROUP BY minute_bucket, symbol
                HAVING COUNT(DISTINCT exchange) >= 2
                ORDER BY minute_bucket DESC
                LIMIT 50;
            """, (since,))
            rows = cur.fetchall()
    finally:
        conn.close()

    return [
        {
            "time":           row[0].strftime("%H:%M"),
            "symbol":         row[1],
            "exchange_count": row[2],
            "exchanges":      row[3],
            "avg_score":      round(float(row[4]), 4),
            "severity":       row[5],
            "scope":          "global" if row[2] == 3 else "cross",
        }
        for row in rows
    ]



def fetch_features(symbol: str, exchange: str = "binance", hours: int = 3) -> list[dict]:
    """OHLCV에서 이상탐지 피처 계산 후 반환."""
    import numpy as np
    since = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    conn  = get_conn()
    try:
        with conn.cursor() as cur:
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

    if not rows:
        return []

    import pandas as pd
    df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
    df["time"] = pd.to_datetime(df["time"], utc=True)
    for col in ["open","high","low","close","volume"]:
        df[col] = df[col].astype(float)

    # 피처 계산 (anomaly_detect.py와 동일)
    close  = df["close"]
    high   = df["high"]
    low    = df["low"]
    open_  = df["open"]
    vol    = df["volume"]
    denom  = close.replace(0, np.nan)

    df["logret"]     = np.log(close).diff()
    df["range"]      = (high - low) / denom
    df["body"]       = (close - open_) / denom
    df["upper_wick"] = (high - np.maximum(open_, close)) / denom
    df["lower_wick"] = (np.minimum(open_, close) - low) / denom
    df["vol_1h"]     = df["logret"].rolling(60,   min_periods=10).std()
    df["vol_1d"]     = df["logret"].rolling(1440, min_periods=60).std()
    vmean            = vol.rolling(1440, min_periods=60).mean()
    vstd             = vol.rolling(1440, min_periods=60).std()
    df["vol_z_1d"]   = (vol - vmean) / vstd.replace(0, np.nan)

    df = df.replace([np.inf, -np.inf], np.nan)

    return [
        {
            "time":        int(row["time"].timestamp()),
            "logret":      None if pd.isna(row["logret"])     else round(float(row["logret"]),     6),
            "range":       None if pd.isna(row["range"])      else round(float(row["range"]),      6),
            "body":        None if pd.isna(row["body"])       else round(float(row["body"]),       6),
            "upper_wick":  None if pd.isna(row["upper_wick"]) else round(float(row["upper_wick"]), 6),
            "lower_wick":  None if pd.isna(row["lower_wick"]) else round(float(row["lower_wick"]), 6),
            "vol_1h":      None if pd.isna(row["vol_1h"])     else round(float(row["vol_1h"]),     6),
            "vol_z_1d":    None if pd.isna(row["vol_z_1d"])   else round(float(row["vol_z_1d"]),   6),
        }
        for _, row in df.iterrows()
    ]

def fetch_symbols(exchange: str = "binance") -> list[str]:
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
async def api_ohlcv(symbol: str, hours: int = 3, exchange: str = "binance"):
    return fetch_ohlcv(symbol.upper(), exchange=exchange, hours=hours)



@app.get("/api/features/{symbol}")
async def api_features(symbol: str, hours: int = 3, exchange: str = "binance"):
    """이상탐지 피처 시계열 반환."""
    return fetch_features(symbol.upper(), exchange=exchange, hours=hours)

@app.get("/api/anomalies/global")
async def api_global_anomalies(hours: int = 3):
    """2개 이상 거래소에서 동시 탐지된 global anomaly."""
    return fetch_global_anomalies(hours=hours)


@app.get("/api/anomalies/{symbol}")
async def api_anomalies(symbol: str, hours: int = 3, exchange: str = "all"):
    return fetch_anomalies(symbol.upper(), exchange=exchange, hours=hours)


# ────────────────────────────────────────
# WebSocket
# ────────────────────────────────────────

@app.websocket("/ws/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):
    await websocket.accept()
    symbol = symbol.upper()
    print(f"[WS] 연결: {symbol}")

    try:
        while True:
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

            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT time, exchange, anomaly_score, severity, reason
                        FROM anomaly_results
                        WHERE symbol = %s
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
                    "exchange":      anom[1],
                    "anomaly_score": round(float(anom[2]), 4),
                    "severity":      anom[3],
                    "reason":        anom[4],
                })

            await asyncio.sleep(60)

    except WebSocketDisconnect:
        print(f"[WS] 연결 종료: {symbol}")


# ────────────────────────────────────────
# 실행
# ────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
