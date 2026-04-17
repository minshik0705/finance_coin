from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone, timedelta

import pandas as pd
import psycopg2
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request

from config import (
    TIMESCALEDB_HOST, TIMESCALEDB_PORT,
    TIMESCALEDB_USER, TIMESCALEDB_PASSWORD, TIMESCALEDB_DB,
    CORE_UNIVERSE_FILE,
)

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

SUPPORTED_EXCHANGES = ["binance", "bybit", "okx"]


def get_conn():
    return psycopg2.connect(
        host=TIMESCALEDB_HOST,
        port=TIMESCALEDB_PORT,
        user=TIMESCALEDB_USER,
        password=TIMESCALEDB_PASSWORD,
        dbname=TIMESCALEDB_DB
    )


def load_core_symbols() -> list[str]:
    try:
        data = json.loads(CORE_UNIVERSE_FILE.read_text(encoding="utf-8"))
        symbols = []
        for item in data.get("membership", []):
            pair = (item.get("pair_symbol") or "").upper().strip()
            if pair:
                symbols.append(pair)
        return symbols or ["BTCUSDT", "ETHUSDT", "XRPUSDT", "BNBUSDT", "SOLUSDT"]
    except Exception:
        return ["BTCUSDT", "ETHUSDT", "XRPUSDT", "BNBUSDT", "SOLUSDT"]


def fetch_symbols() -> list[str]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT symbol
                FROM ohlcv_1m
                WHERE exchange = ANY(%s)
                ORDER BY symbol;
            """, (SUPPORTED_EXCHANGES,))
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def fetch_ohlcv(symbol: str, exchange: str = "binance", hours: int = 3) -> list[dict]:
    since = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (time) time, open, high, low, close, volume
                FROM ohlcv_1m
                WHERE symbol = %s
                  AND exchange = %s
                  AND time >= %s
                ORDER BY time ASC;
            """, (symbol, exchange, since))
            rows = cur.fetchall()
    finally:
        conn.close()

    return [
        {
            "time": int(row[0].timestamp()),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
        }
        for row in rows
    ]


def fetch_compare_ohlcv(symbol: str, exchanges: list[str], hours: int = 3) -> dict:
    since = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    conn = get_conn()
    result: dict[str, list[dict]] = {ex: [] for ex in exchanges}

    try:
        with conn.cursor() as cur:
            for exchange in exchanges:
                cur.execute("""
                    SELECT DISTINCT ON (time) time, open, high, low, close, volume
                    FROM ohlcv_1m
                    WHERE symbol = %s
                      AND exchange = %s
                      AND time >= %s
                    ORDER BY time ASC;
                """, (symbol, exchange, since))
                rows = cur.fetchall()

                result[exchange] = [
                    {
                        "time": int(row[0].timestamp()),
                        "open": float(row[1]),
                        "high": float(row[2]),
                        "low": float(row[3]),
                        "close": float(row[4]),
                        "volume": float(row[5]),
                    }
                    for row in rows
                ]
    finally:
        conn.close()

    return result


def fetch_anomalies(symbol: str, exchange: str = "all", hours: int = 3) -> list[dict]:
    since = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if exchange == "all":
                cur.execute("""
                    SELECT time, exchange, anomaly_score, severity, reason, ohlcv_time
                    FROM anomaly_results
                    WHERE symbol = %s
                      AND time >= %s
                    ORDER BY time DESC
                    LIMIT 50;
                """, (symbol, since))
            else:
                cur.execute("""
                    SELECT time, exchange, anomaly_score, severity, reason, ohlcv_time
                    FROM anomaly_results
                    WHERE symbol = %s
                      AND exchange = %s
                      AND time >= %s
                    ORDER BY time DESC
                    LIMIT 50;
                """, (symbol, exchange, since))
            rows = cur.fetchall()
    finally:
        conn.close()

    return [
        {
            "time": row[0].strftime("%H:%M"),
            "exchange": row[1],
            "anomaly_score": round(float(row[2]), 4),
            "severity": row[3],
            "reason": row[4],
            "ohlcv_time": int(row[5].timestamp()),
        }
        for row in rows
    ]


def fetch_global_anomalies(hours: int = 3) -> list[dict]:
    since = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    DATE_TRUNC('minute', time) AS minute_bucket,
                    symbol,
                    COUNT(DISTINCT exchange) AS exchange_count,
                    ARRAY_AGG(DISTINCT exchange ORDER BY exchange) AS exchanges,
                    AVG(anomaly_score) AS avg_score,
                    MAX(severity) AS max_severity
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
            "time": row[0].strftime("%H:%M"),
            "symbol": row[1],
            "exchange_count": row[2],
            "exchanges": row[3],
            "avg_score": round(float(row[4]), 4),
            "severity": row[5],
            "scope": "global" if row[2] >= 3 else "cross",
            "ohlcv_time": int(row[0].timestamp()),
        }
        for row in rows
    ]


def fetch_features(symbol: str, exchange: str = "binance", hours: int = 3) -> list[dict]:
    import numpy as np

    since = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (time) time, open, high, low, close, volume
                FROM ohlcv_1m
                WHERE symbol = %s
                  AND exchange = %s
                  AND time >= %s
                ORDER BY time ASC;
            """, (symbol, exchange, since))
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return []

    df = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close", "volume"])
    df["time"] = pd.to_datetime(df["time"], utc=True)

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)

    close = df["close"]
    high = df["high"]
    low = df["low"]
    open_ = df["open"]
    vol = df["volume"]
    denom = close.replace(0, np.nan)

    df["logret"] = np.log(close).diff()
    df["range"] = (high - low) / denom
    df["body"] = (close - open_) / denom
    df["upper_wick"] = (high - np.maximum(open_, close)) / denom
    df["lower_wick"] = (np.minimum(open_, close) - low) / denom
    df["vol_1h"] = df["logret"].rolling(60, min_periods=10).std()
    df["vol_1d"] = df["logret"].rolling(1440, min_periods=60).std()

    vmean = vol.rolling(1440, min_periods=60).mean()
    vstd = vol.rolling(1440, min_periods=60).std()
    df["vol_z_1d"] = (vol - vmean) / vstd.replace(0, np.nan)
    df = df.replace([np.inf, -np.inf], np.nan)

    return [
        {
            "time": int(row["time"].timestamp()),
            "logret": None if pd.isna(row["logret"]) else round(float(row["logret"]), 6),
            "range": None if pd.isna(row["range"]) else round(float(row["range"]), 6),
            "body": None if pd.isna(row["body"]) else round(float(row["body"]), 6),
            "upper_wick": None if pd.isna(row["upper_wick"]) else round(float(row["upper_wick"]), 6),
            "lower_wick": None if pd.isna(row["lower_wick"]) else round(float(row["lower_wick"]), 6),
            "vol_1h": None if pd.isna(row["vol_1h"]) else round(float(row["vol_1h"]), 6),
            "vol_z_1d": None if pd.isna(row["vol_z_1d"]) else round(float(row["vol_z_1d"]), 6),
        }
        for _, row in df.iterrows()
    ]


def fetch_latest_prices(symbol: str) -> list[dict]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH ranked AS (
                    SELECT
                        exchange,
                        symbol,
                        time,
                        close,
                        ROW_NUMBER() OVER (
                            PARTITION BY exchange, symbol
                            ORDER BY time DESC
                        ) AS rn
                    FROM ohlcv_1m
                    WHERE symbol = %s
                      AND exchange = ANY(%s)
                )
                SELECT exchange, symbol, time, close
                FROM ranked
                WHERE rn = 1
                ORDER BY exchange;
            """, (symbol, SUPPORTED_EXCHANGES))
            rows = cur.fetchall()
    finally:
        conn.close()

    return [
        {
            "exchange": row[0],
            "symbol": row[1],
            "time": int(row[2].timestamp()),
            "close": float(row[3]),
        }
        for row in rows
    ]


def fetch_core_overview(hours: int = 3) -> list[dict]:
    since = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    core_symbols = load_core_symbols()
    conn = get_conn()

    try:
        with conn.cursor() as cur:
            overview = []

            for symbol in core_symbols:
                cur.execute("""
                    WITH ranked AS (
                        SELECT
                            exchange,
                            symbol,
                            time,
                            close,
                            ROW_NUMBER() OVER (
                                PARTITION BY exchange, symbol
                                ORDER BY time DESC
                            ) AS rn
                        FROM ohlcv_1m
                        WHERE symbol = %s
                          AND exchange = ANY(%s)
                    )
                    SELECT exchange, time, close
                    FROM ranked
                    WHERE rn = 1
                    ORDER BY exchange;
                """, (symbol, SUPPORTED_EXCHANGES))
                price_rows = cur.fetchall()

                prices = []
                close_values = []
                latest_ts = None

                for exchange, ts, close in price_rows:
                    close_f = float(close)
                    prices.append({
                        "exchange": exchange,
                        "time": int(ts.timestamp()),
                        "close": close_f,
                    })
                    close_values.append(close_f)
                    if latest_ts is None or ts > latest_ts:
                        latest_ts = ts

                spread_pct = None
                if len(close_values) >= 2:
                    max_p = max(close_values)
                    min_p = min(close_values)
                    if min_p > 0:
                        spread_pct = round((max_p - min_p) / min_p * 100, 4)

                cur.execute("""
                    SELECT
                        COUNT(DISTINCT exchange) AS exchange_count,
                        ARRAY_AGG(DISTINCT exchange ORDER BY exchange) AS exchanges,
                        MAX(severity) AS max_severity,
                        MAX(time) AS latest_anomaly_time
                    FROM anomaly_results
                    WHERE symbol = %s
                      AND is_anomaly = true
                      AND time >= %s;
                """, (symbol, since))
                row = cur.fetchone()

                exchange_count = row[0] or 0
                anomaly_exchanges = row[1] or []
                max_severity = row[2]
                latest_anomaly_time = row[3]

                scope = "none"
                if exchange_count >= 3:
                    scope = "global"
                elif exchange_count >= 2:
                    scope = "cross"
                elif exchange_count == 1:
                    scope = "local"

                overview.append({
                    "symbol": symbol,
                    "display_symbol": symbol.replace("USDT", ""),
                    "prices": prices,
                    "spread_pct": spread_pct,
                    "latest_time": int(latest_ts.timestamp()) if latest_ts else None,
                    "anomaly_scope": scope,
                    "anomaly_exchange_count": exchange_count,
                    "anomaly_exchanges": anomaly_exchanges,
                    "anomaly_severity": max_severity,
                    "latest_anomaly_time": int(latest_anomaly_time.timestamp()) if latest_anomaly_time else None,
                })

            return overview
    finally:
        conn.close()


@app.get("/")
async def index(request: Request):
    symbols = fetch_symbols()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "symbols": symbols,
            "default_exchange": "binance",
        }
    )


@app.get("/api/symbols")
async def api_symbols():
    return fetch_symbols()


@app.get("/api/core_overview")
async def api_core_overview(hours: int = 3):
    return fetch_core_overview(hours=hours)


@app.get("/api/ohlcv/{symbol}")
async def api_ohlcv(symbol: str, hours: int = 3, exchange: str = "binance"):
    exchange = exchange.lower()
    if exchange not in SUPPORTED_EXCHANGES:
        exchange = "binance"
    return fetch_ohlcv(symbol.upper(), exchange=exchange, hours=hours)


@app.get("/api/ohlcv_compare/{symbol}")
async def api_ohlcv_compare(
    symbol: str,
    hours: int = 3,
    exchanges: str = Query("binance,bybit,okx")
):
    exchange_list = [
        ex.strip().lower()
        for ex in exchanges.split(",")
        if ex.strip().lower() in SUPPORTED_EXCHANGES
    ]
    if not exchange_list:
        exchange_list = SUPPORTED_EXCHANGES
    return fetch_compare_ohlcv(symbol.upper(), exchange_list, hours=hours)


@app.get("/api/features/{symbol}")
async def api_features(symbol: str, hours: int = 3, exchange: str = "binance"):
    exchange = exchange.lower()
    if exchange not in SUPPORTED_EXCHANGES:
        exchange = "binance"
    return fetch_features(symbol.upper(), exchange=exchange, hours=hours)


@app.get("/api/latest_prices/{symbol}")
async def api_latest_prices(symbol: str):
    return fetch_latest_prices(symbol.upper())


@app.get("/api/anomalies/global")
async def api_global_anomalies(hours: int = 3):
    return fetch_global_anomalies(hours=hours)


@app.get("/api/anomalies/{symbol}")
async def api_anomalies(symbol: str, hours: int = 3, exchange: str = "all"):
    exchange = exchange.lower()
    if exchange != "all" and exchange not in SUPPORTED_EXCHANGES:
        exchange = "all"
    return fetch_anomalies(symbol.upper(), exchange=exchange, hours=hours)


@app.websocket("/ws/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):
    await websocket.accept()
    symbol = symbol.upper()

    try:
        while True:
            payload = {"type": "snapshot", "symbol": symbol, "prices": [], "anomalies": []}

            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    for exchange in SUPPORTED_EXCHANGES:
                        cur.execute("""
                            SELECT time, open, high, low, close, volume
                            FROM ohlcv_1m
                            WHERE symbol = %s
                              AND exchange = %s
                            ORDER BY time DESC
                            LIMIT 1;
                        """, (symbol, exchange))
                        row = cur.fetchone()
                        if row:
                            payload["prices"].append({
                                "exchange": exchange,
                                "time": int(row[0].timestamp()),
                                "open": float(row[1]),
                                "high": float(row[2]),
                                "low": float(row[3]),
                                "close": float(row[4]),
                                "volume": float(row[5]),
                            })

                    cur.execute("""
                        SELECT time, exchange, anomaly_score, severity, reason, ohlcv_time
                        FROM anomaly_results
                        WHERE symbol = %s
                        ORDER BY time DESC
                        LIMIT 10;
                    """, (symbol,))
                    rows = cur.fetchall()

                    seen = set()
                    for row in rows:
                        key = (row[1], int(row[5].timestamp()))
                        if key in seen:
                            continue
                        seen.add(key)
                        payload["anomalies"].append({
                            "time": row[0].strftime("%H:%M"),
                            "exchange": row[1],
                            "anomaly_score": round(float(row[2]), 4),
                            "severity": row[3],
                            "reason": row[4],
                            "ohlcv_time": int(row[5].timestamp()),
                        })
            finally:
                conn.close()

            await websocket.send_json(payload)
            await asyncio.sleep(60)

    except WebSocketDisconnect:
        pass


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
