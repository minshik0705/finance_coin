# consumers/ohlcv_consumer.py
from __future__ import annotations

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import json
import time
from collections import defaultdict
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_TRADES,
    TIMESCALEDB_HOST, TIMESCALEDB_PORT,
    TIMESCALEDB_USER, TIMESCALEDB_PASSWORD, TIMESCALEDB_DB,
)

# ────────────────────────────────────────
# DB 연결
# ────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=TIMESCALEDB_HOST,
        port=TIMESCALEDB_PORT,
        user=TIMESCALEDB_USER,
        password=TIMESCALEDB_PASSWORD,
        dbname=TIMESCALEDB_DB
    )


def insert_ohlcv(conn, rows: list[dict]) -> None:
    """
    ohlcv_1m 테이블에 배치 insert.
    같은 (time, exchange, symbol) 이 이미 있으면 업데이트.
    """
    if not rows:
        return

    sql = """
        INSERT INTO ohlcv_1m
            (time, exchange, symbol, open, high, low, close, volume, trade_count)
        VALUES
            %s
        ON CONFLICT DO NOTHING;
    """

    values = [
        (
            r["time"], r["exchange"], r["symbol"],
            r["open"], r["high"], r["low"], r["close"],
            r["volume"], r["trade_count"]
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values)
    conn.commit()


# ────────────────────────────────────────
# 1분 버킷 집계
# ────────────────────────────────────────

def to_minute_bucket(timestamp_ms: int) -> datetime:
    """
    timestamp(ms) → 해당 분의 시작 시각 (UTC)
    예) 12:34:56.789 → 12:34:00.000
    """
    ts_sec = timestamp_ms / 1000
    dt = datetime.fromtimestamp(ts_sec, tz=timezone.utc)
    return dt.replace(second=0, microsecond=0)


def compute_ohlcv(bucket_key: tuple, ticks: list[dict]) -> dict:
    """
    tick 리스트 → OHLCV 딕셔너리
    bucket_key: (exchange, symbol, minute_datetime)
    """
    exchange, symbol, minute_dt = bucket_key

    prices  = [t["price"]    for t in ticks]
    volumes = [t["quantity"] for t in ticks]

    return {
        "time":        minute_dt,
        "exchange":    exchange,
        "symbol":      symbol,
        "open":        prices[0],
        "high":        max(prices),
        "low":         min(prices),
        "close":       prices[-1],
        "volume":      sum(volumes),
        "trade_count": len(ticks),
    }


# ────────────────────────────────────────
# Consumer 메인 루프
# ────────────────────────────────────────

def run():
    print("[INFO] ohlcv_consumer 시작")

    consumer = KafkaConsumer(
        TOPIC_TRADES,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="ohlcv-consumer-group-3",
        # consumer_timeout_ms 제거 → poll() 방식으로 변경
    )

    conn = get_conn()
    print("[INFO] TimescaleDB 연결 완료")

    buckets: dict = defaultdict(list)
    last_flush_minute: datetime | None = None

    try:
        while True:
            # ── 메시지 polling (최대 1초 대기) ──────────
            # poll()은 1초 후 무조건 반환 → flush 로직 실행 가능
            records = consumer.poll(timeout_ms=1000)

            for tp, messages in records.items():
                for message in messages:
                    tick = message.value

                    if tick.get("source_type") != "trade":
                        continue

                    symbol   = tick.get("symbol")
                    exchange = tick.get("exchange")
                    price    = tick.get("price")
                    quantity = tick.get("quantity")
                    ts_ms    = tick.get("timestamp")

                    if not all([symbol, exchange, price, quantity, ts_ms]):
                        continue

                    minute_dt  = to_minute_bucket(ts_ms)
                    bucket_key = (exchange, symbol, minute_dt)
                    buckets[bucket_key].append({
                        "price":    price,
                        "quantity": quantity,
                    })

            # ── Flush 로직 ──────────────────────────────
            now_minute = datetime.now(tz=timezone.utc).replace(
                second=0, microsecond=0
            )

            if last_flush_minute is None:
                last_flush_minute = now_minute
                continue

            if now_minute > last_flush_minute:
                to_insert = []
                done_keys = []

                for key, ticks in buckets.items():
                    _, _, minute_dt = key
                    if minute_dt < now_minute:
                        to_insert.append(compute_ohlcv(key, ticks))
                        done_keys.append(key)

                if to_insert:
                    try:
                        insert_ohlcv(conn, to_insert)
                        print(
                            f"[INFO] {now_minute.strftime('%H:%M')} "
                            f"저장 완료: {len(to_insert)}개 심볼"
                        )
                        for row in to_insert[:3]:
                            print(
                                f"       {row['exchange']} {row['symbol']} "
                                f"O:{row['open']:.4f} H:{row['high']:.4f} "
                                f"L:{row['low']:.4f} C:{row['close']:.4f} "
                                f"V:{row['volume']:.4f} ({row['trade_count']}건)"
                            )
                    except Exception as e:
                        print(f"[ERROR] DB insert 실패: {e}")
                        try:
                            conn = get_conn()
                        except Exception as ce:
                            print(f"[ERROR] DB 재연결 실패: {ce}")

                for key in done_keys:
                    del buckets[key]

                last_flush_minute = now_minute

    except KeyboardInterrupt:
        print("[INFO] ohlcv_consumer 종료")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    run()