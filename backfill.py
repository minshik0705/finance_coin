# backfill.py
# Binance REST API로 과거 1분봉 데이터를 가져와 TimescaleDB에 저장
#
# 사용법:
#   python backfill.py              # 기본: core_universe 심볼 7일치
#   python backfill.py --days 14    # 14일치
#   python backfill.py --symbol BTCUSDT --days 3  # 특정 심볼만
from __future__ import annotations

import sys
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta

sys.path.append(str(Path(__file__).resolve().parent))

import psycopg2
import psycopg2.extras
from binance.client import Client

from config import (
    API_KEY, API_SECRET,
    TIMESCALEDB_HOST, TIMESCALEDB_PORT,
    TIMESCALEDB_USER, TIMESCALEDB_PASSWORD, TIMESCALEDB_DB,
    CORE_UNIVERSE_FILE,
)

import json

# ────────────────────────────────────────
# 설정
# ────────────────────────────────────────

DEFAULT_DAYS     = 7
INTERVAL         = Client.KLINE_INTERVAL_1MINUTE
EXCHANGE_NAME    = "binance"
BATCH_SIZE       = 500   # DB insert 배치 크기
REQUEST_DELAY    = 0.2   # API 호출 간격 (초) — rate limit 방지


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


def insert_rows(conn, rows: list[tuple]) -> int:
    """
    ohlcv_1m에 배치 insert.
    중복 (time, exchange, symbol) 은 스킵.
    반환: 실제 삽입된 행 수
    """
    if not rows:
        return 0

    sql = """
        INSERT INTO ohlcv_1m
            (time, exchange, symbol, open, high, low, close, volume, trade_count)
        VALUES %s
        ON CONFLICT DO NOTHING;
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=BATCH_SIZE)
        # rowcount는 execute_values에서 신뢰도 낮으므로 별도 카운트 안 함
    conn.commit()
    return len(rows)


# ────────────────────────────────────────
# Binance API
# ────────────────────────────────────────

def fetch_klines(client: Client, symbol: str, days: int) -> list[tuple]:
    """
    Binance REST API로 1분봉 kline 조회.
    반환: [(time, exchange, symbol, open, high, low, close, volume, trade_count), ...]
    """
    start_str = f"{days} days ago UTC"

    print(f"  [FETCH] {symbol} {days}일치 요청 중...", end="", flush=True)
    raw = client.get_historical_klines(symbol, INTERVAL, start_str)
    print(f" {len(raw)}행 수신")

    rows = []
    for k in raw:
        # kline 컬럼: [open_time, open, high, low, close, volume, close_time,
        #              quote_asset_vol, trade_count, ...]
        open_time   = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc)
        open_price  = float(k[1])
        high_price  = float(k[2])
        low_price   = float(k[3])
        close_price = float(k[4])
        volume      = float(k[5])
        trade_count = int(k[8])

        rows.append((
            open_time, EXCHANGE_NAME, symbol,
            open_price, high_price, low_price, close_price,
            volume, trade_count,
        ))

    return rows


# ────────────────────────────────────────
# Universe 로드
# ────────────────────────────────────────

def load_symbols() -> list[str]:
    """core_universe.json에서 심볼 목록 로드."""
    data = json.loads(CORE_UNIVERSE_FILE.read_text(encoding="utf-8"))
    return [item["pair_symbol"] for item in data.get("membership", [])]


# ────────────────────────────────────────
# 메인
# ────────────────────────────────────────

def run(symbols: list[str], days: int) -> None:
    client = Client(API_KEY, API_SECRET)
    conn   = get_conn()

    print(f"\n{'='*50}")
    print(f"  Backfill 시작")
    print(f"  심볼: {symbols}")
    print(f"  기간: 최근 {days}일")
    print(f"{'='*50}\n")

    total_inserted = 0
    start_time = time.time()

    for i, symbol in enumerate(symbols, 1):
        print(f"[{i}/{len(symbols)}] {symbol}")
        try:
            rows = fetch_klines(client, symbol, days)

            if not rows:
                print(f"  [WARN] 데이터 없음, 스킵")
                continue

            insert_rows(conn, rows)
            total_inserted += len(rows)
            print(f"  [OK] {len(rows)}행 저장 완료")

        except Exception as e:
            print(f"  [ERROR] {symbol} 실패: {e}")

        # rate limit 방지
        if i < len(symbols):
            time.sleep(REQUEST_DELAY)

    conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'='*50}")
    print(f"  완료! 총 {total_inserted}행 저장")
    print(f"  소요시간: {elapsed:.1f}초")
    print(f"{'='*50}\n")


def verify(symbols: list[str]) -> None:
    """저장 결과 확인."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT symbol, COUNT(*) as rows,
                   MIN(time) as earliest,
                   MAX(time) as latest
            FROM ohlcv_1m
            WHERE exchange = %s
              AND symbol = ANY(%s)
            GROUP BY symbol
            ORDER BY symbol;
        """, (EXCHANGE_NAME, symbols))
        rows = cur.fetchall()
    conn.close()

    print("\n[검증 결과]")
    print(f"{'심볼':<12} {'행수':>8} {'시작':>24} {'끝':>24}")
    print("-" * 72)
    for row in rows:
        print(f"{row[0]:<12} {row[1]:>8} {str(row[2]):>24} {str(row[3]):>24}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Binance 1분봉 backfill")
    parser.add_argument("--days",   type=int, default=DEFAULT_DAYS,
                        help=f"수집할 일수 (기본: {DEFAULT_DAYS})")
    parser.add_argument("--symbol", type=str, default=None,
                        help="특정 심볼만 (예: BTCUSDT). 없으면 core_universe 전체")
    args = parser.parse_args()

    if args.symbol:
        symbols = [args.symbol.upper()]
    else:
        symbols = load_symbols()

    run(symbols, args.days)
    verify(symbols)