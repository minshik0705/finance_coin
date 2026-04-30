from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

# project root를 import path에 추가
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import psycopg2

from config import (
    TIMESCALEDB_HOST,
    TIMESCALEDB_PORT,
    TIMESCALEDB_USER,
    TIMESCALEDB_PASSWORD,
    TIMESCALEDB_DB,
)

def main():
    if len(sys.argv) < 2:
        print("usage: python scripts/export_anomaly_results_parquet.py YYYY-MM-DD")
        sys.exit(1)

    target_date = sys.argv[1]
    day_start = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    conn = psycopg2.connect(
        host=TIMESCALEDB_HOST,
        port=TIMESCALEDB_PORT,
        user=TIMESCALEDB_USER,
        password=TIMESCALEDB_PASSWORD,
        dbname=TIMESCALEDB_DB,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    time,
                    exchange,
                    symbol,
                    anomaly_score,
                    is_anomaly,
                    severity,
                    reason,
                    ohlcv_time,
                    detected_at
                FROM anomaly_results
                WHERE ohlcv_time >= %s
                  AND ohlcv_time < %s
                ORDER BY ohlcv_time, exchange, symbol
                """,
                (day_start, day_end),
            )
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
    finally:
        conn.close()

    if not rows:
        print(f"[WARN] no anomaly rows for {target_date}")
        return

    df = pd.DataFrame(rows, columns=columns)

    for col in ["time", "ohlcv_time", "detected_at"]:
        df[col] = pd.to_datetime(df[col], utc=True)

    df["event_dt"] = df["ohlcv_time"].dt.strftime("%Y-%m-%d")

    base_dir = Path("/tmp/argus_export/anomaly_results")
    written = 0

    for (event_dt, exchange), group in df.groupby(["event_dt", "exchange"], sort=True):
        out_dir = base_dir / f"event_dt={event_dt}" / f"exchange={exchange}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_file = out_dir / "anomalies.parquet"
        group.drop(columns=["event_dt", "exchange"]).to_parquet(
            out_file,
            index=False,
            compression="snappy",
        )
        written += 1
        print(f"[OK] wrote {out_file} ({len(group)} rows)")

    print(f"[DONE] wrote {written} parquet file(s) under {base_dir}")

if __name__ == "__main__":
    main()
