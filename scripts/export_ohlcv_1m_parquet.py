from __future__ import annotations

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import argparse
import logging
import shutil
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import psycopg2

from config import (
    TIMESCALEDB_DB,
    TIMESCALEDB_HOST,
    TIMESCALEDB_PASSWORD,
    TIMESCALEDB_PORT,
    TIMESCALEDB_USER,
)

log = logging.getLogger("export_ohlcv_1m_parquet")

TABLE_NAME = "ohlcv_1m"
DEFAULT_OUT_DIR = Path("/tmp/argus_export/ohlcv_1m")
DEFAULT_EXCHANGES = ("binance", "bybit", "okx")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export TimescaleDB ohlcv_1m rows into Hive-style Parquet partitions: "
            "dt=YYYY-MM-DD/exchange=<name>/ohlcv.parquet"
        )
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="UTC start date inclusive (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        help="UTC end date inclusive (YYYY-MM-DD). Omit to export only start-date.",
    )
    parser.add_argument(
        "--exchanges",
        nargs="+",
        default=list(DEFAULT_EXCHANGES),
        help="Exchange list. Default: binance bybit okx",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help=f"Local output root. Default: {DEFAULT_OUT_DIR}",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete an existing partition directory before writing new parquet.",
    )
    parser.add_argument(
        "--compression",
        default="snappy",
        choices=["snappy", "gzip", "brotli", "zstd", "none"],
        help="Parquet compression codec. Default: snappy",
    )
    return parser.parse_args()


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def get_conn():
    return psycopg2.connect(
        host=TIMESCALEDB_HOST,
        port=TIMESCALEDB_PORT,
        user=TIMESCALEDB_USER,
        password=TIMESCALEDB_PASSWORD,
        dbname=TIMESCALEDB_DB,
    )


def iter_dates(start_dt: date, end_dt: date) -> Iterable[date]:
    current = start_dt
    while current <= end_dt:
        yield current
        current += timedelta(days=1)


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df["time"] = pd.to_datetime(df["time"], utc=True)

    float_cols = ["open", "high", "low", "close", "volume"]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    if "trade_count" in df.columns:
        df["trade_count"] = (
            pd.to_numeric(df["trade_count"], errors="coerce")
            .fillna(0)
            .astype("int64")
        )

    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype("string")

    for col in ["dt", "exchange"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    return df


def fetch_partition_df(conn, exchange: str, day: date) -> pd.DataFrame:
    start_ts = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end_ts = start_ts + timedelta(days=1)

    query = f"""
        SELECT
            time,
            symbol,
            open,
            high,
            low,
            close,
            volume,
            trade_count
        FROM {TABLE_NAME}
        WHERE exchange = %(exchange)s
          AND time >= %(start_ts)s
          AND time < %(end_ts)s
        ORDER BY time ASC, symbol ASC
    """

    return pd.read_sql_query(
        sql=query,
        con=conn,
        params={
            "exchange": exchange,
            "start_ts": start_ts,
            "end_ts": end_ts,
        },
    )


def write_partition(
    df: pd.DataFrame,
    out_root: Path,
    day: date,
    exchange: str,
    overwrite: bool,
    compression: str,
) -> Path:
    partition_dir = out_root / f"dt={day.isoformat()}" / f"exchange={exchange}"

    if partition_dir.exists() and overwrite:
        shutil.rmtree(partition_dir)

    partition_dir.mkdir(parents=True, exist_ok=True)

    outfile = partition_dir / "ohlcv.parquet"
    parquet_compression = None if compression == "none" else compression
    df.to_parquet(outfile, index=False, compression=parquet_compression)
    return outfile


def main() -> int:
    args = parse_args()
    setup_logging()

    start_day = date.fromisoformat(args.start_date)
    end_day = date.fromisoformat(args.end_date or args.start_date)
    out_root = Path(args.out_dir)

    log.info(
        "export start | table=%s | start=%s | end=%s | exchanges=%s | out=%s",
        TABLE_NAME,
        start_day,
        end_day,
        args.exchanges,
        out_root,
    )

    total_rows = 0
    written_files: list[Path] = []

    with get_conn() as conn:
        for day in iter_dates(start_day, end_day):
            for exchange in args.exchanges:
                df = fetch_partition_df(conn, exchange=exchange, day=day)
                if df.empty:
                    log.info("skip empty partition | dt=%s | exchange=%s", day, exchange)
                    continue

                df = normalize_df(df)
                outfile = write_partition(
                    df=df,
                    out_root=out_root,
                    day=day,
                    exchange=exchange,
                    overwrite=args.overwrite,
                    compression=args.compression,
                )

                row_count = len(df)
                total_rows += row_count
                written_files.append(outfile)

                min_time = df["time"].min()
                max_time = df["time"].max()
                symbol_count = df["symbol"].nunique(dropna=True)

                log.info(
                    "wrote %s rows | dt=%s | exchange=%s | symbols=%s | range=%s ~ %s | file=%s",
                    row_count,
                    day,
                    exchange,
                    symbol_count,
                    min_time,
                    max_time,
                    outfile,
                )

    log.info("export done | files=%s | rows=%s", len(written_files), total_rows)

    for path in written_files:
        print(path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
