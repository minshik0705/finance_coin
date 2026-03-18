# airflow/dags/backfill_dag.py
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from binance.client import Client

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

EXCHANGE = "binance"
EXPECTED_ROWS_PER_DAY = 1440
GAP_THRESHOLD = 1425  # 1440 * 0.99


# ────────────────────────────────────────
# 공통 헬퍼
# ────────────────────────────────────────

def _get_project_root() -> Path:
    return Path(
        Variable.get("PROJECT_ROOT", default_var="/opt/airflow/project")
    )


def _get_core_universe_path() -> Path:
    return Path(
        Variable.get(
            "CORE_UNIVERSE_PATH",
            default_var=str(_get_project_root() / "core_universe.json"),
        )
    )


def _get_db_conn():
    return psycopg2.connect(
        host=Variable.get("TIMESCALEDB_HOST", default_var="timescaledb"),
        port=Variable.get("TIMESCALEDB_PORT", default_var="5432"),
        user=Variable.get("TIMESCALEDB_USER", default_var="tsdb"),
        password=Variable.get("TIMESCALEDB_PASSWORD", default_var="tsdb"),
        dbname=Variable.get("TIMESCALEDB_DB", default_var="crypto"),
    )


def _get_binance_client() -> Client:
    api_key = Variable.get("BINANCE_API_KEY", default_var="")
    api_secret = Variable.get("BINANCE_API_SECRET", default_var="")
    return Client(api_key, api_secret)


def _load_core_symbols() -> list[str]:
    universe_path = _get_core_universe_path()
    if not universe_path.exists():
        raise FileNotFoundError(f"core_universe.json not found: {universe_path}")

    data = json.loads(universe_path.read_text(encoding="utf-8"))
    return [item["pair_symbol"] for item in data.get("membership", [])]


def _get_target_day_bounds(context) -> tuple[datetime, datetime, str]:
    """
    Airflow schedule 기준 D-1 하루(UTC)를 계산
    """
    execution_date: datetime = context["data_interval_start"]
    target_date = execution_date.date() - timedelta(days=1)

    day_start = datetime(
        target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc
    )
    day_end = day_start + timedelta(days=1)
    return day_start, day_end, str(target_date)


# ────────────────────────────────────────
# Task 1: gap 확인
# ────────────────────────────────────────

def check_gaps(**context) -> None:
    day_start, day_end, target_date = _get_target_day_bounds(context)
    symbols = _load_core_symbols()

    log.info("gap 확인 날짜: %s", target_date)
    log.info("대상 심볼: %s", symbols)

    gap_symbols: list[str] = []

    conn = _get_db_conn()
    try:
        with conn.cursor() as cur:
            for symbol in symbols:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM ohlcv_1m
                    WHERE exchange = %s
                      AND symbol   = %s
                      AND time >= %s
                      AND time <  %s
                    """,
                    (EXCHANGE, symbol, day_start, day_end),
                )
                count = cur.fetchone()[0]

                if count < GAP_THRESHOLD:
                    gap_symbols.append(symbol)
                    log.warning(
                        "[%s] row_count=%d < threshold=%d (expected=%d)",
                        symbol,
                        count,
                        GAP_THRESHOLD,
                        EXPECTED_ROWS_PER_DAY,
                    )
                else:
                    log.info("[%s] row_count=%d OK", symbol, count)

    finally:
        conn.close()

    ti = context["ti"]
    ti.xcom_push(key="gap_symbols", value=gap_symbols)
    ti.xcom_push(key="target_date", value=target_date)

    log.info("gap 심볼 목록: %s", gap_symbols)


# ────────────────────────────────────────
# Task 2: backfill 실행
# ────────────────────────────────────────

def backfill_symbols(**context) -> None:
    ti = context["ti"]
    gap_symbols: list[str] = ti.xcom_pull(key="gap_symbols", task_ids="check_gaps")
    target_date: str = ti.xcom_pull(key="target_date", task_ids="check_gaps")

    if not gap_symbols:
        log.info("gap 없음. backfill 스킵")
        return

    day_start = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    start_ms = int(day_start.timestamp() * 1000)
    end_ms = int(day_end.timestamp() * 1000)

    log.info("backfill 시작: target_date=%s, gap_symbols=%s", target_date, gap_symbols)

    client = _get_binance_client()
    conn = _get_db_conn()

    insert_sql = """
        INSERT INTO ohlcv_1m
            (time, exchange, symbol, open, high, low, close, volume, trade_count)
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    try:
        for symbol in gap_symbols:
            log.info("[%s] Binance klines 요청", symbol)

            try:
                raw = client.get_historical_klines(
                    symbol=symbol,
                    interval=Client.KLINE_INTERVAL_1MINUTE,
                    start_str=start_ms,
                    end_str=end_ms,
                )
            except Exception as e:
                log.exception("[%s] Binance API 호출 실패: %s", symbol, e)
                continue

            if not raw:
                log.warning("[%s] 수신 데이터 없음", symbol)
                continue

            rows = []
            for k in raw:
                open_time = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc)
                rows.append(
                    (
                        open_time,
                        EXCHANGE,
                        symbol,
                        float(k[1]),
                        float(k[2]),
                        float(k[3]),
                        float(k[4]),
                        float(k[5]),
                        int(k[8]),
                    )
                )

            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    insert_sql,
                    rows,
                    page_size=500,
                )
            conn.commit()

            log.info("[%s] %d rows upsert 완료", symbol, len(rows))

            # Binance REST rate limit 완화
            time.sleep(0.3)

    finally:
        conn.close()


# ────────────────────────────────────────
# Task 3: 결과 검증
# ────────────────────────────────────────

def verify_results(**context) -> None:
    ti = context["ti"]
    gap_symbols: list[str] = ti.xcom_pull(key="gap_symbols", task_ids="check_gaps")
    target_date: str = ti.xcom_pull(key="target_date", task_ids="check_gaps")

    if not gap_symbols:
        log.info("검증 대상 없음. 종료")
        return

    day_start = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    conn = _get_db_conn()
    try:
        with conn.cursor() as cur:
            for symbol in gap_symbols:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM ohlcv_1m
                    WHERE exchange = %s
                      AND symbol   = %s
                      AND time >= %s
                      AND time <  %s
                    """,
                    (EXCHANGE, symbol, day_start, day_end),
                )
                count = cur.fetchone()[0]

                if count < GAP_THRESHOLD:
                    log.warning("[%s] backfill 후에도 부족: %d rows", symbol, count)
                else:
                    log.info("[%s] backfill 정상 완료: %d rows", symbol, count)
    finally:
        conn.close()


with DAG(
    dag_id="backfill_dag",
    description="전날 1분봉 gap 확인 및 Binance REST API backfill",
    schedule="10 0 * * *",  # 매일 00:10 UTC
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["data-quality", "ohlcv", "backfill"],
) as dag:

    t1 = PythonOperator(
        task_id="check_gaps",
        python_callable=check_gaps,
    )

    t2 = PythonOperator(
        task_id="backfill_symbols",
        python_callable=backfill_symbols,
    )

    t3 = PythonOperator(
        task_id="verify_results",
        python_callable=verify_results,
    )

    t1 >> t2 >> t3