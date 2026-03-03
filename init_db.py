# init_db.py
import sys
import psycopg2

sys.path.append(".")
from config import (
    TIMESCALEDB_HOST, TIMESCALEDB_PORT,
    TIMESCALEDB_USER, TIMESCALEDB_PASSWORD, TIMESCALEDB_DB
)

def get_conn():
    return psycopg2.connect(
        host=TIMESCALEDB_HOST,
        port=TIMESCALEDB_PORT,
        user=TIMESCALEDB_USER,
        password=TIMESCALEDB_PASSWORD,
        dbname=TIMESCALEDB_DB
    )

def init():
    conn = get_conn()
    cur = conn.cursor()

    # TimescaleDB 확장 활성화
    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

    # ── ohlcv_1m ──────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv_1m (
            time         TIMESTAMPTZ NOT NULL,
            exchange     TEXT        NOT NULL,
            symbol       TEXT        NOT NULL,
            open         DOUBLE PRECISION,
            high         DOUBLE PRECISION,
            low          DOUBLE PRECISION,
            close        DOUBLE PRECISION,
            volume       DOUBLE PRECISION,
            trade_count  INTEGER
        );
    """)

    # hypertable 변환 (TimescaleDB 핵심 기능)
    # → time 기준으로 자동 파티셔닝, 시계열 쿼리 고속화
    cur.execute("""
        SELECT create_hypertable(
            'ohlcv_1m', 'time',
            if_not_exists => TRUE
        );
    """)

    # 복합 인덱스: 심볼별 시간 조회 최적화
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_time
        ON ohlcv_1m (symbol, time DESC);
    """)

    # ── anomaly_results ───────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS anomaly_results (
            time          TIMESTAMPTZ NOT NULL,
            exchange      TEXT        NOT NULL,
            symbol        TEXT        NOT NULL,
            anomaly_score DOUBLE PRECISION,
            is_anomaly    BOOLEAN,
            severity      TEXT,
            reason        TEXT,
            ohlcv_time    TIMESTAMPTZ
        );
    """)

    cur.execute("""
        SELECT create_hypertable(
            'anomaly_results', 'time',
            if_not_exists => TRUE
        );
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_anomaly_symbol_time
        ON anomaly_results (symbol, time DESC);
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ TimescaleDB 초기화 완료")
    print("   테이블: ohlcv_1m, anomaly_results")

if __name__ == "__main__":
    init()