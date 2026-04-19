# anomaly_detect.py
from __future__ import annotations

import sys
import shap
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent))

import numpy as np
import pandas as pd
import joblib
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone, timedelta
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler
from sklearn.pipeline import Pipeline

from config import (
    TIMESCALEDB_HOST, TIMESCALEDB_PORT,
    TIMESCALEDB_USER, TIMESCALEDB_PASSWORD, TIMESCALEDB_DB,
)

# ────────────────────────────────────────
# 설정
# ────────────────────────────────────────

TRAIN_DAYS = 7
ANOMALY_QUANTILE = 0.005
STRONG_QUANTILE = 0.001

MODEL_DIR = Path(__file__).resolve().parent / "artifacts"
MODEL_DIR.mkdir(exist_ok=True)


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


def load_ohlcv(conn, symbol: str, exchange: str, days: int) -> pd.DataFrame:
    """
    순수 psycopg2로 ohlcv_1m 조회.
    SQLAlchemy 의존성 제거 → Airflow 컨테이너 호환성 확보.
    """
    since = datetime.now(tz=timezone.utc) - timedelta(days=days)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT time, open, high, low, close, volume, trade_count
            FROM ohlcv_1m
            WHERE symbol   = %s
              AND exchange = %s
              AND time     >= %s
            ORDER BY time ASC
        """, (symbol, exchange, since))
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

    df = pd.DataFrame(rows, columns=colnames)
    if not df.empty:
        df["time"] = pd.to_datetime(df["time"], utc=True)
    return df


def insert_anomaly_results(conn, rows: list[dict]) -> None:
    """anomaly_results 테이블에 배치 insert."""
    if not rows:
        return

    sql = """
        INSERT INTO anomaly_results
            (time, exchange, symbol, anomaly_score,
             is_anomaly, severity, reason, ohlcv_time, detected_at)
        VALUES %s
        ON CONFLICT (time, exchange, symbol) DO NOTHING;
    """

    values = [
        (
            r["time"], r["exchange"], r["symbol"],
            r["anomaly_score"], r["is_anomaly"],
            r["severity"], r["reason"], r["ohlcv_time"], r["detected_at"],
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values)
    conn.commit()


# ────────────────────────────────────────
# 피처 엔지니어링
# ────────────────────────────────────────

FEATURE_COLS = [
    "logret", "range", "body", "upper_wick", "lower_wick",
    "dlogvol", "vol_1h", "vol_1d", "vol_z_1d",
]


def build_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    df = df.sort_values("time").reset_index(drop=True)

    open_ = df["open"]
    high  = df["high"]
    low   = df["low"]
    close = df["close"]
    vol   = df["volume"]

    df["logret"] = np.log(close).diff()

    denom = close.replace(0, np.nan)
    df["range"]       = (high - low) / denom
    df["body"]        = (close - open_) / denom
    df["upper_wick"]  = (high - np.maximum(open_, close)) / denom
    df["lower_wick"]  = (np.minimum(open_, close) - low) / denom

    df["logvol"]  = np.log1p(vol)
    df["dlogvol"] = df["logvol"].diff()

    df["vol_1h"] = df["logret"].rolling(60,   min_periods=30).std()
    df["vol_1d"] = df["logret"].rolling(1440, min_periods=60).std()

    vmean = vol.rolling(1440, min_periods=60).mean()
    vstd  = vol.rolling(1440, min_periods=60).std()
    df["vol_z_1d"] = (vol - vmean) / vstd.replace(0, np.nan)

    X = df[FEATURE_COLS].replace([np.inf, -np.inf], np.nan).dropna()
    df_model = df.loc[X.index].copy()

    return df_model, X


# ────────────────────────────────────────
# 이상 원인 해석
# ────────────────────────────────────────

def interpret_row(row, shap_values: np.ndarray) -> str:
    contributions = sorted(
        zip(FEATURE_COLS, shap_values),
        key=lambda x: abs(x[1]),
        reverse=True
    )

    top = [(f, v) for f, v in contributions[:3] if abs(v) > 0.001]

    if not top:
        return "Unknown"

    parts = []
    for feature, value in top:
        direction = "↑" if value > 0 else "↓"
        parts.append(f"{feature}{direction}({value:+.3f})")

    return ", ".join(parts)


# ────────────────────────────────────────
# 모델 학습
# ────────────────────────────────────────

def train_model(X: pd.DataFrame, contamination: float = 0.003) -> Pipeline:
    model = Pipeline(steps=[
        ("scaler", RobustScaler()),
        ("iso", IsolationForest(
            n_estimators=200,
            contamination=contamination,
            max_samples="auto",
            random_state=42,
            n_jobs=-1,
        ))
    ])
    model.fit(X)
    return model


def save_model(model: Pipeline, symbol: str, exchange: str, meta: dict) -> Path:
    path = MODEL_DIR / f"{exchange}_{symbol}_isoforest.joblib"
    joblib.dump({"model": model, "meta": meta}, path)
    print(f"[INFO] 모델 저장: {path}")
    return path


def load_model(symbol: str, exchange: str) -> dict | None:
    path = MODEL_DIR / f"{exchange}_{symbol}_isoforest.joblib"
    if not path.exists():
        return None
    try:
        return joblib.load(path)
    except Exception as e:
        print(f"[WARN] {exchange} {symbol}: model load failed, retrain fallback: {e}")
        try:
            bad_path = path.with_suffix(path.suffix + ".bad")
            path.rename(bad_path)
        except Exception:
            pass
        return None

# ────────────────────────────────────────
# 탐지
# ────────────────────────────────────────

def detect(
    symbol: str,
    exchange: str = "binance",
    retrain: bool = False,
) -> list[dict]:
    conn = get_conn()

    try:
        df = load_ohlcv(conn, symbol, exchange, days=TRAIN_DAYS)

        if df.empty:
            print(f"[WARN] {exchange} {symbol}: 데이터 없음, 건너뜀")
            return []

        if len(df) < 1440:
            print(f"[WARN] {exchange} {symbol}: "
                  f"데이터 부족 ({len(df)}행, 최소 1440 필요)")
            return []

        df_model, X = build_features(df)

        saved = load_model(symbol, exchange)

        if saved is None or retrain:
            print(f"[INFO] {exchange} {symbol}: 모델 학습 중... ({len(X)}행)")
            model = train_model(X)

            meta = {
                "symbol":            symbol,
                "exchange":          exchange,
                "feature_cols":      FEATURE_COLS,
                "train_rows":        len(X),
                "trained_at":        datetime.now(tz=timezone.utc).isoformat(),
                "threshold_overall": float(
                    np.quantile(model.decision_function(X), ANOMALY_QUANTILE)
                ),
                "threshold_strong":  float(
                    np.quantile(model.decision_function(X), STRONG_QUANTILE)
                ),
            }
            save_model(model, symbol, exchange, meta)
        else:
            model = saved["model"]
            meta  = saved["meta"]
            print(f"[INFO] {exchange} {symbol}: 저장된 모델 로드")

        scores = model.decision_function(X)
        df_model = df_model.copy()
        df_model.loc[X.index, "anomaly_score"] = scores

        detect_since = datetime.now(tz=timezone.utc) - timedelta(hours=1)
        df_model = df_model[df_model["time"] >= detect_since]

        scaler     = model.named_steps["scaler"]
        iso        = model.named_steps["iso"]
        X_scaled   = scaler.transform(X)
        explainer   = shap.TreeExplainer(iso)
        shap_matrix = explainer.shap_values(X_scaled)

        shap_df = pd.DataFrame(shap_matrix, index=X.index, columns=FEATURE_COLS)

        thr_overall = meta["threshold_overall"]
        thr_strong  = meta["threshold_strong"]

        now = datetime.now(tz=timezone.utc)
        results = []

        for idx, row in df_model.iterrows():
            score = row.get("anomaly_score")
            if pd.isna(score):
                continue

            is_anomaly = score <= thr_overall
            severity   = "strong" if score <= thr_strong else "normal"

            if is_anomaly:
                shap_vals = shap_df.loc[idx].values
                results.append({
                    "time":          row["time"],   # 캔들 시각 기준
                    "exchange":      exchange,
                    "symbol":        symbol,
                    "anomaly_score": float(score),
                    "is_anomaly":    True,
                    "severity":      severity,
                    "reason":        interpret_row(row, shap_vals),
                    "ohlcv_time":    row["time"],
                    "detected_at":   now,
                })

        print(f"[INFO] {exchange} {symbol}: "
              f"총 {len(df_model)}행 중 이상 {len(results)}건 탐지")

        return results

    finally:
        conn.close()

def run_all(exchange: str = "binance", retrain: bool = False) -> None:
    conn = get_conn()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT symbol
            FROM ohlcv_1m
            WHERE exchange = %s
            ORDER BY symbol;
        """, (exchange,))
        symbols = [row[0] for row in cur.fetchall()]
    conn.close()

    if not symbols:
        print(f"[WARN] {exchange}: 탐지할 심볼 없음")
        return

    print(f"[INFO] 탐지 시작: {exchange} {len(symbols)}개 심볼")

    conn = get_conn()
    try:
        for symbol in symbols:
            try:
                results = detect(symbol, exchange, retrain=retrain)
                if results:
                    insert_anomaly_results(conn, results)
            except Exception as e:
                print(f"[ERROR] {exchange} {symbol} 탐지 실패: {e}")
    finally:
        conn.close()

# ────────────────────────────────────────
# 단독 실행 (테스트용)
# ────────────────────────────────────────

if __name__ == "__main__":
    run_all(exchange="binance", retrain=True)
