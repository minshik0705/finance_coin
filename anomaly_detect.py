# anomaly_detect.py
from __future__ import annotations
from sqlalchemy import create_engine, text

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

# 학습에 사용할 과거 데이터 기간
TRAIN_DAYS = 7

# 이상 탐지 임계값 (하위 0.5% → 이상)
ANOMALY_QUANTILE = 0.005

# 강한 이상 임계값 (하위 0.1%)
STRONG_QUANTILE = 0.001

# 모델 저장 경로
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

def get_engine():
    url = (
        f"postgresql+psycopg2://{TIMESCALEDB_USER}:{TIMESCALEDB_PASSWORD}"
        f"@{TIMESCALEDB_HOST}:{TIMESCALEDB_PORT}/{TIMESCALEDB_DB}"
    )
    return create_engine(url)

def load_ohlcv(conn, symbol: str, exchange: str, days: int) -> pd.DataFrame:
    since = datetime.now(tz=timezone.utc) - timedelta(days=days)

    sql = text("""
        SELECT time, open, high, low, close, volume, trade_count
        FROM ohlcv_1m
        WHERE symbol   = :symbol
          AND exchange = :exchange
          AND time     >= :since
        ORDER BY time ASC;
    """)

    engine = get_engine()
    try:
        with engine.connect() as eng_conn:
            df = pd.read_sql(sql, eng_conn, params={
                "symbol": symbol,
                "exchange": exchange,
                "since": since,
            })
    finally:
        engine.dispose()

    if df.empty:
        return df

    df["time"] = pd.to_datetime(df["time"], utc=True)
    return df


def insert_anomaly_results(conn, rows: list[dict]) -> None:
    """anomaly_results 테이블에 배치 insert."""
    if not rows:
        return

    sql = """
        INSERT INTO anomaly_results
            (time, exchange, symbol, anomaly_score,
             is_anomaly, severity, reason, ohlcv_time)
        VALUES %s
        ON CONFLICT DO NOTHING;
    """

    values = [
        (
            r["time"], r["exchange"], r["symbol"],
            r["anomaly_score"], r["is_anomaly"],
            r["severity"], r["reason"], r["ohlcv_time"],
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values)
    conn.commit()


# ────────────────────────────────────────
# 피처 엔지니어링 (기존 코드 재활용)
# ────────────────────────────────────────

FEATURE_COLS = [
    "logret", "range", "body", "upper_wick", "lower_wick",
    "dlogvol", "vol_1h", "vol_1d", "vol_z_1d",
]


def build_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    기존 anomaly_detect.py의 build_features() 그대로 재활용.
    입력: TimescaleDB에서 읽은 ohlcv DataFrame
    출력: (피처가 추가된 df, 피처만 담긴 X)
    """
    df = df.copy()
    df = df.sort_values("time").reset_index(drop=True)

    open_ = df["open"]
    high  = df["high"]
    low   = df["low"]
    close = df["close"]
    vol   = df["volume"]

    # 기존 피처 그대로
    df["logret"] = np.log(close).diff()

    denom = close.replace(0, np.nan)
    df["range"]       = (high - low) / denom
    df["body"]        = (close - open_) / denom
    df["upper_wick"]  = (high - np.maximum(open_, close)) / denom
    df["lower_wick"]  = (np.minimum(open_, close) - low) / denom

    df["logvol"]  = np.log1p(vol)
    df["dlogvol"] = df["logvol"].diff()

    # 1분봉 기준: 60=1h, 1440=1d
    df["vol_1h"] = df["logret"].rolling(60,   min_periods=30).std()
    df["vol_1d"] = df["logret"].rolling(1440, min_periods=60).std()

    vmean = vol.rolling(1440, min_periods=60).mean()
    vstd  = vol.rolling(1440, min_periods=60).std()
    df["vol_z_1d"] = (vol - vmean) / vstd.replace(0, np.nan)

    X = df[FEATURE_COLS].replace([np.inf, -np.inf], np.nan).dropna()
    df_model = df.loc[X.index].copy()

    return df_model, X


# ────────────────────────────────────────
# 이상 원인 해석 (기존 코드 재활용)
# ────────────────────────────────────────

def interpret_row(row, shap_values: np.ndarray) -> str:
    """
    SHAP 기반 이상 원인 해석.
    shap_values: 해당 행의 SHAP value 배열 (FEATURE_COLS 순서)
    """
    contributions = sorted(
        zip(FEATURE_COLS, shap_values),
        key=lambda x: abs(x[1]),
        reverse=True
    )

    # 기여도 상위 3개 피처만 표시
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
    """
    IsolationForest + RobustScaler Pipeline 학습.
    기존 코드와 동일한 구조.
    """
    model = Pipeline(steps=[
        ("scaler", RobustScaler()),
        ("iso", IsolationForest(
            n_estimators=200,      # 기존 400 → 200 (실시간 속도 고려)
            contamination=contamination,
            max_samples="auto",
            random_state=42,
            n_jobs=-1,
        ))
    ])
    model.fit(X)
    return model


def save_model(model: Pipeline, symbol: str, exchange: str, meta: dict) -> Path:
    """학습된 모델 저장."""
    path = MODEL_DIR / f"{exchange}_{symbol}_isoforest.joblib"
    joblib.dump({"model": model, "meta": meta}, path)
    print(f"[INFO] 모델 저장: {path}")
    return path


def load_model(symbol: str, exchange: str) -> dict | None:
    """저장된 모델 로드. 없으면 None."""
    path = MODEL_DIR / f"{exchange}_{symbol}_isoforest.joblib"
    if not path.exists():
        return None
    return joblib.load(path)


# ────────────────────────────────────────
# 탐지 (핵심 함수)
# ────────────────────────────────────────

def detect(
    symbol: str,
    exchange: str = "binance",
    retrain: bool = False,
) -> list[dict]:
    """
    특정 심볼에 대해 이상탐지 실행.

    흐름:
    1) DB에서 최근 TRAIN_DAYS일 데이터 로드
    2) 피처 계산
    3) 모델 없거나 retrain=True → 학습
    4) 점수 계산 → 이상 여부 판단
    5) 결과 리스트 반환

    반환: anomaly_results 테이블에 넣을 row 리스트
    """
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

        # 모델 로드 or 학습
        saved = load_model(symbol, exchange)

        if saved is None or retrain:
            print(f"[INFO] {exchange} {symbol}: 모델 학습 중... ({len(X)}행)")
            model = train_model(X)

            meta = {
                "symbol":       symbol,
                "exchange":     exchange,
                "feature_cols": FEATURE_COLS,
                "train_rows":   len(X),
                "trained_at":   datetime.now(tz=timezone.utc).isoformat(),
                "threshold_overall": float(
                    np.quantile(model.decision_function(X), ANOMALY_QUANTILE)
                ),
                "threshold_strong": float(
                    np.quantile(model.decision_function(X), STRONG_QUANTILE)
                ),
            }
            save_model(model, symbol, exchange, meta)
        else:
            model = saved["model"]
            meta  = saved["meta"]
            print(f"[INFO] {exchange} {symbol}: 저장된 모델 로드")

        # 점수 계산 (전체 X로 계산)
        scores = model.decision_function(X)
        df_model = df_model.copy()
        df_model.loc[X.index, "anomaly_score"] = scores

        # ── 탐지 범위: 최근 1시간치만 ──────────────
        # 학습은 7일치로 하되, 결과 생성은 최근 데이터만
        detect_since = datetime.now(tz=timezone.utc) - timedelta(hours=1)
        df_model = df_model[df_model["time"] >= detect_since]
        
        # SHAP 계산 (이상 행에만 적용)
        scaler = model.named_steps["scaler"]
        iso    = model.named_steps["iso"]
        X_scaled = scaler.transform(X)

        explainer   = shap.TreeExplainer(iso)
        shap_matrix = explainer.shap_values(X_scaled)  # shape: (n, 9)

        # shap_matrix를 index 기준으로 매핑
        shap_df = pd.DataFrame(shap_matrix, index=X.index, columns=FEATURE_COLS)

        thr_overall = meta["threshold_overall"]
        thr_strong  = meta["threshold_strong"]

        # 결과 생성 (이상인 행만)
        now = datetime.now(tz=timezone.utc)
        results = []

        for idx, row in df_model.iterrows():
            score = row.get("anomaly_score")
            if pd.isna(score):
                continue

            is_anomaly = score <= thr_overall
            severity   = "strong" if score <= thr_strong else "normal"

            if is_anomaly:
                shap_vals = shap_df.loc[idx].values  # 해당 행 SHAP values
                results.append({
                    "time":          now,
                    "exchange":      exchange,
                    "symbol":        symbol,
                    "anomaly_score": float(score),
                    "is_anomaly":    True,
                    "severity":      severity,
                    "reason":        interpret_row(row, shap_vals),
                    "ohlcv_time":    row["time"],
                })

        print(f"[INFO] {exchange} {symbol}: "
              f"총 {len(df_model)}행 중 이상 {len(results)}건 탐지")

        return results

    finally:
        conn.close()


def run_all(exchange: str = "binance", retrain: bool = False) -> None:
    """
    DB에 있는 모든 심볼에 대해 탐지 실행.
    anomaly_consumer.py에서 주기적으로 호출.
    """
    conn = get_conn()

    # DB에 있는 심볼 목록 조회
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
            results = detect(symbol, exchange, retrain=retrain)
            if results:
                insert_anomaly_results(conn, results)
    finally:
        conn.close()


# ────────────────────────────────────────
# 단독 실행 (테스트용)
# ────────────────────────────────────────

if __name__ == "__main__":
    run_all(exchange="binance", retrain=True)