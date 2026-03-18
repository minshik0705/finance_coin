# airflow/dags/retrain_dag.py
"""
매일 02:00 UTC에 실행.
ohlcv_1m 테이블에 있는 모든 심볼에 대해
IsolationForest 모델을 최신 데이터(7일치)로 재학습하고 저장.

흐름:
  1) get_symbols    : DB에서 현재 수집 중인 심볼 목록 조회
  2) retrain_models : 심볼별로 anomaly_detect.detect() 호출 (retrain=True)
  3) report         : 학습 완료된 모델 목록 및 학습 행수 로그 출력

설계 의도 (패턴 B):
  - anomaly_consumer.py: 실시간 탐지 전담 (저장된 모델 그대로 사용)
  - 이 DAG: 매일 새벽에 모델을 최신화
  - consumer가 다음 탐지 사이클에서 자동으로 새 모델을 로드
    (anomaly_detect.load_model()이 파일에서 읽으므로 재시작 불필요)
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

log = logging.getLogger(__name__)

# ────────────────────────────────────────
# 공통 헬퍼
# ────────────────────────────────────────

def _setup_sys_path() -> None:
    """
    anomaly_detect.py가 있는 프로젝트 루트를 sys.path에 추가.
    Airflow 컨테이너에서 프로젝트 코드를 import 하기 위해 필요.
    docker-compose.yaml에서 프로젝트 루트를 volume으로 마운트해야 함.
    예) - ./:/opt/airflow/project
    """
    project_root = Variable.get(
        "PROJECT_ROOT",
        default_var="/opt/airflow/project"
    )
    if project_root not in sys.path:
        sys.path.insert(0, project_root)


def _get_db_conn():
    import psycopg2
    return psycopg2.connect(
        host    =Variable.get("TIMESCALEDB_HOST",     default_var="timescaledb"),
        port    =Variable.get("TIMESCALEDB_PORT",     default_var="5432"),
        user    =Variable.get("TIMESCALEDB_USER",     default_var="tsdb"),
        password=Variable.get("TIMESCALEDB_PASSWORD", default_var="tsdb"),
        dbname  =Variable.get("TIMESCALEDB_DB",       default_var="crypto"),
    )


# ────────────────────────────────────────
# Task 1: 심볼 목록 조회
# ────────────────────────────────────────

def get_symbols(**context) -> None:
    """
    ohlcv_1m에 실제로 데이터가 있는 심볼만 대상으로 삼음.
    core_universe.json이 아닌 DB 기준으로 조회하는 이유:
    - dynamic universe 심볼도 자동으로 포함됨
    - 데이터 없는 심볼은 학습 불가하므로 사전 필터링
    """
    EXCHANGE = "binance"
    MIN_ROWS = 1440  # 최소 1일치

    conn = _get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, COUNT(*) as rows
                FROM ohlcv_1m
                WHERE exchange = %s
                GROUP BY symbol
                HAVING COUNT(*) >= %s
                ORDER BY symbol;
            """, (EXCHANGE, MIN_ROWS))
            results = cur.fetchall()
    finally:
        conn.close()

    symbols = [row[0] for row in results]
    log.info("학습 대상 심볼 (%d개): %s", len(symbols), symbols)
    for row in results:
        log.info("  %s: %d행", row[0], row[1])

    context["ti"].xcom_push(key="symbols", value=symbols)


# ────────────────────────────────────────
# Task 2: 모델 재학습
# ────────────────────────────────────────

def retrain_models(**context) -> None:
    """
    각 심볼에 대해 anomaly_detect.detect(retrain=True) 호출.

    detect(retrain=True)가 하는 일:
      1) DB에서 최근 7일치 데이터 로드
      2) 피처 계산 (logret, range, body 등 9개)
      3) IsolationForest 학습
      4) artifacts/{exchange}_{symbol}_isoforest.joblib 저장
      5) 최근 1시간 이상 결과 반환 → DB 저장

    anomaly_consumer.py는 다음 탐지 사이클에서
    load_model()로 새 파일을 자동 로드함 (재시작 불필요).
    """
    _setup_sys_path()

    # 여기서 import 하는 이유: sys.path 설정 후에 해야 함
    from anomaly_detect import detect, insert_anomaly_results

    ti      = context["ti"]
    symbols = ti.xcom_pull(key="symbols", task_ids="get_symbols")

    if not symbols:
        log.warning("학습할 심볼 없음")
        return

    EXCHANGE = "binance"
    success_list = []
    fail_list    = []

    conn = _get_db_conn()
    try:
        for symbol in symbols:
            log.info("[%s] 재학습 시작...", symbol)
            try:
                results = detect(
                    symbol=symbol,
                    exchange=EXCHANGE,
                    retrain=True,      # 강제 재학습
                )

                # 재학습 과정에서 탐지된 이상 결과도 DB에 저장
                if results:
                    insert_anomaly_results(conn, results)
                    log.info("[%s] 이상 %d건 저장", symbol, len(results))

                success_list.append(symbol)
                log.info("[%s] 재학습 완료 ✅", symbol)

            except Exception as e:
                fail_list.append(symbol)
                log.error("[%s] 재학습 실패: %s", symbol, e, exc_info=True)
    finally:
        conn.close()

    # XCom으로 결과 전달
    context["ti"].xcom_push(key="success_list", value=success_list)
    context["ti"].xcom_push(key="fail_list",    value=fail_list)

    log.info("재학습 완료: 성공 %d개, 실패 %d개", len(success_list), len(fail_list))

    # 실패가 있으면 task를 실패 처리 (Airflow 알림 트리거)
    if fail_list:
        raise RuntimeError(f"일부 심볼 재학습 실패: {fail_list}")


# ────────────────────────────────────────
# Task 3: 결과 리포트
# ────────────────────────────────────────

def report(**context) -> None:
    """
    재학습 결과를 로그로 출력.
    추후 Slack/이메일 알림 연동 시 여기에 추가.
    """
    _setup_sys_path()

    ti           = context["ti"]
    success_list = ti.xcom_pull(key="success_list", task_ids="retrain_models")
    fail_list    = ti.xcom_pull(key="fail_list",    task_ids="retrain_models")

    now = datetime.now().strftime("%Y-%m-%d %H:%M UTC")

    log.info("=" * 50)
    log.info("모델 재학습 리포트 (%s)", now)
    log.info("=" * 50)
    log.info("✅ 성공 (%d개): %s", len(success_list or []), success_list)
    log.info("❌ 실패 (%d개): %s", len(fail_list or []),    fail_list)

    # 저장된 모델 파일 목록 확인
    try:
        from anomaly_detect import MODEL_DIR
        model_files = list(MODEL_DIR.glob("*.joblib"))
        log.info("저장된 모델 파일 (%d개):", len(model_files))
        for f in sorted(model_files):
            import joblib
            saved   = joblib.load(f)
            meta    = saved.get("meta", {})
            trained = meta.get("trained_at", "unknown")
            rows    = meta.get("train_rows", "?")
            log.info("  %s | 학습행수: %s | 학습시각: %s", f.name, rows, trained)
    except Exception as e:
        log.warning("모델 파일 확인 실패: %s", e)

    log.info("=" * 50)


# ────────────────────────────────────────
# DAG 정의
# ────────────────────────────────────────

with DAG(
    dag_id="retrain_dag",
    description="매일 새벽 심볼별 IsolationForest 모델 재학습",
    schedule_interval="0 2 * * *",    # 매일 02:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["ml", "anomaly-detection"],
) as dag:

    t1 = PythonOperator(
        task_id="get_symbols",
        python_callable=get_symbols,
    )

    t2 = PythonOperator(
        task_id="retrain_models",
        python_callable=retrain_models,
    )

    t3 = PythonOperator(
        task_id="report",
        python_callable=report,
        trigger_rule="all_done",   # retrain 실패해도 리포트는 항상 실행
    )

    t1 >> t2 >> t3