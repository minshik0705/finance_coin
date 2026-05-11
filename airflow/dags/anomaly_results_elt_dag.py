from __future__ import annotations

import json
import logging
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)


# ────────────────────────────────────────
# 공통 헬퍼
# ────────────────────────────────────────

def _get_export_root() -> Path:
    return Path(
        Variable.get(
            "ANOMALY_EXPORT_ROOT",
            default_var="/tmp/argus_export/anomaly_results",
        )
    )


def _get_exchanges() -> list[str]:
    raw = Variable.get(
        "ANOMALY_EXPORT_EXCHANGES",
        default_var="binance,bybit,okx",
    )
    return [x.strip() for x in raw.split(",") if x.strip()]


def _get_s3_bucket() -> str:
    return Variable.get(
        "ANOMALY_S3_BUCKET",
        default_var="argus-lake-753101492795-ap-northeast-2-an",
    )


def _get_s3_prefix() -> str:
    return Variable.get(
        "ANOMALY_S3_PREFIX",
        default_var="curated/anomaly_results",
    ).strip("/")


def _get_athena_database() -> str:
    return Variable.get("ATHENA_DATABASE", default_var="coinview_lake")


def _get_athena_table() -> str:
    return Variable.get("ATHENA_ANOMALY_TABLE", default_var="anomaly_results")


def _get_athena_output_location() -> str:
    return Variable.get(
        "ATHENA_OUTPUT_LOCATION",
        default_var="s3://argus-lake-753101492795-ap-northeast-2-an/athena-results/",
    )


def _get_aws_region() -> str:
    return Variable.get("AWS_REGION", default_var="ap-northeast-2")


def _get_athena_workgroup() -> str:
    return Variable.get("ATHENA_WORKGROUP", default_var="primary")


def _get_target_day(context: dict[str, Any]) -> dict[str, str]:
    # 수동 실행 / 스케줄 실행 모두 동일하게 현재 UTC 기준 전날을 export
    target_date = datetime.now(tz=timezone.utc).date() - timedelta(days=1)
    target_date_str = target_date.isoformat()

    return {
        "target_date": target_date_str,
        "start_date": target_date_str,
        "end_date": target_date_str,
    }


def _target_local_partition_dir(target_date: str) -> Path:
    return _get_export_root() / f"event_dt={target_date}"


def _target_s3_partition_prefix(target_date: str) -> str:
    return f"{_get_s3_prefix()}/event_dt={target_date}"


def _get_db_conn():
    import psycopg2

    return psycopg2.connect(
        host=Variable.get("TIMESCALEDB_HOST", default_var="timescaledb"),
        port=Variable.get("TIMESCALEDB_PORT", default_var="5432"),
        user=Variable.get("TIMESCALEDB_USER", default_var="tsdb"),
        password=Variable.get("TIMESCALEDB_PASSWORD", default_var="tsdb"),
        dbname=Variable.get("TIMESCALEDB_DB", default_var="crypto"),
    )


def _get_boto3_client(service_name: str):
    try:
        import boto3
    except ImportError as e:
        raise RuntimeError(
            "boto3가 Airflow 이미지에 없습니다. requirements.txt 또는 Airflow 이미지 구성을 확인하세요."
        ) from e

    return boto3.client(service_name, region_name=_get_aws_region())


def _run_athena_query(query: str, database: str | None = None) -> str:
    athena = _get_boto3_client("athena")

    params = {
        "QueryString": query,
        "ResultConfiguration": {
            "OutputLocation": _get_athena_output_location(),
        },
        "WorkGroup": _get_athena_workgroup(),
    }

    if database:
        params["QueryExecutionContext"] = {"Database": database}

    response = athena.start_query_execution(**params)
    query_execution_id = response["QueryExecutionId"]
    log.info("Athena query started: %s", query_execution_id)

    timeout_seconds = int(Variable.get("ATHENA_QUERY_TIMEOUT_SECONDS", default_var="600"))
    poll_interval = int(Variable.get("ATHENA_QUERY_POLL_SECONDS", default_var="5"))

    started_at = time.time()

    while True:
        execution = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = execution["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            return query_execution_id

        if status in {"FAILED", "CANCELLED"}:
            reason = execution["QueryExecution"]["Status"].get(
                "StateChangeReason",
                "unknown reason",
            )
            raise RuntimeError(f"Athena query {status}: {reason}")

        if time.time() - started_at > timeout_seconds:
            raise TimeoutError(
                f"Athena query timeout after {timeout_seconds}s: {query_execution_id}"
            )

        time.sleep(poll_interval)


def _fetch_athena_rows(query_execution_id: str) -> list[dict[str, str]]:
    athena = _get_boto3_client("athena")

    columns: list[str] = []
    rows: list[dict[str, str]] = []
    next_token: str | None = None

    while True:
        params = {"QueryExecutionId": query_execution_id}

        if next_token:
            params["NextToken"] = next_token

        resp = athena.get_query_results(**params)
        result_rows = resp.get("ResultSet", {}).get("Rows", [])

        if result_rows:
            if not columns:
                columns = [
                    c.get("VarCharValue", "")
                    for c in result_rows[0].get("Data", [])
                ]
                data_rows = result_rows[1:]
            else:
                data_rows = result_rows

            for row in data_rows:
                data = row.get("Data", [])
                values = [cell.get("VarCharValue", "") for cell in data]
                padded = values + [""] * (len(columns) - len(values))
                rows.append(dict(zip(columns, padded)))

        next_token = resp.get("NextToken")
        if not next_token:
            break

    return rows


# ────────────────────────────────────────
# Task 1: 대상 날짜 계산
# ────────────────────────────────────────

def prepare_context(**context) -> None:
    payload = _get_target_day(context)
    target_date = payload["target_date"]

    log.info("anomaly_results ELT 대상 날짜(UTC 전날): %s", target_date)
    log.info("로컬 export 루트: %s", _get_export_root())
    log.info("S3 prefix: s3://%s/%s", _get_s3_bucket(), _get_s3_prefix())
    log.info("대상 거래소: %s", _get_exchanges())

    for key, value in payload.items():
        context["ti"].xcom_push(key=key, value=value)


# ────────────────────────────────────────
# Task 2: anomaly_results parquet export
# ────────────────────────────────────────

def export_anomaly_results_parquet(**context) -> None:
    try:
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as e:
        raise RuntimeError(
            "pandas 또는 pyarrow가 Airflow 이미지에 없습니다. requirements.txt / Docker image를 확인하세요."
        ) from e

    ti = context["ti"]
    target_date: str = ti.xcom_pull(key="target_date", task_ids="prepare_context")

    day_start = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    partition_dir = _target_local_partition_dir(target_date)

    if partition_dir.exists():
        log.info("기존 로컬 partition 삭제: %s", partition_dir)
        shutil.rmtree(partition_dir)

    partition_dir.mkdir(parents=True, exist_ok=True)

    schema = pa.schema([
        pa.field("time", pa.int64()),
        pa.field("symbol", pa.string()),
        pa.field("anomaly_score", pa.float64()),
        pa.field("is_anomaly", pa.bool_()),
        pa.field("severity", pa.string()),
        pa.field("reason", pa.string()),
        pa.field("ohlcv_time", pa.int64()),
        pa.field("detected_at", pa.int64()),
    ])

    columns = [
        "time",
        "symbol",
        "anomaly_score",
        "is_anomaly",
        "severity",
        "reason",
        "ohlcv_time",
        "detected_at",
    ]

    local_counts: dict[str, int] = {}

    conn = _get_db_conn()

    try:
        for exchange in _get_exchanges():
            exchange_dir = partition_dir / f"exchange={exchange}"
            exchange_dir.mkdir(parents=True, exist_ok=True)

            query = """
                SELECT
                    time,
                    symbol,
                    anomaly_score,
                    is_anomaly,
                    severity,
                    reason,
                    ohlcv_time,
                    detected_at
                FROM anomaly_results
                WHERE exchange = %s
                  AND time >= %s
                  AND time <  %s
                ORDER BY time ASC, symbol ASC
            """

            with conn.cursor() as cur:
                cur.execute(query, (exchange, day_start, day_end))
                rows = cur.fetchall()
                colnames = [desc[0] for desc in cur.description]

            df = pd.DataFrame(rows, columns=colnames)

            if df.empty:
                df = pd.DataFrame({
                    "time": pd.Series(dtype="int64"),
                    "symbol": pd.Series(dtype="string"),
                    "anomaly_score": pd.Series(dtype="float64"),
                    "is_anomaly": pd.Series(dtype="bool"),
                    "severity": pd.Series(dtype="string"),
                    "reason": pd.Series(dtype="string"),
                    "ohlcv_time": pd.Series(dtype="int64"),
                    "detected_at": pd.Series(dtype="int64"),
                })
            else:
                # Athena 기존 anomaly_results 스키마가 bigint ns 기준이므로 timestamp → epoch ns로 변환
                for col in ["time", "ohlcv_time", "detected_at"]:
                    df[col] = pd.to_datetime(df[col], utc=True).astype("int64")

                df["symbol"] = df["symbol"].astype("string")
                df["anomaly_score"] = df["anomaly_score"].astype("float64")
                df["is_anomaly"] = df["is_anomaly"].astype("bool")
                df["severity"] = df["severity"].fillna("").astype("string")
                df["reason"] = df["reason"].fillna("").astype("string")

                df = df[columns]

            parquet_path = exchange_dir / "anomalies.parquet"
            table = pa.Table.from_pandas(
                df[columns],
                schema=schema,
                preserve_index=False,
            )
            pq.write_table(table, parquet_path, compression="snappy")

            local_counts[exchange] = int(len(df))
            log.info(
                "[%s] anomaly_results parquet export 완료: %s rows -> %s",
                exchange,
                len(df),
                parquet_path,
            )

    finally:
        conn.close()

    total_rows = sum(local_counts.values())

    log.info("로컬 parquet row counts: %s", local_counts)
    log.info("총 anomaly row 수: %d", total_rows)

    ti.xcom_push(key="local_counts", value=local_counts)
    ti.xcom_push(key="local_total_rows", value=total_rows)
    ti.xcom_push(key="local_partition_dir", value=str(partition_dir))


# ────────────────────────────────────────
# Task 3: 기존 S3 partition 정리
# ────────────────────────────────────────

def purge_s3_partition(**context) -> None:
    ti = context["ti"]
    target_date: str = ti.xcom_pull(key="target_date", task_ids="prepare_context")

    bucket = _get_s3_bucket()
    prefix = _target_s3_partition_prefix(target_date)
    s3 = _get_boto3_client("s3")

    paginator = s3.get_paginator("list_objects_v2")
    to_delete: list[dict[str, str]] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            to_delete.append({"Key": obj["Key"]})

    if not to_delete:
        log.info("기존 S3 partition 없음: s3://%s/%s", bucket, prefix)
        return

    log.info("기존 S3 object %d개 삭제 시작: s3://%s/%s", len(to_delete), bucket, prefix)

    for i in range(0, len(to_delete), 1000):
        batch = to_delete[i : i + 1000]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})

    log.info("기존 S3 partition 삭제 완료")


# ────────────────────────────────────────
# Task 4: S3 업로드
# ────────────────────────────────────────

def upload_to_s3(**context) -> None:
    ti = context["ti"]

    target_date: str = ti.xcom_pull(key="target_date", task_ids="prepare_context")
    local_partition_dir = Path(
        ti.xcom_pull(
            key="local_partition_dir",
            task_ids="export_anomaly_results_parquet",
        )
    )

    if not local_partition_dir.exists():
        raise FileNotFoundError(f"업로드할 로컬 디렉터리가 없습니다: {local_partition_dir}")

    bucket = _get_s3_bucket()
    export_root = _get_export_root()
    s3 = _get_boto3_client("s3")

    uploaded_keys: list[str] = []

    for file_path in sorted(local_partition_dir.rglob("*")):
        if not file_path.is_file():
            continue

        relative_key = file_path.relative_to(export_root).as_posix()
        s3_key = f"{_get_s3_prefix()}/{relative_key}"

        s3.upload_file(str(file_path), bucket, s3_key)
        uploaded_keys.append(s3_key)

        log.info("업로드 완료: %s -> s3://%s/%s", file_path, bucket, s3_key)

    if not uploaded_keys:
        raise RuntimeError(f"업로드된 파일이 없습니다: {local_partition_dir}")

    ti.xcom_push(key="uploaded_keys", value=uploaded_keys)
    ti.xcom_push(key="uploaded_prefix", value=_target_s3_partition_prefix(target_date))


# ────────────────────────────────────────
# Task 5: Athena partition 반영
# ────────────────────────────────────────

def repair_athena_partitions(**context) -> None:
    database = _get_athena_database()
    table = _get_athena_table()

    query = f"MSCK REPAIR TABLE {database}.{table};"
    query_execution_id = _run_athena_query(query=query, database=database)

    log.info("MSCK REPAIR TABLE 성공: %s", query_execution_id)
    context["ti"].xcom_push(key="repair_query_execution_id", value=query_execution_id)


# ────────────────────────────────────────
# Task 6: Athena 검증
# ────────────────────────────────────────

def verify_athena_counts(**context) -> None:
    ti = context["ti"]

    target_date: str = ti.xcom_pull(key="target_date", task_ids="prepare_context")
    local_counts: dict[str, int] = ti.xcom_pull(
        key="local_counts",
        task_ids="export_anomaly_results_parquet",
    ) or {}

    database = _get_athena_database()
    table = _get_athena_table()

    query = f"""
    SELECT
      "exchange" AS exchange_name,
      CAST(COUNT(*) AS BIGINT) AS row_count,
      CAST(MIN(time) AS VARCHAR) AS min_time,
      CAST(MAX(time) AS VARCHAR) AS max_time
    FROM {database}.{table}
    WHERE event_dt = '{target_date}'
    GROUP BY 1
    ORDER BY 1
    """.strip()

    query_execution_id = _run_athena_query(query=query, database=database)
    rows = _fetch_athena_rows(query_execution_id)

    athena_counts: dict[str, int] = {}
    min_times: dict[str, str] = {}
    max_times: dict[str, str] = {}

    for row in rows:
        exchange = row.get("exchange_name") or row.get("exchange") or ""
        count_str = row.get("row_count", "0")

        athena_counts[exchange] = int(count_str)
        min_times[exchange] = row.get("min_time", "")
        max_times[exchange] = row.get("max_time", "")

    log.info("Athena row counts: %s", athena_counts)
    log.info("Athena min times: %s", min_times)
    log.info("Athena max times: %s", max_times)

    # anomaly는 0건이 정상일 수 있으므로 missing exchange는 0건으로 간주
    mismatched = {
        exchange: {
            "local": local_counts.get(exchange, 0),
            "athena": athena_counts.get(exchange, 0),
        }
        for exchange in local_counts
        if athena_counts.get(exchange, 0) != local_counts.get(exchange, 0)
    }

    if mismatched:
        raise RuntimeError(
            f"Athena row count 불일치: {json.dumps(mismatched, ensure_ascii=False)}"
        )

    ti.xcom_push(key="athena_counts", value=athena_counts)
    ti.xcom_push(key="athena_min_times", value=min_times)
    ti.xcom_push(key="athena_max_times", value=max_times)


# ────────────────────────────────────────
# Task 7: 결과 리포트
# ────────────────────────────────────────

def report(**context) -> None:
    ti = context["ti"]

    target_date = ti.xcom_pull(key="target_date", task_ids="prepare_context")
    local_counts = ti.xcom_pull(
        key="local_counts",
        task_ids="export_anomaly_results_parquet",
    )
    athena_counts = ti.xcom_pull(
        key="athena_counts",
        task_ids="verify_athena_counts",
    )
    min_times = ti.xcom_pull(
        key="athena_min_times",
        task_ids="verify_athena_counts",
    )
    max_times = ti.xcom_pull(
        key="athena_max_times",
        task_ids="verify_athena_counts",
    )
    uploaded_keys = ti.xcom_pull(
        key="uploaded_keys",
        task_ids="upload_to_s3",
    )

    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    log.info("=" * 60)
    log.info("anomaly_results ELT 리포트 (%s)", now)
    log.info("=" * 60)
    log.info("대상 날짜: %s", target_date)
    log.info("업로드 파일 수: %d", len(uploaded_keys or []))
    log.info("로컬 parquet row counts: %s", local_counts or {})
    log.info("Athena row counts: %s", athena_counts or {})
    log.info("Athena min times: %s", min_times or {})
    log.info("Athena max times: %s", max_times or {})
    log.info("=" * 60)


with DAG(
    dag_id="anomaly_results_elt_dag",
    description="전날 UTC 기준 anomaly_results parquet export -> S3 업로드 -> Athena partition 반영",
    schedule="55 0 * * *",  # ohlcv_1m_elt_dag(00:40 UTC) 이후 실행
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["elt", "anomaly", "athena", "s3"],
) as dag:

    t1 = PythonOperator(
        task_id="prepare_context",
        python_callable=prepare_context,
    )

    t2 = PythonOperator(
        task_id="export_anomaly_results_parquet",
        python_callable=export_anomaly_results_parquet,
    )

    t3 = PythonOperator(
        task_id="purge_s3_partition",
        python_callable=purge_s3_partition,
    )

    t4 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    t5 = PythonOperator(
        task_id="repair_athena_partitions",
        python_callable=repair_athena_partitions,
    )

    t6 = PythonOperator(
        task_id="verify_athena_counts",
        python_callable=verify_athena_counts,
    )

    t7 = PythonOperator(
        task_id="report",
        python_callable=report,
        trigger_rule="all_done",
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
