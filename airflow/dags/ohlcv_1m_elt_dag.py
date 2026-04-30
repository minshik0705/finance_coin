from __future__ import annotations

import json
import logging
import shutil
import subprocess
import sys
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

def _setup_sys_path() -> None:
    project_root = Variable.get(
        "PROJECT_ROOT",
        default_var="/opt/airflow/project",
    )
    if project_root not in sys.path:
        sys.path.insert(0, project_root)



def _get_project_root() -> Path:
    return Path(
        Variable.get("PROJECT_ROOT", default_var="/opt/airflow/project")
    )



def _get_export_root() -> Path:
    return Path(
        Variable.get(
            "OHLCV_EXPORT_ROOT",
            default_var="/tmp/argus_export/ohlcv_1m",
        )
    )



def _get_export_script_path() -> Path:
    return Path(
        Variable.get(
            "OHLCV_EXPORT_SCRIPT_PATH",
            default_var=str(_get_project_root() / "scripts" / "export_ohlcv_1m_parquet.py"),
        )
    )



def _get_exchanges() -> list[str]:
    raw = Variable.get(
        "OHLCV_EXPORT_EXCHANGES",
        default_var="binance,bybit,okx",
    )
    return [x.strip() for x in raw.split(",") if x.strip()]



def _get_s3_bucket() -> str:
    return Variable.get(
        "OHLCV_S3_BUCKET",
        default_var="argus-lake-753101492795-ap-northeast-2-an",
    )



def _get_s3_prefix() -> str:
    return Variable.get(
        "OHLCV_S3_PREFIX",
        default_var="curated/ohlcv_1m",
    ).strip("/")



def _get_athena_database() -> str:
    return Variable.get("ATHENA_DATABASE", default_var="coinview_lake")



def _get_athena_table() -> str:
    return Variable.get("ATHENA_OHLCV_TABLE", default_var="ohlcv_1m")



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
    # 수동 트리거와 스케줄 실행 모두 동일한 기준을 쓰기 위해
    # 현재 UTC 시각 기준 전날을 대상 날짜로 고정
    target_date = datetime.now(tz=timezone.utc).date() - timedelta(days=1)

    target_date_str = target_date.isoformat()
    return {
        "target_date": target_date_str,
        "start_date": target_date_str,
        "end_date": target_date_str,
    }



def _target_local_partition_dir(target_date: str) -> Path:
    return _get_export_root() / f"dt={target_date}"



def _target_s3_partition_prefix(target_date: str) -> str:
    return f"{_get_s3_prefix()}/dt={target_date}"



def _load_parquet_row_counts(target_date: str) -> dict[str, int]:
    try:
        import pyarrow.parquet as pq
    except ImportError as e:
        raise RuntimeError(
            "pyarrow가 Airflow 이미지에 없습니다. requirements.txt / Docker image를 확인하세요."
        ) from e

    partition_dir = _target_local_partition_dir(target_date)
    if not partition_dir.exists():
        raise FileNotFoundError(f"export 결과 디렉터리가 없습니다: {partition_dir}")

    counts: dict[str, int] = {}

    for exchange in _get_exchanges():
        parquet_path = partition_dir / f"exchange={exchange}" / "ohlcv.parquet"
        if not parquet_path.exists():
            raise FileNotFoundError(f"parquet 파일이 없습니다: {parquet_path}")

        metadata = pq.ParquetFile(parquet_path).metadata
        counts[exchange] = int(metadata.num_rows)

    return counts



def _get_boto3_client(service_name: str):
    try:
        import boto3
    except ImportError as e:
        raise RuntimeError(
            "boto3가 Airflow 이미지에 없습니다. boto3를 requirements.txt에 추가하거나 amazon provider 설치를 확인하세요."
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
                columns = [c.get("VarCharValue", "") for c in result_rows[0].get("Data", [])]
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

    log.info("ELT 대상 날짜(UTC 전날): %s", target_date)
    log.info("프로젝트 루트: %s", _get_project_root())
    log.info("export 스크립트: %s", _get_export_script_path())
    log.info("로컬 export 루트: %s", _get_export_root())
    log.info("S3 prefix: s3://%s/%s", _get_s3_bucket(), _get_s3_prefix())
    log.info("대상 거래소: %s", _get_exchanges())

    for key, value in payload.items():
        context["ti"].xcom_push(key=key, value=value)


# ────────────────────────────────────────
# Task 2: parquet export
# ────────────────────────────────────────

def export_ohlcv_parquet(**context) -> None:
    _setup_sys_path()

    ti = context["ti"]
    target_date: str = ti.xcom_pull(key="target_date", task_ids="prepare_context")

    project_root = _get_project_root()
    export_root = _get_export_root()
    script_path = _get_export_script_path()
    partition_dir = _target_local_partition_dir(target_date)

    if not script_path.exists():
        raise FileNotFoundError(f"export 스크립트를 찾을 수 없습니다: {script_path}")

    if partition_dir.exists():
        log.info("기존 로컬 partition 삭제: %s", partition_dir)
        shutil.rmtree(partition_dir)

    cmd = [
        sys.executable,
        str(script_path),
        "--start-date",
        target_date,
        "--end-date",
        target_date,
        "--exchanges",
        *_get_exchanges(),
        "--out-dir",
        str(export_root),
        "--overwrite",
    ]

    log.info("export command: %s", " ".join(cmd))

    result = subprocess.run(
        cmd,
        cwd=str(project_root),
        capture_output=True,
        text=True,
        check=False,
    )

    if result.stdout:
        log.info("export stdout:\n%s", result.stdout)
    if result.stderr:
        log.warning("export stderr:\n%s", result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"export 스크립트 실패 (exit={result.returncode})")

    counts = _load_parquet_row_counts(target_date)
    total_rows = sum(counts.values())

    log.info("로컬 parquet row counts: %s", counts)
    log.info("총 row 수: %d", total_rows)

    ti.xcom_push(key="local_counts", value=counts)
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
        ti.xcom_pull(key="local_partition_dir", task_ids="export_ohlcv_parquet")
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
        task_ids="export_ohlcv_parquet",
    ) or {}

    database = _get_athena_database()
    table = _get_athena_table()

    query = f'''
    SELECT
      "exchange" AS exchange_name,
      CAST(COUNT(*) AS BIGINT) AS row_count,
      CAST(MAX(time) AS VARCHAR) AS latest_time
    FROM {database}.{table}
    WHERE dt = '{target_date}'
    GROUP BY 1
    ORDER BY 1
    '''.strip()

    query_execution_id = _run_athena_query(query=query, database=database)
    rows = _fetch_athena_rows(query_execution_id)

    athena_counts: dict[str, int] = {}
    latest_times: dict[str, str] = {}

    for row in rows:
        exchange = row.get("exchange_name") or row.get("exchange") or ""
        count_str = row.get("row_count", "0")
        athena_counts[exchange] = int(count_str)
        latest_times[exchange] = row.get("latest_time", "")

    log.info("Athena row counts: %s", athena_counts)
    log.info("Athena latest times: %s", latest_times)

    missing = set(local_counts) - set(athena_counts)
    mismatched = {
        exchange: {"local": local_counts[exchange], "athena": athena_counts.get(exchange)}
        for exchange in local_counts
        if athena_counts.get(exchange) != local_counts[exchange]
    }

    if missing:
        raise RuntimeError(f"Athena partition 누락: {sorted(missing)}")

    if mismatched:
        raise RuntimeError(f"Athena row count 불일치: {json.dumps(mismatched, ensure_ascii=False)}")

    ti.xcom_push(key="athena_counts", value=athena_counts)
    ti.xcom_push(key="athena_latest_times", value=latest_times)


# ────────────────────────────────────────
# Task 7: 결과 리포트
# ────────────────────────────────────────

def report(**context) -> None:
    ti = context["ti"]

    target_date = ti.xcom_pull(key="target_date", task_ids="prepare_context")
    local_counts = ti.xcom_pull(key="local_counts", task_ids="export_ohlcv_parquet")
    athena_counts = ti.xcom_pull(key="athena_counts", task_ids="verify_athena_counts")
    latest_times = ti.xcom_pull(key="athena_latest_times", task_ids="verify_athena_counts")
    uploaded_keys = ti.xcom_pull(key="uploaded_keys", task_ids="upload_to_s3")

    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    log.info("=" * 60)
    log.info("ohlcv_1m ELT 리포트 (%s)", now)
    log.info("=" * 60)
    log.info("대상 날짜: %s", target_date)
    log.info("업로드 파일 수: %d", len(uploaded_keys or []))
    log.info("로컬 parquet row counts: %s", local_counts or {})
    log.info("Athena row counts: %s", athena_counts or {})
    log.info("Athena latest times: %s", latest_times or {})
    log.info("=" * 60)


with DAG(
    dag_id="ohlcv_1m_elt_dag",
    description="전날 UTC 기준 ohlcv_1m parquet export -> S3 업로드 -> Athena partition 반영",
    schedule="40 0 * * *",  # backfill_dag(00:10 UTC) 이후 실행
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["elt", "ohlcv", "athena", "s3"],
) as dag:

    t1 = PythonOperator(
        task_id="prepare_context",
        python_callable=prepare_context,
    )

    t2 = PythonOperator(
        task_id="export_ohlcv_parquet",
        python_callable=export_ohlcv_parquet,
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

