# airflow/dags/dynamic_universe_dag.py
"""
매일 01:00 UTC 실행.
CoinGecko API로 시총 top N 코인을 조회해서
dynamic_universe.json을 갱신한다.

coingecko_producer.py의 리밸런싱 로직을 DAG로 옮긴 것.
(WebSocket Producer와 달리 하루 1회 배치로 충분)

흐름:
  1) fetch_and_update : CoinGecko API 호출 → dynamic_universe.json 갱신
  2) report           : 갱신 결과 로그 출력
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)


# ────────────────────────────────────────
# 설정
# ────────────────────────────────────────

def _get_project_root() -> Path:
    return Path(
        Variable.get("PROJECT_ROOT", default_var="/opt/airflow/project")
    )


def _get_config() -> dict:
    return {
        "vs_currency":         Variable.get("COINGECKO_VS_CURRENCY",         default_var="usd"),
        "market_fetch_n":  int(Variable.get("COINGECKO_MARKET_FETCH_N",      default_var="50")),
        "dynamic_top_n":   int(Variable.get("COINGECKO_DYNAMIC_TOP_N",       default_var="15")),
        "exclude_stables":     Variable.get("COINGECKO_EXCLUDE_STABLECOINS", default_var="true").lower() == "true",
    }

STABLE_SYMBOLS = frozenset({"USDT", "USDC", "DAI", "USDS", "TUSD", "FDUSD", "USDE"})


# ────────────────────────────────────────
# 헬퍼
# ────────────────────────────────────────

def _is_stable(base_symbol: str) -> bool:
    return base_symbol.upper() in STABLE_SYMBOLS


def _fetch_markets(limit: int, vs_currency: str) -> list:
    resp = requests.get(
        "https://api.coingecko.com/api/v3/coins/markets",
        params={
            "vs_currency":             vs_currency,
            "order":                   "market_cap_desc",
            "per_page":                limit,
            "page":                    1,
            "price_change_percentage": "24h",
            "sparkline":               "false",
        },
        timeout=15,
    )
    if resp.status_code == 429:
        raise RuntimeError("CoinGecko rate limited (429) — 내일 재시도")
    resp.raise_for_status()
    rows = resp.json()
    if not isinstance(rows, list):
        raise RuntimeError(f"CoinGecko 응답 형식 오류: {type(rows)}")
    return rows


def _load_state(state_path: Path) -> dict:
    if not state_path.exists():
        return {"version": 0, "symbols": {}}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("state 파일 읽기 실패, 초기화: %s", e)
        return {"version": 0, "symbols": {}}


# ────────────────────────────────────────
# Task 1: CoinGecko 조회 + universe 갱신
# ────────────────────────────────────────

def fetch_and_update(**context) -> None:
    cfg         = _get_config()
    project_root = _get_project_root()

    universe_path = project_root / "producers" / "dynamic_universe.json"
    state_path    = project_root / "producers" / "dynamic_universe_state.json"

    log.info("프로젝트 루트: %s", project_root)
    log.info("universe 경로: %s", universe_path)

    # ── 1) CoinGecko API 호출 ──────────────
    log.info("CoinGecko API 호출 (top %d)", cfg["market_fetch_n"])
    rows = _fetch_markets(cfg["market_fetch_n"], cfg["vs_currency"])
    log.info("수신: %d개 코인", len(rows))

    now_ms = int(time.time() * 1000)

    # ── 2) 후보 필터링 ──────────────────────
    candidates = []
    for r in rows:
        base = (r.get("symbol") or "").upper()
        if not base:
            continue
        if cfg["exclude_stables"] and _is_stable(base):
            continue
        if r.get("current_price") is None:
            continue
        candidates.append(r)

    candidates.sort(key=lambda x: (x.get("market_cap_rank") or 10**9))
    top_n = candidates[:cfg["dynamic_top_n"]]

    membership = [
        {
            "rank":            idx,
            "base_symbol":     (r.get("symbol") or "").upper(),
            "pair_symbol":     f"{(r.get('symbol') or '').upper()}USDT",
            "coingecko_id":    r.get("id"),
            "market_cap_rank": r.get("market_cap_rank"),
        }
        for idx, r in enumerate(top_n, start=1)
    ]

    log.info(
        "선정된 심볼 (%d개): %s",
        len(membership),
        [m["base_symbol"] for m in membership],
    )

    # ── 3) state 갱신 ───────────────────────
    state       = _load_state(state_path)
    prev_symbols = state.get("symbols", {})
    prev_members = {s for s, m in prev_symbols.items() if m.get("is_in_dynamic")}
    new_members  = {item["base_symbol"] for item in membership}
    new_version  = int(state.get("version", 0)) + 1
    next_symbols = dict(prev_symbols)

    # 신규/유지
    for item in membership:
        sym  = item["base_symbol"]
        prev = next_symbols.get(sym, {})
        next_symbols[sym] = {
            "is_in_dynamic":      True,
            "entered_dynamic_at": prev.get("entered_dynamic_at") or now_ms,
            "exited_dynamic_at":  None,
        }

    # 이탈
    exited = prev_members - new_members
    for sym in exited:
        prev = next_symbols.get(sym, {})
        next_symbols[sym] = {
            "is_in_dynamic":      False,
            "entered_dynamic_at": prev.get("entered_dynamic_at"),
            "exited_dynamic_at":  now_ms,
        }

    if exited:
        log.info("이탈 심볼: %s", sorted(exited))
    new_entries = new_members - prev_members
    if new_entries:
        log.info("신규 진입 심볼: %s", sorted(new_entries))

    # ── 4) universe 파일 저장 ───────────────
    payload = {
        "universe_version":    new_version,
        "generated_at_ms":     now_ms,
        "source":              "coingecko",
        "universe_type":       "dynamic",
        "criteria":            f"market_cap_desc_top_{cfg['dynamic_top_n']}",
        "reason":              "daily_dag_rebalance",
        "vs_currency":         cfg["vs_currency"],
        "exclude_stablecoins": cfg["exclude_stables"],
        "membership":          membership,
        "member_state":        next_symbols,
    }

    universe_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    state_path.write_text(
        json.dumps({"version": new_version, "symbols": next_symbols},
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    log.info("dynamic_universe.json 저장 완료 (v%d)", new_version)

    # XCom으로 결과 전달
    context["ti"].xcom_push(key="version",      value=new_version)
    context["ti"].xcom_push(key="member_count", value=len(membership))
    context["ti"].xcom_push(key="new_entries",  value=sorted(new_entries))
    context["ti"].xcom_push(key="exited",       value=sorted(exited))
    context["ti"].xcom_push(key="top5",         value=[m["base_symbol"] for m in membership[:5]])


# ────────────────────────────────────────
# Task 2: 결과 리포트
# ────────────────────────────────────────

def report(**context) -> None:
    ti = context["ti"]

    version      = ti.xcom_pull(key="version",      task_ids="fetch_and_update")
    member_count = ti.xcom_pull(key="member_count", task_ids="fetch_and_update")
    new_entries  = ti.xcom_pull(key="new_entries",  task_ids="fetch_and_update")
    exited       = ti.xcom_pull(key="exited",       task_ids="fetch_and_update")
    top5         = ti.xcom_pull(key="top5",         task_ids="fetch_and_update")

    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    log.info("=" * 50)
    log.info("Dynamic Universe 리밸런싱 리포트 (%s)", now)
    log.info("=" * 50)
    log.info("버전:       v%s", version)
    log.info("총 심볼:    %s개", member_count)
    log.info("Top 5:      %s", top5)
    log.info("신규 진입:  %s", new_entries or "없음")
    log.info("이탈:       %s", exited or "없음")
    log.info("=" * 50)


# ────────────────────────────────────────
# DAG 정의
# ────────────────────────────────────────

with DAG(
    dag_id="dynamic_universe_dag",
    description="CoinGecko 시총 기준 Dynamic Universe 일별 리밸런싱",
    schedule="0 1 * * *",   # 매일 01:00 UTC (backfill 00:10, retrain 02:00 사이)
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),  # CoinGecko rate limit 대비
    },
    tags=["universe", "coingecko"],
) as dag:

    t1 = PythonOperator(
        task_id="fetch_and_update",
        python_callable=fetch_and_update,
    )

    t2 = PythonOperator(
        task_id="report",
        python_callable=report,
        trigger_rule="all_done",
    )

    t1 >> t2