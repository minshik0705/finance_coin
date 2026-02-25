# producers/coingecko_producer.py
import json
import time
from pathlib import Path
from typing import Optional

import requests
from kafka import KafkaProducer

# =========================
# Config
# =========================

TOPIC_NAME = "crypto-market-snapshots"

# [KEEP] 스냅샷 수집 주기 (매분)
POLL_SECONDS = 60

# [NEW] universe 리밸런싱 주기 (예: 하루 1회)
# 테스트할 때는 300(5분) 정도로 줄여도 됨
REBALANCE_SECONDS = 86400

VS_CURRENCY = "usd"

# [추천] 넓게 수집하고 싶으면 50~100으로 늘리기
# Dynamic universe는 이 중 top15를 뽑아도 됨
MARKET_FETCH_N = 50

# [NEW] Dynamic universe 크기
DYNAMIC_TOP_N = 15

# [CHANGED] 가격 이상탐지 목적이면 스테이블 제외를 기본 True로 추천
EXCLUDE_STABLECOINS = True
STABLE_SYMBOLS = {"USDT", "USDC", "DAI", "USDS", "TUSD", "FDUSD", "USDE"}

# [KEEP/CHANGED] 파일 경로
BASE_DIR = Path(__file__).resolve().parent
UNIVERSE_FILE = BASE_DIR / "dynamic_universe.json"
UNIVERSE_STATE_FILE = BASE_DIR / "dynamic_universe_state.json"  # [NEW] 편입/이탈 시각 추적용 상태 파일

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def fetch_markets(limit: int):
    """CoinGecko에서 시총 기준 시장 데이터 조회 (넓게 가져오기)"""
    resp = requests.get(
        "https://api.coingecko.com/api/v3/coins/markets",
        params={
            "vs_currency": VS_CURRENCY,
            "order": "market_cap_desc",
            "per_page": limit,
            "page": 1,
            "price_change_percentage": "24h",
            "sparkline": "false",
        },
        timeout=10
    )

    if resp.status_code == 429:
        raise RuntimeError("CoinGecko rate limited (429)")

    resp.raise_for_status()
    rows = resp.json()
    if not isinstance(rows, list):
        raise RuntimeError(f"Unexpected response type: {type(rows)} / body={rows}")
    return rows


def is_stable(base_symbol: str) -> bool:
    return base_symbol in STABLE_SYMBOLS


def normalize_market_row(r, now_ms):
    """
    CoinGecko /coins/markets row -> 공통 스키마 (스냅샷 메시지)
    """
    base_symbol = (r.get("symbol") or "").upper()
    coingecko_id = r.get("id")
    price = r.get("current_price")
    if price is None:
        return None

    msg = {
        # ===== 기존 필드 =====
        "exchange": "coingecko",
        "symbol": f"{base_symbol}USDT",
        "price": float(price),
        "quantity": 0,
        "timestamp": now_ms,
        "market_cap": r.get("market_cap"),
        "volume_24h": r.get("total_volume"),
        "change_24h_pct": r.get("price_change_percentage_24h"),

        # ===== 분석/매핑 보조 필드 =====
        "source_type": "market_snapshot",
        "base_symbol": base_symbol,
        "quote_symbol": "USDT",
        "coingecko_id": coingecko_id,
        "market_cap_rank": r.get("market_cap_rank"),
        "vs_currency": VS_CURRENCY,

        # [NEW] 스테이블 여부 명시 (모델/필터링에 유용)
        "is_stablecoin": is_stable(base_symbol),
    }
    return msg


def load_universe_state():
    """
    [NEW] dynamic universe 편입/이탈 추적 상태 로드
    상태 예시:
    {
      "version": 3,
      "symbols": {
        "ADA": {"entered_dynamic_at": 1111111, "exited_dynamic_at": null, "is_in_dynamic": true}
      }
    }
    """
    if not UNIVERSE_STATE_FILE.exists():
        return {"version": 0, "symbols": {}}

    try:
        return json.loads(UNIVERSE_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"version": 0, "symbols": {}}


def save_universe_state(state):
    UNIVERSE_STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def build_dynamic_universe_from_rows(rows, now_ms):
    """
    [NEW] rows(넓게 수집한 markets)에서 dynamic universe(DYNAMIC_TOP_N) 생성
    - 스테이블 제외 옵션 반영
    - top N 멤버십 리스트 반환
    """
    candidates = []
    for r in rows:
        base_symbol = (r.get("symbol") or "").upper()
        if not base_symbol:
            continue
        if EXCLUDE_STABLECOINS and is_stable(base_symbol):
            continue

        price = r.get("current_price")
        if price is None:
            continue

        candidates.append(r)

    # 이미 CoinGecko 응답이 시총순이지만, 안전하게 rank 기준 정렬
    candidates.sort(key=lambda x: (x.get("market_cap_rank") or 10**9))

    selected = candidates[:DYNAMIC_TOP_N]

    membership = []
    for idx, r in enumerate(selected, start=1):
        base_symbol = (r.get("symbol") or "").upper()
        membership.append({
            "rank": idx,
            "base_symbol": base_symbol,
            "pair_symbol": f"{base_symbol}USDT",
            "coingecko_id": r.get("id"),
            "market_cap_rank": r.get("market_cap_rank"),
        })

    return membership


def update_universe_with_metadata(membership, now_ms, reason):
    """
    [NEW] 네 블로그에 쓴 메타데이터 반영:
    - universe_version
    - generated_at
    - membership
    - reason
    - 코인별 is_in_dynamic / entered_dynamic_at / exited_dynamic_at
    """
    state = load_universe_state()
    prev_symbols = state.get("symbols", {})
    prev_members = {s for s, meta in prev_symbols.items() if meta.get("is_in_dynamic")}

    new_member_symbols = {item["base_symbol"] for item in membership}

    # version 증가
    new_version = int(state.get("version", 0)) + 1

    # 이전 상태 복사
    next_symbols = dict(prev_symbols)

    # 신규/유지 멤버 처리
    for item in membership:
        sym = item["base_symbol"]
        prev = next_symbols.get(sym, {})

        was_in = bool(prev.get("is_in_dynamic"))
        entered_at = prev.get("entered_dynamic_at")

        if not was_in:
            entered_at = now_ms  # 새 편입 시각 갱신

        next_symbols[sym] = {
            "is_in_dynamic": True,
            "entered_dynamic_at": entered_at,
            "exited_dynamic_at": None,  # 현재 포함 중이면 None
        }

    # 이탈 멤버 처리
    exited = prev_members - new_member_symbols
    for sym in exited:
        prev = next_symbols.get(sym, {})
        next_symbols[sym] = {
            "is_in_dynamic": False,
            "entered_dynamic_at": prev.get("entered_dynamic_at"),
            "exited_dynamic_at": now_ms,
        }

    # [NEW] universe payload (블로그 메타데이터 반영)
    payload = {
        "universe_version": new_version,
        "generated_at_ms": now_ms,
        "source": "coingecko",
        "universe_type": "dynamic",
        "criteria": f"market_cap_desc_top_{DYNAMIC_TOP_N}",
        "reason": reason,  # ex) daily_rebalance
        "vs_currency": VS_CURRENCY,
        "exclude_stablecoins": EXCLUDE_STABLECOINS,
        "membership": membership,
        # [NEW] 코인별 상태 메타데이터
        "member_state": next_symbols,
    }

    # 파일 저장
    UNIVERSE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    save_universe_state({"version": new_version, "symbols": next_symbols})

    return payload


def should_rebalance(now_ms, next_rebalance_at_ms):
    return now_ms >= next_rebalance_at_ms


def poll_markets_and_rebalance_universe():
    """
    [CHANGED] 핵심:
    - 매분: 시장 스냅샷 Kafka publish
    - 고정 주기(REBALANCE_SECONDS): dynamic universe 갱신 + 파일 저장
    """
    backoff_seconds = POLL_SECONDS

    # [NEW] 시작 시 즉시 1회 리밸런싱 하도록 설정
    next_rebalance_at_ms = 0

    while True:
        try:
            rows = fetch_markets(MARKET_FETCH_N)
            now_ms = int(time.time() * 1000)

            # 1) 매분 스냅샷 publish (넓게 수집)
            normalized_messages = []
            for r in rows:
                msg = normalize_market_row(r, now_ms)
                if msg is None:
                    continue
                normalized_messages.append(msg)

            for msg in normalized_messages:
                producer.send(TOPIC_NAME, msg)
            producer.flush(5)

            # 2) 고정 주기로만 dynamic universe 갱신
            if should_rebalance(now_ms, next_rebalance_at_ms):
                membership = build_dynamic_universe_from_rows(rows, now_ms)
                universe_payload = update_universe_with_metadata(
                    membership=membership,
                    now_ms=now_ms,
                    reason="fixed_rebalance_interval"
                )

                # 다음 리밸런싱 시각 설정
                next_rebalance_at_ms = now_ms + (REBALANCE_SECONDS * 1000)

                preview = [m["base_symbol"] for m in universe_payload["membership"][:5]]
                print(
                    f"[REBAlANCE] dynamic universe v{universe_payload['universe_version']} "
                    f"size={len(universe_payload['membership'])} top5={preview}"
                )

            preview = [m["base_symbol"] for m in normalized_messages[:5]]
            print(
                f"sent {len(normalized_messages)} coingecko snapshots "
                f"to {TOPIC_NAME} | top5={preview}"
            )

            backoff_seconds = POLL_SECONDS

        except Exception as e:
            print(f"CoinGecko error: {e}")
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 600)
            continue

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    poll_markets_and_rebalance_universe()