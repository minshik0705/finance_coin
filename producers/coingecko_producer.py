# producers/coingecko_producer.py
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

# [변경] 2단계에서 개편 예정
# 1단계에서는 설정값 분리 + lazy init만 적용

import json
import time
from typing import Optional

import requests
from kafka import KafkaProducer

from config import (
    KAFKA_PRODUCER_CONFIG,
    TOPIC_SNAPSHOTS,
    DYNAMIC_UNIVERSE_FILE,
    DYNAMIC_UNIVERSE_STATE_FILE,
    COINGECKO_POLL_SECONDS,
    COINGECKO_REBALANCE_SECONDS,
    COINGECKO_MARKET_FETCH_N,
    COINGECKO_DYNAMIC_TOP_N,
    COINGECKO_VS_CURRENCY,
    COINGECKO_EXCLUDE_STABLECOINS,
    COINGECKO_STABLE_SYMBOLS,
)

# ────────────────────────────────────────
# Lazy KafkaProducer
# ────────────────────────────────────────

_producer: Optional[KafkaProducer] = None

def _get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            **KAFKA_PRODUCER_CONFIG,
        )
    return _producer

# ────────────────────────────────────────
# Helpers
# ────────────────────────────────────────

def fetch_markets(limit: int) -> list:
    resp = requests.get(
        "https://api.coingecko.com/api/v3/coins/markets",
        params={
            "vs_currency":          COINGECKO_VS_CURRENCY,
            "order":                "market_cap_desc",
            "per_page":             limit,
            "page":                 1,
            "price_change_percentage": "24h",
            "sparkline":            "false",
        },
        timeout=10
    )
    if resp.status_code == 429:
        raise RuntimeError("CoinGecko rate limited (429)")
    resp.raise_for_status()
    rows = resp.json()
    if not isinstance(rows, list):
        raise RuntimeError(f"Unexpected response: {type(rows)}")
    return rows


def is_stable(base_symbol: str) -> bool:
    return base_symbol in COINGECKO_STABLE_SYMBOLS


def normalize_market_row(r: dict, now_ms: int) -> Optional[dict]:
    base_symbol = (r.get("symbol") or "").upper()
    price = r.get("current_price")
    if price is None:
        return None

    return {
        "exchange":        "coingecko",
        "symbol":          f"{base_symbol}USDT",
        "price":           float(price),
        "quantity":        0,
        "timestamp":       now_ms,
        "market_cap":      r.get("market_cap"),
        "volume_24h":      r.get("total_volume"),
        "change_24h_pct":  r.get("price_change_percentage_24h"),
        "source_type":     "market_snapshot",
        "base_symbol":     base_symbol,
        "quote_symbol":    "USDT",
        "coingecko_id":    r.get("id"),
        "market_cap_rank": r.get("market_cap_rank"),
        "vs_currency":     COINGECKO_VS_CURRENCY,
        "is_stablecoin":   is_stable(base_symbol),
    }

# ────────────────────────────────────────
# Universe State
# ────────────────────────────────────────

def load_universe_state() -> dict:
    if not DYNAMIC_UNIVERSE_STATE_FILE.exists():
        return {"version": 0, "symbols": {}}
    try:
        return json.loads(DYNAMIC_UNIVERSE_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"version": 0, "symbols": {}}


def save_universe_state(state: dict) -> None:
    DYNAMIC_UNIVERSE_STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def build_dynamic_universe(rows: list, now_ms: int) -> list:
    candidates = [
        r for r in rows
        if (r.get("symbol") or "").upper()
        and not (COINGECKO_EXCLUDE_STABLECOINS
                 and is_stable((r.get("symbol") or "").upper()))
        and r.get("current_price") is not None
    ]
    candidates.sort(key=lambda x: (x.get("market_cap_rank") or 10**9))

    return [
        {
            "rank":           idx,
            "base_symbol":    (r.get("symbol") or "").upper(),
            "pair_symbol":    f"{(r.get('symbol') or '').upper()}USDT",
            "coingecko_id":   r.get("id"),
            "market_cap_rank": r.get("market_cap_rank"),
        }
        for idx, r in enumerate(candidates[:COINGECKO_DYNAMIC_TOP_N], start=1)
    ]


def update_universe(membership: list, now_ms: int, reason: str) -> dict:
    state = load_universe_state()
    prev_symbols = state.get("symbols", {})
    prev_members = {
        s for s, m in prev_symbols.items() if m.get("is_in_dynamic")
    }
    new_members = {item["base_symbol"] for item in membership}
    new_version = int(state.get("version", 0)) + 1
    next_symbols = dict(prev_symbols)

    # 신규/유지 멤버
    for item in membership:
        sym  = item["base_symbol"]
        prev = next_symbols.get(sym, {})
        entered_at = prev.get("entered_dynamic_at")
        if not prev.get("is_in_dynamic"):
            entered_at = now_ms
        next_symbols[sym] = {
            "is_in_dynamic":      True,
            "entered_dynamic_at": entered_at,
            "exited_dynamic_at":  None,
        }

    # 이탈 멤버
    for sym in (prev_members - new_members):
        prev = next_symbols.get(sym, {})
        next_symbols[sym] = {
            "is_in_dynamic":      False,
            "entered_dynamic_at": prev.get("entered_dynamic_at"),
            "exited_dynamic_at":  now_ms,
        }

    payload = {
        "universe_version":  new_version,
        "generated_at_ms":   now_ms,
        "source":            "coingecko",
        "universe_type":     "dynamic",
        "criteria":          f"market_cap_desc_top_{COINGECKO_DYNAMIC_TOP_N}",
        "reason":            reason,
        "vs_currency":       COINGECKO_VS_CURRENCY,
        "exclude_stablecoins": COINGECKO_EXCLUDE_STABLECOINS,
        "membership":        membership,
        "member_state":      next_symbols,
    }

    DYNAMIC_UNIVERSE_FILE.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    save_universe_state({"version": new_version, "symbols": next_symbols})
    return payload

# ────────────────────────────────────────
# Main Loop
# ────────────────────────────────────────

def poll_and_rebalance() -> None:
    backoff = COINGECKO_POLL_SECONDS
    next_rebalance_at_ms = 0  # 시작 시 즉시 1회 리밸런싱

    while True:
        try:
            rows   = fetch_markets(COINGECKO_MARKET_FETCH_N)
            now_ms = int(time.time() * 1000)
            producer = _get_producer()

            # 1) 매분 스냅샷 publish
            messages = [
                msg for r in rows
                if (msg := normalize_market_row(r, now_ms)) is not None
            ]
            for msg in messages:
                producer.send(TOPIC_SNAPSHOTS, msg)
            producer.flush(timeout=5)

            # 2) 주기적으로 dynamic universe 갱신
            if now_ms >= next_rebalance_at_ms:
                membership = build_dynamic_universe(rows, now_ms)
                result = update_universe(
                    membership=membership,
                    now_ms=now_ms,
                    reason="fixed_rebalance_interval",
                )
                next_rebalance_at_ms = now_ms + (COINGECKO_REBALANCE_SECONDS * 1000)

                preview = [m["base_symbol"] for m in result["membership"][:5]]
                print(f"[REBALANCE] v{result['universe_version']} "
                      f"size={len(result['membership'])} top5={preview}")

            preview = [m.get("base_symbol") for m in messages[:5]]
            print(f"[COINGECKO] sent {len(messages)} snapshots | top5={preview}")

            backoff = COINGECKO_POLL_SECONDS

        except Exception as e:
            print(f"[ERROR] CoinGecko: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 600)
            continue

        time.sleep(COINGECKO_POLL_SECONDS)


if __name__ == "__main__":
    poll_and_rebalance()
