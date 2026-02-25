# producers/okx_producer.py
import json
import time
import threading
from pathlib import Path

import websocket
from kafka import KafkaProducer

# =========================
# Config
# =========================

TOPIC_NAME = "crypto-trades"

# [NEW] Universe 파일 경로 (2층 Universe)
BASE_DIR = Path(__file__).resolve().parent
CORE_UNIVERSE_FILE = BASE_DIR / "core_universe.json"
DYNAMIC_UNIVERSE_FILE = BASE_DIR / "dynamic_universe.json"

# [NEW] universe 파일 변경 감지 주기
UNIVERSE_REFRESH_SECONDS = 30

# [NEW] OKX public websocket endpoint
OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

# [NEW] OKX에서 없을 수 있는 페어 제외용
OKX_BLOCKLIST = {
    # "FIGR_HELOCUSDT",
}

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# 전역 상태
current_symbols = []          # ["BTCUSDT", ...] (내부 표준)
current_okx_inst_ids = []     # ["BTC-USDT", ...] (OKX 구독용)
ws_app = None
stop_event = threading.Event()
reload_event = threading.Event()


# =========================
# Universe Loading
# =========================

def _safe_read_json(path: Path):
    """[NEW] 파일 없거나 JSON 깨져도 죽지 않게 안전 로드"""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[WARN] failed to read {path.name}: {e}")
        return None


def load_core_symbols():
    data = _safe_read_json(CORE_UNIVERSE_FILE)
    if not data:
        return set()

    symbols = set()
    for item in data.get("membership", []):
        pair = (item.get("pair_symbol") or "").upper().strip()
        if pair:
            symbols.add(pair)
    return symbols


def load_dynamic_symbols():
    data = _safe_read_json(DYNAMIC_UNIVERSE_FILE)
    if not data:
        return set()

    symbols = set()
    for item in data.get("membership", []):
        pair = (item.get("pair_symbol") or "").upper().strip()
        if pair:
            symbols.add(pair)
    return symbols


def load_universe_symbols():
    """
    [NEW] Core + Dynamic 합집합 (내부 표준: BTCUSDT)
    """
    merged = load_core_symbols() | load_dynamic_symbols()
    merged = {s for s in merged if s.endswith("USDT")}
    merged = {s for s in merged if s not in OKX_BLOCKLIST}
    return sorted(merged)


# =========================
# Symbol Mapping (Internal <-> OKX)
# =========================

def to_okx_inst_id(symbol: str):
    """
    [NEW] 내부 표준 BTCUSDT -> OKX instId BTC-USDT 변환
    """
    s = symbol.upper()
    if s.endswith("USDT"):
        base = s[:-4]
        return f"{base}-USDT"
    return None


def from_okx_inst_id(inst_id: str):
    """
    [NEW] OKX instId BTC-USDT -> 내부 표준 BTCUSDT 변환
    """
    if not inst_id:
        return None
    return inst_id.replace("-", "").upper()


# =========================
# OKX WS Helpers
# =========================

def send_subscribe(ws, okx_inst_ids):
    """
    [NEW] OKX trades 채널 구독
    형식:
    {
      "op":"subscribe",
      "args":[{"channel":"trades","instId":"BTC-USDT"}, ...]
    }
    """
    args = [{"channel": "trades", "instId": inst_id} for inst_id in okx_inst_ids]
    req = {
        "op": "subscribe",
        "args": args
    }
    ws.send(json.dumps(req))
    print(f"[INFO] OKX subscribe sent for {len(okx_inst_ids)} symbols")


def parse_okx_trade_message(message):
    """
    [NEW] OKX public trades 메시지 파싱
    예시 형식(개략):
    {
      "arg": {"channel":"trades", "instId":"BTC-USDT"},
      "data": [
        {
          "instId":"BTC-USDT",
          "tradeId":"...",
          "px":"68001.4",
          "sz":"0.003",
          "side":"buy",
          "ts":"1710000000000"
        }
      ]
    }

    반환: list[normalized]
    """
    payload = json.loads(message)

    data_list = payload.get("data")
    if not isinstance(data_list, list):
        return []

    arg = payload.get("arg", {})
    channel = arg.get("channel")
    inst_id_from_arg = arg.get("instId")

    normalized_list = []

    for item in data_list:
        inst_id = item.get("instId") or inst_id_from_arg
        price = item.get("px")
        qty = item.get("sz")
        trade_ts = item.get("ts")  # 문자열 ms
        side = item.get("side")

        if not inst_id or price is None or qty is None or trade_ts is None:
            continue

        symbol = from_okx_inst_id(inst_id)
        if not symbol:
            continue

        normalized = {
            "exchange": "okx",
            "symbol": symbol,  # 내부 표준으로 통일 (BTCUSDT)
            "price": float(price),
            "quantity": float(qty),
            "timestamp": int(trade_ts),

            # [NEW] 보조 필드
            "source_type": "trade",
            "base_symbol": symbol[:-4] if symbol.endswith("USDT") else symbol,
            "quote_symbol": "USDT" if symbol.endswith("USDT") else None,

            # [NEW] 원본 보조 정보
            "side": side,              # buy / sell
            "okx_inst_id": inst_id,    # BTC-USDT
            "channel": channel,
        }
        normalized_list.append(normalized)

    return normalized_list


# =========================
# WebSocket Callbacks
# =========================

def on_open(ws):
    print(f"[INFO] OKX websocket opened for {len(current_symbols)} symbols")
    print(f"[INFO] symbols preview: {current_symbols[:10]}")
    print(f"[INFO] instIds preview: {current_okx_inst_ids[:10]}")
    send_subscribe(ws, current_okx_inst_ids)


def on_message(ws, message):
    try:
        payload = json.loads(message)

        # [NEW] event/subscribe ack/error/pong 처리
        if "event" in payload:
            event = payload.get("event")
            if event in {"subscribe", "error"}:
                print(f"[INFO] OKX event: {payload}")
            return

        msgs = parse_okx_trade_message(message)
        if not msgs:
            return

        for normalized in msgs:
            producer.send(TOPIC_NAME, normalized)
            print(f"OKX {normalized['symbol']}: ${normalized['price']:.4f}")

    except Exception as e:
        print(f"[ERROR] OKX on_message: {e}")


def on_error(ws, error):
    print(f"[ERROR] OKX websocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"[INFO] OKX websocket closed: code={close_status_code}, msg={close_msg}")


# =========================
# Runner / Reload Logic
# =========================

def run_ws_for_symbols(symbols):
    """
    [NEW] 주어진 내부 표준 symbols(BTCUSDT...)로 OKX WS 연결
    - OKX 구독 시에는 BTC-USDT 형식으로 변환
    """
    global ws_app, current_symbols, current_okx_inst_ids

    current_symbols = list(symbols)
    current_okx_inst_ids = []

    for s in current_symbols:
        inst_id = to_okx_inst_id(s)
        if inst_id:
            current_okx_inst_ids.append(inst_id)

    if not current_okx_inst_ids:
        print("[WARN] OKX no symbols to subscribe. sleeping...")
        time.sleep(5)
        return

    print(f"[INFO] connecting OKX WS for {len(current_okx_inst_ids)} symbols")

    ws_app = websocket.WebSocketApp(
        OKX_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws_app.run_forever(ping_interval=20, ping_timeout=10)


def universe_watcher():
    """
    [NEW] universe 변경 감지 -> reload_event
    """
    last_symbols = None

    while not stop_event.is_set():
        try:
            symbols = load_universe_symbols()
            if not symbols:
                symbols = ["BTCUSDT"]

            if last_symbols is None:
                last_symbols = symbols
                print(f"[INFO] OKX initial universe loaded ({len(symbols)}): {symbols[:10]}")
            elif symbols != last_symbols:
                print("[INFO] OKX universe changed -> triggering websocket reload")
                print(f"       old({len(last_symbols)}): {last_symbols[:10]}")
                print(f"       new({len(symbols)}): {symbols[:10]}")
                last_symbols = symbols
                reload_event.set()

        except Exception as e:
            print(f"[WARN] OKX universe watcher error: {e}")

        time.sleep(UNIVERSE_REFRESH_SECONDS)


def main():
    watcher_thread = threading.Thread(target=universe_watcher, daemon=True)
    watcher_thread.start()

    retry_sleep = 3

    while not stop_event.is_set():
        symbols = load_universe_symbols()
        if not symbols:
            symbols = ["BTCUSDT"]

        reload_event.clear()

        try:
            ws_thread = threading.Thread(target=run_ws_for_symbols, args=(symbols,), daemon=True)
            ws_thread.start()

            while ws_thread.is_alive() and not stop_event.is_set():
                if reload_event.is_set():
                    print("[INFO] OKX closing websocket for universe reload...")
                    try:
                        if ws_app is not None:
                            ws_app.close()
                    except Exception as e:
                        print(f"[WARN] OKX ws close error: {e}")
                    break
                time.sleep(1)

            ws_thread.join(timeout=5)

        except KeyboardInterrupt:
            print("[INFO] OKX KeyboardInterrupt received. shutting down...")
            stop_event.set()
            try:
                if ws_app is not None:
                    ws_app.close()
            except Exception:
                pass
            break
        except Exception as e:
            print(f"[ERROR] OKX main loop error: {e}")

        if not stop_event.is_set():
            time.sleep(retry_sleep)


if __name__ == "__main__":
    main()