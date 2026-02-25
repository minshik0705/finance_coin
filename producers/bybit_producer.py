# producers/bybit_producer.py
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

# [NEW] Universe 파일 경로 (2층 Universe: Core + Dynamic)
BASE_DIR = Path(__file__).resolve().parent
CORE_UNIVERSE_FILE = BASE_DIR / "core_universe.json"
DYNAMIC_UNIVERSE_FILE = BASE_DIR / "dynamic_universe.json"

# [NEW] universe 파일 변경 감지 주기(초)
UNIVERSE_REFRESH_SECONDS = 30

# [NEW] Bybit public trade WS endpoint (v5)
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/spot"

# [NEW] CoinGecko topN에 있어도 Bybit spot USDT 페어가 없을 수 있음
BYBIT_BLOCKLIST = {
    # "FIGR_HELOCUSDT",
}

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# 전역 상태
current_symbols = []
ws_app = None
stop_event = threading.Event()
reload_event = threading.Event()


# =========================
# Universe Loading (Binance와 동일 패턴)
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
    """
    [NEW] core_universe.json에서 pair_symbol 읽기
    반환: set[str]  ex) {"BTCUSDT", "ETHUSDT"}
    """
    data = _safe_read_json(CORE_UNIVERSE_FILE)
    if not data:
        return set()

    symbols = set()
    membership = data.get("membership", [])
    for item in membership:
        pair = (item.get("pair_symbol") or "").upper().strip()
        if pair:
            symbols.add(pair)
    return symbols


def load_dynamic_symbols():
    """
    [NEW] dynamic_universe.json에서 membership 읽기
    반환: set[str]
    """
    data = _safe_read_json(DYNAMIC_UNIVERSE_FILE)
    if not data:
        return set()

    symbols = set()
    membership = data.get("membership", [])
    for item in membership:
        pair = (item.get("pair_symbol") or "").upper().strip()
        if pair:
            symbols.add(pair)
    return symbols


def load_universe_symbols():
    """
    [NEW] 2층 Universe = Core + Dynamic 합집합
    """
    core_syms = load_core_symbols()
    dyn_syms = load_dynamic_symbols()

    merged = (core_syms | dyn_syms)
    merged = {s for s in merged if s.endswith("USDT")}
    merged = {s for s in merged if s not in BYBIT_BLOCKLIST}
    return sorted(merged)


# =========================
# Bybit WS Helpers
# =========================

def parse_bybit_trade_message(message):
    """
    [NEW] Bybit v5 public spot trade 메시지 파싱
    예시 형식(개략):
    {
      "topic":"publicTrade.BTCUSDT",
      "type":"snapshot",
      "ts": 1710000000000,
      "data":[
        {
          "i":"tradeId",
          "T":1710000000000,   # trade time ms
          "s":"BTCUSDT",
          "p":"68001.4",
          "v":"0.003",
          "S":"Buy" / "Sell"
        },
        ...
      ]
    }

    반환: list[normalized]
    """
    payload = json.loads(message)

    topic = payload.get("topic", "")
    data_list = payload.get("data")

    # ping/pong / subscribe ack 등 무시
    if not isinstance(data_list, list):
        return []

    normalized_list = []

    for item in data_list:
        symbol = item.get("s")
        price = item.get("p")
        qty = item.get("v")
        trade_ts = item.get("T")  # ms

        if not symbol or price is None or qty is None or trade_ts is None:
            continue

        normalized = {
            "exchange": "bybit",
            "symbol": symbol,
            "price": float(price),
            "quantity": float(qty),
            "timestamp": int(trade_ts),

            # [NEW] 보조 필드
            "source_type": "trade",
            "base_symbol": symbol[:-4] if symbol.endswith("USDT") else symbol,
            "quote_symbol": "USDT" if symbol.endswith("USDT") else None,

            # [NEW] 원본 보조 정보 (선택)
            "side": item.get("S"),    # Buy / Sell
            "topic": topic,
        }
        normalized_list.append(normalized)

    return normalized_list


def send_subscribe(ws, symbols):
    """
    [NEW] Bybit 구독 메시지 전송
    topics 예: publicTrade.BTCUSDT
    """
    topics = [f"publicTrade.{s}" for s in symbols]
    req = {
        "op": "subscribe",
        "args": topics
    }
    ws.send(json.dumps(req))
    print(f"[INFO] Bybit subscribe sent for {len(symbols)} symbols")


# =========================
# WebSocket Callbacks
# =========================

def on_open(ws):
    print(f"[INFO] Bybit websocket opened for {len(current_symbols)} symbols")
    preview = current_symbols[:10]
    print(f"[INFO] symbols preview: {preview}")
    send_subscribe(ws, current_symbols)


def on_message(ws, message):
    try:
        payload = json.loads(message)

        # [NEW] ping/pong/ack 로그 최소 처리
        if payload.get("op") in {"pong", "ping", "subscribe"}:
            # subscribe 응답 등
            # print(f"[DEBUG] Bybit control msg: {payload}")
            return

        msgs = parse_bybit_trade_message(message)
        if not msgs:
            return

        for normalized in msgs:
            producer.send(TOPIC_NAME, normalized)
            print(f"Bybit {normalized['symbol']}: ${normalized['price']:.4f}")

    except Exception as e:
        print(f"[ERROR] Bybit on_message: {e}")


def on_error(ws, error):
    print(f"[ERROR] Bybit websocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"[INFO] Bybit websocket closed: code={close_status_code}, msg={close_msg}")


# =========================
# Runner / Reload Logic
# =========================

def run_ws_for_symbols(symbols):
    """
    [NEW] 주어진 symbols로 Bybit WS 연결
    """
    global ws_app, current_symbols
    current_symbols = list(symbols)

    if not current_symbols:
        print("[WARN] Bybit no symbols to subscribe. sleeping...")
        time.sleep(5)
        return

    print(f"[INFO] connecting Bybit WS for {len(current_symbols)} symbols")

    ws_app = websocket.WebSocketApp(
        BYBIT_WS_URL,
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
                print(f"[INFO] Bybit initial universe loaded ({len(symbols)}): {symbols[:10]}")
            elif symbols != last_symbols:
                print("[INFO] Bybit universe changed -> triggering websocket reload")
                print(f"       old({len(last_symbols)}): {last_symbols[:10]}")
                print(f"       new({len(symbols)}): {symbols[:10]}")
                last_symbols = symbols
                reload_event.set()

        except Exception as e:
            print(f"[WARN] Bybit universe watcher error: {e}")

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
                    print("[INFO] Bybit closing websocket for universe reload...")
                    try:
                        if ws_app is not None:
                            ws_app.close()
                    except Exception as e:
                        print(f"[WARN] Bybit ws close error: {e}")
                    break
                time.sleep(1)

            ws_thread.join(timeout=5)

        except KeyboardInterrupt:
            print("[INFO] Bybit KeyboardInterrupt received. shutting down...")
            stop_event.set()
            try:
                if ws_app is not None:
                    ws_app.close()
            except Exception:
                pass
            break
        except Exception as e:
            print(f"[ERROR] Bybit main loop error: {e}")

        if not stop_event.is_set():
            time.sleep(retry_sleep)


if __name__ == "__main__":
    main()