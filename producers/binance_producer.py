# producers/binance_producer.py
import json
import time
import threading
from pathlib import Path
from urllib.parse import quote

import websocket
from kafka import KafkaProducer

# =========================
# Config
# =========================

# [CHANGED] Kafka topic 이름 상수화 (coingecko와 동일 스키마 토픽으로 유지 가능)
TOPIC_NAME = "crypto-trades"

# [NEW] Universe 파일 경로 (2층 Universe: Core + Dynamic)
BASE_DIR = Path(__file__).resolve().parent
CORE_UNIVERSE_FILE = BASE_DIR / "core_universe.json"
DYNAMIC_UNIVERSE_FILE = BASE_DIR / "dynamic_universe.json"

# [NEW] universe 파일 변경 감지 주기(초)
UNIVERSE_REFRESH_SECONDS = 30

# [NEW] Binance websocket endpoint (combined stream 사용)
BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream?streams="

# [NEW] Binance에서 지원하지 않을 가능성이 큰 심볼 제외용(필요시 확장)
# 예: CoinGecko topN에 들어오지만 Binance 현물 USDT 페어 없는 경우가 있음
BINANCE_BLOCKLIST = {
    # "FIGR_HELOCUSDT",  # 예시
}

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# [NEW] 전역 상태 (현재 구독 심볼 / ws 인스턴스 / 리로드 시그널)
current_symbols = []
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
    반환: set[str] ex) {"BTCUSDT", "SOLUSDT"}
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
    - Binance blocklist 제거
    - 정렬해서 반환 (비교/로그 안정성)
    """
    core_syms = load_core_symbols()
    dyn_syms = load_dynamic_symbols()

    merged = (core_syms | dyn_syms)
    merged = {s for s in merged if s.endswith("USDT")}
    merged = {s for s in merged if s not in BINANCE_BLOCKLIST}

    return sorted(merged)


# =========================
# Binance WS Helpers
# =========================

def build_combined_stream_url(symbols):
    """
    [NEW] Binance combined stream URL 생성
    ex) btcusdt@trade/ethusdt@trade
    """
    if not symbols:
        return None

    streams = "/".join([f"{s.lower()}@trade" for s in symbols])
    # quote는 엄밀히 필요 없지만 안전하게 유지 가능
    return BINANCE_WS_BASE + quote(streams, safe="/@")

def parse_binance_trade_message(message):
    """
    [NEW] Binance combined stream 메시지 파싱
    combined stream 형식:
    {
      "stream":"btcusdt@trade",
      "data": {... trade payload ...}
    }
    """
    payload = json.loads(message)

    # [CHANGED] 단일 stream / combined stream 둘 다 지원
    if "data" in payload:
        data = payload["data"]
    else:
        data = payload

    # trade event 기준 필드:
    # s: symbol, p: price, q: quantity, E: event time(ms)
    symbol = data.get("s")  # ex) BTCUSDT
    price = data.get("p")
    qty = data.get("q")
    event_time = data.get("E")

    if not symbol or price is None or qty is None or event_time is None:
        return None

    normalized = {
        "exchange": "binance",
        "symbol": symbol,
        "price": float(price),
        "quantity": float(qty),
        "timestamp": int(event_time),

        # [NEW] 보조 필드 (coingecko snapshot과 merge/분석에 도움)
        "source_type": "trade",
        "base_symbol": symbol[:-4] if symbol.endswith("USDT") else symbol,
        "quote_symbol": "USDT" if symbol.endswith("USDT") else None,
    }
    return normalized


# =========================
# WebSocket Callbacks
# =========================

def on_message(ws, message):
    # [CHANGED] BTC 고정 -> 실제 메시지 symbol 사용
    try:
        normalized = parse_binance_trade_message(message)
        if normalized is None:
            return

        producer.send(TOPIC_NAME, normalized)
        # print 너무 많으면 병목될 수 있으니 간단 로그 권장
        print(f"Binance {normalized['symbol']}: ${normalized['price']:.4f}")

    except Exception as e:
        print(f"[ERROR] on_message: {e}")


def on_error(ws, error):
    print(f"[ERROR] websocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"[INFO] websocket closed: code={close_status_code}, msg={close_msg}")


def on_open(ws):
    print(f"[INFO] websocket opened for {len(current_symbols)} symbols")
    preview = current_symbols[:10]
    print(f"[INFO] symbols preview: {preview}")


# =========================
# Runner / Reload Logic
# =========================

def run_ws_for_symbols(symbols):
    """
    [NEW] 주어진 symbols로 Binance combined stream 연결
    - reload_event가 켜지면 현재 연결을 닫고 빠져나가도록 설계
    """
    global ws_app, current_symbols

    current_symbols = list(symbols)

    url = build_combined_stream_url(current_symbols)
    if not url:
        print("[WARN] no symbols to subscribe. sleeping...")
        time.sleep(5)
        return

    print(f"[INFO] connecting Binance WS for {len(current_symbols)} symbols")
    # print(url)  # 필요시 디버깅

    ws_app = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    # [CHANGED] ping 설정 추가 (연결 안정성)
    # run_forever는 블로킹. 외부에서 ws_app.close() 호출하면 종료됨.
    ws_app.run_forever(ping_interval=20, ping_timeout=10)


def universe_watcher():
    """
    [NEW] universe 파일을 주기적으로 읽고 변경 감지
    - Core + Dynamic 합집합이 바뀌면 reload_event set
    """
    last_symbols = None

    while not stop_event.is_set():
        try:
            symbols = load_universe_symbols()

            # [NEW] 최소 안전장치: universe 파일 없을 때 기본값 (초기 부팅용)
            if not symbols:
                symbols = ["BTCUSDT"]

            if last_symbols is None:
                last_symbols = symbols
                print(f"[INFO] initial universe loaded ({len(symbols)}): {symbols[:10]}")
            elif symbols != last_symbols:
                print("[INFO] universe changed -> triggering websocket reload")
                print(f"       old({len(last_symbols)}): {last_symbols[:10]}")
                print(f"       new({len(symbols)}): {symbols[:10]}")
                last_symbols = symbols
                reload_event.set()

        except Exception as e:
            print(f"[WARN] universe watcher error: {e}")

        time.sleep(UNIVERSE_REFRESH_SECONDS)


def main():
    """
    [NEW] 메인 루프:
    1) watcher thread가 universe 변경 감지
    2) ws 연결 실행
    3) 변경 감지되면 ws 재연결
    """
    watcher_thread = threading.Thread(target=universe_watcher, daemon=True)
    watcher_thread.start()

    retry_sleep = 3

    while not stop_event.is_set():
        symbols = load_universe_symbols()
        if not symbols:
            symbols = ["BTCUSDT"]  # [NEW] 초기 fallback

        reload_event.clear()

        try:
            # ws를 별도 thread로 실행해서 reload_event 감시 가능하게 구성
            ws_thread = threading.Thread(target=run_ws_for_symbols, args=(symbols,), daemon=True)
            ws_thread.start()

            # [NEW] loop: reload_event 또는 stop_event 대기
            while ws_thread.is_alive() and not stop_event.is_set():
                if reload_event.is_set():
                    print("[INFO] closing websocket for universe reload...")
                    try:
                        if ws_app is not None:
                            ws_app.close()
                    except Exception as e:
                        print(f"[WARN] ws close error: {e}")
                    break
                time.sleep(1)

            # ws_thread 종료 대기(짧게)
            ws_thread.join(timeout=5)

        except KeyboardInterrupt:
            print("[INFO] KeyboardInterrupt received. shutting down...")
            stop_event.set()
            try:
                if ws_app is not None:
                    ws_app.close()
            except Exception:
                pass
            break
        except Exception as e:
            print(f"[ERROR] main loop error: {e}")

        if not stop_event.is_set():
            time.sleep(retry_sleep)


if __name__ == "__main__":
    main()