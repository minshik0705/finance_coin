# producers/base_producer.py
from __future__ import annotations

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import json
import time
import threading
from abc import ABC, abstractmethod

import websocket
from kafka import KafkaProducer

from config import (
    KAFKA_PRODUCER_CONFIG,
    CORE_UNIVERSE_FILE,
    DYNAMIC_UNIVERSE_FILE,
    UNIVERSE_REFRESH_SECONDS,
    TOPIC_TRADES,
)


class BaseProducer(ABC):

    EXCHANGE_NAME: str = "unknown"
    BLOCKLIST: frozenset = frozenset()

    def __init__(self):
        # [변경] 전역 변수 → 인스턴스 변수
        # 여러 거래소를 같은 프로세스에서 돌려도 서로 충돌 없음
        self.current_symbols: list = []
        self.ws_app = None
        self.stop_event  = threading.Event()
        self.reload_event = threading.Event()
        self._producer = None  # Lazy init

    # ────────────────────────────────────────
    # Kafka (Lazy initialization)
    # ────────────────────────────────────────

    @property
    def producer(self) -> KafkaProducer:
        """
        [변경] 기존: 모듈 레벨에서 즉시 KafkaProducer() 실행
               개선: 처음 send() 호출 시점에 연결
        → Kafka가 아직 안 떴어도 import만 해서는 에러 안 남
        """
        if self._producer is None:
            self._producer = KafkaProducer(
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                **KAFKA_PRODUCER_CONFIG,
            )
        return self._producer

    def send(self, topic: str, message: dict) -> None:
        """Kafka 전송 실패 시 프로세스가 죽지 않고 로그만 남김."""
        try:
            self.producer.send(topic, message)
        except Exception as e:
            print(f"[ERROR] [{self.EXCHANGE_NAME}] Kafka send failed: {e}")

    # ────────────────────────────────────────
    # Universe Loading
    # ────────────────────────────────────────

    @staticmethod
    def _safe_read_json(path: Path) -> dict | None:
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            # write 도중 read가 일어나 JSON이 깨진 경우
            print(f"[WARN] failed to read {path.name}: {e}")
            return None

    def _extract_symbols(self, path: Path) -> set:
        data = self._safe_read_json(path)
        if not data:
            return set()
        symbols = set()
        for item in data.get("membership", []):
            pair = (item.get("pair_symbol") or "").upper().strip()
            if pair:
                symbols.add(pair)
        return symbols

    def load_universe_symbols(self) -> list:
        core    = self._extract_symbols(CORE_UNIVERSE_FILE)
        dynamic = self._extract_symbols(DYNAMIC_UNIVERSE_FILE)

        merged = (core | dynamic)
        merged = {s for s in merged if s.endswith("USDT")}
        merged = {s for s in merged if s not in self.BLOCKLIST}
        return sorted(merged)

    # ────────────────────────────────────────
    # 추상 메서드 (거래소마다 다른 부분)
    # ────────────────────────────────────────

    @abstractmethod
    def build_ws_connection(self, symbols: list) -> websocket.WebSocketApp:
        """WebSocketApp 인스턴스 생성. on_open에서 subscribe 포함."""
        ...

    @abstractmethod
    def parse_message(self, message: str) -> list[dict]:
        """
        WS 메시지 파싱 → 정규화된 메시지 리스트 반환.
        제어 메시지(ping/pong/subscribe ack)는 빈 리스트 반환.
        """
        ...

    # ────────────────────────────────────────
    # WebSocket 공통 콜백
    # ────────────────────────────────────────

    def _on_message(self, ws, message: str) -> None:
        try:
            for msg in self.parse_message(message):
                self.send(TOPIC_TRADES, msg)
                print(f"[{self.EXCHANGE_NAME}] {msg['symbol']}: ${msg['price']:.4f}")
        except Exception as e:
            print(f"[ERROR] [{self.EXCHANGE_NAME}] on_message: {e}")

    def _on_error(self, ws, error) -> None:
        print(f"[ERROR] [{self.EXCHANGE_NAME}] websocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg) -> None:
        print(f"[INFO] [{self.EXCHANGE_NAME}] websocket closed: "
              f"code={close_status_code}, msg={close_msg}")

    # ────────────────────────────────────────
    # Runner / Reload (공통 main 루프)
    # ────────────────────────────────────────

    def _run_ws(self, symbols: list) -> None:
        """WS 연결 실행 (블로킹)."""
        self.current_symbols = list(symbols)

        if not self.current_symbols:
            print(f"[WARN] [{self.EXCHANGE_NAME}] no symbols. sleeping...")
            time.sleep(5)
            return

        print(f"[INFO] [{self.EXCHANGE_NAME}] connecting for "
              f"{len(self.current_symbols)} symbols")

        self.ws_app = self.build_ws_connection(self.current_symbols)
        self.ws_app.run_forever(ping_interval=20, ping_timeout=10)

    def _universe_watcher(self) -> None:
        """
        Universe 파일 변경 감지 → reload_event set.

        [변경] 기존 대비 추가된 것:
        last_good_symbols 캐싱
        → JSON write 도중 read로 빈 리스트가 오면
          이전에 성공했던 값으로 fallback
        → universe가 갑자기 ["BTCUSDT"] 하나로 쪼그라드는 상황 방지
        """
        last_symbols = None
        last_good_symbols = None  # fallback 캐시

        while not self.stop_event.is_set():
            try:
                symbols = self.load_universe_symbols()

                if not symbols:
                    # 파일 읽기 실패 → fallback
                    symbols = last_good_symbols or ["BTCUSDT"]
                    print(f"[WARN] [{self.EXCHANGE_NAME}] universe empty, "
                          f"using fallback ({len(symbols)} symbols)")
                else:
                    last_good_symbols = symbols  # 정상 읽기 성공 시 캐싱

                if last_symbols is None:
                    last_symbols = symbols
                    print(f"[INFO] [{self.EXCHANGE_NAME}] initial universe "
                          f"({len(symbols)}): {symbols[:5]}")
                elif symbols != last_symbols:
                    print(f"[INFO] [{self.EXCHANGE_NAME}] universe changed → reload")
                    print(f"       old({len(last_symbols)}): {last_symbols[:5]}")
                    print(f"       new({len(symbols)}): {symbols[:5]}")
                    last_symbols = symbols
                    self.reload_event.set()

            except Exception as e:
                print(f"[WARN] [{self.EXCHANGE_NAME}] universe watcher error: {e}")

            # sleep 대신 wait: stop_event가 오면 즉시 깨어남
            self.stop_event.wait(timeout=UNIVERSE_REFRESH_SECONDS)

    def _close_ws(self) -> None:
        try:
            if self.ws_app is not None:
                self.ws_app.close()
        except Exception as e:
            print(f"[WARN] [{self.EXCHANGE_NAME}] ws close error: {e}")

    def run(self) -> None:
        """메인 실행 루프. 각 거래소 파일의 __main__에서 호출."""
        watcher = threading.Thread(target=self._universe_watcher, daemon=True)
        watcher.start()

        retry_sleep = 3

        while not self.stop_event.is_set():
            symbols = self.load_universe_symbols() or ["BTCUSDT"]
            self.reload_event.clear()

            try:
                ws_thread = threading.Thread(
                    target=self._run_ws, args=(symbols,), daemon=True
                )
                ws_thread.start()

                while ws_thread.is_alive() and not self.stop_event.is_set():
                    if self.reload_event.is_set():
                        print(f"[INFO] [{self.EXCHANGE_NAME}] reloading websocket...")
                        self._close_ws()
                        break
                    time.sleep(1)

                ws_thread.join(timeout=5)

            except KeyboardInterrupt:
                print(f"[INFO] [{self.EXCHANGE_NAME}] shutting down...")
                self.stop_event.set()
                self._close_ws()
                break
            except Exception as e:
                print(f"[ERROR] [{self.EXCHANGE_NAME}] main loop: {e}")

            if not self.stop_event.is_set():
                time.sleep(retry_sleep)

        # 종료 시 Kafka flush
        if self._producer is not None:
            try:
                self._producer.flush(timeout=5)
                self._producer.close()
            except Exception:
                pass