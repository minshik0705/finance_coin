# producers/bybit_producer.py
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import json
import time
import requests
import websocket

from base_producer import BaseProducer
from config import BYBIT_BLOCKLIST

BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/spot"


class BybitProducer(BaseProducer):

    EXCHANGE_NAME = "bybit"
    BLOCKLIST = BYBIT_BLOCKLIST

    def load_universe_symbols(self) -> list:
        """Bybit 실제 상장 심볼과 교집합만 구독"""
        symbols = super().load_universe_symbols()
        listed = self._get_bybit_listed_symbols()
        if listed:
            symbols = [s for s in symbols if s in listed]
            print(f"[INFO] [bybit] 필터링 후 {len(symbols)}개 심볼")
        return symbols

    @staticmethod
    def _get_bybit_listed_symbols() -> set:
        try:
            resp = requests.get(
                "https://api.bybit.com/v5/market/instruments-info",
                params={"category": "spot"},
                timeout=10
            )
            data = resp.json()
            symbols = set()
            for item in data["result"]["list"]:
                if item["quoteCoin"] == "USDT" and item["status"] == "Trading":
                    symbols.add(item["symbol"])
            print(f"[INFO] [bybit] 상장 심볼 {len(symbols)}개 조회 완료")
            return symbols
        except Exception as e:
            print(f"[WARN] [bybit] 심볼 목록 조회 실패: {e}")
            return set()

    # ────────────────────────────────────────
    # WS 연결
    # ────────────────────────────────────────

    def build_ws_connection(self, symbols: list) -> websocket.WebSocketApp:
        return websocket.WebSocketApp(
            BYBIT_WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

    def _run_ws(self, symbols: list) -> None:
        """Bybit는 ping_interval=0 (클라이언트 ping 비활성화)"""
        self.current_symbols = list(symbols)

        if not self.current_symbols:
            print(f"[WARN] [{self.EXCHANGE_NAME}] no symbols. sleeping...")
            time.sleep(5)
            return

        print(f"[INFO] [{self.EXCHANGE_NAME}] connecting for "
              f"{len(self.current_symbols)} symbols")

        self.ws_app = self.build_ws_connection(self.current_symbols)
        self.ws_app.run_forever(ping_interval=0)

    def _on_open(self, ws) -> None:
        print(f"[INFO] [{self.EXCHANGE_NAME}] websocket opened "
              f"for {len(self.current_symbols)} symbols")
        print(f"[INFO] preview: {self.current_symbols[:5]}")
        self._send_subscribe(ws)

    def _send_subscribe(self, ws) -> None:
        topics = [f"publicTrade.{s}" for s in self.current_symbols]

        chunk_size = 10
        for i in range(0, len(topics), chunk_size):
            chunk = topics[i:i + chunk_size]
            ws.send(json.dumps({
                "op":   "subscribe",
                "args": chunk,
            }))
            print(f"[INFO] [{self.EXCHANGE_NAME}] subscribe sent "
                  f"{i+1}~{i+len(chunk)} / {len(topics)}")

    # ────────────────────────────────────────
    # 메시지 파싱
    # ────────────────────────────────────────

    def parse_message(self, message: str) -> list[dict]:
        payload = json.loads(message)

        if payload.get("op") in {"pong", "ping", "subscribe"}:
            return []

        data_list = payload.get("data")
        if not isinstance(data_list, list):
            return []

        topic = payload.get("topic", "")
        results = []

        for item in data_list:
            symbol = item.get("s")
            price  = item.get("p")
            qty    = item.get("v")
            ts     = item.get("T")

            if not symbol or price is None or qty is None or ts is None:
                continue

            results.append({
                "exchange":     "bybit",
                "symbol":       symbol,
                "price":        float(price),
                "quantity":     float(qty),
                "timestamp":    int(ts),
                "source_type":  "trade",
                "base_symbol":  symbol[:-4] if symbol.endswith("USDT") else symbol,
                "quote_symbol": "USDT" if symbol.endswith("USDT") else None,
                "side":         item.get("S"),
                "topic":        topic,
            })

        return results


if __name__ == "__main__":
    BybitProducer().run()
