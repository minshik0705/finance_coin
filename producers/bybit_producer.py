# producers/bybit_producer.py
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import json
import websocket

from base_producer import BaseProducer
from config import BYBIT_BLOCKLIST

BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/spot"


class BybitProducer(BaseProducer):

    EXCHANGE_NAME = "bybit"
    BLOCKLIST = BYBIT_BLOCKLIST

    # ────────────────────────────────────────
    # WS 연결
    # ────────────────────────────────────────

    def build_ws_connection(self, symbols: list) -> websocket.WebSocketApp:
        """
        Bybit는 고정 URL에 연결 후
        on_open에서 subscribe 메시지를 보내는 방식.
        (Binance처럼 URL에 심볼 목록을 넣지 않음)
        """
        return websocket.WebSocketApp(
            BYBIT_WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,  # BaseProducer 공통 콜백
            on_error=self._on_error,      # BaseProducer 공통 콜백
            on_close=self._on_close,      # BaseProducer 공통 콜백
        )

    def _on_open(self, ws) -> None:
        print(f"[INFO] [{self.EXCHANGE_NAME}] websocket opened "
              f"for {len(self.current_symbols)} symbols")
        print(f"[INFO] preview: {self.current_symbols[:5]}")
        self._send_subscribe(ws)

    def _send_subscribe(self, ws) -> None:
        """
        Bybit 구독 메시지 전송.
        topic 형식: "publicTrade.BTCUSDT"
        """
        topics = [f"publicTrade.{s}" for s in self.current_symbols]
        ws.send(json.dumps({
            "op":   "subscribe",
            "args": topics,
        }))
        print(f"[INFO] [{self.EXCHANGE_NAME}] subscribe sent "
              f"for {len(topics)} symbols")

    # ────────────────────────────────────────
    # 메시지 파싱
    # ────────────────────────────────────────

    def parse_message(self, message: str) -> list[dict]:
        """
        Bybit v5 public spot trade 메시지 파싱.

        형식:
        {
            "topic": "publicTrade.BTCUSDT",
            "type": "snapshot",
            "ts": 1710000000000,
            "data": [
                {
                    "T": 1710000000000,   ← trade time (ms)
                    "s": "BTCUSDT",
                    "p": "68001.4",
                    "v": "0.003",
                    "S": "Buy"            ← side
                }
            ]
        }
        """
        payload = json.loads(message)

        # ping/pong/subscribe ack 무시
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
                "side":         item.get("S"),  # Buy / Sell
                "topic":        topic,
            })

        return results


if __name__ == "__main__":
    BybitProducer().run()