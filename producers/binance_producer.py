# producers/binance_producer.py
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import json
import websocket

from base_producer import BaseProducer
from config import BINANCE_BLOCKLIST

BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream?streams="


class BinanceProducer(BaseProducer):

    EXCHANGE_NAME = "binance"
    BLOCKLIST = BINANCE_BLOCKLIST

    # ────────────────────────────────────────
    # WS 연결
    # ────────────────────────────────────────

    def build_ws_connection(self, symbols: list) -> websocket.WebSocketApp:
        """
        Binance combined stream URL 생성 후 WebSocketApp 반환.
        예) wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade
        """
        streams = "/".join([f"{s.lower()}@trade" for s in symbols])
        url = BINANCE_WS_BASE + streams

        return websocket.WebSocketApp(
            url,
            on_open=self._on_open,
            on_message=self._on_message,  # BaseProducer 공통 콜백
            on_error=self._on_error,      # BaseProducer 공통 콜백
            on_close=self._on_close,      # BaseProducer 공통 콜백
        )

    def _on_open(self, ws) -> None:
        # Binance combined stream은 URL에 이미 심볼이 포함되어 있어서
        # on_open에서 별도 subscribe 메시지 전송 불필요
        print(f"[INFO] [{self.EXCHANGE_NAME}] websocket opened "
              f"for {len(self.current_symbols)} symbols")
        print(f"[INFO] preview: {self.current_symbols[:5]}")

    # ────────────────────────────────────────
    # 메시지 파싱
    # ────────────────────────────────────────

    def parse_message(self, message: str) -> list[dict]:
        """
        Binance combined stream 메시지 파싱.

        combined stream 형식:
        {
            "stream": "btcusdt@trade",
            "data": {"s": "BTCUSDT", "p": "68001.4", "q": "0.003", "E": 1710000000000}
        }

        단일 stream 형식 (fallback):
        {"s": "BTCUSDT", "p": "68001.4", "q": "0.003", "E": 1710000000000}
        """
        payload = json.loads(message)

        # combined / 단일 stream 둘 다 지원
        data = payload.get("data", payload)

        symbol     = data.get("s")
        price      = data.get("p")
        qty        = data.get("q")
        event_time = data.get("E")

        if not symbol or price is None or qty is None or event_time is None:
            return []

        return [{
            "exchange":     "binance",
            "symbol":       symbol,
            "price":        float(price),
            "quantity":     float(qty),
            "timestamp":    int(event_time),
            "source_type":  "trade",
            "base_symbol":  symbol[:-4] if symbol.endswith("USDT") else symbol,
            "quote_symbol": "USDT" if symbol.endswith("USDT") else None,
        }]


if __name__ == "__main__":
    BinanceProducer().run()