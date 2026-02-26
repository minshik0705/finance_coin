# producers/okx_producer.py
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import json
import websocket

from base_producer import BaseProducer
from config import OKX_BLOCKLIST

OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"


class OKXProducer(BaseProducer):

    EXCHANGE_NAME = "okx"
    BLOCKLIST = OKX_BLOCKLIST

    def __init__(self):
        super().__init__()
        # OKX 구독 시 "BTC-USDT" 형식이 필요하므로 별도 관리
        self._okx_inst_ids: list = []

    # ────────────────────────────────────────
    # 심볼 변환 (OKX 고유)
    # ────────────────────────────────────────

    @staticmethod
    def _to_okx(symbol: str) -> str | None:
        """
        내부 표준 → OKX 형식
        BTCUSDT → BTC-USDT
        """
        if symbol.endswith("USDT"):
            return f"{symbol[:-4]}-USDT"
        return None

    @staticmethod
    def _from_okx(inst_id: str) -> str | None:
        """
        OKX 형식 → 내부 표준
        BTC-USDT → BTCUSDT
        """
        if not inst_id:
            return None
        return inst_id.replace("-", "").upper()

    # ────────────────────────────────────────
    # WS 연결
    # ────────────────────────────────────────

    def build_ws_connection(self, symbols: list) -> websocket.WebSocketApp:
        """
        고정 URL 연결 후 on_open에서 subscribe.
        연결 전에 OKX 형식으로 변환해서 캐싱해둠.
        """
        self._okx_inst_ids = [
            inst_id
            for s in symbols
            if (inst_id := self._to_okx(s)) is not None
        ]

        return websocket.WebSocketApp(
            OKX_WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,  # BaseProducer 공통 콜백
            on_error=self._on_error,      # BaseProducer 공통 콜백
            on_close=self._on_close,      # BaseProducer 공통 콜백
        )

    def _on_open(self, ws) -> None:
        print(f"[INFO] [{self.EXCHANGE_NAME}] websocket opened "
              f"for {len(self.current_symbols)} symbols")
        print(f"[INFO] instIds preview: {self._okx_inst_ids[:5]}")
        self._send_subscribe(ws)

    def _send_subscribe(self, ws) -> None:
        """
        OKX 구독 메시지 전송.

        형식:
        {
            "op": "subscribe",
            "args": [
                {"channel": "trades", "instId": "BTC-USDT"},
                {"channel": "trades", "instId": "ETH-USDT"},
                ...
            ]
        }
        """
        args = [
            {"channel": "trades", "instId": inst_id}
            for inst_id in self._okx_inst_ids
        ]
        ws.send(json.dumps({"op": "subscribe", "args": args}))
        print(f"[INFO] [{self.EXCHANGE_NAME}] subscribe sent "
              f"for {len(args)} symbols")

    # ────────────────────────────────────────
    # 메시지 파싱
    # ────────────────────────────────────────

    def parse_message(self, message: str) -> list[dict]:
        """
        OKX public trades 메시지 파싱.

        형식:
        {
            "arg": {"channel": "trades", "instId": "BTC-USDT"},
            "data": [
                {
                    "instId": "BTC-USDT",
                    "px":     "68001.4",   ← price
                    "sz":     "0.003",     ← size (quantity)
                    "side":   "buy",
                    "ts":     "1710000000000"
                }
            ]
        }
        """
        payload = json.loads(message)

        # subscribe ack / error 이벤트 무시
        if "event" in payload:
            event = payload.get("event")
            if event in {"subscribe", "error"}:
                print(f"[INFO] [{self.EXCHANGE_NAME}] event: {payload}")
            return []

        data_list = payload.get("data")
        if not isinstance(data_list, list):
            return []

        arg             = payload.get("arg", {})
        channel         = arg.get("channel")
        inst_id_from_arg = arg.get("instId")

        results = []

        for item in data_list:
            inst_id  = item.get("instId") or inst_id_from_arg
            price    = item.get("px")
            qty      = item.get("sz")
            trade_ts = item.get("ts")

            if not inst_id or price is None or qty is None or trade_ts is None:
                continue

            symbol = self._from_okx(inst_id)
            if not symbol:
                continue

            results.append({
                "exchange":     "okx",
                "symbol":       symbol,        # 내부 표준 (BTCUSDT)
                "price":        float(price),
                "quantity":     float(qty),
                "timestamp":    int(trade_ts),
                "source_type":  "trade",
                "base_symbol":  symbol[:-4] if symbol.endswith("USDT") else symbol,
                "quote_symbol": "USDT" if symbol.endswith("USDT") else None,
                "side":         item.get("side"),  # buy / sell
                "okx_inst_id":  inst_id,           # BTC-USDT (원본)
                "channel":      channel,
            })

        return results


if __name__ == "__main__":
    OKXProducer().run()