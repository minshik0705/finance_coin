# producers/okx_producer.py
import websocket
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode()
)

def on_open(ws):
    subscribe_msg = {
        "op": "subscribe",
        "args": [{"channel": "trades", "instId": "BTC-USDT"}]
    }
    ws.send(json.dumps(subscribe_msg))
    print("Subscribed to OKX trades")

def on_message(ws, message):
    data = json.loads(message)
    
    if "data" not in data:
        return
    
    for trade in data["data"]:
        normalized = {
            "exchange": "okx",
            "symbol": "BTCUSDT",
            "price": float(trade["px"]),
            "quantity": float(trade["sz"]),
            "timestamp": int(trade["ts"])
        }
        producer.send("crypto-trades", normalized)
        print(f"OKX:     ${normalized['price']:.2f}")

ws = websocket.WebSocketApp(
    "wss://ws.okx.com:8443/ws/v5/public",
    on_open=on_open,
    on_message=on_message
)
ws.run_forever()