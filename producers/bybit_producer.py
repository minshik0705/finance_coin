# producers/bybit_producer.py
import websocket
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode()
)

def on_open(ws):
    # Bybit requires a subscription message
    subscribe_msg = {
        "op": "subscribe",
        "args": ["publicTrade.BTCUSDT"]
    }
    ws.send(json.dumps(subscribe_msg))
    print("Subscribed to Bybit trades")

def on_message(ws, message):
    data = json.loads(message)
    
    if "data" not in data:
        return
        
    for trade in data["data"]:
        normalized = {
            "exchange": "bybit",
            "symbol": "BTCUSDT",
            "price": float(trade["p"]),
            "quantity": float(trade["v"]),
            "timestamp": int(trade["T"])
        }
        producer.send("crypto-trades", normalized)
        print(f"Bybit:   ${normalized['price']:.2f}")

ws = websocket.WebSocketApp(
    "wss://stream.bybit.com/v5/public/spot",
    on_open=on_open,
    on_message=on_message
)
ws.run_forever()