# producers/binance_producer.py
import websocket
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode()
)

def on_message(ws, message):
    data = json.loads(message)
    
    # 일반 형식으로 정규화
    normalized = {
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "timestamp": data["E"]   # milliseconds
    }
    
    producer.send("crypto-trades", normalized)
    print(f"Binance: ${normalized['price']:.2f}")

ws = websocket.WebSocketApp(
    "wss://stream.binance.com:9443/ws/btcusdt@trade",
    on_message=on_message
)
ws.run_forever()