# producers/coingecko_producer.py
import requests, json, time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def poll_markets_top15():
    while True:
        try:
            resp = requests.get(
                "https://api.coingecko.com/api/v3/coins/markets",
                params={
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": 15,
                    "page": 1,
                    "price_change_percentage": "24h",
                },
                timeout=10
            )
            resp.raise_for_status()
            rows = resp.json()

            now_ms = int(time.time() * 1000)
            for r in rows:
                msg = {
                    "exchange": "coingecko",
                    "symbol": (r.get("symbol") or "").upper() + "USDT",
                    "price": float(r["current_price"]),
                    "quantity": 0,
                    "timestamp": now_ms,
                    "market_cap": r.get("market_cap"),
                    "volume_24h": r.get("total_volume"),
                    "change_24h_pct": r.get("price_change_percentage_24h"),
                }
                producer.send("crypto-trades", msg)

            producer.flush(5)
            print(f"sent {len(rows)} coingecko market rows")

        except Exception as e:
            print(f"CoinGecko error: {e}")

        time.sleep(60)

if __name__ == "__main__":
    poll_markets_top15()