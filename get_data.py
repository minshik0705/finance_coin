import csv
from binance.client import Client
import config

client = Client(config.API_KEY, config.API_SECRET)

SYMBOL   = "BTCUSDT"                      
INTERVAL = Client.KLINE_INTERVAL_5MINUTE
START    = "1 Jan, 2020"
END      = "1 Jan, 2025"

try:
    rows = client.get_historical_klines(SYMBOL, INTERVAL, START, END)
    print(f"Fetched {len(rows)} rows")

    if not rows:
        print("No data returned. Check symbol/interval/date range.")
    else:
        with open("2020-2025.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "Open time","Open","High","Low","Close","Volume",
                "Close time","Quote asset volume","Number of trades",
                "Taker buy base asset volume","Taker buy quote asset volume","Ignore"
            ])
            w.writerows(rows)
        print("Wrote 2020-2025.csv")
except Exception as e:
    print("Error:", e)
