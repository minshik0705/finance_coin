import os
from dotenv import load_dotenv

load_dotenv() 

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

# ── 추가: Kafka ────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

KAFKA_PRODUCER_CONFIG = {
    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    "buffer_memory":      33_554_432,
    "max_block_ms":        5_000,
    "request_timeout_ms": 10_000,
    "retries":             3,
    "linger_ms":           10,
}

# ── 추가: Topics ───────────────────
TOPIC_TRADES    = os.getenv("TOPIC_TRADES",    "crypto-trades")
TOPIC_SNAPSHOTS = os.getenv("TOPIC_SNAPSHOTS", "crypto-market-snapshots")

# ── 추가: Universe 파일 경로 ────────
from pathlib import Path
_BASE_DIR = Path(__file__).resolve().parent

CORE_UNIVERSE_FILE          = _BASE_DIR / "producers" / "core_universe.json"
DYNAMIC_UNIVERSE_FILE       = _BASE_DIR / "producers" / "dynamic_universe.json"
DYNAMIC_UNIVERSE_STATE_FILE = _BASE_DIR / "producers" / "dynamic_universe_state.json"

# ── 추가: Universe 주기 ─────────────
UNIVERSE_REFRESH_SECONDS = int(os.getenv("UNIVERSE_REFRESH_SECONDS", 30))

# ── 추가: CoinGecko ─────────────────
COINGECKO_POLL_SECONDS        = int(os.getenv("COINGECKO_POLL_SECONDS",      60))
COINGECKO_REBALANCE_SECONDS   = int(os.getenv("COINGECKO_REBALANCE_SECONDS", 86400))
COINGECKO_MARKET_FETCH_N      = int(os.getenv("COINGECKO_MARKET_FETCH_N",    50))
COINGECKO_DYNAMIC_TOP_N       = int(os.getenv("COINGECKO_DYNAMIC_TOP_N",     15))
COINGECKO_VS_CURRENCY         = os.getenv("COINGECKO_VS_CURRENCY",           "usd")
COINGECKO_EXCLUDE_STABLECOINS = os.getenv("COINGECKO_EXCLUDE_STABLECOINS", "true").lower() == "true"
COINGECKO_STABLE_SYMBOLS      = frozenset({"USDT", "USDC", "DAI", "USDS", "TUSD", "FDUSD", "USDE"})

# ── 추가: Exchange Blocklists ───────
BINANCE_BLOCKLIST = frozenset()
BYBIT_BLOCKLIST   = frozenset()
OKX_BLOCKLIST     = frozenset()