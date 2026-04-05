import sys
from pathlib import Path

# Ensure project root (backend/) is on PYTHONPATH for Pylance
ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from datetime import datetime
from typing import Dict, Any

from colorama import Fore, Style, init

from app.domain.pipeline.trading_pipeline import TradingPipeline
import redis
from fyers_apiv3 import fyersModel
from app.core.settings import settings


class InMemoryDB:
    def __init__(self):
        self._store: Dict[str, Any] = {}

    def save_candles(self, symbol: str, timeframe: str, df):
        self._store[f"{symbol}:{timeframe}"] = df

    def get_candles(self, symbol: str, timeframe: str):
        return self._store.get(f"{symbol}:{timeframe}")


def get_db_client():
    return InMemoryDB()


# Initialize colorama
init(autoreset=True)


def _print_header():
    print("\n" + "=" * 70)
    print(Fore.CYAN + Style.BRIGHT + "🚀 FULL STRATEGY PIPELINE (E2E TEST)")
    print("=" * 70)


def _print_row(result: Dict[str, Any]):
    signal = result.get("signal", "NA")
    stage = result.get("stage", "NA")

    signal_color = Fore.GREEN if signal == "BUY" else Fore.YELLOW
    if signal == "NO_TRADE":
        signal_color = Fore.RED

    print(
        f"{Fore.WHITE}Symbol: {Fore.CYAN}{result.get('symbol', 'NA')}  | "
        f"{Fore.WHITE}Signal: {signal_color}{signal}  | "
        f"{Fore.WHITE}Stage: {Fore.MAGENTA}{stage}"
    )

    print(
        f"{Fore.WHITE}Entry: {Fore.YELLOW}{result.get('entry')}  | "
        f"{Fore.WHITE}SL: {Fore.RED}{result.get('stop_loss')}  | "
        f"{Fore.WHITE}Target: {Fore.GREEN}{result.get('target')}  | "
        f"{Fore.WHITE}RR: {Fore.CYAN}{result.get('rr')}"
    )

    print("-" * 70)


def test_full_strategy_pipeline_real():
    """
    ⚠️ REAL E2E TEST (NO MOCKS)

    Requirements:
    - Fyers API configured
    - Redis running
    - DB configured
    """

    # Initialize real clients (config driven)
    redis_client = get_redis_client()
    db_client = get_db_client()

    raw_token = redis_client.get("fyers_access_token")  # type: ignore
    assert raw_token is not None, "Run make token first"

    access_token = (
        raw_token.decode()
        if isinstance(raw_token, (bytes, bytearray))
        else str(raw_token)
    )
    assert settings.FYERS_CLIENT_ID is not None, "FYERS_CLIENT_ID missing in .env"
    fyers_client = fyersModel.FyersModel(
        client_id=settings.FYERS_CLIENT_ID,
        token=access_token,
        is_async=False,
        log_path="",
    )

    config = {
        "redis_client": redis_client,
        "db_client": db_client,
        "fyers_client": fyers_client,
        "market_data": {
            "chunking": {
                "enabled": True,
                "days_per_chunk": 30,
                "max_parallel_requests": 3,
            },
            "cache": {"enabled": True, "ttl_seconds": 300},
            "db": {"min_rows_required": {"D": 50, "4H": 50, "1H": 50}},
        },
        "timeframes": {"trend": "D", "pullback": "4H", "trigger": "1H"},
        "strategy": {
            "data": {
                "resolution_map": {"D": "1D", "4H": "240", "1H": "60"},
            }
        },
        "indicators": {
            "ema": {"fast": 5, "short": 10, "medium": 20, "long": 50},
            "sma": {"long_term": 200},
            "rsi": {"period": 14, "pullback_range": [40, 60]},
            "volume": {"ma_period": 20, "breakout_multiplier": 1.5},
            "atr": {"period": 14, "stop_loss_multiplier": 1.5, "target_multiplier": 2},
        },
        "entry": {"pullback": {"ema_zone_buffer": 0.02}},
        "risk": {"mode": "atr", "capital_per_trade_percent": 1},
    }

    pipeline = TradingPipeline(config)

    symbols = [
        "NSE:RELIANCE-EQ",
        "NSE:TCS-EQ",
        "NSE:INFY-EQ",
    ]

    start = datetime(2024, 1, 1)
    end = datetime.now()

    _print_header()

    results = []

    for symbol in symbols:
        try:
            result = pipeline.run(symbol, "D", start, end)
            if result:
                result["symbol"] = symbol
                results.append(result)
        except Exception as e:
            print(Fore.RED + f"❌ Error processing {symbol}: {e}")

    print(Fore.BLUE + Style.BRIGHT + "\n📊 RESULTS SUMMARY\n")

    for res in results:
        _print_row(res)

    print("=" * 70 + "\n")

    # Basic assertion
    assert len(results) > 0


def get_redis_client():
    return redis.Redis(host="localhost", port=6379, db=0)
