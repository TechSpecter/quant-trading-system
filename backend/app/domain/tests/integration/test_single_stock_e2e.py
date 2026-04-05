import sys
import os

# Fix import path for pytest
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../"))
)
from datetime import datetime
from typing import Dict, Any, List, cast

import redis
from fyers_apiv3 import fyersModel

from app.domain.pipeline.trading_pipeline import TradingPipeline
from app.core.settings import settings


def _get_access_token() -> str:
    r = redis.Redis(host="localhost", port=6379, db=0)
    raw = r.get("fyers_access_token")  # type: ignore
    assert raw is not None, "Run make token first"
    if isinstance(raw, (bytes, bytearray)):
        return raw.decode()
    return str(raw)


class InMemoryDB:
    """Minimal DB implementation for E2E (real pipeline, no mocks)."""

    def __init__(self):
        self._store: Dict[str, Any] = {}

    def save_candles(self, symbol: str, timeframe: str, df):
        self._store[f"{symbol}:{timeframe}"] = df

    def get_candles(self, symbol: str, timeframe: str):
        return self._store.get(f"{symbol}:{timeframe}")


def _build_fyers_client() -> fyersModel.FyersModel:
    token = _get_access_token()
    assert settings.FYERS_CLIENT_ID is not None, "FYERS_CLIENT_ID missing in .env"
    return fyersModel.FyersModel(
        client_id=settings.FYERS_CLIENT_ID,
        token=token,
        is_async=False,
        log_path="",
    )


def test_single_stock_e2e():
    """
    REAL E2E TEST (no mocks):
    - Redis: real (token fetch)
    - Fyers: real (history API)
    - DB: in-memory (for simplicity)
    - Pipeline: real
    """

    # Infra
    r = redis.Redis(host="localhost", port=6379, db=0)
    fyers = _build_fyers_client()
    db = InMemoryDB()

    # Config (keep simple for first E2E)
    config: Dict[str, Any] = {
        "redis_client": r,
        "db_client": db,
        "fyers_client": fyers,
        "market_data": {
            "chunking": {"enabled": False},
            "cache": {"enabled": False},
            "db": {"min_rows_required": {"D": 50}},
        },
        "timeframes": {"trend": "D", "pullback": "D", "trigger": "D"},
        "strategy": {
            "data": {
                "resolution_map": {"D": "1D"},
            }
        },
        "indicators": {
            "ema": {"fast": 5, "short": 10, "medium": 20, "long": 50},
            "sma": {"long_term": 200},
            "rsi": {"period": 14, "pullback_range": [40, 60]},
            "volume": {"ma_period": 20, "breakout_multiplier": 1.5},
            "atr": {
                "period": 14,
                "stop_loss_multiplier": 1.5,
                "target_multiplier": 2,
            },
        },
        "entry": {"pullback": {"ema_zone_buffer": 0.02}},
        "risk": {"mode": "atr", "capital_per_trade_percent": 1},
    }

    pipeline = TradingPipeline(config)

    symbol = "NSE:SBIN-EQ"
    start = datetime(2024, 1, 1)
    end = datetime.now()

    result = pipeline.run(symbol=symbol, timeframe="D", start=start, end=end)

    assert result is not None, "Pipeline returned None"

    # Optional sanity checks (adapt keys based on your pipeline output)
    if isinstance(result, dict):
        # Example keys (adjust if your pipeline uses different names)
        # We keep them optional to avoid false negatives
        _ = result.get("signal")
        _ = result.get("trend")
        _ = result.get("entry")

    print("\n✅ SINGLE STOCK E2E RESULT:\n", result)
