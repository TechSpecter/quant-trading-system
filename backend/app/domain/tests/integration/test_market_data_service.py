from __future__ import annotations

from datetime import datetime
from typing import Dict, Any
import pandas as pd

from app.domain.market_data.services.market_data_service import get_market_data


# =========================
# MOCKS
# =========================
class MockRedis:
    def __init__(self):
        self.store: Dict[str, str] = {}

    def get(self, key: str):
        return self.store.get(key)

    def set(self, key: str, value: str, ex: int = 0):
        self.store[key] = value


class MockDB:
    def __init__(self):
        self.data: Dict[str, pd.DataFrame] = {}

    def get_candles(self, symbol: str, timeframe: str):
        key = f"{symbol}:{timeframe}"
        return self.data.get(key)

    def save_candles(self, symbol: str, timeframe: str, df: pd.DataFrame):
        key = f"{symbol}:{timeframe}"
        self.data[key] = df


class MockFyers:
    def history(self, payload: Dict[str, Any]):
        # Return 5 rows dummy candles
        candles = []
        base_ts = 1700000000

        for i in range(5):
            candles.append(
                [
                    base_ts + i * 60,
                    100 + i,
                    105 + i,
                    95 + i,
                    102 + i,
                    1000 + i,
                ]
            )

        return {"candles": candles}


# =========================
# TEST
# =========================
def test_market_data_full_flow():
    config = {
        "market_data": {
            "chunking": {
                "enabled": True,
                "days_per_chunk": 2,
                "max_parallel_requests": 2,
            },
            "cache": {"enabled": True, "ttl_seconds": 300},
            "db": {"min_rows_required": {"D": 1}},
        },
        "strategy": {
            "data": {
                "resolution_map": {"D": "1D"},
            }
        },
        "redis_client": MockRedis(),
        "db_client": MockDB(),
        "fyers_client": MockFyers(),
    }

    symbol = "TEST"
    timeframe = "D"

    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 5)

    # First call → should hit fyers → save DB → cache
    df = get_market_data(symbol, timeframe, start, end, config)

    assert df is not None
    assert not df.empty
    assert len(df) > 0

    # Second call → should hit cache (no failure expected)
    df_cached = get_market_data(symbol, timeframe, start, end, config)

    assert df_cached is not None
    assert not df_cached.empty

    # DB should also have data
    db_client = config["db_client"]
    db_df = db_client.get_candles(symbol, timeframe)

    assert db_df is not None
    assert not db_df.empty
