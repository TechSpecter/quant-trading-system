from __future__ import annotations

from datetime import datetime
from typing import Dict, Any
import pandas as pd

from app.domain.market_data.services.market_data_service import get_market_data
from app.domain.indicators.indicator_pipeline import IndicatorPipeline
from app.domain.strategies.mtf.strategy_orchestrator import StrategyOrchestrator
from app.domain.risk.risk_manager import evaluate_risk


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
        return self.data.get(f"{symbol}:{timeframe}")

    def save_candles(self, symbol: str, timeframe: str, df: pd.DataFrame):
        self.data[f"{symbol}:{timeframe}"] = df


class MockFyers:
    def history(self, payload: Dict[str, Any]):
        candles = []
        base = 1700000000

        # Generate upward trend data
        for i in range(50):
            candles.append(
                [
                    base + i * 86400,
                    100 + i,
                    105 + i,
                    95 + i,
                    102 + i,
                    1000 + i * 10,
                ]
            )

        return {"candles": candles}


# =========================
# TEST
# =========================
def test_full_strategy_pipeline():
    config = {
        "market_data": {
            "chunking": {
                "enabled": True,
                "days_per_chunk": 10,
                "max_parallel_requests": 2,
            },
            "cache": {"enabled": True, "ttl_seconds": 300},
            "db": {"min_rows_required": {"D": 10}},
        },
        "strategy": {
            "data": {
                "resolution_map": {"D": "1D"},
            },
        },
        "timeframes": {"trend": "D", "pullback": "D", "trigger": "D"},
        "indicators": {
            "ema": {"fast": 5, "short": 10, "medium": 20, "long": 50},
            "sma": {"long_term": 200},
            "rsi": {"period": 14, "pullback_range": [40, 60]},
            "volume": {"ma_period": 20, "breakout_multiplier": 1.2},
            "atr": {"period": 14, "stop_loss_multiplier": 1.5, "target_multiplier": 2},
        },
        "entry": {"pullback": {"ema_zone_buffer": 0.02}},
        "risk": {"mode": "atr", "capital_per_trade_percent": 1},
        "redis_client": MockRedis(),
        "db_client": MockDB(),
        "fyers_client": MockFyers(),
    }

    symbol = "TEST"
    timeframe = "D"
    start = datetime(2024, 1, 1)
    end = datetime(2024, 2, 20)

    # 1️⃣ Fetch market data
    df = get_market_data(symbol, timeframe, start, end, config)

    assert df is not None
    assert not df.empty

    # 2️⃣ Apply indicators
    pipeline = IndicatorPipeline(config)
    df_ind = pipeline.apply(df)

    assert df_ind is not None
    assert not df_ind.empty

    # 3️⃣ Run strategy
    mtf_data = {"D": df_ind}
    orchestrator = StrategyOrchestrator(config)
    strategy_output = orchestrator.run(mtf_data)

    assert strategy_output is not None
    assert "signal" in strategy_output

    # 4️⃣ Apply risk
    if strategy_output.get("signal") == "BUY":
        entry_price = strategy_output.get("entry")
    else:
        entry_price = None
    risk_output = evaluate_risk(df_ind, entry_price, config)

    assert risk_output is not None

    # Validate fields
    assert "stop_loss" in risk_output
    assert "target" in risk_output
    assert "rr" in risk_output
