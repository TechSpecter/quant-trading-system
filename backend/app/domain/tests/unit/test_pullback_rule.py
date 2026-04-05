import pandas as pd
from app.domain.strategies.pullback.pullback_rule import is_pullback


def test_pullback_true():
    df = pd.DataFrame(
        [
            {
                "EMA_20": 110,
                "EMA_50": 100,
                "close": 105,
                "RSI": 45,
                "volume": 1000,
                "VOL_MA": 2000,
            }
        ]
    )

    config = {
        "entry": {"pullback": {"ema_zone_buffer": 0.02}},
        "indicators": {"rsi": {"pullback_range": [40, 60]}},
    }

    assert is_pullback(df, 0, config) is True


def test_pullback_false_rsi():
    df = pd.DataFrame(
        [
            {
                "EMA_20": 110,
                "EMA_50": 100,
                "close": 105,
                "RSI": 70,
                "volume": 1000,
                "VOL_MA": 2000,
            }
        ]
    )

    config = {
        "entry": {"pullback": {"ema_zone_buffer": 0.02}},
        "indicators": {"rsi": {"pullback_range": [40, 60]}},
    }

    assert is_pullback(df, 0, config) is False
