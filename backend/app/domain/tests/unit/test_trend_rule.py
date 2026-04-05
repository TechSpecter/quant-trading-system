import pandas as pd
from app.domain.strategies.trend.trend_rule import is_bull_trend


def test_bull_trend_true():
    row = pd.Series({"close": 120, "SMA_200": 100, "EMA_50": 110, "EMA_20": 115})

    config = {
        "trend": {
            "rules": [
                {"left": "close", "operator": ">", "right": "SMA_200"},
                {"left": "EMA_50", "operator": ">", "right": "SMA_200"},
                {"left": "EMA_20", "operator": ">", "right": "EMA_50"},
            ]
        }
    }

    assert is_bull_trend(row, config) is True


def test_bull_trend_false():
    row = pd.Series({"close": 90, "SMA_200": 100})

    config = {
        "trend": {
            "rules": [
                {"left": "close", "operator": ">", "right": "SMA_200"},
            ]
        }
    }

    assert is_bull_trend(row, config) is False
