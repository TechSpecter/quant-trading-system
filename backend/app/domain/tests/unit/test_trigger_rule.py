import pandas as pd
from app.domain.strategies.trigger.trigger_rule import is_trigger


def test_trigger_true():
    df = pd.DataFrame(
        [
            {"EMA_5": 100, "EMA_10": 105},
            {"EMA_5": 110, "EMA_10": 108, "volume": 2000, "VOL_MA": 1000},
        ]
    )

    config = {"indicators": {"volume": {"breakout_multiplier": 1.2}}}

    assert is_trigger(df, 1, config) is True


def test_trigger_false_no_cross():
    df = pd.DataFrame([{"EMA_5": 110, "EMA_10": 105}, {"EMA_5": 108, "EMA_10": 107}])

    config = {}

    assert is_trigger(df, 1, config) is False
