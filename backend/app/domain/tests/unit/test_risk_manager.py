import pandas as pd
from app.domain.risk.risk_manager import evaluate_risk


def test_risk_calculation():
    df = pd.DataFrame([{"close": 100, "ATR": 10, "high": 120, "low": 90}])

    config = {
        "risk": {
            "stop_loss_mode": "atr",
            "target_mode": "rr",
            "reward_to_risk_ratio": 2,
        },
        "indicators": {"atr": {"stop_loss_multiplier": 1.5, "target_multiplier": 3}},
    }

    result = evaluate_risk(df, 100, config)

    assert result["stop_loss"] is not None
    assert result["target"] is not None
    assert result["rr"] is not None
