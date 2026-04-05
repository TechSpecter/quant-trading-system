from typing import Optional
import pandas as pd


def _atr_target(entry: float, atr: float, multiplier: float) -> float:
    return entry + (atr * multiplier)


def _rr_target(entry: float, stop_loss: float, rr: float) -> float:
    risk = entry - stop_loss
    return entry + (risk * rr)


def _get_swing_high(df: pd.DataFrame, lookback: int) -> Optional[float]:
    if len(df) < lookback:
        return None
    try:
        return float(df.iloc[-lookback:]["high"].max())
    except Exception:
        return None


def calculate_target(
    df: pd.DataFrame, entry_price: float, stop_loss: float, config
) -> Optional[float]:

    risk_cfg = config.get("risk", {})
    atr_cfg = config.get("indicators", {}).get("atr", {})

    mode = risk_cfg.get("target_mode", "rr")

    last_row = df.iloc[-1]

    if mode == "rr":
        rr = risk_cfg.get("reward_to_risk_ratio", 2)
        return _rr_target(entry_price, stop_loss, rr)

    if mode == "atr":
        atr = last_row.get("atr") or last_row.get("ATR")
        if atr is None:
            return None
        return _atr_target(entry_price, float(atr), atr_cfg.get("target_multiplier", 3))

    if mode == "swing":
        lookback = risk_cfg.get("swing_lookback", 20)
        return _get_swing_high(df, lookback)

    return None
