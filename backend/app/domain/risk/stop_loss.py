from typing import Optional, cast
import pandas as pd


def _get_atr(row: pd.Series) -> Optional[float]:
    val = row.get("atr") or row.get("ATR")
    return float(val) if val is not None else None


def _get_swing_low(df: pd.DataFrame, lookback: int) -> Optional[float]:
    if len(df) < lookback:
        return None
    try:
        return float(df.iloc[-lookback:]["low"].min())
    except Exception:
        return None


def _atr_stop_loss(entry: float, atr: float, multiplier: float) -> float:
    return entry - (atr * multiplier)


def calculate_stop_loss(
    df: pd.DataFrame, entry_price: float, config
) -> Optional[float]:

    risk_cfg = config.get("risk", {})
    atr_cfg = config.get("indicators", {}).get("atr", {})

    mode = risk_cfg.get("stop_loss_mode", "atr")

    last_row = df.iloc[-1]

    if mode == "atr":
        atr = _get_atr(last_row)
        if atr is None:
            return None

        multiplier = atr_cfg.get("stop_loss_multiplier", 1.5)
        return _atr_stop_loss(entry_price, atr, multiplier)

    if mode == "swing":
        lookback = risk_cfg.get("swing_lookback", 20)
        return _get_swing_low(df, lookback)

    return None
