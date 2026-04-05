import pandas as pd
from typing import cast


def _get(row, key):
    return row.get(key) or row.get(key.lower())


def _check_ema_cross(
    prev_ema5: float, prev_ema10: float, curr_ema5: float, curr_ema10: float
) -> bool:
    return prev_ema5 <= prev_ema10 and curr_ema5 > curr_ema10


def _check_volume_breakout(vol: float, vol_ma: float, multiplier: float) -> bool:
    return vol >= vol_ma * multiplier


def is_trigger(df: pd.DataFrame, i: int, config) -> bool:
    if i == 0 or i >= len(df):
        return False

    row = df.iloc[i]
    prev = df.iloc[i - 1]

    ind_cfg = config.get("indicators", {})

    prev_ema5 = _get(prev, "EMA_5")
    prev_ema10 = _get(prev, "EMA_10")
    curr_ema5 = _get(row, "EMA_5")
    curr_ema10 = _get(row, "EMA_10")

    if any(
        v is None or pd.isna(v) for v in [prev_ema5, prev_ema10, curr_ema5, curr_ema10]
    ):
        return False

    # 🔥 Explicit type narrowing for Pylance
    prev_ema5 = float(cast(float, prev_ema5))
    prev_ema10 = float(cast(float, prev_ema10))
    curr_ema5 = float(cast(float, curr_ema5))
    curr_ema10 = float(cast(float, curr_ema10))

    prev_ema5_f = prev_ema5
    prev_ema10_f = prev_ema10
    curr_ema5_f = curr_ema5
    curr_ema10_f = curr_ema10

    if not _check_ema_cross(prev_ema5_f, prev_ema10_f, curr_ema5_f, curr_ema10_f):
        return False

    # Volume confirmation
    vol = _get(row, "volume")
    vol_ma = _get(row, "VOL_MA")

    if vol is None or vol_ma is None or pd.isna(vol) or pd.isna(vol_ma):
        return True  # allow trigger if volume missing

    try:
        multiplier = ind_cfg.get("volume", {}).get("breakout_multiplier", 1.2)
        vol_f = float(vol)
        vol_ma_f = float(vol_ma)

        if not _check_volume_breakout(vol_f, vol_ma_f, multiplier):
            return False
    except (TypeError, ValueError):
        return True

    return True
