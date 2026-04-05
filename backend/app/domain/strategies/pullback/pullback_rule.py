import pandas as pd
from typing import cast


# Helper function for safe column access
def _get(row, key):
    return row.get(key) or row.get(key.lower())


def _check_ema_trend(ema20: float, ema50: float) -> bool:
    return ema20 > ema50


def _check_price_in_ema_zone(
    close: float, ema20: float, ema50: float, buffer: float
) -> bool:
    lower = ema50 * (1 - buffer)
    upper = ema20 * (1 + buffer)
    return lower <= close <= upper


def _check_rsi(rsi: float, rsi_low: float, rsi_high: float) -> bool:
    return rsi_low <= rsi <= rsi_high


def _check_volume(vol, vol_ma) -> bool:
    try:
        if vol is None or vol_ma is None:
            return True
        return float(vol) <= float(vol_ma)
    except Exception:
        return True


def is_pullback(df: pd.DataFrame, i: int, config) -> bool:
    if i >= len(df):
        return False

    row = df.iloc[i]

    entry_cfg = config.get("entry", {}).get("pullback", {})
    ind_cfg = config.get("indicators", {})

    ema20 = _get(row, "EMA_20")
    ema50 = _get(row, "EMA_50")
    close = _get(row, "close")
    rsi = _get(row, "RSI")
    vol = _get(row, "volume")
    vol_ma = _get(row, "VOL_MA")

    if None in [ema20, ema50, close, rsi]:
        return False

    try:
        ema20 = float(cast(float, ema20))
        ema50 = float(cast(float, ema50))
        close = float(cast(float, close))
        rsi = float(cast(float, rsi))
    except Exception:
        return False

    if not _check_ema_trend(ema20, ema50):
        return False

    buffer = entry_cfg.get("ema_zone_buffer", 0.02)
    if not _check_price_in_ema_zone(close, ema20, ema50, buffer):
        return False

    rsi_cfg = ind_cfg.get("rsi", {})
    rsi_range = rsi_cfg.get("pullback_range")

    if not rsi_range or len(rsi_range) != 2:
        return False

    rsi_low, rsi_high = rsi_range
    if not _check_rsi(rsi, rsi_low, rsi_high):
        return False

    if not _check_volume(vol, vol_ma):
        return False

    return True
