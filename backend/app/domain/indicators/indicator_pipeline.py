from __future__ import annotations

from typing import Dict, Any
import pandas as pd

from app.domain.indicators.ema import calculate_ema
from app.domain.indicators.sma import calculate_sma
from app.domain.indicators.rsi import calculate_rsi
from app.domain.indicators.atr import calculate_atr
from app.domain.indicators.volume import calculate_volume_ma


# =========================
# HELPERS
# =========================
def _is_valid_df(df: pd.DataFrame | None) -> bool:
    return df is not None and not df.empty


def _get_indicator_config(config: Dict[str, Any], key: str) -> Dict[str, Any]:
    return config.get("indicators", {}).get(key, {})


# =========================
# APPLY FUNCTIONS (SMALL UNITS)
# =========================
def _apply_ema(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    ema_cfg = _get_indicator_config(config, "ema")
    if not ema_cfg:
        return df

    fast = int(ema_cfg.get("fast", 5))
    short = int(ema_cfg.get("short", 10))
    medium = int(ema_cfg.get("medium", 20))
    long = int(ema_cfg.get("long", 50))

    df["EMA_5"] = calculate_ema(df, fast)
    df["EMA_10"] = calculate_ema(df, short)
    df["EMA_20"] = calculate_ema(df, medium)
    df["EMA_50"] = calculate_ema(df, long)

    return df


def _apply_sma(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    sma_cfg = _get_indicator_config(config, "sma")
    if not sma_cfg:
        return df

    period = int(sma_cfg.get("long_term", 200))
    df["SMA_200"] = calculate_sma(df, period)

    return df


def _apply_rsi(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    rsi_cfg = _get_indicator_config(config, "rsi")
    if not rsi_cfg:
        return df

    period = int(rsi_cfg.get("period", 14))
    df["RSI"] = calculate_rsi(df, period)

    return df


def _apply_atr(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    atr_cfg = _get_indicator_config(config, "atr")
    if not atr_cfg:
        return df

    period = int(atr_cfg.get("period", 14))
    df["ATR"] = calculate_atr(df, period)

    return df


def _apply_volume(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    vol_cfg = _get_indicator_config(config, "volume")
    if not vol_cfg:
        return df

    period = int(vol_cfg.get("ma_period", 20))
    df["VOL_MA"] = calculate_volume_ma(df, period)

    return df


# =========================
# MAIN PIPELINE
# =========================
class IndicatorPipeline:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def apply(self, df: pd.DataFrame | None) -> pd.DataFrame:
        if not _is_valid_df(df):
            return pd.DataFrame()

        df = df.copy() if df is not None else pd.DataFrame()

        # Apply indicators step by step
        df = _apply_ema(df, self.config)
        df = _apply_sma(df, self.config)
        df = _apply_rsi(df, self.config)
        df = _apply_atr(df, self.config)
        df = _apply_volume(df, self.config)

        return df
