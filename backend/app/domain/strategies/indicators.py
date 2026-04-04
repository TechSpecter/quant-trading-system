import pandas as pd
import numpy as np
from typing import Dict, Any


class IndicatorEngine:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.ind_cfg = config.get("indicators", {})

    # =========================
    # BASIC INDICATORS
    # =========================

    def sma(self, series: pd.Series, period: int) -> pd.Series:
        return series.rolling(window=period).mean()

    def ema(self, series: pd.Series, period: int) -> pd.Series:
        return series.ewm(span=period, adjust=False).mean()

    def rsi(self, series: pd.Series, period: int) -> pd.Series:
        delta = series.diff()
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)

        gain_ema = pd.Series(gain).ewm(alpha=1 / period, adjust=False).mean()
        loss_ema = pd.Series(loss).ewm(alpha=1 / period, adjust=False).mean()

        rs = gain_ema / (loss_ema + 1e-9)
        return 100 - (100 / (1 + rs))

    def atr(self, df: pd.DataFrame, period: int) -> pd.Series:
        high_low = df["high"] - df["low"]
        high_close = (df["high"] - df["close"].shift()).abs()
        low_close = (df["low"] - df["close"].shift()).abs()

        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return tr.rolling(window=period).mean()

    def volume_ma(self, series: pd.Series, period: int) -> pd.Series:
        return series.rolling(window=period).mean()

    # =========================
    # APPLY ALL INDICATORS
    # =========================

    def apply_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enhance dataframe with all indicators"""

        df = df.copy()

        # --- SMA ---
        sma_period = self.ind_cfg["sma"]["long_term"]
        df[f"SMA_{sma_period}"] = self.sma(df["close"], sma_period)

        # --- EMA ---
        ema_cfg = self.ind_cfg["ema"]
        df["EMA_5"] = self.ema(df["close"], ema_cfg["fast"])
        df["EMA_10"] = self.ema(df["close"], ema_cfg["short"])
        df["EMA_20"] = self.ema(df["close"], ema_cfg["medium"])
        df["EMA_50"] = self.ema(df["close"], ema_cfg["long"])

        # --- RSI ---
        rsi_period = self.ind_cfg["rsi"]["period"]
        df["RSI"] = self.rsi(df["close"], rsi_period)

        # --- ATR ---
        atr_period = self.ind_cfg["atr"]["period"]
        df["ATR"] = self.atr(df, atr_period)

        # --- Volume MA ---
        vol_period = self.ind_cfg["volume"]["ma_period"]

        if "volume" in df.columns:
            df["VOL_MA"] = self.volume_ma(df["volume"], vol_period)
        else:
            df["VOL_MA"] = None

        return df
