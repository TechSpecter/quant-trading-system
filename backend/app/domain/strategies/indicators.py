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

        gain = pd.Series(np.where(delta > 0, delta, 0), index=series.index)
        loss = pd.Series(np.where(delta < 0, -delta, 0), index=series.index)

        gain_ema = gain.ewm(alpha=1 / period, adjust=False).mean()
        loss_ema = loss.ewm(alpha=1 / period, adjust=False).mean()

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

        # Ensure sorted by time
        if "timestamp" in df.columns:
            df = df.sort_values("timestamp").reset_index(drop=True)

        df_original = None  # will assign after indicators

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

        if "volume" in df.columns and df["volume"].notna().any():
            df["VOL_MA"] = self.volume_ma(df["volume"], vol_period)
        else:
            df["VOL_MA"] = np.nan

        # Backup AFTER all indicators are computed (ensures fallback has indicator cols)
        df_original = df.copy()

        # 🔥 SAFE warmup (do not kill dataset)
        raw_min_period = max(sma_period, atr_period, ema_cfg["long"])

        # Cap warmup to 20% of data (more lenient for 4H)
        min_period = min(raw_min_period, int(len(df) * 0.2))

        # Ensure we always keep sufficient rows
        min_period = max(min_period, 5)

        if len(df) <= min_period:
            min_period = max(1, int(len(df) * 0.1))

        df = df.iloc[min_period:].reset_index(drop=True)

        # 🔥 FINAL CLEANUP: remove rows where key indicators are NaN
        required_cols = [
            f"SMA_{sma_period}",
            "EMA_20",
            "RSI",
        ]

        cleaned_df = df.dropna(subset=required_cols)

        if cleaned_df.empty:
            print("⚠️ Indicator output empty after cleanup — using fallback slice")
            # ensure df_original exists
            if df_original is None:
                df_original = df.copy()
            fallback_size = max(50, int(len(df_original) * 0.3))
            df = df_original.tail(fallback_size).reset_index(drop=True)
        else:
            df = cleaned_df

        if df is None or df.empty:
            print("❌ Indicator returned empty — forcing minimal fallback")
            if df_original is None:
                df_original = df.copy() if df is not None else pd.DataFrame()
            df = df_original.tail(10).reset_index(drop=True)

        return df
