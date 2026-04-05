from typing import Any, Dict

import pandas as pd

from app.domain.indicators.atr import calculate_atr
from app.domain.indicators.ema import calculate_ema
from app.domain.indicators.rsi import calculate_rsi
from app.domain.indicators.sma import calculate_sma
from app.domain.indicators.volume import calculate_volume_ma


class IndicatorPipeline:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.ind_cfg = config.get("indicators", {})

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df

        df = df.copy()

        # =========================
        # CLEAN BASE DATA
        # =========================
        df = df.loc[:, ~df.columns.duplicated()].copy()

        # Ensure lowercase columns (STANDARDIZATION 🔥)
        df.columns = [str(c).lower() for c in df.columns]

        # Ensure required columns exist
        required_cols = ["open", "high", "low", "close", "volume"]
        for col in required_cols:
            if col not in df.columns:
                df[col] = pd.NA

        # Fill missing safely (FIX for your earlier error 🔥)
        df = df.ffill().bfill()

        # =========================
        # SMA
        # =========================
        sma_cfg = self.ind_cfg.get("sma", {})
        sma_period = sma_cfg.get("long_term", 200)

        df["sma_200"] = calculate_sma(df, sma_period)

        # =========================
        # EMA
        # =========================
        ema_cfg = self.ind_cfg.get("ema", {})

        df["ema_5"] = calculate_ema(df, ema_cfg.get("fast", 5))
        df["ema_10"] = calculate_ema(df, ema_cfg.get("short", 10))
        df["ema_20"] = calculate_ema(df, ema_cfg.get("medium", 20))
        df["ema_50"] = calculate_ema(df, ema_cfg.get("long", 50))

        # =========================
        # RSI
        # =========================
        rsi_cfg = self.ind_cfg.get("rsi", {})
        df["rsi"] = calculate_rsi(df, rsi_cfg.get("period", 14))

        # =========================
        # ATR
        # =========================
        atr_cfg = self.ind_cfg.get("atr", {})
        df["atr"] = calculate_atr(df, atr_cfg.get("period", 14))

        # =========================
        # VOLUME
        # =========================
        vol_cfg = self.ind_cfg.get("volume", {})
        df["vol_ma"] = calculate_volume_ma(df, vol_cfg.get("ma_period", 20))

        # =========================
        # FINAL CLEANUP
        # =========================
        df = df.loc[:, ~df.columns.duplicated()].copy()
        df = df.ffill().bfill()

        print(f"📊 Indicator output rows: {len(df)}")

        return df
