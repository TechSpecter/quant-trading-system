import pandas as pd
from typing import Dict, Any


class MTFEngine:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.ind_cfg = config.get("indicators", {})
        self.entry_cfg = config.get("entry", {})
        self.state_cfg = config.get("state", {})

    # =========================
    # HELPER FUNCTIONS
    # =========================

    def is_bull_trend(self, row: pd.Series) -> bool:
        sma_col = f"SMA_{self.ind_cfg['sma']['long_term']}"
        return row["close"] > row[sma_col]

    def is_pullback(self, df: pd.DataFrame, i: int) -> bool:
        row = df.iloc[i]

        ema20 = row["EMA_20"]
        ema50 = row["EMA_50"]
        close = row["close"]
        rsi = row["RSI"]
        vol = row.get("volume", None)
        vol_ma = row.get("VOL_MA", None)

        # Condition 1: EMA20 > EMA50
        if ema20 <= ema50:
            return False

        # Condition 2: Price near EMA zone
        buffer = self.entry_cfg["pullback"]["ema_zone_buffer"]
        lower = ema50 * (1 - buffer)
        upper = ema20 * (1 + buffer)

        if not (lower <= close <= upper):
            return False

        # Condition 3: RSI in pullback range
        rsi_low, rsi_high = self.ind_cfg["rsi"]["pullback_range"]
        if not (rsi_low <= rsi <= rsi_high):
            return False

        # Condition 4: Volume dry-up (optional for indices)
        if vol is not None and vol_ma is not None:
            if vol > vol_ma:
                return False

        return True

    def is_trigger(self, df: pd.DataFrame, i: int) -> bool:
        if i == 0:
            return False

        row = df.iloc[i]
        prev = df.iloc[i - 1]

        # EMA crossover
        cross = prev["EMA_5"] <= prev["EMA_10"] and row["EMA_5"] > row["EMA_10"]

        if not cross:
            return False

        # Volume confirmation
        vol = row.get("volume", None)
        vol_ma = row.get("VOL_MA", None)

        if vol is not None and vol_ma is not None:
            multiplier = self.ind_cfg["volume"]["breakout_multiplier"]
            if vol < vol_ma * multiplier:
                return False

        return True

    # =========================
    # MAIN ENGINE
    # =========================

    def generate_signal(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Process dataframe and return latest signal
        """

        state = "IDLE"
        signal = "HOLD"
        entry_price = None
        stop_loss = None

        for i in range(len(df)):
            row = df.iloc[i]

            # --- Gate 1: Macro Trend ---
            if not self.is_bull_trend(row):
                state = "IDLE"
                continue

            # --- Gate 2: Setup ---
            if self.is_pullback(df, i):
                state = "SETUP"

            # --- Gate 3: Trigger ---
            if state == "SETUP" and self.is_trigger(df, i):
                state = "TRIGGERED"
                signal = "BUY"
                entry_price = row["close"]

                # Stop loss = ATR based
                atr = row.get("ATR", None)
                if atr:
                    multiplier = self.ind_cfg["atr"]["stop_loss_multiplier"]
                    stop_loss = entry_price - (multiplier * atr)

        return {
            "signal": signal,
            "state": state,
            "entry_price": entry_price,
            "stop_loss": stop_loss,
        }
