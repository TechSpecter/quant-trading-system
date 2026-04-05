from typing import Dict, Any
from typing import cast
import pandas as pd

from app.domain.strategies.trend.trend_rule import is_bull_trend
from app.domain.strategies.pullback.pullback_rule import is_pullback
from app.domain.strategies.trigger.trigger_rule import is_trigger
from app.domain.risk.risk_manager import evaluate_risk


def _get_timeframes(config):
    tf_cfg = config.get("timeframes", {})
    return (
        tf_cfg.get("trend", "D"),
        tf_cfg.get("pullback", "4H"),
        tf_cfg.get("trigger", "1H"),
    )


def _validate_df(df):
    return df is not None and not df.empty


def _get_last_index(df: pd.DataFrame) -> int:
    return len(df) - 1


class StrategyOrchestrator:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def run(self, mtf_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        trend_tf, pullback_tf, trigger_tf = _get_timeframes(self.config)

        df_trend = mtf_data.get(trend_tf)
        df_pullback = mtf_data.get(pullback_tf)
        df_trigger = mtf_data.get(trigger_tf)

        if not _validate_df(df_trend):
            return {"signal": "NO_DATA"}
        df_trend = cast(pd.DataFrame, df_trend)

        # =========================
        # GATE 1 — TREND
        # =========================
        trend_row = df_trend.iloc[-1]
        trend_pass = is_bull_trend(trend_row, self.config)

        if not trend_pass:
            return {"signal": "NO_TRADE", "stage": "TREND_FAIL"}

        # =========================
        # GATE 2 — PULLBACK
        # =========================
        if not _validate_df(df_pullback):
            return {"signal": "NO_DATA"}
        df_pullback = cast(pd.DataFrame, df_pullback)

        i = _get_last_index(df_pullback)
        pullback_pass = is_pullback(df_pullback, i, self.config)

        if not pullback_pass:
            return {"signal": "WATCH", "stage": "TREND_ONLY"}

        # =========================
        # GATE 3 — TRIGGER
        # =========================
        if not _validate_df(df_trigger):
            return {"signal": "WAIT", "stage": "SETUP"}
        df_trigger = cast(pd.DataFrame, df_trigger)

        j = _get_last_index(df_trigger)
        trigger_pass = is_trigger(df_trigger, j, self.config)

        if trigger_pass:
            last_row = df_trigger.iloc[-1]

            entry_price = last_row.get("close") or last_row.get("Close")

            risk = evaluate_risk(df_trigger, entry_price, self.config)

            return {
                "signal": "BUY",
                "stage": "TRIGGERED",
                "entry": float(entry_price) if entry_price is not None else None,
                **risk,
            }

        return {"signal": "WAIT", "stage": "SETUP"}
