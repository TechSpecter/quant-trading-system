from typing import Dict, Any, Optional
from datetime import datetime
import pandas as pd

from app.domain.market_data.services.market_data_service import get_market_data
from app.domain.indicators.indicator_pipeline import IndicatorPipeline
from app.domain.strategies.mtf.strategy_orchestrator import StrategyOrchestrator
from app.domain.risk.risk_manager import evaluate_risk


def _validate_dataframe(df: Optional[pd.DataFrame]) -> bool:
    return df is not None and not df.empty


def _get_timeframes(config: Dict[str, Any]) -> Dict[str, str]:
    tf_cfg = config.get("timeframes", {})
    return {
        "trend": tf_cfg.get("trend", "D"),
        "pullback": tf_cfg.get("pullback", "4H"),
        "trigger": tf_cfg.get("trigger", "1H"),
    }


def _fetch_single_tf(
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    config: Dict[str, Any],
) -> Optional[pd.DataFrame]:
    df = get_market_data(symbol, timeframe, start, end, config)
    if not _validate_dataframe(df):
        return None
    return df.copy()


def _apply_indicators_to_df(
    df: pd.DataFrame, indicator_pipeline: IndicatorPipeline
) -> Optional[pd.DataFrame]:
    df_ind = indicator_pipeline.apply(df)
    if not _validate_dataframe(df_ind):
        return None
    return df_ind


def _fetch_mtf_data(
    symbol: str,
    start: datetime,
    end: datetime,
    config: Dict[str, Any],
    indicator_pipeline: IndicatorPipeline,
) -> Dict[str, pd.DataFrame]:

    timeframes = _get_timeframes(config)

    mtf_data: Dict[str, pd.DataFrame] = {}

    for _, tf in timeframes.items():
        df = _fetch_single_tf(symbol, tf, start, end, config)
        if df is None:
            continue

        df_ind = _apply_indicators_to_df(df, indicator_pipeline)
        if df_ind is None:
            continue

        mtf_data[tf] = df_ind

    return mtf_data


def _extract_entry_price(result: Dict[str, Any]) -> Optional[float]:
    entry = result.get("entry") or result.get("entry_price")
    try:
        return float(entry) if entry is not None else None
    except Exception:
        return None


class TradingPipeline:
    """
    End-to-end trading pipeline:

    1. Fetch Market Data
    2. Apply Indicators
    3. Run Strategy (MTF)
    4. Apply Risk Management

    This becomes the SINGLE ENTRY POINT for:
    - Backtesting
    - Live Trading
    - API Layer
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.indicator_pipeline = IndicatorPipeline(config)
        self.strategy = StrategyOrchestrator(config)

    def run(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> Dict[str, Any]:

        mtf_data = _fetch_mtf_data(
            symbol,
            start,
            end,
            self.config,
            self.indicator_pipeline,
        )

        if not mtf_data:
            return {"signal": "NO_DATA"}

        # =========================
        # STEP 4: Run Strategy
        # =========================
        strategy_output = self.strategy.run(mtf_data)

        # =========================
        # STEP 5: Apply Risk
        # =========================
        entry_price = _extract_entry_price(strategy_output)

        # Use trigger timeframe for risk (more precise)
        trigger_tf = _get_timeframes(self.config).get("trigger", "1H")
        df_trigger = mtf_data.get(trigger_tf)

        if not _validate_dataframe(df_trigger):
            return {**strategy_output, "stop_loss": None, "target": None, "rr": None}

        # Type narrowing for Pylance
        df_trigger = df_trigger if isinstance(df_trigger, pd.DataFrame) else None
        if df_trigger is None:
            return {**strategy_output, "stop_loss": None, "target": None, "rr": None}

        risk_output = evaluate_risk(df_trigger, entry_price, self.config)

        # =========================
        # FINAL OUTPUT
        # =========================
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            **strategy_output,
            **risk_output,
        }
