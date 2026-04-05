from typing import Dict, Any, Optional
from datetime import datetime
import pandas as pd

from app.domain.market_data.services.market_data_service import get_market_data
from app.domain.indicators.indicator_pipeline import IndicatorPipeline
from app.domain.strategies.mtf.strategy_orchestrator import StrategyOrchestrator
from app.domain.risk.risk_manager import evaluate_risk


def _validate_dataframe(df: Optional[pd.DataFrame]) -> bool:
    return df is not None and not df.empty


def _prepare_mtf_data(
    df: pd.DataFrame, config: Dict[str, Any]
) -> Dict[str, pd.DataFrame]:
    """
    Prepares MTF structure. For now same DF reused.
    Later can split per timeframe.
    """
    tf_cfg = config.get("timeframes", {})
    trend_tf = tf_cfg.get("trend", "D")
    pullback_tf = tf_cfg.get("pullback", "4H")
    trigger_tf = tf_cfg.get("trigger", "1H")

    return {
        trend_tf: df,
        pullback_tf: df,
        trigger_tf: df,
    }


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

        # =========================
        # STEP 1: Fetch Market Data
        # =========================
        df = get_market_data(symbol, timeframe, start, end, self.config)

        if not _validate_dataframe(df):
            return {"signal": "NO_DATA"}

        df = df.copy()

        # =========================
        # STEP 2: Apply Indicators
        # =========================
        df_ind = self.indicator_pipeline.apply(df)

        if not _validate_dataframe(df_ind):
            return {"signal": "NO_DATA"}

        # =========================
        # STEP 3: Prepare MTF Data
        # =========================
        mtf_data = _prepare_mtf_data(df_ind, self.config)

        # =========================
        # STEP 4: Run Strategy
        # =========================
        strategy_output = self.strategy.run(mtf_data)

        # =========================
        # STEP 5: Apply Risk
        # =========================
        entry_price = _extract_entry_price(strategy_output)

        risk_output = evaluate_risk(df_ind, entry_price, self.config)

        # =========================
        # FINAL OUTPUT
        # =========================
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            **strategy_output,
            **risk_output,
        }
