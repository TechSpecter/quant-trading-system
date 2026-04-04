import yaml
from typing import List, Dict, Any

from app.domain.market.market_data_service import MarketDataService
from app.domain.strategies.indicators import IndicatorEngine
from app.domain.strategies.mtf_engine import MTFEngine


class StrategyService:
    def __init__(self, db):
        self.db = db
        self.config = self._load_config()

        self.market_data_service = MarketDataService(db)
        self.indicator_engine = IndicatorEngine(self.config)
        self.mtf_engine = MTFEngine(self.config)

    # =========================
    # CONFIG LOADER
    # =========================

    def _load_config(self) -> Dict[str, Any]:
        """Load strategy config using absolute path"""
        import os

        # Navigate to project root (quant-trading-system)
        base_dir = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        )

        config_path = os.path.join(base_dir, "config", "strategy.yaml")

        print("📁 Loading config from:", config_path)

        with open(config_path, "r") as f:
            return yaml.safe_load(f)

    # =========================
    # SINGLE SYMBOL
    # =========================

    async def process_symbol(self, symbol: str, timeframe: str = "D") -> Dict[str, Any]:
        """
        Fetch data → apply indicators → generate signal
        """

        df = await self.market_data_service.get_symbol_data(symbol, timeframe)

        if df is None or df.empty:
            return {"symbol": symbol, "signal": "NO_DATA"}

        # Apply indicators
        df = self.indicator_engine.apply_indicators(df)

        # Generate signal
        result = self.mtf_engine.generate_signal(df)

        result["symbol"] = symbol

        return result

    # =========================
    # MULTIPLE SYMBOLS
    # =========================

    async def process_universe(
        self, symbols: List[str], timeframe: str = "D"
    ) -> List[Dict[str, Any]]:
        """
        Process full universe
        """

        results = []

        for symbol in symbols:
            print(f"Processing {symbol}")

            res = await self.process_symbol(symbol, timeframe)
            results.append(res)

        return results
