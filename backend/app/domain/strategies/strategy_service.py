import yaml
from typing import List, Dict, Any, Optional

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

    def _get_mtf_config(self):
        strategy_cfg = self.config.get("strategy", {})
        timeframes_cfg = strategy_cfg.get("timeframes", {})
        data_cfg = strategy_cfg.get("data", {})

        primary = timeframes_cfg.get("primary", "D")
        confirmation = timeframes_cfg.get("confirmation", "4H")

        lookback_config = data_cfg.get("lookback", {"D": 365, "4H": 90})

        return {
            "timeframes": [primary, confirmation],
            "lookback_config": lookback_config,
        }

    async def _get_market_trend(self) -> Optional[bool]:
        """Check if market (e.g., NIFTY50) is in uptrend"""
        market_cfg = self.config.get("market_filter", {})

        if not market_cfg.get("enabled", False):
            return None

        symbol = market_cfg.get("symbol")
        sma_period = market_cfg.get("sma_period", 200)

        try:
            df = await self.market_data_service.get_symbol_mtf_data(
                symbol=symbol,
                timeframes=["D"],
                lookback_config={"D": 365},
            )

            df_daily = df.get("D")

            if df_daily is None or df_daily.empty:
                return None

            df_daily = self.indicator_engine.apply_indicators(df_daily)

            last = df_daily.iloc[-1]
            sma_col = f"SMA_{sma_period}"

            close = last.get("close")
            sma = last.get(sma_col)

            if close is None or sma is None:
                return None

            return close > sma

        except Exception as e:
            print(f"⚠️ Market filter error: {e}")
            return None

    # =========================
    # SINGLE SYMBOL
    # =========================

    async def process_symbol(
        self, symbol: str, timeframe: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetch MTF data → apply indicators → generate signal
        """

        mtf_cfg = self._get_mtf_config()
        timeframes = mtf_cfg["timeframes"]
        lookback_config = mtf_cfg["lookback_config"]

        mtf_data = await self.market_data_service.get_symbol_mtf_data(
            symbol=symbol,
            timeframes=timeframes,
            lookback_config=lookback_config,
        )

        print(f"\n🔍 STRATEGY DEBUG: {symbol}")
        for tf, df in mtf_data.items():
            if df is None:
                print(f"   {tf}: None")
            else:
                print(f"   {tf}: rows={len(df)}")

        if not mtf_data or all(df is None or df.empty for df in mtf_data.values()):
            return {"symbol": symbol, "signal": "NO_DATA"}

        # Apply indicators per timeframe
        for tf, df in mtf_data.items():
            if df is not None and not df.empty:
                mtf_data[tf] = self.indicator_engine.apply_indicators(df)

        # 🔥 FIX: Do NOT over-filter data (this was causing NO_DATA)
        mtf_data_clean = {}

        for tf, df in mtf_data.items():
            if df is None:
                print(f"⚠️ Dropping {tf}: None")
                continue

            try:
                rows = len(df)
            except Exception:
                print(f"⚠️ Dropping {tf}: invalid dataframe")
                continue

            # 🔥 DEBUG: inspect last row to understand why it's considered empty
            try:
                last_row = df.tail(1).to_dict("records")[0]
                print(f"🔎 {tf} last row sample: {last_row}")
            except Exception as e:
                print(f"⚠️ Could not inspect {tf}: {e}")

            if rows == 0:
                print(f"⚠️ Dropping {tf}: empty")
                continue

            # 🔥 IMPORTANT: DO NOT drop even if indicators are NaN
            print(f"✅ Keeping {tf}: rows={rows}")
            mtf_data_clean[tf] = df

        # 🔥 Relaxed validation: allow partial timeframe data
        if len(mtf_data_clean) == 0:
            print(f"❌ No usable data for {symbol}")
            return {"symbol": symbol, "signal": "NO_DATA"}

        if len(mtf_data_clean) < len(timeframes):
            print(f"⚠️ Partial timeframe data for {symbol} → continuing")

        print(
            f"🚀 Passing data to MTF Engine for {symbol}: {list(mtf_data_clean.keys())}"
        )

        # Generate MTF signal
        result = self.mtf_engine.generate_signal(mtf_data_clean)

        result["symbol"] = symbol

        return result

    # =========================
    # MULTIPLE SYMBOLS
    # =========================

    async def process_universe(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """
        Process full universe using MTF
        """

        results = []

        # 🔥 Evaluate market condition once
        market_trend = await self._get_market_trend()

        # Store for UI (test_strategy)
        self.market_trend = market_trend

        if market_trend is not None:
            print(
                f"📊 Market (NIFTY50) Trend: {'BULLISH' if market_trend else 'BEARISH'}"
            )

        for symbol in symbols:
            print(f"Processing {symbol}")

            res = await self.process_symbol(symbol)

            # 🔥 Apply market filter (Option 2: downgrade, not skip)
            if market_trend is False:
                if res.get("signal") in ["BUY", "WEAK_BUY"]:
                    res["signal"] = "HOLD"
                    res["state"] = "AVOID"

                    existing_reason = res.get("reason")

                    if existing_reason:
                        res["reason"] = f"{existing_reason} | Market (NIFTY50) bearish"
                    else:
                        res["reason"] = "Market (NIFTY50) bearish"

            results.append(res)

        return results
