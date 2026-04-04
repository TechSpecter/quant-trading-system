from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

from app.domain.market.fyers_client import FyersAPIClient
from app.core.cache import get_cache, set_cache
from app.db.repositories.market_repository import MarketRepository

from app.core.utils.market_utils import is_index


class MarketDataService:
    """
    Orchestrates data fetching with priority:
    Redis -> DB -> Fyers API
    """

    def __init__(self, db, bypass_cache: bool = False):
        self.client = FyersAPIClient()
        self.repo = MarketRepository()
        self.db = db
        self.bypass_cache = bypass_cache

    def _cap_lookback_days(self, timeframe: str, days: int) -> int:
        """
        Fyers limitation:
        Intraday resolutions (<= 240 mins) support max 100 days.
        We cap to 90 for safety buffer.
        """
        intraday_timeframes = {
            "1",
            "2",
            "3",
            "5",
            "10",
            "15",
            "20",
            "30",
            "45",
            "60",
            "120",
            "180",
            "240",
            "1H",
            "4H",
        }

        if timeframe in intraday_timeframes:
            return min(days, 90)

        return days

    async def get_symbol_data(
        self,
        symbol: str,
        timeframe: str = "D",
        lookback_days: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        if lookback_days is None:
            raise ValueError(
                f"lookback_days must be provided via config for timeframe={timeframe}"
            )
        # -------------------------
        # 1. REDIS CACHE
        # -------------------------
        cached = None if self.bypass_cache else await get_cache(symbol, timeframe)
        if cached is not None and not cached.empty:
            return cached

        MIN_CANDLES = {
            "D": 150,
            "4H": 80,
            "1H": 120,
        }

        # -------------------------
        # 2. DATABASE
        # -------------------------
        db_data = await self.repo.get_candles(self.db, symbol, timeframe)

        if db_data is not None and not db_data.empty:
            min_required = MIN_CANDLES.get(timeframe, 80)
            rows = len(db_data)
            print(
                f"🔍 DB rows check {symbol} [{timeframe}] → rows={rows} | min_required={min_required}"
            )

            if rows < min_required:
                print(
                    f"⚠️ Using partial DB data for {symbol} [{timeframe}] (rows={rows})"
                )
            else:
                print(f"💾 DB hit: {symbol} [{timeframe}] (rows={rows})")

            # Ensure index volume is cleaned before caching
            if is_index(symbol):
                db_data = db_data.copy()
                db_data["volume"] = None

            await set_cache(symbol, timeframe, db_data)
            return db_data

        # -------------------------
        # 3. FYERS API
        # -------------------------
        # 🔥 Apply Fyers-safe lookback cap
        safe_days = self._cap_lookback_days(timeframe, lookback_days)

        print(f"📡 API fetch: {symbol} | days={safe_days} (requested={lookback_days})")

        today = datetime.utcnow()
        start = today - timedelta(days=safe_days)

        print(
            f"📊 Fyers Request → symbol={symbol}, timeframe={timeframe}, days={safe_days}"
        )

        df = await self.client.fetch_historical_data(
            symbol=symbol,
            resolution=timeframe,
            date_from=start.strftime("%Y-%m-%d"),
            date_to=today.strftime("%Y-%m-%d"),
        )

        if df is not None:
            print(f"📊 API rows: {len(df)} for {symbol}")

        if df is not None and not df.empty:
            min_required = MIN_CANDLES.get(timeframe, 80)
            rows = len(df)
            print(
                f"🔍 API rows check {symbol} [{timeframe}] → rows={rows} | min_required={min_required}"
            )

            if rows < min_required:
                print(
                    f"⚠️ Using partial API data for {symbol} [{timeframe}] (rows={rows})"
                )
            else:
                print(
                    f"✅ Sufficient API data for {symbol} [{timeframe}] (rows={rows})"
                )

            if is_index(symbol):
                df = df.copy()
                df["volume"] = None

            # Persist and cache regardless (important for MTF continuity)
            await self.repo.insert_candles(self.db, df, symbol, timeframe)
            await set_cache(symbol, timeframe, df)

        return df

    async def get_symbol_mtf_data(
        self,
        symbol: str,
        timeframes: List[str],
        lookback_config: Dict[str, int],
    ) -> Dict[str, Optional[pd.DataFrame]]:
        """
        Fetch data for multiple timeframes (MTF)
        Returns:
            {
                "D": df_daily,
                "4H": df_4h
            }
        """
        results: Dict[str, Optional[pd.DataFrame]] = {}

        for tf in timeframes:
            print(f"📊 Fetching {symbol} [{tf}]")

            if tf not in lookback_config:
                raise ValueError(f"Missing lookback_days config for timeframe={tf}")

            tf_lookback = lookback_config[tf]

            df = await self.get_symbol_data(
                symbol=symbol,
                timeframe=tf,
                lookback_days=tf_lookback,
            )

            results[tf] = df

        return results

    async def get_universe_data(
        self,
        symbols: List[str],
        timeframe: str = "D",
        lookback_days: Optional[int] = None,
    ) -> Dict[str, Optional[pd.DataFrame]]:
        results: Dict[str, Optional[pd.DataFrame]] = {}

        for symbol in symbols:
            print(f"Processing {symbol}")
            if lookback_days is None:
                raise ValueError(
                    f"lookback_days must be provided for timeframe={timeframe}"
                )

            df = await self.get_symbol_data(
                symbol=symbol,
                timeframe=timeframe,
                lookback_days=lookback_days,
            )
            results[symbol] = df

        return results

    async def get_universe_mtf_data(
        self,
        symbols: List[str],
        timeframes: List[str],
        lookback_config: Dict[str, int],
    ) -> Dict[str, Dict[str, Optional[pd.DataFrame]]]:
        """
        Fetch MTF data for entire universe
        Returns:
            {
                "NSE:RELIANCE-EQ": {
                    "D": df_daily,
                    "4H": df_4h
                }
            }
        """
        results: Dict[str, Dict[str, Optional[pd.DataFrame]]] = {}

        for symbol in symbols:
            print(f"🚀 Processing MTF for {symbol}")

            mtf_data = await self.get_symbol_mtf_data(
                symbol=symbol,
                timeframes=timeframes,
                lookback_config=lookback_config,
            )

            results[symbol] = mtf_data

        return results
