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

    def __init__(self, db):
        self.client = FyersAPIClient()
        self.repo = MarketRepository()
        self.db = db

    async def get_symbol_data(
        self,
        symbol: str,
        timeframe: str = "D",
        lookback_days: int = 30,
    ) -> Optional[pd.DataFrame]:
        # -------------------------
        # 1. REDIS CACHE
        # -------------------------
        cached = await get_cache(symbol, timeframe)
        if cached is not None and not cached.empty:
            print(f"⚡ Redis hit: {symbol}")
            return cached

        # -------------------------
        # 2. DATABASE
        # -------------------------
        db_data = await self.repo.get_candles(self.db, symbol, timeframe)
        if db_data is not None and not db_data.empty:
            print(f"💾 DB hit: {symbol}")

            # Ensure index volume is cleaned before caching
            if is_index(symbol):
                db_data = db_data.copy()
                db_data["volume"] = None

            await set_cache(symbol, timeframe, db_data)
            return db_data

        # -------------------------
        # 3. FYERS API
        # -------------------------
        print(f"🌐 Fetching from API: {symbol}")

        today = datetime.now()
        start = today - timedelta(days=lookback_days)

        df = await self.client.fetch_historical_data(
            symbol=symbol,
            resolution=timeframe,
            date_from=start.strftime("%Y-%m-%d"),
            date_to=today.strftime("%Y-%m-%d"),
        )

        if df is not None and not df.empty:
            if is_index(symbol):
                df = df.copy()
                df["volume"] = None

            # Persist and cache for ALL symbols
            await self.repo.insert_candles(self.db, df, symbol, timeframe)
            await set_cache(symbol, timeframe, df)

        return df

    async def get_universe_data(
        self,
        symbols: List[str],
        timeframe: str = "D",
        lookback_days: int = 30,
    ) -> Dict[str, Optional[pd.DataFrame]]:
        results: Dict[str, Optional[pd.DataFrame]] = {}

        for symbol in symbols:
            print(f"Processing {symbol}")
            df = await self.get_symbol_data(
                symbol=symbol,
                timeframe=timeframe,
                lookback_days=lookback_days,
            )
            results[symbol] = df

        return results
