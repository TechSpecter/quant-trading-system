from sqlalchemy import text
import pandas as pd
from app.core.utils.market_utils import is_index


class MarketRepository:
    """
    Handles DB operations for market candle data.
    """

    async def insert_candles(self, db, df: pd.DataFrame, symbol: str, timeframe: str):
        if df is None or df.empty:
            return

        query = text(
            """
            INSERT INTO candles (
                timestamp,
                symbol,
                timeframe,
                open,
                high,
                low,
                close,
                volume
            ) VALUES (
                :timestamp,
                :symbol,
                :timeframe,
                :open,
                :high,
                :low,
                :close,
                :volume
            )
            ON CONFLICT DO NOTHING
        """
        )

        is_idx = is_index(symbol)

        for r in df.to_dict("records"):
            await db.execute(
                query,
                {
                    "timestamp": r["timestamp"],
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "open": r["open"],
                    "high": r["high"],
                    "low": r["low"],
                    "close": r["close"],
                    "volume": None if is_idx else r["volume"],
                },
            )

        await db.commit()

    async def get_candles(self, db, symbol: str, timeframe: str, limit: int = 200):
        query = text(
            """
            SELECT
                timestamp,
                open,
                high,
                low,
                close,
                volume
            FROM candles
            WHERE symbol = :symbol
              AND timeframe = :timeframe
            ORDER BY timestamp DESC
            LIMIT :limit
        """
        )

        result = await db.execute(
            query, {"symbol": symbol, "timeframe": timeframe, "limit": limit}
        )

        rows = result.fetchall()

        if not rows:
            return None

        df = pd.DataFrame(rows)
        return df
