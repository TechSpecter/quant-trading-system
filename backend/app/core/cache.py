from io import StringIO
from typing import Optional
import pandas as pd

from app.db.session import redis_client
from app.core.utils.market_utils import is_index


MIN_REQUIRED_ROWS = 200  # for SMA200 safety


def _key(symbol: str, timeframe: str) -> str:
    # Include timeframe explicitly to avoid collisions across MTF
    return f"candles:{symbol}:{timeframe}"


async def get_cache(
    symbol: str,
    timeframe: str,
    bypass_cache: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Read from Redis cache with safety guards.
    - bypass_cache=True will skip Redis entirely
    - ignores cache if rows are insufficient (< MIN_REQUIRED_ROWS)
    """

    if bypass_cache:
        print(f"⛔ Bypass cache enabled for {symbol} [{timeframe}]")
        return None

    data = await redis_client.get(_key(symbol, timeframe))

    if not data:
        return None

    try:
        json_str = (
            data.decode("utf-8") if isinstance(data, (bytes, bytearray)) else data
        )

        df = pd.read_json(StringIO(json_str))

        # 🔥 Critical validation
        if df is None or df.empty:
            print(f"⚠️ Empty cache ignored for {symbol} [{timeframe}]")
            return None

        if len(df) < MIN_REQUIRED_ROWS:
            print(
                f"⚠️ Insufficient cache ignored for {symbol} [{timeframe}] (rows={len(df)})"
            )
            return None

        print(f"⚡ Redis hit: {symbol} [{timeframe}] (rows={len(df)})")
        return df

    except Exception as e:
        print(f"❌ Redis GET decode error for {symbol} [{timeframe}]: {e}")
        return None


async def set_cache(
    symbol: str,
    timeframe: str,
    df: pd.DataFrame,
    ttl_seconds: int = 300,
):
    """
    Write to Redis cache with normalization.
    - Sets volume=None for index symbols
    """

    if df is None or df.empty:
        return

    # Avoid caching insufficient data
    if len(df) < MIN_REQUIRED_ROWS:
        print(
            f"⚠️ Skipping cache (insufficient rows) for {symbol} [{timeframe}] (rows={len(df)})"
        )
        return

    try:
        if is_index(symbol):
            df = df.copy()
            df["volume"] = None

        await redis_client.set(
            _key(symbol, timeframe),
            df.to_json(),
            ex=ttl_seconds,
        )

        print(f"⚡ Redis SET: {symbol} [{timeframe}] (rows={len(df)})")

    except Exception as e:
        print(f"❌ Redis SET error for {symbol} [{timeframe}]: {e}")
