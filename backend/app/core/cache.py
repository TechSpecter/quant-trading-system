from io import StringIO
import pandas as pd
from app.db.session import redis_client
from app.core.utils.market_utils import is_index


def _key(symbol: str, timeframe: str):
    return f"candles:{symbol}:{timeframe}"


async def get_cache(symbol: str, timeframe: str):
    data = await redis_client.get(_key(symbol, timeframe))

    if not data:
        return None

    try:
        json_str = (
            data.decode("utf-8") if isinstance(data, (bytes, bytearray)) else data
        )

        df = pd.read_json(StringIO(json_str))

        # Validate dataframe
        if df is None or df.empty:
            return None

        return df

    except Exception as e:
        print(f"❌ Redis GET decode error for {symbol}: {e}")
        return None


async def set_cache(symbol: str, timeframe: str, df: pd.DataFrame):
    if df is None or df.empty:
        return

    if is_index(symbol):
        df = df.copy()
        df["volume"] = None

    try:
        await redis_client.set(
            _key(symbol, timeframe),
            df.to_json(),
            ex=300,
        )

        print(f"⚡ Redis SET: {symbol}")

    except Exception as e:
        print(f"❌ Redis SET error for {symbol}: {e}")
