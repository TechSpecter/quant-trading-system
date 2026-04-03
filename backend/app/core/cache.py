import pandas as pd
from app.db.session import redis_client


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
        return pd.read_json(json_str)
    except Exception:
        return None


async def set_cache(symbol: str, timeframe: str, df: pd.DataFrame):
    await redis_client.set(_key(symbol, timeframe), df.to_json(), ex=300)
