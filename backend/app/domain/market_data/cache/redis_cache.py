from __future__ import annotations

from typing import Dict, Any, Optional
import pandas as pd
import json


# =========================
# CONFIG HELPERS
# =========================
def _is_cache_enabled(config: Dict[str, Any]) -> bool:
    md_cfg = config.get("market_data", {})
    cache_cfg = md_cfg.get("cache", {})
    return bool(cache_cfg.get("enabled", True))


def _get_ttl(config: Dict[str, Any]) -> int:
    md_cfg = config.get("market_data", {})
    cache_cfg = md_cfg.get("cache", {})
    ttl = cache_cfg.get("ttl_seconds", 300)

    try:
        return int(ttl)
    except Exception:
        return 300


import redis


def _get_redis_client(config: Optional[Dict[str, Any]] = None):
    # 1️⃣ Try config-based client
    if config is not None:
        client = config.get("redis_client")
        if client is not None:
            return client

    # 2️⃣ Fallback to default Redis (for auth flow / standalone usage)
    try:
        return redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    except Exception as e:
        raise ValueError(f"Redis connection failed: {e}")


def _build_cache_key(symbol: str, timeframe: str) -> str:
    return f"md:{symbol}:{timeframe}"


# =========================
# SERIALIZATION
# =========================
def _serialize_df(df: pd.DataFrame) -> str:
    try:
        return df.to_json(orient="records")
    except Exception:
        return ""


def _deserialize_df(data: str) -> pd.DataFrame:
    try:
        records = json.loads(data)
        return pd.DataFrame(records)
    except Exception:
        return pd.DataFrame()


# =========================
# GET / SET
# =========================
def get_from_cache(
    symbol: str,
    timeframe: str,
    config: Dict[str, Any],
) -> Optional[pd.DataFrame]:
    if not _is_cache_enabled(config):
        return None

    client = _get_redis_client(config)
    key = _build_cache_key(symbol, timeframe)

    try:
        data = client.get(key)
    except Exception:
        return None

    if data is None:
        return None

    # Ensure data is a string
    if isinstance(data, bytes):
        data = data.decode("utf-8")

    if not isinstance(data, str):
        return None

    df = _deserialize_df(data)

    if df.empty:
        return None

    return df


def set_to_cache(
    symbol: str,
    timeframe: str,
    df: pd.DataFrame,
    config: Dict[str, Any],
) -> None:
    if not _is_cache_enabled(config):
        return

    if df is None or df.empty:
        return

    client = _get_redis_client(config)
    key = _build_cache_key(symbol, timeframe)
    ttl = _get_ttl(config)

    data = _serialize_df(df)

    if not data:
        return

    try:
        client.set(key, data, ex=ttl)
    except Exception:
        return
