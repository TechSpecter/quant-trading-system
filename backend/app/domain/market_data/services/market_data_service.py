from __future__ import annotations

from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
import pandas as pd

from app.domain.market_data.cache.redis_cache import (
    get_from_cache,
    set_to_cache,
)
from app.domain.market_data.storage.db_repository import (
    get_data_from_db,
    save_data_to_db,
)
from app.domain.market_data.providers.fyers_provider import fetch_candles
from app.domain.market_data.utils.chunker import generate_chunks
from app.domain.market_data.utils.parallel_executor import run_parallel


DateRange = Tuple[datetime, datetime]


# =========================
# HELPERS
# =========================
def _merge_dataframes(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    valid = [df for df in dfs if df is not None and not df.empty]

    if not valid:
        return pd.DataFrame()

    df = pd.concat(valid, ignore_index=True)

    # Remove duplicates (based on timestamp if exists)
    if "timestamp" in df.columns:
        df = df.drop_duplicates(subset=["timestamp"])

    # Sort by timestamp if present
    if "timestamp" in df.columns:
        df = df.sort_values(by="timestamp")

    df = df.reset_index(drop=True)

    return df


def _build_tasks(
    symbol: str,
    timeframe: str,
    chunks: List[DateRange],
    config: Dict[str, Any],
):
    tasks = []
    for start, end in chunks:
        tasks.append((symbol, timeframe, start, end, config))
    return tasks


def _fetch_chunk(task) -> pd.DataFrame:
    try:
        symbol, timeframe, start, end, config = task
        df = fetch_candles(symbol, timeframe, start, end, config)
        return _normalize_dataframe(df)
    except Exception:
        return pd.DataFrame()


def _normalize_dataframe(df: pd.DataFrame | dict) -> pd.DataFrame:
    # Handle raw fyers response
    if isinstance(df, dict):
        candles = df.get("candles", [])
        if not candles:
            return pd.DataFrame()

        try:
            df = pd.DataFrame(
                candles,
                columns=["timestamp", "open", "high", "low", "close", "volume"],
            )
        except Exception:
            return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    expected_cols = ["timestamp", "open", "high", "low", "close", "volume"]

    if list(df.columns) != expected_cols:
        try:
            df = pd.DataFrame(df.values, columns=expected_cols)
        except Exception:
            return pd.DataFrame()

    return df


# =========================
# CORE FLOW
# =========================
def _fetch_from_source(
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    config: Dict[str, Any],
) -> pd.DataFrame:
    chunks = generate_chunks(start, end, config)

    # 🔥 FIX: if chunking fails or returns empty → fallback direct fetch
    if not chunks:
        df = fetch_candles(symbol, timeframe, start, end, config)
        return _normalize_dataframe(df)

    tasks = _build_tasks(symbol, timeframe, chunks, config)

    results = run_parallel(_fetch_chunk, tasks, config)

    # 🔥 FIX: fallback if parallel returns empty
    if not results:
        df = fetch_candles(symbol, timeframe, start, end, config)
        return _normalize_dataframe(df)

    normalized_results = [_normalize_dataframe(df) for df in results]
    df = _merge_dataframes(normalized_results)

    # 🔥 FIX: final fallback if merge failed
    if df is None or df.empty:
        df = fetch_candles(symbol, timeframe, start, end, config)
        return _normalize_dataframe(df)

    return df


# =========================
# PUBLIC API
# =========================
def get_market_data(
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    config: Dict[str, Any],
) -> pd.DataFrame:
    """
    Full pipeline:
    1. Redis
    2. DB
    3. Fyers (chunk + parallel)
    4. Save DB
    5. Cache Redis
    """

    # 1️⃣ Cache
    df_cache = get_from_cache(symbol, timeframe, config)
    if df_cache is not None and not df_cache.empty:
        return df_cache

    # 2️⃣ DB
    df_db = get_data_from_db(symbol, timeframe, config)
    if df_db is not None and not df_db.empty:
        set_to_cache(symbol, timeframe, df_db, config)
        return df_db

    # 3️⃣ Source (Fyers)
    df_source = _fetch_from_source(symbol, timeframe, start, end, config)

    if df_source is None or df_source.empty:
        return pd.DataFrame()

    # 4️⃣ Save DB
    save_data_to_db(symbol, timeframe, df_source, config)

    # 5️⃣ Cache
    set_to_cache(symbol, timeframe, df_source, config)

    return df_source
