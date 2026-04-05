from __future__ import annotations

from typing import Dict, Any, Optional
import pandas as pd


# =========================
# CONFIG HELPERS
# =========================
def _get_min_rows(timeframe: str, config: Dict[str, Any]) -> int:
    md_cfg = config.get("market_data", {})
    db_cfg = md_cfg.get("db", {})
    min_rows_map = db_cfg.get("min_rows_required", {})

    value = min_rows_map.get(timeframe, 0)

    try:
        return int(value)
    except Exception:
        return 0


def _get_db_client(config: Dict[str, Any]):
    """
    Expect DB client injected via config
    """
    client = config.get("db_client")
    if client is None:
        raise ValueError("DB client not found in config")
    return client


# =========================
# FETCH FROM DB
# =========================
def _fetch_from_db(
    symbol: str,
    timeframe: str,
    config: Dict[str, Any],
) -> Optional[pd.DataFrame]:
    client = _get_db_client(config)

    try:
        df = client.get_candles(symbol=symbol, timeframe=timeframe)
    except Exception:
        return None

    if df is None or df.empty:
        return None

    return df


# =========================
# SAVE TO DB
# =========================
def _save_to_db(
    symbol: str,
    timeframe: str,
    df: pd.DataFrame,
    config: Dict[str, Any],
) -> None:
    if df is None or df.empty:
        return

    client = _get_db_client(config)

    try:
        client.save_candles(symbol=symbol, timeframe=timeframe, df=df)
    except Exception:
        return


# =========================
# VALIDATION
# =========================
def _is_data_sufficient(
    df: Optional[pd.DataFrame],
    timeframe: str,
    config: Dict[str, Any],
) -> bool:
    if df is None or df.empty:
        return False

    min_rows = _get_min_rows(timeframe, config)

    if min_rows <= 0:
        return True

    return len(df) >= min_rows


# =========================
# PUBLIC API
# =========================
def get_data_from_db(
    symbol: str,
    timeframe: str,
    config: Dict[str, Any],
) -> Optional[pd.DataFrame]:
    """
    Fetch + validate DB data
    """
    df = _fetch_from_db(symbol, timeframe, config)

    if not _is_data_sufficient(df, timeframe, config):
        return None

    return df


def save_data_to_db(
    symbol: str,
    timeframe: str,
    df: pd.DataFrame,
    config: Dict[str, Any],
) -> None:
    """
    Save candles to DB
    """
    _save_to_db(symbol, timeframe, df, config)
