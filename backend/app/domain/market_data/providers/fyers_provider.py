from __future__ import annotations

from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd


# =========================
# CONFIG HELPERS
# =========================
def _get_resolution(timeframe: str, config: Dict[str, Any]) -> str:
    md_cfg = config.get("strategy", {})
    tf_map = md_cfg.get("data", {}).get("resolution_map", {})

    return tf_map.get(timeframe, timeframe)


def _get_date_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


# =========================
# FYERS CLIENT WRAPPER
# =========================
def _get_fyers_client(config: Dict[str, Any]):
    """
    Expect fyers client to be injected via config
    """
    client = config.get("fyers_client")
    if client is None:
        raise ValueError("Fyers client not found in config")
    return client


# =========================
# API CALL
# =========================
def _fetch_from_fyers(
    symbol: str,
    resolution: str,
    start: datetime,
    end: datetime,
    config: Dict[str, Any],
) -> pd.DataFrame:
    client = _get_fyers_client(config)

    payload = {
        "symbol": symbol,
        "resolution": resolution,
        "date_format": "1",
        "range_from": _get_date_str(start),
        "range_to": _get_date_str(end),
        "cont_flag": "1",
    }

    response = {}
    try:
        response = client.history(payload)
    except Exception:
        return pd.DataFrame()

    if not isinstance(response, dict):
        return pd.DataFrame()

    candles = response.get("candles", [])

    if not candles:
        return pd.DataFrame()

    try:
        df = pd.DataFrame(
            candles,
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )
    except Exception:
        return pd.DataFrame()

    return df


# =========================
# PUBLIC API
# =========================
def fetch_candles(
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    config: Dict[str, Any],
) -> pd.DataFrame:
    """
    Fetch candles from Fyers
    - Pure provider (no DB, no cache)
    """

    resolution = _get_resolution(timeframe, config)

    df = _fetch_from_fyers(symbol, resolution, start, end, config)

    return df if df is not None else pd.DataFrame()
