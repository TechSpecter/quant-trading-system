from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any, Optional


DateRange = Tuple[datetime, datetime]


def _get_chunk_days(config: Dict[str, Any]) -> int:
    md_cfg = config.get("market_data", {})
    chunk_cfg = md_cfg.get("chunking", {})
    days = chunk_cfg.get("days_per_chunk", 30)

    try:
        days_int = int(days)
        return max(1, days_int)
    except Exception:
        return 30


def _is_chunking_enabled(config: Dict[str, Any]) -> bool:
    md_cfg = config.get("market_data", {})
    chunk_cfg = md_cfg.get("chunking", {})
    return bool(chunk_cfg.get("enabled", True))


def _validate_dates(
    start: Optional[datetime], end: Optional[datetime]
) -> Tuple[datetime, datetime]:
    if start is None or end is None:
        raise ValueError("start and end dates must not be None")

    if not isinstance(start, datetime) or not isinstance(end, datetime):
        raise TypeError("start and end must be datetime objects")

    if start > end:
        raise ValueError("start date cannot be greater than end date")

    return start, end


def _create_single_chunk(start: datetime, end: datetime) -> List[DateRange]:
    return [(start, end)]


def _split_into_chunks(
    start: datetime, end: datetime, chunk_days: int
) -> List[DateRange]:
    chunks: List[DateRange] = []

    current_start = start

    while current_start <= end:
        current_end = min(current_start + timedelta(days=chunk_days - 1), end)
        chunks.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)

    return chunks


def generate_chunks(
    start: datetime,
    end: datetime,
    config: Dict[str, Any],
) -> List[DateRange]:
    """
    Public API

    Splits date range into chunks based on config:
    - If chunking disabled → single chunk
    - Else → multiple chunks based on days_per_chunk
    """

    start_dt, end_dt = _validate_dates(start, end)

    if not _is_chunking_enabled(config):
        return _create_single_chunk(start_dt, end_dt)

    chunk_days = _get_chunk_days(config)

    return _split_into_chunks(start_dt, end_dt, chunk_days)
