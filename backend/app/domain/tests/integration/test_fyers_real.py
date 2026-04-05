from datetime import datetime
import redis
from fyers_apiv3 import fyersModel
from typing import Dict, Any, List, cast

from app.core.settings import settings


def test_fyers_history():
    # Redis client
    r = redis.Redis(host="localhost", port=6379, db=0)

    raw_token = r.get("fyers_access_token")  # type: ignore

    assert raw_token is not None, "Run make token first"

    # Fix decode typing issue
    access_token: str = (
        raw_token.decode()
        if isinstance(raw_token, (bytes, bytearray))
        else str(raw_token)
    )

    # Fyers client (SYNC)
    assert settings.FYERS_CLIENT_ID is not None, "FYERS_CLIENT_ID missing in .env"
    fyers = fyersModel.FyersModel(
        client_id=settings.FYERS_CLIENT_ID,
        token=access_token,
        is_async=False,  # IMPORTANT
        log_path="",
    )

    data: Dict[str, str] = {
        "symbol": "NSE:SBIN-EQ",
        "resolution": "D",
        "date_format": "1",
        "range_from": "2024-01-01",
        "range_to": "2024-02-01",
        "cont_flag": "1",
    }

    response_raw = fyers.history(data=data)

    # Force cast to dict (SDK is poorly typed)
    response = cast(Dict[str, Any], response_raw)

    assert response.get("s") == "ok", f"API failed: {response}"

    candles = cast(List[List[Any]], response.get("candles", []))

    assert len(candles) > 0, "No candles returned"

    print(f"✅ Fetched {len(candles)} candles from Fyers")
