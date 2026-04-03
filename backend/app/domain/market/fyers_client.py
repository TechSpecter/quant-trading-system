from dotenv import load_dotenv

load_dotenv()

"""
Fyers Client (API v3)
---------------------
Handles:
- Authenticated requests to Fyers
- Historical candle fetching
- Basic validation
"""

import os
import time
from typing import List, Dict

import httpx
from fastapi import HTTPException

from app.domain.market.fyers_auth import FyersAuthService, get_fyers_auth_service


class FyersClient:
    def __init__(self, auth_service: FyersAuthService):
        self.auth_service = auth_service
        self.base_url = "https://api.fyers.in/api/v3"

    def _map_timeframe(self, timeframe: str) -> str:
        mapping = {
            "1D": "D",
            "15Min": "15",
            "5Min": "5",
        }
        return mapping.get(timeframe, timeframe)

    # --------------------------------------------------
    # Fetch Historical Data
    # --------------------------------------------------
    async def fetch_candles(
        self,
        symbol: str,
        timeframe: str,
        lookback_days: int,
    ) -> List[Dict]:
        """
        Fetch historical candles from Fyers
        """

        token = self.auth_service.require_token()

        to_ts = int(time.time())
        from_ts = to_ts - (lookback_days * 86400)

        resolution = self._map_timeframe(timeframe)

        payload = {
            "symbol": symbol,
            "resolution": resolution,
            "date_format": "0",  # UNIX timestamps
            "range_from": str(from_ts),
            "range_to": str(to_ts),
            "cont_flag": 1,
        }

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        # url = f"{self.base_url}/history"
        url = "https://api.fyers.in/data-rest/v2/history"

        print("\n=== FYERS REQUEST DEBUG ===")
        print("URL:", url)
        print("HEADERS:", headers)
        print("PARAMS:", payload)

        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=payload, headers=headers)

        print("\n=== FYERS RESPONSE DEBUG ===")
        print("STATUS:", response.status_code)
        print("RESPONSE TEXT:", response.text)

        if response.status_code != 200:
            raise HTTPException(
                status_code=400,
                detail=f"Fyers API error: {response.text}",
            )

        data = response.json()

        if data.get("s") != "ok":
            raise HTTPException(
                status_code=400,
                detail=f"Fyers returned error: {data}",
            )

        candles = data.get("candles", [])

        return self._format_candles(candles)

    # --------------------------------------------------
    # Format candles
    # --------------------------------------------------
    def _format_candles(self, candles: List[List]) -> List[Dict]:
        """
        Convert Fyers response to structured format
        """
        formatted = []

        for c in candles:
            formatted.append(
                {
                    "timestamp": c[0],
                    "open": c[1],
                    "high": c[2],
                    "low": c[3],
                    "close": c[4],
                    "volume": c[5],
                }
            )

        return formatted


# --------------------------------------------------
# Factory
# --------------------------------------------------
def get_fyers_client() -> FyersClient:
    auth_service = get_fyers_auth_service()
    return FyersClient(auth_service)
