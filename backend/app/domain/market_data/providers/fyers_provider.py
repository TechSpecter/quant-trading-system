from __future__ import annotations

from typing import Optional
from fyers_apiv3 import fyersModel


class FyersAuth:
    def __init__(self, client_id: str, secret_key: str, redirect_uri: str):
        self.client_id = client_id
        self.secret_key = secret_key
        self.redirect_uri = redirect_uri

    # =========================
    # LOGIN URL (v3)
    # =========================
    def generate_login_url(self) -> str:
        """
        Generate login URL using Fyers v3 SessionModel
        """
        session = fyersModel.SessionModel(
            client_id=self.client_id,
            secret_key=self.secret_key,
            redirect_uri=self.redirect_uri,
            response_type="code",
            grant_type="authorization_code",
        )
        return session.generate_authcode()

    # =========================
    # ACCESS TOKEN (v3)
    # =========================
    def generate_access_token(self, auth_code: str) -> Optional[str]:
        """
        Exchange auth_code for access_token using v3 flow
        """
        try:
            session = fyersModel.SessionModel(
                client_id=self.client_id,
                secret_key=self.secret_key,
                redirect_uri=self.redirect_uri,
                response_type="code",
                grant_type="authorization_code",
            )

            session.set_token(auth_code)
            response = session.generate_token()

            if not isinstance(response, dict):
                return None

            access_token = response.get("access_token")
            return access_token if isinstance(access_token, str) else None

        except Exception:
            return None

    # =========================
    # CLIENT FACTORY
    # =========================
    def get_client(self, access_token: str):
        """
        Create FyersModel client using access_token
        """
        return fyersModel.FyersModel(
            client_id=self.client_id,
            token=access_token,
            is_async=False,
            log_path="",
        )


from datetime import datetime
import pandas as pd


def fetch_candles(
    symbol: str, timeframe: str, start: datetime, end: datetime, config
) -> Optional[pd.DataFrame]:
    """
    Fetch candles from Fyers with proper resolution mapping, date handling, and rate protection
    """
    fyers = config.get("fyers_client")

    if fyers is None:
        return None

    try:
        # =========================
        # RESOLUTION MAPPING
        # =========================
        resolution_map = {
            "D": "1D",
            "1H": "60",
            "4H": "240",
        }

        resolution = resolution_map.get(timeframe, timeframe)

        # =========================
        # DATE RANGE CLAMP
        # =========================
        from datetime import timedelta

        if resolution == "1D":
            max_days = 365
        else:
            max_days = 90

        safe_start = max(start, end - timedelta(days=max_days))

        # =========================
        # BUILD PAYLOAD (v3 compatible)
        # =========================
        payload = {
            "symbol": symbol,
            "resolution": resolution,
            "date_format": "1",  # YYYY-MM-DD
            "range_from": safe_start.strftime("%Y-%m-%d"),
            "range_to": end.strftime("%Y-%m-%d"),
            "cont_flag": "1",
        }

        # =========================
        # API CALL
        # =========================
        response = fyers.history(payload)

        # Handle API errors
        if not isinstance(response, dict) or response.get("s") != "ok":
            return None

        candles = response.get("candles", [])

        if not candles:
            return None

        # =========================
        # DATAFRAME CONVERSION
        # =========================
        df = pd.DataFrame(
            candles,
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )

        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")

        return df

    except Exception as e:
        print(f"❌ Fyers fetch error: {e}")
        return None
