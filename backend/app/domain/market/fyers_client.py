import logging
from typing import Optional
from pathlib import Path

import pandas as pd
from fyers_apiv3 import fyersModel

from app.core.settings import settings
from app.db.session import redis_client

logger = logging.getLogger(__name__)


class FyersAPIClient:
    def __init__(self):
        # Validate config (fail fast)
        if (
            not settings.FYERS_CLIENT_ID
            or not settings.FYERS_SECRET_KEY
            or not settings.FYERS_REDIRECT_URI
        ):
            raise ValueError("Missing Fyers configuration in environment variables")

        self.client_id: str = str(settings.FYERS_CLIENT_ID)
        self.secret_key: str = str(settings.FYERS_SECRET_KEY)
        self.redirect_uri: str = str(settings.FYERS_REDIRECT_URI)

        # Setup logs directory (important for Fyers SDK)
        BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
        self.log_dir = BASE_DIR / "logs"
        self.log_dir.mkdir(exist_ok=True)

    def get_active_client(self):
        """Retrieves token from Redis or returns None if re-auth is needed."""
        raw_token = redis_client.get("fyers_access_token")
        token: Optional[str] = None

        if isinstance(raw_token, bytes):
            token = raw_token.decode()
        elif isinstance(raw_token, str):
            token = raw_token

        if not token:
            logger.error("No valid Fyers token found in Redis.")
            return None

        return fyersModel.FyersModel(
            client_id=self.client_id,
            token=str(token),
            is_async=False,
            log_path=str(self.log_dir) + "/",  # IMPORTANT FIX
        )

    def generate_auth_url(self):
        """Generates the URL for manual browser login."""
        session = fyersModel.SessionModel(
            client_id=self.client_id,
            secret_key=self.secret_key,
            redirect_uri=self.redirect_uri,
            response_type="code",
        )
        return session.generate_authcode()

    def authenticate(self, auth_code: str):
        """Exchanges auth_code for access_token and stores in Redis."""
        session = fyersModel.SessionModel(
            client_id=self.client_id,
            secret_key=self.secret_key,
            redirect_uri=self.redirect_uri,
            grant_type="authorization_code",
        )

        session.set_token(auth_code)
        response = session.generate_token()

        if isinstance(response, dict) and response.get("s") == "ok":
            token = response.get("access_token")

            if not token:
                logger.error("Access token missing in Fyers response")
                return None

            # Store in Redis for 24 hours
            redis_client.set("fyers_access_token", token, ex=86400)

            logger.info("✅ Fyers Access Token stored in Redis")
            return token

        logger.error(f"❌ Authentication Failed: {response}")
        return None

    def fetch_historical_data(
        self, symbol: str, resolution: str, date_from: str, date_to: str
    ) -> Optional[pd.DataFrame]:
        """Fetch historical OHLCV data from Fyers."""
        client = self.get_active_client()

        if not client:
            logger.error("No active Fyers client. Please authenticate first.")
            return None

        payload = {
            "symbol": symbol,
            "resolution": resolution,
            "date_format": "1",  # yyyy-mm-dd
            "range_from": date_from,
            "range_to": date_to,
            "cont_flag": "1",
        }

        response = client.history(data=payload)

        if isinstance(response, dict) and response.get("s") == "ok":
            candles = response.get("candles", [])

            if not candles:
                logger.warning(f"No data returned for {symbol}")
                return None

            df = pd.DataFrame(candles)
            df.columns = ["timestamp", "open", "high", "low", "close", "volume"]

            # Convert timestamp to IST
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
            df["timestamp"] = (
                df["timestamp"].dt.tz_localize("UTC").dt.tz_convert("Asia/Kolkata")
            )

            return df

        logger.error(f"Fyers API Error for {symbol}: {response}")
        return None
