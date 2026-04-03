import logging
from typing import Optional
import pandas as pd
from fyers_apiv3 import fyersModel
from app.core.settings import settings

from app.db.session import redis_client

logger = logging.getLogger(__name__)


class FyersAPIClient:
    def __init__(self):
        # NOTE: Ensure your FYERS_CLIENT_ID in .env ends with "-100"
        self.client_id: str = str(settings.FYERS_CLIENT_ID)
        self.secret_key: str = str(settings.FYERS_SECRET_KEY)
        self.redirect_uri: str = str(settings.FYERS_REDIRECT_URI)

    def get_active_client(self):
        """Retrieves token from Redis or returns None if re-auth is needed."""
        raw_token = redis_client.get("fyers_access_token")
        token: Optional[str] = None

        if isinstance(raw_token, bytes):
            token = raw_token.decode()
        elif isinstance(raw_token, str):
            token = raw_token

        if token is None:
            return None

        # Ensure token is string for type safety
        token = str(token)

        if not token:
            return None

        return fyersModel.FyersModel(
            client_id=self.client_id, token=token, is_async=False, log_path="../logs"
        )

    def generate_auth_url(self):
        """Generates the URL for manual browser login."""
        # STRICT ISOLATION: Only pass response_type for the GET request
        session = fyersModel.SessionModel(
            client_id=self.client_id,
            secret_key=self.secret_key,
            redirect_uri=self.redirect_uri,
            response_type="code",
        )
        return session.generate_authcode()

    def authenticate(self, auth_code: str):
        """Exchanges auth_code for access_token and stores in Redis."""
        # STRICT ISOLATION: Only pass grant_type for the POST request
        session = fyersModel.SessionModel(
            client_id=self.client_id,
            secret_key=self.secret_key,
            redirect_uri=self.redirect_uri,
            grant_type="authorization_code",
        )
        session.set_token(auth_code)
        response = session.generate_token()

        if response.get("s") == "ok":
            token = response.get("access_token")
            # Store in Redis with 24-hour expiry (86400 seconds)
            redis_client.set("fyers_access_token", token, ex=86400)
            logger.info("✅ Fyers Access Token secured and stored in Redis.")
            return token
        else:
            logger.error(f"Authentication Failed: {response}")
            return None

    def fetch_historical_data(
        self, symbol: str, resolution: str, date_from: str, date_to: str
    ) -> Optional[pd.DataFrame]:
        """Fetch historical OHLCV data from Fyers."""
        client = self.get_active_client()
        if not client:
            logger.error("No active Fyers client found in Redis. Please authenticate.")
            return None

        payload = {
            "symbol": symbol,
            "resolution": resolution,
            "date_format": "1",  # 1 means yyyy-mm-dd format
            "range_from": date_from,
            "range_to": date_to,
            "cont_flag": "1",  # Continuous data for futures
        }

        # This call is synchronous because we set is_async=False in the Model
        response = client.history(data=payload)

        if isinstance(response, dict) and response.get("s") == "ok":
            df = pd.DataFrame(response["candles"])
            if not df.empty:
                df.columns = ["timestamp", "open", "high", "low", "close", "volume"]
                # Convert Unix timestamp to localized Indian Standard Time (IST)
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
                df["timestamp"] = (
                    df["timestamp"].dt.tz_localize("UTC").dt.tz_convert("Asia/Kolkata")
                )
            return df
        else:
            logger.error(f"Fyers API Error: {response}")
            return None
