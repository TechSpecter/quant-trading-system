import logging
from typing import Optional
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
from fyers_apiv3 import fyersModel
import yaml

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

        # FIX: move to project root (outside backend)
        PROJECT_ROOT = BASE_DIR.parent

        # Load strategy.yaml config (single source of truth)
        config_path = PROJECT_ROOT / "config" / "strategy.yaml"
        print(f"📁 Loading config from: {config_path}")
        try:
            with open(config_path, "r") as f:
                self.strategy_config = yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load strategy.yaml from {config_path}: {e}")
            self.strategy_config = {}

    async def get_active_client(self):
        """Retrieves token from Redis or returns None if re-auth is needed."""
        raw_token = await redis_client.get("fyers_access_token")
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

    async def fetch_historical_data(
        self,
        symbol: str,
        resolution: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """Fetch historical OHLCV data from Fyers."""
        client = await self.get_active_client()

        if not client:
            logger.error("No active Fyers client. Please authenticate first.")
            return None

        # 🔥 Load resolution + lookback from strategy.yaml
        data_cfg = self.strategy_config.get("data", {})

        resolution_map = data_cfg.get("resolution_map", {})
        lookback_map = data_cfg.get("lookback", {})

        # 🔥 Fallback safety mapping (prevents API errors if YAML fails)
        default_resolution_map = {
            "D": "1D",
            "4H": "240",
            "1H": "60",
        }

        default_lookback = {
            "D": 365,
            "4H": 90,
            "1H": 30,
        }

        # Priority: YAML → fallback → raw
        api_resolution = resolution_map.get(resolution) or default_resolution_map.get(
            resolution, resolution
        )
        lookback_days = lookback_map.get(resolution) or default_lookback.get(
            resolution, 30
        )

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=lookback_days)

        range_from = date_from or start_date.strftime("%Y-%m-%d")
        range_to = date_to or end_date.strftime("%Y-%m-%d")

        print(
            f"📊 Requesting {symbol} | TF={resolution} → API={api_resolution} | lookback={lookback_days} | from={range_from} to={range_to}"
        )

        payload = {
            "symbol": symbol,
            "resolution": api_resolution,
            "date_format": "1",  # yyyy-mm-dd
            "range_from": range_from,
            "range_to": range_to,
            "cont_flag": 0,
        }

        response = client.history(data=payload)

        print(
            f"📡 Raw response status for {symbol}: {response.get('s') if isinstance(response, dict) else 'invalid'}"
        )

        if isinstance(response, dict) and response.get("s") == "ok":
            candles = response.get("candles", [])

            if not candles:
                logger.warning(f"No data returned for {symbol}")
                return None

            df = pd.DataFrame(candles)
            print(f"📊 Received rows for {symbol}: {len(df)}")
            df.columns = ["timestamp", "open", "high", "low", "close", "volume"]

            # Convert timestamp to IST
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
            df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Kolkata")

            # Ensure sufficient data for indicators (relaxed for intraday like 4H)
            min_required = 200 if resolution == "D" else 100

            if len(df) < min_required:
                logger.warning(
                    f"⚠️ Not enough candles for {symbol}: {len(df)} rows (required={min_required})"
                )
            else:
                logger.info(
                    f"✅ Sufficient candles for {symbol}: {len(df)} rows (required={min_required})"
                )

            return df

        logger.error(f"Fyers API Error for {symbol}: {response}")
        return None
