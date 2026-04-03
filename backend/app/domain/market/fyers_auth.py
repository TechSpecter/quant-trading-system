"""
Minimal Fyers Auth (V3 SDK)
--------------------------
Purpose:
- Generate login URL
- Accept auth_code manually
- Exchange for access_token
- Print token
"""

import logging
from fyers_apiv3 import fyersModel
import redis

logger = logging.getLogger(__name__)


class FyersAuth:
    def __init__(self, client_id: str, secret_key: str, redirect_uri: str):
        self.client_id = client_id
        self.secret_key = secret_key
        self.redirect_uri = redirect_uri

        # Redis connection (local for now)
        self.redis = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # ----------------------------------
    # Step 1: Generate Login URL
    # ----------------------------------
    def generate_login_url(self) -> str:
        session = fyersModel.SessionModel(
            client_id=self.client_id,
            secret_key=self.secret_key,
            redirect_uri=self.redirect_uri,
            response_type="code",
        )

        url = session.generate_authcode()
        print("\n🔗 Login URL:")
        print(url)
        return url

    # ----------------------------------
    # Step 2: Exchange auth_code → token
    # ----------------------------------
    def generate_access_token(self, auth_code: str) -> str | None:
        session = fyersModel.SessionModel(
            client_id=self.client_id,
            secret_key=self.secret_key,
            redirect_uri=self.redirect_uri,
            grant_type="authorization_code",
        )

        session.set_token(auth_code)

        response = session.generate_token()

        print("\n📦 Raw Response:")
        print(response)

        if response.get("s") == "ok":
            access_token = response.get("access_token")

            # Store in Redis (24 hours)
            self.redis.set("fyers_access_token", access_token, ex=86400)

            print("\n✅ ACCESS TOKEN (stored in Redis):")
            print(access_token)

            return access_token

        print("\n❌ Failed to generate token")
        return None

    def get_stored_token(self) -> str | None:
        token = self.redis.get("fyers_access_token")

        # Ensure token is always a string
        if isinstance(token, bytes):
            token = token.decode()

        if isinstance(token, str):
            print("\n📦 Token fetched from Redis")
            return token

        return None
