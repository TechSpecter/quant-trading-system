"""
Fyers Auth Module
-----------------
Handles:
- OAuth login URL generation
- Callback handling
- Access token storage in Redis
- Token reuse until expiry (24h)
"""

import os
import time
from typing import Optional, cast
import hashlib

import httpx
import redis
from fastapi import HTTPException
import urllib.parse


class FyersAuthService:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

        # Load from env
        self.client_id = os.getenv("FYERS_CLIENT_ID")
        self.secret_key = os.getenv("FYERS_SECRET_KEY")
        self.redirect_uri = os.getenv("FYERS_REDIRECT_URI")
        self.static_token = os.getenv("FYERS_ACCESS_TOKEN")

        if not self.client_id or not self.secret_key or not self.redirect_uri:
            raise ValueError("Missing Fyers credentials in environment variables")

        self.auth_url = "https://api.fyers.in/api/v3/generate-authcode"
        self.token_url = "https://api.fyers.in/api/v3/token"

        self.redis_key = "fyers:access_token"

    # --------------------------------------------------
    # Step 1: Generate Login URL
    # --------------------------------------------------
    def get_login_url(self) -> str:
        """Return Fyers login URL to open in browser"""

        encoded_redirect = urllib.parse.quote(self.redirect_uri, safe="")

        return (
            f"{self.auth_url}?client_id={self.client_id}"
            f"&redirect_uri={encoded_redirect}"
            f"&response_type=code"
            f"&state=sample_state"
        )

    # --------------------------------------------------
    # Step 2: Exchange auth_code for access token
    # --------------------------------------------------
    async def generate_access_token(self, auth_code: str) -> str:
        raise HTTPException(
            status_code=501,
            detail="Token exchange via HTTP is disabled. Use FYERS_ACCESS_TOKEN in .env for now.",
        )

    # --------------------------------------------------
    # Step 3: Get Token (Reuse if exists)
    # --------------------------------------------------
    def get_access_token(self) -> Optional[str]:
        """Fetch access token from ENV or Redis"""

        if self.static_token:
            return self.static_token

        token = self.redis.get(self.redis_key)
        if token:
            return cast(str, token)
        return None

    # --------------------------------------------------
    # Step 4: Ensure Token Exists
    # --------------------------------------------------
    def require_token(self) -> str:
        token = self.get_access_token()
        if not token:
            raise HTTPException(
                status_code=401,
                detail="Fyers token missing. Set FYERS_ACCESS_TOKEN in .env",
            )
        return token


# --------------------------------------------------
# Helper to create Redis client
# --------------------------------------------------
def get_redis_client() -> redis.Redis:
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        decode_responses=True,
    )


# --------------------------------------------------
# Factory
# --------------------------------------------------
def get_fyers_auth_service() -> FyersAuthService:
    redis_client = get_redis_client()
    return FyersAuthService(redis_client)
