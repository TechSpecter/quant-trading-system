from fastapi import APIRouter, Query
from app.domain.market_data.fyers_auth import FyersAuth
from app.core.settings import settings

from app.domain.market_data.cache.redis_cache import _get_redis_client

router = APIRouter(prefix="/auth", tags=["Auth"])

# Validate required settings
if not settings.FYERS_CLIENT_ID or not settings.FYERS_REDIRECT_URI:
    raise ValueError("Missing Fyers configuration in environment variables")

auth_service = FyersAuth(
    client_id=str(settings.FYERS_CLIENT_ID),
    secret_key=str(settings.FYERS_SECRET_KEY),
    redirect_uri=str(settings.FYERS_REDIRECT_URI),
)


@router.get("/login")
def login():
    url = auth_service.generate_login_url()
    return {"login_url": url}


@router.get("/callback")
def callback(auth_code: str = Query(...)):
    try:
        token = auth_service.generate_access_token(auth_code)

        if token is None:
            return {"status": "failed", "error": "Token generation failed"}

        redis_client = _get_redis_client()
        redis_client.set("fyers_access_token", token, ex=86400)

        return {"status": "success"}

    except Exception as e:
        return {"status": "failed", "error": str(e)}
