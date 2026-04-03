from fastapi import APIRouter, Query
from app.domain.market.fyers_auth import FyersAuth
from app.core.settings import settings

router = APIRouter(prefix="/auth", tags=["Auth"])

# Validate required settings
if (
    not settings.FYERS_CLIENT_ID
    or not settings.FYERS_SECRET_KEY
    or not settings.FYERS_REDIRECT_URI
):
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
    token = auth_service.generate_access_token(auth_code)

    if token:
        return {"status": "success", "token": token}

    return {"status": "failed"}
