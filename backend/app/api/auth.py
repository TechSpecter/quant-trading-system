"""
Auth API Routes
---------------
Provides endpoints for:
- Fyers login URL generation
- OAuth callback handling
"""

from fastapi import APIRouter, Depends, Query
from fastapi.responses import RedirectResponse

from app.domain.market.fyers_auth import (
    FyersAuthService,
    get_fyers_auth_service,
)

router = APIRouter(prefix="/auth", tags=["Auth"])


# --------------------------------------------------
# 1. Login Endpoint
# --------------------------------------------------
@router.get("/login")
def login(fyers_auth: FyersAuthService = Depends(get_fyers_auth_service)):
    """
    Returns Fyers login URL
    """
    login_url = fyers_auth.get_login_url()
    return {"login_url": login_url}


# --------------------------------------------------
# 2. Callback Endpoint
# --------------------------------------------------
@router.get("/callback")
async def callback(
    code: str = Query(..., description="Authorization code from Fyers"),
    fyers_auth: FyersAuthService = Depends(get_fyers_auth_service),
):
    """
    Handles Fyers OAuth callback
    Exchanges auth code for access token
    """

    await fyers_auth.generate_access_token(code)

    # Redirect to a simple success page (or frontend later)
    return RedirectResponse(url="/auth/success")


# --------------------------------------------------
# 3. Success Endpoint (temporary)
# --------------------------------------------------
@router.get("/success")
def success():
    return {"message": "Fyers authentication successful. Token stored in Redis."}
