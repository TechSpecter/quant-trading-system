from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI
from fastapi import Request
from app.db.session import redis_client

from app.api import auth

# Create FastAPI app
app = FastAPI(title="Quant Trading System", version="0.1.0")

# Register routers
app.include_router(auth.router)


# Health check
@app.get("/")
def root():
    return {"message": "Quant Trading System is running"}


# FYERS auth callback endpoint
@app.get("/auth/callback")
async def fyers_callback(request: Request):
    # Extract auth_code from query params
    auth_code = request.query_params.get("auth_code")

    if not auth_code:
        return {"status": "error", "message": "auth_code not found in callback"}

    try:
        # Store in Redis (expires in 1 day)
        await redis_client.set("fyers_auth_code", auth_code, ex=86400)

        print(f"✅ FYERS auth_code stored in Redis: {auth_code}")

        return {
            "status": "success",
            "message": "Auth code captured successfully. You can close this tab.",
        }

    except Exception as e:
        print(f"❌ Error storing auth_code: {e}")
        return {"status": "error", "message": str(e)}


@app.get("/auth/status")
async def auth_status():
    auth_code = await redis_client.get("fyers_auth_code")
    access_token = await redis_client.get("fyers_access_token")

    return {
        "auth_code": auth_code.decode("utf-8") if auth_code else None,
        "access_token": access_token.decode("utf-8") if access_token else None,
    }
