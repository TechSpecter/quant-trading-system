from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI

from app.api import auth

# Create FastAPI app
app = FastAPI(title="Quant Trading System", version="0.1.0")

# Register routers
app.include_router(auth.router)


# Health check
@app.get("/")
def root():
    return {"message": "Quant Trading System is running"}
