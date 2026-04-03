import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    # Fyers
    FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
    FYERS_SECRET_KEY = os.getenv("FYERS_SECRET_KEY")
    FYERS_REDIRECT_URI = os.getenv("FYERS_REDIRECT_URI")

    # Postgres
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
    POSTGRES_DB = os.getenv("POSTGRES_DB", "quant")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

    # Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))


settings = Settings()
