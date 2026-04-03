import redis
from app.core.settings import settings

# -----------------------------
# Redis Client
# -----------------------------
redis_client = redis.Redis(
    host=settings.REDIS_HOST, port=settings.REDIS_PORT, decode_responses=True
)

# -----------------------------
# (Optional later) Postgres
# -----------------------------
# We will add SQLAlchemy later
