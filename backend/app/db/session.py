from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
import redis.asyncio as redis

from app.core.settings import settings

# -----------------------------
# Redis (ASYNC)
# -----------------------------
redis_client = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    decode_responses=False,  # keep bytes → we handle decoding manually
)


# -----------------------------
# Database (ASYNC SQLAlchemy)
# -----------------------------


engine = create_async_engine(settings.DATABASE_URL, echo=False, future=True)

async_session = async_sessionmaker(engine, expire_on_commit=False)
