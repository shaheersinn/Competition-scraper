"""
Centralised config — reads from .env
Copy .env.example → .env and fill in your values.
"""
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # MongoDB
    MONGO_URI: str = "mongodb://localhost:27017"
    MONGO_DB: str = "competition_cases"

    # Sentry (GitHub Education gives you Sentry Team free)
    SENTRY_DSN: str = ""

    # Crawl politeness — seconds between requests
    CRAWL_DELAY_SECONDS: float = 1.5

    class Config:
        env_file = ".env"

settings = Settings()

# Init Sentry early so all exceptions are captured
import sentry_sdk
if settings.SENTRY_DSN:
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        traces_sample_rate=0.2,
        environment="production",
    )
